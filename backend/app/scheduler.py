"""Single coordinator, durable intent before external side effects, Ray as executor."""
import json
import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import psutil
import redis
from ray.job_submission import JobSubmissionClient

from . import config, store

log = logging.getLogger(__name__)
TERMINAL = {'succeeded', 'failed', 'cancelled'}
RAY_TERMINAL = {'SUCCEEDED', 'FAILED', 'STOPPED'}


def job_dir(job):
    return config.DATA / 'jobs' / job['id'] / (job['ray_submission_id'] or 'attempt-1')


def read_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return {}


def process_alive(pid, created):
    if not pid or not created:
        return False
    try:
        p = psutil.Process(int(pid))
        return abs(p.create_time() - float(created)) < 0.1 and p.is_running() and p.status() != psutil.STATUS_ZOMBIE
    except (psutil.Error, ValueError):
        return False


def stop_local(job):
    """Kill only the recorded process tree, checking creation time against PID reuse."""
    info = read_json(job_dir(job) / 'process.json')
    identities = list(info.get('children', [])) + [
        {'pid': info.get('pid'), 'create_time': info.get('create_time')},
        {'pid': info.get('worker_pid'), 'create_time': info.get('worker_create_time')}]
    for identity in identities:
        if not process_alive(identity['pid'], identity['create_time']):
            continue
        try:
            parent = psutil.Process(identity['pid'])
            children = parent.children(recursive=True)
            for p in reversed(children):
                try:
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
            parent.kill()
            psutil.wait_procs(children + [parent], timeout=2)
        except psutil.NoSuchProcess:
            pass


def local_alive(job):
    info = read_json(job_dir(job) / 'process.json')
    identities = list(info.get('children', [])) + [
        {'pid': info.get('pid'), 'create_time': info.get('create_time')},
        {'pid': info.get('worker_pid'), 'create_time': info.get('worker_create_time')}]
    return any(process_alive(p['pid'], p['create_time']) for p in identities)


class Scheduler:
    def __init__(self):
        self.lock = threading.RLock()
        self.halt = threading.Event()
        self.redis = redis.Redis.from_url(config.REDIS_URL, decode_responses=True,
                                          socket_connect_timeout=1, socket_timeout=1)
        self.client = None
        self.ray_ok = False
        self.redis_ok = False
        self.redis_recovered_at = None
        self.last_error = None
        self.thread = None
        self.owner_file = None

    def start(self):
        # ponytail: one local coordinator; add a fenced lease before multiple API replicas.
        self.owner_file = open(config.DATA / 'scheduler.lock', 'a+b')
        self.owner_file.seek(0)
        self.owner_file.write(b'0')
        self.owner_file.flush()
        self.owner_file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.owner_file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.owner_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self.owner_file.close()
            raise RuntimeError('该数据目录已有调度器运行；请使用单个 Uvicorn worker') from exc
        self.thread = threading.Thread(target=self.run, name='scheduler', daemon=True)
        self.thread.start()

    def close(self):
        self.halt.set()
        if self.thread:
            self.thread.join(timeout=15)
        if self.owner_file:
            self.owner_file.close()
        self.redis.close()

    def run(self):
        while not self.halt.is_set():
            try:
                self.tick()
            except Exception as exc:
                self.last_error = str(exc)
                log.exception('Scheduler tick failed; reservations retained')
            self.halt.wait(config.INTERVAL)

    def tick(self):
        was_redis_ok = self.redis_ok
        try:
            self.redis_ok = bool(self.redis.ping())
        except redis.RedisError:
            self.redis_ok = False
        if self.redis_ok and not was_redis_ok:
            self.redis_recovered_at = time.time()
        try:
            if self.client is None:
                self.client = JobSubmissionClient(config.RAY_ADDRESS)
            self.client.get_version()
            self.ray_ok = True
        except Exception as exc:
            self.ray_ok = False
            self.last_error = f'Ray unavailable: {exc}'
            # Lost connectivity is not evidence of stopped execution. Preserve reservations.
            return
        with self.lock:
            with store.connection() as db:
                active = [dict(r) for r in db.execute('SELECT * FROM jobs WHERE reserved=1')]
            # Reconcile ALL old reservations before considering any new work.
            for job in active:
                self.reconcile(job)
            if self.redis_ok:
                self.dispatch()
            self.last_error = None

    def submit(self, job):
        folder = job_dir(job)
        folder.mkdir(parents=True, exist_ok=True)
        (folder / 'config.json').write_text(json.dumps({k: job[k] for k in
             ['name', 'command', 'cpu', 'timeout_seconds']}, ensure_ascii=False), encoding='utf-8')
        env = {
            'TRAINPLANE_DATA_DIR': str(config.DATA), 'REDIS_URL': config.REDIS_URL,
            'TRAIN_CPUS': str(job['cpu']), 'TRAIN_JOB_DIR': str(folder),
            'PYTHONUNBUFFERED': '1', 'PYTHONUTF8': '1',
            'OMP_NUM_THREADS': str(job['cpu']), 'MKL_NUM_THREADS': str(job['cpu']),
            'YOLO_CONFIG_DIR': str(config.DATA / 'ultralytics'),
            'PYTHONPATH': str(config.ROOT),
        }
        args = [sys.executable, '-m', 'app.worker', '--job-id', job['id'],
                '--attempt-id', job['ray_submission_id']]
        entrypoint = subprocess.list2cmdline(args) if os.name == 'nt' else __import__('shlex').join(args)
        self.client.submit_job(submission_id=job['ray_submission_id'], entrypoint=entrypoint,
                               entrypoint_num_cpus=job['cpu'], entrypoint_num_gpus=0,
                               runtime_env={'env_vars': env})
        store.update(job['id'], submitted=1)

    def dispatch(self):
        with store.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            used = db.execute('SELECT COALESCE(SUM(cpu),0) FROM jobs WHERE reserved=1').fetchone()[0]
            queue = db.execute("SELECT * FROM jobs WHERE status='pending' AND reserved=0 ORDER BY priority DESC,created_at,id").fetchall()
            selected = []
            for row in queue:
                job = dict(row)
                if job['cpu'] > config.TOTAL_CPUS - used:
                    # Strict priority/FIFO: no backfill that could starve a large first job.
                    break
                attempt = job['id'] + '-1'
                db.execute('UPDATE jobs SET reserved=1,ray_submission_id=? WHERE id=?', (attempt, job['id']))
                job.update(reserved=1, ray_submission_id=attempt)
                selected.append(job)
                used += job['cpu']
        for job in selected:
            try:
                self.submit(job)
            except Exception:
                # The server may have accepted the request before a transport error.
                # Keep the durable reservation and query the stable submission ID next tick.
                log.exception('Submission uncertain: %s', job['id'])

    def finish(self, job, status, exit_code=None, error=None):
        store.update(job['id'], status=status, reserved=0, finished_at=time.time(),
                     exit_code=exit_code, error=error)

    def request_cancel(self, job_id):
        with self.lock:
            job = store.get(job_id)
            if not job or job['status'] in TERMINAL:
                return job
            if not job['reserved']:
                self.finish(job, 'cancelled', error='用户取消排队任务')
            else:
                store.update(job_id, stop_reason=job['stop_reason'] or 'cancelled')
            return store.get(job_id)

    def reconcile(self, job):
        try:
            info = self.client.get_job_info(job['ray_submission_id'])
        except RuntimeError as exc:
            if 'does not exist' not in str(exc) and 'not found' not in str(exc).lower():
                raise
            if not job['submitted'] and not job['stop_reason']:
                self.submit(job)
            else:
                stop_local(job)
                if not local_alive(job):
                    status = 'cancelled' if job['stop_reason'] == 'cancelled' else 'failed'
                    self.finish(job, status, error='Ray 中任务已不存在' if status == 'failed' else '用户取消')
            return
        state = str(info.status.value if hasattr(info.status, 'value') else info.status)
        if not job['submitted']:
            store.update(job['id'], submitted=1)
        folder = job_dir(job)
        process = read_json(folder / 'process.json')
        now = time.time()
        fields = {}
        if state == 'RUNNING' and job['status'] == 'pending':
            fields.update(status='running', running_seen_at=now)
        if process.get('started_at') and not job['started_at']:
            fields['started_at'] = process['started_at']
        if self.redis_ok:
            heartbeat = self.redis.get('heartbeat:' + job['ray_submission_id'])
            if heartbeat:
                fields['heartbeat_at'] = float(heartbeat)
        if fields:
            store.update(job['id'], **fields)
            job.update(fields)
        result = read_json(folder / 'result.json')
        reason = job['stop_reason']
        if state not in RAY_TERMINAL and not reason:
            if job['started_at'] and now - job['started_at'] > job['timeout_seconds']:
                reason = 'runtime_timeout'
            elif state == 'RUNNING' and self.redis_ok and (self.redis_recovered_at is None or now-self.redis_recovered_at > 4):
                if job['heartbeat_at'] and now - job['heartbeat_at'] > config.HEARTBEAT_TIMEOUT:
                    reason = 'heartbeat_timeout'
                elif not job['heartbeat_at'] and now - (job['running_seen_at'] or now) > config.STARTUP_GRACE:
                    reason = 'heartbeat_startup_timeout'
            if reason:
                store.update(job['id'], stop_reason=reason)
                job['stop_reason'] = reason
        if reason and state not in RAY_TERMINAL:
            if self.redis_ok:
                self.redis.set('cancel:' + job['ray_submission_id'], reason, ex=3600)
            stop_local(job)
            self.client.stop_job(job['ray_submission_id'])
            return
        if state in RAY_TERMINAL:
            if local_alive(job):
                stop_local(job)
                if local_alive(job):
                    return
            code = result.get('exit_code', getattr(info, 'driver_exit_code', None))
            if reason:
                status = 'cancelled' if reason == 'cancelled' else 'failed'
                errors = {'cancelled': '用户取消', 'runtime_timeout': '超过最大运行时长',
                          'heartbeat_timeout': 'worker 心跳超时', 'heartbeat_startup_timeout': 'worker 启动后未上报心跳'}
                self.finish(job, status, code, errors[reason])
            elif (result.get('error') or '').startswith('timeout'):
                self.finish(job, 'failed', code, '超过最大运行时长')
            elif state == 'SUCCEEDED' and code == 0:
                self.finish(job, 'succeeded', code)
            else:
                self.finish(job, 'failed', code, result.get('error') or info.message or f'Ray exited: {state}')

