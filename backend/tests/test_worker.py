"""Real subprocess checks. Only Redis cancellation needs a running Redis service."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid

import psutil
import pytest
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from app.worker import matching_process, stop_tree


BACKEND = Path(__file__).resolve().parents[1]


def command_line(args):
    return subprocess.list2cmdline(args) if os.name == 'nt' else shlex.join(args)


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    raise AssertionError('condition not reached before timeout')


def read_json(path):
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return {}


def identity_alive(identity):
    process = matching_process(identity['pid'], identity['create_time'])
    try:
        return process is not None and process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


@contextmanager
def worker(tmp_path, command, timeout=30):
    job_id = uuid.uuid4().hex
    attempt_id = job_id + '-1'
    folder = tmp_path / 'jobs' / job_id / attempt_id
    folder.mkdir(parents=True)
    (folder / 'config.json').write_text(json.dumps({
        'name': 'worker test', 'command': command, 'cpu': 1,
        'timeout_seconds': timeout,
    }), encoding='utf-8')
    environment = dict(os.environ, TRAINPLANE_DATA_DIR=str(tmp_path), PYTHONIOENCODING='utf-8',
                       PYTHONPATH=str(BACKEND))
    process = subprocess.Popen(
        [sys.executable, '-m', 'app.worker', '--job-id', job_id, '--attempt-id', attempt_id],
        cwd=BACKEND, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0),
    )
    try:
        yield process, folder, attempt_id
    finally:
        info = read_json(folder / 'process.json')
        identities = {item['pid']: item['create_time'] for item in info.get('children', [])}
        for pid_key, time_key in [('pid', 'create_time'), ('worker_pid', 'worker_create_time')]:
            if info.get(pid_key) and info.get(time_key):
                identities[info[pid_key]] = info[time_key]
        stop_tree(identities)
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


@pytest.mark.parametrize('exit_code', [0, 7])
def test_real_exit_code_and_utf8_log(tmp_path, exit_code):
    # Bare python must resolve to the worker's venv, with app importable from backend.
    command = command_line(['python', '-m', 'app.demo', '--sleep', '0.1', '--exit-code', str(exit_code)])
    with worker(tmp_path, command) as (process, folder, _):
        out, err = process.communicate(timeout=15)
        assert process.returncode == exit_code, (out, err, read_json(folder / 'result.json'))
        result = read_json(folder / 'result.json')
        assert result['exit_code'] == exit_code and result['error'] is None
        assert '中文日志 ✓ <tag> & "quotes"' in (folder / 'train.log').read_text(encoding='utf-8')
        assert not identity_alive(read_json(folder / 'process.json'))


def test_worker_timeout_stops_training(tmp_path):
    with worker(tmp_path, 'python -m app.demo --sleep 120', timeout=1) as (process, folder, _):
        process.communicate(timeout=15)
        result = read_json(folder / 'result.json')
        assert process.returncode != 0 and result['exit_code'] != 0
        assert result['error'].startswith('timeout')
        assert not identity_alive(read_json(folder / 'process.json'))


def spawn_tree_script(tmp_path, parent_sleep=120):
    script = tmp_path / 'parent with spaces.py'
    script.write_text(
        'import json, os, pathlib, subprocess, sys, time\n'
        'import psutil\n'
        'child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"])\n'
        'identity = {"pid": child.pid, "create_time": psutil.Process(child.pid).create_time()}\n'
        'folder = pathlib.Path(os.environ["TRAIN_JOB_DIR"])\n'
        '(folder / "child.json").write_text(json.dumps(identity), encoding="utf-8")\n'
        'print("training spawned child", flush=True)\n'
        f'time.sleep({parent_sleep})\n', encoding='utf-8',
    )
    return command_line(['python', str(script)])


def test_parent_exit_does_not_leave_known_child_alive(tmp_path):
    with worker(tmp_path, spawn_tree_script(tmp_path, parent_sleep=2)) as (process, folder, _):
        child = wait_for(lambda: read_json(folder / 'child.json'))
        wait_for(lambda: read_json(folder / 'process.json').get('children'))
        assert identity_alive(child)
        process.communicate(timeout=15)
        assert process.returncode == 0, read_json(folder / 'result.json')
        assert not identity_alive(child)
        assert child in read_json(folder / 'process.json')['children']


def test_redis_cancel_stops_training_and_descendant(tmp_path):
    client = redis.Redis.from_url(
        os.environ.get('REDIS_URL', 'redis://127.0.0.1:6379/0'),
        socket_connect_timeout=1, socket_timeout=1, retry=Retry(NoBackoff(), 0),
    )
    try:
        client.ping()
    except redis.RedisError:
        client.close()
        pytest.skip('Redis is required for the real cancellation integration check')
    attempt_id = None
    try:
        with worker(tmp_path, spawn_tree_script(tmp_path)) as (process, folder, attempt_id):
            child = wait_for(lambda: read_json(folder / 'child.json'))
            wait_for(lambda: read_json(folder / 'process.json').get('children'))
            wait_for(lambda: client.get('heartbeat:' + attempt_id))
            assert identity_alive(child)
            client.set('cancel:' + attempt_id, 'cancelled', ex=30)
            process.communicate(timeout=15)
            result = read_json(folder / 'result.json')
            assert process.returncode != 0 and result['exit_code'] != 0
            assert result['error'] == 'cancelled'
            assert not identity_alive(read_json(folder / 'process.json'))
            assert not identity_alive(child)
    finally:
        if attempt_id:
            client.delete('cancel:' + attempt_id, 'heartbeat:' + attempt_id)
        client.close()
