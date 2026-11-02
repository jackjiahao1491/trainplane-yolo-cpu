import asyncio
import json
import math
import os
import stat
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psutil
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from . import config, store
from .auth import authorize, get_token
from .scheduler import Scheduler, TERMINAL, effective_priority, job_dir, read_json

scheduler = Scheduler()


@asynccontextmanager
async def lifespan(app):
    if (config.TOTAL_CPUS < 1 or config.TOTAL_MEMORY_MB < 64 or
            not 64 <= config.DEFAULT_MEMORY_MB <= config.TOTAL_MEMORY_MB or
            config.AGING_INTERVAL_SECONDS <= 0 or config.BACKFILL_MAX_BYPASS < 1 or
            config.EXECUTION_MODE not in {'docker', 'process'}):
        raise RuntimeError('CPU、内存、老化间隔、回填次数或执行模式配置无效')
    store.initialize()
    get_token()
    scheduler.start()
    yield
    scheduler.close()


app = FastAPI(title='TrainPlane · CPU Training Platform', version='1.1.0', lifespan=lifespan,
              dependencies=[Depends(authorize)])
app.add_middleware(CORSMiddleware, allow_origins=['http://127.0.0.1:5173', 'http://localhost:5173'],
                   allow_methods=['GET', 'POST'], allow_headers=['Content-Type', 'Authorization', 'Last-Event-ID'])


def public(job):
    out = {k: v for k, v in job.items() if k not in {'reserved', 'submitted', 'running_seen_at', 'stop_requested_at'}}
    now = time.time()
    end = job['started_at'] or job['finished_at'] or now
    out['queue_wait_seconds'] = max(0, end-job['created_at'])
    out['effective_priority'] = effective_priority(job, end)
    out['backfilled'] = bool(job.get('backfilled'))
    out['resource_usage'] = read_json(job_dir(job) / 'resource.json') or None
    for field in ['created_at', 'started_at', 'finished_at', 'heartbeat_at']:
        if out[field] is not None:
            out[field] = datetime.fromtimestamp(out[field], timezone.utc).isoformat()
    return out


def require(job_id):
    job = store.get(job_id)
    if not job:
        raise HTTPException(404, '任务不存在')
    return job


class JobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    command: str = Field(default=config.DEFAULT_COMMAND, min_length=1, max_length=4096)
    cpu: int = Field(default=2, ge=1)
    timeout_seconds: int = Field(default=1800, ge=1, le=86400)
    priority: int = Field(default=0, ge=0, le=10)
    memory_mb: int = Field(default=config.DEFAULT_MEMORY_MB, ge=64)


@app.get('/api/health')
def health():
    return {'status': 'ok' if scheduler.ray_ok and scheduler.redis_ok and not scheduler.last_error else 'degraded',
            'ray_ok': scheduler.ray_ok, 'redis_ok': scheduler.redis_ok, 'error': scheduler.last_error}


@app.get('/api/auth/session')
def session():
    return {'authenticated': True, 'username': 'operator'}


@app.get('/api/cluster')
def cluster():
    with store.connection() as db:
        used = db.execute('SELECT COALESCE(SUM(cpu),0) FROM jobs WHERE reserved=1').fetchone()[0]
        memory = db.execute('SELECT COALESCE(SUM(memory_mb),0) FROM jobs WHERE reserved=1').fetchone()[0]
        counts = dict(db.execute('SELECT status,COUNT(*) FROM jobs GROUP BY status').fetchall())
    return {'total_cpus': config.TOTAL_CPUS, 'allocated_cpus': used,
            'available_cpus': max(0, config.TOTAL_CPUS-used),
            'total_memory_mb': config.TOTAL_MEMORY_MB, 'allocated_memory_mb': memory,
            'available_memory_mb': max(0, config.TOTAL_MEMORY_MB-memory),
            'aging_interval_seconds': config.AGING_INTERVAL_SECONDS,
            'backfill_max_bypass': config.BACKFILL_MAX_BYPASS,
            'execution_mode': config.EXECUTION_MODE,
            'pending_jobs': counts.get('pending', 0), 'running_jobs': counts.get('running', 0),
            'redis_ok': scheduler.redis_ok, 'ray_ok': scheduler.ray_ok,
            'nodes': [{'id': 'local', 'name': '本机 CPU 节点', 'total_cpus': config.TOTAL_CPUS,
                       'allocated_cpus': used, 'cpu_percent': psutil.cpu_percent(),
                       'memory_percent': psutil.virtual_memory().percent}]}


@app.post('/api/jobs', status_code=201)
def create_job(body: JobCreate):
    if body.cpu > config.TOTAL_CPUS:
        raise HTTPException(422, f'CPU 申请不能超过平台总配额 {config.TOTAL_CPUS}')
    if body.memory_mb > config.TOTAL_MEMORY_MB:
        raise HTTPException(422, f'内存申请不能超过平台总配额 {config.TOTAL_MEMORY_MB} MiB')
    if not body.name.strip() or not body.command.strip():
        raise HTTPException(422, '任务名称和命令不能为空')
    job_id = uuid.uuid4().hex[:16]
    with store.connection() as db:
        db.execute('INSERT INTO jobs(id,name,command,cpu,timeout_seconds,priority,memory_mb,created_at) VALUES(?,?,?,?,?,?,?,?)',
                   (job_id, body.name.strip(), body.command.strip(), body.cpu, body.timeout_seconds, body.priority,
                    body.memory_mb, time.time()))
    return public(store.get(job_id))


@app.get('/api/jobs')
def list_jobs(page: int = Query(1, ge=1), page_size: int = Query(10, ge=1, le=100),
              status: str | None = None, search: str = Query('', max_length=100)):
    clauses, args = [], []
    if status:
        if status not in TERMINAL | {'pending', 'running'}:
            raise HTTPException(422, '未知任务状态')
        clauses.append('status=?')
        args.append(status)
    if search:
        clauses.append("id LIKE ? ESCAPE '\\'")
        args.append('%' + search.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%')
    where = ' WHERE ' + ' AND '.join(clauses) if clauses else ''
    with store.connection() as db:
        total = db.execute('SELECT COUNT(*) FROM jobs' + where, args).fetchone()[0]
        rows = db.execute('SELECT * FROM jobs' + where + ' ORDER BY created_at DESC,id LIMIT ? OFFSET ?',
                          (*args, page_size, (page-1)*page_size)).fetchall()
    return {'items': [public(dict(r)) for r in rows], 'total': total, 'page': page, 'page_size': page_size}


@app.get('/api/jobs/{job_id}')
def detail(job_id: str):
    return public(require(job_id))


@app.post('/api/jobs/{job_id}/cancel')
def cancel(job_id: str):
    require(job_id)
    return public(scheduler.request_cancel(job_id))


@app.get('/api/jobs/{job_id}/logs')
def logs(job_id: str, offset: int = Query(0, ge=0), limit: int = Query(65536, ge=4, le=1048576)):
    job = require(job_id)
    path = job_dir(job) / 'train.log'
    try:
        with path.open('rb') as stream:
            size = path.stat().st_size
            if offset > size:
                raise HTTPException(416, 'offset 超过日志大小，请从 0 重新读取')
            stream.seek(offset)
            chunk = stream.read(limit)
        try:
            content = chunk.decode('utf-8')
        except UnicodeDecodeError as exc:
            if exc.reason == 'unexpected end of data' and exc.end == len(chunk) and (offset+len(chunk) < size or job['status'] not in TERMINAL):
                chunk = chunk[:exc.start]
            content = chunk.decode('utf-8', errors='replace')
        next_offset = offset + len(chunk)
        return {'content': content, 'next_offset': next_offset, 'eof': next_offset >= size,
                'finished': job['status'] in TERMINAL}
    except FileNotFoundError:
        if offset:
            raise HTTPException(416, '日志尚不存在或已被移除，请从0重新读取')
        return {'content': '', 'next_offset': 0, 'eof': True, 'finished': job['status'] in TERMINAL}


LOG_STREAM_POLL_SECONDS = 0.5
LOG_STREAM_HEARTBEAT_SECONDS = 10


def log_event(event, payload):
    cursor = f"id: {payload['next_offset']}\n" if 'next_offset' in payload else ''
    return f'event: {event}\n{cursor}data: {json.dumps(payload, ensure_ascii=False)}\n\n'


@app.get('/api/jobs/{job_id}/logs/stream', response_class=StreamingResponse,
         responses={200: {'content': {'text/event-stream': {'schema': {'type': 'string'}}}}})
def log_stream(job_id: str, request: Request, offset: int = Query(0, ge=0),
               last_event_id: str | None = Header(None, max_length=32)):
    if last_event_id is not None:
        if not last_event_id or not last_event_id.isascii() or not last_event_id.isdecimal():
            raise HTTPException(422, 'Last-Event-ID必须为非负十进制字节位置')
        offset = int(last_event_id)
    # Validate before sending HTTP 200. Authentication remains the app's Bearer dependency.
    logs(job_id, offset=offset, limit=65536)

    async def stream():
        cursor = offset
        heartbeat_at = time.monotonic()
        yield 'retry: 1000\n\n'
        while not await request.is_disconnected():
            try:
                # Re-read status BEFORE the file, and resolve the current attempt path
                # each time. A terminal snapshot guarantees the worker has closed its log.
                chunk = await run_in_threadpool(logs, job_id, cursor, 65536)
            except HTTPException as exc:
                code = {404: 'job_missing', 416: 'log_truncated'}.get(exc.status_code, 'log_read_error')
                yield log_event('error', {'code': code, 'message': str(exc.detail)})
                return
            except (OSError, ValueError):
                yield log_event('error', {'code': 'log_read_error', 'message': '日志暂不可读，请重新连接'})
                return
            if chunk['content']:
                cursor = chunk['next_offset']
                yield log_event('log', chunk)
                heartbeat_at = time.monotonic()
            if chunk['finished'] and chunk['eof']:
                # Final content was already delivered by log; end never repeats it.
                yield log_event('end', {**chunk, 'content': ''})
                return
            if chunk['content'] and not chunk['eof']:
                continue
            if time.monotonic() - heartbeat_at >= LOG_STREAM_HEARTBEAT_SECONDS:
                yield ': heartbeat\n\n'
                heartbeat_at = time.monotonic()
            await asyncio.sleep(LOG_STREAM_POLL_SECONDS)

    return StreamingResponse(stream(), media_type='text/event-stream',
                             headers={'Cache-Control': 'no-cache, no-transform', 'X-Accel-Buffering': 'no'})


@app.get('/api/jobs/{job_id}/metrics')
def metrics(job_id: str):
    job = require(job_id)
    items = []
    folder = job_dir(job)
    workspace = folder / 'workspace'
    path = (workspace if workspace.is_dir() else folder) / 'metrics.jsonl'
    try:
        # The container may write this file. Never follow its symlinks or block on a FIFO.
        descriptor = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            os.close(descriptor)
            raise HTTPException(400, '指标路径必须为普通文件')
        with os.fdopen(descriptor, encoding='utf-8', errors='replace') as stream:
            consumed = 0
            while consumed < 1048576:
                line = stream.readline(16384)
                if not line:
                    break
                consumed += len(line.encode('utf-8'))
                try:
                    entry = json.loads(line)
                    if not isinstance(entry, dict) or type(entry.get('epoch')) is not int:
                        continue
                    values = entry.get('metrics', entry)
                    if not isinstance(values, dict):
                        continue
                    def value(*keys):
                        raw = next((values[k] for k in keys if k in values), None)
                        try:
                            number = float(raw)
                            return number if math.isfinite(number) else None
                        except (TypeError, ValueError, OverflowError):
                            return None
                    items.append({'epoch': entry['epoch'],
                                  'box_loss': value('box_loss', 'train/box_loss'),
                                  'cls_loss': value('cls_loss', 'train/cls_loss'),
                                  'dfl_loss': value('dfl_loss', 'train/dfl_loss'),
                                  'map50': value('map50', 'metrics/mAP50(B)'),
                                  'map50_95': value('map50_95', 'metrics/mAP50-95(B)')})
                    if len(items) >= 1000:
                        break
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        pass
    except OSError:
        raise HTTPException(400, '指标文件不可读或包含符号链接')
    return {'items': items}


@app.get('/api/jobs/{job_id}/resources')
def resources(job_id: str, after: int = Query(0, ge=0), limit: int = Query(300, ge=1, le=1000)):
    job = require(job_id)
    folder = job_dir(job)
    items, offset, eof = [], after, True
    try:
        with (folder / 'resources.jsonl').open('rb') as stream:
            size = (folder / 'resources.jsonl').stat().st_size
            if after > size:
                raise HTTPException(416, '采样游标超出文件大小，请从0重新读取')
            stream.seek(after)
            # Bound both rows and bytes; leave a concurrently written partial line for the next poll.
            while len(items) < limit and stream.tell()-after < 1048576:
                line = stream.readline(16384)
                if not line or not line.endswith(b'\n'):
                    break
                offset = stream.tell()
                try:
                    items.append(json.loads(line))
                except ValueError:
                    continue
            eof = offset >= size
    except FileNotFoundError:
        offset = 0
    return {'items': items, 'next_offset': offset, 'eof': eof,
            'finished': job['status'] in TERMINAL, 'latest': read_json(folder / 'resource.json') or None}
