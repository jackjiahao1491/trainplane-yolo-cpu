import json
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import psutil
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import config, store
from .scheduler import Scheduler, TERMINAL, job_dir

scheduler = Scheduler()


@asynccontextmanager
async def lifespan(app):
    store.initialize()
    scheduler.start()
    yield
    scheduler.close()


app = FastAPI(title='TrainPlane · CPU Training Platform', version='1.0.0', lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=['http://127.0.0.1:5173', 'http://localhost:5173'],
                   allow_methods=['GET', 'POST'], allow_headers=['Content-Type'])


def public(job):
    out = {k: v for k, v in job.items() if k not in {'reserved', 'submitted', 'running_seen_at'}}
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


@app.get('/api/health')
def health():
    return {'status': 'ok' if scheduler.ray_ok and scheduler.redis_ok else 'degraded',
            'ray_ok': scheduler.ray_ok, 'redis_ok': scheduler.redis_ok, 'error': scheduler.last_error}


@app.get('/api/cluster')
def cluster():
    with store.connection() as db:
        used = db.execute('SELECT COALESCE(SUM(cpu),0) FROM jobs WHERE reserved=1').fetchone()[0]
        counts = dict(db.execute('SELECT status,COUNT(*) FROM jobs GROUP BY status').fetchall())
    return {'total_cpus': config.TOTAL_CPUS, 'allocated_cpus': used,
            'available_cpus': max(0, config.TOTAL_CPUS-used),
            'pending_jobs': counts.get('pending', 0), 'running_jobs': counts.get('running', 0),
            'redis_ok': scheduler.redis_ok, 'ray_ok': scheduler.ray_ok,
            'nodes': [{'id': 'local', 'name': '本机 CPU 节点', 'total_cpus': config.TOTAL_CPUS,
                       'allocated_cpus': used, 'cpu_percent': psutil.cpu_percent(),
                       'memory_percent': psutil.virtual_memory().percent}]}


@app.post('/api/jobs', status_code=201)
def create_job(body: JobCreate):
    if body.cpu > config.TOTAL_CPUS:
        raise HTTPException(422, f'CPU 申请不能超过平台总配额 {config.TOTAL_CPUS}')
    if not body.name.strip() or not body.command.strip():
        raise HTTPException(422, '任务名称和命令不能为空')
    job_id = uuid.uuid4().hex[:16]
    with store.connection() as db:
        db.execute('INSERT INTO jobs(id,name,command,cpu,timeout_seconds,priority,created_at) VALUES(?,?,?,?,?,?,?)',
                   (job_id, body.name.strip(), body.command.strip(), body.cpu, body.timeout_seconds, body.priority, time.time()))
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
            if exc.reason == 'unexpected end of data' and exc.end == len(chunk) and offset+len(chunk) < size:
                chunk = chunk[:exc.start]
            content = chunk.decode('utf-8', errors='replace')
        next_offset = offset + len(chunk)
        return {'content': content, 'next_offset': next_offset, 'eof': next_offset >= size,
                'finished': job['status'] in TERMINAL}
    except FileNotFoundError:
        return {'content': '', 'next_offset': 0, 'eof': True, 'finished': job['status'] in TERMINAL}


@app.get('/api/jobs/{job_id}/metrics')
def metrics(job_id: str):
    job = require(job_id)
    items = []
    try:
        with (job_dir(job) / 'metrics.jsonl').open(encoding='utf-8') as stream:
            for line in stream:
                try:
                    entry = json.loads(line)
                    values = entry.get('metrics', entry)
                    def value(*keys):
                        return next((values[k] for k in keys if k in values), None)
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
    return {'items': items}
