"""Deterministic failure/race checks; the HTTP smoke separately exercises real Ray."""
import json
import time
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app import config, store
from app.main import app
from app.scheduler import Scheduler, job_dir


class RayStub:
    def __init__(self):
        self.jobs = {}
        self.stopped = []

    def submit_job(self, **kwargs):
        key = kwargs['submission_id']
        assert key not in self.jobs, 'duplicate execution'
        assert kwargs['entrypoint_num_cpus'] > 0
        self.jobs[key] = SimpleNamespace(status='PENDING', message='', driver_exit_code=None)

    def get_job_info(self, key):
        if key not in self.jobs:
            raise RuntimeError('Job does not exist')
        return self.jobs[key]

    def stop_job(self, key):
        self.stopped.append(key)
        self.jobs[key].status = 'STOPPED'


class RedisStub:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, **kwargs):
        self.values[key] = value


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'TOTAL_CPUS', 2)
    store.initialize()
    scheduler = Scheduler()
    scheduler.client = RayStub()
    scheduler.redis = RedisStub()
    scheduler.redis_ok = True
    return scheduler, TestClient(app)


def create(client, name='test', **kwargs):
    response = client.post('/api/jobs', json={'name': name, **kwargs})
    assert response.status_code == 201, response.text
    return response.json()


def test_queue_priority_recovery_and_exactly_once_submission(setup):
    scheduler, client = setup
    low = create(client, 'low', priority=0)
    high = create(client, 'high', priority=10)
    scheduler.dispatch()
    assert store.get(high['id'])['reserved'] == 1
    assert store.get(low['id'])['reserved'] == 0
    high = store.get(high['id'])
    # Simulate crash AFTER Ray accepted, BEFORE submitted=1 was saved.
    store.update(high['id'], submitted=0)
    scheduler.reconcile(store.get(high['id']))
    assert len(scheduler.client.jobs) == 1
    scheduler.client.jobs[high['ray_submission_id']].status = 'SUCCEEDED'
    (job_dir(high) / 'result.json').write_text(json.dumps({'exit_code': 0, 'error': None}))
    scheduler.reconcile(store.get(high['id']))
    scheduler.dispatch()
    assert store.get(high['id'])['status'] == 'succeeded'
    assert store.get(low['id'])['reserved'] == 1


def test_cancel_pending_and_running_keeps_reservation_until_confirmed(setup):
    scheduler, client = setup
    first = create(client)
    queued = create(client)
    scheduler.dispatch()
    scheduler.request_cancel(queued['id'])
    assert store.get(queued['id'])['status'] == 'cancelled'
    scheduler.request_cancel(first['id'])
    job = store.get(first['id'])
    assert job['reserved'] == 1
    scheduler.reconcile(job)
    assert store.get(first['id'])['reserved'] == 1
    scheduler.reconcile(store.get(first['id']))
    assert store.get(first['id'])['status'] == 'cancelled'
    assert store.get(first['id'])['reserved'] == 0


@pytest.mark.parametrize('failure', ['runtime_timeout', 'heartbeat_timeout'])
def test_watchdogs_stop_before_release(setup, failure):
    scheduler, client = setup
    created = create(client, timeout_seconds=100)
    scheduler.dispatch()
    job = store.get(created['id'])
    scheduler.client.jobs[job['ray_submission_id']].status = 'RUNNING'
    store.update(job['id'], status='running', started_at=time.time()-(101 if failure == 'runtime_timeout' else 1),
                 heartbeat_at=time.time()-20)
    scheduler.reconcile(store.get(job['id']))
    assert store.get(job['id'])['stop_reason'] == failure
    assert store.get(job['id'])['reserved'] == 1
    scheduler.reconcile(store.get(job['id']))
    assert store.get(job['id'])['status'] == 'failed'
    assert store.get(job['id'])['reserved'] == 0


def test_redis_outage_is_not_worker_death(setup):
    scheduler, client = setup
    created = create(client)
    scheduler.dispatch()
    job = store.get(created['id'])
    scheduler.client.jobs[job['ray_submission_id']].status = 'RUNNING'
    store.update(job['id'], status='running', started_at=time.time(), heartbeat_at=time.time()-100)
    scheduler.redis_ok = False
    scheduler.reconcile(store.get(job['id']))
    assert not scheduler.client.stopped
    assert store.get(job['id'])['reserved'] == 1


def test_log_byte_offsets_utf8_filter_pagination_and_validation(setup):
    scheduler, client = setup
    job = create(client)
    folder = job_dir(job)
    folder.mkdir(parents=True)
    text = '第一行 🧪\nsecond line\n'
    (folder / 'train.log').write_bytes(text.encode('utf-8'))
    offset, actual = 0, ''
    for _ in range(50):
        response = client.get(f"/api/jobs/{job['id']}/logs", params={'offset': offset, 'limit': 5})
        assert response.status_code == 200
        data = response.json()
        actual += data['content']
        offset = data['next_offset']
        if data['eof']:
            break
    assert actual == text
    assert offset == len(text.encode('utf-8'))
    assert client.get(f"/api/jobs/{job['id']}/logs?offset=9999").status_code == 416
    result = client.get('/api/jobs', params={'search': job['id'][:8], 'status': 'pending', 'page_size': 1}).json()
    assert result['total'] == 1 and result['items'][0]['id'] == job['id']
    assert client.get('/api/jobs?page=0').status_code == 422
    assert client.post('/api/jobs', json={'name': 'bad', 'cpu': 3}).status_code == 422
    assert client.get('/api/jobs/does-not-exist').status_code == 404
