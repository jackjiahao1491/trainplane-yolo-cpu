"""Authentication, migration and the byte cursor used by resource monitoring."""
import json
import os

import pytest
from fastapi.testclient import TestClient

from app import config, store
from app.auth import get_token
from app.main import app
from app.scheduler import job_dir


@pytest.fixture
def client(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'API_TOKEN', 'unit-test-operator-token')
    store.initialize()
    return TestClient(app, headers={'Authorization': 'Bearer ' + get_token()})


def test_operator_authentication_and_health(client):
    anonymous = TestClient(app)
    assert anonymous.get('/api/health').status_code == 200
    for path in ['/api/cluster', '/api/jobs', '/api/auth/session', '/api/jobs/unknown/resources']:
        assert anonymous.get(path).status_code == 401
    assert anonymous.post('/api/jobs', json={'name': 'unauthorized'}).status_code == 401
    assert anonymous.post('/api/jobs/unknown/cancel').status_code == 401
    assert client.get('/api/auth/session').json()['authenticated'] is True
    assert client.get('/api/jobs', headers={'Authorization': 'Bearer wrong'}).status_code == 401


def test_memory_validation_and_migration(client):
    assert client.post('/api/jobs', json={'name': 'small', 'memory_mb': 63}).status_code == 422
    assert client.post('/api/jobs', json={'name': 'large', 'memory_mb': config.TOTAL_MEMORY_MB+1}).status_code == 422
    job = client.post('/api/jobs', json={'name': 'migration', 'memory_mb': 128}).json()
    with store.connection() as db:
        for column in ['memory_mb', 'bypass_count', 'backfilled']:
            db.execute('ALTER TABLE jobs DROP COLUMN ' + column)
    store.initialize()
    migrated = store.get(job['id'])
    assert migrated['memory_mb'] == 1024 and migrated['bypass_count'] == 0
    assert migrated['name'] == 'migration'


def test_incremental_resources_preserve_partial_tail(client):
    job = client.post('/api/jobs', json={'name': 'resources'}).json()
    folder = job_dir(store.get(job['id']))
    folder.mkdir(parents=True)
    first = {'timestamp': 1, 'cpu_percent': 50, 'memory_mb': 80, 'memory_limit_mb': 1024, 'pids': 2}
    second = {**first, 'timestamp': 2}
    one = (json.dumps(first)+'\n').encode()
    two = (json.dumps(second)+'\n').encode()
    path = folder/'resources.jsonl'
    path.write_bytes(one+two[:10])
    (folder/'resource.json').write_text(json.dumps(first), encoding='utf-8')
    page = client.get(f"/api/jobs/{job['id']}/resources?limit=1").json()
    assert page['items'] == [first] and page['next_offset'] == len(one) and not page['eof']
    partial = client.get(f"/api/jobs/{job['id']}/resources?after={len(one)}").json()
    assert partial['items'] == [] and partial['next_offset'] == len(one)
    with path.open('ab') as stream:
        stream.write(two[10:])
    final = client.get(f"/api/jobs/{job['id']}/resources?after={len(one)}").json()
    assert final['items'] == [second] and final['eof'] and final['latest'] == first
    assert client.get(f"/api/jobs/{job['id']}/resources?after=99999").status_code == 416


@pytest.mark.skipif(os.name == 'nt', reason='Linux container filesystem isolation')
def test_untrusted_metric_symlink_and_fifo_are_rejected(client, tmp_path):
    job = client.post('/api/jobs', json={'name': 'untrusted-metrics'}).json()
    workspace = job_dir(store.get(job['id']))/'workspace'
    workspace.mkdir(parents=True)
    outside = tmp_path/'outside.jsonl'
    outside.write_text('{"epoch":1}\n', encoding='utf-8')
    path = workspace/'metrics.jsonl'
    path.symlink_to(outside)
    assert client.get(f"/api/jobs/{job['id']}/metrics").status_code == 400
    path.unlink()
    os.mkfifo(path)
    assert client.get(f"/api/jobs/{job['id']}/metrics").status_code == 400
