"""Regression cases for completion/cancel races, UTF-8 tails and uncertain submission recovery."""

import json
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app import config, main, scheduler as scheduler_module, store
from app.scheduler import Scheduler, job_dir


@pytest.fixture
def edge_case(tmp_path, monkeypatch):
    # Direct unit calls only: no live Ray/Redis clients, scheduler threads or real jobs.
    monkeypatch.setattr(config, 'DATA', tmp_path)
    store.initialize()
    job = main.create_job(main.JobCreate(name='edge case', cpu=1))
    store.update(job['id'], reserved=1, submitted=1, status='running',
                 ray_submission_id=job['id'] + '-1', started_at=time.time() - 2)
    job = store.get(job['id'])
    folder = job_dir(job)
    folder.mkdir(parents=True)
    coordinator = Scheduler()
    coordinator.redis = SimpleNamespace(get=lambda _key: None, set=Mock())
    coordinator.redis_ok = False
    coordinator.client = SimpleNamespace(
        get_job_info=lambda _key: SimpleNamespace(status='SUCCEEDED', driver_exit_code=0, message=''),
        stop_job=Mock(),
    )
    return coordinator, job, folder


def test_cancel_after_actual_success_preserves_completed_result(edge_case):
    coordinator, job, folder = edge_case
    # Training completed before the cancel request; only the API polling state is stale.
    (folder / 'result.json').write_text(json.dumps({
        'exit_code': 0, 'finished_at': time.time() - 1, 'error': None,
    }), encoding='utf-8')
    coordinator.request_cancel(job['id'])
    coordinator.reconcile(store.get(job['id']))
    completed = store.get(job['id'])
    assert completed['status'] == 'succeeded'
    assert completed['reserved'] == 0


def test_live_log_incomplete_utf8_tail_is_retried_without_corruption(edge_case):
    _, job, folder = edge_case
    payload = 'A中'.encode('utf-8')
    path = folder / 'train.log'
    path.write_bytes(payload[:-1])
    first = main.logs(job['id'], offset=0, limit=4)
    with path.open('ab') as stream:
        stream.write(payload[-1:])
    second = main.logs(job['id'], offset=first['next_offset'], limit=4)
    assert first['content'] + second['content'] == 'A中'
    assert second['next_offset'] == len(payload)


def test_uncertain_submission_with_surviving_worker_is_not_submitted_again(edge_case, monkeypatch):
    coordinator, job, folder = edge_case
    # Ray accepted a submission, API crashed before submitted=1, then control-plane
    # metadata disappeared. A surviving local worker must not get a duplicate peer.
    store.update(job['id'], submitted=0)
    (folder / 'process.json').write_text(json.dumps({
        'worker_pid': 12345, 'worker_create_time': time.time() - 2,
        'pid': 12346, 'create_time': time.time() - 2, 'children': [],
    }), encoding='utf-8')
    coordinator.client.get_job_info = Mock(side_effect=RuntimeError('Job does not exist'))
    coordinator.submit = Mock()
    monkeypatch.setattr(scheduler_module, 'local_alive', lambda _job: True)
    monkeypatch.setattr(scheduler_module, 'stop_local', Mock())
    coordinator.reconcile(store.get(job['id']))
    coordinator.submit.assert_not_called()
    assert store.get(job['id'])['reserved'] == 1
