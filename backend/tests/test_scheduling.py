"""Resource accounting, bounded backfill, aging, and isolated-container recovery."""

import json
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from app import config, scheduler as scheduling, store
from app.scheduler import Scheduler, effective_priority, job_dir, local_alive, local_identities, stop_local


class RayStub:
    def __init__(self):
        self.jobs = {}
        self.submissions = []
        self.stopped = []

    def submit_job(self, **kwargs):
        attempt = kwargs['submission_id']
        assert attempt not in self.jobs, 'same attempt submitted twice'
        self.jobs[attempt] = SimpleNamespace(status='PENDING', driver_exit_code=None, message='')
        self.submissions.append(kwargs)

    def get_job_info(self, attempt):
        if attempt not in self.jobs:
            raise RuntimeError('Job does not exist')
        return self.jobs[attempt]

    def stop_job(self, attempt):
        self.stopped.append(attempt)
        self.jobs[attempt].status = 'STOPPED'


@pytest.fixture
def context(tmp_path, monkeypatch):
    for name, value in {
        'DATA': tmp_path, 'TOTAL_CPUS': 4, 'TOTAL_MEMORY_MB': 4096,
        'AGING_INTERVAL_SECONDS': 60, 'BACKFILL_MAX_BYPASS': 3, 'EXECUTION_MODE': 'process',
    }.items():
        monkeypatch.setattr(config, name, value)
    store.initialize()
    clock = {'now': 1000.0}
    monkeypatch.setattr(scheduling, 'time', SimpleNamespace(time=lambda: clock['now']))
    coordinator = Scheduler()
    coordinator.client = RayStub()
    coordinator.redis = SimpleNamespace(get=lambda _key: None, set=Mock())
    coordinator.redis_ok = True
    return coordinator, clock


def create(name, cpu=1, memory=512, priority=0, created_at=1000):
    with store.connection() as db:
        db.execute('''INSERT INTO jobs
            (id,name,command,cpu,memory_mb,timeout_seconds,priority,created_at)
            VALUES(?,?,?,?,?,?,?,?)''',
            (name, name, 'python -m app.demo --sleep 1', cpu, memory, 600, priority, created_at))
    return store.get(name)


def reserve(name, cpu=1, memory=512):
    create(name, cpu, memory)
    store.update(name, reserved=1, submitted=1, status='running', ray_submission_id=name + '-1')


def usage():
    with store.connection() as db:
        row = db.execute('SELECT COALESCE(SUM(cpu),0),COALESCE(SUM(memory_mb),0) FROM jobs WHERE reserved=1').fetchone()
        return tuple(row)


def fake_containers(monkeypatch, running=True):
    monkeypatch.setattr(config, 'EXECUTION_MODE', 'docker')
    module = ModuleType('app.containers')
    module.info = {'State': {'Running': running}}
    module.inspect_task = Mock(side_effect=lambda _attempt: module.info)
    module.stop_task = Mock()
    monkeypatch.setitem(sys.modules, 'app.containers', module)
    return module


def test_aging_is_unbounded_clamps_future_times_and_keeps_fifo(context, monkeypatch):
    coordinator, _ = context
    monkeypatch.setattr(config, 'TOTAL_CPUS', 1)
    old = create('old', priority=0, created_at=340)
    create('new', priority=10, created_at=1000)
    assert effective_priority(old, 1000) == 11  # Aging must not cap at original priority limit 10.
    assert effective_priority({'priority': 2, 'created_at': 1060}, 1000) == 2
    coordinator.dispatch()
    assert store.get('old')['reserved'] == 1
    coordinator.finish(store.get('old'), 'succeeded', 0)
    # Both now have effective priority 10; earlier enqueue time wins the tie.
    create('tie-old', priority=0, created_at=400)
    coordinator.dispatch()
    assert store.get('tie-old')['reserved'] == 1
    assert store.get('new')['reserved'] == 0


@pytest.mark.parametrize('active_cpu,active_memory,head_cpu,head_memory,small_cpu,small_memory', [
    (3, 512, 2, 512, 1, 512),  # CPU is insufficient for the head.
    (1, 3584, 1, 1024, 1, 512),  # Memory is insufficient for the head.
])
def test_cpu_and_memory_admission_allows_only_fitting_backfill(
        context, active_cpu, active_memory, head_cpu, head_memory, small_cpu, small_memory):
    coordinator, _ = context
    reserve('active', active_cpu, active_memory)
    create('head', head_cpu, head_memory, priority=10)
    create('small', small_cpu, small_memory)
    coordinator.dispatch()
    assert store.get('head')['reserved'] == 0
    assert store.get('head')['bypass_count'] == 1
    assert store.get('small')['reserved'] == 1
    assert store.get('small')['backfilled'] == 1
    used_cpu, used_memory = usage()
    assert used_cpu <= config.TOTAL_CPUS and used_memory <= config.TOTAL_MEMORY_MB


def test_memory_cannot_be_overcommitted_even_when_cpu_fits(context):
    coordinator, _ = context
    create('first', cpu=2, memory=2000, priority=3)
    create('second', memory=1500, priority=2)
    create('too-large-now', memory=1000, priority=1)
    create('fits-tail', memory=512)
    coordinator.dispatch()
    assert usage() == (4, 4012)
    assert store.get('too-large-now')['reserved'] == 0
    assert store.get('fits-tail')['reserved'] == 1
    assert store.get('fits-tail')['backfilled'] == 1


def test_bypass_budget_counts_reservations_not_ticks_and_survives_restart(context, monkeypatch):
    coordinator, _ = context
    monkeypatch.setattr(config, 'TOTAL_CPUS', 2)
    reserve('active', cpu=1)
    create('head', cpu=2, priority=10)
    for number in range(4):
        create(f'small-{number}', memory=256)
    for number in range(3):
        coordinator.dispatch()
        selected = store.get(f'small-{number}')
        assert selected['reserved'] == 1 and selected['backfilled'] == 1
        assert store.get('head')['bypass_count'] == number + 1
        coordinator.dispatch()  # No newly selected task: do not charge another bypass.
        assert store.get('head')['bypass_count'] == number + 1
        coordinator.finish(selected, 'succeeded', 0)
        replacement = Scheduler()  # All fairness state must come from SQLite, not Python memory.
        replacement.client, replacement.redis = coordinator.client, coordinator.redis
        coordinator = replacement
    coordinator.dispatch()
    assert store.get('small-3')['reserved'] == 0
    assert store.get('head')['bypass_count'] == 3
    assert usage()[0] == 1  # Keep capacity available for the protected head.
    coordinator.finish(store.get('active'), 'succeeded', 0)
    coordinator.dispatch()
    assert store.get('head')['reserved'] == 1
    assert store.get('small-3')['reserved'] == 0


def test_one_backfill_charges_all_preceding_blocked_jobs(context, monkeypatch):
    coordinator, _ = context
    monkeypatch.setattr(config, 'BACKFILL_MAX_BYPASS', 2)
    reserve('active', cpu=1)
    create('head-a', cpu=4, priority=10)
    create('head-b', cpu=4, priority=9)
    for number in range(3):
        create(f'small-{number}', memory=256)
    coordinator.dispatch()
    assert store.get('head-a')['bypass_count'] == 2
    assert store.get('head-b')['bypass_count'] == 2
    assert store.get('small-0')['reserved'] == store.get('small-1')['reserved'] == 1
    assert store.get('small-2')['reserved'] == 0
    assert usage()[0] == 3  # A third backfill would fit, but the persisted budget forbids it.


def test_impossible_memory_request_is_never_reserved(context):
    coordinator, _ = context
    create('oversized', memory=config.TOTAL_MEMORY_MB + 1, priority=10)
    coordinator.dispatch()
    coordinator.dispatch()
    assert store.get('oversized')['reserved'] == 0
    assert store.get('oversized')['bypass_count'] == 0
    assert not coordinator.client.submissions


def test_submission_carries_memory_and_docker_runtime_environment(context, monkeypatch):
    coordinator, _ = context
    monkeypatch.setattr(config, 'EXECUTION_MODE', 'docker')
    monkeypatch.setenv('TRAIN_IMAGE', 'unit-image:local')
    monkeypatch.setenv('DOCKER_HOST', 'unix:///unit-docker.sock')
    create('submitted', memory=768)
    coordinator.dispatch()
    job = store.get('submitted')
    saved = json.loads((job_dir(job) / 'config.json').read_text(encoding='utf-8'))
    assert saved['memory_mb'] == 768
    env = coordinator.client.submissions[0]['runtime_env']['env_vars']
    assert env['EXECUTION_MODE'] == 'docker'
    assert env['TRAIN_IMAGE'] == 'unit-image:local'
    assert env['DOCKER_HOST'] == 'unix:///unit-docker.sock'


@pytest.mark.parametrize('reason,expected_status', [('cancelled', 'cancelled'), ('heartbeat_timeout', 'failed')])
def test_stop_confirmation_releases_cpu_and_memory_after_ray_and_container_exit(
        context, monkeypatch, reason, expected_status):
    coordinator, clock = context
    containers = fake_containers(monkeypatch)
    create('stopped', cpu=2, memory=1536)
    coordinator.dispatch()
    job = store.get('stopped')
    attempt = job['ray_submission_id']
    coordinator.client.jobs[attempt].status = 'RUNNING'
    store.update(job['id'], started_at=999, heartbeat_at=900)
    if reason == 'cancelled':
        coordinator.request_cancel(job['id'])
    coordinator.reconcile(store.get(job['id']))
    assert store.get(job['id'])['stop_reason'] == reason
    assert usage() == (2, 1536)
    assert not containers.stop_task.called  # Bounded grace lets the worker flush its result.
    clock['now'] += 5
    containers.stop_task.side_effect = lambda _attempt: containers.info['State'].update(Running=False)
    coordinator.reconcile(store.get(job['id']))
    containers.stop_task.assert_called_once_with(attempt)
    assert coordinator.client.stopped == [attempt]
    assert usage() == (2, 1536)  # Stop request alone is not termination confirmation.
    coordinator.reconcile(store.get(job['id']))
    assert store.get(job['id'])['status'] == expected_status
    assert usage() == (0, 0)


@pytest.mark.parametrize('failure', ['inspect', 'stop'])
def test_docker_inspection_or_stop_errors_preserve_reservations(context, monkeypatch, failure):
    coordinator, _ = context
    containers = fake_containers(monkeypatch)
    create('uncertain', memory=1024)
    coordinator.dispatch()
    job = store.get('uncertain')
    coordinator.client.jobs[job['ray_submission_id']].status = 'SUCCEEDED'
    if failure == 'inspect':
        containers.inspect_task.side_effect = RuntimeError('Docker inspection unavailable')
    else:
        containers.stop_task.side_effect = RuntimeError('Docker stop unavailable')
    with pytest.raises(RuntimeError, match='Docker'):
        coordinator.reconcile(job)
    assert usage() == (1, 1024)
    assert store.get(job['id'])['status'] == 'pending'


def test_ray_terminal_is_not_enough_while_container_is_still_running(context, monkeypatch):
    coordinator, _ = context
    containers = fake_containers(monkeypatch)
    create('orphan', memory=1024)
    coordinator.dispatch()
    job = store.get('orphan')
    coordinator.client.jobs[job['ray_submission_id']].status = 'SUCCEEDED'
    coordinator.client.jobs[job['ray_submission_id']].driver_exit_code = 0
    coordinator.reconcile(job)
    assert usage() == (1, 1024)
    containers.stop_task.assert_called_once_with(job['ray_submission_id'])
    containers.info = {'State': {'Running': False}}
    coordinator.reconcile(store.get(job['id']))
    assert store.get(job['id'])['status'] == 'succeeded'
    assert usage() == (0, 0)


def test_docker_mode_never_interprets_training_namespace_pids_as_local(context, monkeypatch):
    coordinator, _ = context
    containers = fake_containers(monkeypatch, running=False)
    create('namespace')
    coordinator.dispatch()
    job = store.get('namespace')
    (job_dir(job) / 'process.json').write_text(json.dumps({
        'worker_pid': 100, 'worker_create_time': 200,
        'pid': 1, 'create_time': 300, 'children': [{'pid': 2, 'create_time': 400}],
    }), encoding='utf-8')
    assert local_identities(job) == [{'pid': 100, 'create_time': 200}]
    alive = Mock(return_value=False)
    monkeypatch.setattr(scheduling, 'process_alive', alive)
    assert not local_alive(job)
    stop_local(job)
    assert all(call.args == (100, 200) for call in alive.call_args_list)
    containers.stop_task.assert_called_once_with(job['ray_submission_id'])
