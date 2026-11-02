"""SSE framing, byte cursors, final-tail races and disconnected subscribers."""
import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from app import config, main, store
from app.scheduler import job_dir


class Request:
    disconnected = False

    async def is_disconnected(self):
        return self.disconnected


def event(frame):
    fields = dict(line.split(': ', 1) for line in frame.splitlines() if ': ' in line)
    if 'data' in fields:
        fields['data'] = json.loads(fields['data'])
    return fields


@pytest.fixture
def sample(tmp_path, monkeypatch):
    monkeypatch.setattr(config, 'DATA', tmp_path)
    monkeypatch.setattr(config, 'API_TOKEN', 'sse-unit-token')
    monkeypatch.setattr(main, 'LOG_STREAM_POLL_SECONDS', .002)
    store.initialize()
    job = main.create_job(main.JobCreate(name='SSE regression', cpu=1))
    folder = job_dir(store.get(job['id']))
    folder.mkdir(parents=True)
    client = TestClient(main.app, headers={'Authorization': 'Bearer sse-unit-token'})
    return job, folder, client


def test_sse_history_utf8_multiple_chunks_and_header_resume(sample):
    job, folder, client = sample
    text = '中文 🧪\n'*10000
    (folder/'train.log').write_bytes(text.encode('utf-8'))
    store.update(job['id'], status='succeeded')
    path = f"/api/jobs/{job['id']}/logs/stream"
    response = client.get(path)
    assert response.status_code == 200
    assert response.headers['content-type'].startswith('text/event-stream')
    assert response.headers['x-accel-buffering'] == 'no'
    frames = [event(frame) for frame in response.text.strip().split('\n\n')]
    logs = [frame for frame in frames if frame.get('event') == 'log']
    assert len(logs) > 1 and ''.join(frame['data']['content'] for frame in logs) == text
    assert all(int(frame['id']) == frame['data']['next_offset'] for frame in logs)
    assert frames[-1]['event'] == 'end' and frames[-1]['data']['content'] == ''
    assert frames[-1]['data']['next_offset'] == len(text.encode('utf-8'))
    cursor = logs[0]['id']
    resumed = client.get(path+'?offset=0', headers={'Last-Event-ID': cursor})
    rest = [event(frame) for frame in resumed.text.strip().split('\n\n')]
    assert ''.join(frame['data']['content'] for frame in rest if frame.get('event') == 'log') == text.encode('utf-8')[int(cursor):].decode('utf-8')


def test_sse_validates_before_streaming_headers(sample):
    job, folder, client = sample
    path = f"/api/jobs/{job['id']}/logs/stream"
    (folder/'train.log').write_text('hello', encoding='utf-8')
    assert TestClient(main.app).get(path).status_code == 401
    assert client.get('/api/jobs/missing/logs/stream').status_code == 404
    assert client.get(path+'?offset=6').status_code == 416
    assert client.get(path+'?offset=-1').status_code == 422
    for value in ['-1', '1.2', 'abc', ' 1', '1'*33]:
        assert client.get(path, headers={'Last-Event-ID': value}).status_code == 422


def test_missing_terminal_log_ends_but_positive_offset_is_not_rewound(sample):
    job, _, client = sample
    store.update(job['id'], status='cancelled')
    path = f"/api/jobs/{job['id']}/logs/stream"
    response = client.get(path)
    assert response.status_code == 200
    assert 'event: end' in response.text and 'event: log' not in response.text
    assert client.get(path+'?offset=1').status_code == 416
    assert client.get(f"/api/jobs/{job['id']}/logs?offset=1").status_code == 416


def test_pending_attempt_path_changes_and_partial_utf8_tail(sample):
    job, _, _ = sample

    async def check():
        response = main.log_stream(job['id'], Request(), offset=0, last_event_id=None)
        body = response.body_iterator
        assert await anext(body) == 'retry: 1000\n\n'
        store.update(job['id'], status='running', reserved=1, ray_submission_id=job['id']+'-1')
        folder = job_dir(store.get(job['id']))
        folder.mkdir(parents=True)
        path = folder/'train.log'
        path.write_bytes(b'A'+ '中'.encode('utf-8')[:2])
        first = event(await asyncio.wait_for(anext(body), 1))
        assert first['data']['content'] == 'A' and first['data']['next_offset'] == 1
        with path.open('ab') as stream:
            stream.write('中'.encode('utf-8')[2:]+b'\n')
        store.update(job['id'], status='succeeded', reserved=0)
        second = event(await asyncio.wait_for(anext(body), 1))
        assert second['data']['content'] == '中\n' and second['data']['next_offset'] == 5
        last = event(await anext(body))
        assert last['event'] == 'end' and last['data']['content'] == '' and last['id'] == '5'
        await body.aclose()

    asyncio.run(check())


def test_terminal_transition_after_old_eof_does_not_lose_final_tail(sample, monkeypatch):
    job, folder, _ = sample
    path = folder/'train.log'
    path.write_bytes(b'')
    original = main.logs
    armed = False

    def read_then_finish(*args, **kwargs):
        nonlocal armed
        chunk = original(*args, **kwargs)
        if armed:
            armed = False
            path.write_bytes('最后一行\n'.encode('utf-8'))
            store.update(job['id'], status='succeeded')
        return chunk

    monkeypatch.setattr(main, 'logs', read_then_finish)

    async def check():
        nonlocal armed
        body = main.log_stream(job['id'], Request(), offset=0, last_event_id=None).body_iterator
        await anext(body)
        armed = True
        frame = event(await asyncio.wait_for(anext(body), 1))
        assert frame['event'] == 'log' and frame['data']['content'] == '最后一行\n'
        assert event(await anext(body))['event'] == 'end'
        await body.aclose()

    asyncio.run(check())


@pytest.mark.parametrize('remove', [False, True])
def test_active_log_truncation_or_removal_emits_explicit_error(sample, remove):
    job, folder, _ = sample
    path = folder/'train.log'
    path.write_bytes(b'hello')

    async def check():
        body = main.log_stream(job['id'], Request(), offset=0, last_event_id=None).body_iterator
        await anext(body)
        assert event(await anext(body))['id'] == '5'
        path.unlink() if remove else path.write_bytes(b'x')
        error = event(await anext(body))
        assert error['event'] == 'error' and error['data']['code'] == 'log_truncated'
        with pytest.raises(StopAsyncIteration):
            await anext(body)

    asyncio.run(check())


def test_idle_heartbeat_disconnect_and_generator_cancellation(sample, monkeypatch):
    job, _, _ = sample
    monkeypatch.setattr(main, 'LOG_STREAM_HEARTBEAT_SECONDS', .005)
    calls = 0
    original = main.logs

    def tracked(*args, **kwargs):
        nonlocal calls
        calls += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(main, 'logs', tracked)

    async def check():
        request = Request()
        body = main.log_stream(job['id'], request, offset=0, last_event_id=None).body_iterator
        await anext(body)
        assert await asyncio.wait_for(anext(body), 1) == ': heartbeat\n\n'
        request.disconnected = True
        with pytest.raises(StopAsyncIteration):
            await anext(body)
        monkeypatch.setattr(main, 'LOG_STREAM_HEARTBEAT_SECONDS', 1000)
        body = main.log_stream(job['id'], Request(), offset=0, last_event_id=None).body_iterator
        await anext(body)
        pending = asyncio.create_task(anext(body))
        await asyncio.sleep(.02)
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
        await body.aclose()
        stopped_at = calls
        await asyncio.sleep(.02)
        assert calls == stopped_at

    asyncio.run(check())
