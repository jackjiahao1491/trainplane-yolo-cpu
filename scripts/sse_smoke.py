"""Real SSE acceptance through Vite, with optional API-only restart recovery."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import time

import requests

from platform_http import auth_headers

ROOT = Path(__file__).resolve().parents[1]
TERMINAL = {'succeeded', 'failed', 'cancelled'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', default='http://127.0.0.1:5173/api')
    parser.add_argument('--restart-api', action='store_true')
    parser.add_argument('--restart-delay', type=float, default=0, help='Optional pause to attach a browser before API restart')
    parser.add_argument('--output', type=Path, default=ROOT/'docs/sse-results.json')
    args = parser.parse_args()
    created, checks = [], []
    report = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'passed': False,
              'base_url': args.base_url, 'checks': checks}

    def request(path, body=None):
        response = requests.request('GET' if body is None else 'POST', args.base_url+path,
                                    json=body, headers=auth_headers(), timeout=(5, 15))
        response.raise_for_status()
        return response.json()

    def submit(label, seconds):
        job = request('/jobs', {'name': 'acceptance-sse-'+label,
                               'command': f'python -m app.demo --sleep {seconds}',
                               'cpu': 2, 'memory_mb': 128, 'timeout_seconds': 120})
        created.append(job['id'])
        return job['id']

    def wait(check, timeout=90):
        deadline = time.monotonic()+timeout
        while time.monotonic() < deadline:
            value = check()
            if value:
                return value
            time.sleep(.5)
        raise TimeoutError('SSE acceptance condition did not become true')

    def started(job_id):
        return wait(lambda: (j if (j := request('/jobs/'+job_id))['started_at'] and j['heartbeat_at'] else None))

    def frames(job_id, offset=0, last_id=None):
        headers = {**auth_headers(), 'Accept': 'text/event-stream'}
        if last_id is not None:
            headers['Last-Event-ID'] = str(last_id)
        with requests.get(f'{args.base_url}/jobs/{job_id}/logs/stream?offset={offset}',
                          headers=headers, stream=True, timeout=(5, 20)) as response:
            response.raise_for_status()
            assert response.headers['Content-Type'].startswith('text/event-stream')
            assert response.headers['X-Accel-Buffering'] == 'no'
            response.encoding = 'utf-8'
            kind, data, cursor = '', [], None
            for line in response.iter_lines(chunk_size=1, decode_unicode=True):
                if not line:
                    if data:
                        yield kind or 'message', json.loads('\n'.join(data)), cursor
                    kind, data, cursor = '', [], None
                elif line.startswith(':'):
                    yield 'heartbeat', None, None
                elif line.startswith('retry:'):
                    yield 'retry', int(line.split(':', 1)[1]), None
                elif line.startswith('event:'):
                    kind = line.split(':', 1)[1].strip()
                elif line.startswith('id:'):
                    cursor = int(line.split(':', 1)[1])
                elif line.startswith('data:'):
                    data.append(line.split(':', 1)[1].lstrip(' '))

    def full_log(job_id):
        offset, text = 0, ''
        while True:
            chunk = request(f'/jobs/{job_id}/logs?offset={offset}&limit=65536')
            text += chunk['content']
            offset = chunk['next_offset']
            if chunk['eof'] and chunk['finished']:
                return text

    def passed(name, **evidence):
        checks.append({'check': name, 'passed': True, **evidence})
        print('PASS '+name, flush=True)

    try:
        cluster = request('/cluster')
        assert cluster['allocated_cpus'] == cluster['pending_jobs'] == 0, 'Run on an idle platform'
        assert requests.get(args.base_url+'/jobs/missing/logs/stream', timeout=10).status_code == 401
        assert requests.get(args.base_url+'/jobs/missing/logs/stream', headers=auth_headers(), timeout=10).status_code == 404
        passed('authentication_and_preflight_status_codes')

        blocker = submit('queue-blocker', 40)
        started(blocker)
        queued = submit('pending-to-running', 4)
        assert request('/jobs/'+queued)['status'] == 'pending'
        chunks, cursor, saw_heartbeat, saw_end = [], 0, False, False
        with_close = frames(queued)
        try:
            for kind, data, event_id in with_close:
                if kind == 'heartbeat':
                    assert request('/jobs/'+queued)['status'] == 'pending'
                    saw_heartbeat = True
                    request('/jobs/'+blocker+'/cancel', {})
                elif kind == 'log':
                    assert saw_heartbeat and event_id == data['next_offset'] > cursor
                    chunks.append(data['content'])
                    cursor = event_id
                elif kind == 'end':
                    assert data['finished'] and data['eof'] and not data['content'] and event_id == cursor
                    saw_end = True
                    break
                elif kind == 'error':
                    raise AssertionError(data)
        finally:
            with_close.close()
        assert saw_heartbeat and saw_end
        actual = ''.join(chunks)
        assert actual == full_log(queued), 'SSE and persistent log differ'
        result = request('/jobs/'+queued)
        assert result['status'] == 'succeeded' and result['exit_code'] == 0, result
        assert '中文日志' in actual and '[worker] finished exit_code=0' in actual
        assert cursor == len(actual.encode('utf-8'))
        assert request('/jobs/'+blocker)['status'] == 'cancelled'
        passed('idle_heartbeat_pending_path_and_final_tail', job_id=queued, bytes=cursor)

        live_id = submit('resume-after-disconnect', 25 if args.restart_api else 7)
        identity = started(live_id)
        live = frames(live_id)
        first_at = time.monotonic()
        while True:
            kind, data, event_id = next(live)
            if kind == 'log':
                first_text, cursor = data['content'], event_id
                break
        assert request('/jobs/'+live_id)['status'] == 'running', 'SSE was buffered until task completion'
        assert time.monotonic()-first_at < 5
        if args.restart_api:
            print('READY API restart SSE job '+live_id, flush=True)
            if args.restart_delay > 0:
                time.sleep(min(args.restart_delay, 30))
            ray_before = subprocess.check_output(['docker', 'compose', '-f', str(ROOT/'compose.yaml'), 'ps', '-q', 'ray'], text=True).strip()
            started_at = time.monotonic()
            subprocess.run(['docker', 'compose', '-f', str(ROOT/'compose.yaml'), 'restart', 'api'],
                           check=True, capture_output=True, timeout=45)
            restart_seconds = time.monotonic()-started_at

            def healthy():
                try:
                    return request('/health')['status'] == 'ok'
                except requests.RequestException:
                    return False

            wait(healthy, 60)
            assert subprocess.check_output(['docker', 'compose', '-f', str(ROOT/'compose.yaml'), 'ps', '-q', 'ray'], text=True).strip() == ray_before
            assert request('/jobs/'+live_id)['ray_submission_id'] == identity['ray_submission_id']
            report['api_restart_seconds'] = round(restart_seconds, 3)
        live.close()
        resumed, saw_end = [], False
        channel = frames(live_id, offset=0, last_id=cursor)
        try:
            for kind, data, event_id in channel:
                if kind == 'log':
                    assert event_id == data['next_offset'] > cursor
                    cursor = event_id
                    resumed.append(data['content'])
                elif kind == 'end':
                    assert event_id == cursor and not data['content']
                    saw_end = True
                    break
                elif kind == 'error':
                    raise AssertionError(data)
        finally:
            channel.close()
        assert saw_end
        actual = first_text+''.join(resumed)
        assert actual == full_log(live_id) and cursor == len(actual.encode('utf-8'))
        assert actual.count('[demo] started') == 1 and actual.endswith('[worker] finished exit_code=0, oom_killed=False\n')
        passed('header_resume_without_duplicate_or_missing_logs', job_id=live_id, bytes=cursor,
               api_restarted=args.restart_api)
        path = f'{args.base_url}/jobs/{live_id}/logs/stream'
        assert requests.get(path+'?offset='+str(cursor+1), headers=auth_headers(), timeout=10).status_code == 416
        assert requests.get(path, headers={**auth_headers(), 'Last-Event-ID': '-1'}, timeout=10).status_code == 422
        passed('resume_validation_before_http_200')
        final = request('/cluster')
        assert final['allocated_cpus'] == final['allocated_memory_mb'] == final['pending_jobs'] == 0
        report.update(passed=True, final_cluster=final)
    except Exception as exc:
        report['error'] = f'{type(exc).__name__}: {exc}'
        raise
    finally:
        for job_id in created:
            try:
                if request('/jobs/'+job_id)['status'] not in TERMINAL:
                    request('/jobs/'+job_id+'/cancel', {})
            except requests.RequestException:
                pass
        report['jobs'] = created
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
