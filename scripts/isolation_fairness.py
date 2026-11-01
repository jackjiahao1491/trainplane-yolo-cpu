"""Real HTTP acceptance: auth, per-task limits/OOM, monitoring, backfill and aging."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shlex
import subprocess
import time
import urllib.error
import urllib.request

from platform_http import auth_headers

BASE = 'http://127.0.0.1:8000/api'
TERMINAL = {'succeeded', 'failed', 'cancelled'}


def request(path, body=None, authenticated=True):
    headers = {'Content-Type': 'application/json'}
    if authenticated:
        headers.update(auth_headers())
    req = urllib.request.Request(BASE+path, data=None if body is None else json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(req, timeout=20) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--aging', action='store_true', help='Also wait one real aging interval (default 60 seconds)')
    parser.add_argument('--output', type=Path, default=Path('docs/isolation-fairness-results.json'))
    args = parser.parse_args()
    created, checks = [], []
    report = {'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'passed': False, 'checks': checks}

    def submit(label, command=None, cpu=1, memory_mb=128, seconds=5, priority=0):
        job = request('/jobs', {'name': 'acceptance-isolation-'+label,
                               'command': command or f'python -m app.demo --sleep {seconds}',
                               'cpu': cpu, 'memory_mb': memory_mb, 'priority': priority, 'timeout_seconds': 180})
        created.append(job['id'])
        return job['id']

    def wait(job_id, predicate, timeout=120):
        end = time.monotonic()+timeout
        while time.monotonic() < end:
            job = request('/jobs/'+job_id)
            if predicate(job):
                return job
            if job['status'] in TERMINAL:
                raise AssertionError(f'Unexpected terminal result: {job}')
            time.sleep(.5)
        raise TimeoutError(f'Task {job_id} did not meet the condition')

    def completed(job_id, status='succeeded'):
        return wait(job_id, lambda job: job['status'] == status)

    def started(job_id):
        return wait(job_id, lambda job: bool(job['started_at'] and job['heartbeat_at']))

    def passed(label, **evidence):
        checks.append({'check': label, 'passed': True, **evidence})
        print('PASS '+label, flush=True)

    try:
        cluster = request('/cluster')
        assert cluster['allocated_cpus'] == cluster['allocated_memory_mb'] == cluster['pending_jobs'] == 0, 'Use an idle platform'
        assert cluster['execution_mode'] == 'docker' and cluster['total_cpus'] == 2
        assert request('/health', authenticated=False)['status'] == 'ok'
        try:
            request('/cluster', authenticated=False)
        except urllib.error.HTTPError as exc:
            assert exc.code == 401
        else:
            raise AssertionError('Unauthenticated request was allowed')
        assert request('/auth/session')['authenticated']
        passed('bearer_authentication')

        code = (
            "import os,pathlib,json,time; "
            "assert os.getuid()==1000; "
            "assert not pathlib.Path('/var/run/docker.sock').exists(); "
            "assert not pathlib.Path('/app/data/platform.sqlite3').exists(); "
            "assert not any(k in os.environ for k in ['TRAINPLANE_API_TOKEN','REDIS_URL','DOCKER_HOST']); "
            "pathlib.Path('/task/probe.txt').write_text('isolated'); "
            "print('[probe] '+json.dumps({'uid':os.getuid(),'net':os.listdir('/sys/class/net')}),flush=True); "
            "memory=bytearray(16*1024*1024); end=time.monotonic()+7; "
            "exec('while time.monotonic()<end: pass')"
        )
        probe_id = submit('probe', shlex.join(['python', '-c', code]))
        probe = completed(probe_id)
        inspected = json.loads(subprocess.run(['docker', 'inspect', 'trainplane-task-'+probe['ray_submission_id']],
                                              capture_output=True, text=True, encoding='utf-8', check=True).stdout)[0]
        host = inspected['HostConfig']
        assert host['Memory'] == host['MemorySwap'] == 128*1024**2 and host['NanoCpus'] == 1_000_000_000
        assert host['PidsLimit'] == 128 and host['ReadonlyRootfs'] and host['NetworkMode'] == 'none'
        assert inspected['Config']['User'] == '1000:1000' and host['CapDrop'] == ['ALL']
        offset, samples = 0, []
        while True:
            page = request(f'/jobs/{probe_id}/resources?after={offset}&limit=1')
            samples.extend(page['items'])
            assert page['next_offset'] >= offset
            offset = page['next_offset']
            if page['finished'] and page['eof']:
                break
        assert len(samples) >= 2 and any(sample['memory_mb'] > 0 for sample in samples)
        assert any(sample['cpu_percent'] > 0 for sample in samples)
        log = request(f'/jobs/{probe_id}/logs')['content']
        assert '"uid": 1000' in log and '"net": ["lo"]' in log
        passed('isolated_execution_and_resource_cursor', job_id=probe_id, samples=samples,
               limits={key: host[key] for key in ['NanoCpus', 'Memory', 'MemorySwap', 'PidsLimit', 'NetworkMode', 'ReadonlyRootfs']})

        oom_id = submit('oom', "python -c 'x=bytearray(256*1024*1024)'", memory_mb=64)
        oom = completed(oom_id, 'failed')
        assert oom['exit_code'] == 137 and 'memory_limit' in oom['error'], oom
        assert request('/cluster')['allocated_memory_mb'] == 0
        passed('kernel_memory_limit_and_reclamation', job_id=oom_id, exit_code=oom['exit_code'], error=oom['error'])

        blocker = submit('backfill-blocker', seconds=30)
        started(blocker)
        head = submit('backfill-large-head', cpu=2, seconds=1, priority=10)
        small = submit('backfill-small', seconds=5)
        small_job = started(small)
        assert small_job['backfilled'] and request('/jobs/'+head)['status'] == 'pending'
        head_job = request('/jobs/'+head)
        assert 1 <= head_job['bypass_count'] <= cluster['backfill_max_bypass']
        request('/jobs/'+blocker+'/cancel', {})
        completed(blocker, 'cancelled')
        completed(small)
        completed(head)
        passed('live_backfill_and_cancel_reclamation', blocker=blocker, head=head, small=small,
               bypass_count=head_job['bypass_count'])

        if args.aging:
            interval = cluster['aging_interval_seconds']
            assert interval <= 90, 'Use an aging interval <=90 seconds for the live acceptance'
            blocker = submit('aging-blocker', cpu=2, seconds=interval+15)
            started(blocker)
            older = submit('aging-older', cpu=2, seconds=2, priority=0)
            time.sleep(interval+1)
            newer = submit('aging-newer', cpu=2, seconds=1, priority=1)
            aged = request('/jobs/'+older)
            assert aged['effective_priority'] >= 1
            old_result, new_result = completed(older), completed(newer)
            completed(blocker)
            assert old_result['started_at'] <= new_result['started_at']
            passed('live_priority_aging', older=older, newer=newer,
                   observed_effective_priority=aged['effective_priority'], interval_seconds=interval)
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
            except OSError:
                pass
        report['jobs'] = created
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
