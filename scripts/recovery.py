"""Verify API-only restart while a real Ray Job and queued task remain managed."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone

import psutil
from platform_http import auth_headers


ROOT = Path(__file__).resolve().parents[1]
BASE = "http://127.0.0.1:8000/api"


def request(path, body=None):
    req = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", **auth_headers()})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def wait_until(check, seconds=100):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = check()
        if result:
            return result
        time.sleep(0.5)
    raise TimeoutError("Recovery check timed out")


def manifest():
    return {r["name"]: r for r in json.loads((ROOT / ".runtime/processes.json").read_text(encoding="utf-8-sig"))}


def compose(*args):
    result = subprocess.run(
        ["docker", "compose", "-f", str(ROOT / "compose.yaml"), *args],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=90,
    )
    if result.returncode:
        raise RuntimeError(f"docker compose {' '.join(args)}: {result.stderr}")
    return result.stdout.strip()


def container_state(service):
    container_id = compose("ps", "-q", service)
    assert container_id, f"Project {service} container is missing"
    result = subprocess.run(["docker", "inspect", container_id], capture_output=True,
                            text=True, encoding="utf-8", errors="replace", timeout=15, check=True)
    item = json.loads(result.stdout)[0]
    assert item["State"]["Running"], f"Project {service} container is not running"
    return {"id": item["Id"], "started_at": item["State"]["StartedAt"]}


def run_script(name):
    # Background children can retain inherited PIPE handles on Windows; use a file.
    log_path = ROOT / ".runtime" / ("recovery-" + name + ".log")
    with log_path.open("wb") as stream:
        completed = subprocess.run(
            ["powershell.exe", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
             str(ROOT / "scripts" / name), "-ApiOnly"],
            stdout=stream, stderr=subprocess.STDOUT, timeout=100,
            creationflags=subprocess.CREATE_NO_WINDOW,
        )
    output = log_path.read_text(encoding="utf-8", errors="replace")
    if output.strip():
        print(output.strip(), flush=True)
    if completed.returncode:
        raise RuntimeError(f"{name} exited {completed.returncode}: {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", action="store_true", help="Restart only the Compose API service")
    parser.add_argument('--output', type=Path, default=ROOT / 'docs/recovery-results.json')
    args = parser.parse_args()
    started_clock = time.monotonic()
    if args.docker:
        before = {name: container_state(name) for name in ("api", "ray")}
    else:
        before = manifest()
        assert "api" in before, "The API must have been started by scripts/start.ps1"
        actual = psutil.Process(before["api"]["pid"])
        recorded = datetime.fromisoformat(before["api"]["started_at"]).timestamp()
        assert abs(actual.create_time() - recorded) < 1, "The recorded API PID has been reused"
    cluster = request("/cluster")
    assert cluster["allocated_cpus"] == 0 and cluster["pending_jobs"] == 0, "Run on an idle platform"
    cpu = cluster["total_cpus"]
    created = []
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "passed": False,
              "runtime": "docker-linux" if args.docker else "windows-native"}
    try:
        job = request("/jobs", {"name": "acceptance-api-restart", "command": "python -m app.demo --sleep 40",
                               "cpu": cpu, "timeout_seconds": 120, "priority": 0})
        job_id = job["id"]
        created.append(job_id)
        job = wait_until(lambda: (j if (j := request(f"/jobs/{job_id}"))["status"] == "running" and j["heartbeat_at"] and j["started_at"] else None))
        job_folder = ROOT / "backend/data/jobs" / job_id / job["ray_submission_id"]
        identity_before = json.loads((job_folder / "process.json").read_text(encoding="utf-8"))
        queued = request("/jobs", {"name": "acceptance-restart-queue", "command": "python -m app.demo --sleep 2",
                                  "cpu": cpu, "timeout_seconds": 60, "priority": 0})["id"]
        created.append(queued)
        assert request(f"/jobs/{queued}")["status"] == "pending"
        print(f"Restarting only API; Ray job {job_id}, worker PID {identity_before['worker_pid']}", flush=True)
        if args.docker:
            print(compose("restart", "api"), flush=True)
        else:
            run_script("stop.ps1")
            time.sleep(3)
            run_script("start.ps1")

        def healthy():
            try:
                return request("/health")["status"] == "ok"
            except OSError:
                return False

        wait_until(healthy, seconds=60)
        if args.docker:
            after = {name: container_state(name) for name in ("api", "ray")}
            assert before["api"]["started_at"] != after["api"]["started_at"], "API container did not restart"
            assert before["ray"] == after["ray"], "Ray container changed during API-only restart"
        else:
            after = manifest()
            assert before["api"]["pid"] != after["api"]["pid"], "API did not restart"
            if "ray" in before:
                assert before["ray"] == after["ray"], "Ray process ownership changed during API-only restart"
        recovered = request(f"/jobs/{job_id}")
        assert recovered["status"] == "running", recovered
        assert recovered["ray_submission_id"] == job["ray_submission_id"]
        identity_after = json.loads((job_folder / "process.json").read_text(encoding="utf-8"))
        for key in ("worker_pid", "worker_create_time", "pid", "create_time", "started_at", "container_id"):
            assert identity_before.get(key) == identity_after.get(key), "Worker or training execution was started again"
        if args.docker:
            # Training containers use a separate PID namespace. Verify both execution identities.
            compose("exec", "-T", "api", "python", "-c",
                    "import psutil,sys; p=psutil.Process(int(sys.argv[1])); "
                    "assert p.is_running() and abs(p.create_time()-float(sys.argv[2]))<0.1; print(p.pid)",
                    str(identity_after["worker_pid"]), str(identity_after["worker_create_time"]))
            if identity_after.get('container_id'):
                compose('exec', '-T', 'api', 'python', '-c',
                        'import sys; from app.containers import inspect_task; c=inspect_task(sys.argv[1]); assert c["State"]["Running"] and c["Id"]==sys.argv[2]',
                        job['ray_submission_id'], identity_after['container_id'])
        else:
            assert abs(psutil.Process(identity_after["pid"]).create_time() - identity_after["create_time"]) < 0.1
        assert request("/cluster")["allocated_cpus"] == cpu
        assert request(f"/jobs/{queued}")["status"] == "pending"
        for current in created:
            terminal = wait_until(lambda: (j if (j := request(f"/jobs/{current}"))["status"] in {"succeeded", "failed", "cancelled"} else None))
            assert terminal["status"] == "succeeded" and terminal["exit_code"] == 0, terminal
        logs = request(f"/jobs/{job_id}/logs?offset=0&limit=65536")["content"]
        assert logs.count("[demo] started") == 1, "The workload ran multiple times"
        final = request("/cluster")
        assert final["allocated_cpus"] == 0 and final["available_cpus"] == cpu and final['allocated_memory_mb'] == 0
        report.update({"passed": True, "jobs": created, "ray_submission_id": job["ray_submission_id"],
                       "identity_before": identity_before, "identity_after": identity_after,
                       "execution_before": before, "execution_after": after,
                       "workload_start_count": logs.count("[demo] started"), "final_cluster": final})
        print("PASS API recovery: same Ray/worker/training identity, queued task resumed, CPU and memory fully reclaimed.", flush=True)
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started_clock, 3)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        for current in created:
            try:
                if request(f"/jobs/{current}")["status"] in {"pending", "running"}:
                    request(f"/jobs/{current}/cancel", {})
            except OSError:
                pass


if __name__ == "__main__":
    main()
