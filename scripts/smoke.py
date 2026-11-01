"""HTTP acceptance checks against a real running platform; creates labeled test jobs."""
import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000/api")
    parser.add_argument("--output", type=Path, default=Path("docs/smoke-results.json"))
    args = parser.parse_args()
    created = []
    results = []
    prefix = f"acceptance-{int(time.time())}"

    def request(path, data=None):
        req = urllib.request.Request(
            args.base_url + path,
            data=None if data is None else json.dumps(data).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.load(response)

    def submit(label, seconds=3, exit_code=0, timeout=60):
        job = request("/jobs", {
            "name": f"{prefix}-{label}",
            "command": f"python -m app.demo --sleep {seconds} --exit-code {exit_code}",
            "cpu": cpu,
            "timeout_seconds": timeout,
            "priority": 0,
        })
        created.append(job["id"])
        return job["id"]

    def wait(job_id, statuses, seconds=90):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            job = request(f"/jobs/{job_id}")
            if job["status"] in statuses:
                return job
            if job["status"] in {"succeeded", "failed", "cancelled"}:
                raise AssertionError(f"Unexpected terminal result: {job}")
            time.sleep(0.5)
        raise TimeoutError(f"Job {job_id} did not reach {statuses}")

    def passed(name, detail):
        results.append({"check": name, "passed": True, "detail": detail})
        print(f"PASS {name}: {detail}", flush=True)

    try:
        health = request("/health")
        cluster = request("/cluster")
        assert cluster["ray_ok"] and cluster["redis_ok"], cluster
        assert cluster["allocated_cpus"] == 0 and cluster["pending_jobs"] == 0, "Run on an idle demo platform."
        cpu = cluster["total_cpus"]
        passed("health_and_resources", {"health": health, "total_cpus": cpu})

        running = submit("running-cancel", seconds=40)
        wait(running, {"running"})
        # Ray RUNNING can precede the actual training child; cancel after heartbeat.
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            started = request(f"/jobs/{running}")
            if started["started_at"] and started["heartbeat_at"]:
                break
            time.sleep(0.5)
        else:
            raise TimeoutError("Training child did not start before running-cancel check")
        queued = submit("queued-cancel", seconds=2)
        time.sleep(1)
        assert request(f"/jobs/{queued}")["status"] == "pending"
        current = request("/cluster")
        assert current["allocated_cpus"] == cpu and current["available_cpus"] == 0, current
        request(f"/jobs/{queued}/cancel", {})
        wait(queued, {"cancelled"})
        passed("queue_and_pending_cancel", {"running": running, "queued": queued})

        successor = submit("auto-dispatch", seconds=3)
        request(f"/jobs/{running}/cancel", {})
        wait(running, {"cancelled"})
        completed = wait(successor, {"succeeded"})
        assert completed["exit_code"] == 0, completed
        passed("running_cancel_and_auto_dispatch", {"cancelled": running, "succeeded": successor})

        offset = 0
        chunks = []
        for _ in range(1000):
            page = request(f"/jobs/{successor}/logs?offset={offset}&limit=17")
            assert page["next_offset"] >= offset, page
            chunks.append(page["content"])
            if page["eof"]:
                break
            assert page["next_offset"] > offset, "Log cursor made no progress."
            offset = page["next_offset"]
        else:
            raise AssertionError("Log pagination did not terminate")
        all_logs = request(f"/jobs/{successor}/logs?offset=0&limit=1048576")
        assert "".join(chunks) == all_logs["content"], "Incremental log chunks differ from full short test log"
        assert all_logs["content"], "Demo produced no logs"
        passed("incremental_logs", {"job": successor, "characters": len(all_logs["content"])})

        failed = submit("exit-failure", seconds=1, exit_code=7)
        result = wait(failed, {"failed"})
        assert result["exit_code"] not in {None, 0}, result
        passed("nonzero_exit", {"job": failed, "exit_code": result["exit_code"]})

        timeout = submit("timeout", seconds=40, timeout=3)
        result = wait(timeout, {"failed"})
        assert result.get("stop_reason") == "runtime_timeout" or any(word in str(result.get("error", "")).lower() for word in ["timeout", "超时", "最大运行时长"]), result
        passed("runtime_timeout", {"job": timeout, "error": result["error"]})

        heartbeat = request("/jobs", {
            "name": f"{prefix}-heartbeat",
            "command": "python -m app.demo --sleep 120 --freeze-heartbeat 3",
            "cpu": cpu, "timeout_seconds": 180, "priority": 0,
        })["id"]
        created.append(heartbeat)
        result = wait(heartbeat, {"failed"}, seconds=150)
        assert result.get("stop_reason") == "heartbeat_timeout", result
        passed("heartbeat_timeout", {"job": heartbeat, "error": result["error"]})

        page = request("/jobs?" + urllib.parse.urlencode({"page": 1, "page_size": 2, "status": "cancelled", "search": running}))
        assert page["total"] == 1 and page["items"][0]["id"] == running, page
        passed("filter_search_pagination", {"job": running})

        final = request("/cluster")
        assert final["allocated_cpus"] == 0 and final["available_cpus"] == cpu, final
        passed("resource_reclamation", {"available_cpus": final["available_cpus"]})
    except Exception as exc:
        results.append({"check": "exception", "passed": False, "detail": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        for job_id in created:
            try:
                if request(f"/jobs/{job_id}")["status"] in {"pending", "running"}:
                    request(f"/jobs/{job_id}/cancel", {})
            except (OSError, ValueError):
                pass
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "base_url": args.base_url,
            "jobs": created,
            "results": results,
        }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
