"""Prove two real one-CPU workloads overlap, a third queues, and allocation stays bounded."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
import time
import urllib.request
from platform_http import auth_headers


BASE = "http://127.0.0.1:8000/api"


def request(path, body=None):
    req = urllib.request.Request(BASE + path, data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", **auth_headers()})
    with urllib.request.urlopen(req, timeout=15) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path(__file__).resolve().parents[1] / 'docs/concurrency-results.json')
    args = parser.parse_args()
    created = []
    observations = []
    report = {"timestamp_utc": datetime.now(timezone.utc).isoformat(), "passed": False}
    try:
        cluster = request("/cluster")
        assert cluster["total_cpus"] == 2, "This acceptance scenario expects TOTAL_CPUS=2"
        assert cluster["allocated_cpus"] == 0 and cluster["pending_jobs"] == 0, "Run on an idle platform"
        for index, seconds in enumerate([20, 20, 2]):
            created.append(request("/jobs", {
                "name": f"acceptance-concurrency-{int(time.time())}-{index+1}",
                "command": f"python -m app.demo --sleep {seconds}",
                "cpu": 1, "timeout_seconds": 120, "priority": 0,
            })["id"])
        deadline = time.monotonic() + 150
        overlapped = False
        finished = None
        while time.monotonic() < deadline:
            jobs = [request(f"/jobs/{job_id}") for job_id in created]
            current = request("/cluster")
            assert 0 <= current["allocated_cpus"] <= 2, current
            observations.append({"time": time.time(), "allocated_cpus": current["allocated_cpus"],
                                 "statuses": [j["status"] for j in jobs],
                                 "started": [j["started_at"] is not None for j in jobs]})
            if all(j["status"] == "running" and j["started_at"] and j["heartbeat_at"] for j in jobs[:2]):
                assert jobs[2]["status"] == "pending", jobs
                assert current["allocated_cpus"] == 2 and current["available_cpus"] == 0, current
                if not overlapped:
                    print(f"PASS overlap: real children {created[:2]} running; {created[2]} pending.", flush=True)
                overlapped = True
            if any(j["status"] in {"failed", "cancelled"} for j in jobs):
                raise AssertionError(jobs)
            if all(j["status"] == "succeeded" for j in jobs):
                finished = jobs
                break
            time.sleep(0.5)
        assert overlapped, "Did not observe two actual started workloads at the same time"
        assert finished, "Tasks did not finish"
        parse = lambda value: datetime.fromisoformat(value).timestamp()
        assert parse(finished[2]["started_at"]) >= min(parse(j["finished_at"]) for j in finished[:2])
        assert all(j["exit_code"] == 0 for j in finished)
        final = request("/cluster")
        assert final["allocated_cpus"] == 0 and final["available_cpus"] == 2, final
        report.update({"passed": True, "jobs": finished, "observations": observations,
                       "maximum_observed_allocation": max(o["allocated_cpus"] for o in observations),
                       "final_cluster": final})
        print("PASS queue auto-dispatch and resource recovery; allocation never exceeded 2.", flush=True)
    except Exception as exc:
        report.update({"jobs": created, "observations": observations, "error": f"{type(exc).__name__}: {exc}"})
        raise
    finally:
        output = args.output
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        for job_id in created:
            try:
                if request(f"/jobs/{job_id}")["status"] in {"pending", "running"}:
                    request(f"/jobs/{job_id}/cancel", {})
            except OSError:
                pass


if __name__ == "__main__":
    main()
