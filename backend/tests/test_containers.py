"""Isolation boundary checks; opt-in Docker checks create only labeled test containers."""
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import time
import uuid

from docker.errors import APIError, NotFound
import pytest

from app import containers


class FakeContainer:
    def __init__(self, options):
        self.options = options
        self.id = "test-container-id"
        self.attrs = {"Id": self.id, "Config": {"Labels": options["labels"]},
                      "State": {"Running": False, "Status": "created"}}


class FakeCollection:
    def __init__(self):
        self.container = None
        self.created = 0

    def get(self, name):
        if self.container is None:
            raise NotFound("not found")
        return self.container

    def create(self, image, **options):
        self.created += 1
        self.container = FakeContainer(options)
        return self.container


@pytest.fixture
def engine(tmp_path, monkeypatch):
    collection = FakeCollection()
    client = type("Client", (), {"containers": collection})()
    monkeypatch.setattr(containers, "_client", lambda: client)
    monkeypatch.setattr(containers, "_mount_sources", lambda: {
        "/app/data": "/approved/data", "/app/yolov8n.pt": "/approved/yolov8n.pt"})
    monkeypatch.setenv("TRAINPLANE_DATA_DIR", str(tmp_path))
    return collection


def test_task_limits_mounts_and_environment_are_controlled(engine, monkeypatch):
    monkeypatch.setenv("REDIS_URL", "secret-redis")
    monkeypatch.setenv("TRAINPLANE_API_TOKEN", "secret-api")
    monkeypatch.setenv("RAY_ADDRESS", "secret-ray")
    config = {"command": "python -m app.demo --sleep 1", "cpu": 2, "memory_mb": 128}
    task = containers.create_task("job1", "job1-1", config)
    options = task.options
    assert options["user"] == "1000:1000" and options["network_mode"] == "none"
    assert options["nano_cpus"] == 2_000_000_000
    assert options["mem_limit"] == options["memswap_limit"] == 128 * 1024**2
    assert options["pids_limit"] == 128 and options["read_only"] and options["init"]
    assert options["cap_drop"] == ["ALL"] and options["security_opt"] == ["no-new-privileges:true"]
    assert not options["auto_remove"]
    mounts = {m["Target"]: m for m in options["mounts"]}
    assert set(mounts) == {"/task", "/app/data/datasets", "/app/yolov8n.pt"}
    assert mounts["/task"]["Source"] == "/approved/data/jobs/job1/job1-1/workspace"
    assert not mounts["/task"]["ReadOnly"]
    assert mounts["/app/data/datasets"]["ReadOnly"] and mounts["/app/yolov8n.pt"]["ReadOnly"]
    env = options["environment"]
    assert not {"REDIS_URL", "RAY_ADDRESS", "TRAINPLANE_API_TOKEN", "DOCKER_HOST"} & env.keys()
    assert env["TRAIN_JOB_DIR"] == "/task" and env["CUDA_VISIBLE_DEVICES"] == "-1"
    assert options["command"][:3] == ["/usr/local/bin/python", "-m", "app.demo"]
    assert containers.create_task("job1", "job1-1", config) is task
    assert engine.created == 1
    with pytest.raises(RuntimeError, match="different execution"):
        containers.create_task("job1", "job1-1", dict(config, cpu=1))


def test_missing_is_only_404_and_name_collision_is_not_controlled(engine, monkeypatch):
    assert containers.inspect_task("missing") is None
    engine.container = FakeContainer({"labels": {containers.MANAGED_LABEL: "foreign"}})
    with pytest.raises(RuntimeError, match="unowned"):
        containers.stop_task("missing")
    def unavailable(name):
        raise APIError("Engine unavailable")
    monkeypatch.setattr(engine, "get", unavailable)
    with pytest.raises(APIError):
        containers.inspect_task("missing")


def test_unsafe_ids_cannot_escape_mounts(engine):
    for value in ["..", "../elsewhere", "id/next", "id\\next", "/host", ""]:
        with pytest.raises(ValueError):
            containers.container_name(value)
    with pytest.raises(ValueError):
        containers._host_child("/approved/data", "..")


def test_stop_never_started_container_removes_its_immutable_id(engine):
    task = containers.create_task("job1", "job1-1", {"command": "python -m app.demo", "cpu": 1})
    removed = []
    def remove(force=False):
        assert force
        removed.append(task.id)
        engine.container = None
    task.remove = remove
    assert containers.stop_task("job1-1") is True
    assert removed == [task.id] and containers.inspect_task("job1-1") is None


def test_resource_delta_is_single_core_percent_and_memory_working_set():
    stats = {"cpu_stats": {"cpu_usage": {"total_usage": 2_000_000_000},
                           "system_cpu_usage": 8_000_000_000, "online_cpus": 4},
             "memory_stats": {"usage": 128 * 1024**2, "stats": {"inactive_file": 32 * 1024**2}},
             "pids_stats": {"current": 3}}
    previous = {"cpu_stats": {"cpu_usage": {"total_usage": 1_000_000_000}, "system_cpu_usage": 4_000_000_000}}
    sample = containers.resource_sample(stats, previous, {"Id": "real-id", "HostConfig": {"Memory": 256 * 1024**2}, "State": {"Status": "running"}})
    assert sample["cpu_percent"] == 100 and sample["memory_mb"] == 96
    assert sample["memory_usage_mb"] == 128 and sample["memory_limit_mb"] == 256 and sample["pids"] == 3
    assert sample["container_id"] == "real-id"


@pytest.mark.skipif(os.environ.get("RUN_DOCKER_TESTS") != "1", reason="Set RUN_DOCKER_TESTS=1 for real Engine isolation checks")
def test_real_engine_uid_mounts_caps_network_stats_and_oom():
    """Uses the configured control-plane mounts; never touches unrelated containers."""
    data = Path(os.environ["TRAINPLANE_DATA_DIR"])
    made = []
    try:
        job = "isolation-" + uuid.uuid4().hex[:12]
        attempt = job + "-1"
        made.append(attempt)
        code = (
            "import json,os,pathlib,time; "
            "assert os.getuid()==1000; "
            "assert not pathlib.Path('/var/run/docker.sock').exists(); "
            "assert not pathlib.Path('/app/data/platform.sqlite3').exists(); "
            "assert os.access('/tmp/ultralytics/Ultralytics',os.W_OK); "
            "assert not any(k in os.environ for k in ['REDIS_URL','RAY_ADDRESS','DOCKER_HOST','TRAINPLANE_API_TOKEN']); "
            "pathlib.Path('/task/writable.txt').write_text('uid1000'); "
            "print(json.dumps({'uid':os.getuid(),'network':os.listdir('/sys/class/net')}),flush=True); "
            "end=time.monotonic()+5; "
            "exec('while time.monotonic()<end: pass')"
        )
        task = containers.create_task(job, attempt, {"cpu": 1, "memory_mb": 64,
                                      "command": shlex.join(["python", "-c", code])})
        task.start()
        task.reload()
        actual = containers.inspect_task(attempt)
        host = actual["HostConfig"]
        assert host["NanoCpus"] == 1_000_000_000
        assert host["Memory"] == host["MemorySwap"] == 64 * 1024**2 and host["PidsLimit"] == 128
        assert host["NetworkMode"] == "none" and host["ReadonlyRootfs"]
        assert host["CapDrop"] == ["ALL"] and any("no-new-privileges" in s for s in host["SecurityOpt"])
        assert host.get("PidMode", "") == "" and actual["Config"]["User"] == "1000:1000"
        assert not host["AutoRemove"]
        first = task.stats(stream=False, one_shot=True)
        time.sleep(0.5)
        sample = containers.resource_sample(task.stats(stream=False, one_shot=True), first, actual)
        assert 0 <= sample["cpu_percent"] <= 130 and sample["memory_mb"] > 0 and sample["pids"] > 0
        assert task.wait(timeout=15)["StatusCode"] == 0, task.logs().decode(errors="replace")
        diagnostic = json.loads(task.logs().decode().strip().splitlines()[-1])
        assert diagnostic["uid"] == 1000 and diagnostic["network"] == ["lo"]
        assert (data / "jobs" / job / attempt / "workspace" / "writable.txt").read_text() == "uid1000"

        oom_job = "oom-" + uuid.uuid4().hex[:12]
        oom_attempt = oom_job + "-1"
        made.append(oom_attempt)
        task = containers.create_task(oom_job, oom_attempt, {"cpu": 1, "memory_mb": 64,
                                      "command": "python -c 'x=bytearray(256*1024*1024)'"})
        task.start()
        assert task.wait(timeout=20)["StatusCode"] == 137
        assert containers.inspect_task(oom_attempt)["State"]["OOMKilled"] is True
    finally:
        for attempt in made:
            # Only UUID attempts created by this test, verified by ownership labels.
            containers.stop_task(attempt)
            item = containers._task(attempt)
            if item is not None:
                item.remove()


@pytest.mark.skipif(os.environ.get("RUN_DOCKER_TESTS") != "1", reason="Set RUN_DOCKER_TESTS=1 for real Engine supervisor checks")
@pytest.mark.parametrize("oom", [False, True])
def test_real_container_worker_persists_logs_resources_and_memory_failure(oom):
    data = Path(os.environ["TRAINPLANE_DATA_DIR"])
    job = "worker-isolation-" + uuid.uuid4().hex[:10]
    attempt = job + "-1"
    folder = data / "jobs" / job / attempt
    folder.mkdir(parents=True)
    command = "python -c 'x=bytearray(256*1024*1024)'" if oom else "python -m app.demo --sleep 3 --exit-code 7"
    (folder / "config.json").write_text(json.dumps({"command": command, "cpu": 1, "memory_mb": 64,
                                                     "timeout_seconds": 20}), encoding="utf-8")
    environment = dict(os.environ, EXECUTION_MODE="docker", PYTHONUTF8="1")
    backend = Path(__file__).resolve().parents[1]
    process = None
    try:
        process = subprocess.Popen([sys.executable, "-m", "app.worker", "--job-id", job, "--attempt-id", attempt],
                                   cwd=backend, env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
        stdout, stderr = process.communicate(timeout=30)
        result = json.loads((folder / "result.json").read_text(encoding="utf-8"))
        assert result["container_id"] and result["execution_mode"] == "docker", (stdout, stderr, result)
        final = containers.inspect_task(attempt)
        assert not final["State"]["Running"]
        if oom:
            assert result["exit_code"] == 137 and result["oom_killed"] is True
            assert result["error"].startswith("memory_limit") and process.returncode != 0
        else:
            assert result["exit_code"] == 7 and result["error"] is None
            assert '中文日志' in (folder / "train.log").read_text(encoding="utf-8")
            samples = [json.loads(line) for line in (folder / "resources.jsonl").read_text().splitlines()]
            assert samples and all(s["container_id"] == result["container_id"] for s in samples)
            assert all(s["memory_limit_mb"] == 64 and s["pids"] >= 0 for s in samples)
            assert (folder / "resource.json").is_file()
            assert not (folder / "workspace" / "result.json").exists()
    finally:
        containers.stop_task(attempt)
        if process is not None and process.poll() is None:
            process.kill()
            process.communicate(timeout=5)
        item = containers._task(attempt)
        if item is not None:
            item.remove()
