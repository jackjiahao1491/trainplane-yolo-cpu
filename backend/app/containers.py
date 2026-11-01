"""Docker Engine boundary for one isolated, resource-limited training attempt.

Only trusted control-plane code can supply an image or mounts. User commands are
argv inside a container; they never run in the Ray worker or inherit its env.
"""
from __future__ import annotations

from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path, PurePosixPath, PureWindowsPath
import re
import shlex
import time

import docker
from docker.errors import APIError, NotFound
from docker.types import Mount

MANAGED_LABEL = "io.trainplane.managed"
ATTEMPT_LABEL = "io.trainplane.attempt"
JOB_LABEL = "io.trainplane.job"
SPEC_LABEL = "io.trainplane.spec"
MANAGED_VALUE = "training-v1"


def _safe_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", value) or value in {".", ".."}:
        raise ValueError("invalid job or attempt ID")
    return value


def container_name(attempt_id: str) -> str:
    return "trainplane-task-" + _safe_id(attempt_id)


@lru_cache(maxsize=1)
def _client():
    return docker.from_env(version="auto", timeout=5)


def _owned(container, attempt_id: str):
    labels = container.attrs.get("Config", {}).get("Labels") or {}
    if labels.get(MANAGED_LABEL) != MANAGED_VALUE or labels.get(ATTEMPT_LABEL) != attempt_id:
        raise RuntimeError("container name collision: refusing to control an unowned container")
    return container


def _task(attempt_id: str):
    try:
        return _owned(_client().containers.get(container_name(attempt_id)), attempt_id)
    except NotFound:
        return None


def inspect_task(attempt_id: str) -> dict | None:
    """None means an Engine-confirmed 404; connectivity/permission errors propagate."""
    container = _task(attempt_id)
    return None if container is None else container.attrs


def stop_task(attempt_id: str) -> bool:
    """Stop only this labeled attempt; retain its inspect/exit/OOM evidence."""
    container = _task(attempt_id)
    if container is None:
        return False
    if container.attrs.get("State", {}).get("Status") == "created":
        # Never-started attempts have no exit evidence. Remove the immutable ID
        # so a stale supervisor cannot start it after cancellation was confirmed.
        try:
            container.remove(force=True)
        except NotFound:
            pass
        if inspect_task(attempt_id) is not None:
            raise RuntimeError("never-started task removal was not confirmed")
        return True
    if container.attrs.get("State", {}).get("Running") or container.attrs.get("State", {}).get("Paused"):
        if container.attrs["State"].get("Paused"):
            container.unpause()
        try:
            container.stop(timeout=2)
        except APIError:
            # A concurrent natural exit is safe only after a successful re-inspect.
            current = inspect_task(attempt_id)
            if current is None or current.get("State", {}).get("Running"):
                raise
        current = inspect_task(attempt_id)
        if current is not None and current.get("State", {}).get("Running"):
            raise RuntimeError("task container is still running after stop")
    return True


def _host_child(source: str, *parts: str) -> str:
    """Join inspected Engine paths, including Docker Desktop's Windows API proxy."""
    if re.match(r"^[A-Za-z]:[\\/]", source):
        windows = PureWindowsPath(source)
        # Linux control-plane socket expects the actual Docker Desktop VM bind path.
        if os.name != "nt":
            base = PurePosixPath("/run/desktop/mnt/host") / windows.drive[0].lower()
            base = base.joinpath(*windows.parts[1:])
        else:
            base = windows
    else:
        base = PurePosixPath(source)
        if not base.is_absolute():
            raise RuntimeError("control-plane bind source must be absolute")
    if any(part in {"", ".", ".."} or "/" in part or "\\" in part for part in parts):
        raise ValueError("unsafe mount path component")
    return str(base.joinpath(*parts))


def _mount_sources() -> dict[str, str]:
    control = os.environ.get("TRAIN_CONTROL_CONTAINER") or os.environ.get("HOSTNAME") or "trainplane-ray"
    own = _client().containers.get(control).attrs
    sources = {entry["Destination"]: entry["Source"] for entry in own.get("Mounts", [])
               if entry.get("Type") == "bind"}
    if "/app/data" not in sources or "/app/yolov8n.pt" not in sources:
        raise RuntimeError("control plane must bind /app/data and /app/yolov8n.pt")
    return sources


def create_task(job_id: str, attempt_id: str, config: dict):
    job_id, attempt_id = _safe_id(job_id), _safe_id(attempt_id)
    cpu, memory_mb = int(config["cpu"]), int(config.get("memory_mb", 1024))
    if cpu < 1 or memory_mb < 6:
        raise ValueError("invalid CPU or memory limit")
    command = config.get("command")
    if not isinstance(command, str) or not command.strip():
        raise ValueError("command must be a nonempty string")
    argv = shlex.split(command, posix=True)
    if not argv:
        raise ValueError("command is empty")
    if argv[0] in {"python", "python3", "python.exe"}:
        argv[0] = "/usr/local/bin/python"
    image = os.environ.get("TRAIN_IMAGE", "trainplane-backend:local")
    spec = hashlib.sha256(json.dumps({"argv": argv, "cpu": cpu, "memory_mb": memory_mb,
                                     "image": image, "job": job_id}, sort_keys=True).encode()).hexdigest()
    existing = _task(attempt_id)
    if existing is not None:
        if existing.attrs["Config"]["Labels"].get(SPEC_LABEL) != spec:
            raise RuntimeError("attempt already exists with a different execution specification")
        return existing

    data = Path(os.environ.get("TRAINPLANE_DATA_DIR", Path(__file__).resolve().parents[1] / "data")).resolve()
    attempt = data / "jobs" / job_id / attempt_id
    workspace = attempt / "workspace"
    workspace.mkdir(parents=True, exist_ok=True)
    if workspace.is_symlink() or not workspace.resolve().is_relative_to(data):
        raise RuntimeError("workspace escaped the trusted data directory")
    if os.name != "nt":
        os.chown(workspace, 1000, 1000)
    # Docker Desktop bind permissions can ignore chown; only this workspace is writable.
    workspace.chmod(0o777)
    sources = _mount_sources()
    mounts = [
        Mount("/task", _host_child(sources["/app/data"], "jobs", job_id, attempt_id, "workspace"), type="bind"),
        Mount("/app/data/datasets", _host_child(sources["/app/data"], "datasets"), type="bind", read_only=True),
        Mount("/app/yolov8n.pt", _host_child(sources["/app/yolov8n.pt"]), type="bind", read_only=True),
    ]
    font = data / "ultralytics" / "Ultralytics" / "Arial.ttf"
    if font.is_file():
        mounts.append(Mount("/tmp/ultralytics/Ultralytics/Arial.ttf", _host_child(sources["/app/data"], "ultralytics", "Ultralytics", "Arial.ttf"),
                            type="bind", read_only=True))
    # A deliberately explicit allowlist: never copy Ray/Redis/API/Docker credentials.
    environment = {
        "TRAIN_JOB_DIR": "/task", "TRAINPLANE_DATA_DIR": "/app/data", "TRAIN_CPUS": str(cpu),
        "YOLO_CONFIG_DIR": "/tmp/ultralytics", "HOME": "/tmp", "XDG_CONFIG_HOME": "/tmp/.config",
        "MPLCONFIGDIR": "/tmp/matplotlib", "PYTHONUNBUFFERED": "1", "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8", "PYTHONDONTWRITEBYTECODE": "1", "CUDA_VISIBLE_DEVICES": "-1",
        "OMP_NUM_THREADS": str(cpu), "MKL_NUM_THREADS": str(cpu), "OPENBLAS_NUM_THREADS": str(cpu),
        "NUMEXPR_NUM_THREADS": str(cpu),
    }
    labels = {MANAGED_LABEL: MANAGED_VALUE, ATTEMPT_LABEL: attempt_id, JOB_LABEL: job_id, SPEC_LABEL: spec}
    try:
        return _client().containers.create(
            image, command=argv, name=container_name(attempt_id), labels=labels, environment=environment,
            working_dir="/app", user="1000:1000", mounts=mounts, detach=True, init=True,
            nano_cpus=cpu * 1_000_000_000, mem_limit=memory_mb * 1024**2, memswap_limit=memory_mb * 1024**2,
            pids_limit=128, network_mode="none", cap_drop=["ALL"], security_opt=["no-new-privileges:true"],
            read_only=True, tmpfs={"/tmp": "rw,noexec,nosuid,size=134217728,mode=1777",
                                  "/tmp/ultralytics/Ultralytics": "rw,noexec,nosuid,size=16777216,mode=1777"},
            auto_remove=False, restart_policy={"Name": "no"}, stdin_open=False, tty=False,
        )
    except APIError as exc:
        if exc.status_code != 409:
            raise
        existing = _task(attempt_id)
        if existing is None or existing.attrs["Config"]["Labels"].get(SPEC_LABEL) != spec:
            raise
        return existing


def resource_sample(stats: dict, previous: dict | None, inspect: dict) -> dict:
    """Docker CPU convention: one fully busy logical CPU is 100%, not quota-normalized."""
    current_cpu = stats.get("cpu_stats", {})
    previous_cpu = (previous or {}).get("cpu_stats") or stats.get("precpu_stats", {})
    cpu_delta = current_cpu.get("cpu_usage", {}).get("total_usage", 0) - previous_cpu.get("cpu_usage", {}).get("total_usage", 0)
    system_delta = current_cpu.get("system_cpu_usage", 0) - previous_cpu.get("system_cpu_usage", 0)
    online = current_cpu.get("online_cpus") or len(current_cpu.get("cpu_usage", {}).get("percpu_usage", [])) or 1
    cpu_percent = cpu_delta / system_delta * online * 100 if previous_cpu and cpu_delta >= 0 and system_delta > 0 else 0.0
    memory = stats.get("memory_stats", {})
    inactive = memory.get("stats", {}).get("inactive_file", memory.get("stats", {}).get("total_inactive_file", 0))
    usage = memory.get("usage", 0)
    return {"timestamp": time.time(), "container_id": inspect["Id"], "execution_mode": "docker",
            "cpu_percent": round(cpu_percent, 3), "memory_mb": round(max(0, usage - inactive) / 1024**2, 3),
            "memory_usage_mb": round(usage / 1024**2, 3), "pids": stats.get("pids_stats", {}).get("current", 0),
            "memory_limit_mb": inspect.get("HostConfig", {}).get("Memory", 0) / 1024**2,
            "status": inspect.get("State", {}).get("Status")}
