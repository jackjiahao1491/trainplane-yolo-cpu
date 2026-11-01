"""Ray Job entry point: supervise one training subprocess and its process tree."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import signal
import subprocess
import sys
import threading
import time

import psutil
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry


def write_json(path: Path, payload: dict) -> None:
    """Readers see either the old complete document or the new one."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=False)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def matching_process(pid: int, create_time: float) -> psutil.Process | None:
    try:
        process = psutil.Process(pid)
        # PID 可能被操作系统复用；只操作本次训练创建的进程。
        if abs(process.create_time() - create_time) < 0.01:
            return process
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass
    return None


def stop_tree(identities: dict[int, float]) -> None:
    processes = []
    for pid, created in list(identities.items()):
        process = matching_process(pid, created)
        if process is None:
            continue
        try:
            for child in process.children(recursive=True):
                identities[child.pid] = child.create_time()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    for pid, created in identities.items():
        process = matching_process(pid, created)
        if process is not None:
            processes.append(process)
    for process in reversed(processes):
        try:
            process.terminate()
        except psutil.NoSuchProcess:
            pass
    _, alive = psutil.wait_procs(processes, timeout=3)
    for process in alive:
        # 再次核对身份，避免在等待过程中 PID 被复用。
        if matching_process(process.pid, identities[process.pid]) is not None:
            try:
                process.kill()
            except psutil.NoSuchProcess:
                pass
    _, alive = psutil.wait_procs(alive, timeout=3)
    if alive:
        raise RuntimeError("could not stop process tree: " + ", ".join(str(p.pid) for p in alive))


def safe_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value) or value in {".", ".."}:
        raise argparse.ArgumentTypeError("ID may contain only letters, digits, _, - and .")
    return value


def run_container(args, job_dir: Path) -> int:
    """Trusted Ray supervisor. The untrusted command runs only in its task container."""
    from datetime import datetime
    import docker
    from . import containers

    stop_monitor, cancelled, interrupted = threading.Event(), threading.Event(), threading.Event()
    logs_halt = threading.Event()
    monitor = log_thread = None
    log_stream = [None]
    log_errors = []
    container = None
    exit_code, error, oom_killed = 1, None, False
    config = {}
    started_clock = time.monotonic()

    def handle_signal(_signum, _frame):
        interrupted.set()

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handle_signal)

    with (job_dir / "train.log").open("ab", buffering=0) as output:
        output_lock = threading.Lock()

        def log(message):
            with output_lock:
                output.write((f"[worker] {message}\n").encode("utf-8", errors="replace"))

        def copy_logs():
            # A separate cancellable stream can wait for quiet training without
            # imposing an unbounded wait on cancellation/timeout supervision.
            client = None
            try:
                client = docker.from_env(version=containers._client().api._version, timeout=None)
                logged = client.containers.get(container.id)
                stream = logged.logs(stdout=True, stderr=True, stream=True, follow=True)
                log_stream[0] = stream
                for chunk in stream:
                    if logs_halt.is_set():
                        break
                    with output_lock:
                        output.write(chunk)
            except Exception as exc:
                if not logs_halt.is_set():
                    log_errors.append(str(exc))
            finally:
                if client is not None:
                    client.close()

        def heartbeat_loop():
            client = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                                          socket_connect_timeout=1, socket_timeout=1,
                                          retry=Retry(NoBackoff(), 0))
            unavailable = False
            try:
                while not stop_monitor.is_set():
                    now = time.time()
                    freeze = ((args.no_heartbeat_after is not None and time.monotonic() - started_clock >= args.no_heartbeat_after)
                              or (job_dir / "workspace" / ".heartbeat-disabled").exists())
                    if not freeze:
                        write_json(job_dir / "heartbeat.json", {"timestamp": now, "attempt_id": args.attempt_id})
                    try:
                        if not freeze:
                            client.set("heartbeat:" + args.attempt_id, str(now), ex=30)
                        if client.get("cancel:" + args.attempt_id) is not None:
                            cancelled.set()
                        if unavailable:
                            log("Redis connection recovered")
                        unavailable = False
                    except redis.RedisError as exc:
                        if not unavailable:
                            log(f"Redis unavailable; isolated training continues: {exc}")
                        unavailable = True
                    stop_monitor.wait(2)
            finally:
                client.close()

        try:
            # Publish supervisor identity before any Engine RPC. Cancellation
            # must be able to find us even during container creation/startup.
            write_json(job_dir / "process.json", {
                "worker_pid": os.getpid(), "worker_create_time": psutil.Process(os.getpid()).create_time(),
                "pid": None, "create_time": None, "children": [], "started_at": None,
                "execution_mode": "docker", "container_id": None,
                "container_name": containers.container_name(args.attempt_id),
            })
            config = json.loads((job_dir / "config.json").read_text(encoding="utf-8"))
            timeout = float(config["timeout_seconds"])
            if timeout <= 0:
                raise ValueError("timeout_seconds must be positive")
            monitor = threading.Thread(target=heartbeat_loop, name="heartbeat", daemon=True)
            monitor.start()
            if cancelled.is_set() or interrupted.is_set():
                raise InterruptedError("cancelled before container creation")
            container = containers.create_task(args.job_id, args.attempt_id, config)
            container.reload()
            if container.attrs["State"]["Status"] == "created":
                if cancelled.is_set() or interrupted.is_set():
                    raise InterruptedError("cancelled before container start")
                container.start()
                container.reload()
            state = container.attrs["State"]
            started_at = datetime.fromisoformat(state["StartedAt"].replace("Z", "+00:00")).timestamp()
            started_clock = time.monotonic() - max(0, time.time() - started_at)
            write_json(job_dir / "process.json", {
                "worker_pid": os.getpid(), "worker_create_time": psutil.Process(os.getpid()).create_time(),
                "pid": None, "create_time": None, "children": [], "started_at": started_at,
                "execution_mode": "docker", "container_id": container.id,
                "container_name": containers.container_name(args.attempt_id),
            })
            log(f"isolated container={container.id}, cpu={config['cpu']}, memory_mb={config.get('memory_mb', 1024)}, user=1000:1000")
            log_thread = threading.Thread(target=copy_logs, name="container-logs", daemon=True)
            log_thread.start()
            previous_stats, next_sample = None, 0.0
            while True:
                container.reload()
                state = container.attrs["State"]
                if not state.get("Running"):
                    exit_code = int(state["ExitCode"])
                    oom_killed = bool(state.get("OOMKilled"))
                    if oom_killed:
                        exit_code, error = 137, f"memory_limit: exceeded {config.get('memory_mb', 1024)} MiB"
                    break
                if cancelled.is_set() or interrupted.is_set():
                    error, exit_code = "cancelled", 130
                    break
                if time.monotonic() - started_clock >= timeout:
                    error, exit_code = f"timeout: exceeded {timeout:g} seconds", 124
                    break
                if log_errors:
                    raise RuntimeError("container log stream failed: " + log_errors[0])
                if time.monotonic() >= next_sample:
                    stats = container.stats(stream=False, one_shot=True)
                    sample = containers.resource_sample(stats, previous_stats, container.attrs)
                    with (job_dir / "resources.jsonl").open("a", encoding="utf-8") as samples:
                        samples.write(json.dumps(sample, allow_nan=False) + "\n")
                    write_json(job_dir / "resource.json", sample)
                    previous_stats, next_sample = stats, time.monotonic() + 2
                stop_monitor.wait(0.2)
        except InterruptedError:
            error, exit_code = "cancelled", 130
        except Exception as exc:
            error, exit_code = f"worker error: {type(exc).__name__}: {exc}", 1
            log(error)
        finally:
            # Always look up the deterministic name, even if create timed out
            # after Engine accepted it but before returning its container ID.
            try:
                containers.stop_task(args.attempt_id)
                final = containers.inspect_task(args.attempt_id)
                if final:
                    oom_killed = bool(final["State"].get("OOMKilled"))
                    if oom_killed:
                        error, exit_code = f"memory_limit: exceeded {config.get('memory_mb', 1024)} MiB", 137
                    write_json(job_dir / "container.json", final)
            except Exception as exc:
                error = f"{error + '; ' if error else ''}container cleanup unconfirmed: {exc}"
                exit_code = exit_code or 1
                log(error)
            stop_monitor.set()
            if monitor is not None:
                monitor.join(timeout=4)
            if log_thread is not None:
                log_thread.join(timeout=4)
            logs_halt.set()
            if log_stream[0] is not None:
                try:
                    log_stream[0].close()
                except (OSError, RuntimeError):
                    pass
            if log_thread is not None:
                log_thread.join(timeout=1)
            log(f"finished exit_code={exit_code}, oom_killed={oom_killed}")
            write_json(job_dir / "result.json", {
                "exit_code": exit_code, "finished_at": time.time(), "error": error,
                "pid": None, "create_time": None, "children": [], "execution_mode": "docker",
                "container_id": container.id if container is not None else None,
                "container_name": containers.container_name(args.attempt_id), "oom_killed": oom_killed,
                "memory_limit_mb": int(config.get("memory_mb", 1024)),
            })
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id", type=safe_id, required=True)
    parser.add_argument("--attempt-id", type=safe_id, required=True)
    parser.add_argument("--no-heartbeat-after", type=float, default=None,
                        help="Fault injection: stop heartbeat writes after N seconds")
    args = parser.parse_args()
    data_dir = Path(os.environ.get("TRAINPLANE_DATA_DIR", Path(__file__).resolve().parents[1] / "data")).resolve()
    job_dir = data_dir / "jobs" / args.job_id / args.attempt_id
    job_dir.mkdir(parents=True, exist_ok=True)
    execution_mode = os.environ.get("EXECUTION_MODE", "process")
    if execution_mode == "docker":
        return run_container(args, job_dir)
    if execution_mode != "process":
        raise ValueError("EXECUTION_MODE must be docker or process")
    stop_monitor = threading.Event()
    cancelled = threading.Event()
    interrupted = threading.Event()
    process: subprocess.Popen | None = None
    identities: dict[int, float] = {}
    monitor: threading.Thread | None = None
    exit_code = 1
    error: str | None = None
    pid: int | None = None
    create_time: float | None = None

    def handle_signal(_signum, _frame):
        interrupted.set()

    for name in ("SIGTERM", "SIGINT", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), handle_signal)

    with (job_dir / "train.log").open("ab", buffering=0) as log_file:
        def log(message: str) -> None:
            log_file.write((f"[worker] {message}\n").encode("utf-8", errors="replace"))

        try:
            config = json.loads((job_dir / "config.json").read_text(encoding="utf-8"))
            command = config["command"]
            if not isinstance(command, str) or not command.strip():
                raise ValueError("command must be a non-empty string")
            cpu = int(config["cpu"])
            timeout = float(config["timeout_seconds"])
            if cpu < 1 or timeout <= 0:
                raise ValueError("cpu and timeout_seconds must be positive")
            environment = os.environ.copy()
            environment.update({
                "TRAIN_JOB_DIR": str(job_dir), "TRAIN_CPUS": str(cpu),
                "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8",
                "CUDA_VISIBLE_DEVICES": "-1", "OMP_NUM_THREADS": str(cpu),
                "MKL_NUM_THREADS": str(cpu), "OPENBLAS_NUM_THREADS": str(cpu),
                "NUMEXPR_NUM_THREADS": str(cpu),
            })
            # Windows CreateProcess 正确处理原生命令行引号；不启动额外 shell。
            if os.name == "nt":
                argv = re.sub(
                    r'^\s*(?:"python(?:\.exe)?"|python(?:\.exe)?)(?=\s|$)',
                    lambda _match: subprocess.list2cmdline([sys.executable]),
                    command, count=1, flags=re.IGNORECASE,
                )
            else:
                argv = shlex.split(command)
                if argv[0] in {"python", "python3", "python.exe"}:
                    argv[0] = sys.executable
            process = subprocess.Popen(
                argv, shell=False, stdin=subprocess.DEVNULL,
                stdout=log_file, stderr=subprocess.STDOUT, env=environment,
                cwd=Path(__file__).resolve().parents[1],
                creationflags=(subprocess.CREATE_NO_WINDOW | subprocess.CREATE_NEW_PROCESS_GROUP) if os.name == "nt" else 0,
                start_new_session=os.name != "nt",
            )
            pid = process.pid
            try:
                create_time = psutil.Process(pid).create_time()
                identities[pid] = create_time
            except psutil.NoSuchProcess:
                # 极短命令可能已经退出，仍应以 Popen 保存的退出码作为结果。
                if process.poll() is None:
                    raise
            started_at = time.time()
            started_clock = time.monotonic()
            process_info = {
                "worker_pid": os.getpid(),
                "worker_create_time": psutil.Process(os.getpid()).create_time(),
                "pid": pid, "create_time": create_time, "started_at": started_at,
                "children": [],
            }
            write_json(job_dir / "process.json", process_info)
            log(f"started pid={pid}, cpu={cpu}, timeout={timeout:g}s")

            def heartbeat_loop() -> None:
                client = redis.Redis.from_url(
                    os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"),
                    socket_connect_timeout=1, socket_timeout=1,
                    retry=Retry(NoBackoff(), 0),
                )
                redis_unavailable = False
                while not stop_monitor.is_set():
                    now = time.time()
                    freeze = ((args.no_heartbeat_after is not None
                               and time.monotonic() - started_clock >= args.no_heartbeat_after)
                              or (job_dir / ".heartbeat-disabled").exists())
                    if not freeze:
                        try:
                            write_json(job_dir / "heartbeat.json", {"timestamp": now, "attempt_id": args.attempt_id})
                        except OSError as exc:
                            log(f"local heartbeat write failed: {exc}")
                    try:
                        if not freeze:
                            client.set(f"heartbeat:{args.attempt_id}", str(now), ex=30)
                        if client.get(f"cancel:{args.attempt_id}") is not None:
                            cancelled.set()
                        if redis_unavailable:
                            log("Redis connection recovered")
                        redis_unavailable = False
                    except redis.RedisError as exc:
                        if not redis_unavailable:
                            log(f"Redis unavailable; training continues: {exc}")
                        redis_unavailable = True
                    stop_monitor.wait(2)
                client.close()

            monitor = threading.Thread(target=heartbeat_loop, name="heartbeat", daemon=True)
            monitor.start()
            while process.poll() is None:
                if cancelled.is_set() or interrupted.is_set():
                    error = "cancelled" if cancelled.is_set() else "cancelled: worker received a termination signal"
                    break
                if time.monotonic() - started_clock >= timeout:
                    error = f"timeout: exceeded {timeout:g} seconds"
                    break
                try:
                    for child in psutil.Process(pid).children(recursive=True):
                        identities[child.pid] = child.create_time()
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass
                children = [{"pid": child_pid, "create_time": created}
                            for child_pid, created in identities.items() if child_pid != pid]
                if children != process_info["children"]:
                    # 父进程退出后无法再向下枚举，提前保存后代身份供恢复与兜底清理。
                    process_info["children"] = children
                    write_json(job_dir / "process.json", process_info)
                stop_monitor.wait(0.2)
            if error:
                log(error)
                stop_tree(identities)
            exit_code = process.wait()
            if error and exit_code == 0:
                exit_code = 124 if error.startswith("timeout") else 130
        except Exception as exc:
            error = f"worker error: {type(exc).__name__}: {exc}"
            exit_code = 1
            log(error)
        finally:
            stop_monitor.set()
            if monitor is not None:
                monitor.join(timeout=4)
            try:
                stop_tree(identities)
                if process is not None:
                    process.wait(timeout=3)
            except (psutil.Error, OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                error = f"{error + '; ' if error else ''}cleanup failed: {exc}"
                exit_code = exit_code or 1
                log(error)
            log(f"finished exit_code={exit_code}")
            write_json(job_dir / "result.json", {
                "exit_code": exit_code, "finished_at": time.time(), "error": error,
                "pid": pid, "create_time": create_time,
                "children": [{"pid": child_pid, "create_time": created}
                             for child_pid, created in identities.items() if child_pid != pid],
            })
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
