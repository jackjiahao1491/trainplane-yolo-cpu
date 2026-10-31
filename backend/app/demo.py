"""Deterministic lightweight workload for scheduling and failure acceptance checks."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import time


def nonnegative(value: str) -> float:
    number = float(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return number


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sleep", type=nonnegative, default=10)
    parser.add_argument("--exit-code", type=int, default=0)
    parser.add_argument("--freeze-heartbeat", type=nonnegative, default=None,
                        help="Stop worker heartbeat writes after N seconds (training keeps running)")
    args = parser.parse_args()
    if args.freeze_heartbeat is not None and not os.environ.get("TRAIN_JOB_DIR"):
        parser.error("--freeze-heartbeat requires TRAIN_JOB_DIR; run this command through the worker")
    start = time.monotonic()
    frozen = False
    tick = 0
    print(f"[demo] started: duration={args.sleep:g}s, exit_code={args.exit_code}", flush=True)
    while True:
        elapsed = time.monotonic() - start
        if args.freeze_heartbeat is not None and not frozen and elapsed >= args.freeze_heartbeat:
            (Path(os.environ["TRAIN_JOB_DIR"]) / ".heartbeat-disabled").write_text(str(time.time()), encoding="utf-8")
            print("[demo] heartbeat frozen; training is still alive", flush=True)
            frozen = True
        if elapsed >= args.sleep:
            break
        print(f"[demo] tick={tick} elapsed={elapsed:.1f}s 中文日志 ✓ <tag> & \"quotes\"", flush=True)
        tick += 1
        time.sleep(min(1, args.sleep - elapsed))
    print(f"[demo] finished with exit_code={args.exit_code}", file=sys.stderr if args.exit_code else sys.stdout, flush=True)
    return args.exit_code


if __name__ == "__main__":
    sys.exit(main())
