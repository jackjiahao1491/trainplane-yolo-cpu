"""Stop only the recorded project processes, rejecting reused process IDs."""
import argparse
import datetime as dt
import json
from pathlib import Path

import psutil


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--api-only", action="store_true")
    parser.add_argument("--web-only", action="store_true")
    args = parser.parse_args()
    records = json.loads(args.manifest.read_text(encoding="utf-8-sig"))
    if isinstance(records, dict):
        records = [records]
    keep = []
    failed = False
    for record in sorted(records, key=lambda row: {"web": 0, "api": 1, "ray": 2}.get(row["name"], 3)):
        if (args.api_only and record["name"] != "api") or (args.web_only and record["name"] != "web"):
            keep.append(record)
            continue
        try:
            process = psutil.Process(record["pid"])
            started = dt.datetime.fromisoformat(record["started_at"]).timestamp()
            if abs(process.create_time() - started) > 1:
                print(f"Skip reused PID {record['pid']} ({record['name']})")
                continue
            children = process.children(recursive=True)
            # psutil Process objects check creation time before kill, avoiding PID reuse.
            for child in reversed(children):
                try:
                    child.kill()
                except psutil.NoSuchProcess:
                    pass
            process.kill()
            _, alive = psutil.wait_procs(children + [process], timeout=10)
            if alive:
                failed = True
                keep.append(record)
                print(f"Still alive: {[child.pid for child in alive]}")
            else:
                print(f"Stopped {record['name']} (PID {record['pid']})")
        except psutil.NoSuchProcess:
            pass
        except psutil.Error as exc:
            failed = True
            keep.append(record)
            print(f"Cannot stop {record['name']}: {exc}")
    args.manifest.write_text(json.dumps(keep, indent=2), encoding="utf-8")
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
