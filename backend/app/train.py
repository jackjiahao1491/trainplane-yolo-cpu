"""Small, CPU-only coco128 training workload used by the platform."""

from __future__ import annotations

import argparse
import json
import math
import os
from pathlib import Path
import time
import yaml


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--epochs", type=positive_int, default=1)
    parser.add_argument("--imgsz", "--size", dest="imgsz", type=positive_int, default=320)
    parser.add_argument("--batch", type=positive_int, default=1)
    parser.add_argument("--model", default="yolov8n.pt")
    args = parser.parse_args()
    cpu = max(1, int(os.environ.get("TRAIN_CPUS", "1")))
    os.environ["CUDA_VISIBLE_DEVICES"] = "-1"
    os.environ["OMP_NUM_THREADS"] = str(cpu)
    os.environ["MKL_NUM_THREADS"] = str(cpu)

    import torch
    import ultralytics
    from ultralytics import YOLO, settings

    torch.set_num_threads(cpu)
    torch.set_num_interop_threads(1)
    job_dir = Path(os.environ.get("TRAIN_JOB_DIR", Path.cwd() / "training-output")).resolve()
    job_dir.mkdir(parents=True, exist_ok=True)
    data_root = Path(os.environ.get('TRAINPLANE_DATA_DIR', Path(__file__).resolve().parents[1] / 'data'))
    datasets_dir = (data_root / 'datasets').resolve()
    settings.update({'datasets_dir': str(datasets_dir)})
    data = 'coco128.yaml'
    if (datasets_dir / 'coco128' / 'images' / 'train2017').is_dir():
        # Ultralytics caches DATASETS_DIR at import time. An absolute dataset path
        # makes the prepared data portable across Windows and container paths.
        official_yaml = Path(ultralytics.__file__).parent / 'cfg' / 'datasets' / 'coco128.yaml'
        definition = yaml.safe_load(official_yaml.read_text(encoding='utf-8'))
        definition['path'] = str(datasets_dir / 'coco128')
        definition.pop('download', None)
        dataset_yaml = job_dir / 'coco128.yaml'
        dataset_yaml.write_text(yaml.safe_dump(definition, allow_unicode=True), encoding='utf-8')
        data = str(dataset_yaml)
    recorded_epochs: set[int] = set()

    def save_metrics(trainer) -> None:
        epoch = trainer.epoch + 1
        # Ultralytics 最后评估最佳模型时也触发该回调；每个训练 epoch 只记一次。
        if epoch in recorded_epochs:
            return
        values = dict(trainer.metrics or {})
        if trainer.tloss is not None:
            values.update(trainer.label_loss_items(trainer.tloss, prefix="train"))
        metrics = {}
        for name, value in values.items():
            try:
                number = float(value)
            except (TypeError, ValueError):
                continue
            if math.isfinite(number):
                metrics[str(name)] = number
        record = {"epoch": epoch, "timestamp": time.time(), "metrics": metrics}
        # 每轮追加一条，不需要解析终端进度条，也不会覆盖历史指标。
        with (job_dir / "metrics.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n")
            stream.flush()
        recorded_epochs.add(epoch)
        print("[metrics] " + json.dumps(record, ensure_ascii=False), flush=True)

    model = YOLO(args.model)
    def enforce_cpu_threads(_trainer):
        # Ultralytics select_device() resets CPU threads, so enforce the job's
        # allocation after trainer/validator initialization, including final validation.
        torch.set_num_threads(cpu)
        print(f'[resources] device=cpu torch_threads={torch.get_num_threads()} quota={cpu}', flush=True)

    for event in ('on_pretrain_routine_start', 'on_train_start', 'on_val_start'):
        model.add_callback(event, enforce_cpu_threads)
    model.add_callback("on_fit_epoch_end", save_metrics)
    print(f"CPU-only training: cpus={cpu}, coco128, epochs={args.epochs}, imgsz={args.imgsz}, batch={args.batch}", flush=True)
    model.train(
        data=data, device="cpu", epochs=args.epochs,
        imgsz=args.imgsz, batch=args.batch, workers=0, cache=False,
        val=True, plots=False, amp=False, seed=0,
        project=str(job_dir / "artifacts"), name="train", exist_ok=True,
    )


if __name__ == "__main__":
    main()
