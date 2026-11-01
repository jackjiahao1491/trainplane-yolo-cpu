"""Cache official YOLO weights and coco128 before an offline interview demo."""
import hashlib
import argparse
import json
from pathlib import Path
import zipfile
import shutil
import os

import requests

ROOT = Path(__file__).resolve().parents[1]
BACKEND = ROOT / 'backend'
DATASETS = BACKEND / 'data' / 'datasets'


def download(session, url, path, expected_sha256=None):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with session.get(url, stream=True, timeout=(15, 30)) as response:
            response.raise_for_status()
            temporary = path.with_suffix(path.suffix + '.part')
            with temporary.open('wb') as output:
                for block in response.iter_content(65536):
                    output.write(block)
            temporary.replace(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if expected_sha256 and digest != expected_sha256:
        raise ValueError(f'{path.name}: SHA256 does not match the official Ultralytics file')
    print(f'Ready: {path.name}, {path.stat().st_size} bytes, SHA256 {digest}', flush=True)
    return {'url': url, 'bytes': path.stat().st_size, 'sha256': digest}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mirror', action='store_true', help='Use HF mirror for model transport; verify official SHA256')
    args = parser.parse_args()
    session = requests.Session()
    # Explicit direct HTTPS for these public assets: this host's registry proxy stalls GitHub release downloads.
    session.trust_env = False
    report = {}
    model_url = ('https://hf-mirror.com/Ultralytics/YOLOv8/resolve/main/yolov8n.pt' if args.mirror else
                 'https://github.com/ultralytics/assets/releases/download/v8.3.0/yolov8n.pt')
    # Official digest: https://huggingface.co/Ultralytics/YOLOv8/blob/main/yolov8n.pt
    expected = '31e20dde3def09e2cf938c7be6fe23d9150bbbe503982af13345706515f2ef95' if args.mirror else None
    report['model'] = download(session, model_url, BACKEND / 'yolov8n.pt', expected)
    archive = DATASETS / 'coco128.zip'
    # Kaggle works through this host's configured proxy; keep its normal requests routing.
    report['dataset'] = download(requests.Session(), 'https://www.kaggle.com/api/v1/datasets/download/ultralytics/coco128', archive)
    with zipfile.ZipFile(archive) as zipped:
        names = zipped.namelist()
        destination = DATASETS if any(n.startswith('coco128/images/') for n in names) else DATASETS / 'coco128'
        for member in zipped.infolist():
            target = (destination / member.filename).resolve()
            if not target.is_relative_to(destination.resolve()):
                raise ValueError('Unsafe archive member')
        zipped.extractall(destination)
    images = list((DATASETS / 'coco128' / 'images' / 'train2017').glob('*.jpg'))
    assert len(images) == 128, f'Expected 128 images, got {len(images)}'
    report['images'] = len(images)
    font = Path(os.environ.get('WINDIR', 'C:/Windows')) / 'Fonts' / 'arial.ttf'
    if os.name == 'nt' and font.is_file():
        font_target = BACKEND / 'data' / 'ultralytics' / 'Arial.ttf'
        font_target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(font, font_target)
        report['local_font_cached'] = True
    (ROOT / 'docs' / 'training-assets.json').write_text(json.dumps(report, indent=2), encoding='utf-8')
    print('coco128: 128 images prepared.', flush=True)


if __name__ == '__main__':
    main()
