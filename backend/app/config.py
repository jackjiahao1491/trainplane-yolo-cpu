import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.getenv('TRAINPLANE_DATA_DIR', str(ROOT / 'data'))).resolve()
REDIS_URL = os.getenv('REDIS_URL', 'redis://127.0.0.1:6379/0')
RAY_ADDRESS = os.getenv('RAY_ADDRESS', 'http://127.0.0.1:8265')
TOTAL_CPUS = int(os.getenv('TOTAL_CPUS', '2'))
HEARTBEAT_TIMEOUT = float(os.getenv('HEARTBEAT_TIMEOUT', '15'))
STARTUP_GRACE = float(os.getenv('HEARTBEAT_STARTUP_GRACE', '90'))
INTERVAL = float(os.getenv('SCHEDULER_INTERVAL', '1'))
DEFAULT_COMMAND = 'python -m app.train --epochs 1 --imgsz 320 --batch 1'
TOTAL_MEMORY_MB = int(os.getenv('TOTAL_MEMORY_MB', '2048'))
DEFAULT_MEMORY_MB = int(os.getenv('DEFAULT_MEMORY_MB', '1024'))
AGING_INTERVAL_SECONDS = float(os.getenv('AGING_INTERVAL_SECONDS', '60'))
BACKFILL_MAX_BYPASS = int(os.getenv('BACKFILL_MAX_BYPASS', '3'))
EXECUTION_MODE = os.getenv('EXECUTION_MODE', 'process')
API_TOKEN = os.getenv('TRAINPLANE_API_TOKEN', '')

