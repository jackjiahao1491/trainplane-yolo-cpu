import sqlite3
from contextlib import contextmanager
from . import config


def initialize():
    config.DATA.mkdir(parents=True, exist_ok=True)
    with connection() as db:
        db.execute('PRAGMA journal_mode=WAL')
        db.execute('''CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, command TEXT NOT NULL,
            cpu INTEGER NOT NULL, timeout_seconds INTEGER NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'pending', created_at REAL NOT NULL,
            started_at REAL, finished_at REAL, heartbeat_at REAL,
            exit_code INTEGER, error TEXT,
            ray_submission_id TEXT UNIQUE, reserved INTEGER NOT NULL DEFAULT 0,
            submitted INTEGER NOT NULL DEFAULT 0, stop_reason TEXT,
            running_seen_at REAL
        )''')
        db.execute('CREATE INDEX IF NOT EXISTS jobs_queue ON jobs(status, priority DESC, created_at)')


@contextmanager
def connection():
    db = sqlite3.connect(config.DATA / 'platform.sqlite3', timeout=10)
    db.row_factory = sqlite3.Row
    db.execute('PRAGMA busy_timeout=10000')
    try:
        yield db
        db.commit()
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def get(job_id):
    with connection() as db:
        row = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
        return dict(row) if row else None


def update(job_id, **fields):
    with connection() as db:
        db.execute('UPDATE jobs SET ' + ','.join(f'{k}=?' for k in fields) + ' WHERE id=?',
                   (*fields.values(), job_id))

