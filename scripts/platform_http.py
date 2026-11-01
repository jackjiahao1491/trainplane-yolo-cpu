"""Shared local operator authentication for the command-line acceptance scripts."""
import os
from pathlib import Path


def auth_headers():
    root = Path(__file__).resolve().parents[1]
    values = {}
    path = root / '.env'
    if path.exists():
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if '=' in line and not line.lstrip().startswith('#'):
                key, value = line.split('=', 1)
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in '\"\'':
                    value = value[1:-1]
                values[key.strip()] = value
    token = os.environ.get('TRAINPLANE_API_TOKEN') or values.get('TRAINPLANE_API_TOKEN')
    if not token:
        data = os.environ.get('TRAINPLANE_DATA_DIR') or values.get('TRAINPLANE_DATA_DIR')
        folder = Path(data) if data else root / 'backend' / 'data'
        explicit = os.environ.get('TRAINPLANE_TOKEN_FILE')
        path = Path(explicit) if explicit else root / 'backend' / '.auth' / 'api-token'
        if not path.exists() and not explicit:
            path = folder / 'api-token'
        token = path.read_text(encoding='utf-8').strip()
    return {'Authorization': 'Bearer ' + token}
