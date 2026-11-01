"""One operator token for this local platform; never pass it to training containers."""
import os
import secrets
from pathlib import Path

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import config

bearer = HTTPBearer(auto_error=False)


def get_token():
    if config.API_TOKEN:
        return config.API_TOKEN
    path = Path(os.environ.get('TRAINPLANE_TOKEN_FILE', str(config.DATA / 'api-token')))
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        pass
    else:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
            stream.write(secrets.token_urlsafe(32))
    token = path.read_text(encoding='utf-8').strip()
    if not token:
        raise RuntimeError('访问令牌文件为空，请配置 TRAINPLANE_API_TOKEN 或重新生成 api-token')
    return token


def authorize(request: Request, credentials: HTTPAuthorizationCredentials | None = Depends(bearer)):
    if request.url.path == '/api/health':
        return
    if credentials is None or not secrets.compare_digest(credentials.credentials.encode('utf-8'), get_token().encode('utf-8')):
        raise HTTPException(401, '访问令牌无效或未提供，请重新登录', headers={'WWW-Authenticate': 'Bearer'})
