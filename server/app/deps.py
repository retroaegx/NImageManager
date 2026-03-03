from __future__ import annotations

from fastapi import Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from .db import get_conn
from .security import decode_token

bearer = HTTPBearer(auto_error=False)

def _extract_token(request: Request, creds: HTTPAuthorizationCredentials | None) -> str | None:
    if creds and creds.credentials:
        return creds.credentials
    # for <img> / <a> etc: use httpOnly cookie
    return request.cookies.get("nai_token")

def get_user_optional(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict | None:
    token = _extract_token(request, creds)
    if not token:
        return None
    try:
        data = decode_token(token)
    except Exception:
        return None

    user_id = int(data.get("sub"))
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, username, role, disabled FROM users WHERE id=?",
            (user_id,),
        ).fetchone()
        if not row or int(row["disabled"]) == 1:
            return None
        return {"id": row["id"], "username": row["username"], "role": row["role"]}
    finally:
        conn.close()

def get_user(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(bearer),
) -> dict:
    user = get_user_optional(request, creds)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user

def require_admin(user: dict = Depends(get_user)) -> dict:
    if user.get("role") not in {"admin", "master"}:
        raise HTTPException(status_code=403, detail="Admin only")
    return user


def require_master(user: dict = Depends(get_user)) -> dict:
    if user.get("role") != "master":
        raise HTTPException(status_code=403, detail="Master only")
    return user
