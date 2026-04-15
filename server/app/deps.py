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
        try:
            row = conn.execute(
                """
                SELECT u.id, u.username, u.role, u.disabled,
                       COALESCE(us.share_works,0) AS share_works,
                       COALESCE(us.share_bookmarks,0) AS share_bookmarks,
                       COALESCE(NULLIF(TRIM(us.ui_language),''),'auto') AS ui_language
                FROM users u
                LEFT JOIN user_settings us ON us.user_id = u.id
                WHERE u.id=?
                """,
                (user_id,),
            ).fetchone()
            ui_language = str(row["ui_language"] or "auto") if row else "auto"
        except Exception as exc:
            if "ui_language" not in str(exc):
                raise
            row = conn.execute(
                """
                SELECT u.id, u.username, u.role, u.disabled,
                       COALESCE(us.share_works,0) AS share_works,
                       COALESCE(us.share_bookmarks,0) AS share_bookmarks
                FROM users u
                LEFT JOIN user_settings us ON us.user_id = u.id
                WHERE u.id=?
                """,
                (user_id,),
            ).fetchone()
            ui_language = "auto"
        if not row or int(row["disabled"]) == 1:
            return None
        return {
            "id": row["id"],
            "username": row["username"],
            "role": row["role"],
            "share_works": int(row["share_works"] or 0),
            "share_bookmarks": int(row["share_bookmarks"] or 0),
            "ui_language": ui_language,
        }
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
