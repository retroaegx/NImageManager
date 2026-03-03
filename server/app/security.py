from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any

from jose import jwt
from passlib.context import CryptContext

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "server" / "data"
SECRET_PATH = DATA_DIR / "jwt_secret.key"

# NOTE:
# bcrypt backend can break depending on the platform/package versions.
# This project defaults to a pure-python hash to keep startup reliable.
pwd_ctx = CryptContext(
    schemes=["pbkdf2_sha256"],
    deprecated="auto",
)

ALGO = "HS256"


def _load_secret() -> str:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    if SECRET_PATH.exists():
        return SECRET_PATH.read_text(encoding="utf-8").strip()
    import secrets

    s = secrets.token_urlsafe(48)
    SECRET_PATH.write_text(s, encoding="utf-8")
    return s


_SECRET: str | None = None


def get_secret() -> str:
    global _SECRET
    if _SECRET is None:
        _SECRET = _load_secret()
    return _SECRET


def hash_password(pw: str) -> str:
    return pwd_ctx.hash(pw)


def verify_password(pw: str, hashed: str) -> bool:
    try:
        return pwd_ctx.verify(pw, hashed)
    except Exception:
        return False


def create_token(*, user_id: int, username: str, role: str) -> str:
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "username": username,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(days=30)).timestamp()),
    }
    return jwt.encode(payload, get_secret(), algorithm=ALGO)


def decode_token(token: str) -> dict[str, Any]:
    return jwt.decode(token, get_secret(), algorithms=[ALGO])
