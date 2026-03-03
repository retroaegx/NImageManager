from __future__ import annotations

import io
import re
import os
import json
import sqlite3
import csv
import math
import threading
import fnmatch
import time
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile, Request
from fastapi.responses import Response, StreamingResponse, FileResponse
from pydantic import BaseModel

from .db import get_conn, ORIGINALS_DIR, ASSETS_DIR
from .deps import get_user, require_admin, require_master
from .security import create_token, verify_password, hash_password
from .services.metadata_extract import extract_novelai_metadata
from .services.tag_parser import parse_tag_list, normalize_tag, main_sig_hash
from .services.derivatives import make_webp_derivative, make_avif_derivative, avif_available, avif_probe_error
from .services import stats as stats_service

api_router = APIRouter()


# ---- quality tag cache (exact + wildcard patterns) ----
#
# Quality tags are used only for UI grouping (artist/quality/character/other).
# Avoid 1 SQL query per tag by caching the small quality_tags set in memory.
#
# Wildcards:
# - '*' is treated as a glob wildcard (fnmatch), with a fast path for trailing '*'.
# - Patterns are matched against canonical (normalized) tags.

_QUALITY_LOCK = threading.Lock()
_QUALITY_LOADED = False
_QUALITY_EXACT: set[str] = set()
_QUALITY_PATTERNS: list[str] = []


# ---- maintenance background ops (reparse/rebuild) ----
#
# Requirements:
# - Reparse runs in background, keeps progressing even if UI page is closed.
# - Progress can be polled anytime via state endpoints.
# - Reparse and rebuild must be mutually exclusive.

_MAINT_LOCK = threading.Lock()
_REPARSE_BG_THREAD: threading.Thread | None = None
_REBUILD_BG_THREAD: threading.Thread | None = None

_HB_MAX_AGE_SEC = 180


# ---- public base URL (Cloudflare Tunnel) ----
#
# Password setup/reset URLs must be shareable; build them from the server-known
# Cloudflare Tunnel URL when available.
#
# Prefer stable (named tunnel) hostnames when the user already configured a
# cloudflared ingress that routes to this app port.
# Fallback order:
#   1) NAI_IM_PUBLIC_BASE_URL (explicit override)
#   2) named tunnel hostname discovered from cloudflared config ingress
#   3) quick tunnel URL parsed from the log
#   4) request headers (x-forwarded-*/host)
#   5) http://localhost:32287

_PUBLIC_URL_LOCK = threading.Lock()
_PUBLIC_URL_CACHE: str | None = None
_PUBLIC_URL_MTIME: float = 0.0

_TRYCLOUDFLARE_RE = re.compile(r"(https://[a-z0-9\-]+\.trycloudflare\.com)", re.IGNORECASE)
_INGRESS_KEY_RE = re.compile(r"^ingress\s*:\s*$", re.IGNORECASE)
_KV_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_\-]*)\s*:\s*(.*)$")


def _strip_yaml_scalar(v: str) -> str:
    v = (v or "").strip()
    if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
        v = v[1:-1].strip()
    return v


def _service_matches_port(service: str, port: int) -> bool:
    s = _strip_yaml_scalar(service).strip().lower()
    if not s:
        return False
    if f":{port}" not in s:
        return False
    if "localhost" in s or "127.0.0.1" in s or "0.0.0.0" in s:
        return True
    return False


def _parse_cloudflared_ingress_hostname(cfg_text: str, port: int) -> str | None:
    """Extract the first hostname that routes to http://localhost:<port>."""
    in_ingress = False
    cur: dict[str, str] = {}

    def flush_current() -> str | None:
        if not cur:
            return None
        host = (cur.get("hostname") or "").strip()
        svc = (cur.get("service") or "").strip()
        if host and _service_matches_port(svc, port):
            return host
        return None

    for raw in (cfg_text or "").splitlines():
        line = raw.rstrip("\n\r")
        s = line.strip()
        if not s or s.startswith("#"):
            continue

        if not in_ingress:
            if _INGRESS_KEY_RE.match(s):
                in_ingress = True
            continue

        # End of ingress block: a new top-level key starts.
        if (len(line) - len(line.lstrip())) == 0 and not s.startswith("-") and ":" in s:
            break

        if s.startswith("-"):
            hit = flush_current()
            if hit:
                return hit
            cur = {}
            after = s[1:].strip()
            m = _KV_RE.match(after)
            if m:
                cur[m.group(1).lower()] = _strip_yaml_scalar(m.group(2))
            continue

        m = _KV_RE.match(s)
        if not m:
            continue
        cur[m.group(1).lower()] = _strip_yaml_scalar(m.group(2))

    return flush_current()


def _candidate_cloudflared_config_paths() -> list[Path]:
    paths: list[Path] = []

    cfg_env = (os.environ.get("NAI_IM_CLOUDFLARED_CONFIG") or "").strip()
    if cfg_env:
        paths.append(Path(cfg_env))

    # local cwd (useful when the user runs cloudflared from project dir)
    for name in ("config.yml", "config.yaml"):
        paths.append(Path.cwd() / name)

    home = Path.home()
    for d in (home / ".cloudflared", home / ".cloudflare-warp", home / "cloudflare-warp"):
        for name in ("config.yml", "config.yaml"):
            paths.append(d / name)

    if os.name == "nt":
        program_data = Path(os.environ.get("ProgramData", r"C:\\ProgramData"))
        for name in ("config.yml", "config.yaml"):
            paths.append(program_data / "cloudflared" / name)

    # de-dup
    out: list[Path] = []
    seen = set()
    for p in paths:
        try:
            rp = str(p.resolve())
        except Exception:
            rp = str(p)
        if rp in seen:
            continue
        seen.add(rp)
        out.append(p)
    return out


def _read_named_tunnel_url_from_config(port: int = 32287) -> str | None:
    try:
        for cfg in _candidate_cloudflared_config_paths():
            if not cfg.exists() or cfg.stat().st_size <= 0:
                continue
            txt = cfg.read_text(encoding="utf-8", errors="ignore")
            host = _parse_cloudflared_ingress_hostname(txt, port)
            if not host:
                continue
            return f"https://{host}".rstrip("/")
        return None
    except Exception:
        return None

def _read_quick_tunnel_url_from_log() -> str | None:
    # The installer writes this file when Quick Tunnel is enabled.
    try:
        log_path = (Path(__file__).resolve().parents[2] / "server" / "data" / "cloudflared_quick_tunnel.log")
        if not log_path.exists():
            return None
        st = log_path.stat()
        mtime = float(st.st_mtime)
        global _PUBLIC_URL_CACHE, _PUBLIC_URL_MTIME
        with _PUBLIC_URL_LOCK:
            if _PUBLIC_URL_CACHE and _PUBLIC_URL_MTIME == mtime:
                return _PUBLIC_URL_CACHE
            txt = log_path.read_text(encoding="utf-8", errors="ignore")
            urls = _TRYCLOUDFLARE_RE.findall(txt)
            url = None
            for u in urls:
                if "api.trycloudflare.com" in u.lower():
                    continue
                url = u
            _PUBLIC_URL_CACHE = url
            _PUBLIC_URL_MTIME = mtime
            return url
    except Exception:
        return None

def _public_base_url(request: Request | None = None) -> str:
    env = (os.environ.get("NAI_IM_PUBLIC_BASE_URL") or "").strip()
    if env:
        return env.rstrip("/")
    named = _read_named_tunnel_url_from_config(32287)
    if named:
        return named.rstrip("/")
    url = _read_quick_tunnel_url_from_log()
    if url:
        return url.rstrip("/")
    if request is not None:
        proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "http").split(",")[0].strip()
        host = (request.headers.get("x-forwarded-host") or request.headers.get("host") or "").split(",")[0].strip()
        if host:
            return f"{proto}://{host}".rstrip("/")
    return "http://localhost:32287"

def _abs_url(path: str, request: Request | None = None) -> str:
    # path: "/set-password.html?token=..."
    base = _public_base_url(request)
    if not path.startswith("/"):
        path = "/" + path
    return f"{base}{path}"


def _hb_now() -> int:
    return int(time.time())


def _hb_active(running_flag: int, hb_ts: int) -> bool:
    if int(running_flag or 0) != 1:
        return False
    if int(hb_ts or 0) <= 0:
        return False
    return (_hb_now() - int(hb_ts)) <= _HB_MAX_AGE_SEC


def _load_quality_from_asset() -> tuple[set[str], list[str]]:
    """Load quality tags from bundled asset CSV.

    Asset is treated as source-of-truth for UI grouping. This also acts as a
    fallback when an existing DB has not yet been bootstrapped with quality_tags.
    """
    exact: set[str] = set()
    patterns: list[str] = []
    try:
        qfile = ASSETS_DIR / "tags" / "extra-quality-tags.csv"
        if not qfile.exists():
            return exact, patterns
        with qfile.open("r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row:
                    continue
                t = (row[0] or "").strip()
                if not t:
                    continue
                tn = normalize_tag(t)
                if not tn:
                    continue
                # Accept patterns like "year *" (normalized to "year_*") as "year*".
                if tn.endswith("_*"):
                    tn = tn[:-2] + "*"
                if "*" in tn:
                    patterns.append(tn)
                else:
                    exact.add(tn)
    except Exception:
        # best-effort
        pass
    return exact, patterns


def _ensure_quality_cache(conn: sqlite3.Connection) -> None:
    global _QUALITY_LOADED, _QUALITY_EXACT, _QUALITY_PATTERNS
    if _QUALITY_LOADED:
        return
    with _QUALITY_LOCK:
        if _QUALITY_LOADED:
            return
        exact: set[str] = set()
        patterns: list[str] = []
        try:
            rows = conn.execute("SELECT tag FROM quality_tags").fetchall()
            for r in rows:
                t = (r[0] if isinstance(r, tuple) else r.get("tag"))
                t = (t or "").strip()
                if not t:
                    continue
                tn = normalize_tag(t)
                if not tn:
                    continue
                # Accept patterns like "year *" (normalized to "year_*") as "year*".
                if tn.endswith("_*"):
                    tn = tn[:-2] + "*"
                if "*" in tn:
                    patterns.append(tn)
                else:
                    exact.add(tn)
        except Exception:
            exact = set()
            patterns = []
        # Union with asset tags (source-of-truth + fallback when DB isn't bootstrapped yet)
        ax, ap = _load_quality_from_asset()
        exact |= ax
        for p in ap:
            if p not in patterns:
                patterns.append(p)
        _QUALITY_EXACT = exact
        _QUALITY_PATTERNS = patterns
        _QUALITY_LOADED = True


def _is_quality_tag(conn: sqlite3.Connection, canonical: str) -> bool:
    if not canonical:
        return False
    _ensure_quality_cache(conn)
    if canonical in _QUALITY_EXACT:
        return True
    if not _QUALITY_PATTERNS:
        return False
    for pat in _QUALITY_PATTERNS:
        # Fast-path for prefix patterns like "year_*".
        if pat.endswith("*") and pat.count("*") == 1:
            if canonical.startswith(pat[:-1]):
                return True
            continue
        try:
            if fnmatch.fnmatchcase(canonical, pat):
                return True
        except Exception:
            # ignore bad patterns
            continue
    return False


def _kv_get(conn: sqlite3.Connection, key: str) -> str | None:
    try:
        r = conn.execute("SELECT value FROM admin_kv WHERE key=?", (key,)).fetchone()
        if not r:
            return None
        return str(r[0] if isinstance(r, tuple) else r["value"])
    except Exception:
        return None


def _kv_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    try:
        conn.execute(
            """
            INSERT INTO admin_kv(key, value, updated_at)
            VALUES (?,?,datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=datetime('now')
            """,
            (key, value),
        )
    except Exception:
        # Admin KV is best-effort; core flows must still work.
        return


def _create_run(conn: sqlite3.Connection, kind: str, params: dict) -> int:
    try:
        pj = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)
        cur = conn.execute(
            "INSERT INTO maintenance_runs(kind, params_json, status) VALUES (?,?, 'running')",
            (kind, pj),
        )
        return int(cur.lastrowid)
    except Exception:
        return 0


def _run_add_counts(conn: sqlite3.Connection, run_id: int, *, last_image_id: int, processed: int, updated: int, error_count: int, done: bool) -> None:
    if run_id <= 0:
        return
    try:
        conn.execute(
            """
            UPDATE maintenance_runs
            SET updated_at=datetime('now'),
                last_image_id=CASE WHEN ? > last_image_id THEN ? ELSE last_image_id END,
                processed=processed + ?,
                updated=updated + ?,
                error_count=error_count + ?,
                status=CASE WHEN ? THEN 'done' ELSE status END
            WHERE id=?
            """,
            (int(last_image_id or 0), int(last_image_id or 0), int(processed or 0), int(updated or 0), int(error_count or 0), 1 if done else 0, int(run_id)),
        )
    except Exception:
        return


def _run_log_error(conn: sqlite3.Connection, run_id: int, image_id: int | None, stage: str, err: str) -> None:
    if run_id <= 0:
        return
    try:
        conn.execute(
            "INSERT INTO maintenance_errors(run_id, image_id, stage, error) VALUES (?,?,?,?)",
            (int(run_id), int(image_id) if image_id is not None else None, (stage or "")[:32], (err or "")[:512]),
        )
        # simple retention (per run)
        conn.execute(
            """
            DELETE FROM maintenance_errors
            WHERE run_id=? AND id NOT IN (
              SELECT id FROM maintenance_errors WHERE run_id=? ORDER BY id DESC LIMIT 2000
            )
            """,
            (int(run_id), int(run_id)),
        )
    except Exception:
        return


def _fetch_run(conn: sqlite3.Connection, run_id: int) -> dict | None:
    if int(run_id or 0) <= 0:
        return None
    try:
        r = conn.execute(
            "SELECT id, kind, params_json, status, created_at, updated_at, last_image_id, processed, updated, error_count FROM maintenance_runs WHERE id=?",
            (int(run_id),),
        ).fetchone()
        if not r:
            return None
        if isinstance(r, tuple):
            return {
                "id": int(r[0]),
                "kind": r[1],
                "status": r[3],
                "created_at": r[4],
                "updated_at": r[5],
                "last_image_id": int(r[6] or 0),
                "processed": int(r[7] or 0),
                "updated": int(r[8] or 0),
                "error_count": int(r[9] or 0),
            }
        return {
            "id": int(r["id"]),
            "kind": r["kind"],
            "status": r["status"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "last_image_id": int(r["last_image_id"] or 0),
            "processed": int(r["processed"] or 0),
            "updated": int(r["updated"] or 0),
            "error_count": int(r["error_count"] or 0),
        }
    except Exception:
        return None


def _set_run_status(conn: sqlite3.Connection, run_id: int, status: str) -> None:
    if int(run_id or 0) <= 0:
        return
    if status not in {"running", "done", "stopped"}:
        status = "stopped"
    try:
        conn.execute(
            "UPDATE maintenance_runs SET updated_at=datetime('now'), status=? WHERE id=?",
            (status, int(run_id)),
        )
    except Exception:
        return

def _table_has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    try:
        rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
        for r in rows:
            name = r[1] if isinstance(r, tuple) else r["name"]
            if name == col:
                return True
        return False
    except Exception:
        return False


def _iter_tag_rows(tag_rows):
    """Yield normalized tag rows.

    tag_rows historically had 8 fields:
      (canonical, tag_text, tag_raw, cat, etype, brace, numw, group)

    Newer versions may append:
      - src_mask as the 9th field (int)
      - seq as the 10th field (int, prompt order)
    """
    for row in tag_rows or []:
        try:
            canonical, tag_text, tag_raw, cat, etype, brace, numw, group = row[:8]
            src_mask = int(row[8]) if (len(row) > 8 and row[8] is not None) else 1
            seq = int(row[9]) if (len(row) > 9 and row[9] is not None) else 0
        except Exception:
            continue
        yield canonical, tag_text, tag_raw, cat, etype, brace, numw, group, src_mask, seq



class LoginReq(BaseModel):
    username: str
    password: str


@api_router.get("/auth/setup_status")
def setup_status():
    """Return whether the instance has any users configured."""
    conn = get_conn()
    try:
        n = int(conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        return {"needs_setup": n == 0}
    finally:
        conn.close()


class SetupMasterReq(BaseModel):
    username: str
    password: str
    password2: str


@api_router.post("/auth/setup_master")
def setup_master(req: SetupMasterReq, response: Response):
    """Create the first (master) admin user. Only allowed when no users exist."""
    username = (req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")
    if not req.password or not req.password2:
        raise HTTPException(status_code=400, detail="password required")
    if req.password != req.password2:
        raise HTTPException(status_code=400, detail="password mismatch")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="password too short")

    conn = get_conn()
    try:
        n = int(conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
        if n != 0:
            raise HTTPException(status_code=409, detail="already initialized")

        try:
            conn.execute(
                "INSERT INTO users(username, password_hash, role, must_set_password, pw_set_at) VALUES (?,?,?,?,datetime('now'))",
                (username, hash_password(req.password), "master", 0),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="username already exists")

        row = conn.execute(
            "SELECT id, username, role FROM users WHERE username=?",
            (username,),
        ).fetchone()
        token = create_token(user_id=int(row["id"]), username=row["username"], role=row["role"])
        response.set_cookie(
            key="nai_token",
            value=token,
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,
            path="/",
        )
        return {"ok": True, "token": token, "user": {"id": row["id"], "username": row["username"], "role": row["role"]}}
    finally:
        conn.close()

@api_router.post("/auth/login")
def login(req: LoginReq, response: Response):
    conn = get_conn()
    try:
        row = conn.execute("SELECT id, username, password_hash, role, disabled FROM users WHERE username=?", (req.username,)).fetchone()
        if not row or int(row["disabled"]) == 1:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        if not verify_password(req.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid credentials")
        token = create_token(user_id=int(row["id"]), username=row["username"], role=row["role"])
        # Cookie-based auth is required for <img> tags and direct downloads.
        response.set_cookie(
            key="nai_token",
            value=token,
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,
            path="/",
        )
        return {"token": token, "user": {"id": row["id"], "username": row["username"], "role": row["role"]}}
    finally:
        conn.close()

@api_router.get("/me")
def me(user: dict = Depends(get_user)):
    return user


@api_router.post("/auth/logout")
def logout(response: Response):
    response.delete_cookie("nai_token", path="/")
    return {"ok": True}


@api_router.post("/auth/password_link")
def self_password_link(request: Request, user: dict = Depends(get_user)):
    """Issue a password reset URL for the current user (does not change the password)."""
    import secrets
    from datetime import datetime, timedelta, timezone

    token = secrets.token_urlsafe(32)
    exp = datetime.now(timezone.utc) + timedelta(days=7)
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO password_tokens(token, user_id, kind, created_by, expires_at) VALUES (?,?,?,?,?)",
            (token, int(user["id"]), "reset", int(user["id"]), exp.isoformat()),
        )
        conn.commit()
        return {"ok": True, "reset_url": _abs_url(f"/set-password.html?token={token}", request)}
    finally:
        conn.close()

class CreateUserReq(BaseModel):
    username: str
    role: str

@api_router.post("/admin/users")
def admin_create_user(request: Request, req: CreateUserReq, admin: dict = Depends(require_admin)):
    """Create a user and return a password-setup URL.

    - Admins can create/delete normal users.
    - Only master can create/delete admin users.
    """
    if req.role not in {"admin", "user"}:
        raise HTTPException(status_code=400, detail="role must be admin/user")
    if req.role == "admin" and admin.get("role") != "master":
        raise HTTPException(status_code=403, detail="master required to create admin")
    username = (req.username or "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="username required")
    conn = get_conn()
    try:
        import secrets
        from datetime import datetime, timedelta, timezone

        try:
            # Create with a random placeholder hash; user sets their own password via URL.
            placeholder = secrets.token_urlsafe(32)
            conn.execute(
                "INSERT INTO users(username, password_hash, role, must_set_password) VALUES (?,?,?,1)",
                (username, hash_password(placeholder), req.role),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="username already exists")

        urow = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
        user_id = int(urow["id"])
        token = secrets.token_urlsafe(32)
        exp = datetime.now(timezone.utc) + timedelta(days=14)
        conn.execute(
            "INSERT INTO password_tokens(token, user_id, kind, created_by, expires_at) VALUES (?,?,?,?,?)",
            (token, user_id, "setup", int(admin["id"]), exp.isoformat()),
        )
        conn.commit()

        return {"ok": True, "setup_url": _abs_url(f"/set-password.html?token={token}", request)}
    finally:
        conn.close()


@api_router.get("/admin/users")
def admin_list_users(_admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT id, username, role, disabled, created_at, must_set_password, pw_set_at FROM users ORDER BY id ASC"
        ).fetchall()
        return [
            {
                "id": int(r["id"]),
                "username": r["username"],
                "role": r["role"],
                "disabled": int(r["disabled"] or 0),
                "created_at": r["created_at"],
                "must_set_password": int(r["must_set_password"] or 0),
                "pw_set_at": r["pw_set_at"],
            }
            for r in rows
        ]
    finally:
        conn.close()


class UpdateUserReq(BaseModel):
    role: str | None = None
    disabled: int | None = None
    password: str | None = None  # deprecated; kept for compatibility


@api_router.post("/admin/users/{user_id}")
def admin_update_user(user_id: int, req: UpdateUserReq, admin: dict = Depends(require_admin)):
    if req.password is not None:
        raise HTTPException(status_code=400, detail="password cannot be set here; use reset URL")

    if req.role is not None and req.role not in {"admin", "user"}:
        raise HTTPException(status_code=400, detail="role must be admin/user")
    if req.disabled is not None and int(req.disabled) not in {0, 1}:
        raise HTTPException(status_code=400, detail="disabled must be 0/1")
    if int(user_id) == int(admin["id"]) and req.disabled is not None and int(req.disabled) == 1:
        raise HTTPException(status_code=400, detail="cannot disable yourself")

    conn = get_conn()
    try:
        row = conn.execute("SELECT id, role FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")

        target_role = row["role"]
        if target_role == "master":
            if req.role is not None:
                raise HTTPException(status_code=400, detail="cannot change master role")
            if req.disabled is not None:
                raise HTTPException(status_code=400, detail="cannot disable master")

        # Role changes are master-only.
        if req.role is not None and admin.get("role") != "master":
            raise HTTPException(status_code=403, detail="master required to change role")

        # Only master can modify admin accounts (except self disable is already blocked).
        if target_role == "admin" and admin.get("role") != "master" and int(user_id) != int(admin["id"]):
            if req.disabled is not None:
                raise HTTPException(status_code=403, detail="master required to modify admin")

        if req.role is not None:
            conn.execute("UPDATE users SET role=? WHERE id=?", (req.role, user_id))
        if req.disabled is not None:
            conn.execute("UPDATE users SET disabled=? WHERE id=?", (int(req.disabled), user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.delete("/admin/users/{user_id}")
def admin_delete_user(user_id: int, admin: dict = Depends(require_admin)):
    if int(user_id) == int(admin["id"]):
        raise HTTPException(status_code=400, detail="cannot delete yourself")
    conn = get_conn()
    try:
        row = conn.execute("SELECT id, role FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        role = row["role"]
        if role == "master":
            raise HTTPException(status_code=400, detail="cannot delete master")
        if role == "admin" and admin.get("role") != "master":
            raise HTTPException(status_code=403, detail="master required to delete admin")

        conn.execute("DELETE FROM users WHERE id=?", (user_id,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.post("/admin/users/{user_id}/password_link")
def admin_issue_password_link(request: Request, user_id: int, admin: dict = Depends(get_user)):
    """Issue a password reset URL.

    Rules:
      - Any user can issue a link for themselves.
      - Admins can issue links for normal users.
      - Only master can issue links for admin users.
      - No one can issue a link for the master user except the master themselves.
    """
    conn = get_conn()
    try:
        row = conn.execute("SELECT id, role FROM users WHERE id=?", (user_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        target_role = row["role"]

        is_self = int(admin["id"]) == int(user_id)
        is_admin = admin.get("role") in {"admin", "master"}
        is_master = admin.get("role") == "master"

        if target_role == "master" and not is_self:
            raise HTTPException(status_code=403, detail="not allowed")

        if not is_self:
            if not is_admin:
                raise HTTPException(status_code=403, detail="not allowed")
            if target_role == "admin" and not is_master:
                raise HTTPException(status_code=403, detail="master required")

        import secrets
        from datetime import datetime, timedelta, timezone

        token = secrets.token_urlsafe(32)
        exp = datetime.now(timezone.utc) + timedelta(days=7)
        conn.execute(
            "INSERT INTO password_tokens(token, user_id, kind, created_by, expires_at) VALUES (?,?,?,?,?)",
            (token, int(user_id), "reset", int(admin["id"]), exp.isoformat()),
        )
        conn.commit()
        return {"ok": True, "reset_url": _abs_url(f"/set-password.html?token={token}", request)}
    finally:
        conn.close()


@api_router.get("/auth/password_tokens/info")
def password_token_info(token: str):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT pt.token, pt.kind, pt.expires_at, pt.used_at, u.username "
            "FROM password_tokens pt JOIN users u ON u.id=pt.user_id WHERE pt.token=?",
            (token,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if row["used_at"]:
            return {"ok": False, "status": "used", "kind": row["kind"], "username": row["username"]}
        # expiry check: store isoformat; lexicographic compares ok for UTC, but parse safely.
        try:
            from datetime import datetime, timezone

            exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > exp:
                return {"ok": False, "status": "expired", "kind": row["kind"], "username": row["username"]}
        except Exception:
            pass
        return {"ok": True, "status": "ok", "kind": row["kind"], "username": row["username"], "expires_at": row["expires_at"]}
    finally:
        conn.close()


class ConsumePasswordTokenReq(BaseModel):
    token: str
    password: str
    password2: str


@api_router.post("/auth/password_tokens/consume")
def consume_password_token(req: ConsumePasswordTokenReq):
    if not req.token:
        raise HTTPException(status_code=400, detail="token required")
    if not req.password or not req.password2:
        raise HTTPException(status_code=400, detail="password required")
    if req.password != req.password2:
        raise HTTPException(status_code=400, detail="password mismatch")
    if len(req.password) < 6:
        raise HTTPException(status_code=400, detail="password too short")

    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT token, user_id, kind, expires_at, used_at FROM password_tokens WHERE token=?",
            (req.token,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if row["used_at"]:
            raise HTTPException(status_code=409, detail="token already used")

        # expiry
        try:
            from datetime import datetime, timezone

            exp = datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00"))
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > exp:
                raise HTTPException(status_code=410, detail="token expired")
        except HTTPException:
            raise
        except Exception:
            pass

        uid = int(row["user_id"])
        conn.execute(
            "UPDATE users SET password_hash=?, must_set_password=0, pw_set_at=datetime('now') WHERE id=?",
            (hash_password(req.password), uid),
        )
        conn.execute(
            "UPDATE password_tokens SET used_at=datetime('now') WHERE token=?",
            (req.token,),
        )
        # keep table tidy: invalidate other setup tokens for same user
        try:
            conn.execute(
                "UPDATE password_tokens SET used_at=datetime('now') WHERE user_id=? AND kind='setup' AND used_at IS NULL",
                (uid,),
            )
        except Exception:
            pass
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


class ReparseReq(BaseModel):
    image_id: int | None = None
    after_id: int = 0
    limit: int = 50
    date_from: str | None = None  # YYYY-MM-DD
    date_to: str | None = None    # YYYY-MM-DD
    only_missing: int = 0
    # Targeted selector: images that look like NovelAI 4.5 in existing DB rows
    # (software/model_name/metadata_raw contains "4.5") AND are missing major fields.
    # This is for operators to apply newly improved metadata extraction to old rows,
    # without reparsing the entire library.
    only_nai45_missing: int = 0
    rebuild_stats: int = 0
    recompute_dedup: int = 0


class ReparseSkipReq(BaseModel):
    image_id: int
    skip: int = 1


class ReparseOneReq(BaseModel):
    image_id: int
    clear_skip: int = 0


def _safe_basename(name: str) -> str:
    # Prevent path traversal / weird separators. Keep Unicode (Windows-safe) but strip control chars.
    base = os.path.basename(name or "upload")
    base = base.replace("\\", "_").replace("/", "_").replace(":", "_")
    base = "".join(ch if (ch >= " " and ch != "\u007f") else "_" for ch in base)
    base = base.strip() or "upload"
    return base[:180]


def _write_original_to_disk(
    *, image_id: int, original_filename: str, ext: str | None, sha256: str | None, raw: bytes
) -> str:
    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    base = _safe_basename(original_filename)
    ext0 = (ext or "").strip(".").lower()
    if ext0 and not base.lower().endswith("." + ext0):
        # If the original name has a different/ext-less suffix, keep original name and append ext.
        if "." not in base:
            base = base + "." + ext0
        # else: respect original suffix (do not rewrite); we store ext separately in DB.

    # Always prefix with image_id to avoid collisions.
    fn = f"{image_id}_{base}"
    path = ORIGINALS_DIR / fn
    if path.exists():
        # Very unlikely (id is unique), but be safe.
        suf = (sha256 or "")[:10] or str(int(datetime.now(timezone.utc).timestamp()))
        stem, dot, sx = fn.rpartition(".")
        if dot:
            fn = f"{stem}_{suf}.{sx}"
        else:
            fn = f"{fn}_{suf}"
        path = ORIGINALS_DIR / fn

    tmp = str(path) + ".tmp"
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, str(path))
    return str(path)


def _ensure_original_on_disk(conn: sqlite3.Connection, image_id: int) -> str | None:
    row = conn.execute(
        "SELECT image_files.disk_path AS p, image_files.bytes AS b, images.original_filename AS fn, images.ext AS ext, images.sha256 AS sha "
        "FROM image_files JOIN images ON images.id=image_files.image_id WHERE image_files.image_id=?",
        (image_id,),
    ).fetchone()
    if not row:
        return None
    p = row["p"]
    if p and os.path.exists(p):
        return str(p)

    b = row["b"]
    if b is None:
        return None
    if isinstance(b, memoryview):
        b = b.tobytes()

    try:
        new_p = _write_original_to_disk(
            image_id=image_id,
            original_filename=row["fn"] or f"{image_id}.bin",
            ext=row["ext"],
            sha256=row["sha"],
            raw=b,
        )
        conn.execute(
            "UPDATE image_files SET disk_path=?, size=?, bytes=NULL WHERE image_id=?",
            (new_p, int(len(b)), image_id),
        )
        return new_p
    except Exception:
        return None


def _read_image_bytes(conn: sqlite3.Connection, image_id: int) -> bytes | None:
    row = conn.execute(
        "SELECT disk_path, bytes FROM image_files WHERE image_id=?",
        (image_id,),
    ).fetchone()
    if not row:
        return None
    p = row["disk_path"]
    if p and os.path.exists(p):
        try:
            with open(p, "rb") as f:
                return f.read()
        except Exception:
            pass

    # Fallback: export bytes to disk (and return bytes).
    b = row["bytes"]
    if b is None:
        # maybe this row was migrated but not exported yet.
        p2 = _ensure_original_on_disk(conn, image_id)
        if p2 and os.path.exists(p2):
            with open(p2, "rb") as f:
                return f.read()
        return None
    if isinstance(b, memoryview):
        b = b.tobytes()
    _ensure_original_on_disk(conn, image_id)
    return b


def _reparse_one(conn: sqlite3.Connection, image_id: int) -> dict:
    """Re-extract metadata and regenerate tags for an existing image."""
    img = conn.execute(
        """
        SELECT id, ext, original_filename, file_mtime_utc, uploader_user_id, main_sig_hash,
               software
        FROM images
        WHERE id=?
        """,
        (image_id,),
    ).fetchone()

    old_sig = (img['main_sig_hash'] if img else None)
    if not img:
        return {"id": image_id, "ok": False, "error": "not found"}

    # old state (for incremental cache updates)
    old_software = (img["software"] if img else None)
    try:
        old_tags_rows = conn.execute(
            "SELECT DISTINCT tag_canonical FROM image_tags WHERE image_id=?",
            (image_id,),
        ).fetchall()
        old_tags = {str(r["tag_canonical"]) for r in old_tags_rows if r and r["tag_canonical"]}
    except Exception:
        old_tags = set()

    raw = _read_image_bytes(conn, image_id)
    if not raw:
        return {"id": image_id, "ok": False, "error": "missing bytes"}

    # Write to temp file because Pillow/EXIF loaders are file-path friendly.
    import tempfile
    tmp_path = None
    try:
        suffix = ("." + (img["ext"] or "bin")).lower()
        fd, p = tempfile.mkstemp(prefix="nai_reparse_", suffix=suffix)
        os.close(fd)
        tmp_path = p
        with open(tmp_path, "wb") as f:
            f.write(raw)

        meta = extract_novelai_metadata(tmp_path)
        if not meta:
            return {"id": image_id, "ok": False, "error": "no metadata"}

        software = meta.software
        model = meta.model
        prompt_pos = meta.prompt
        prompt_neg = meta.negative
        prompt_char = meta.character_prompt
        params_json = json.dumps(meta.params, ensure_ascii=False) if meta.params else None
        potion_raw = json.dumps(meta.potion, ensure_ascii=False).encode("utf-8") if meta.potion else None
        has_potion = 1 if potion_raw else 0
        metadata_raw = None
        try:
            metadata_raw = json.dumps({"info": meta.raw, "json": meta.raw_json_str}, ensure_ascii=False)[:65535]
        except Exception:
            metadata_raw = None

        # tags (main / character are tracked separately via src_mask)
        parsed_tags = parse_tag_list(prompt_pos or "")
        parsed_char_tags = parse_tag_list(prompt_char or "")
        tag_rows = []
        canonical_main = []
        is_nsfw = 0
        seq = 0

        def _push_tag(t, for_signature: bool, src_mask: int):
            nonlocal is_nsfw, seq
            tag_norm = normalize_tag(t.tag_text)
            if not tag_norm:
                return
            canonical = _lookup_alias(conn, tag_norm)
            cat = _get_tag_category(conn, canonical)
            cat = _effective_tag_category(conn, canonical, cat)
            group = _category_group(conn, canonical, cat)
            if for_signature:
                canonical_main.append(canonical)

            if canonical in {"nsfw", "explicit"}:
                is_nsfw = 1
            brace = int(t.brace_level or 0)
            numw = float(t.numeric_weight or 0)
            cur_seq = seq
            seq += 1
            tag_rows.append((canonical, t.tag_text, t.tag_raw_one, cat, t.emphasis_type, brace, numw, group, int(src_mask), int(cur_seq)))

        for t in parsed_tags:
            _push_tag(t, for_signature=True, src_mask=1)
        for t in parsed_char_tags:
            _push_tag(t, for_signature=False, src_mask=2)

        # Per-image unique tag set for cache updates
        new_tag_cat: dict[str, int | None] = {}
        for (canonical, _tag_text, _tag_raw, cat, _etype, _brace, _numw, _group, _src_mask, _seq) in _iter_tag_rows(tag_rows):
            if canonical not in new_tag_cat or (new_tag_cat[canonical] is None and cat is not None):
                new_tag_cat[canonical] = cat
        new_tags = set(new_tag_cat.keys())

        sig = main_sig_hash(canonical_main) if canonical_main else None

        conn.execute(
            """
            UPDATE images
            SET software=?, model_name=?,
                prompt_positive_raw=?, prompt_negative_raw=?, prompt_character_raw=?,
                params_json=?, potion_raw=?, has_potion=?, metadata_raw=?,
                main_sig_hash=?, dedup_flag=?, is_nsfw=?
            WHERE id=?
            """,
            (
                software,
                model,
                prompt_pos,
                prompt_neg,
                prompt_char,
                params_json,
                potion_raw,
                has_potion,
                metadata_raw,
                sig,
                1,
                is_nsfw,
                image_id,
            ),
        )

        has_src_mask = _table_has_col(conn, "image_tags", "src_mask")
        has_seq = _table_has_col(conn, "image_tags", "seq")
        conn.execute("DELETE FROM image_tags WHERE image_id=?", (image_id,))
        for (canonical, tag_text, tag_raw, cat, etype, brace, numw, group, src_mask, seq) in _iter_tag_rows(tag_rows):
            if has_src_mask and has_seq:
                conn.execute(
                    """INSERT INTO image_tags(
                           image_id, tag_canonical, tag_text, tag_raw, category,
                           emphasis_type, brace_level, numeric_weight, src_mask, seq
                         ) VALUES (?,?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
                         DO UPDATE SET
                           src_mask = (image_tags.src_mask | excluded.src_mask),
                           seq = CASE WHEN excluded.seq < image_tags.seq THEN excluded.seq ELSE image_tags.seq END,
                           category = COALESCE(image_tags.category, excluded.category),
                           tag_text = COALESCE(image_tags.tag_text, excluded.tag_text),
                           tag_raw = COALESCE(image_tags.tag_raw, excluded.tag_raw)
                    """,
                    (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, src_mask, seq),
                )
            elif has_src_mask:
                conn.execute(
                    """INSERT INTO image_tags(
                           image_id, tag_canonical, tag_text, tag_raw, category,
                           emphasis_type, brace_level, numeric_weight, src_mask
                         ) VALUES (?,?,?,?,?,?,?,?,?)
                         ON CONFLICT(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
                         DO UPDATE SET
                           src_mask = (image_tags.src_mask | excluded.src_mask),
                           category = COALESCE(image_tags.category, excluded.category),
                           tag_text = COALESCE(image_tags.tag_text, excluded.tag_text),
                           tag_raw = COALESCE(image_tags.tag_raw, excluded.tag_raw)
                    """,
                    (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, src_mask),
                )
            elif has_seq:
                conn.execute(
                    """INSERT OR IGNORE INTO image_tags(
                           image_id, tag_canonical, tag_text, tag_raw, category,
                           emphasis_type, brace_level, numeric_weight, seq
                         ) VALUES (?,?,?,?,?,?,?,?,?)""",
                    (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, seq),
                )
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO image_tags(image_id, tag_canonical, tag_text, tag_raw, category, emphasis_type, brace_level, numeric_weight)
                         VALUES (?,?,?,?,?,?,?,?)""",
                    (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw),
                )
        # ---- incremental stat_* maintenance (avoid requiring full rebuild after reparse) ----
        try:
            # software cache
            if (old_software or "") != (software or ""):
                if old_software:
                    stats_service.dec_software(conn, str(old_software))
                if software:
                    stats_service.bump_software(conn, str(software))

            # tag cache (per-image distinct)
            removed = old_tags - new_tags
            added = new_tags - old_tags
            for tcanon in removed:
                stats_service.dec_tag(conn, tcanon)
            for tcanon in added:
                stats_service.bump_tag(conn, tcanon, new_tag_cat.get(tcanon))
            # categories might become known later; update category without touching counts
            for tcanon, cat in new_tag_cat.items():
                if cat is None:
                    continue
                conn.execute(
                    "UPDATE stat_tag_counts SET category=? WHERE tag_canonical=? AND category IS NULL",
                    (int(cat), tcanon),
                )
        except Exception:
            # If cache tables are missing or corrupted, reparse should still succeed.
            pass

        return {"id": image_id, "ok": True, "tags": len(tag_rows), "old_sig": old_sig, "new_sig": sig}
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


@api_router.post("/admin/reparse")
def admin_reparse(req: ReparseReq, _admin: dict = Depends(require_admin)):
    limit = max(1, min(200, int(req.limit or 50)))

    conn = get_conn()
    try:
        if _rebuild_active(conn):
            raise HTTPException(status_code=409, detail="統計再集計が実行中です")
        # ---- persistent ops state (best-effort) ----
        params_key = {
            "date_from": (req.date_from or "") or None,
            "date_to": (req.date_to or "") or None,
            "only_missing": 1 if int(req.only_missing or 0) == 1 else 0,
            "only_nai45_missing": 1 if int(getattr(req, "only_nai45_missing", 0) or 0) == 1 else 0,
        }
        params_json = json.dumps(params_key, ensure_ascii=False, sort_keys=True)
        run_id = 0
        try:
            prev_params = _kv_get(conn, "reparse_params")
            prev_run = _kv_get(conn, "reparse_run_id")
            if prev_params != params_json or not prev_run or not str(prev_run).isdigit():
                run_id = _create_run(conn, "reparse", params_key)
                if run_id:
                    _kv_set(conn, "reparse_run_id", str(run_id))
                    _kv_set(conn, "reparse_params", params_json)
                    _kv_set(conn, "reparse_after_id", str(int(req.after_id or 0)))
            else:
                run_id = int(prev_run)
        except Exception:
            run_id = 0

        ids: list[int] = []

        if req.image_id is not None:
            ids = [int(req.image_id)]
        else:
            where: list[str] = ["id > ?"]
            params: list = [int(req.after_id or 0)]

            if req.date_from:
                where.append("substr(file_mtime_utc,1,10) >= ?")
                params.append(req.date_from)
            if req.date_to:
                where.append("substr(file_mtime_utc,1,10) <= ?")
                params.append(req.date_to)

            if int(req.only_missing or 0) == 1:
                # Missing any of the major fields
                where.append(
                    "(software IS NULL OR TRIM(software)='' OR prompt_positive_raw IS NULL OR TRIM(prompt_positive_raw)='')"
                )

            # NovelAI 4.5 targeted reparse: only rows that already look like 4.5
            # (based on existing DB fields) AND are missing major fields.
            if int(getattr(req, "only_nai45_missing", 0) or 0) == 1:
                where.append(
                    "((software IS NOT NULL AND software LIKE '%4.5%')"
                    " OR (model_name IS NOT NULL AND model_name LIKE '%4.5%')"
                    " OR (metadata_raw IS NOT NULL AND metadata_raw LIKE '%4.5%'))"
                )
                # Always pair with major-missing to match UI meaning: "差分未反映のみ".
                where.append(
                    "(software IS NULL OR TRIM(software)='' OR prompt_positive_raw IS NULL OR TRIM(prompt_positive_raw)='')"
                )

            # Operator skip list (do not keep hitting known-bad images in only_missing runs)
            if _table_has_col(conn, "images", "reparse_skip"):
                where.append("(reparse_skip IS NULL OR reparse_skip = 0)")

            q = "SELECT id FROM images WHERE " + " AND ".join(where) + " ORDER BY id ASC LIMIT ?"
            rows = conn.execute(q, params + [limit]).fetchall()
            ids = [int(r["id"]) for r in rows]

        results = []
        errors: list[dict] = []
        touched_hashes: set[str] = set()
        for iid in ids:
            try:
                r = _reparse_one(conn, iid)
                results.append(r)
                try:
                    if r.get("old_sig"):
                        touched_hashes.add(str(r.get("old_sig")))
                    if r.get("new_sig"):
                        touched_hashes.add(str(r.get("new_sig")))
                except Exception:
                    pass
                if not r.get("ok"):
                    errors.append({"id": iid, "error": r.get("error")})
                    _run_log_error(conn, run_id, iid, "reparse", str(r.get("error") or "error"))
            except Exception as e:
                errors.append({"id": iid, "error": (str(e) or "error")[:240]})
                _run_log_error(conn, run_id, iid, "exception", str(e) or "error")

        # Update dedup_flag for touched signatures (representative=MIN(id)).
        # This keeps the "重複排除" toggle working immediately after reparse batches.
        if touched_hashes and int(req.recompute_dedup or 0) == 0:
            stats_service.recompute_dedup_flags_for_hashes(conn, sorted(touched_hashes))

        # Optional heavy rebuilds
        if int(req.recompute_dedup or 0) == 1:
            stats_service.recompute_dedup_flags(conn)
        if int(req.rebuild_stats or 0) == 1:
            stats_service.rebuild_all(conn)

        conn.commit()

        next_after_id = None
        if ids:
            next_after_id = ids[-1]

        # Save continuation cursor
        try:
            if next_after_id is not None:
                _kv_set(conn, "reparse_after_id", str(int(next_after_id)))
        except Exception:
            pass

        # Update run counters
        try:
            done = (len(ids) < limit)
            _run_add_counts(
                conn,
                run_id,
                last_image_id=int(next_after_id or 0),
                processed=len(ids),
                updated=sum(1 for r in results if r.get("ok")),
                error_count=len(errors),
                done=done,
            )
        except Exception:
            pass

        ok_n = sum(1 for r in results if r.get("ok"))
        return {
            "ok": True,
            "processed": len(ids),
            "updated": ok_n,
            "next_after_id": next_after_id,
            "errors": errors[:50],
        }
    finally:
        conn.close()


@api_router.post("/admin/reparse_one")
def admin_reparse_one(req: ReparseOneReq, _admin: dict = Depends(require_admin)):
    """Reparse a single image without touching the saved batch cursor.

    This is intended for "retry from error list" in admin UI.
    """
    conn = get_conn()
    try:
        image_id = int(req.image_id or 0)
        if image_id <= 0:
            raise HTTPException(status_code=400, detail="image_id required")

        row = conn.execute("SELECT id FROM images WHERE id=?", (image_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")

        # Optional: clear operator skip flag before retry.
        if int(req.clear_skip or 0) == 1 and _table_has_col(conn, "images", "reparse_skip"):
            conn.execute("UPDATE images SET reparse_skip=0 WHERE id=?", (image_id,))

        # Attach to current reparse run if available, but do not change its params/cursor.
        run_id = 0
        try:
            prev_run = _kv_get(conn, "reparse_run_id")
            if prev_run and str(prev_run).isdigit():
                run_id = int(prev_run)
        except Exception:
            run_id = 0

        r = _reparse_one(conn, image_id)

        # Keep dedup toggle coherent for this single retry.
        try:
            touched_hashes: set[str] = set()
            if r.get("old_sig"):
                touched_hashes.add(str(r.get("old_sig")))
            if r.get("new_sig"):
                touched_hashes.add(str(r.get("new_sig")))
            if touched_hashes:
                stats_service.recompute_dedup_flags_for_hashes(conn, sorted(touched_hashes))
        except Exception:
            pass

        # Run accounting & error log (best-effort)
        try:
            if not r.get("ok"):
                _run_log_error(conn, run_id, image_id, "retry", str(r.get("error") or "error"))
                _run_add_counts(conn, run_id, last_image_id=image_id, processed=1, updated=0, error_count=1, done=False)
            else:
                _run_add_counts(conn, run_id, last_image_id=image_id, processed=1, updated=1, error_count=0, done=False)
        except Exception:
            pass

        conn.commit()
        return {"ok": bool(r.get("ok")), "result": r}
    finally:
        conn.close()


@api_router.get("/admin/reparse_state")
def admin_reparse_state(_admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        run_id = int(_kv_get(conn, "reparse_run_id") or "0")
        after_id = int(_kv_get(conn, "reparse_after_id") or "0")
        params_json = _kv_get(conn, "reparse_params") or "{}"
        try:
            params = json.loads(params_json) if params_json else {}
        except Exception:
            params = {}

        run = _fetch_run(conn, run_id)

        running_flag = int(_kv_get(conn, "reparse_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "reparse_bg_heartbeat") or "0")
        active = _hb_active(running_flag, hb_ts) and (not run or (run.get("status") == "running"))

        errors: list[dict] = []
        if run_id > 0:
            try:
                rows = conn.execute(
                    """
                    SELECT e.id, e.image_id, e.stage, e.error, e.created_at,
                           COALESCE(i.reparse_skip, 0) AS reparse_skip
                    FROM maintenance_errors e
                    LEFT JOIN images i ON i.id = e.image_id
                    WHERE e.run_id=?
                    ORDER BY e.id DESC
                    LIMIT 100
                    """,
                    (run_id,),
                ).fetchall()
                for r in rows:
                    if isinstance(r, tuple):
                        errors.append(
                            {
                                "id": int(r[0]),
                                "image_id": int(r[1] or 0),
                                "stage": r[2],
                                "error": r[3],
                                "created_at": r[4],
                                "skip": int(r[5] or 0),
                            }
                        )
                    else:
                        errors.append(
                            {
                                "id": int(r["id"]),
                                "image_id": int(r["image_id"] or 0),
                                "stage": r["stage"],
                                "error": r["error"],
                                "created_at": r["created_at"],
                                "skip": int(r["reparse_skip"] or 0),
                            }
                        )
            except Exception:
                errors = []

        skipped = 0
        try:
            skipped = int(conn.execute("SELECT COUNT(*) FROM images WHERE reparse_skip = 1").fetchone()[0])
        except Exception:
            skipped = 0

        total_images = 0
        max_image_id = 0
        try:
            total_images = int(conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0)
            max_image_id = int(conn.execute("SELECT COALESCE(MAX(id),0) FROM images").fetchone()[0] or 0)
        except Exception:
            total_images = 0
            max_image_id = 0

        hb_age_sec = None
        try:
            hb_age_sec = int(_hb_now() - int(hb_ts or 0)) if int(hb_ts or 0) > 0 else None
        except Exception:
            hb_age_sec = None

        history: list[dict] = []
        try:
            rows2 = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, last_image_id, processed, updated, error_count
                FROM maintenance_runs
                WHERE kind='reparse_all'
                ORDER BY id DESC
                LIMIT 5
                """
            ).fetchall()
            for r in rows2:
                if isinstance(r, tuple):
                    history.append({
                        "id": int(r[0]),
                        "kind": r[1],
                        "status": r[2],
                        "created_at": r[3],
                        "updated_at": r[4],
                        "last_image_id": int(r[5] or 0),
                        "processed": int(r[6] or 0),
                        "updated": int(r[7] or 0),
                        "error_count": int(r[8] or 0),
                    })
                else:
                    history.append({
                        "id": int(r["id"]),
                        "kind": r["kind"],
                        "status": r["status"],
                        "created_at": r["created_at"],
                        "updated_at": r["updated_at"],
                        "last_image_id": int(r["last_image_id"] or 0),
                        "processed": int(r["processed"] or 0),
                        "updated": int(r["updated"] or 0),
                        "error_count": int(r["error_count"] or 0),
                    })
        except Exception:
            history = []

        return {
            "run_id": run_id,
            "after_id": after_id,
            "params": params,
            "run": run,
            "errors": errors,
            "skipped_total": skipped,
            "active": bool(active),
            "hb_age_sec": hb_age_sec,
            "total_images": total_images,
            "max_image_id": max_image_id,
            "history": history,
        }
    finally:
        conn.close()


@api_router.post("/admin/reparse_skip")
def admin_reparse_skip(req: ReparseSkipReq, _admin: dict = Depends(require_admin)):
    if int(req.skip or 0) not in {0, 1}:
        raise HTTPException(status_code=400, detail="skip must be 0/1")
    conn = get_conn()
    try:
        row = conn.execute("SELECT id FROM images WHERE id=?", (int(req.image_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if not _table_has_col(conn, "images", "reparse_skip"):
            raise HTTPException(status_code=400, detail="reparse_skip column missing")
        conn.execute("UPDATE images SET reparse_skip=? WHERE id=?", (int(req.skip), int(req.image_id)))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def _reparse_active(conn: sqlite3.Connection) -> bool:
    try:
        running_flag = int(_kv_get(conn, "reparse_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "reparse_bg_heartbeat") or "0")
        return _hb_active(running_flag, hb_ts)
    except Exception:
        return False


def _rebuild_active(conn: sqlite3.Connection) -> bool:
    try:
        running_flag = int(_kv_get(conn, "rebuild_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "rebuild_bg_heartbeat") or "0")
        return _hb_active(running_flag, hb_ts)
    except Exception:
        return False


def _reparse_all_worker(run_id: int, batch_size: int = 50, interval_sec: float = 0.6) -> None:
    """Background worker: reparse all images in batches."""
    global _REPARSE_BG_THREAD
    try:
        while True:
            conn = get_conn()
            try:
                # heartbeat
                _kv_set(conn, "reparse_bg_running", "1")
                _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))

                after_id = int(_kv_get(conn, "reparse_after_id") or "0")

                where = ["id > ?"]
                params: list = [after_id]
                if _table_has_col(conn, "images", "reparse_skip"):
                    where.append("(reparse_skip IS NULL OR reparse_skip = 0)")
                rows = conn.execute(
                    "SELECT id FROM images WHERE " + " AND ".join(where) + " ORDER BY id ASC LIMIT ?",
                    params + [int(batch_size)],
                ).fetchall()
                ids = [int(r[0] if isinstance(r, tuple) else r["id"]) for r in rows]

                if not ids:
                    # done
                    _run_add_counts(conn, run_id, last_image_id=after_id, processed=0, updated=0, error_count=0, done=True)
                    _set_run_status(conn, run_id, "done")
                    _kv_set(conn, "reparse_bg_running", "0")
                    _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    break

                touched_hashes: set[str] = set()
                ok_n = 0
                err_n = 0
                _last_hb = time.time()
                for iid in ids:
                    try:
                        # keep heartbeat fresh even when parsing is slow
                        if (time.time() - _last_hb) >= 5.0:
                            _kv_set(conn, "reparse_bg_running", "1")
                            _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                            _last_hb = time.time()
                        r = _reparse_one(conn, iid)
                        if r.get("ok"):
                            ok_n += 1
                        else:
                            err_n += 1
                            _run_log_error(conn, run_id, iid, "reparse", str(r.get("error") or "error"))
                        if r.get("old_sig"):
                            touched_hashes.add(str(r.get("old_sig")))
                        if r.get("new_sig"):
                            touched_hashes.add(str(r.get("new_sig")))
                    except Exception as e:
                        err_n += 1
                        _run_log_error(conn, run_id, iid, "exception", str(e) or "error")

                if touched_hashes:
                    try:
                        stats_service.recompute_dedup_flags_for_hashes(conn, sorted(touched_hashes))
                    except Exception:
                        pass

                last_id = ids[-1]
                _kv_set(conn, "reparse_after_id", str(int(last_id)))
                _run_add_counts(conn, run_id, last_image_id=int(last_id), processed=len(ids), updated=ok_n, error_count=err_n, done=False)

                _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                conn.commit()
            finally:
                conn.close()

            time.sleep(max(0.1, float(interval_sec)))
    except Exception:
        # best-effort stop flag
        try:
            conn = get_conn()
            try:
                _kv_set(conn, "reparse_bg_running", "0")
                _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                _set_run_status(conn, run_id, "stopped")
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass
    finally:
        with _MAINT_LOCK:
            _REPARSE_BG_THREAD = None


@api_router.post("/admin/reparse_all_start")
def admin_reparse_all_start(_admin: dict = Depends(require_admin)):
    """Start or resume background full-library reparse."""
    global _REPARSE_BG_THREAD

    conn = get_conn()
    try:
        if _rebuild_active(conn):
            raise HTTPException(status_code=409, detail="統計再集計が実行中です")

        run_id = int(_kv_get(conn, "reparse_run_id") or "0")
        run = _fetch_run(conn, run_id)

        if not run or run.get("status") != "running":
            # new run (reset cursor)
            run_id = _create_run(conn, "reparse_all", {"batch": 50})
            _kv_set(conn, "reparse_run_id", str(int(run_id)))
            _kv_set(conn, "reparse_params", "{}")
            _kv_set(conn, "reparse_after_id", "0")
            _kv_set(conn, "reparse_bg_running", "0")
            _kv_set(conn, "reparse_bg_heartbeat", "0")
            conn.commit()

        # Determine whether it's currently active.
        active = _reparse_active(conn)
        started = False
        if not active:
            with _MAINT_LOCK:
                if not (_REPARSE_BG_THREAD and _REPARSE_BG_THREAD.is_alive()):
                    _kv_set(conn, "reparse_bg_running", "1")
                    _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    t = threading.Thread(target=_reparse_all_worker, args=(int(run_id),), daemon=True)
                    _REPARSE_BG_THREAD = t
                    t.start()
                    started = True
        return {"ok": True, "run_id": int(run_id), "started": bool(started)}
    finally:
        conn.close()


def _rebuild_stats_worker(run_id: int) -> None:
    global _REBUILD_BG_THREAD
    try:
        conn = get_conn()
        try:
            _kv_set(conn, "rebuild_bg_running", "1")
            _kv_set(conn, "rebuild_bg_heartbeat", str(_hb_now()))
            conn.commit()

            # keep heartbeat fresh even if rebuild steps take long
            _stop_ping = {"stop": False}
            def _ping():
                while not _stop_ping["stop"]:
                    try:
                        c = get_conn()
                        try:
                            _kv_set(c, "rebuild_bg_running", "1")
                            _kv_set(c, "rebuild_bg_heartbeat", str(_hb_now()))
                            c.commit()
                        finally:
                            c.close()
                    except Exception:
                        pass
                    time.sleep(5.0)
            _ping_t = threading.Thread(target=_ping, daemon=True)
            _ping_t.start()
            try:
                stats_service.recompute_dedup_flags(conn)
                stats_service.rebuild_all(conn)
            finally:
                _stop_ping["stop"] = True

            _kv_set(conn, "rebuild_bg_heartbeat", str(_hb_now()))
            _set_run_status(conn, run_id, "done")
            _kv_set(conn, "rebuild_bg_running", "0")
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        try:
            conn = get_conn()
            try:
                _run_log_error(conn, run_id, None, "rebuild", str(e) or "error")
                _set_run_status(conn, run_id, "stopped")
                _kv_set(conn, "rebuild_bg_running", "0")
                _kv_set(conn, "rebuild_bg_heartbeat", str(_hb_now()))
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass
    finally:
        with _MAINT_LOCK:
            _REBUILD_BG_THREAD = None


@api_router.post("/admin/rebuild_stats_start")
def admin_rebuild_stats_start(_admin: dict = Depends(require_admin)):
    """Start background rebuild (stats + dedup)."""
    global _REBUILD_BG_THREAD

    conn = get_conn()
    try:
        if _reparse_active(conn):
            raise HTTPException(status_code=409, detail="再解析が実行中です")

        run_id = int(_kv_get(conn, "rebuild_run_id") or "0")
        run = _fetch_run(conn, run_id)

        if not run or run.get("status") != "running":
            run_id = _create_run(conn, "rebuild_stats", {"rebuild_stats": 1, "recompute_dedup": 1})
            _kv_set(conn, "rebuild_run_id", str(int(run_id)))
            _kv_set(conn, "rebuild_bg_running", "0")
            _kv_set(conn, "rebuild_bg_heartbeat", "0")
            conn.commit()

        active = _rebuild_active(conn)
        started = False
        if not active:
            with _MAINT_LOCK:
                if not (_REBUILD_BG_THREAD and _REBUILD_BG_THREAD.is_alive()):
                    _kv_set(conn, "rebuild_bg_running", "1")
                    _kv_set(conn, "rebuild_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    t = threading.Thread(target=_rebuild_stats_worker, args=(int(run_id),), daemon=True)
                    _REBUILD_BG_THREAD = t
                    t.start()
                    started = True

        return {"ok": True, "run_id": int(run_id), "started": bool(started)}
    finally:
        conn.close()


@api_router.get("/admin/rebuild_state")
def admin_rebuild_state(_admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        run_id = int(_kv_get(conn, "rebuild_run_id") or "0")
        run = _fetch_run(conn, run_id)

        running_flag = int(_kv_get(conn, "rebuild_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "rebuild_bg_heartbeat") or "0")
        active = _hb_active(running_flag, hb_ts) and (not run or (run.get("status") == "running"))

        hb_age_sec = None
        try:
            hb_age_sec = int(_hb_now() - int(hb_ts or 0)) if int(hb_ts or 0) > 0 else None
        except Exception:
            hb_age_sec = None

        history: list[dict] = []
        try:
            rows2 = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, processed, updated, error_count
                FROM maintenance_runs
                WHERE kind='rebuild_stats'
                ORDER BY id DESC
                LIMIT 5
                """
            ).fetchall()
            for r in rows2:
                if isinstance(r, tuple):
                    history.append({
                        "id": int(r[0]),
                        "kind": r[1],
                        "status": r[2],
                        "created_at": r[3],
                        "updated_at": r[4],
                        "processed": int(r[5] or 0),
                        "updated": int(r[6] or 0),
                        "error_count": int(r[7] or 0),
                    })
                else:
                    history.append({
                        "id": int(r["id"]),
                        "kind": r["kind"],
                        "status": r["status"],
                        "created_at": r["created_at"],
                        "updated_at": r["updated_at"],
                        "processed": int(r["processed"] or 0),
                        "updated": int(r["updated"] or 0),
                        "error_count": int(r["error_count"] or 0),
                    })
        except Exception:
            history = []

        return {
            "run_id": run_id,
            "run": run,
            "active": bool(active),
            "hb_age_sec": hb_age_sec,
            "history": history,
        }
    finally:
        conn.close()


class RebuildReq(BaseModel):
    rebuild_stats: int = 1
    recompute_dedup: int = 1


@api_router.post("/admin/rebuild_caches")
def admin_rebuild_caches(req: RebuildReq, _admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        if _reparse_active(conn):
            raise HTTPException(status_code=409, detail="再解析が実行中です")
        if int(req.recompute_dedup or 0) == 1:
            stats_service.recompute_dedup_flags(conn)
        if int(req.rebuild_stats or 0) == 1:
            stats_service.rebuild_all(conn)
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.get("/admin/status")
def admin_status(_admin: dict = Depends(require_admin)):
    """Return lightweight progress/status counters for admin UI.

    This helps operators understand how much of the library has been parsed,
    and whether caches/derivatives are up to date.
    """
    conn = get_conn()
    try:
        def one(sql: str, params: tuple = ()) -> int:
            r = conn.execute(sql, params).fetchone()
            if not r:
                return 0
            # sqlite row might be tuple
            v = r[0] if isinstance(r, tuple) else list(r)[0]
            try:
                return int(v or 0)
            except Exception:
                return 0

        total_images = one("SELECT COUNT(*) FROM images")

        meta_software = one("SELECT COUNT(*) FROM images WHERE software IS NOT NULL AND TRIM(software) <> ''")
        meta_prompt = one("SELECT COUNT(*) FROM images WHERE prompt_positive_raw IS NOT NULL AND TRIM(prompt_positive_raw) <> ''")
        meta_char = one("SELECT COUNT(*) FROM images WHERE prompt_character_raw IS NOT NULL AND TRIM(prompt_character_raw) <> ''")
        meta_sig = one("SELECT COUNT(*) FROM images WHERE main_sig_hash IS NOT NULL AND TRIM(main_sig_hash) <> ''")

        dedup_1 = one("SELECT COUNT(*) FROM images WHERE dedup_flag = 1")
        dedup_2 = one("SELECT COUNT(*) FROM images WHERE dedup_flag = 2")

        # derivatives (if table exists)
        try:
            grid_deriv = one("SELECT COUNT(DISTINCT image_id) FROM image_derivatives WHERE kind='grid'")
            overlay_deriv = one("SELECT COUNT(DISTINCT image_id) FROM image_derivatives WHERE kind='overlay'")
        except Exception:
            grid_deriv = 0
            overlay_deriv = 0

        total_tags = one("SELECT COUNT(*) FROM image_tags")
        has_src = _table_has_col(conn, "image_tags", "src_mask")
        if has_src:
            char_src_tags = one("SELECT COUNT(*) FROM image_tags WHERE (src_mask & 2) != 0")
            main_src_tags = one("SELECT COUNT(*) FROM image_tags WHERE (src_mask & 1) != 0")
        else:
            char_src_tags = 0
            main_src_tags = total_tags

        # stat caches
        creators_rows = one("SELECT COUNT(*) FROM stat_creators")
        software_rows = one("SELECT COUNT(*) FROM stat_software")
        day_rows = one("SELECT COUNT(*) FROM stat_day_counts")
        tag_rows = one("SELECT COUNT(*) FROM stat_tag_counts")

        # cache consistency (cheap checks; no full scans)
        creators_sum = one("SELECT COALESCE(SUM(image_count),0) FROM stat_creators")
        software_sum = one("SELECT COALESCE(SUM(image_count),0) FROM stat_software")
        day_sum = one("SELECT COALESCE(SUM(image_count),0) FROM stat_day_counts")
        day_expected = one(
            "SELECT COUNT(*) FROM images WHERE file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10"
        )
        tag_expected_rows = one("SELECT COUNT(DISTINCT tag_canonical) FROM image_tags")

        # missing / candidates for reparse
        missing_major = one(
            """
            SELECT COUNT(*)
            FROM images
            WHERE (software IS NULL OR TRIM(software)=''
               OR prompt_positive_raw IS NULL OR TRIM(prompt_positive_raw)='')
            """
        )
        missing_char = one(
            """
            SELECT COUNT(*)
            FROM images
            WHERE prompt_character_raw IS NULL OR TRIM(prompt_character_raw)=''
            """
        )

        nai45_total = one(
            """
            SELECT COUNT(*)
            FROM images
            WHERE ((software IS NOT NULL AND software LIKE '%4.5%')
                OR (model_name IS NOT NULL AND model_name LIKE '%4.5%')
                OR (metadata_raw IS NOT NULL AND metadata_raw LIKE '%4.5%'))
            """
        )
        nai45_missing_major = one(
            """
            SELECT COUNT(*)
            FROM images
            WHERE ((software IS NOT NULL AND software LIKE '%4.5%')
                OR (model_name IS NOT NULL AND model_name LIKE '%4.5%')
                OR (metadata_raw IS NOT NULL AND metadata_raw LIKE '%4.5%'))
              AND (software IS NULL OR TRIM(software)=''
                OR prompt_positive_raw IS NULL OR TRIM(prompt_positive_raw)='')
            """
        )

        # If operator skip is enabled, provide an "unskipped" view for automation.
        # This keeps "残数0" meaningful even when some rows are intentionally skipped.
        if _table_has_col(conn, "images", "reparse_skip"):
            nai45_missing_major_unskipped = one(
                """
                SELECT COUNT(*)
                FROM images
                WHERE ((software IS NOT NULL AND software LIKE '%4.5%')
                    OR (model_name IS NOT NULL AND model_name LIKE '%4.5%')
                    OR (metadata_raw IS NOT NULL AND metadata_raw LIKE '%4.5%'))
                  AND (software IS NULL OR TRIM(software)=''
                    OR prompt_positive_raw IS NULL OR TRIM(prompt_positive_raw)='')
                  AND (reparse_skip IS NULL OR reparse_skip = 0)
                """
            )
        else:
            nai45_missing_major_unskipped = nai45_missing_major

        return {
            # Human-readable build label for admin UI.
            # Keep stable and short; bump when DB/derivative policy changes.
            "build": f"deriv:{DERIV_VERSION}",
            "images": {
                "total": total_images,
                "dedup_flag_1": dedup_1,
                "dedup_flag_2": dedup_2,
            },
            "metadata": {
                "software_ok": meta_software,
                "prompt_ok": meta_prompt,
                "character_ok": meta_char,
                "main_sig_ok": meta_sig,
            },
            "derivatives": {
                "grid": grid_deriv,
                "overlay": overlay_deriv,
            },
            "tags": {
                "total_rows": total_tags,
                "has_src_mask": bool(has_src),
                "main_src_rows": main_src_tags,
                "character_src_rows": char_src_tags,
            },
            "caches": {
                "creators": creators_rows,
                "software": software_rows,
                "day_counts": day_rows,
                "tag_counts": tag_rows,
            },
            "cache_consistency": {
                "creators_ok": bool(creators_sum == total_images),
                "creators_sum": creators_sum,
                "creators_expected": total_images,
                "software_ok": bool(software_sum == meta_software),
                "software_sum": software_sum,
                "software_expected": meta_software,
                "day_ok": bool(day_sum == day_expected),
                "day_sum": day_sum,
                "day_expected": day_expected,
                "tag_rows_ok": bool(tag_rows == tag_expected_rows),
                "tag_rows": tag_rows,
                "tag_rows_expected": tag_expected_rows,
            },
            "needs": {
                "reparse_missing_major": missing_major,
                "reparse_missing_character": missing_char,
                "reparse_nai45_total": nai45_total,
                "reparse_nai45_missing_major": nai45_missing_major,
                "reparse_nai45_missing_major_unskipped": nai45_missing_major_unskipped,
            },
        }
    finally:
        conn.close()


@api_router.get("/stats/creators")
def stats_creators(user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        rows = conn.execute("SELECT creator, image_count FROM stat_creators ORDER BY image_count DESC, creator ASC").fetchall()
        return [{"creator": r["creator"], "count": r["image_count"]} for r in rows]
    finally:
        conn.close()

@api_router.get("/stats/software")
def stats_software(user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        rows = conn.execute("SELECT software, image_count FROM stat_software ORDER BY image_count DESC, software ASC").fetchall()
        return [{"software": r["software"], "count": r["image_count"]} for r in rows]
    finally:
        conn.close()

@api_router.get("/stats/day_counts")
def stats_day_counts(month: str, user: dict = Depends(get_user)):
    # month=YYYY-MM
    if not month or len(month) != 7:
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT ymd, image_count FROM stat_day_counts WHERE ymd LIKE ? ORDER BY ymd ASC",
            (month + "-%",),
        ).fetchall()
        return [{"ymd": r["ymd"], "count": r["image_count"]} for r in rows]
    finally:
        conn.close()

@api_router.get("/stats/month_counts")
def stats_month_counts(year: str, user: dict = Depends(get_user)):
    # year=YYYY
    if (not year) or (len(year) != 4) or (not year.isdigit()):
        raise HTTPException(status_code=400, detail="year must be YYYY")
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT ym, image_count FROM stat_month_counts WHERE ym LIKE ? ORDER BY ym ASC",
            (year + "-%",),
        ).fetchall()
        yrow = conn.execute("SELECT image_count FROM stat_year_counts WHERE year=?", (year,)).fetchone()
        year_total = int((yrow["image_count"] if yrow else 0) or 0)
        return {
            "year": year,
            "year_total": year_total,
            "items": [{"ym": r["ym"], "count": r["image_count"]} for r in rows],
        }
    finally:
        conn.close()


@api_router.get("/stats/year_counts")
def stats_year_counts(user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        rows = conn.execute("SELECT year, image_count FROM stat_year_counts ORDER BY year DESC").fetchall()
        return [{"year": r["year"], "count": r["image_count"]} for r in rows]
    finally:
        conn.close()


@api_router.get("/tags/suggest")
def tag_suggest(q: str = "", limit: int = 20, user: dict = Depends(get_user)):
    qn = normalize_tag(q) if q else ""
    conn = get_conn()
    try:
        if not qn:
            rows = conn.execute(
                "SELECT tag_canonical, image_count, category FROM stat_tag_counts ORDER BY image_count DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT tag_canonical, image_count, category FROM stat_tag_counts WHERE tag_canonical LIKE ? ORDER BY image_count DESC LIMIT ?",
                (qn + "%", limit),
            ).fetchall()
        return [{"tag": r["tag_canonical"], "count": r["image_count"], "category": r["category"]} for r in rows]
    finally:
        conn.close()

def _lookup_alias(conn: sqlite3.Connection, tag_norm: str) -> str:
    row = conn.execute("SELECT canonical FROM tag_aliases WHERE alias=?", (tag_norm,)).fetchone()
    if row:
        return str(row["canonical"])
    return tag_norm

def _category_group(conn: sqlite3.Connection, canonical: str, cat: int | None) -> str:
    # groups for UI: artist / quality / character / other
    if cat == 1:
        return "artist"
    if cat == 4:
        return "character"
    # quality: explicit list / wildcard patterns OR meta category 5
    if cat == 5 or _is_quality_tag(conn, canonical):
        return "quality"
    return "other"

def _get_tag_category(conn: sqlite3.Connection, canonical: str) -> int | None:
    row = conn.execute("SELECT category FROM tags_master WHERE tag=?", (canonical,)).fetchone()
    if row and row["category"] is not None:
        return int(row["category"])
    return None


def _effective_tag_category(conn: sqlite3.Connection, canonical: str, cat: int | None) -> int | None:
    """Return the category used for UI grouping.

    Keep artist/character categories from the master dictionary, but allow
    the bundled "extra-quality-tags.csv" (including wildcards) to upgrade
    otherwise-unknown tags into quality (5).

    This fixes cases like "masterpiece" being treated as "other" when the
    master dictionary doesn't label it as quality.
    """
    if cat in (1, 4, 5):
        return cat
    try:
        if _is_quality_tag(conn, canonical):
            return 5
    except Exception:
        pass
    return cat

def _parse_last_modified_ms(val: str | None) -> str | None:
    if not val:
        return None
    try:
        ms = int(val)
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None


@api_router.post("/upload")
async def upload_image(
    bg: BackgroundTasks,
    file: UploadFile = File(...),
    last_modified_ms: str | None = Form(default=None),
    user: dict = Depends(get_user),
):
    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="empty file")

    mtime = _parse_last_modified_ms(last_modified_ms)
    if not mtime:
        mtime = datetime.now(timezone.utc).isoformat()

    conn = get_conn()
    try:
        return _upload_image_core(
            conn=conn,
            bg=bg,
            raw=raw,
            filename=(file.filename or "upload"),
            mime=(file.content_type or "application/octet-stream"),
            mtime_iso=mtime,
            user_id=int(user["id"]),
            username=str(user["username"]),
        )
    finally:
        conn.close()


def _allowed_image_ext(filename: str) -> bool:
    ext = (filename.rsplit(".", 1)[-1] if "." in filename else "").lower()
    return ext in {"png", "webp", "jpg", "jpeg", "avif"}


def _zipinfo_mtime_iso(info) -> str:
    try:
        # ZipInfo.date_time is (Y,M,D,H,M,S) in local time; treat as UTC for deterministic ordering.
        dt = datetime(*info.date_time, tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _upload_image_core(
    *,
    conn: sqlite3.Connection,
    bg: BackgroundTasks | None,
    raw: bytes,
    filename: str,
    mime: str,
    mtime_iso: str,
    user_id: int,
    username: str,
) -> dict:
    """Core upload logic used by both direct upload and zip jobs.

    Keeps the existing semantics:
    - binary sha256 dedup
    - metadata extract
    - tag parsing + stats bumps
    - original stored on disk, derivatives stored in DB
    - derivatives created in background (or inline when bg is None)
    """

    import hashlib

    sha = hashlib.sha256(raw).hexdigest()

    # dedup by binary
    row = conn.execute("SELECT id FROM images WHERE sha256=?", (sha,)).fetchone()
    if row:
        return {"ok": True, "dedup": True, "image_id": row["id"]}

    # basic image info
    from PIL import Image
    try:
        with Image.open(io.BytesIO(raw)) as im:
            width, height = im.size
            _fmt = (im.format or "").upper()
    except Exception:
        width = height = None
        _fmt = ""

    ext = (filename.rsplit(".", 1)[-1].lower() if "." in filename else "bin")

    # metadata
    tmp_path = None
    meta = None
    try:
        import tempfile
        fd, p = tempfile.mkstemp(prefix="nai_up_", suffix="." + ext)
        os.close(fd)
        tmp_path = p
        with open(tmp_path, "wb") as f:
            f.write(raw)
        meta = extract_novelai_metadata(tmp_path)
    except Exception:
        meta = None
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    software = meta.software if meta else None
    model = meta.model if meta else None
    prompt_pos = meta.prompt if meta else None
    prompt_neg = meta.negative if meta else None
    prompt_char = meta.character_prompt if meta else None
    seed = None
    try:
        if meta and meta.params and ("seed" in meta.params):
            seed = int(meta.params.get("seed"))
    except Exception:
        seed = None
    params_json = json.dumps(meta.params, ensure_ascii=False) if (meta and meta.params) else None
    potion_raw = json.dumps(meta.potion, ensure_ascii=False).encode("utf-8") if (meta and meta.potion) else None
    has_potion = 1 if potion_raw else 0
    metadata_raw = None
    if meta:
        try:
            metadata_raw = json.dumps({"info": meta.raw, "json": meta.raw_json_str}, ensure_ascii=False)[:65535]
        except Exception:
            metadata_raw = None

    # Full-meta dedup (treat as true duplicate ONLY when everything matches, including seed).
    # This is separate from main_sig_hash (prompt-only dedup_flag=2).
    full_meta_hash = None
    try:
        src = "\n".join(
            [
                (software or ""),
                (model or ""),
                (prompt_pos or ""),
                (prompt_char or ""),
                (prompt_neg or ""),
                (str(seed) if seed is not None else ""),
            ]
        )
        full_meta_hash = hashlib.sha1(src.encode("utf-8", errors="ignore")).hexdigest()
    except Exception:
        full_meta_hash = None

    if full_meta_hash:
        row2 = conn.execute(
            """
            SELECT id FROM images
            WHERE full_meta_hash = ?
               OR (
                    full_meta_hash IS NULL
                AND software IS ?
                AND model_name IS ?
                AND prompt_positive_raw IS ?
                AND prompt_negative_raw IS ?
                AND prompt_character_raw IS ?
                AND seed IS ?
               )
            LIMIT 1
            """,
            (full_meta_hash, software, model, prompt_pos, prompt_neg, prompt_char, seed),
        ).fetchone()
        if row2:
            return {"ok": True, "dedup": True, "image_id": row2["id"], "dedup_reason": "full"}

    # tags
    parsed_tags = parse_tag_list(prompt_pos or "")
    parsed_char_tags = parse_tag_list(prompt_char or "")
    tag_rows = []

    canonical_main = []
    is_nsfw = 0
    seq = 0

    def _push_tag(t, for_signature: bool, src_mask: int):
        nonlocal is_nsfw, seq
        tag_norm = normalize_tag(t.tag_text)
        if not tag_norm:
            return
        canonical = _lookup_alias(conn, tag_norm)
        cat = _get_tag_category(conn, canonical)
        cat = _effective_tag_category(conn, canonical, cat)
        group = _category_group(conn, canonical, cat)
        if for_signature:
            canonical_main.append(canonical)

        if canonical in {"nsfw", "explicit"}:
            is_nsfw = 1
        brace = int(t.brace_level or 0)
        numw = float(t.numeric_weight or 0)
        cur_seq = seq
        seq += 1
        tag_rows.append((canonical, t.tag_text, t.tag_raw_one, cat, t.emphasis_type, brace, numw, group, int(src_mask), int(cur_seq)))

    for t in parsed_tags:
        _push_tag(t, for_signature=True, src_mask=1)

    for t in parsed_char_tags:
        _push_tag(t, for_signature=False, src_mask=2)

    sig = None
    dedup_flag = 1
    if canonical_main:
        sig = main_sig_hash(canonical_main)
        exist = conn.execute("SELECT 1 FROM images WHERE main_sig_hash=? LIMIT 1", (sig,)).fetchone()
        if exist:
            dedup_flag = 2

    cur = conn.execute(
        """
        INSERT INTO images(
          sha256, original_filename, ext, mime, width, height, file_mtime_utc, uploader_user_id,
          software, model_name, prompt_positive_raw, prompt_negative_raw, params_json,
          prompt_character_raw,
          seed,
          potion_raw, has_potion, metadata_raw, main_sig_hash, dedup_flag
          , full_meta_hash
          , favorite, is_nsfw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            sha,
            filename or "upload",
            ext,
            mime,
            width,
            height,
            mtime_iso,
            int(user_id),
            software,
            model,
            prompt_pos,
            prompt_neg,
            params_json,
            prompt_char,
            seed,
            potion_raw,
            has_potion,
            metadata_raw,
            sig,
            dedup_flag,
            full_meta_hash,
            0,
            is_nsfw,
        ),
    )
    image_id = int(cur.lastrowid)

    # Store original on disk (keep app.db small). Bytes column is only for legacy migrations.
    disk_path = _write_original_to_disk(
        image_id=image_id,
        original_filename=filename or "upload",
        ext=ext,
        sha256=sha,
        raw=raw,
    )

    conn.execute(
        "INSERT INTO image_files(image_id, disk_path, size, bytes) VALUES (?,?,?,NULL)",
        (image_id, disk_path, int(len(raw))),
    )

    # image_tags
    has_src_mask = _table_has_col(conn, "image_tags", "src_mask")
    has_seq = _table_has_col(conn, "image_tags", "seq")
    for (canonical, tag_text, tag_raw, cat, etype, brace, numw, group, src_mask, seq2) in _iter_tag_rows(tag_rows):
        if has_src_mask and has_seq:
            conn.execute(
                """INSERT INTO image_tags(
                       image_id, tag_canonical, tag_text, tag_raw, category,
                       emphasis_type, brace_level, numeric_weight, src_mask, seq
                     ) VALUES (?,?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
                     DO UPDATE SET
                       src_mask = (image_tags.src_mask | excluded.src_mask),
                       seq = CASE WHEN excluded.seq < image_tags.seq THEN excluded.seq ELSE image_tags.seq END,
                       category = COALESCE(image_tags.category, excluded.category),
                       tag_text = COALESCE(image_tags.tag_text, excluded.tag_text),
                       tag_raw = COALESCE(image_tags.tag_raw, excluded.tag_raw)
                """,
                (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, src_mask, seq2),
            )
        elif has_src_mask:
            conn.execute(
                """INSERT INTO image_tags(
                       image_id, tag_canonical, tag_text, tag_raw, category,
                       emphasis_type, brace_level, numeric_weight, src_mask
                     ) VALUES (?,?,?,?,?,?,?,?,?)
                     ON CONFLICT(image_id, tag_canonical, emphasis_type, brace_level, numeric_weight)
                     DO UPDATE SET
                       src_mask = (image_tags.src_mask | excluded.src_mask),
                       category = COALESCE(image_tags.category, excluded.category),
                       tag_text = COALESCE(image_tags.tag_text, excluded.tag_text),
                       tag_raw = COALESCE(image_tags.tag_raw, excluded.tag_raw)
                """,
                (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, src_mask),
            )
        elif has_seq:
            conn.execute(
                """INSERT OR IGNORE INTO image_tags(
                       image_id, tag_canonical, tag_text, tag_raw, category,
                       emphasis_type, brace_level, numeric_weight, seq
                     ) VALUES (?,?,?,?,?,?,?,?,?)""",
                (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw, seq2),
            )
        else:
            conn.execute(
                """INSERT OR IGNORE INTO image_tags(image_id, tag_canonical, tag_text, tag_raw, category, emphasis_type, brace_level, numeric_weight)
                     VALUES (?,?,?,?,?,?,?,?)""",
                (image_id, canonical, tag_text, tag_raw, cat, etype, brace, numw),
            )

    # tag stats (count per-image; avoid double counting when emphasis differs)
    uniq: dict[str, int | None] = {}
    for (canonical, _tag_text, _tag_raw, cat, _etype, _brace, _numw, _group, _src_mask, _seq) in _iter_tag_rows(tag_rows):
        if canonical not in uniq or (uniq[canonical] is None and cat is not None):
            uniq[canonical] = cat
    for canonical, cat in uniq.items():
        stats_service.bump_tag(conn, canonical, cat)

    # stats (creator/software/day)
    stats_service.bump_creator(conn, username)
    if software:
        stats_service.bump_software(conn, software)
    if mtime_iso:
        ymd = mtime_iso[:10]
        stats_service.bump_day(conn, ymd)
        stats_service.bump_month(conn, ymd[:7])
        stats_service.bump_year(conn, ymd[:4])

    conn.commit()

    # background derivative creation
    if bg is not None:
        bg.add_task(_ensure_derivatives, image_id)
    else:
        _ensure_derivatives(image_id)

    return {"ok": True, "dedup": False, "image_id": image_id, "dedup_flag": dedup_flag}


@api_router.post("/upload_zip")
async def upload_zip(
    bg: BackgroundTasks,
    file: UploadFile = File(...),
    user: dict = Depends(get_user),
):
    # Stream to disk (avoid loading large zip into memory)
    import tempfile
    fd, tmp = tempfile.mkstemp(prefix="nim_zip_", suffix=".zip")
    os.close(fd)
    total_bytes = 0
    try:
        with open(tmp, "wb") as f:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total_bytes += len(chunk)
                f.write(chunk)
    except Exception:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise
    if total_bytes <= 0:
        try:
            os.remove(tmp)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="empty file")

    # NOTE: Avoid scanning the zip here.
    # Large zips can have many entries; scanning in the request thread makes the UI feel frozen.
    # We'll scan once in the background worker and update `total` there.
    total = 0

    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO upload_zip_jobs(user_id, filename, total, status) VALUES (?,?,?,?)",
            (int(user["id"]), file.filename or "upload.zip", int(total), "queued"),
        )
        job_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    bg.add_task(_upload_zip_worker, job_id, tmp, int(user["id"]), str(user["username"]))
    return {"ok": True, "job_id": job_id, "total": int(total)}


# ---------- zip chunk upload (for Cloudflare Quick Tunnel stability) ----------
# NOTE: This is a local app. We keep incoming upload state in-process.
import time
import uuid
import threading

_ZIP_INCOMING_LOCK = threading.Lock()
_ZIP_INCOMING: dict[str, dict] = {}


def _zip_incoming_gc() -> None:
    """Remove stale incoming uploads."""
    now = time.time()
    kill: list[str] = []
    with _ZIP_INCOMING_LOCK:
        for token, st in list(_ZIP_INCOMING.items()):
            if now - float(st.get("updated_at", 0) or 0) > 60 * 30:
                kill.append(token)
        for token in kill:
            st = _ZIP_INCOMING.pop(token, None)
            if st:
                try:
                    os.remove(st.get("tmp") or "")
                except Exception:
                    pass


@api_router.post("/upload_zip_chunk/init")
async def upload_zip_chunk_init(
    filename: str = Form("upload.zip"),
    total_bytes: int = Form(0),
    user: dict = Depends(get_user),
):
    _zip_incoming_gc()
    import tempfile
    fd, tmp = tempfile.mkstemp(prefix="nim_zipc_", suffix=".zip")
    os.close(fd)
    token = uuid.uuid4().hex
    with _ZIP_INCOMING_LOCK:
        _ZIP_INCOMING[token] = {
            "user_id": int(user["id"]),
            "filename": filename or "upload.zip",
            "total_bytes": int(total_bytes or 0),
            "received": 0,
            "tmp": tmp,
            "updated_at": time.time(),
        }
    return {"ok": True, "token": token}


@api_router.post("/upload_zip_chunk/append")
async def upload_zip_chunk_append(
    token: str = Form(...),
    offset: int = Form(0),
    chunk: UploadFile = File(...),
    user: dict = Depends(get_user),
):
    _zip_incoming_gc()
    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
    if not st:
        raise HTTPException(status_code=404, detail="token not found")
    if int(st.get("user_id") or 0) != int(user["id"]):
        raise HTTPException(status_code=403, detail="forbidden")
    if int(offset or 0) != int(st.get("received") or 0):
        raise HTTPException(status_code=409, detail="offset mismatch")

    tmp = str(st.get("tmp") or "")
    wrote = 0
    with open(tmp, "ab") as f:
        while True:
            b = await chunk.read(1024 * 1024)
            if not b:
                break
            wrote += len(b)
            f.write(b)

    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
        if st:
            st["received"] = int(st.get("received") or 0) + wrote
            st["updated_at"] = time.time()
            received = int(st["received"])
            total = int(st.get("total_bytes") or 0)
        else:
            received = 0
            total = 0
    return {"ok": True, "received": received, "total_bytes": total}


@api_router.post("/upload_zip_chunk/finish")
async def upload_zip_chunk_finish(
    bg: BackgroundTasks,
    token: str = Form(...),
    user: dict = Depends(get_user),
):
    _zip_incoming_gc()
    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
    if not st:
        raise HTTPException(status_code=404, detail="token not found")
    if int(st.get("user_id") or 0) != int(user["id"]):
        raise HTTPException(status_code=403, detail="forbidden")

    tmp = str(st.get("tmp") or "")
    filename = str(st.get("filename") or "upload.zip")

    # Same as /upload_zip: defer scanning to the worker.
    total = 0

    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO upload_zip_jobs(user_id, filename, total, status) VALUES (?,?,?,?)",
            (int(user["id"]), filename, int(total), "queued"),
        )
        job_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    with _ZIP_INCOMING_LOCK:
        _ZIP_INCOMING.pop(token, None)

    bg.add_task(_upload_zip_worker, job_id, tmp, int(user["id"]), str(user["username"]))
    return {"ok": True, "job_id": job_id, "total": int(total)}


@api_router.get("/upload_zip/{job_id}")
def upload_zip_status(job_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, user_id, filename, total, done, failed, dup, status, error FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if int(row["user_id"] or 0) != int(user["id"]):
            raise HTTPException(status_code=403, detail="forbidden")

        items = conn.execute(
            "SELECT filename, state, image_id FROM upload_zip_items WHERE job_id=? ORDER BY seq DESC LIMIT 12",
            (int(job_id),),
        ).fetchall()

        return {
            "job_id": int(row["id"]),
            "filename": row["filename"],
            "total": int(row["total"] or 0),
            "done": int(row["done"] or 0),
            "failed": int(row["failed"] or 0),
            "dup": int(row["dup"] or 0),
            "status": row["status"],
            "error": row["error"],
            "items": [
                {"filename": r["filename"], "state": r["state"], "image_id": r["image_id"]}
                for r in items
            ],
        }
    finally:
        conn.close()


@api_router.post("/upload_zip/{job_id}/cancel")
def upload_zip_cancel(job_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute("SELECT user_id, status FROM upload_zip_jobs WHERE id=?", (int(job_id),)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if int(row["user_id"] or 0) != int(user["id"]):
            raise HTTPException(status_code=403, detail="forbidden")
        if row["status"] in {"done", "error"}:
            return {"ok": True, "status": row["status"]}
        conn.execute(
            "UPDATE upload_zip_jobs SET status='cancelled', updated_at_utc=datetime('now') WHERE id=?",
            (int(job_id),),
        )
        conn.commit()
        return {"ok": True, "status": "cancelled"}
    finally:
        conn.close()


def _upload_zip_worker(job_id: int, zip_path: str, user_id: int, username: str) -> None:
    import zipfile
    import mimetypes

    conn = get_conn()
    try:
        conn.execute(
            "UPDATE upload_zip_jobs SET status='scanning', updated_at_utc=datetime('now') WHERE id=?",
            (int(job_id),),
        )
        conn.commit()

        done = failed = dup = 0
        seq = 0

        cancelled = False
        with zipfile.ZipFile(zip_path, "r") as z:
            # Scan once and update total here (background). This avoids an extra pass in /upload_zip.
            entries: list[zipfile.ZipInfo] = []
            for info in z.infolist():
                if info.is_dir():
                    continue
                base = os.path.basename(info.filename or "")
                if not base or not _allowed_image_ext(base):
                    continue
                entries.append(info)

            conn.execute(
                "UPDATE upload_zip_jobs SET total=?, updated_at_utc=datetime('now') WHERE id=?",
                (int(len(entries)), int(job_id)),
            )
            conn.execute(
                "UPDATE upload_zip_jobs SET status='running', updated_at_utc=datetime('now') WHERE id=?",
                (int(job_id),),
            )
            conn.commit()

            last_commit = 0
            for info in entries:
                # Cancel check (throttled)
                if (seq % 50) == 0:
                    strow = conn.execute("SELECT status FROM upload_zip_jobs WHERE id=?", (int(job_id),)).fetchone()
                    if strow and str(strow["status"] or "") == "cancelled":
                        cancelled = True
                        break

                base = os.path.basename(info.filename or "")
                seq += 1
                state_txt = "完了"
                image_id = None
                msg = None
                try:
                    raw = z.read(info)
                    if not raw:
                        raise RuntimeError("empty")
                    mime = mimetypes.guess_type(base)[0] or "application/octet-stream"
                    mtime_iso = _zipinfo_mtime_iso(info)
                    res = _upload_image_core(
                        conn=conn,
                        bg=None,
                        raw=raw,
                        filename=base,
                        mime=mime,
                        mtime_iso=mtime_iso,
                        user_id=int(user_id),
                        username=str(username),
                    )
                    if res.get("dedup"):
                        state_txt = "重複"
                        dup += 1
                    else:
                        done += 1
                    image_id = res.get("image_id")
                except Exception as e:
                    state_txt = "失敗"
                    failed += 1
                    msg = f"{type(e).__name__}"

                conn.execute(
                    "INSERT INTO upload_zip_items(job_id, seq, filename, state, image_id, message) VALUES (?,?,?,?,?,?)",
                    (int(job_id), int(seq), base, state_txt, image_id, msg),
                )

                # Batch progress update/commit (reduce sqlite overhead)
                if (seq - last_commit) >= 10:
                    conn.execute(
                        "UPDATE upload_zip_jobs SET done=?, failed=?, dup=?, updated_at_utc=datetime('now') WHERE id=?",
                        (int(done), int(failed), int(dup), int(job_id)),
                    )
                    conn.commit()
                    last_commit = seq

            # final progress write
            conn.execute(
                "UPDATE upload_zip_jobs SET done=?, failed=?, dup=?, updated_at_utc=datetime('now') WHERE id=?",
                (int(done), int(failed), int(dup), int(job_id)),
            )
            conn.commit()

        conn.execute(
            "UPDATE upload_zip_jobs SET status=?, updated_at_utc=datetime('now') WHERE id=?",
            ("cancelled" if cancelled else "done", int(job_id)),
        )
        conn.commit()
    except Exception as e:
        try:
            conn.execute(
                "UPDATE upload_zip_jobs SET status='error', error=?, updated_at_utc=datetime('now') WHERE id=?",
                (f"{type(e).__name__}: {e}", int(job_id)),
            )
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()
        try:
            os.remove(zip_path)
        except Exception:
            pass
DERIV_VERSION = "18"

def _ensure_derivatives(image_id: int, kinds: tuple[str, ...] = ("grid", "overlay")) -> None:
    conn = get_conn()
    try:
        src = _read_image_bytes(conn, image_id)
        if not src:
            return

        # Derivative policy
        # - grid: *lossy* and small for list
        # - overlay: higher quality for detail preview
        targets = {
            "grid": {"max_side": 320, "quality": 70},
            "overlay": {"max_side": 1400, "quality": 82},
        }

        def needs_refresh(kind: str, fmt: str) -> bool:
            t = targets[kind]
            r = conn.execute(
                "SELECT width, height, quality, bytes FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
                (image_id, kind, fmt),
            ).fetchone()
            if not r:
                return True
            try:
                q = int(r["quality"] or 0)
                w = int(r["width"] or 0)
                h = int(r["height"] or 0)
                b = r["bytes"]
            except Exception:
                return True
            if isinstance(b, memoryview):
                b = b.tobytes()
            if not b or len(b) < 128:
                return True
            # quick container sanity (very light)
            if fmt == "webp":
                if not (b[:4] == b"RIFF" and b[8:12] == b"WEBP"):
                    return True
            if fmt == "avif":
                if b[4:12] not in (b"ftypavif", b"ftypavis"):
                    return True
            if q != int(t["quality"]):
                return True
            if max(w, h) > int(t["max_side"]) + 2:
                return True
            return False

        do_avif = avif_available()

        for kind in kinds:
            max_side = int(targets[kind]["max_side"])
            quality = int(targets[kind]["quality"])

            # AVIF first (only when server supports encoding)
            if do_avif:
                try:
                    if needs_refresh(kind, "avif"):
                        b, w, h = make_avif_derivative(src, max_side=max_side, quality=quality)
                        conn.execute(
                            "INSERT OR REPLACE INTO image_derivatives(image_id, kind, format, width, height, quality, bytes) VALUES (?,?,?,?,?,?,?)",
                            (image_id, kind, "avif", w, h, quality, b),
                        )
                except Exception:
                    # If encode fails for some specific image, keep WEBP fallback.
                    pass

            # Always keep WEBP fallback
            try:
                if needs_refresh(kind, "webp"):
                    b, w, h = make_webp_derivative(src, max_side=max_side, quality=quality)
                    conn.execute(
                        "INSERT OR REPLACE INTO image_derivatives(image_id, kind, format, width, height, quality, bytes) VALUES (?,?,?,?,?,?,?)",
                        (image_id, kind, "webp", w, h, quality, b),
                    )
            except Exception:
                pass

        conn.commit()
    finally:
        conn.close()

@api_router.get("/images")

def list_images(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,  # comma-separated canonical tags
    date_from: str | None = None,  # YYYY-MM-DD
    date_to: str | None = None,    # YYYY-MM-DD
    dedup_only: int = 0,
    limit: int = 60,
    cursor: int | None = None,
    user: dict = Depends(get_user),
):
    limit = max(1, min(200, int(limit)))
    conn = get_conn()
    try:
        creator_id: int | None = None
        if creator:
            r = conn.execute("SELECT id FROM users WHERE username=?", (creator,)).fetchone()
            if not r:
                return {"items": [], "next_cursor": None}
            creator_id = int(r["id"])

        q = """
        SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
               images.favorite, images.is_nsfw,
               users.username AS creator
        FROM images
        JOIN users ON users.id = images.uploader_user_id
        """
        where = []
        params: list = []

        if creator_id is not None:
            where.append("images.uploader_user_id = ?")
            params.append(creator_id)
        if software:
            where.append("images.software = ?")
            params.append(software)
        if date_from:
            where.append("substr(images.file_mtime_utc,1,10) >= ?")
            params.append(date_from)
        if date_to:
            where.append("substr(images.file_mtime_utc,1,10) <= ?")
            params.append(date_to)
        if dedup_only:
            where.append("images.dedup_flag = 1")

        tag_list = []
        if tags:
            tag_list = [normalize_tag(t) for t in tags.split(",") if t.strip()]
        if tag_list:
            # AND search: image must have all tags
            # Use intersection by grouping
            q += " JOIN image_tags ON image_tags.image_id = images.id "
            where.append("image_tags.tag_canonical IN (%s)" % ",".join(["?"] * len(tag_list)))
            params.extend(tag_list)
            q += " "

        if where:
            q += " WHERE " + " AND ".join(where)

        # pagination
        if cursor is not None:
            q += (" AND " if where else " WHERE ") + " images.id < ? "
            params.append(int(cursor))

        q += " GROUP BY images.id "
        if tag_list:
            q += " HAVING COUNT(DISTINCT image_tags.tag_canonical) = ? "
            params.append(len(set(tag_list)))

        q += " ORDER BY images.file_mtime_utc DESC, images.id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(q, params).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "w": r["width"],
                "h": r["height"],
                "mtime": r["file_mtime_utc"],
                "software": r["software"],
                "creator": r["creator"],
                "dedup_flag": r["dedup_flag"],
                "favorite": int(r["favorite"] or 0),
                "is_nsfw": int(r["is_nsfw"] or 0),
                "thumb": f"/api/images/{r['id']}/thumb?kind=grid&v={DERIV_VERSION}",
            })
        next_cursor = out[-1]["id"] if out else None
        return {"items": out, "next_cursor": next_cursor}
    finally:
        conn.close()


@api_router.get("/images_page")
def list_images_page(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    dedup_only: int = 0,
    fav_only: int = 0,
    sort: str = "newest",
    page: int = 1,
    limit: int = 16,
    include_total: int = 1,
    user: dict = Depends(get_user),
):
    """Offset-based paging for gallery UI.

    Kept separate from /images (cursor) so older clients keep working.
    """
    limit = max(1, min(72, int(limit)))
    page = max(1, int(page))
    offset = (page - 1) * limit

    sort_key = (sort or "newest").lower()
    if sort_key not in {"newest", "oldest", "favorite"}:
        sort_key = "newest"

    tag_list: list[str] = []
    if tags:
        tag_list = [normalize_tag(t) for t in tags.split(",") if t.strip()]
        tag_list = list(dict.fromkeys(tag_list))

    conn = get_conn()
    try:
        creator_id: int | None = None
        if creator:
            r = conn.execute("SELECT id FROM users WHERE username=?", (creator,)).fetchone()
            if not r:
                return {
                    "items": [],
                    "page": page,
                    "limit": limit,
                    "total_count": 0,
                    "total_pages": 0,
                    "sort": sort_key,
                    "fav_only": int(1 if fav_only else 0),
                }
            creator_id = int(r["id"])

        where: list[str] = []
        params: list = []
        if creator_id is not None:
            where.append("images.uploader_user_id = ?")
            params.append(creator_id)
        if software:
            where.append("images.software = ?")
            params.append(software)
        if date_from:
            where.append("images.file_mtime_utc >= ?")
            params.append(date_from)
        if date_to:
            try:
                dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
                where.append("images.file_mtime_utc < ?")
                params.append(dt.strftime("%Y-%m-%d"))
            except Exception:
                where.append("substr(images.file_mtime_utc,1,10) <= ?")
                params.append(date_to)
        if dedup_only:
            where.append("images.dedup_flag = 1")
        if fav_only:
            where.append("images.favorite = 1")
        for t in tag_list:
            where.append("EXISTS (SELECT 1 FROM image_tags it WHERE it.image_id = images.id AND it.tag_canonical = ?)")
            params.append(t)

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""

        if sort_key == "oldest":
            order_sql = " ORDER BY images.file_mtime_utc ASC, images.id ASC "
        elif sort_key == "favorite":
            order_sql = " ORDER BY images.favorite DESC, images.file_mtime_utc DESC, images.id DESC "
        else:
            order_sql = " ORDER BY images.file_mtime_utc DESC, images.id DESC "

        total = None
        total_pages = None
        if int(include_total):
            cnt_row = conn.execute(
                "SELECT COUNT(*) AS n FROM images" + where_sql,
                params,
            ).fetchone()
            total = int(cnt_row["n"] if cnt_row else 0)
            total_pages = int(math.ceil(total / float(limit))) if total else 0

        rows = conn.execute(
            """
            SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
                   images.favorite, images.is_nsfw,
                   users.username AS creator, images.original_filename AS filename
            FROM images
            JOIN users ON users.id = images.uploader_user_id
            """ + where_sql +
            order_sql + " LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()

        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "w": r["width"],
                "h": r["height"],
                "mtime": r["file_mtime_utc"],
                "software": r["software"],
                "creator": r["creator"],
                "filename": r["filename"],
                "dedup_flag": r["dedup_flag"],
                "favorite": int(r["favorite"] or 0),
                "is_nsfw": int(r["is_nsfw"] or 0),
                "thumb": f"/api/images/{r['id']}/thumb?kind=grid&v={DERIV_VERSION}",
            })

        return {
            "items": out,
            "page": page,
            "limit": limit,
            "total_count": total,
            "total_pages": total_pages,
            "sort": sort_key,
            "fav_only": int(1 if fav_only else 0),
        }
    finally:
        conn.close()


def _parse_scroll_cursor(sort_key: str, cursor: str | None):
    if not cursor:
        return None
    try:
        parts = cursor.split("|")
        if sort_key == "favorite":
            if len(parts) != 3:
                return None
            fav = int(parts[0])
            mtime = parts[1]
            iid = int(parts[2])
            return (fav, mtime, iid)
        if len(parts) != 2:
            return None
        mtime = parts[0]
        iid = int(parts[1])
        return (mtime, iid)
    except Exception:
        return None


def _make_scroll_cursor(sort_key: str, row: sqlite3.Row) -> str:
    if sort_key == "favorite":
        fav = int(row["favorite"] or 0)
        mtime = row["file_mtime_utc"] or ""
        iid = int(row["id"])
        return f"{fav}|{mtime}|{iid}"
    mtime = row["file_mtime_utc"] or ""
    iid = int(row["id"])
    return f"{mtime}|{iid}"


@api_router.get("/images_scroll")
def list_images_scroll(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    dedup_only: int = 0,
    fav_only: int = 0,
    sort: str = "newest",
    # mobile-first: load in smaller chunks by default
    limit: int = 30,
    cursor: str | None = None,
    include_total: int = 1,
    user: dict = Depends(get_user),
):
    """Cursor-based list for gallery (includes total_count for UI)."""
    limit = max(1, min(120, int(limit)))

    sort_key = (sort or "newest").lower()
    if sort_key not in {"newest", "oldest", "favorite"}:
        sort_key = "newest"

    tag_list: list[str] = []
    if tags:
        tag_list = [normalize_tag(t) for t in tags.split(",") if t.strip()]
        tag_list = list(dict.fromkeys(tag_list))

    conn = get_conn()
    try:
        creator_id: int | None = None
        if creator:
            r = conn.execute("SELECT id FROM users WHERE username=?", (creator,)).fetchone()
            if not r:
                return {
                    "items": [],
                    "next_cursor": None,
                    "sort": sort_key,
                    "fav_only": int(1 if fav_only else 0),
                    "total_count": 0,
                }
            creator_id = int(r["id"])

        where: list[str] = []
        params: list = []
        if creator_id is not None:
            where.append("images.uploader_user_id = ?")
            params.append(creator_id)
        if software:
            where.append("images.software = ?")
            params.append(software)
        if date_from:
            where.append("images.file_mtime_utc >= ?")
            params.append(date_from)
        if date_to:
            try:
                dt = datetime.strptime(date_to, "%Y-%m-%d") + timedelta(days=1)
                where.append("images.file_mtime_utc < ?")
                params.append(dt.strftime("%Y-%m-%d"))
            except Exception:
                where.append("substr(images.file_mtime_utc,1,10) <= ?")
                params.append(date_to)
        if dedup_only:
            where.append("images.dedup_flag = 1")
        if fav_only:
            where.append("images.favorite = 1")
        for t in tag_list:
            where.append("EXISTS (SELECT 1 FROM image_tags it WHERE it.image_id = images.id AND it.tag_canonical = ?)")
            params.append(t)

        # Save a copy *before* applying cursor constraints.
        where_base = list(where)
        params_base = list(params)

        cur_tuple = _parse_scroll_cursor(sort_key, cursor)
        if cur_tuple is not None:
            if sort_key == "oldest":
                where.append("(images.file_mtime_utc > ? OR (images.file_mtime_utc = ? AND images.id > ?))")
                params.extend([cur_tuple[0], cur_tuple[0], cur_tuple[1]])
            elif sort_key == "favorite":
                fav, mtime, iid = cur_tuple
                where.append(
                    "(images.favorite < ? OR "
                    "(images.favorite = ? AND images.file_mtime_utc < ?) OR "
                    "(images.favorite = ? AND images.file_mtime_utc = ? AND images.id < ?))"
                )
                params.extend([fav, fav, mtime, fav, mtime, iid])
            else:
                where.append("(images.file_mtime_utc < ? OR (images.file_mtime_utc = ? AND images.id < ?))")
                params.extend([cur_tuple[0], cur_tuple[0], cur_tuple[1]])

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        where_sql_base = (" WHERE " + " AND ".join(where_base)) if where_base else ""

        if sort_key == "oldest":
            order_sql = " ORDER BY images.file_mtime_utc ASC, images.id ASC "
        elif sort_key == "favorite":
            order_sql = " ORDER BY images.favorite DESC, images.file_mtime_utc DESC, images.id DESC "
        else:
            order_sql = " ORDER BY images.file_mtime_utc DESC, images.id DESC "

        total_count = None
        if int(include_total):
            total_count = conn.execute(
                "SELECT COUNT(1) AS c FROM images" + where_sql_base,
                params_base,
            ).fetchone()[0]

        rows = conn.execute(
            """
            SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
                   images.favorite, images.is_nsfw,
                   users.username AS creator, images.original_filename AS filename
            FROM images
            JOIN users ON users.id = images.uploader_user_id
            """ + where_sql + order_sql + " LIMIT ?",
            params + [limit],
        ).fetchall()

        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "w": r["width"],
                "h": r["height"],
                "mtime": r["file_mtime_utc"],
                "software": r["software"],
                "creator": r["creator"],
                "filename": r["filename"],
                "dedup_flag": r["dedup_flag"],
                "favorite": int(r["favorite"] or 0),
                "is_nsfw": int(r["is_nsfw"] or 0),
                "thumb": f"/api/images/{r['id']}/thumb?kind=grid&v={DERIV_VERSION}",
            })

        next_cursor = _make_scroll_cursor(sort_key, rows[-1]) if rows else None
        return {
            "items": out,
            "next_cursor": next_cursor,
            "sort": sort_key,
            "fav_only": int(1 if fav_only else 0),
            "total_count": (int(total_count) if total_count is not None else None),
        }
    finally:
        conn.close()

@api_router.get("/images/{image_id}/thumb")
def get_thumb(image_id: int, request: Request, kind: str = "grid", user: dict = Depends(get_user)):
    if kind not in {"grid", "overlay"}:
        raise HTTPException(status_code=400, detail="kind must be grid/overlay")

    # Prefer AVIF when the browser supports it; fall back to WEBP.
    accept = (request.headers.get("accept") or "").lower()
    fmts = ["webp"]
    if "image/avif" in accept and avif_available():
        fmts = ["avif", "webp"]

    # match _ensure_derivatives targets
    targets = {
        "grid": {"max_side": 320, "quality": 70},
        "overlay": {"max_side": 1400, "quality": 82},
    }

    def row_needs_refresh(r, fmt: str) -> bool:
        if not r:
            return True
        t = targets[kind]
        try:
            q = int(r["quality"] or 0)
            w = int(r["width"] or 0)
            h = int(r["height"] or 0)
            b = r["bytes"]
        except Exception:
            return True
        if isinstance(b, memoryview):
            b = b.tobytes()
        if not b or len(b) < 128:
            return True
        if fmt == "webp":
            if not (b[:4] == b"RIFF" and b[8:12] == b"WEBP"):
                return True
        if fmt == "avif":
            if b[4:12] not in (b"ftypavif", b"ftypavis"):
                return True
        if q != int(t["quality"]):
            return True
        if max(w, h) > int(t["max_side"]) + 2:
            return True
        return False

    conn = get_conn()
    try:
        chosen = None
        chosen_fmt = None

        # try preferred formats
        for fmt in fmts:
            row = conn.execute(
                "SELECT format, width, height, quality, bytes, created_at_utc FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
                (image_id, kind, fmt),
            ).fetchone()
            if row_needs_refresh(row, fmt):
                _ensure_derivatives(image_id, (kind,))
                row = conn.execute(
                    "SELECT format, width, height, quality, bytes, created_at_utc FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
                    (image_id, kind, fmt),
                ).fetchone()
            if row and not row_needs_refresh(row, fmt):
                chosen = row
                chosen_fmt = fmt
                break

        if not chosen:
            raise HTTPException(status_code=404, detail="thumb not ready")

        b = chosen["bytes"]
        if isinstance(b, memoryview):
            b = b.tobytes()

        etag = f'W/"deriv-{image_id}-{kind}-{chosen_fmt}-{chosen["quality"]}-{chosen["width"]}x{chosen["height"]}-{chosen["created_at_utc"]}"'
        headers = {
            # keep it cacheable but allow recovery from regeneration within a reasonable time
            "Cache-Control": "private, max-age=604800",
            "ETag": etag,
            "Vary": "Accept",
        }
        inm = request.headers.get("if-none-match")
        if inm and inm == etag:
            return Response(status_code=304, headers=headers)

        mt = "image/webp" if chosen_fmt == "webp" else "image/avif"
        return Response(content=b, media_type=mt, headers=headers)
    finally:
        conn.close()



class PrefetchDerivativesReq(BaseModel):
    ids: list[int] = []
    kind: str = "overlay"  # overlay / grid / both


@api_router.post("/cache/prefetch_derivatives")
def prefetch_derivatives(req: PrefetchDerivativesReq, bg: BackgroundTasks, user: dict = Depends(get_user)):
    kind = (req.kind or "overlay").lower()
    if kind not in {"overlay", "grid", "both"}:
        kind = "overlay"

    ids: list[int] = []
    for x in (req.ids or []):
        try:
            iid = int(x)
            if iid > 0:
                ids.append(iid)
        except Exception:
            continue

    # Keep it small: this runs in background tasks in the same process.
    ids = list(dict.fromkeys(ids))[:48]

    if kind == "both":
        for iid in ids:
            bg.add_task(_ensure_derivatives, iid)
    else:
        for iid in ids:
            bg.add_task(_ensure_derivatives, iid, (kind,))

    return {"ok": True, "n": len(ids), "kind": kind}

@api_router.get("/images/{image_id}/file")

def download_original(image_id: int, request: Request, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT images.original_filename AS fn, images.mime AS mime, images.sha256 AS sha, image_files.disk_path AS p, image_files.bytes AS bytes "
            "FROM images JOIN image_files ON image_files.image_id=images.id WHERE images.id=?",
            (image_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        fn = row["fn"] or f"{image_id}.bin"
        mime = row["mime"] or "application/octet-stream"
        etag = f'"{row["sha"]}"'
        headers = {
            "Content-Disposition": f'attachment; filename="{fn}"',
            "Cache-Control": "private, max-age=31536000, immutable",
            "ETag": etag,
        }
        if request is not None:
            inm = request.headers.get("if-none-match")
            if inm and inm == etag:
                return Response(status_code=304, headers=headers)

        p = row["p"]
        if p and os.path.exists(p):
            return FileResponse(path=p, media_type=mime, headers=headers)

        # Legacy fallback: bytes in DB (and export to disk for future)
        b = row["bytes"]
        if isinstance(b, memoryview):
            b = b.tobytes()
        p2 = _ensure_original_on_disk(conn, image_id)
        conn.commit()
        if p2 and os.path.exists(p2):
            return FileResponse(path=p2, media_type=mime, headers=headers)
        return Response(content=b or b"", media_type=mime, headers=headers)
    finally:
        conn.close()


@api_router.get("/images/{image_id}/view")
def view_original(image_id: int, user: dict = Depends(get_user)):
    """Inline view for full size image (useful for browser zoom)."""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT images.original_filename AS fn, images.mime AS mime, image_files.disk_path AS p, image_files.bytes AS bytes "
            "FROM images JOIN image_files ON image_files.image_id=images.id WHERE images.id=?",
            (image_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        fn = row["fn"] or f"image_{image_id}"
        mime = row["mime"] or "application/octet-stream"
        headers = {"Content-Disposition": f'inline; filename="{fn}"'}

        p = row["p"]
        if p and os.path.exists(p):
            return FileResponse(path=p, media_type=mime, headers=headers)

        b = row["bytes"]
        if isinstance(b, memoryview):
            b = b.tobytes()
        p2 = _ensure_original_on_disk(conn, image_id)
        conn.commit()
        if p2 and os.path.exists(p2):
            return FileResponse(path=p2, media_type=mime, headers=headers)
        return Response(content=b or b"", media_type=mime, headers=headers)
    finally:
        conn.close()

@api_router.get("/images/{image_id}/metadata_json")
def download_metadata(image_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT prompt_positive_raw, prompt_negative_raw, prompt_character_raw, params_json, software, model_name, metadata_raw, has_potion FROM images WHERE id=?",
            (image_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        payload = {
            "software": row["software"],
            "model": row["model_name"],
            "prompt_positive_raw": row["prompt_positive_raw"],
            "prompt_negative_raw": row["prompt_negative_raw"],
            "prompt_character_raw": row["prompt_character_raw"],
            "params": json.loads(row["params_json"]) if row["params_json"] else None,
            "metadata_raw": row["metadata_raw"],
            "has_potion": bool(row["has_potion"]),
        }
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        return Response(content=data, media_type="application/json", headers={"Content-Disposition": f'attachment; filename="image_{image_id}_metadata.json"'})
    finally:
        conn.close()

@api_router.get("/images/{image_id}/potion")
def download_potion(image_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute("SELECT potion_raw, has_potion FROM images WHERE id=?", (image_id,)).fetchone()
        if not row or int(row["has_potion"]) != 1:
            raise HTTPException(status_code=404, detail="no potion")
        data = row["potion_raw"] or b""
        return Response(content=data, media_type="application/json", headers={"Content-Disposition": f'attachment; filename="image_{image_id}_potion.json"'})
    finally:
        conn.close()

@api_router.get("/images/{image_id}/detail")
def image_detail(image_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        img = conn.execute(
            """
            SELECT images.*, users.username AS creator, COALESCE(image_files.size, LENGTH(image_files.bytes)) AS file_bytes
            FROM images
            JOIN users ON users.id=images.uploader_user_id
            JOIN image_files ON image_files.image_id = images.id
            WHERE images.id=?
            """,
            (image_id,),
        ).fetchone()
        if not img:
            raise HTTPException(status_code=404, detail="not found")
        has_seq = _table_has_col(conn, "image_tags", "seq")
        cols = "tag_canonical, tag_text, tag_raw, category, emphasis_type, brace_level, numeric_weight"
        if has_seq:
            cols += ", seq"
        order_sql = " ORDER BY category ASC, tag_canonical ASC"
        if has_seq:
            order_sql = " ORDER BY seq ASC, category ASC, tag_canonical ASC"
        tags = conn.execute(
            f"SELECT {cols} FROM image_tags WHERE image_id=?" + order_sql,
            (image_id,),
        ).fetchall()

        # IMPORTANT: Display grouping must be **category-based**.
        # Do NOT use src_mask (prompt origin) for UI classification.
        grouped = {"artist": [], "quality": [], "character": [], "other": []}
        for t in tags:
            canonical = t["tag_canonical"]
            cat = int(t["category"]) if t["category"] is not None else None
            group = _category_group(conn, canonical, cat)
            grouped[group].append({
                "canonical": canonical,
                "text": t["tag_text"] or canonical,
                "raw_one": t["tag_raw"] or canonical,
                "emphasis_type": t["emphasis_type"],
                "brace_level": t["brace_level"],
                "numeric_weight": t["numeric_weight"],
            })

        return {
            "id": img["id"],
            "filename": img["original_filename"],
            "mime": img["mime"],
            "file_bytes": int(img["file_bytes"] or 0),
            "w": img["width"],
            "h": img["height"],
            "mtime": img["file_mtime_utc"],
            "uploaded_at": img["uploaded_at_utc"],
            "software": img["software"],
            "model": img["model_name"],
            "creator": img["creator"],
            "dedup_flag": img["dedup_flag"],
            "favorite": int(img["favorite"] or 0),
            "is_nsfw": int(img["is_nsfw"] or 0),
            "prompt_positive_raw": img["prompt_positive_raw"],
            "prompt_negative_raw": img["prompt_negative_raw"],
            "prompt_character_raw": img["prompt_character_raw"],
            "params_json": img["params_json"],
            "has_potion": bool(img["has_potion"]),
            "tags": grouped,
            "overlay": f"/api/images/{image_id}/thumb?kind=overlay&v={DERIV_VERSION}",
            "view_full": f"/api/images/{image_id}/view",
            "download_file": f"/api/images/{image_id}/file",
            "download_meta": f"/api/images/{image_id}/metadata_json",
            "download_potion": f"/api/images/{image_id}/potion",
        }
    finally:
        conn.close()


class FavReq(BaseModel):
    favorite: int | None = None
    toggle: bool = False


@api_router.post("/images/{image_id}/favorite")
def set_favorite(image_id: int, req: FavReq, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute("SELECT favorite FROM images WHERE id=?", (image_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        cur = int(row["favorite"] or 0)
        if req.toggle:
            nxt = 0 if cur else 1
        elif req.favorite is None:
            nxt = cur
        else:
            nxt = 1 if int(req.favorite) else 0
        conn.execute("UPDATE images SET favorite=? WHERE id=?", (nxt, image_id))
        conn.commit()
        return {"ok": True, "favorite": nxt}
    finally:
        conn.close()