from __future__ import annotations

import os
import shutil
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]

def _parse_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            if "=" not in s:
                continue
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                continue
            if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                v = v[1:-1]
            out[k] = v
    except Exception:
        return out
    return out

def _ensure_dotenv() -> None:
    env_path = ROOT_DIR / ".env"
    tpl_path = ROOT_DIR / ".env.template"
    if (not env_path.exists()) and tpl_path.exists():
        try:
            shutil.copyfile(str(tpl_path), str(env_path))
            print("[nim] created .env from .env.template")
        except Exception:
            pass
    if env_path.exists() and env_path.stat().st_size > 0:
        for k, v in _parse_dotenv(env_path).items():
            if k not in os.environ:
                os.environ[k] = v

_ensure_dotenv()


from fastapi import Depends, FastAPI
from fastapi.responses import FileResponse, RedirectResponse, ORJSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from .api import api_router
from .db import ensure_bootstrap, init_db
from .deps import get_user_optional, require_admin
from .db import get_conn
from .services.derivatives import probe_avif

app = FastAPI(
    title="NovelAI Image Manager",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
)

# Compress JSON payloads (especially helpful over cloudflared tunnels).
app.add_middleware(GZipMiddleware, minimum_size=900)

@app.on_event("startup")
def _startup() -> None:
    init_db()
    ensure_bootstrap()

    ok, err = probe_avif()
    if ok:
        print("[nim] AVIF encoder: OK")
    else:
        # Make the cause visible; otherwise users only see "why is it always webp".
        # Typical fix: install `pillow-avif-plugin` into the venv.
        print(f"[nim] AVIF encoder: UNAVAILABLE ({err})")

app.include_router(api_router, prefix="/api")

WEB_DIR = ROOT_DIR / "server" / "web"

def _file(name: str) -> FileResponse:
    return FileResponse(str(WEB_DIR / name))

@app.get("/")
def _root(user: dict | None = Depends(get_user_optional)):
    if not user:
        return RedirectResponse(url="/login.html", status_code=302)
    return _file("index.html")

@app.get("/index.html")
def _index(user: dict | None = Depends(get_user_optional)):
    if not user:
        return RedirectResponse(url="/login.html", status_code=302)
    return _file("index.html")

@app.get("/login.html")
def _login(user: dict | None = Depends(get_user_optional)):
    if user:
        return RedirectResponse(url="/", status_code=302)
    # If no users exist, redirect to first-time setup.
    conn = get_conn()
    try:
        n = int(conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
    finally:
        conn.close()
    if n == 0:
        return RedirectResponse(url="/setup.html", status_code=302)
    return _file("login.html")


@app.get("/setup.html")
def _setup(user: dict | None = Depends(get_user_optional)):
    if user:
        return RedirectResponse(url="/", status_code=302)
    conn = get_conn()
    try:
        n = int(conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
    finally:
        conn.close()
    if n != 0:
        return RedirectResponse(url="/login.html", status_code=302)
    return _file("setup.html")


@app.get("/set-password.html")
def _set_password(user: dict | None = Depends(get_user_optional)):
    # Password tokens are verified by API; page is public.
    if user:
        return _file("set-password.html")
    return _file("set-password.html")


@app.get("/admin.html")
def _admin_page(user: dict | None = Depends(get_user_optional)):
    if not user:
        return RedirectResponse(url="/login.html", status_code=302)
    if user.get("role") not in {"admin", "master"}:
        return RedirectResponse(url="/", status_code=302)
    return _file("admin.html")


@app.get("/maintenance.html")
def _maintenance_page(user: dict | None = Depends(get_user_optional)):
    if not user:
        return RedirectResponse(url="/login.html", status_code=302)
    if user.get("role") not in {"admin", "master"}:
        return RedirectResponse(url="/", status_code=302)
    return _file("maintenance.html")

# Static UI assets
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
