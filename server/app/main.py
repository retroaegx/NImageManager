from __future__ import annotations

import os
import mimetypes
import re
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

mimetypes.add_type("image/avif", ".avif")

from shared.dotenv_utils import ensure_dotenv

ensure_dotenv(ROOT_DIR, log_prefix="[nim]")


from fastapi import Depends, FastAPI, Request
from fastapi.responses import FileResponse, RedirectResponse, ORJSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware

from . import api as api_module
from .db import ensure_bootstrap, init_db, PUBLIC_THUMBS_DIR
from .deps import get_user_optional, require_admin
from .db import get_conn
from .services.derivatives import probe_avif
from .services.update_checker import start_update_checker, stop_update_checker
from .logging_utils import configure_perf_logging, log_perf, new_trace_id, perf_log_path, perf_logging_enabled

app = FastAPI(
    title="NovelAI Image Manager",
    docs_url="/api/docs",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
)

# Compress JSON payloads (especially helpful over cloudflared tunnels).
app.add_middleware(GZipMiddleware, minimum_size=900)


@app.middleware("http")
async def _perf_request_middleware(request: Request, call_next):
    trace_id = request.headers.get("x-nim-client-trace-id") or new_trace_id()
    request.state.trace_id = trace_id
    start = time.perf_counter()
    response = None
    exc_name = None
    try:
        response = await call_next(request)
        return response
    except Exception as exc:
        exc_name = exc.__class__.__name__
        raise
    finally:
        duration_ms = round((time.perf_counter() - start) * 1000.0, 3)
        if response is not None:
            try:
                response.headers["X-NIM-Trace-Id"] = trace_id
            except Exception:
                pass
        path = request.url.path
        should_log = (
            path == "/api/debug/perf"
            or path == "/api/cache/prefetch_derivatives"
            or (path.startswith("/api/images/") and path.endswith("/detail"))
        )
        if should_log:
            log_perf(
                "http_request",
                trace_id=trace_id,
                method=request.method,
                path=path,
                query=request.url.query or "",
                status=(response.status_code if response is not None else 500),
                duration_ms=duration_ms,
                client_trace_id=request.headers.get("x-nim-client-trace-id") or None,
                detail_source=request.headers.get("x-nim-detail-source") or None,
                detail_page=request.headers.get("x-nim-detail-page") or None,
                detail_mode=request.headers.get("x-nim-detail-mode") or None,
                exc=exc_name,
            )


@app.on_event("startup")
def _startup() -> None:
    configure_perf_logging()
    init_db()
    ensure_bootstrap()
    api_module.publish_existing_public_thumbs()
    api_module.start_background_workers()
    start_update_checker()

    if perf_logging_enabled():
        print(f"[nim] perf log: {perf_log_path()}")

    ok, err = probe_avif()
    if ok:
        print("[nim] AVIF encoder: OK")
    else:
        # Make the cause visible; otherwise users only see "why is it always webp".
        # Typical fix: install `pillow-avif-plugin` into the venv.
        print(f"[nim] AVIF encoder: UNAVAILABLE ({err})")


@app.on_event("shutdown")
def _shutdown() -> None:
    stop_update_checker()
    api_module.stop_background_workers()


app.include_router(api_module.api_router, prefix="/api")

WEB_DIR = ROOT_DIR / "server" / "web"

def _asset_version(name: str) -> str:
    path = WEB_DIR / name
    try:
        return str(path.stat().st_mtime_ns)
    except Exception:
        return "0"


def _html_with_asset_versions(name: str) -> HTMLResponse:
    path = WEB_DIR / name
    html = path.read_text(encoding="utf-8")
    repls = {
        "styles.css": _asset_version("styles.css"),
        "app.js": _asset_version("app.js"),
        "admin.js": _asset_version("admin.js"),
        "maintenance.js": _asset_version("maintenance.js"),
        "settings.js": _asset_version("settings.js"),
        "login.js": _asset_version("login.js"),
        "setup.js": _asset_version("setup.js"),
        "set-password.js": _asset_version("set-password.js"),
    }
    for asset, version in repls.items():
        html = re.sub(
            rf"([\"'])/({re.escape(asset)})(?:\?v=[^\"']*)?([\"'])",
            rf'\1/\2?v={version}\3',
            html,
        )
    return HTMLResponse(content=html)


def _file(name: str):
    path = WEB_DIR / name
    if path.suffix.lower() == ".html":
        return _html_with_asset_versions(name)
    return FileResponse(str(path))

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


@app.get("/settings.html")
def _settings(user: dict | None = Depends(get_user_optional)):
    if not user:
        return RedirectResponse(url="/login.html", status_code=302)
    return _file("settings.html")


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
PUBLIC_THUMBS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/thumbs", StaticFiles(directory=str(PUBLIC_THUMBS_DIR), html=False), name="thumbs")
app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
