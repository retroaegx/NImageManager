from __future__ import annotations

import io
import re
import os
import json
import sqlite3
import csv
import math
import threading
import queue
import fnmatch
import time
import unicodedata
import mimetypes
import shutil
import tempfile
import secrets
from datetime import datetime, timezone, timedelta
from typing import List, Optional
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile, Request
from fastapi.responses import Response, StreamingResponse, FileResponse, JSONResponse
from pydantic import BaseModel, Field, ValidationError

from .db import get_conn, ORIGINALS_DIR, DERIVATIVES_DIR, PUBLIC_THUMBS_DIR, ASSETS_DIR
from .deps import get_user, get_user_optional, require_admin, require_master
from .security import create_token, verify_password, hash_password
from .services.metadata_extract import (
    extract_novelai_metadata,
    extract_novelai_metadata_bytes,
    detect_generation_usage_from_storage,
)
from .services.tag_parser import parse_tag_list, normalize_tag, main_sig_hash
from .services.derivatives import (
    make_webp_derivative,
    make_avif_derivative,
    avif_available,
    avif_probe_error,
    decode_source_image,
    make_resized_variant,
    encode_webp_image,
    encode_avif_image,
    derivative_targets,
)
from .services import stats as stats_service
from .services.prompt_view import (
    build_prompt_view_payload,
    ensure_prompt_view_cache,
    parse_prompt_multiline_to_tag_objs,
)
from .services.gallery_query import (
    apply_common_filters,
    build_user_bookmark_join,
    normalize_bookmark_list_id,
    normalize_gallery_filters,
    resolve_creator_id,
)
from .services.derivative_queue import enqueue_derivative_job, enqueue_upload_item_job, start_derivative_worker, stop_derivative_worker
from .logging_utils import log_perf, perf_logging_enabled
from .services.update_checker import get_update_status

api_router = APIRouter()


def _cookie_secure_flag(request: Request) -> bool:
    override = os.getenv("NAI_IM_COOKIE_SECURE", "").strip().lower()
    if override in {"1", "true", "yes", "on"}:
        return True
    if override in {"0", "false", "no", "off"}:
        return False
    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",")[0].strip().lower()
    if forwarded_proto == "https":
        return True
    return (request.url.scheme or "").lower() == "https"


def _queue_derivative_request(
    image_id: int,
    kinds: tuple[str, ...] = ("grid", "overlay"),
    *,
    source: str,
    trace_id: str | None = None,
) -> bool:
    return enqueue_derivative_job(
        int(image_id),
        kinds,
        source=str(source or "unknown"),
        trace_id=(trace_id or None),
    )


def _queue_upload_item_request(
    item_id: int,
    *,
    source: str,
    trace_id: str | None = None,
) -> bool:
    return enqueue_upload_item_job(
        int(item_id),
        source=str(source or "unknown"),
        trace_id=(trace_id or None),
    )


DERIVATIVE_TARGETS = derivative_targets()


def _normalize_derivative_kinds(kinds: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in (kinds or ()):  # type: ignore[arg-type]
        kind = str(raw or "").strip().lower()
        if kind not in {"grid", "overlay"}:
            continue
        if kind in seen:
            continue
        seen.add(kind)
        out.append(kind)
    return tuple(out)


_DROP_IMPORT_OBSERVER = None
_DROP_IMPORT_WORKER_THREAD: threading.Thread | None = None
_DROP_IMPORT_STOP = threading.Event()
_DROP_IMPORT_QUEUE: "queue.Queue[Path]" = queue.Queue()
_DROP_IMPORT_PENDING: dict[str, float] = {}
_DROP_IMPORT_PENDING_LOCK = threading.Lock()


def _app_root_dir() -> Path:
    return Path(__file__).resolve().parents[2]


def _drop_import_enabled() -> bool:
    return str(os.getenv("NAI_IM_DROP_IMPORT_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


def _drop_import_settle_sec() -> float:
    raw = str(os.getenv("NAI_IM_DROP_IMPORT_SETTLE_SEC", "3") or "3").strip()
    try:
        return max(0.5, float(raw))
    except Exception:
        return 3.0


def _drop_import_max_depth() -> int:
    raw = str(os.getenv("NAI_IM_DROP_IMPORT_MAX_DEPTH", "1") or "1").strip()
    try:
        return max(0, int(raw))
    except Exception:
        return 1


def _drop_import_root_dir() -> Path | None:
    raw = str(os.getenv("NAI_IM_DROP_IMPORT_DIR", "") or "").strip()
    if not raw:
        return None
    p = Path(os.path.expandvars(os.path.expanduser(raw)))
    if not p.is_absolute():
        p = _app_root_dir() / p
    try:
        return p.resolve(strict=False)
    except Exception:
        return p.absolute()


def _drop_import_path_key(path: Path) -> str:
    try:
        return str(path.resolve(strict=False))
    except Exception:
        return str(path.absolute())


def _drop_import_is_allowed_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in {".png", ".webp"}


def _drop_import_relative_depth(root: Path, path: Path) -> int | None:
    try:
        rel = path.relative_to(root)
    except Exception:
        return None
    return max(0, len(rel.parts) - 1)


def _drop_import_is_watched_path(path: Path) -> bool:
    root = _drop_import_root_dir()
    if root is None:
        return False
    try:
        p = path.resolve(strict=False)
    except Exception:
        return False
    depth = _drop_import_relative_depth(root, p)
    if depth is None:
        return False
    return depth <= _drop_import_max_depth()


def _drop_import_log(message: str) -> None:
    print(f"[nim] drop-import: {message}", flush=True)


def _schedule_drop_import_path(path: Path) -> None:
    if not _drop_import_is_watched_path(path):
        return
    key = _drop_import_path_key(path)
    with _DROP_IMPORT_PENDING_LOCK:
        _DROP_IMPORT_PENDING[key] = time.monotonic()
    _DROP_IMPORT_QUEUE.put(path)


def _drop_import_take_due_paths() -> list[Path]:
    now = time.monotonic()
    due: list[Path] = []
    settle_sec = _drop_import_settle_sec()
    with _DROP_IMPORT_PENDING_LOCK:
        for key, seen_at in list(_DROP_IMPORT_PENDING.items()):
            if now - float(seen_at) < settle_sec:
                continue
            _DROP_IMPORT_PENDING.pop(key, None)
            due.append(Path(key))
    return due


def _drop_import_wait_until_stable(path: Path) -> bool:
    settle_sec = _drop_import_settle_sec()
    stable_since: float | None = None
    last_sig: tuple[int, int] | None = None
    deadline = time.monotonic() + max(15.0, settle_sec * 6.0)
    while not _DROP_IMPORT_STOP.is_set():
        try:
            st = path.stat()
        except FileNotFoundError:
            return False
        except Exception:
            time.sleep(0.2)
            continue
        if not path.is_file() or st.st_size <= 0:
            stable_since = None
            last_sig = None
            if time.monotonic() >= deadline:
                return False
            _DROP_IMPORT_STOP.wait(0.25)
            continue
        sig = (int(st.st_size), int(st.st_mtime_ns))
        if sig != last_sig:
            last_sig = sig
            stable_since = time.monotonic()
        elif stable_since is not None and (time.monotonic() - stable_since) >= settle_sec:
            return True
        if time.monotonic() >= deadline:
            return False
        _DROP_IMPORT_STOP.wait(min(0.5, max(0.2, settle_sec / 2.0)))
    return False


def _record_drop_import_failure(conn: sqlite3.Connection, path: Path, *, error: str) -> None:
    try:
        st = path.stat()
        size = int(st.st_size)
        mtime_ns = int(st.st_mtime_ns)
    except Exception:
        size = -1
        mtime_ns = -1
    conn.execute(
        """
        INSERT INTO drop_import_failures(path, size, mtime_ns, error, failed_at_utc)
        VALUES (?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
          size=excluded.size,
          mtime_ns=excluded.mtime_ns,
          error=excluded.error,
          failed_at_utc=excluded.failed_at_utc
        """,
        (_drop_import_path_key(path), size, mtime_ns, str(error or "")[:500], datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
    )


def _clear_drop_import_failure(conn: sqlite3.Connection, path: Path) -> None:
    conn.execute("DELETE FROM drop_import_failures WHERE path=?", (_drop_import_path_key(path),))


def _drop_import_should_skip_failed(conn: sqlite3.Connection, path: Path) -> bool:
    try:
        st = path.stat()
    except Exception:
        return False
    row = conn.execute(
        "SELECT size, mtime_ns FROM drop_import_failures WHERE path=?",
        (_drop_import_path_key(path),),
    ).fetchone()
    if not row:
        return False
    return int(row["size"] or -1) == int(st.st_size) and int(row["mtime_ns"] or -1) == int(st.st_mtime_ns)


def _get_master_uploader(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT id, username FROM users WHERE role='master' AND disabled=0 ORDER BY id LIMIT 1"
    ).fetchone()


def _process_drop_import_path(path: Path) -> None:
    if not _drop_import_is_watched_path(path):
        return
    if not path.exists():
        return
    if not _drop_import_is_allowed_file(path):
        return
    if not _drop_import_wait_until_stable(path):
        if path.exists():
            _schedule_drop_import_path(path)
        return
    conn = get_conn()
    try:
        if _drop_import_should_skip_failed(conn, path):
            return
        master = _get_master_uploader(conn)
        if not master:
            raise RuntimeError("master user not found")
        st = path.stat()
        res = _upload_image_from_path_core(
            conn=conn,
            bg=None,
            file_path=str(path),
            filename=path.name,
            mime=(mimetypes.guess_type(path.name)[0] or "application/octet-stream"),
            mtime_iso=datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
            user_id=int(master["id"]),
            username=str(master["username"]),
            ensure_derivatives=True,
            derivative_kinds=("grid", "overlay"),
            upload_bookmark_list_id=None,
        )
        if bool(res.get("dedup")):
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        _clear_drop_import_failure(conn, path)
        conn.commit()
        _drop_import_log(f"imported: {path.name} (image_id={int(res.get('image_id') or 0)}, dedup={'yes' if bool(res.get('dedup')) else 'no'})")
    except Exception as exc:
        try:
            _record_drop_import_failure(conn, path, error=f"{type(exc).__name__}: {exc}")
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        _drop_import_log(f"failed: {path.name} ({type(exc).__name__}: {exc})")
    finally:
        conn.close()


def _drop_import_initial_scan() -> None:
    root = _drop_import_root_dir()
    if root is None or not root.exists():
        return
    max_depth = _drop_import_max_depth()
    for p in sorted(root.rglob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in {".png", ".webp"}:
            continue
        depth = _drop_import_relative_depth(root, p)
        if depth is None or depth > max_depth:
            continue
        _schedule_drop_import_path(p)


def _drop_import_worker_loop() -> None:
    while not _DROP_IMPORT_STOP.is_set():
        try:
            _DROP_IMPORT_QUEUE.get(timeout=0.5)
        except queue.Empty:
            pass
        for path in _drop_import_take_due_paths():
            if _DROP_IMPORT_STOP.is_set():
                return
            _process_drop_import_path(path)


def start_drop_import_watcher() -> None:
    global _DROP_IMPORT_OBSERVER, _DROP_IMPORT_WORKER_THREAD
    if not _drop_import_enabled():
        return
    root = _drop_import_root_dir()
    if root is None:
        _drop_import_log("disabled: NAI_IM_DROP_IMPORT_DIR is empty")
        return
    root.mkdir(parents=True, exist_ok=True)
    if _DROP_IMPORT_WORKER_THREAD and _DROP_IMPORT_WORKER_THREAD.is_alive():
        return
    try:
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer
    except Exception as exc:
        _drop_import_log(f"disabled: watchdog unavailable ({exc})")
        return

    class _DropImportHandler(FileSystemEventHandler):
        def on_created(self, event):
            if not event.is_directory:
                _schedule_drop_import_path(Path(event.src_path))

        def on_modified(self, event):
            if not event.is_directory:
                _schedule_drop_import_path(Path(event.src_path))

        def on_moved(self, event):
            if not event.is_directory:
                _schedule_drop_import_path(Path(event.dest_path))

    _DROP_IMPORT_STOP.clear()
    with _DROP_IMPORT_PENDING_LOCK:
        _DROP_IMPORT_PENDING.clear()
    while True:
        try:
            _DROP_IMPORT_QUEUE.get_nowait()
        except queue.Empty:
            break
    observer = Observer()
    observer.schedule(_DropImportHandler(), str(root), recursive=True)
    observer.start()
    worker = threading.Thread(target=_drop_import_worker_loop, name="nim-drop-import", daemon=True)
    worker.start()
    _DROP_IMPORT_OBSERVER = observer
    _DROP_IMPORT_WORKER_THREAD = worker
    _drop_import_log(f"watching: {root} (max_depth={_drop_import_max_depth()}, settle={_drop_import_settle_sec():g}s)")
    _drop_import_initial_scan()


def stop_drop_import_watcher() -> None:
    global _DROP_IMPORT_OBSERVER, _DROP_IMPORT_WORKER_THREAD
    _DROP_IMPORT_STOP.set()
    observer = _DROP_IMPORT_OBSERVER
    worker = _DROP_IMPORT_WORKER_THREAD
    _DROP_IMPORT_OBSERVER = None
    _DROP_IMPORT_WORKER_THREAD = None
    if observer is not None:
        try:
            observer.stop()
            observer.join(timeout=3.0)
        except Exception:
            pass
    if worker is not None:
        try:
            worker.join(timeout=3.0)
        except Exception:
            pass
    with _DROP_IMPORT_PENDING_LOCK:
        _DROP_IMPORT_PENDING.clear()
    while True:
        try:
            _DROP_IMPORT_QUEUE.get_nowait()
        except queue.Empty:
            break


def start_background_workers() -> None:
    start_derivative_worker(_process_upload_item_job, _process_derivative_job)
    start_drop_import_watcher()


def stop_background_workers() -> None:
    stop_drop_import_watcher()
    stop_derivative_worker()


UPLOAD_STAGING_DIR = ORIGINALS_DIR.parent / "upload_staging"


def _normalize_username(value: str | None) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip()
    return text.casefold()


def _find_user_by_username(conn: sqlite3.Connection, username: str | None, columns: str) -> sqlite3.Row | None:
    norm = _normalize_username(username)
    if not norm:
        return None
    return conn.execute(f"SELECT {columns} FROM users WHERE username_norm=?", (norm,)).fetchone()


def _ensure_upload_staging_dir() -> Path:
    UPLOAD_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    return UPLOAD_STAGING_DIR


def _job_staging_dir(job_id: int) -> Path:
    return _ensure_upload_staging_dir() / f"job_{int(job_id)}"


def _staged_direct_item_mtime_iso(path: Path) -> str:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()


def _upsert_direct_upload_item_row(
    conn: sqlite3.Connection,
    *,
    job_id: int,
    seq_i: int,
    filename: str,
    staged_path: str,
    mtime_iso: str,
) -> int:
    existing = conn.execute(
        "SELECT id, state FROM upload_zip_items WHERE job_id=? AND seq=? ORDER BY id DESC LIMIT 1",
        (int(job_id), int(seq_i)),
    ).fetchone()
    if existing:
        item_id = int(existing["id"])
        state_now = str(existing["state"] or "")
        next_state = state_now if state_now in {"処理中", "完了", "重複"} else "受信済み"
        conn.execute(
            "UPDATE upload_zip_items SET filename=?, staged_path=?, mtime_iso=?, state=?, message=CASE WHEN ? IN ('処理中','完了','重複') THEN message ELSE NULL END WHERE id=?",
            (str(filename), str(staged_path), str(mtime_iso), next_state, state_now, int(item_id)),
        )
        return item_id
    cur = conn.execute(
        "INSERT INTO upload_zip_items(job_id, seq, filename, state, image_id, message, staged_path, mtime_iso) VALUES (?,?,?,?,?,?,?,?)",
        (int(job_id), int(seq_i), str(filename), "受信済み", None, None, str(staged_path), str(mtime_iso)),
    )
    return int(cur.lastrowid)


def _register_direct_upload_item(job_id: int, seq_i: int, filename: str, staged_path: str, *, trace_id: str | None = None) -> None:
    item_id: int | None = None
    conn = get_conn()
    try:
        job = conn.execute(
            "SELECT status, source_kind FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not job:
            return
        if str(job["source_kind"] or "zip") != "direct":
            return
        if str(job["status"] or "") in {"cancelled", "error", "done"}:
            return
        item_id = _upsert_direct_upload_item_row(
            conn,
            job_id=int(job_id),
            seq_i=int(seq_i),
            filename=str(filename),
            staged_path=str(staged_path),
            mtime_iso=_staged_direct_item_mtime_iso(Path(staged_path)),
        )
        total_items = int(conn.execute("SELECT COUNT(*) AS n FROM upload_zip_items WHERE job_id=?", (int(job_id),)).fetchone()[0])
        conn.execute(
            "UPDATE upload_zip_jobs SET total=?, updated_at_utc=datetime('now') WHERE id=?",
            (int(total_items), int(job_id)),
        )
        _refresh_upload_job_progress(conn, int(job_id), seal=False)
        conn.commit()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        log_perf(
            "direct_upload_append_register_failed",
            job_id=int(job_id),
            seq=int(seq_i),
            filename=str(filename),
            trace_id=(trace_id or None),
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
        return
    finally:
        conn.close()

    if item_id is None:
        return
    try:
        _queue_upload_item_request(int(item_id), source="direct_append", trace_id=trace_id)
    except Exception as exc:
        log_perf(
            "direct_upload_append_enqueue_failed",
            job_id=int(job_id),
            item_id=int(item_id),
            seq=int(seq_i),
            filename=str(filename),
            trace_id=(trace_id or None),
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )


def _spawn_direct_upload_item_registration(job_id: int, seq_i: int, filename: str, staged_path: str, *, trace_id: str | None = None) -> None:
    thread = threading.Thread(
        target=_register_direct_upload_item,
        args=(int(job_id), int(seq_i), str(filename), str(staged_path)),
        kwargs={"trace_id": (trace_id or None)},
        name=f"nim-direct-append-{int(job_id)}-{int(seq_i)}",
        daemon=True,
    )
    thread.start()


def _load_direct_staging_rows(staging_dir: Path) -> list[tuple[int, str, str, str]]:
    rows: list[tuple[int, str, str, str]] = []
    if not staging_dir.exists():
        return rows
    for p in sorted(staging_dir.iterdir()):
        if not p.is_file():
            continue
        name = p.name
        if '_' not in name:
            continue
        seq_text, base = name.split('_', 1)
        try:
            seq_i = int(seq_text)
        except Exception:
            continue
        rows.append((seq_i, base, str(p), _staged_direct_item_mtime_iso(p)))
    rows.sort(key=lambda x: x[0])
    return rows


def _sync_direct_upload_items(conn: sqlite3.Connection, job_id: int, staging_dir: Path) -> tuple[int, list[int]]:
    rows = _load_direct_staging_rows(staging_dir)
    item_ids: list[int] = []
    for seq_i, base, path_text, mtime_iso in rows:
        item_id = _upsert_direct_upload_item_row(
            conn,
            job_id=int(job_id),
            seq_i=int(seq_i),
            filename=str(base),
            staged_path=str(path_text),
            mtime_iso=str(mtime_iso),
        )
        item_ids.append(int(item_id))
    conn.execute(
        "UPDATE upload_zip_jobs SET total=?, updated_at_utc=datetime('now') WHERE id=?",
        (int(len(rows)), int(job_id)),
    )
    return len(rows), item_ids

async def _read_json_body_loose(request: Request) -> dict:
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        try:
            text = raw.decode('utf-8', errors='ignore').strip()
        except Exception:
            text = ''
        if not text:
            return {}
        try:
            data = json.loads(text)
        except Exception:
            raise HTTPException(status_code=422, detail='invalid json body')
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except Exception:
            raise HTTPException(status_code=422, detail='invalid json body')
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail='json body must be an object')
    return data


def _validate_body_model(model_cls: type[BaseModel], data: dict):
    try:
        return model_cls.model_validate(data)
    except ValidationError as e:
        # Mirror FastAPI's validation shape closely enough for the UI.
        raise HTTPException(status_code=422, detail=e.errors())

def _thumb_rev_token(value: str | None) -> str:
    token = str(value or "0").strip()
    token = re.sub(r"[^0-9A-Za-z]+", "", token)
    return token or "0"


def _new_public_id() -> str:
    return secrets.token_hex(16)


def _public_thumb_rel_path(public_id: str, rev: str, fmt: str = "webp") -> str:
    pid = re.sub(r"[^0-9A-Za-z]+", "", str(public_id or "").strip().lower())
    token = _thumb_rev_token(rev)
    shard = pid[:2] or "xx"
    clean_fmt = "avif" if str(fmt).strip().lower() == "avif" else "webp"
    return f"grid/{clean_fmt}/{shard}/{pid}-r{token}.{clean_fmt}"


def _public_thumb_abs_path(public_id: str, rev: str, fmt: str = "webp") -> Path:
    return PUBLIC_THUMBS_DIR / _public_thumb_rel_path(public_id, rev, fmt=fmt)


def _public_thumb_url(public_id: str, rev: str, fmt: str = "webp") -> str:
    return f"/thumbs/{_public_thumb_rel_path(public_id, rev, fmt=fmt)}"


def _ensure_image_public_id(conn: sqlite3.Connection, image_id: int) -> str:
    row = conn.execute("SELECT public_id FROM images WHERE id=?", (int(image_id),)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="image not found")
    public_id = str(row["public_id"] or "").strip().lower()
    if public_id:
        return public_id
    while True:
        candidate = _new_public_id()
        exists = conn.execute("SELECT 1 FROM images WHERE public_id=? LIMIT 1", (candidate,)).fetchone()
        if exists:
            continue
        conn.execute("UPDATE images SET public_id=? WHERE id=?", (candidate, int(image_id)))
        return candidate


def _write_bytes_to_path(path: Path, raw: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, str(path))
    return str(path)


def _cleanup_public_thumb_versions(
    public_id: str,
    *,
    fmt: str | None = None,
    keep_paths: list[str] | tuple[str, ...] | set[str] | None = None,
) -> None:
    pid = re.sub(r"[^0-9A-Za-z]+", "", str(public_id or "").strip().lower())
    if not pid:
        return
    shard = pid[:2] or "xx"
    fmts = [str(fmt).strip().lower()] if fmt else ["webp", "avif"]
    keep_abs = {os.path.abspath(str(p)) for p in (keep_paths or []) if str(p).strip()}
    for one_fmt in fmts:
        if one_fmt not in {"webp", "avif"}:
            continue
        shard_dir = PUBLIC_THUMBS_DIR / "grid" / one_fmt / shard
        if not shard_dir.exists():
            continue
        for p in shard_dir.glob(f"{pid}-r*.{one_fmt}"):
            try:
                if os.path.abspath(str(p)) in keep_abs:
                    continue
                p.unlink(missing_ok=True)
            except Exception:
                pass


def _thumb_rev_map(conn: sqlite3.Connection, ids: list[int] | tuple[int, ...], *, kind: str = "grid") -> dict[int, str]:
    clean_ids: list[int] = []
    seen: set[int] = set()
    for raw_iid in ids:
        try:
            iid = int(raw_iid)
        except Exception:
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        clean_ids.append(iid)
    if not clean_ids:
        return {}

    placeholders = ",".join("?" for _ in clean_ids)
    rev_by_id: dict[int, str] = {}

    rows = conn.execute(
        f"SELECT id, uploaded_at_utc FROM images WHERE id IN ({placeholders})",
        clean_ids,
    ).fetchall()
    for row in rows:
        rev_by_id[int(row["id"])] = _thumb_rev_token(row["uploaded_at_utc"] if row["uploaded_at_utc"] is not None else None)

    drows = conn.execute(
        f"""
        SELECT image_id, MAX(created_at_utc) AS rev
        FROM image_derivatives
        WHERE kind=? AND image_id IN ({placeholders})
        GROUP BY image_id
        """,
        [kind] + clean_ids,
    ).fetchall()
    for row in drows:
        rev_by_id[int(row["image_id"])] = _thumb_rev_token(row["rev"] if row["rev"] is not None else None)

    return rev_by_id


def _public_thumb_url_map(conn: sqlite3.Connection, ids: list[int] | tuple[int, ...]) -> dict[int, str]:
    clean_ids: list[int] = []
    seen: set[int] = set()
    for raw_iid in ids:
        try:
            iid = int(raw_iid)
        except Exception:
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        clean_ids.append(iid)
    if not clean_ids:
        return {}

    placeholders = ",".join("?" for _ in clean_ids)
    rows = conn.execute(
        f"SELECT id, public_id FROM images WHERE id IN ({placeholders})",
        clean_ids,
    ).fetchall()
    rev_by_id = _thumb_rev_map(conn, clean_ids, kind="grid")
    out: dict[int, str] = {}
    for row in rows:
        iid = int(row["id"])
        public_id = str(row["public_id"] or "").strip().lower()
        if not public_id:
            public_id = _ensure_image_public_id(conn, iid)
        out[iid] = _public_thumb_url(public_id, rev_by_id.get(iid, "0"))
    return out


def _thumb_url(conn: sqlite3.Connection, image_id: int, *, kind: str = "grid", rev: str | None = None) -> str:
    token = rev or _thumb_rev_map(conn, [int(image_id)], kind=kind).get(int(image_id), "0")
    if str(kind) == "grid":
        return _public_thumb_url(_ensure_image_public_id(conn, int(image_id)), token)
    return f"/api/images/{int(image_id)}/thumb?kind={kind}&v={DERIV_VERSION}&rev={token}"


def _thumb_url_map(conn: sqlite3.Connection, ids: list[int] | tuple[int, ...], *, kind: str = "grid") -> dict[int, str]:
    if str(kind) == "grid":
        return _public_thumb_url_map(conn, ids)
    rev_by_id = _thumb_rev_map(conn, ids, kind=kind)
    return {int(iid): _thumb_url(conn, int(iid), kind=kind, rev=rev_by_id.get(int(iid), "0")) for iid in rev_by_id.keys()}


def _append_bm_list_id_to_url(url: str, bm_list_id: int | None) -> str:
    if not url or bm_list_id is None:
        return url
    sep = '&' if '?' in str(url) else '?'
    return f"{url}{sep}bm_list_id={int(bm_list_id)}"


def publish_existing_public_thumbs() -> None:
    conn = get_conn()
    try:
        rows = conn.execute(
            """
            SELECT d.image_id, d.disk_path, d.created_at_utc, d.format, i.public_id
            FROM image_derivatives d
            JOIN images i ON i.id = d.image_id
            WHERE d.kind='grid' AND d.format IN ('webp','avif')
            ORDER BY d.image_id ASC, CASE WHEN d.format='webp' THEN 0 ELSE 1 END ASC
            """
        ).fetchall()
        if not rows:
            return
        image_ids = [int(row["image_id"]) for row in rows]
        shared_rev_by_id = _thumb_rev_map(conn, image_ids, kind="grid")
        dirty = False
        keep_by_public_id: dict[str, list[str]] = {}
        for row in rows:
            image_id = int(row["image_id"])
            fmt = str(row["format"] or "").strip().lower()
            if fmt not in {"webp", "avif"}:
                continue
            src_path = str(row["disk_path"] or "").strip()
            public_id = str(row["public_id"] or "").strip().lower()
            if not public_id:
                public_id = _ensure_image_public_id(conn, image_id)
                dirty = True
            rev = shared_rev_by_id.get(image_id, _thumb_rev_token(row["created_at_utc"] if row["created_at_utc"] is not None else None))
            dst = _public_thumb_abs_path(public_id, rev, fmt=fmt)
            dst_str = str(dst)
            if src_path and os.path.abspath(src_path) != os.path.abspath(dst_str) and os.path.exists(src_path):
                _write_bytes_to_path(dst, Path(src_path).read_bytes())
                dirty = True
            elif src_path and os.path.abspath(src_path) == os.path.abspath(dst_str) and os.path.exists(src_path):
                pass
            elif dst.exists():
                pass
            else:
                continue
            if str(row["disk_path"] or "") != dst_str:
                size = int(dst.stat().st_size) if dst.exists() else None
                conn.execute(
                    "UPDATE image_derivatives SET disk_path=?, size=?, bytes=NULL WHERE image_id=? AND kind='grid' AND format=?",
                    (dst_str, size, image_id, fmt),
                )
                dirty = True
            keep_by_public_id.setdefault(public_id, []).append(dst_str)
            if src_path and os.path.abspath(src_path) != os.path.abspath(dst_str):
                try:
                    if os.path.exists(src_path):
                        os.remove(src_path)
                except Exception:
                    pass
        for public_id, keep_paths in keep_by_public_id.items():
            _cleanup_public_thumb_versions(public_id, keep_paths=keep_paths)
        if dirty:
            conn.commit()
    finally:
        conn.close()



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


# ---- tag lookup caches (alias/category/group) ----
#
# Zip imports and tag-heavy prompts can cause a large number of repetitive SQL lookups.
# These tables are effectively read-only at runtime, so we can memoize them safely.
#
_TAG_ALIAS_CACHE: dict[str, str] = {}
_TAG_ALIAS_LOCK = threading.Lock()
_TAG_ALIAS_MAX = 200_000

_TAG_CAT_CACHE: dict[str, int] = {}  # -1 means NULL/unknown
_TAG_CAT_LOCK = threading.Lock()
_TAG_CAT_MAX = 200_000

_TAG_EFFECTIVE_CAT_CACHE: dict[str, int] = {}  # -1 means NULL/unknown (after quality upgrade)
_TAG_EFFECTIVE_CAT_LOCK = threading.Lock()
_TAG_EFFECTIVE_CAT_MAX = 200_000

_TAG_GROUP_CACHE: dict[tuple[str, int | None], str] = {}
_TAG_GROUP_LOCK = threading.Lock()
_TAG_GROUP_MAX = 200_000

def _cache_put(d: dict, lock: threading.Lock, k, v, max_size: int) -> None:
    try:
        with lock:
            if len(d) >= int(max_size):
                d.clear()
            d[k] = v
    except Exception:
        # best-effort only
        pass

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


def _ext_auth_failed(request: Request | None = None) -> JSONResponse:
    payload = {
        "ok": False,
        "code": "AUTH_REQUIRED",
        "message": "再ログインが必要です",
    }
    if request is not None:
        payload["login_url"] = _abs_url("/login.html", request)
    return JSONResponse(status_code=401, content=payload)


def _login_user_row(username: str, password: str) -> sqlite3.Row | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, username, password_hash, role, disabled FROM users WHERE username_norm=?",
            (_normalize_username(username),),
        ).fetchone()
        if not row or int(row["disabled"] or 0) == 1:
            return None
        if not verify_password(password, row["password_hash"]):
            return None
        return row
    finally:
        conn.close()


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
def setup_master(request: Request, req: SetupMasterReq, response: Response):
    """Create the first (master) admin user. Only allowed when no users exist."""
    username = (req.username or "").strip()
    username_norm = _normalize_username(username)
    if not username_norm:
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
                "INSERT INTO users(username, username_norm, password_hash, role, must_set_password, pw_set_at) VALUES (?,?,?,?,?,datetime('now'))",
                (username, username_norm, hash_password(req.password), "master", 0),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="username already exists")

        row = conn.execute(
            "SELECT id, username, role FROM users WHERE username_norm=?",
            (username_norm,),
        ).fetchone()
        token = create_token(user_id=int(row["id"]), username=row["username"], role=row["role"])
        response.set_cookie(
            key="nai_token",
            value=token,
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 30,
            path="/",
            secure=_cookie_secure_flag(request),
        )
        return {"ok": True, "token": token, "user": {"id": row["id"], "username": row["username"], "role": row["role"]}}
    finally:
        conn.close()

@api_router.post("/auth/login")
def login(request: Request, req: LoginReq, response: Response):
    row = _login_user_row(req.username, req.password)
    if not row:
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
        secure=_cookie_secure_flag(request),
    )
    return {"token": token, "user": {"id": row["id"], "username": row["username"], "role": row["role"]}}


@api_router.post("/ext/login")
def ext_login(request: Request, req: LoginReq):
    row = _login_user_row(req.username, req.password)
    if not row:
        return JSONResponse(
            status_code=401,
            content={
                "ok": False,
                "code": "INVALID_CREDENTIALS",
                "message": "ログインに失敗しました",
            },
        )
    token = create_token(user_id=int(row["id"]), username=row["username"], role=row["role"])
    return {
        "ok": True,
        "token": token,
        "user": {"id": row["id"], "username": row["username"], "role": row["role"]},
        "message": "ログインしました",
        "login_url": _abs_url("/login.html", request),
    }


@api_router.get("/ext/session")
def ext_session(request: Request, user: dict | None = Depends(get_user_optional)):
    if not user:
        return _ext_auth_failed(request)
    return {
        "ok": True,
        "user": {
            "id": int(user["id"]),
            "username": str(user["username"]),
            "role": str(user["role"]),
            "share_works": int(user.get("share_works") or 0),
            "share_bookmarks": int(user.get("share_bookmarks") or 0),
        },
        "login_url": _abs_url("/login.html", request),
    }

@api_router.get("/me")
def me(user: dict = Depends(get_user)):
    data = dict(user or {})
    data["perf_enabled"] = bool(perf_logging_enabled())
    return data


@api_router.get("/app/update_status")
def app_update_status(user: dict = Depends(get_user)):
    state = get_update_status()
    is_master = str(user.get("role") or "") == "master"
    data = dict(state or {})
    data["is_master"] = is_master
    data["visible"] = bool(is_master and data.get("update_available"))
    if not is_master:
        data["update_available"] = False
    return data


class UpdateMeSettingsReq(BaseModel):
    share_works: int | None = None
    share_bookmarks: int | None = None


@api_router.patch("/me/settings")
@api_router.post("/me/settings")
def update_me_settings(req: UpdateMeSettingsReq, user: dict = Depends(get_user)):
    """Update current user's sharing settings."""
    uid = int(user.get("id") or 0)
    if uid <= 0:
        raise HTTPException(status_code=401, detail="Not authenticated")

    sw = None if req.share_works is None else (1 if int(req.share_works or 0) else 0)
    sb = None if req.share_bookmarks is None else (1 if int(req.share_bookmarks or 0) else 0)
    if sw is None and sb is None:
        return {"ok": True, **user}

    conn = get_conn()
    try:
        # Upsert (SQLite).
        # Keep updated_at fresh for debugging.
        cur = conn.execute("SELECT share_works, share_bookmarks FROM user_settings WHERE user_id=?", (uid,)).fetchone()
        if cur:
            nsw = int(cur[0] or 0) if sw is None else sw
            nsb = int(cur[1] or 0) if sb is None else sb
            conn.execute(
                "UPDATE user_settings SET share_works=?, share_bookmarks=?, updated_at=datetime('now') WHERE user_id=?",
                (nsw, nsb, uid),
            )
        else:
            conn.execute(
                "INSERT INTO user_settings(user_id, share_works, share_bookmarks) VALUES (?,?,?)",
                (uid, (sw or 0), (sb or 0)),
            )
        conn.commit()
        # reflect back
        row = conn.execute(
            "SELECT COALESCE(share_works,0) AS share_works, COALESCE(share_bookmarks,0) AS share_bookmarks FROM user_settings WHERE user_id=?",
            (uid,),
        ).fetchone()
        user2 = dict(user)
        if row:
            user2["share_works"] = int(row["share_works"] or 0)
            user2["share_bookmarks"] = int(row["share_bookmarks"] or 0)
        return {"ok": True, **user2}
    finally:
        conn.close()


@api_router.delete("/me")
def delete_me(user: dict = Depends(get_user)):
    uid = int(user.get("id") or 0)
    if uid <= 0:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if str(user.get("role") or "") == "master":
        raise HTTPException(status_code=400, detail="cannot delete master")

    conn = get_conn()
    try:
        result = _delete_user_account(conn, user_id=uid)
        conn.commit()

        for p in result.pop("disk_paths", []):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        for public_id in result.pop("public_ids", []):
            try:
                _cleanup_public_thumb_versions(str(public_id))
            except Exception:
                pass
        for p in result.pop("staging_dirs", []):
            try:
                if p and os.path.exists(p):
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass
        return result
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except sqlite3.IntegrityError as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=409, detail=f"delete failed: {e}")
    finally:
        conn.close()


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
async def admin_create_user(request: Request, admin: dict = Depends(require_admin)):
    req = _validate_body_model(CreateUserReq, await _read_json_body_loose(request))
    """Create a user and return a password-setup URL.

    - Admins can create/delete normal users.
    - Only master can create/delete admin users.
    """
    if req.role not in {"admin", "user"}:
        raise HTTPException(status_code=400, detail="role must be admin/user")
    if req.role == "admin" and admin.get("role") != "master":
        raise HTTPException(status_code=403, detail="master required to create admin")
    username = (req.username or "").strip()
    username_norm = _normalize_username(username)
    if not username_norm:
        raise HTTPException(status_code=400, detail="username required")
    conn = get_conn()
    try:
        import secrets
        from datetime import datetime, timedelta, timezone

        try:
            # Create with a random placeholder hash; user sets their own password via URL.
            placeholder = secrets.token_urlsafe(32)
            conn.execute(
                "INSERT INTO users(username, username_norm, password_hash, role, must_set_password) VALUES (?,?,?,?,1)",
                (username, username_norm, hash_password(placeholder), req.role),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            raise HTTPException(status_code=409, detail="username already exists")

        urow = conn.execute("SELECT id FROM users WHERE username_norm=?", (username_norm,)).fetchone()
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


def _delete_user_owned_images(conn: sqlite3.Connection, *, user_id: int, username: str) -> dict:
    deleted_images = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM images WHERE uploader_user_id=?",
            (int(user_id),),
        ).fetchone()[0]
        or 0
    )
    if deleted_images <= 0:
        return {
            "deleted_images": 0,
            "disk_paths": [],
            "public_ids": [],
        }

    disk_paths: list[str] = []
    public_ids: list[str] = []

    for r in conn.execute(
        "SELECT disk_path FROM image_files WHERE image_id IN (SELECT id FROM images WHERE uploader_user_id=?)",
        (int(user_id),),
    ).fetchall():
        p = r["disk_path"] if not isinstance(r, tuple) else r[0]
        if p and str(p).strip():
            disk_paths.append(str(p))

    for r in conn.execute(
        "SELECT disk_path FROM image_derivatives WHERE image_id IN (SELECT id FROM images WHERE uploader_user_id=?)",
        (int(user_id),),
    ).fetchall():
        p = r["disk_path"] if not isinstance(r, tuple) else r[0]
        if p and str(p).strip():
            disk_paths.append(str(p))

    for r in conn.execute(
        "SELECT public_id FROM images WHERE uploader_user_id=? AND public_id IS NOT NULL AND TRIM(public_id) <> ''",
        (int(user_id),),
    ).fetchall():
        public_id = r["public_id"] if not isinstance(r, tuple) else r[0]
        if public_id and str(public_id).strip():
            public_ids.append(str(public_id))

    hashes = [
        str((r["h"] if not isinstance(r, tuple) else r[0]) or "")
        for r in conn.execute(
            "SELECT DISTINCT main_sig_hash AS h FROM images WHERE uploader_user_id=? AND main_sig_hash IS NOT NULL AND TRIM(main_sig_hash) <> ''",
            (int(user_id),),
        ).fetchall()
    ]
    hashes = [h for h in hashes if h.strip()]

    softwares = conn.execute(
        "SELECT software AS k, COUNT(*) AS c FROM images WHERE uploader_user_id=? AND software IS NOT NULL AND TRIM(software) <> '' GROUP BY software",
        (int(user_id),),
    ).fetchall()
    days = conn.execute(
        "SELECT SUBSTR(file_mtime_utc,1,10) AS k, COUNT(*) AS c FROM images WHERE uploader_user_id=? AND file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 10 GROUP BY SUBSTR(file_mtime_utc,1,10)",
        (int(user_id),),
    ).fetchall()
    months = conn.execute(
        "SELECT SUBSTR(file_mtime_utc,1,7) AS k, COUNT(*) AS c FROM images WHERE uploader_user_id=? AND file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 7 GROUP BY SUBSTR(file_mtime_utc,1,7)",
        (int(user_id),),
    ).fetchall()
    years = conn.execute(
        "SELECT SUBSTR(file_mtime_utc,1,4) AS k, COUNT(*) AS c FROM images WHERE uploader_user_id=? AND file_mtime_utc IS NOT NULL AND LENGTH(file_mtime_utc) >= 4 GROUP BY SUBSTR(file_mtime_utc,1,4)",
        (int(user_id),),
    ).fetchall()
    tags = conn.execute(
        "SELECT it.tag_canonical AS k, COUNT(DISTINCT it.image_id) AS c FROM image_tags it JOIN images i ON i.id=it.image_id WHERE i.uploader_user_id=? GROUP BY it.tag_canonical",
        (int(user_id),),
    ).fetchall()

    conn.execute("DELETE FROM images WHERE uploader_user_id=?", (int(user_id),))

    if username:
        _apply_bulk_dec(conn, "stat_creators", "creator", str(username), int(deleted_images))

    for rows, table, key_col in (
        (softwares, "stat_software", "software"),
        (days, "stat_day_counts", "ymd"),
        (months, "stat_month_counts", "ym"),
        (years, "stat_year_counts", "year"),
        (tags, "stat_tag_counts", "tag_canonical"),
    ):
        for r in rows:
            key = str(r["k"] if not isinstance(r, tuple) else r[0])
            count = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, table, key_col, key, count)

    if hashes:
        try:
            stats_service.recompute_dedup_flags_for_hashes(conn, hashes)
        except Exception:
            pass

    return {
        "deleted_images": int(deleted_images),
        "disk_paths": disk_paths,
        "public_ids": list(dict.fromkeys(public_ids)),
    }



def _delete_user_account(conn: sqlite3.Connection, *, user_id: int) -> dict:
    row = conn.execute(
        "SELECT id, username, role FROM users WHERE id=?",
        (int(user_id),),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not found")

    username = str(row["username"] or "")
    cleanup = _delete_user_owned_images(conn, user_id=int(user_id), username=username)

    staging_dirs: list[str] = []
    for r in conn.execute(
        "SELECT staging_dir FROM upload_zip_jobs WHERE user_id=? AND staging_dir IS NOT NULL AND TRIM(staging_dir) <> ''",
        (int(user_id),),
    ).fetchall():
        p = r["staging_dir"] if not isinstance(r, tuple) else r[0]
        if p and str(p).strip():
            staging_dirs.append(str(p))

    deleted_upload_jobs = int(
        conn.execute(
            "SELECT COUNT(*) AS n FROM upload_zip_jobs WHERE user_id=?",
            (int(user_id),),
        ).fetchone()[0]
        or 0
    )

    conn.execute("UPDATE password_tokens SET created_by=NULL WHERE created_by=?", (int(user_id),))
    conn.execute("DELETE FROM upload_zip_jobs WHERE user_id=?", (int(user_id),))
    conn.execute("DELETE FROM users WHERE id=?", (int(user_id),))
    if username:
        conn.execute("DELETE FROM stat_creators WHERE creator=?", (username,))

    return {
        "ok": True,
        "deleted_images": int(cleanup["deleted_images"]),
        "deleted_upload_jobs": int(deleted_upload_jobs),
        "disk_paths": cleanup["disk_paths"],
        "public_ids": cleanup["public_ids"],
        "staging_dirs": list(dict.fromkeys(staging_dirs)),
    }


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

        result = _delete_user_account(conn, user_id=int(user_id))
        conn.commit()

        for p in result.pop("disk_paths", []):
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        for public_id in result.pop("public_ids", []):
            try:
                _cleanup_public_thumb_versions(str(public_id))
            except Exception:
                pass
        for p in result.pop("staging_dirs", []):
            try:
                if p and os.path.exists(p):
                    shutil.rmtree(p, ignore_errors=True)
            except Exception:
                pass
        return result
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except sqlite3.IntegrityError as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=409, detail=f"delete failed: {e}")
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


def _derivative_file_path(image_id: int, kind: str, fmt: str) -> Path:
    image_dir = DERIVATIVES_DIR / f"{int(image_id) // 1000:06d}" / str(int(image_id))
    return image_dir / f"{str(kind)}.{str(fmt)}"


def _write_derivative_to_disk(*, image_id: int, kind: str, fmt: str, raw: bytes) -> str:
    path = _derivative_file_path(int(image_id), str(kind), str(fmt))
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "wb") as f:
        f.write(raw)
    os.replace(tmp, str(path))
    return str(path)


def _ensure_derivative_on_disk(conn: sqlite3.Connection, image_id: int, kind: str, fmt: str) -> str | None:
    row = conn.execute(
        "SELECT id, disk_path, size, bytes, created_at_utc FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
        (int(image_id), str(kind), str(fmt)),
    ).fetchone()
    if not row:
        return None
    p = row["disk_path"]
    if p and os.path.exists(p):
        return str(p)

    b = row["bytes"]
    if b is None:
        return None
    if isinstance(b, memoryview):
        b = b.tobytes()
    if not b:
        return None

    try:
        if str(kind) == "grid" and str(fmt) in {"webp", "avif"}:
            public_id = _ensure_image_public_id(conn, int(image_id))
            rev_token = _thumb_rev_token(row["created_at_utc"] if row["created_at_utc"] is not None else None)
            new_p = _write_bytes_to_path(_public_thumb_abs_path(public_id, rev_token, fmt=str(fmt)), b)
            _cleanup_public_thumb_versions(public_id, fmt=str(fmt), keep_paths=[new_p])
        else:
            new_p = _write_derivative_to_disk(image_id=int(image_id), kind=str(kind), fmt=str(fmt), raw=b)
        conn.execute(
            "UPDATE image_derivatives SET disk_path=?, size=?, bytes=NULL WHERE id=?",
            (new_p, int(len(b)), int(row["id"])),
        )
        return new_p
    except Exception:
        return None


def _derivative_quality_for(kind: str, fmt: str) -> int:
    target = DERIVATIVE_TARGETS[str(kind)]
    if str(fmt) == "avif":
        return int(target.avif.quality)
    return int(target.webp.quality)


def _derivative_format_enabled(kind: str, fmt: str) -> bool:
    target = DERIVATIVE_TARGETS[str(kind)]
    if str(fmt) == "avif":
        return bool(target.avif.enabled and avif_available())
    return str(fmt) == "webp"


def _derivative_row_is_ready(conn: sqlite3.Connection, row, *, kind: str, fmt: str) -> bool:
    if not row:
        return False
    target = DERIVATIVE_TARGETS[str(kind)]
    try:
        q = int(row["quality"] or 0)
        w = int(row["width"] or 0)
        h = int(row["height"] or 0)
    except Exception:
        return False
    if q != _derivative_quality_for(kind, fmt):
        return False
    if max(w, h) > int(target.max_side) + 2:
        return False
    p = row["disk_path"] if "disk_path" in row.keys() else None
    if p and os.path.exists(p):
        try:
            return os.path.getsize(p) > 0
        except Exception:
            return False
    p2 = _ensure_derivative_on_disk(conn, int(row["image_id"]), kind, fmt) if "image_id" in row.keys() else None
    return bool(p2 and os.path.exists(p2))


def _upsert_derivative_file(
    conn: sqlite3.Connection,
    *,
    image_id: int,
    kind: str,
    fmt: str,
    width: int,
    height: int,
    quality: int,
    raw: bytes,
    created_at_utc: str | None = None,
) -> None:
    prev = conn.execute(
        "SELECT disk_path FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
        (int(image_id), str(kind), str(fmt)),
    ).fetchone()
    prev_disk_path = str(prev["disk_path"] or "").strip() if prev else ""
    created_at_utc = str(created_at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    if str(kind) == "grid" and str(fmt) in {"webp", "avif"}:
        public_id = _ensure_image_public_id(conn, int(image_id))
        rev_token = _thumb_rev_token(created_at_utc)
        disk_path = _write_bytes_to_path(_public_thumb_abs_path(public_id, rev_token, fmt=str(fmt)), raw)
        _cleanup_public_thumb_versions(public_id, fmt=str(fmt), keep_paths=[disk_path])
    else:
        disk_path = _write_derivative_to_disk(image_id=int(image_id), kind=str(kind), fmt=str(fmt), raw=raw)
    conn.execute(
        """
        INSERT OR REPLACE INTO image_derivatives(
          image_id, kind, format, width, height, quality, disk_path, size, bytes, created_at_utc
        ) VALUES (?,?,?,?,?,?,?,?,NULL,?)
        """,
        (int(image_id), str(kind), str(fmt), int(width), int(height), int(quality), disk_path, int(len(raw)), created_at_utc),
    )
    if prev_disk_path and os.path.abspath(prev_disk_path) != os.path.abspath(str(disk_path)):
        try:
            if os.path.exists(prev_disk_path):
                os.remove(prev_disk_path)
        except Exception:
            pass


def _collect_derivative_disk_paths(conn: sqlite3.Connection, where_sql: str, params: tuple | list = ()) -> list[str]:
    rows = conn.execute(
        f"SELECT disk_path FROM image_derivatives {where_sql}",
        params,
    ).fetchall()
    out: list[str] = []
    for row in rows:
        p = row["disk_path"] if not isinstance(row, tuple) else row[0]
        if p and str(p).strip():
            out.append(str(p))
    return out


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
        character_entries, main_negative_combined_raw = build_prompt_view_payload(conn, prompt_neg, prompt_char, meta.params if meta else None)
        character_entries_json = json.dumps(character_entries, ensure_ascii=False)
        potion_raw = json.dumps(meta.potion, ensure_ascii=False).encode("utf-8") if meta.potion else None
        has_potion = 1 if (meta and meta.uses_potion) else 0
        uses_potion = 1 if (meta and meta.uses_potion) else 0
        uses_precise_reference = 1 if (meta and meta.uses_precise_reference) else 0
        sampler = meta.sampler if meta else None
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

        # NSFW: also treat the main prompt itself containing 'nsfw' as NSFW.
        if not is_nsfw:
            ptxt = (prompt_pos or "")
            if ptxt and ("nsfw" in ptxt.lower()):
                is_nsfw = 1

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
                params_json=?, character_entries_json=?, main_negative_combined_raw=?, potion_raw=?, has_potion=?, uses_potion=?, uses_precise_reference=?, sampler=?, metadata_raw=?,
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
                character_entries_json,
                main_negative_combined_raw,
                potion_raw,
                has_potion,
                uses_potion,
                uses_precise_reference,
                sampler,
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
                    stats_service.dec_creator_software(conn, int(img["uploader_user_id"] or 0), str(old_software))
                if software:
                    stats_service.bump_software(conn, str(software))
                    stats_service.bump_creator_software(conn, int(img["uploader_user_id"] or 0), str(software))

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
        if _derivative_fill_active(conn):
            raise HTTPException(status_code=409, detail="派生画像補完が実行中です")
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


def _derivative_fill_active(conn: sqlite3.Connection) -> bool:
    try:
        running_flag = int(_kv_get(conn, "derivative_fill_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "derivative_fill_bg_heartbeat") or "0")
        return _hb_active(running_flag, hb_ts)
    except Exception:
        return False


def _derivative_kind_ready(conn: sqlite3.Connection, image_id: int, kind: str) -> bool:
    for fmt in ("avif", "webp"):
        if not _derivative_format_enabled(kind, fmt):
            continue
        row = conn.execute(
            "SELECT image_id, width, height, quality, disk_path, size, bytes FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
            (int(image_id), str(kind), str(fmt)),
        ).fetchone()
        if _derivative_row_is_ready(conn, row, kind=str(kind), fmt=str(fmt)):
            return True
    return False


def _missing_derivative_kinds(conn: sqlite3.Connection, image_id: int) -> tuple[str, ...]:
    missing: list[str] = []
    for kind in ("grid", "overlay"):
        if not _derivative_kind_ready(conn, int(image_id), kind):
            missing.append(kind)
    return tuple(missing)


def _estimate_missing_derivative_count(conn: sqlite3.Connection, kind: str) -> int:
    try:
        row = conn.execute(
            """
            SELECT COUNT(*)
            FROM images i
            WHERE NOT EXISTS (
              SELECT 1
              FROM image_derivatives d
              WHERE d.image_id = i.id
                AND d.kind = ?
                AND d.format = 'webp'
            )
            """,
            (str(kind),),
        ).fetchone()
        return int(row[0] if isinstance(row, tuple) else row[0] or 0)
    except Exception:
        return 0


_SOURCE_DECODE_DELETE_MARKERS = (
    "cannot identify image file",
    "image file is truncated",
)


def _is_unrecoverable_source_decode_error(exc: Exception) -> bool:
    msg = str(exc or "").strip().lower()
    if not msg:
        return False
    return any(marker in msg for marker in _SOURCE_DECODE_DELETE_MARKERS)


def _delete_invalid_image_record(conn: sqlite3.Connection, image_id: int) -> bool:
    iid = int(image_id)
    row = conn.execute(
        """
        SELECT images.id, images.uploader_user_id, images.software, images.file_mtime_utc,
               images.main_sig_hash, users.username AS creator
        FROM images
        JOIN users ON users.id = images.uploader_user_id
        WHERE images.id=?
        """,
        (iid,),
    ).fetchone()
    if not row:
        return False

    disk_paths: list[str] = []

    drows = conn.execute("SELECT disk_path FROM image_files WHERE image_id=?", (iid,)).fetchall()
    for r in drows:
        p = r["disk_path"] if not isinstance(r, tuple) else r[0]
        if p and str(p).strip():
            disk_paths.append(str(p))

    ddrows = conn.execute("SELECT disk_path FROM image_derivatives WHERE image_id=?", (iid,)).fetchall()
    for r in ddrows:
        p = r["disk_path"] if not isinstance(r, tuple) else r[0]
        if p and str(p).strip():
            disk_paths.append(str(p))

    creator = str(row["creator"] if not isinstance(row, tuple) else row[5])
    creator_id = int(row["uploader_user_id"] if not isinstance(row, tuple) else row[1])
    software = row["software"] if not isinstance(row, tuple) else row[2]
    file_mtime_utc = str((row["file_mtime_utc"] if not isinstance(row, tuple) else row[3]) or "")
    sig_hash = str((row["main_sig_hash"] if not isinstance(row, tuple) else row[4]) or "")

    day_key = file_mtime_utc[:10] if len(file_mtime_utc) >= 10 else ""
    month_key = file_mtime_utc[:7] if len(file_mtime_utc) >= 7 else ""
    year_key = file_mtime_utc[:4] if len(file_mtime_utc) >= 4 else ""

    tags = conn.execute(
        "SELECT tag_canonical FROM image_tags WHERE image_id=? GROUP BY tag_canonical",
        (iid,),
    ).fetchall()

    conn.execute("DELETE FROM images WHERE id=?", (iid,))

    if creator:
        _apply_bulk_dec(conn, "stat_creators", "creator", creator, 1)

    if software is not None and str(software).strip():
        software_key = str(software)
        _apply_bulk_dec(conn, "stat_software", "software", software_key, 1)
        conn.execute(
            "UPDATE stat_creator_software SET image_count = image_count - 1 WHERE creator_id=? AND software=?",
            (creator_id, software_key),
        )
        conn.execute(
            "DELETE FROM stat_creator_software WHERE creator_id=? AND software=? AND image_count <= 0",
            (creator_id, software_key),
        )

    if day_key:
        _apply_bulk_dec(conn, "stat_day_counts", "ymd", day_key, 1)
        conn.execute(
            "UPDATE stat_creator_day_counts SET image_count = image_count - 1 WHERE creator_id=? AND ymd=?",
            (creator_id, day_key),
        )
        conn.execute(
            "DELETE FROM stat_creator_day_counts WHERE creator_id=? AND ymd=? AND image_count <= 0",
            (creator_id, day_key),
        )

    if month_key:
        _apply_bulk_dec(conn, "stat_month_counts", "ym", month_key, 1)
        conn.execute(
            "UPDATE stat_creator_month_counts SET image_count = image_count - 1 WHERE creator_id=? AND ym=?",
            (creator_id, month_key),
        )
        conn.execute(
            "DELETE FROM stat_creator_month_counts WHERE creator_id=? AND ym=? AND image_count <= 0",
            (creator_id, month_key),
        )

    if year_key:
        _apply_bulk_dec(conn, "stat_year_counts", "year", year_key, 1)
        conn.execute(
            "UPDATE stat_creator_year_counts SET image_count = image_count - 1 WHERE creator_id=? AND year=?",
            (creator_id, year_key),
        )
        conn.execute(
            "DELETE FROM stat_creator_year_counts WHERE creator_id=? AND year=? AND image_count <= 0",
            (creator_id, year_key),
        )

    for r in tags:
        tag_key = str(r["tag_canonical"] if not isinstance(r, tuple) else r[0])
        if tag_key:
            _apply_bulk_dec(conn, "stat_tag_counts", "tag_canonical", tag_key, 1)

    if sig_hash:
        try:
            stats_service.recompute_dedup_flags_for_hashes(conn, [sig_hash])
        except Exception:
            pass

    conn.commit()

    for p in disk_paths:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    return True


def _fill_missing_derivatives_worker(run_id: int, batch_size: int = 100, interval_sec: float = 0.1) -> None:
    try:
        while True:
            conn = get_conn()
            try:
                _kv_set(conn, "derivative_fill_bg_running", "1")
                _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))

                after_id = int(_kv_get(conn, "derivative_fill_after_id") or "0")
                rows = conn.execute(
                    "SELECT id FROM images WHERE id > ? ORDER BY id ASC LIMIT ?",
                    (after_id, int(batch_size)),
                ).fetchall()
                ids = [int(r[0] if isinstance(r, tuple) else r["id"]) for r in rows]

                if not ids:
                    _run_add_counts(conn, run_id, last_image_id=after_id, processed=0, updated=0, error_count=0, done=True)
                    _set_run_status(conn, run_id, "done")
                    _kv_set(conn, "derivative_fill_bg_running", "0")
                    _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    break

                updated_n = 0
                error_n = 0
                last_id = after_id
                for iid in ids:
                    last_id = int(iid)
                    try:
                        missing_kinds = _missing_derivative_kinds(conn, int(iid))
                        if missing_kinds:
                            _ensure_derivatives(int(iid), missing_kinds, trigger="maintenance_fill")
                            updated_n += 1
                    except Exception as exc:
                        if _is_unrecoverable_source_decode_error(exc):
                            deleted = False
                            try:
                                deleted = _delete_invalid_image_record(conn, int(iid))
                            except Exception as delete_exc:
                                error_n += 1
                                _run_log_error(conn, run_id, int(iid), "derivative_fill", f"{str(exc) or 'error'} / delete_failed: {type(delete_exc).__name__}: {delete_exc}")
                            else:
                                if deleted:
                                    updated_n += 1
                                    _run_log_error(conn, run_id, int(iid), "derivative_fill_delete", f"source image invalid; deleted from DB: {str(exc) or 'error'}")
                                else:
                                    error_n += 1
                                    _run_log_error(conn, run_id, int(iid), "derivative_fill", str(exc) or "error")
                        else:
                            error_n += 1
                            _run_log_error(conn, run_id, int(iid), "derivative_fill", str(exc) or "error")
                    if (last_id % 20) == 0:
                        _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))

                _kv_set(conn, "derivative_fill_after_id", str(int(last_id)))
                _run_add_counts(conn, run_id, last_image_id=int(last_id), processed=len(ids), updated=updated_n, error_count=error_n, done=False)
                _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))
                conn.commit()
            finally:
                conn.close()

            time.sleep(max(0.05, float(interval_sec)))
    except Exception:
        try:
            conn = get_conn()
            try:
                _kv_set(conn, "derivative_fill_bg_running", "0")
                _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))
                _set_run_status(conn, run_id, "stopped")
                conn.commit()
            finally:
                conn.close()
        except Exception:
            pass


def _reparse_all_worker(run_id: int, batch_size: int = 100, interval_sec: float = 0.2) -> None:
    """Background worker: reparse all images in batches."""
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

@api_router.post("/admin/reparse_all_start")
def admin_reparse_all_start(_admin: dict = Depends(require_admin)):
    """Start or resume background full-library reparse."""

    conn = get_conn()
    try:
        if _rebuild_active(conn):
            raise HTTPException(status_code=409, detail="統計再集計が実行中です")
        if _derivative_fill_active(conn):
            raise HTTPException(status_code=409, detail="派生画像補完が実行中です")

        run_id = int(_kv_get(conn, "reparse_run_id") or "0")
        run = _fetch_run(conn, run_id)

        if not run or run.get("status") != "running":
            # new run (reset cursor)
            run_id = _create_run(conn, "reparse_all", {"batch": 100, "interval_sec": 0.2})
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
                if not _reparse_active(conn):
                    _kv_set(conn, "reparse_bg_running", "1")
                    _kv_set(conn, "reparse_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    threading.Thread(target=_reparse_all_worker, args=(int(run_id),), daemon=True).start()
                    started = True
        return {"ok": True, "run_id": int(run_id), "started": bool(started)}
    finally:
        conn.close()


def _rebuild_stats_worker(run_id: int) -> None:
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

@api_router.post("/admin/rebuild_stats_start")
def admin_rebuild_stats_start(_admin: dict = Depends(require_admin)):
    """Start background rebuild (stats + dedup)."""

    conn = get_conn()
    try:
        if _reparse_active(conn):
            raise HTTPException(status_code=409, detail="再解析が実行中です")
        if _derivative_fill_active(conn):
            raise HTTPException(status_code=409, detail="派生画像補完が実行中です")

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
                if not _rebuild_active(conn):
                    _kv_set(conn, "rebuild_bg_running", "1")
                    _kv_set(conn, "rebuild_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    threading.Thread(target=_rebuild_stats_worker, args=(int(run_id),), daemon=True).start()
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


@api_router.post("/admin/derivative_fill_start")
def admin_derivative_fill_start(_admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        if _reparse_active(conn):
            raise HTTPException(status_code=409, detail="再解析が実行中です")
        if _rebuild_active(conn):
            raise HTTPException(status_code=409, detail="統計再集計が実行中です")

        run_id = int(_kv_get(conn, "derivative_fill_run_id") or "0")
        run = _fetch_run(conn, run_id)

        if not run or run.get("status") != "running":
            run_id = _create_run(conn, "fill_derivatives_missing", {"batch": 100, "interval_sec": 0.1})
            _kv_set(conn, "derivative_fill_run_id", str(int(run_id)))
            _kv_set(conn, "derivative_fill_after_id", "0")
            _kv_set(conn, "derivative_fill_bg_running", "0")
            _kv_set(conn, "derivative_fill_bg_heartbeat", "0")
            conn.commit()

        active = _derivative_fill_active(conn)
        started = False
        if not active:
            with _MAINT_LOCK:
                if not _derivative_fill_active(conn):
                    _kv_set(conn, "derivative_fill_bg_running", "1")
                    _kv_set(conn, "derivative_fill_bg_heartbeat", str(_hb_now()))
                    conn.commit()
                    threading.Thread(target=_fill_missing_derivatives_worker, args=(int(run_id),), daemon=True).start()
                    started = True

        return {"ok": True, "run_id": int(run_id), "started": bool(started)}
    finally:
        conn.close()


@api_router.get("/admin/derivative_fill_state")
def admin_derivative_fill_state(_admin: dict = Depends(require_admin)):
    conn = get_conn()
    try:
        run_id = int(_kv_get(conn, "derivative_fill_run_id") or "0")
        after_id = int(_kv_get(conn, "derivative_fill_after_id") or "0")
        run = _fetch_run(conn, run_id)

        running_flag = int(_kv_get(conn, "derivative_fill_bg_running") or "0")
        hb_ts = int(_kv_get(conn, "derivative_fill_bg_heartbeat") or "0")
        active = _hb_active(running_flag, hb_ts) and (not run or (run.get("status") == "running"))

        hb_age_sec = None
        try:
            hb_age_sec = int(_hb_now() - int(hb_ts or 0)) if int(hb_ts or 0) > 0 else None
        except Exception:
            hb_age_sec = None

        total_images = 0
        max_image_id = 0
        try:
            total_images = int(conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0)
            max_image_id = int(conn.execute("SELECT COALESCE(MAX(id),0) FROM images").fetchone()[0] or 0)
        except Exception:
            total_images = 0
            max_image_id = 0

        grid_missing = _estimate_missing_derivative_count(conn, "grid")
        overlay_missing = _estimate_missing_derivative_count(conn, "overlay")

        errors: list[dict] = []
        if run_id > 0:
            try:
                rows = conn.execute(
                    """
                    SELECT id, image_id, stage, error, created_at
                    FROM maintenance_errors
                    WHERE run_id=?
                    ORDER BY id DESC
                    LIMIT 100
                    """,
                    (run_id,),
                ).fetchall()
                for r in rows:
                    if isinstance(r, tuple):
                        errors.append({
                            "id": int(r[0]),
                            "image_id": int(r[1] or 0),
                            "stage": r[2],
                            "error": r[3],
                            "created_at": r[4],
                        })
                    else:
                        errors.append({
                            "id": int(r["id"]),
                            "image_id": int(r["image_id"] or 0),
                            "stage": r["stage"],
                            "error": r["error"],
                            "created_at": r["created_at"],
                        })
            except Exception:
                errors = []

        history: list[dict] = []
        try:
            rows2 = conn.execute(
                """
                SELECT id, kind, status, created_at, updated_at, last_image_id, processed, updated, error_count
                FROM maintenance_runs
                WHERE kind='fill_derivatives_missing'
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
            "run": run,
            "active": bool(active),
            "hb_age_sec": hb_age_sec,
            "total_images": total_images,
            "max_image_id": max_image_id,
            "grid_missing": grid_missing,
            "overlay_missing": overlay_missing,
            "errors": errors,
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
        role = str(user.get("role") or "user")
        if role in {"admin", "master"}:
            rows = conn.execute(
                "SELECT software, image_count FROM stat_software ORDER BY image_count DESC, software ASC"
            ).fetchall()
            return [{"software": r["software"], "count": r["image_count"]} for r in rows]

        # For normal users, the sidebar software counts must reflect *visible creators*.
        uid = int(user.get("id") or 0)

        # Best-effort backfill if the new table exists but hasn't been built yet.
        try:
            has = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stat_creator_software'"
            ).fetchone()
            if has:
                n = int(conn.execute("SELECT COUNT(*) AS n FROM stat_creator_software").fetchone()[0])
                if n == 0:
                    stats_service.rebuild_creator_software(conn)
                    conn.commit()
        except Exception:
            pass

        try:
            rows = conn.execute(
                """
                SELECT software, SUM(image_count) AS c
                FROM stat_creator_software cs
                WHERE software IS NOT NULL AND TRIM(software) <> ''
                  AND (
                    cs.creator_id = ?
                    OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=cs.creator_id)
                  )
                GROUP BY software
                ORDER BY c DESC, software ASC
                """,
                (uid, uid),
            ).fetchall()
            return [{"software": r["software"], "count": int(r["c"] or 0)} for r in rows]
        except Exception:
            # Fallback: exact aggregation on images table.
            rows = conn.execute(
                """
                SELECT images.software AS software, COUNT(*) AS c
                FROM images
                WHERE images.software IS NOT NULL AND TRIM(images.software) <> ''
                  AND (
                    images.uploader_user_id = ?
                    OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=images.uploader_user_id)
                  )
                GROUP BY images.software
                ORDER BY c DESC, software ASC
                """,
                (uid, uid),
            ).fetchall()
            return [{"software": r["software"], "count": int(r["c"] or 0)} for r in rows]
    finally:
        conn.close()


@api_router.get("/creators/list")
def my_creator_list(user: dict = Depends(get_user)):
    """List creators registered in the current user's author list (+ self).

    Returned items are used by the sidebar and the filter dropdown.
    """
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        rows = conn.execute(
            """
            SELECT u.id AS id,
                   u.username AS creator,
                   COALESCE(sc.image_count, 0) AS count,
                   COALESCE(us.share_works,0) AS share_works,
                   COALESCE(us.share_bookmarks,0) AS share_bookmarks,
                   CASE WHEN u.id = ? THEN 1 ELSE 0 END AS is_self
            FROM users u
            LEFT JOIN user_settings us ON us.user_id=u.id
            LEFT JOIN user_creators uc ON uc.user_id=? AND uc.creator_user_id=u.id
            LEFT JOIN stat_creators sc ON sc.creator=u.username
            WHERE u.id = ?
               OR (
                    uc.creator_user_id IS NOT NULL
                    AND u.disabled=0
                    AND COALESCE(us.share_works,0)=1
               )
            ORDER BY is_self DESC, LOWER(u.username) ASC
            """,
            (uid, uid, uid),
        ).fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "creator": str(r["creator"] or ""),
                    "count": int(r["count"] or 0),
                    "share_works": int(r["share_works"] or 0),
                    "share_bookmarks": int(r["share_bookmarks"] or 0),
                    "is_self": int(r["is_self"] or 0),
                }
            )
        return out
    finally:
        conn.close()


class CreatorListReq(BaseModel):
    username: str | None = None
    user_id: int | None = None


@api_router.post("/creators/list")
def add_creator_to_list(req: CreatorListReq, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        target_id = None
        if req.user_id is not None:
            try:
                target_id = int(req.user_id)
            except Exception:
                target_id = None
        if target_id is None and req.username:
            row = _find_user_by_username(conn, str(req.username).strip(), "id")
            target_id = int(row["id"]) if row else None
        if not target_id or int(target_id) <= 0:
            raise HTTPException(status_code=404, detail="user not found")
        if int(target_id) == uid:
            return {"ok": True}

        # Only allow adding creators that explicitly share works.
        srow = conn.execute(
            """
            SELECT u.id,
                   u.disabled,
                   COALESCE(us.share_works,0) AS share_works
            FROM users u
            LEFT JOIN user_settings us ON us.user_id=u.id
            WHERE u.id=?
            """,
            (int(target_id),),
        ).fetchone()
        if not srow or int(srow["disabled"] or 0) == 1:
            raise HTTPException(status_code=404, detail="user not found")
        if int(srow["share_works"] or 0) != 1:
            raise HTTPException(status_code=400, detail="user does not share works")

        conn.execute(
            "INSERT OR IGNORE INTO user_creators(user_id, creator_user_id) VALUES (?,?)",
            (uid, int(target_id)),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.delete("/creators/list/{creator_id}")
def remove_creator_from_list(creator_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        cid = int(creator_id)
        if cid <= 0 or cid == uid:
            return {"ok": True}
        conn.execute(
            "DELETE FROM user_creators WHERE user_id=? AND creator_user_id=?",
            (uid, cid),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.get("/bookmarks/sidebar")
def bookmark_sidebar(user: dict = Depends(get_user)):
    """Sidebar bookmark lists: own lists + subscribed creators' lists (read-only)."""
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        _ensure_default_bookmark_list(conn, uid)
        mine, any_count = _list_bookmark_lists(conn, uid)

        # creators subscribed by current user
        creators = conn.execute(
            """
            SELECT u.id AS id, u.username AS username,
                   COALESCE(us.share_works,0) AS share_works,
                   COALESCE(us.share_bookmarks,0) AS share_bookmarks
            FROM user_bookmark_creators sub
            JOIN users u ON u.id=sub.creator_user_id
            LEFT JOIN user_settings us ON us.user_id=u.id
            WHERE sub.user_id=?
            ORDER BY LOWER(u.username) ASC
            """,
            (uid,),
        ).fetchall()

        others = []
        for r in creators:
            cid = int(r["id"])
            if cid == uid:
                continue
            # show only when creator currently shares bookmarks
            if int(r["share_bookmarks"] or 0) != 1:
                continue

            # Ensure shared creators have at least the default list; otherwise the UI looks broken.
            _ensure_default_bookmark_list(conn, cid)

            lists_rows = conn.execute(
                """
                SELECT bl.id, bl.name, bl.is_default,
                       (SELECT COUNT(*) FROM bookmarks b WHERE b.list_id = bl.id) AS cnt
                FROM bookmark_lists bl
                WHERE bl.user_id=?
                ORDER BY bl.sort_order ASC, bl.id ASC
                """,
                (cid,),
            ).fetchall()
            lists = []
            for lr in lists_rows:
                lists.append(
                    {
                        "id": int(lr["id"]),
                        "name": str(lr["name"] or ""),
                        "is_default": int(lr["is_default"] or 0),
                        "count": int(lr["cnt"] or 0),
                    }
                )
            others.append(
                {
                    "creator_id": cid,
                    "creator": str(r["username"] or ""),
                    "lists": lists,
                }
            )

        return {"mine": {"lists": mine, "any_count": int(any_count)}, "others": others}
    finally:
        conn.close()


class BookmarkSubReq(BaseModel):
    username: str | None = None
    user_id: int | None = None


@api_router.post("/bookmarks/subscriptions")
def add_bookmark_subscription(req: BookmarkSubReq, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        target_id = None
        if req.user_id is not None:
            try:
                target_id = int(req.user_id)
            except Exception:
                target_id = None
        if target_id is None and req.username:
            row = _find_user_by_username(conn, str(req.username).strip(), "id")
            target_id = int(row["id"]) if row else None
        if not target_id or int(target_id) <= 0:
            raise HTTPException(status_code=404, detail="user not found")
        if int(target_id) == uid:
            return {"ok": True}

        # Only allow subscribing to creators that currently share bookmarks.
        srow = conn.execute(
            """
            SELECT u.id,
                   u.disabled,
                   COALESCE(us.share_works,0) AS share_works,
                   COALESCE(us.share_bookmarks,0) AS share_bookmarks
            FROM users u
            LEFT JOIN user_settings us ON us.user_id=u.id
            WHERE u.id=?
            """,
            (int(target_id),),
        ).fetchone()
        if not srow or int(srow["disabled"] or 0) == 1:
            raise HTTPException(status_code=404, detail="user not found")
        if int(srow["share_bookmarks"] or 0) != 1:
            raise HTTPException(status_code=400, detail="user does not share bookmarks")

        conn.execute(
            "INSERT OR IGNORE INTO user_bookmark_creators(user_id, creator_user_id) VALUES (?,?)",
            (uid, int(target_id)),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.delete("/bookmarks/subscriptions/{creator_id}")
def remove_bookmark_subscription(creator_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        cid = int(creator_id)
        if cid <= 0 or cid == uid:
            return {"ok": True}
        conn.execute(
            "DELETE FROM user_bookmark_creators WHERE user_id=? AND creator_user_id=?",
            (uid, cid),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@api_router.get("/users/suggest")
def users_suggest(kind: str = "creators", q: str = "", limit: int = 20, user: dict = Depends(get_user)):
    """Suggest users for creator list / bookmark subscriptions.

    - When q is empty: returns random 10.
    - When q is given: partial match (LIKE).
    """
    kind = (kind or "creators").strip().lower()
    limit = max(1, min(40, int(limit or 20)))
    conn = get_conn()
    try:
        uid = int(user.get("id") or 0)
        q2 = str(q or "").strip()

        # Filter candidates by kind to match UI rules.
        # - creators: must share works
        # - bookmarks: must share bookmarks
        cond = "1=1"
        if kind == "bookmarks":
            cond = "COALESCE(us.share_bookmarks,0)=1"
        else:
            cond = "COALESCE(us.share_works,0)=1"

        if not q2:
            limit = min(limit, 10)
            sql = f"""
                SELECT u.id, u.username,
                       COALESCE(us.share_works,0) AS share_works,
                       COALESCE(us.share_bookmarks,0) AS share_bookmarks,
                       CASE WHEN uc.creator_user_id IS NOT NULL THEN 1 ELSE 0 END AS in_creator_list,
                       CASE WHEN ub.creator_user_id IS NOT NULL THEN 1 ELSE 0 END AS in_bookmark_subs
                FROM users u
                LEFT JOIN user_settings us ON us.user_id=u.id
                LEFT JOIN user_creators uc ON uc.user_id=? AND uc.creator_user_id=u.id
                LEFT JOIN user_bookmark_creators ub ON ub.user_id=? AND ub.creator_user_id=u.id
                WHERE u.disabled=0 AND u.id <> ?
                  AND ({cond})
                ORDER BY RANDOM()
                LIMIT ?
            """
            rows = conn.execute(sql, (uid, uid, uid, limit)).fetchall()
        else:
            like = f"%{q2}%"
            sql = f"""
                SELECT u.id, u.username,
                       COALESCE(us.share_works,0) AS share_works,
                       COALESCE(us.share_bookmarks,0) AS share_bookmarks,
                       CASE WHEN uc.creator_user_id IS NOT NULL THEN 1 ELSE 0 END AS in_creator_list,
                       CASE WHEN ub.creator_user_id IS NOT NULL THEN 1 ELSE 0 END AS in_bookmark_subs
                FROM users u
                LEFT JOIN user_settings us ON us.user_id=u.id
                LEFT JOIN user_creators uc ON uc.user_id=? AND uc.creator_user_id=u.id
                LEFT JOIN user_bookmark_creators ub ON ub.user_id=? AND ub.creator_user_id=u.id
                WHERE u.disabled=0 AND u.id <> ? AND u.username LIKE ?
                  AND ({cond})
                ORDER BY LENGTH(u.username) ASC, LOWER(u.username) ASC
                LIMIT ?
            """
            rows = conn.execute(sql, (uid, uid, uid, like, limit)).fetchall()

        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r["id"]),
                    "username": str(r["username"] or ""),
                    "share_works": int(r["share_works"] or 0),
                    "share_bookmarks": int(r["share_bookmarks"] or 0),
                    "in_creator_list": int(r["in_creator_list"] or 0),
                    "in_bookmark_subs": int(r["in_bookmark_subs"] or 0),
                }
            )
        # kind is currently informational for the frontend; keep output stable.
        return {"items": out, "kind": kind}
    finally:
        conn.close()

@api_router.get("/stats/day_counts")
def stats_day_counts(month: str, user: dict = Depends(get_user)):
    # month=YYYY-MM
    if not month or len(month) != 7:
        raise HTTPException(status_code=400, detail="month must be YYYY-MM")
    conn = get_conn()
    try:
        role = str(user.get("role") or "user")
        if role in {"admin", "master"}:
            rows = conn.execute(
                "SELECT ymd, image_count FROM stat_day_counts WHERE ymd LIKE ? ORDER BY ymd ASC",
                (month + "-%",),
            ).fetchall()
            return [{"ymd": r["ymd"], "count": r["image_count"]} for r in rows]

        # Best-effort backfill for per-creator calendar stats (older DBs won't have it built).
        try:
            n = int(conn.execute("SELECT COUNT(*) FROM stat_creator_day_counts").fetchone()[0] or 0)
            if n == 0:
                nimg = int(conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0)
                if nimg > 0:
                    stats_service.rebuild_creator_day_counts(conn)
                    stats_service.rebuild_creator_month_counts(conn)
                    stats_service.rebuild_creator_year_counts(conn)
                    conn.commit()
        except Exception:
            pass

        uid = int(user.get("id") or 0)
        rows = conn.execute(
            """
            SELECT ymd, SUM(image_count) AS c
            FROM stat_creator_day_counts sc
            WHERE ymd LIKE ?
              AND (
                sc.creator_id = ?
                OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=sc.creator_id)
              )
            GROUP BY ymd
            ORDER BY ymd ASC
            """,
            (month + "-%", uid, uid),
        ).fetchall()
        return [{"ymd": r["ymd"], "count": int(r["c"] or 0)} for r in rows]
    finally:
        conn.close()

@api_router.get("/stats/month_counts")
def stats_month_counts(year: str, user: dict = Depends(get_user)):
    # year=YYYY
    if (not year) or (len(year) != 4) or (not year.isdigit()):
        raise HTTPException(status_code=400, detail="year must be YYYY")
    conn = get_conn()
    try:
        role = str(user.get("role") or "user")
        if role in {"admin", "master"}:
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

        try:
            n = int(conn.execute("SELECT COUNT(*) FROM stat_creator_month_counts").fetchone()[0] or 0)
            if n == 0:
                nimg = int(conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0)
                if nimg > 0:
                    stats_service.rebuild_creator_day_counts(conn)
                    stats_service.rebuild_creator_month_counts(conn)
                    stats_service.rebuild_creator_year_counts(conn)
                    conn.commit()
        except Exception:
            pass

        uid = int(user.get("id") or 0)
        rows = conn.execute(
            """
            SELECT ym, SUM(image_count) AS c
            FROM stat_creator_month_counts sm
            WHERE ym LIKE ?
              AND (
                sm.creator_id = ?
                OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=sm.creator_id)
              )
            GROUP BY ym
            ORDER BY ym ASC
            """,
            (year + "-%", uid, uid),
        ).fetchall()

        yrow = conn.execute(
            """
            SELECT SUM(image_count) AS c
            FROM stat_creator_year_counts sy
            WHERE year=?
              AND (
                sy.creator_id = ?
                OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=sy.creator_id)
              )
            """,
            (year, uid, uid),
        ).fetchone()
        year_total = int((yrow["c"] if yrow else 0) or 0)

        return {
            "year": year,
            "year_total": year_total,
            "items": [{"ym": r["ym"], "count": int(r["c"] or 0)} for r in rows],
        }
    finally:
        conn.close()


@api_router.get("/stats/year_counts")
def stats_year_counts(user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        role = str(user.get("role") or "user")
        if role in {"admin", "master"}:
            rows = conn.execute("SELECT year, image_count FROM stat_year_counts ORDER BY year DESC").fetchall()
            return [{"year": r["year"], "count": r["image_count"]} for r in rows]

        try:
            n = int(conn.execute("SELECT COUNT(*) FROM stat_creator_year_counts").fetchone()[0] or 0)
            if n == 0:
                nimg = int(conn.execute("SELECT COUNT(*) FROM images").fetchone()[0] or 0)
                if nimg > 0:
                    stats_service.rebuild_creator_day_counts(conn)
                    stats_service.rebuild_creator_month_counts(conn)
                    stats_service.rebuild_creator_year_counts(conn)
                    conn.commit()
        except Exception:
            pass

        uid = int(user.get("id") or 0)
        rows = conn.execute(
            """
            SELECT year, SUM(image_count) AS c
            FROM stat_creator_year_counts sy
            WHERE (
              sy.creator_id = ?
              OR EXISTS(SELECT 1 FROM user_creators uc WHERE uc.user_id=? AND uc.creator_user_id=sy.creator_id)
            )
            GROUP BY year
            ORDER BY year DESC
            """,
            (uid, uid),
        ).fetchall()
        return [{"year": r["year"], "count": int(r["c"] or 0)} for r in rows]
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
            # Substring match (so "girl" can suggest "1girl" etc.) while still preferring prefix hits.
            pattern = qn.replace("%", "\\%").replace("_", "\\_")
            rows = conn.execute(
                "SELECT tag_canonical, image_count, category FROM stat_tag_counts "
                "WHERE tag_canonical LIKE ? ESCAPE '\\' "
                "ORDER BY (CASE WHEN tag_canonical LIKE ? ESCAPE '\\' THEN 0 ELSE 1 END), image_count DESC "
                "LIMIT ?",
                ("%" + pattern + "%", pattern + "%", limit),
            ).fetchall()
        return [{"tag": r["tag_canonical"], "count": r["image_count"], "category": r["category"]} for r in rows]
    finally:
        conn.close()

def _lookup_alias(conn: sqlite3.Connection, tag_norm: str) -> str:
    if not tag_norm:
        return ""
    v = _TAG_ALIAS_CACHE.get(tag_norm)
    if v is not None:
        return v
    row = conn.execute("SELECT canonical FROM tag_aliases WHERE alias=?", (tag_norm,)).fetchone()
    canonical = str(row["canonical"]) if row else tag_norm
    _cache_put(_TAG_ALIAS_CACHE, _TAG_ALIAS_LOCK, tag_norm, canonical, _TAG_ALIAS_MAX)
    return canonical


def _tag_candidates_for_filter(conn: sqlite3.Connection, token: str, *, limit: int = 80) -> list[str]:
    """Resolve a user token into canonical tag candidates.

    - If the token matches an existing canonical tag, return [canonical].
    - Otherwise expand by substring match against stat_tag_counts.

    This allows queries like "girl" to match tags such as "1girl", and
    ambiguous character names to match their disambiguated canonical forms.
    """

    tn = normalize_tag(token or "")
    if not tn:
        return []
    canonical = _lookup_alias(conn, tn)

    # Prefer exact matches when they exist.
    try:
        r = conn.execute(
            "SELECT 1 FROM stat_tag_counts WHERE tag_canonical=? LIMIT 1",
            (canonical,),
        ).fetchone()
        if not r:
            r = conn.execute(
                "SELECT 1 FROM image_tags WHERE tag_canonical=? LIMIT 1",
                (canonical,),
            ).fetchone()
        if r:
            return [canonical]
    except Exception:
        # If stats tables are missing in some early DB, fall back to exact.
        return [canonical]

    # Expand by substring match (bounded list so the main query remains sane).
    pattern = canonical.replace("%", "\\%").replace("_", "\\_")
    rows = conn.execute(
        "SELECT tag_canonical FROM stat_tag_counts "
        "WHERE tag_canonical LIKE ? ESCAPE '\\' "
        "ORDER BY (CASE WHEN tag_canonical LIKE ? ESCAPE '\\' THEN 0 ELSE 1 END), image_count DESC "
        "LIMIT ?",
        ("%" + pattern + "%", pattern + "%", int(limit)),
    ).fetchall()
    out: list[str] = []
    for rr in rows:
        v = rr[0] if isinstance(rr, tuple) else rr["tag_canonical"]
        if v:
            out.append(str(v))
    return out


def _apply_tag_filters(
    conn: sqlite3.Connection,
    where: list[str],
    params: list,
    include_tokens: list[str],
    exclude_tokens: list[str],
    *,
    image_alias: str = "images",
) -> None:
    """Append SQL WHERE fragments for tag include/exclude.

    Each include token is ANDed together; each token matches if **any** of its
    candidate tags exists on the image (OR within the token).
    """

    inc_cands: list[list[str]] = []
    inc_all: set[str] = set()
    for t in include_tokens or []:
        cands = _tag_candidates_for_filter(conn, t)
        if not cands:
            # No possible matches => whole query is empty.
            where.append("0")
            continue
        # de-dupe while preserving order
        cands = list(dict.fromkeys([c for c in cands if c]))
        inc_cands.append(cands)
        inc_all.update(cands)

    exc_cands: list[list[str]] = []
    for t in exclude_tokens or []:
        cands = _tag_candidates_for_filter(conn, t)
        if not cands:
            continue
        cands = [c for c in list(dict.fromkeys([c for c in cands if c])) if c not in inc_all]
        if cands:
            exc_cands.append(cands)

    for cands in inc_cands:
        if len(cands) == 1:
            where.append(
                f"EXISTS (SELECT 1 FROM image_tags it WHERE it.image_id = {image_alias}.id AND it.tag_canonical = ?)"
            )
            params.append(cands[0])
        else:
            where.append(
                f"EXISTS (SELECT 1 FROM image_tags it WHERE it.image_id = {image_alias}.id AND it.tag_canonical IN (" + ",".join(["?"] * len(cands)) + "))"
            )
            params.extend(cands)

    for cands in exc_cands:
        if len(cands) == 1:
            where.append(
                f"NOT EXISTS (SELECT 1 FROM image_tags itn WHERE itn.image_id = {image_alias}.id AND itn.tag_canonical = ?)"
            )
            params.append(cands[0])
        else:
            where.append(
                f"NOT EXISTS (SELECT 1 FROM image_tags itn WHERE itn.image_id = {image_alias}.id AND itn.tag_canonical IN (" + ",".join(["?"] * len(cands)) + "))"
            )
            params.extend(cands)
def _category_group(conn: sqlite3.Connection, canonical: str, cat: int | None) -> str:
    key = (canonical or "", cat)
    v = _TAG_GROUP_CACHE.get(key)
    if v is not None:
        return v

    # groups for UI: artist / quality / character / other
    if cat == 1:
        grp = "artist"
    elif cat == 4:
        grp = "character"
    elif cat == 5 or _is_quality_tag(conn, canonical):
        grp = "quality"
    else:
        grp = "other"

    _cache_put(_TAG_GROUP_CACHE, _TAG_GROUP_LOCK, key, grp, _TAG_GROUP_MAX)
    return grp
def _get_tag_category(conn: sqlite3.Connection, canonical: str) -> int | None:
    if not canonical:
        return None
    v = _TAG_CAT_CACHE.get(canonical)
    if v is not None:
        return None if int(v) < 0 else int(v)
    row = conn.execute("SELECT category FROM tags_master WHERE tag=?", (canonical,)).fetchone()
    cat = int(row["category"]) if (row and row["category"] is not None) else None
    _cache_put(_TAG_CAT_CACHE, _TAG_CAT_LOCK, canonical, (-1 if cat is None else int(cat)), _TAG_CAT_MAX)
    return cat
def _effective_tag_category(conn: sqlite3.Connection, canonical: str, cat: int | None) -> int | None:
    """Return the category used for UI grouping.

    Keep artist/character categories from the master dictionary, but allow
    the bundled "extra-quality-tags.csv" (including wildcards) to upgrade
    otherwise-unknown tags into quality (5).

    This fixes cases like "masterpiece" being treated as "other" when the
    master dictionary doesn't label it as quality.
    """
    if not canonical:
        return cat

    v = _TAG_EFFECTIVE_CAT_CACHE.get(canonical)
    if v is not None:
        return None if int(v) < 0 else int(v)

    base = cat
    if base in (1, 4, 5):
        eff = base
    else:
        eff = base
        try:
            if _is_quality_tag(conn, canonical):
                eff = 5
        except Exception:
            pass

    _cache_put(_TAG_EFFECTIVE_CAT_CACHE, _TAG_EFFECTIVE_CAT_LOCK, canonical, (-1 if eff is None else int(eff)), _TAG_EFFECTIVE_CAT_MAX)
    return eff
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


def _write_ext_upload_to_temp(upload: UploadFile) -> tuple[str, str, int]:
    safe_name = _safe_basename(upload.filename or "upload")
    suffix = "." + safe_name.rsplit(".", 1)[-1] if "." in safe_name else ".bin"
    fd, tmp_path = tempfile.mkstemp(prefix="nim_ext_", suffix=suffix)
    os.close(fd)
    total = 0
    with open(tmp_path, "wb") as out:
        while True:
            chunk = upload.file.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            out.write(chunk)
    return tmp_path, safe_name, total


def _poll_ext_upload_job(*, request: Request, job_id: int, user_id: int, timeout_sec: float = 90.0) -> JSONResponse | dict:
    deadline = time.monotonic() + max(5.0, float(timeout_sec))
    last_status = ""
    last_error = None
    while time.monotonic() < deadline:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT id, user_id, status, error FROM upload_zip_jobs WHERE id=?",
                (int(job_id),),
            ).fetchone()
            if not row:
                return JSONResponse(status_code=404, content={"ok": False, "code": "JOB_NOT_FOUND", "message": "アップロードジョブが見つかりません"})
            if int(row["user_id"] or 0) != int(user_id):
                return JSONResponse(status_code=403, content={"ok": False, "code": "FORBIDDEN", "message": "forbidden"})
            last_status = str(row["status"] or "")
            last_error = row["error"]
            item = conn.execute(
                "SELECT seq, filename, state, image_id, message FROM upload_zip_items WHERE job_id=? ORDER BY seq ASC LIMIT 1",
                (int(job_id),),
            ).fetchone()
            if last_status == "done" and item:
                state_txt = str(item["state"] or "")
                if state_txt in {"完了", "重複"}:
                    image_id = int(item["image_id"] or 0)
                    return {
                        "ok": True,
                        "image_id": image_id,
                        "dedup": state_txt == "重複",
                        "message": ("既存画像として登録済みです" if state_txt == "重複" else "登録しました"),
                        "detail_url": _abs_url(f"/api/images/{image_id}/detail", request),
                        "job_id": int(job_id),
                    }
                if state_txt == "失敗":
                    return JSONResponse(
                        status_code=500,
                        content={
                            "ok": False,
                            "code": "UPLOAD_FAILED",
                            "message": str(item["message"] or last_error or "アップロードに失敗しました"),
                            "job_id": int(job_id),
                        },
                    )
            if last_status in {"error", "cancelled"}:
                return JSONResponse(
                    status_code=500,
                    content={
                        "ok": False,
                        "code": "UPLOAD_FAILED",
                        "message": str(last_error or "アップロードに失敗しました"),
                        "job_id": int(job_id),
                    },
                )
        finally:
            conn.close()
        time.sleep(0.2)
    return JSONResponse(
        status_code=504,
        content={
            "ok": False,
            "code": "UPLOAD_TIMEOUT",
            "message": "アップロード処理がタイムアウトしました",
            "job_id": int(job_id),
            "status": last_status or "processing",
        },
    )


@api_router.post("/ext/upload")
async def ext_upload_image(
    request: Request,
    file: UploadFile = File(...),
    last_modified_ms: str | None = Form(default=None),
    bookmark_enabled: str | None = Form(default=None),
    bookmark_list_id: str | None = Form(default=None),
    user: dict | None = Depends(get_user_optional),
):
    if not user:
        return _ext_auth_failed(request)

    tmp_path, safe_name, total = _write_ext_upload_to_temp(file)
    staging_dir: Path | None = None
    try:
        if total <= 0:
            return JSONResponse(status_code=400, content={"ok": False, "code": "EMPTY_FILE", "message": "画像データが空です"})
        if not _allowed_image_ext(safe_name):
            return JSONResponse(status_code=400, content={"ok": False, "code": "UNSUPPORTED_FILE_TYPE", "message": "unsupported file type"})

        conn = get_conn()
        try:
            upload_bookmark_list_id = _normalize_upload_bookmark_list_id(
                conn,
                user_id=int(user["id"]),
                bookmark_enabled=bookmark_enabled,
                bookmark_list_id=bookmark_list_id,
            )
            cur = conn.execute(
                "INSERT INTO upload_zip_jobs(user_id, filename, source_kind, staging_dir, bookmark_enabled, bookmark_list_id, total, status) VALUES (?,?,?,?,?,?,?,?)",
                (int(user["id"]), safe_name, "direct", "", 1 if upload_bookmark_list_id else 0, upload_bookmark_list_id, 1, "collecting"),
            )
            job_id = int(cur.lastrowid)
            staging_dir = _job_staging_dir(job_id)
            staging_dir.mkdir(parents=True, exist_ok=True)
            conn.execute(
                "UPDATE upload_zip_jobs SET staging_dir=?, updated_at_utc=datetime('now') WHERE id=?",
                (str(staging_dir), int(job_id)),
            )
            conn.commit()
        finally:
            conn.close()

        dst = staging_dir / f"{1:06d}_{safe_name}"
        shutil.move(tmp_path, dst)
        tmp_path = ""

        mtime_iso = _parse_last_modified_ms(last_modified_ms) or _staged_direct_item_mtime_iso(dst)
        try:
            parsed_dt = datetime.fromisoformat(str(mtime_iso).replace('Z', '+00:00'))
            ts = parsed_dt.timestamp()
            os.utime(dst, (ts, ts))
            mtime_iso = _staged_direct_item_mtime_iso(dst)
        except Exception:
            pass

        conn = get_conn()
        try:
            item_id = _upsert_direct_upload_item_row(
                conn,
                job_id=int(job_id),
                seq_i=1,
                filename=str(safe_name),
                staged_path=str(dst),
                mtime_iso=str(mtime_iso),
            )
            conn.execute(
                "UPDATE upload_zip_jobs SET total=1, updated_at_utc=datetime('now') WHERE id=?",
                (int(job_id),),
            )
            status, _staging_dir, _source_kind = _refresh_upload_job_progress(conn, int(job_id), seal=True)
            conn.execute(
                "UPDATE upload_zip_jobs SET error=NULL, updated_at_utc=datetime('now') WHERE id=?",
                (int(job_id),),
            )
            conn.commit()
        finally:
            conn.close()

        _queue_upload_item_request(int(item_id), source="ext_upload", trace_id=getattr(request.state, "trace_id", None))
        return _poll_ext_upload_job(request=request, job_id=int(job_id), user_id=int(user["id"]))
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except Exception:
                pass


@api_router.post("/upload")
async def upload_image(
    bg: BackgroundTasks,
    file: UploadFile = File(...),
    last_modified_ms: str | None = Form(default=None),
    bookmark_enabled: str | None = Form(default=None),
    bookmark_list_id: str | None = Form(default=None),
    user: dict = Depends(get_user),
):
    safe_name = _safe_basename(file.filename or "upload")
    suffix = "." + safe_name.rsplit(".", 1)[-1] if "." in safe_name else ".bin"
    fd, tmp_path = tempfile.mkstemp(prefix="nim_direct_", suffix=suffix)
    os.close(fd)
    total = 0
    try:
        with open(tmp_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                out.write(chunk)
        if total <= 0:
            raise HTTPException(status_code=400, detail="empty file")

        mtime = _parse_last_modified_ms(last_modified_ms)
        if not mtime:
            mtime = datetime.now(timezone.utc).isoformat()

        conn = get_conn()
        try:
            upload_bookmark_list_id = _normalize_upload_bookmark_list_id(
                conn,
                user_id=int(user["id"]),
                bookmark_enabled=bookmark_enabled,
                bookmark_list_id=bookmark_list_id,
            )
            return _upload_image_from_path_core(
                conn=conn,
                bg=bg,
                file_path=tmp_path,
                filename=safe_name,
                mime=(file.content_type or "application/octet-stream"),
                mtime_iso=mtime,
                user_id=int(user["id"]),
                username=str(user["username"]),
                upload_bookmark_list_id=upload_bookmark_list_id,
            )
        finally:
            conn.close()
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


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
    upload_bookmark_list_id: int | None = None,
) -> dict:
    """Core upload logic used by both direct upload and zip jobs.

    NOTE:
    This helper is shared by request uploads and zip workers. Do not read the
    request-scoped `user` dependency in here; use the explicit `user_id` /
    `username` arguments only.

    Keeps the existing semantics:
    - binary sha256 dedup
    - metadata extract
    - tag parsing + stats bumps
    - original and derivative images stored on disk; DB keeps metadata/paths
    - derivatives created in background (or inline when bg is None)
    """

    import hashlib

    sha = hashlib.sha256(raw).hexdigest()

    # dedup by binary
    row = conn.execute("SELECT id FROM images WHERE sha256=?", (sha,)).fetchone()
    if row:
        iid = int(row["id"])
        _apply_upload_bookmark(conn, user_id=int(user_id), image_id=iid, bookmark_list_id=upload_bookmark_list_id)
        conn.commit()
        return {"ok": True, "dedup": True, "image_id": iid, "thumb": _thumb_url(conn, iid, kind="grid"), "detail": _image_detail_summary(conn, iid, user_id=int(user_id)), "dedup_reason": "binary"}

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
    character_entries, main_negative_combined_raw = build_prompt_view_payload(conn, prompt_neg, prompt_char, meta.params if meta else None)
    character_entries_json = json.dumps(character_entries, ensure_ascii=False)
    potion_raw = json.dumps(meta.potion, ensure_ascii=False).encode("utf-8") if (meta and meta.potion) else None
    has_potion = 1 if (meta and meta.uses_potion) else 0
    uses_potion = 1 if (meta and meta.uses_potion) else 0
    uses_precise_reference = 1 if (meta and meta.uses_precise_reference) else 0
    sampler = meta.sampler if meta else None
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
            iid = int(row2["id"])
            _apply_upload_bookmark(conn, user_id=int(user_id), image_id=iid, bookmark_list_id=upload_bookmark_list_id)
            conn.commit()
            return {"ok": True, "dedup": True, "image_id": iid, "thumb": _thumb_url(conn, iid, kind="grid"), "detail": _image_detail_summary(conn, iid, user_id=int(user_id)), "dedup_reason": "full"}

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

    # NSFW: also treat the main prompt itself containing 'nsfw' as NSFW.
    if not is_nsfw:
        ptxt = (prompt_pos or "")
        if ptxt and ("nsfw" in ptxt.lower()):
            is_nsfw = 1

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
          public_id, sha256, original_filename, ext, mime, width, height, file_mtime_utc, uploader_user_id,
          software, model_name, prompt_positive_raw, prompt_negative_raw, params_json,
          prompt_character_raw, character_entries_json, main_negative_combined_raw,
          seed,
          potion_raw, has_potion, uses_potion, uses_precise_reference, sampler, metadata_raw, main_sig_hash, dedup_flag
          , full_meta_hash
          , favorite, is_nsfw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            _new_public_id(),
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
            character_entries_json,
            main_negative_combined_raw,
            seed,
            potion_raw,
            has_potion,
            uses_potion,
            uses_precise_reference,
            sampler,
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
        stats_service.bump_creator_software(conn, int(user_id), software)
    if mtime_iso:
        ymd = mtime_iso[:10]
        stats_service.bump_day(conn, ymd)
        stats_service.bump_month(conn, ymd[:7])
        stats_service.bump_year(conn, ymd[:4])
        stats_service.bump_creator_day(conn, int(user_id), ymd)
        stats_service.bump_creator_month(conn, int(user_id), ymd[:7])
        stats_service.bump_creator_year(conn, int(user_id), ymd[:4])

    _apply_upload_bookmark(conn, user_id=int(user_id), image_id=int(image_id), bookmark_list_id=upload_bookmark_list_id)

    conn.commit()

    _queue_derivative_request(
        int(image_id),
        ("grid", "overlay"),
        source="upload",
    )

    return {"ok": True, "dedup": False, "image_id": image_id, "dedup_flag": dedup_flag, "thumb": _thumb_url(conn, int(image_id), kind="grid"), "detail": _image_detail_summary(conn, int(image_id), user_id=int(user_id))}


def _build_derivative_rows_from_base_image(
    base_image,
    *,
    kinds: tuple[str, ...] = ("grid", "overlay"),
) -> list[tuple[str, str, int, int, int, bytes]]:
    rows: list[tuple[str, str, int, int, int, bytes]] = []
    clean_kinds = _normalize_derivative_kinds(kinds)
    do_avif = avif_available()
    for kind in clean_kinds:
        target = DERIVATIVE_TARGETS[kind]
        variant = make_resized_variant(base_image, max_side=int(target.max_side))
        try:
            width, height = int(variant.size[0]), int(variant.size[1])
            if do_avif and target.avif.enabled:
                rows.append((
                    kind,
                    "avif",
                    width,
                    height,
                    int(target.avif.quality),
                    encode_avif_image(
                        variant,
                        quality=int(target.avif.quality),
                        speed=int(target.avif.speed),
                        codec=str(target.avif.codec),
                        max_threads=int(target.avif.max_threads),
                    ),
                ))
            rows.append((
                kind,
                "webp",
                width,
                height,
                int(target.webp.quality),
                encode_webp_image(
                    variant,
                    quality=int(target.webp.quality),
                    method=int(target.webp.method),
                    lossless=bool(target.webp.lossless),
                    alpha_quality=int(target.webp.alpha_quality),
                ),
            ))
        finally:
            try:
                variant.close()
            except Exception:
                pass
    return rows



def _refresh_upload_job_progress(conn: sqlite3.Connection, job_id: int, *, seal: bool = False) -> tuple[str, str, str]:
    row = conn.execute(
        "SELECT total, status, staging_dir, source_kind FROM upload_zip_jobs WHERE id=?",
        (int(job_id),),
    ).fetchone()
    if not row:
        return "", "", ""
    counts = conn.execute(
        """
        SELECT
          COUNT(*) AS n_all,
          SUM(CASE WHEN state IN ('完了','重複') THEN 1 ELSE 0 END) AS n_done,
          SUM(CASE WHEN state='失敗' THEN 1 ELSE 0 END) AS n_failed,
          SUM(CASE WHEN state='重複' THEN 1 ELSE 0 END) AS n_dup,
          SUM(CASE WHEN state='処理中' THEN 1 ELSE 0 END) AS n_running,
          SUM(CASE WHEN state IN ('待機','受信済み') THEN 1 ELSE 0 END) AS n_pending
        FROM upload_zip_items
        WHERE job_id=?
        """,
        (int(job_id),),
    ).fetchone()
    total_items = int(counts["n_all"] or 0) if counts else 0
    done = int(counts["n_done"] or 0) if counts else 0
    failed = int(counts["n_failed"] or 0) if counts else 0
    dup = int(counts["n_dup"] or 0) if counts else 0
    running = int(counts["n_running"] or 0) if counts else 0
    pending = int(counts["n_pending"] or 0) if counts else 0
    total = max(int(row["total"] or 0), total_items)
    cur_status = str(row["status"] or "")
    new_status = cur_status
    if cur_status not in {"error", "cancelled"}:
        if cur_status == "collecting" and not seal:
            new_status = "collecting"
        else:
            finished = done + failed
            if total > 0 and finished >= total:
                new_status = "done"
            elif running > 0 or pending > 0:
                new_status = "running"
            else:
                new_status = "queued"
    conn.execute(
        "UPDATE upload_zip_jobs SET total=?, done=?, failed=?, dup=?, status=?, updated_at_utc=datetime('now') WHERE id=?",
        (int(total), int(done), int(failed), int(dup), new_status, int(job_id)),
    )
    return new_status, str(row["staging_dir"] or ""), str(row["source_kind"] or "")



def _cleanup_upload_staging_dir(path_str: str) -> None:
    if not path_str:
        return
    try:
        shutil.rmtree(path_str, ignore_errors=True)
    except Exception:
        pass



def _process_upload_item_job(item_id: int, source: str | None = None, trace_id: str | None = None) -> None:
    conn = get_conn()
    try:
        row = conn.execute(
            """
            SELECT i.id, i.job_id, i.filename, i.staged_path, i.mtime_iso, i.state,
                   j.status AS job_status, j.source_kind, j.staging_dir, j.user_id,
                   j.bookmark_enabled, j.bookmark_list_id,
                   u.username AS username
            FROM upload_zip_items i
            JOIN upload_zip_jobs j ON j.id=i.job_id
            JOIN users u ON u.id=j.user_id
            WHERE i.id=?
            """,
            (int(item_id),),
        ).fetchone()
        if not row:
            return
        job_id = int(row["job_id"])
        staged_path = str(row["staged_path"] or "")
        if str(row["job_status"] or "") == "cancelled":
            conn.execute("UPDATE upload_zip_items SET state='失敗', message=? WHERE id=?", ("cancelled", int(item_id)))
            status, staging_dir, _source_kind = _refresh_upload_job_progress(conn, job_id, seal=False)
            conn.commit()
            if status == "done":
                _cleanup_upload_staging_dir(staging_dir)
            return
        conn.execute("UPDATE upload_zip_items SET state='処理中', message=NULL WHERE id=?", (int(item_id),))
        status, _staging_dir, _source_kind = _refresh_upload_job_progress(conn, job_id, seal=False)
        conn.commit()
        upload_bookmark_list_id = _job_upload_bookmark_list_id(
            conn,
            user_id=int(row["user_id"]),
            bookmark_enabled=row["bookmark_enabled"],
            bookmark_list_id=row["bookmark_list_id"],
        )
        res = _upload_image_from_path_core(
            conn=conn,
            bg=None,
            file_path=staged_path,
            filename=str(row["filename"] or "upload"),
            mime=(mimetypes.guess_type(str(row["filename"] or ""))[0] or "application/octet-stream"),
            mtime_iso=str(row["mtime_iso"] or datetime.now(timezone.utc).isoformat()),
            user_id=int(row["user_id"]),
            username=str(row["username"] or ""),
            ensure_derivatives=True,
            derivative_kinds=("grid", "overlay"),
            upload_bookmark_list_id=upload_bookmark_list_id,
        )
        state_txt = "重複" if bool(res.get("dedup")) else "完了"
        image_id = int(res.get("image_id") or 0) or None
        msg = str(res.get("dedup_reason") or "") or None
        conn.execute(
            "UPDATE upload_zip_items SET state=?, image_id=?, message=? WHERE id=?",
            (state_txt, image_id, msg, int(item_id)),
        )
        status, staging_dir, _source_kind = _refresh_upload_job_progress(conn, job_id, seal=False)
        conn.commit()
        if status == "done":
            _cleanup_upload_staging_dir(staging_dir)
    except Exception as e:
        try:
            conn.execute(
                "UPDATE upload_zip_items SET state='失敗', image_id=NULL, message=? WHERE id=?",
                (f"{type(e).__name__}: {e}"[:255], int(item_id)),
            )
            row2 = conn.execute("SELECT job_id FROM upload_zip_items WHERE id=?", (int(item_id),)).fetchone()
            if row2:
                status, staging_dir, _source_kind = _refresh_upload_job_progress(conn, int(row2["job_id"]), seal=False)
            else:
                status, staging_dir = "", ""
            conn.commit()
            if status == "done":
                _cleanup_upload_staging_dir(staging_dir)
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
        raise
    finally:
        conn.close()



def _upload_image_from_path_core(
    *,
    conn: sqlite3.Connection,
    bg: BackgroundTasks | None,
    file_path: str,
    filename: str,
    mime: str,
    mtime_iso: str,
    user_id: int,
    username: str,
    ensure_derivatives: bool = True,
    derivative_kinds: tuple[str, ...] = ("grid", "overlay"),
    upload_bookmark_list_id: int | None = None,
) -> dict:
    import hashlib
    from PIL import Image, ImageOps

    raw = Path(file_path).read_bytes()
    total = len(raw)
    if total <= 0:
        raise HTTPException(status_code=400, detail="empty file")

    sha = hashlib.sha256(raw).hexdigest()
    row = conn.execute("SELECT id FROM images WHERE sha256=?", (sha,)).fetchone()
    if row:
        iid = int(row["id"])
        _apply_upload_bookmark(conn, user_id=int(user_id), image_id=iid, bookmark_list_id=upload_bookmark_list_id)
        conn.commit()
        return {"ok": True, "dedup": True, "image_id": iid, "dedup_reason": "binary"}

    width = height = None
    _fmt = ""
    base_image = None
    try:
        with Image.open(io.BytesIO(raw)) as im:
            width, height = im.size
            _fmt = (im.format or "").upper()
            im.load()
            transposed = ImageOps.exif_transpose(im)
            transposed.load()
            base_image = transposed.copy()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"invalid image file: {exc.__class__.__name__}")

    ext = (filename.rsplit(".", 1)[-1].lower() if "." in filename else "bin")

    meta = None
    try:
        meta = extract_novelai_metadata_bytes(raw)
    except Exception:
        meta = None

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
    character_entries, main_negative_combined_raw = build_prompt_view_payload(conn, prompt_neg, prompt_char, meta.params if meta else None)
    character_entries_json = json.dumps(character_entries, ensure_ascii=False)
    potion_raw = json.dumps(meta.potion, ensure_ascii=False).encode("utf-8") if (meta and meta.potion) else None
    has_potion = 1 if (meta and meta.uses_potion) else 0
    uses_potion = 1 if (meta and meta.uses_potion) else 0
    uses_precise_reference = 1 if (meta and meta.uses_precise_reference) else 0
    sampler = meta.sampler if meta else None
    metadata_raw = None
    if meta:
        try:
            metadata_raw = json.dumps({"info": meta.raw, "json": meta.raw_json_str}, ensure_ascii=False)[:65535]
        except Exception:
            metadata_raw = None

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

    if not is_nsfw:
        ptxt = (prompt_pos or "")
        if ptxt and ("nsfw" in ptxt.lower()):
            is_nsfw = 1

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
          public_id, sha256, original_filename, ext, mime, width, height, file_mtime_utc, uploader_user_id,
          software, model_name, prompt_positive_raw, prompt_negative_raw, params_json,
          prompt_character_raw, character_entries_json, main_negative_combined_raw,
          seed,
          potion_raw, has_potion, uses_potion, uses_precise_reference, sampler, metadata_raw, main_sig_hash, dedup_flag
          , full_meta_hash
          , favorite, is_nsfw
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            _new_public_id(),
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
            character_entries_json,
            main_negative_combined_raw,
            seed,
            potion_raw,
            has_potion,
            uses_potion,
            uses_precise_reference,
            sampler,
            metadata_raw,
            sig,
            dedup_flag,
            full_meta_hash,
            0,
            is_nsfw,
        ),
    )
    image_id = int(cur.lastrowid)

    ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
    base = _safe_basename(filename or "upload")
    ext0 = (ext or "").strip(".").lower()
    if ext0 and not base.lower().endswith("." + ext0):
        if "." not in base:
            base = base + "." + ext0
    fn = f"{image_id}_{base}"
    path = ORIGINALS_DIR / fn
    if path.exists():
        suf = (sha or "")[:10] or str(int(datetime.now(timezone.utc).timestamp()))
        stem, dot, sx = fn.rpartition(".")
        if dot:
            fn = f"{stem}_{suf}.{sx}"
        else:
            fn = f"{fn}_{suf}"
        path = ORIGINALS_DIR / fn

    tmp = str(path) + ".tmp"
    try:
        os.replace(file_path, tmp)
    except Exception:
        shutil.copyfile(file_path, tmp)
        try:
            os.remove(file_path)
        except Exception:
            pass
    os.replace(tmp, str(path))
    disk_path = str(path)

    conn.execute(
        "INSERT INTO image_files(image_id, disk_path, size, bytes) VALUES (?,?,?,NULL)",
        (image_id, disk_path, int(total)),
    )

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

    uniq: dict[str, int | None] = {}
    for (canonical, _tag_text, _tag_raw, cat, _etype, _brace, _numw, _group, _src_mask, _seq) in _iter_tag_rows(tag_rows):
        if canonical not in uniq or (uniq[canonical] is None and cat is not None):
            uniq[canonical] = cat
    for canonical, cat in uniq.items():
        stats_service.bump_tag(conn, canonical, cat)

    stats_service.bump_creator(conn, username)
    if software:
        stats_service.bump_software(conn, software)
        stats_service.bump_creator_software(conn, int(user_id), software)
    if mtime_iso:
        ymd = mtime_iso[:10]
        stats_service.bump_day(conn, ymd)
        stats_service.bump_month(conn, ymd[:7])
        stats_service.bump_year(conn, ymd[:4])
        stats_service.bump_creator_day(conn, int(user_id), ymd)
        stats_service.bump_creator_month(conn, int(user_id), ymd[:7])
        stats_service.bump_creator_year(conn, int(user_id), ymd[:4])

    _apply_upload_bookmark(conn, user_id=int(user_id), image_id=int(image_id), bookmark_list_id=upload_bookmark_list_id)

    if ensure_derivatives and base_image is not None:
        derivative_created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        for kind, fmt, dw, dh, quality, blob in _build_derivative_rows_from_base_image(base_image, kinds=derivative_kinds):
            _upsert_derivative_file(
                conn,
                image_id=int(image_id),
                kind=kind,
                fmt=fmt,
                width=int(dw),
                height=int(dh),
                quality=int(quality),
                raw=blob,
                created_at_utc=derivative_created_at_utc,
            )

    conn.commit()

    if base_image is not None:
        try:
            base_image.close()
        except Exception:
            pass

    return {"ok": True, "dedup": False, "image_id": image_id, "dedup_flag": dedup_flag}


@api_router.post("/upload_zip")
async def upload_zip(
    bg: BackgroundTasks,
    request: Request,
    user: dict = Depends(get_user),
):
    filename = request.query_params.get("filename") or "upload.zip"
    bookmark_enabled = request.query_params.get("bookmark_enabled")
    bookmark_list_id = request.query_params.get("bookmark_list_id")

    # Stream to disk (avoid loading large zip into memory)
    import tempfile
    fd, tmp = tempfile.mkstemp(prefix="nim_zip_", suffix=".zip")
    os.close(fd)
    total_bytes = 0
    try:
        with open(tmp, "wb") as f:
            async for chunk in request.stream():
                if not chunk:
                    continue
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
        upload_bookmark_list_id = _normalize_upload_bookmark_list_id(
            conn,
            user_id=int(user["id"]),
            bookmark_enabled=bookmark_enabled,
            bookmark_list_id=bookmark_list_id,
        )
        cur = conn.execute(
            "INSERT INTO upload_zip_jobs(user_id, filename, source_kind, staging_dir, bookmark_enabled, bookmark_list_id, total, status) VALUES (?,?,?,?,?,?,?,?)",
            (int(user["id"]), filename, "zip", "", 1 if upload_bookmark_list_id else 0, upload_bookmark_list_id, int(total), "queued"),
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
    request: Request,
    user: dict = Depends(get_user),
):
    filename = request.query_params.get("filename") or "upload.zip"
    bookmark_enabled = request.query_params.get("bookmark_enabled")
    bookmark_list_id = request.query_params.get("bookmark_list_id")
    try:
        total_bytes = int(request.query_params.get("total_bytes") or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid total_bytes")

    _zip_incoming_gc()
    import tempfile
    fd, tmp = tempfile.mkstemp(prefix="nim_zipc_", suffix=".zip")
    os.close(fd)
    token = uuid.uuid4().hex
    conn = get_conn()
    try:
        upload_bookmark_list_id = _normalize_upload_bookmark_list_id(
            conn,
            user_id=int(user["id"]),
            bookmark_enabled=bookmark_enabled,
            bookmark_list_id=bookmark_list_id,
        )
    finally:
        conn.close()
    with _ZIP_INCOMING_LOCK:
        _ZIP_INCOMING[token] = {
            "user_id": int(user["id"]),
            "filename": filename,
            "total_bytes": int(total_bytes or 0),
            "received": 0,
            "tmp": tmp,
            "bookmark_enabled": 1 if upload_bookmark_list_id else 0,
            "bookmark_list_id": upload_bookmark_list_id,
            "updated_at": time.time(),
        }
    return {"ok": True, "token": token}


@api_router.post("/upload_zip_chunk/append")
async def upload_zip_chunk_append(
    request: Request,
    user: dict = Depends(get_user),
):
    token = request.query_params.get("token") or ""
    if not token:
        raise HTTPException(status_code=400, detail="missing token")
    try:
        off = int(request.query_params.get("offset") or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid offset")

    _zip_incoming_gc()
    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
    if not st:
        raise HTTPException(status_code=404, detail="token not found")
    if int(st.get("user_id") or 0) != int(user["id"]):
        raise HTTPException(status_code=403, detail="forbidden")
    tmp = str(st.get("tmp") or "")
    expected = int(st.get("received") or 0)

    # Idempotent append: allow retry/resume.
    # - if off > expected: client skipped bytes
    # - if off < expected: treat as retry; truncate back to off and overwrite
    if off > expected:
        raise HTTPException(status_code=409, detail="offset mismatch")

    wrote = 0
    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
        if not st:
            raise HTTPException(status_code=404, detail="token not found")
        if off < int(st.get("received") or 0):
            st["received"] = off
        st["updated_at"] = time.time()

    # write chunk at offset (seek + truncate if needed)
    try:
        with open(tmp, "r+b") as f:
            f.seek(off)
            f.truncate(off)
            async for b in request.stream():
                if not b:
                    continue
                wrote += len(b)
                f.write(b)
    except FileNotFoundError:
        # should not happen, but create and retry once
        with open(tmp, "wb") as _f:
            pass
        with open(tmp, "r+b") as f:
            f.seek(off)
            f.truncate(off)
            async for b in request.stream():
                if not b:
                    continue
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
    request: Request,
    user: dict = Depends(get_user),
):
    token = request.query_params.get("token") or ""
    if not token:
        raise HTTPException(status_code=400, detail="missing token")

    _zip_incoming_gc()
    with _ZIP_INCOMING_LOCK:
        st = _ZIP_INCOMING.get(token)
    if not st:
        raise HTTPException(status_code=404, detail="token not found")
    if int(st.get("user_id") or 0) != int(user["id"]):
        raise HTTPException(status_code=403, detail="forbidden")

    tmp = str(st.get("tmp") or "")
    filename = str(st.get("filename") or "upload.zip")
    bookmark_enabled = int(st.get("bookmark_enabled") or 0)
    bookmark_list_id = st.get("bookmark_list_id")
    received = int(st.get("received") or 0)
    total_bytes = int(st.get("total_bytes") or 0)

    if total_bytes > 0 and received < total_bytes:
        raise HTTPException(status_code=409, detail="upload not complete")

    # Same as /upload_zip: defer scanning to the worker.
    total = 0

    conn = get_conn()
    try:
        cur = conn.execute(
            "INSERT INTO upload_zip_jobs(user_id, filename, source_kind, staging_dir, bookmark_enabled, bookmark_list_id, total, status) VALUES (?,?,?,?,?,?,?,?)",
            (int(user["id"]), filename, "zip", "", int(bookmark_enabled or 0), (int(bookmark_list_id) if bookmark_list_id else None), int(total), "queued"),
        )
        job_id = int(cur.lastrowid)
        conn.commit()
    finally:
        conn.close()

    with _ZIP_INCOMING_LOCK:
        _ZIP_INCOMING.pop(token, None)

    bg.add_task(_upload_zip_worker, job_id, tmp, int(user["id"]), str(user["username"]))
    return {"ok": True, "job_id": job_id, "total": int(total)}


class UploadBatchInitReq(BaseModel):
    total: int = 0
    bookmark_enabled: int | bool | str | None = None
    bookmark_list_id: int | None = None


@api_router.post("/upload_batch/init")
async def upload_batch_init(request: Request, user: dict = Depends(get_user)):
    req = _validate_body_model(UploadBatchInitReq, await _read_json_body_loose(request))
    requested_total = max(0, min(100000, int(req.total or 0)))
    conn = get_conn()
    try:
        upload_bookmark_list_id = _normalize_upload_bookmark_list_id(
            conn,
            user_id=int(user["id"]),
            bookmark_enabled=req.bookmark_enabled,
            bookmark_list_id=req.bookmark_list_id,
        )
        cur = conn.execute(
            "INSERT INTO upload_zip_jobs(user_id, filename, source_kind, staging_dir, bookmark_enabled, bookmark_list_id, total, status) VALUES (?,?,?,?,?,?,?,?)",
            (int(user["id"]), "direct upload", "direct", "", 1 if upload_bookmark_list_id else 0, upload_bookmark_list_id, int(requested_total), "collecting"),
        )
        job_id = int(cur.lastrowid)
        staging_dir = _job_staging_dir(job_id)
        staging_dir.mkdir(parents=True, exist_ok=True)
        conn.execute(
            "UPDATE upload_zip_jobs SET staging_dir=?, updated_at_utc=datetime('now') WHERE id=?",
            (str(staging_dir), int(job_id)),
        )
        conn.commit()
        return {"ok": True, "job_id": job_id, "total": int(requested_total)}
    finally:
        conn.close()


@api_router.post("/upload_batch/{job_id}/append")
async def upload_batch_append(
    job_id: int,
    request: Request,
    user: dict = Depends(get_user),
):
    try:
        seq_i = int(request.query_params.get("seq") or 0)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid seq")
    if seq_i <= 0:
        raise HTTPException(status_code=400, detail="invalid seq")

    last_modified_ms = request.query_params.get("last_modified_ms")
    filename = request.query_params.get("filename") or f"upload_{seq_i}"

    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT user_id, status, source_kind, staging_dir FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="not found")
    if int(row["user_id"] or 0) != int(user["id"]):
        raise HTTPException(status_code=403, detail="forbidden")
    if str(row["source_kind"] or "zip") != "direct":
        raise HTTPException(status_code=400, detail="not a direct upload job")
    if str(row["status"] or "") != "collecting":
        raise HTTPException(status_code=409, detail="upload closed")

    safe_name = _safe_basename(filename)
    if not _allowed_image_ext(safe_name):
        raise HTTPException(status_code=400, detail="unsupported file type")

    staging_dir = Path(str(row["staging_dir"] or "") or str(_job_staging_dir(job_id)))
    staging_dir.mkdir(parents=True, exist_ok=True)
    dst = staging_dir / f"{seq_i:06d}_{safe_name}"
    total = 0
    with open(dst, "wb") as out:
        async for chunk in request.stream():
            if not chunk:
                continue
            total += len(chunk)
            out.write(chunk)
    if total <= 0:
        try:
            dst.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail="empty file")

    try:
        parsed = _parse_last_modified_ms(last_modified_ms)
        if parsed:
            ts = datetime.fromisoformat(parsed.replace('Z', '+00:00')).timestamp()
            os.utime(dst, (ts, ts))
    except Exception:
        pass

    _spawn_direct_upload_item_registration(
        int(job_id),
        int(seq_i),
        str(safe_name),
        str(dst),
        trace_id=getattr(request.state, "trace_id", None),
    )
    return {"ok": True, "seq": int(seq_i)}


@api_router.post("/upload_batch/{job_id}/finish")
def upload_batch_finish(job_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT user_id, status, source_kind, staging_dir FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if int(row["user_id"] or 0) != int(user["id"]):
            raise HTTPException(status_code=403, detail="forbidden")
        if str(row["source_kind"] or "zip") != "direct":
            raise HTTPException(status_code=400, detail="not a direct upload job")
        status = str(row["status"] or "")
        if status in {"done", "error", "cancelled"}:
            count = int(conn.execute("SELECT COUNT(*) AS n FROM upload_zip_items WHERE job_id=?", (int(job_id),)).fetchone()[0])
            return {"ok": True, "job_id": int(job_id), "total": count, "status": status}

        staging_dir = Path(str(row["staging_dir"] or "") or str(_job_staging_dir(job_id)))
        count, item_ids = _sync_direct_upload_items(conn, int(job_id), staging_dir)
        if count <= 0:
            raise HTTPException(status_code=400, detail="no uploaded files")
        status, _staging_dir, _source_kind = _refresh_upload_job_progress(conn, int(job_id), seal=True)
        conn.execute(
            "UPDATE upload_zip_jobs SET error=NULL, updated_at_utc=datetime('now') WHERE id=?",
            (int(job_id),),
        )
        conn.commit()
    finally:
        conn.close()

    for item_id in item_ids:
        try:
            _queue_upload_item_request(int(item_id), source="direct_finish")
        except Exception:
            continue
    return {"ok": True, "job_id": int(job_id), "total": int(count), "status": status or "queued"}


def _image_detail_summary(conn: sqlite3.Connection, iid: int, *, user_id: int | None = None, has_seq: bool | None = None) -> dict | None:
    """Small summary for upload responses; keep this cheap."""
    img = conn.execute(
        "SELECT id, software FROM images WHERE id=?",
        (int(iid),),
    ).fetchone()
    if not img:
        return None
    return {
        "id": int(img["id"]),
        "software": img["software"],
    }


def _image_detail_summary_map(conn: sqlite3.Connection, ids: list[int] | tuple[int, ...]) -> dict[int, dict]:
    clean_ids: list[int] = []
    seen: set[int] = set()
    for raw_iid in ids:
        try:
            iid = int(raw_iid)
        except Exception:
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        clean_ids.append(iid)
    if not clean_ids:
        return {}
    placeholders = ",".join("?" for _ in clean_ids)
    rows = conn.execute(
        f"SELECT id, software FROM images WHERE id IN ({placeholders})",
        clean_ids,
    ).fetchall()
    return {
        int(row["id"]): {
            "id": int(row["id"]),
            "software": row["software"],
        }
        for row in rows
    }



@api_router.get("/upload_zip/{job_id}")
def upload_zip_status(
    job_id: int,
    after_seq: int = 0,
    limit: int = 300,
    user: dict = Depends(get_user),
):
    limit_i = int(limit or 0)
    if limit_i <= 0:
        limit_i = 300
    if limit_i > 5000:
        limit_i = 5000

    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, user_id, filename, source_kind, total, done, failed, dup, status, error FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if int(row["user_id"] or 0) != int(user["id"]):
            raise HTTPException(status_code=403, detail="forbidden")

        latest_row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) AS latest_seq FROM upload_zip_items WHERE job_id=?",
            (int(job_id),),
        ).fetchone()
        latest_seq = int(latest_row["latest_seq"] or 0) if latest_row else 0

        source_kind = str(row["source_kind"] or "zip")
        if source_kind == "direct":
            items = conn.execute(
                "SELECT seq, filename, state, image_id, message FROM upload_zip_items WHERE job_id=? ORDER BY seq ASC LIMIT ?",
                (int(job_id), int(limit_i)),
            ).fetchall()
        else:
            items = conn.execute(
                "SELECT seq, filename, state, image_id, message FROM upload_zip_items WHERE job_id=? AND seq>? ORDER BY seq ASC LIMIT ?",
                (int(job_id), int(after_seq or 0), int(limit_i)),
            ).fetchall()

        detail_map = _image_detail_summary_map(
            conn,
            [int(r["image_id"]) for r in items if r["image_id"]],
        )

        return {
            "job_id": int(row["id"]),
            "filename": row["filename"],
            "source_kind": source_kind,
            "total": int(row["total"] or 0),
            "done": int(row["done"] or 0),
            "failed": int(row["failed"] or 0),
            "dup": int(row["dup"] or 0),
            "status": row["status"],
            "error": row["error"],
            "latest_seq": int(latest_seq),
            "items": [
                {
                    "seq": int(r["seq"] or 0),
                    "filename": r["filename"],
                    "state": r["state"],
                    "image_id": r["image_id"],
                    "message": r["message"],
                    "thumb": None,
                    "detail": (detail_map.get(int(r["image_id"])) if r["image_id"] else None),
                }
                for r in items
            ],
        }
    finally:
        conn.close()


@api_router.post("/upload_zip/{job_id}/cancel")
def upload_zip_cancel(job_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    staging_dir = None
    try:
        row = conn.execute(
            "SELECT user_id, status, source_kind, staging_dir FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        if int(row["user_id"] or 0) != int(user["id"]):
            raise HTTPException(status_code=403, detail="forbidden")
        if row["status"] in {"done", "error"}:
            return {"ok": True, "status": row["status"]}
        staging_dir = str(row["staging_dir"] or "")
        conn.execute(
            "UPDATE upload_zip_jobs SET status='cancelled', updated_at_utc=datetime('now') WHERE id=?",
            (int(job_id),),
        )
        conn.commit()
    finally:
        conn.close()

    if staging_dir and str(row["source_kind"] or "zip") == "direct":
        try:
            shutil.rmtree(staging_dir, ignore_errors=True)
        except Exception:
            pass
    return {"ok": True, "status": "cancelled"}


def _process_upload_job(job_id: int, user_id: int, username: str) -> None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT status FROM upload_zip_jobs WHERE id=?",
            (int(job_id),),
        ).fetchone()
        if not row:
            return
        if str(row["status"] or "") == "cancelled":
            return

        rows = conn.execute(
            "SELECT id FROM upload_zip_items WHERE job_id=? ORDER BY seq ASC",
            (int(job_id),),
        ).fetchall()
        conn.execute(
            "UPDATE upload_zip_jobs SET total=?, error=NULL, updated_at_utc=datetime('now') WHERE id=?",
            (int(len(rows)), int(job_id)),
        )
        status, _staging_dir, _source_kind = _refresh_upload_job_progress(conn, int(job_id), seal=True)
        conn.commit()
    finally:
        conn.close()

    for r in rows:
        _queue_upload_item_request(int(r["id"]), source="upload_zip")

def _upload_zip_worker(job_id: int, zip_path: str, user_id: int, username: str) -> None:
    import zipfile

    conn = get_conn()
    staging_dir = str(_job_staging_dir(job_id))
    try:
        shutil.rmtree(staging_dir, ignore_errors=True)
        Path(staging_dir).mkdir(parents=True, exist_ok=True)
        conn.execute(
            "UPDATE upload_zip_jobs SET status='scanning', staging_dir=?, total=0, done=0, failed=0, dup=0, error=NULL, updated_at_utc=datetime('now') WHERE id=?",
            (staging_dir, int(job_id)),
        )
        conn.execute("DELETE FROM upload_zip_items WHERE job_id=?", (int(job_id),))
        conn.commit()

        seq = 0
        cancelled = False
        with zipfile.ZipFile(zip_path, "r") as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                base = os.path.basename(info.filename or "")
                if not base or not _allowed_image_ext(base):
                    continue
                if (seq % 50) == 0:
                    strow = conn.execute("SELECT status FROM upload_zip_jobs WHERE id=?", (int(job_id),)).fetchone()
                    if strow and str(strow["status"] or "") == "cancelled":
                        cancelled = True
                        break

                seq += 1
                safe_base = _safe_basename(base)
                dst = Path(staging_dir) / f"{seq:06d}_{safe_base}"
                with z.open(info, "r") as src, open(dst, "wb") as out:
                    shutil.copyfileobj(src, out, length=1024 * 1024)
                try:
                    mtime_iso = _zipinfo_mtime_iso(info)
                except Exception:
                    mtime_iso = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    "INSERT INTO upload_zip_items(job_id, seq, filename, state, image_id, message, staged_path, mtime_iso) VALUES (?,?,?,?,?,?,?,?)",
                    (int(job_id), int(seq), safe_base, "待機", None, None, str(dst), mtime_iso),
                )
                if (seq % 20) == 0:
                    conn.execute(
                        "UPDATE upload_zip_jobs SET total=?, updated_at_utc=datetime('now') WHERE id=?",
                        (int(seq), int(job_id)),
                    )
                    conn.commit()

        if cancelled:
            conn.execute(
                "UPDATE upload_zip_jobs SET status='cancelled', updated_at_utc=datetime('now') WHERE id=?",
                (int(job_id),),
            )
            conn.commit()
            return

        conn.execute(
            "UPDATE upload_zip_jobs SET total=?, status='queued', updated_at_utc=datetime('now') WHERE id=?",
            (int(seq), int(job_id)),
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
        return
    finally:
        conn.close()
        try:
            os.remove(zip_path)
        except Exception:
            pass

    _process_upload_job(int(job_id), int(user_id), str(username))

DERIV_VERSION = "19"


def _process_derivative_job(image_id: int, kinds: tuple[str, ...], source: str | None = None, trace_id: str | None = None) -> None:
    _ensure_derivatives(image_id, kinds, trigger=(source or "queue"), trace_id=trace_id)


def _ensure_derivatives(
    image_id: int,
    kinds: tuple[str, ...] = ("grid", "overlay"),
    *,
    trigger: str = "direct",
    trace_id: str | None = None,
) -> None:
    started = time.perf_counter()
    conn = get_conn()
    per_kind: dict[str, dict] = {}
    try:
        src = _read_image_bytes(conn, image_id)
        if not src:
            log_perf("ensure_derivatives", image_id=int(image_id), kinds=list(kinds), trigger=str(trigger or "direct"), trace_id=(trace_id or None), skipped="no_source", total_ms=round((time.perf_counter() - started) * 1000.0, 3))
            return

        targets = DERIVATIVE_TARGETS

        def needs_refresh(kind: str, fmt: str) -> bool:
            if not _derivative_format_enabled(kind, fmt):
                return False
            r = conn.execute(
                "SELECT image_id, width, height, quality, disk_path, size, bytes FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
                (image_id, kind, fmt),
            ).fetchone()
            if not r:
                return True
            return not _derivative_row_is_ready(conn, r, kind=kind, fmt=fmt)

        do_avif = avif_available()
        clean_kinds = _normalize_derivative_kinds(kinds)
        need_any = False
        refresh_map: dict[str, dict[str, bool]] = {}
        for kind in clean_kinds:
            refresh_map[kind] = {
                "avif": bool(do_avif and needs_refresh(kind, "avif")),
                "webp": bool(needs_refresh(kind, "webp")),
            }
            if refresh_map[kind]["avif"] or refresh_map[kind]["webp"]:
                need_any = True
        if not need_any:
            log_perf(
                "ensure_derivatives",
                image_id=int(image_id),
                kinds=list(clean_kinds),
                trigger=str(trigger or "direct"),
                trace_id=(trace_id or None),
                avif_enabled=bool(do_avif),
                per_kind={kind: {"avif_refresh": refresh_map[kind]["avif"], "webp_refresh": refresh_map[kind]["webp"], "duration_ms": 0.0} for kind in clean_kinds},
                commit_ms=0.0,
                total_ms=round((time.perf_counter() - started) * 1000.0, 3),
            )
            return

        base_image = decode_source_image(src)
        try:
            for kind in clean_kinds:
                kind_started = time.perf_counter()
                target = targets[kind]
                kind_meta = {
                    "max_side": int(target.max_side),
                    "webp_quality": int(target.webp.quality),
                    "webp_method": int(target.webp.method),
                    "avif_quality": int(target.avif.quality),
                    "avif_speed": int(target.avif.speed),
                    "avif_codec": str(target.avif.codec),
                    "avif_refresh": refresh_map[kind]["avif"],
                    "webp_refresh": refresh_map[kind]["webp"],
                    "avif_written": False,
                    "webp_written": False,
                    "avif_error": None,
                    "webp_error": None,
                }
                if not (refresh_map[kind]["avif"] or refresh_map[kind]["webp"]):
                    kind_meta["duration_ms"] = round((time.perf_counter() - kind_started) * 1000.0, 3)
                    per_kind[kind] = kind_meta
                    continue
                variant = make_resized_variant(base_image, max_side=int(target.max_side))
                derivative_created_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                try:
                    width, height = int(variant.size[0]), int(variant.size[1])
                    if refresh_map[kind]["avif"]:
                        try:
                            b = encode_avif_image(
                                variant,
                                quality=int(target.avif.quality),
                                speed=int(target.avif.speed),
                                codec=str(target.avif.codec),
                                max_threads=int(target.avif.max_threads),
                            )
                            _upsert_derivative_file(
                                conn,
                                image_id=int(image_id),
                                kind=kind,
                                fmt="avif",
                                width=width,
                                height=height,
                                quality=int(target.avif.quality),
                                raw=b,
                                created_at_utc=derivative_created_at_utc,
                            )
                            kind_meta["avif_written"] = True
                        except Exception as exc:
                            kind_meta["avif_error"] = exc.__class__.__name__
                    if refresh_map[kind]["webp"]:
                        try:
                            b = encode_webp_image(
                                variant,
                                quality=int(target.webp.quality),
                                method=int(target.webp.method),
                                lossless=bool(target.webp.lossless),
                                alpha_quality=int(target.webp.alpha_quality),
                            )
                            _upsert_derivative_file(
                                conn,
                                image_id=int(image_id),
                                kind=kind,
                                fmt="webp",
                                width=width,
                                height=height,
                                quality=int(target.webp.quality),
                                raw=b,
                                created_at_utc=derivative_created_at_utc,
                            )
                            kind_meta["webp_written"] = True
                        except Exception as exc:
                            kind_meta["webp_error"] = exc.__class__.__name__
                finally:
                    try:
                        variant.close()
                    except Exception:
                        pass
                kind_meta["duration_ms"] = round((time.perf_counter() - kind_started) * 1000.0, 3)
                per_kind[kind] = kind_meta
        finally:
            try:
                base_image.close()
            except Exception:
                pass

        commit_started = time.perf_counter()
        conn.commit()
        commit_ms = round((time.perf_counter() - commit_started) * 1000.0, 3)
        log_perf(
            "ensure_derivatives",
            image_id=int(image_id),
            kinds=list(clean_kinds),
            trigger=str(trigger or "direct"),
            trace_id=(trace_id or None),
            avif_enabled=bool(do_avif),
            per_kind=per_kind,
            commit_ms=commit_ms,
            total_ms=round((time.perf_counter() - started) * 1000.0, 3),
        )
    finally:
        conn.close()

@api_router.get("/images")
def list_images(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,  # comma-separated canonical tags
    tags_not: str | None = None,  # comma-separated canonical tags to exclude
    date_from: str | None = None,  # YYYY-MM-DD
    date_to: str | None = None,    # YYYY-MM-DD
    dedup_only: int = 0,
    limit: int = 60,
    cursor: int | None = None,
    user: dict = Depends(get_user),
):
    """Legacy cursor list (kept for older clients)."""
    limit = max(1, min(200, int(limit)))
    conn = get_conn()
    try:
        uid = int(user["id"])

        creator_id: int | None = None
        if creator:
            r = _find_user_by_username(conn, creator, "id")
            if not r:
                return {"items": [], "next_cursor": None}
            creator_id = int(r["id"])

        q = """
        SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
               COALESCE(ubm.bm, 0) AS favorite, images.is_nsfw,
               users.username AS creator
        FROM images
        JOIN users ON users.id = images.uploader_user_id
        LEFT JOIN (
          SELECT DISTINCT b.image_id AS image_id, 1 AS bm
          FROM bookmarks b
          JOIN bookmark_lists bl ON bl.id=b.list_id
          WHERE bl.user_id=?
        ) ubm ON ubm.image_id = images.id
        """
        where: list[str] = []
        params: list = [uid]

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

        tag_list: list[str] = []
        tag_not_list: list[str] = []
        if tags:
            tag_list = [t.strip() for t in tags.split(",") if t.strip()]
            tag_list = list(dict.fromkeys(tag_list))
        if tags_not:
            tag_not_list = [t.strip() for t in tags_not.split(",") if t.strip()]
            tag_not_list = list(dict.fromkeys(tag_not_list))

        _apply_tag_filters(conn, where, params, tag_list, tag_not_list, image_alias="images")

        _append_visibility_filter(where, params, viewer=user, bm_list_id=None)

        if where:
            q += " WHERE " + " AND ".join(where)

        if cursor is not None:
            q += (" AND " if where else " WHERE ") + " images.id < ? "
            params.append(int(cursor))

        q += " ORDER BY images.file_mtime_utc DESC, images.id DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(q, params).fetchall()
        thumb_urls = _thumb_url_map(conn, [int(r["id"]) for r in rows], kind="grid")
        out = []
        for r in rows:
            out.append(
                {
                    "id": r["id"],
                    "w": r["width"],
                    "h": r["height"],
                    "mtime": r["file_mtime_utc"],
                    "software": r["software"],
                    "creator": r["creator"],
                    "dedup_flag": r["dedup_flag"],
                    "favorite": int(r["favorite"] or 0),
                    "is_nsfw": int(r["is_nsfw"] or 0),
                    "thumb": thumb_urls.get(int(r["id"]), _thumb_url(conn, int(r["id"]), kind="grid")),
                }
            )
        next_cursor = out[-1]["id"] if out else None
        return {"items": out, "next_cursor": next_cursor}
    finally:
        conn.close()

@api_router.get("/images_page")
def list_images_page(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,
    tags_not: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    dedup_only: int = 0,
    bm_any: int = 0,
    bm_list_id: int | None = None,
    # backward-compat
    fav_only: int = 0,
    sort: str = "newest",
    page: int = 1,
    limit: int = 16,
    include_total: int = 1,
    user: dict = Depends(get_user),
):
    """Offset-based paging for gallery UI."""
    limit = max(1, min(72, int(limit)))
    page = max(1, int(page))
    offset = (page - 1) * limit

    filters = normalize_gallery_filters(
        creator=creator,
        software=software,
        tags=tags,
        tags_not=tags_not,
        date_from=date_from,
        date_to=date_to,
        dedup_only=dedup_only,
        bm_any=bm_any,
        bm_list_id=bm_list_id,
        fav_only=fav_only,
        sort=sort,
        normalize_tag=normalize_tag,
    )

    conn = get_conn()
    try:
        uid = int(user["id"])
        filters.bm_list_id = normalize_bookmark_list_id(
            conn,
            viewer=user,
            bm_list_id=filters.bm_list_id,
            can_view_bookmark_list=lambda c, viewer, lid: _can_view_bookmark_list(c, viewer=viewer, list_id=lid),
        )

        creator_ok, creator_id = resolve_creator_id(conn, filters.creator)
        if not creator_ok:
            return {
                "items": [],
                "page": page,
                "limit": limit,
                "total_count": 0,
                "total_pages": 0,
                "sort": filters.sort_key,
                "bm_any": int(filters.bm_any),
                "bm_list_id": (int(filters.bm_list_id) if filters.bm_list_id is not None else None),
            }

        join_sql, join_params = build_user_bookmark_join(uid, filters.bm_list_id)
        where: list[str] = []
        params: list = []
        apply_common_filters(
            conn,
            filters=filters,
            creator_id=creator_id,
            where=where,
            params=params,
            viewer=user,
            apply_tag_filters=lambda c, w, p, tag_list, tag_not_list, image_alias: _apply_tag_filters(c, w, p, tag_list, tag_not_list, image_alias=image_alias),
            append_visibility_filter=lambda w, p, viewer: _append_visibility_filter(w, p, viewer=viewer, bm_list_id=filters.bm_list_id),
        )

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        fav_expr = "COALESCE(ubm.bm,0)"
        if filters.sort_key == "oldest":
            order_sql = " ORDER BY images.file_mtime_utc ASC, images.id ASC "
        elif filters.sort_key == "favorite":
            order_sql = f" ORDER BY {fav_expr} DESC, images.file_mtime_utc DESC, images.id DESC "
        else:
            order_sql = " ORDER BY images.file_mtime_utc DESC, images.id DESC "

        from_sql = (
            " FROM images "
            " JOIN users ON users.id = images.uploader_user_id "
            + join_sql
        )

        total = None
        total_pages = None
        if int(include_total):
            cnt_row = conn.execute(
                "SELECT COUNT(*) AS n " + from_sql + where_sql,
                join_params + params,
            ).fetchone()
            total = int(cnt_row["n"] if cnt_row else 0)
            total_pages = int(math.ceil(total / float(limit))) if total else 0

        rows = conn.execute(
            """
            SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
                   COALESCE(ubm.bm,0) AS favorite, images.is_nsfw,
                   users.username AS creator, images.original_filename AS filename
            """
            + from_sql
            + where_sql
            + order_sql
            + " LIMIT ? OFFSET ?",
            join_params + params + [limit, offset],
        ).fetchall()

        thumb_urls = _thumb_url_map(conn, [int(r["id"]) for r in rows], kind="grid")
        out = []
        for r in rows:
            out.append(
                {
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
                    "thumb": _append_bm_list_id_to_url(thumb_urls.get(int(r["id"]), _thumb_url(conn, int(r["id"]), kind="grid")), filters.bm_list_id),
                }
            )

        return {
            "items": out,
            "page": page,
            "limit": limit,
            "total_count": total,
            "total_pages": total_pages,
            "sort": filters.sort_key,
            "bm_any": int(filters.bm_any),
            "bm_list_id": (int(filters.bm_list_id) if filters.bm_list_id is not None else None),
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


def _normalize_bm_list_id_for_visibility(conn: sqlite3.Connection, *, viewer: dict, bm_list_id: int | None) -> int | None:
    if bm_list_id is None:
        return None
    try:
        lid = int(bm_list_id)
    except Exception:
        return None
    if lid <= 0:
        return None
    if not _can_view_bookmark_list(conn, viewer=viewer, list_id=lid):
        return None
    return lid


def _append_visibility_filter(where: list[str], params: list, *, viewer: dict, bm_list_id: int | None = None) -> None:
    """Apply per-user visibility rule for images."""
    uid = int(viewer.get("id") or 0)
    if bm_list_id is not None:
        where.append(
            "EXISTS(SELECT 1 FROM bookmarks b WHERE b.list_id = ? AND b.image_id = images.id)"
        )
        params.append(int(bm_list_id))
        where.append(
            "(images.uploader_user_id = ? "
            "OR EXISTS("
            "SELECT 1 FROM users u2 "
            "LEFT JOIN user_settings us2 ON us2.user_id=u2.id "
            "WHERE u2.id=images.uploader_user_id "
            "AND u2.disabled=0 "
            "AND COALESCE(us2.share_works,0)=1"
            "))"
        )
        params.append(uid)
        return

    where.append(
        "(images.uploader_user_id = ? "
        "OR EXISTS("
        "SELECT 1 FROM user_creators uc "
        "JOIN users u2 ON u2.id=uc.creator_user_id "
        "LEFT JOIN user_settings us2 ON us2.user_id=uc.creator_user_id "
        "WHERE uc.user_id = ? "
        "AND uc.creator_user_id = images.uploader_user_id "
        "AND u2.disabled=0 "
        "AND COALESCE(us2.share_works,0)=1"
        "))"
    )
    params.extend([uid, uid])


def _assert_image_visible(conn: sqlite3.Connection, *, image_id: int, viewer: dict, bm_list_id: int | None = None) -> None:
    """Raise 404 if viewer cannot see the image in the current context."""
    uid = int(viewer.get("id") or 0)
    iid = int(image_id)
    row = conn.execute("SELECT uploader_user_id FROM images WHERE id=?", (iid,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="not found")
    creator_id = int(row["uploader_user_id"])

    lid = _normalize_bm_list_id_for_visibility(conn, viewer=viewer, bm_list_id=bm_list_id)
    if lid is not None:
        listed = conn.execute(
            "SELECT 1 FROM bookmarks WHERE list_id=? AND image_id=?",
            (lid, iid),
        ).fetchone()
        if not listed:
            raise HTTPException(status_code=404, detail="not found")
        if creator_id == uid:
            return
        shared = conn.execute(
            """
            SELECT 1
            FROM users u
            LEFT JOIN user_settings us ON us.user_id=u.id
            WHERE u.id=?
              AND u.disabled=0
              AND COALESCE(us.share_works,0)=1
            """,
            (creator_id,),
        ).fetchone()
        if not shared:
            raise HTTPException(status_code=404, detail="not found")
        return

    if creator_id == uid:
        return
    ok = conn.execute(
        """
        SELECT 1
        FROM user_creators uc
        JOIN users u ON u.id=uc.creator_user_id
        LEFT JOIN user_settings us ON us.user_id=uc.creator_user_id
        WHERE uc.user_id=?
          AND uc.creator_user_id=?
          AND u.disabled=0
          AND COALESCE(us.share_works,0)=1
        """,
        (uid, creator_id),
    ).fetchone()
    if not ok:
        raise HTTPException(status_code=404, detail="not found")


def _can_view_bookmark_list(conn: sqlite3.Connection, *, viewer: dict, list_id: int) -> bool:
    """Whether viewer can filter by the given bookmark list id."""
    uid = int(viewer.get("id") or 0)
    lid = int(list_id)

    row = conn.execute(
        "SELECT user_id FROM bookmark_lists WHERE id=?",
        (lid,),
    ).fetchone()
    if not row:
        return False
    owner_id = int(row["user_id"])
    if owner_id == uid:
        return True

    ok = conn.execute(
        """
        SELECT 1
        FROM users u
        LEFT JOIN user_settings us ON us.user_id=u.id
        JOIN user_bookmark_creators sub ON sub.user_id=? AND sub.creator_user_id=u.id
        WHERE u.id=?
          AND u.disabled=0
          AND COALESCE(us.share_bookmarks,0)=1
        LIMIT 1
        """,
        (uid, owner_id),
    ).fetchone()
    return bool(ok)


@api_router.get("/images_scroll")
def list_images_scroll(
    creator: str | None = None,
    software: str | None = None,
    tags: str | None = None,
    tags_not: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    dedup_only: int = 0,
    bm_any: int = 0,
    bm_list_id: int | None = None,
    # backward-compat
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

    filters = normalize_gallery_filters(
        creator=creator,
        software=software,
        tags=tags,
        tags_not=tags_not,
        date_from=date_from,
        date_to=date_to,
        dedup_only=dedup_only,
        bm_any=bm_any,
        bm_list_id=bm_list_id,
        fav_only=fav_only,
        sort=sort,
        normalize_tag=normalize_tag,
    )

    conn = get_conn()
    try:
        uid = int(user["id"])
        filters.bm_list_id = normalize_bookmark_list_id(
            conn,
            viewer=user,
            bm_list_id=filters.bm_list_id,
            can_view_bookmark_list=lambda c, viewer, lid: _can_view_bookmark_list(c, viewer=viewer, list_id=lid),
        )

        creator_ok, creator_id = resolve_creator_id(conn, filters.creator)
        if not creator_ok:
            return {
                "items": [],
                "next_cursor": None,
                "sort": filters.sort_key,
                "bm_any": int(filters.bm_any),
                "bm_list_id": (int(filters.bm_list_id) if filters.bm_list_id is not None else None),
                "total_count": 0,
            }

        join_sql, join_params = build_user_bookmark_join(uid, filters.bm_list_id)
        where: list[str] = []
        params: list = []
        apply_common_filters(
            conn,
            filters=filters,
            creator_id=creator_id,
            where=where,
            params=params,
            viewer=user,
            apply_tag_filters=lambda c, w, p, tag_list, tag_not_list, image_alias: _apply_tag_filters(c, w, p, tag_list, tag_not_list, image_alias=image_alias),
            append_visibility_filter=lambda w, p, viewer: _append_visibility_filter(w, p, viewer=viewer, bm_list_id=filters.bm_list_id),
        )

        where_base = list(where)
        params_base = list(params)

        fav_expr = "COALESCE(ubm.bm,0)"
        cur_tuple = _parse_scroll_cursor(filters.sort_key, cursor)
        if cur_tuple is not None:
            if filters.sort_key == "oldest":
                where.append("(images.file_mtime_utc > ? OR (images.file_mtime_utc = ? AND images.id > ?))")
                params.extend([cur_tuple[0], cur_tuple[0], cur_tuple[1]])
            elif filters.sort_key == "favorite":
                fav, mtime, iid = cur_tuple
                where.append(
                    f"({fav_expr} < ? OR "
                    f"({fav_expr} = ? AND images.file_mtime_utc < ?) OR "
                    f"({fav_expr} = ? AND images.file_mtime_utc = ? AND images.id < ?))"
                )
                params.extend([fav, fav, mtime, fav, mtime, iid])
            else:
                where.append("(images.file_mtime_utc < ? OR (images.file_mtime_utc = ? AND images.id < ?))")
                params.extend([cur_tuple[0], cur_tuple[0], cur_tuple[1]])

        where_sql = (" WHERE " + " AND ".join(where)) if where else ""
        where_sql_base = (" WHERE " + " AND ".join(where_base)) if where_base else ""

        if filters.sort_key == "oldest":
            order_sql = " ORDER BY images.file_mtime_utc ASC, images.id ASC "
        elif filters.sort_key == "favorite":
            order_sql = f" ORDER BY {fav_expr} DESC, images.file_mtime_utc DESC, images.id DESC "
        else:
            order_sql = " ORDER BY images.file_mtime_utc DESC, images.id DESC "

        from_sql = (
            " FROM images "
            " JOIN users ON users.id = images.uploader_user_id "
            + join_sql
        )

        total_count = None
        if int(include_total):
            total_count = conn.execute(
                "SELECT COUNT(1) AS c " + from_sql + where_sql_base,
                join_params + params_base,
            ).fetchone()[0]

        rows = conn.execute(
            """
            SELECT images.id, images.width, images.height, images.file_mtime_utc, images.software, images.dedup_flag,
                   COALESCE(ubm.bm,0) AS favorite, images.is_nsfw,
                   users.username AS creator, images.original_filename AS filename
            """
            + from_sql
            + where_sql
            + order_sql
            + " LIMIT ?",
            join_params + params + [limit],
        ).fetchall()

        thumb_urls = _thumb_url_map(conn, [int(r["id"]) for r in rows], kind="grid")
        out = []
        for r in rows:
            out.append(
                {
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
                    "thumb": _append_bm_list_id_to_url(thumb_urls.get(int(r["id"]), _thumb_url(conn, int(r["id"]), kind="grid")), filters.bm_list_id),
                }
            )

        next_cursor = _make_scroll_cursor(filters.sort_key, rows[-1]) if rows else None
        return {
            "items": out,
            "next_cursor": next_cursor,
            "sort": filters.sort_key,
            "bm_any": int(filters.bm_any),
            "bm_list_id": (int(filters.bm_list_id) if filters.bm_list_id is not None else None),
            "total_count": (int(total_count) if total_count is not None else None),
        }
    finally:
        conn.close()

@api_router.get("/images/{image_id}/thumb")
def get_thumb(image_id: int, request: Request, kind: str = "grid", bm_list_id: int | None = None, user: dict = Depends(get_user)):
    if kind not in {"grid", "overlay"}:
        raise HTTPException(status_code=400, detail="kind must be grid/overlay")

    accept = (request.headers.get("accept") or "").lower()
    fmts = ["webp"]
    if "image/avif" in accept and _derivative_format_enabled(kind, "avif"):
        fmts = ["avif", "webp"]

    conn = get_conn()
    try:
        _assert_image_visible(conn, image_id=int(image_id), viewer=user, bm_list_id=bm_list_id)
        chosen = None
        chosen_fmt = None

        for fmt in fmts:
            row = conn.execute(
                "SELECT image_id, format, width, height, quality, disk_path, size, bytes, created_at_utc FROM image_derivatives WHERE image_id=? AND kind=? AND format=?",
                (image_id, kind, fmt),
            ).fetchone()
            if _derivative_row_is_ready(conn, row, kind=kind, fmt=fmt):
                chosen = row
                chosen_fmt = fmt
                break

        if not chosen or not chosen_fmt:
            raise HTTPException(status_code=404, detail="thumb not ready")

        disk_path = chosen["disk_path"]
        if not disk_path or not os.path.exists(disk_path):
            disk_path = _ensure_derivative_on_disk(conn, int(image_id), kind, chosen_fmt)
            conn.commit()
        if not disk_path or not os.path.exists(disk_path):
            raise HTTPException(status_code=404, detail="thumb not ready")

        etag = f'W/"deriv-{image_id}-{kind}-{chosen_fmt}-{chosen["quality"]}-{chosen["width"]}x{chosen["height"]}-{chosen["created_at_utc"]}"'
        headers = {
            "Cache-Control": "private, max-age=604800",
            "ETag": etag,
            "Vary": "Accept",
        }
        inm = request.headers.get("if-none-match")
        if inm and inm == etag:
            return Response(status_code=304, headers=headers)

        mt = "image/webp" if chosen_fmt == "webp" else "image/avif"
        return FileResponse(path=str(disk_path), media_type=mt, headers=headers)
    finally:
        conn.close()


class PrefetchDerivativesReq(BaseModel):
    ids: list[int] = []
    kind: str = "overlay"  # overlay / grid / both


class ClientPerfLogReq(BaseModel):
    event: str = Field(default="client_perf", max_length=64)
    trace_id: str | None = Field(default=None, max_length=128)
    image_id: int | None = None
    page: int | None = None
    mode: str | None = Field(default=None, max_length=32)
    source: str | None = Field(default=None, max_length=32)
    fetch_ms: float | None = None
    parse_ms: float | None = None
    total_ms: float | None = None
    note: str | None = Field(default=None, max_length=256)


class ImageDetailsBatchReq(BaseModel):
    ids: list[int] = []
    bm_list_id: int | None = None


@api_router.post("/debug/perf")
def debug_perf_log(req: ClientPerfLogReq, request: Request, user: dict = Depends(get_user)):
    log_perf(
        "client_perf",
        trace_id=(req.trace_id or getattr(request.state, "trace_id", None)),
        user_id=int(user["id"]) if user and user.get("id") is not None else None,
        image_id=(int(req.image_id) if req.image_id else None),
        page=(int(req.page) if req.page else None),
        mode=(req.mode or None),
        source=(req.source or None),
        fetch_ms=(round(float(req.fetch_ms), 3) if req.fetch_ms is not None else None),
        parse_ms=(round(float(req.parse_ms), 3) if req.parse_ms is not None else None),
        total_ms=(round(float(req.total_ms), 3) if req.total_ms is not None else None),
        note=(req.note or None),
        ua=request.headers.get("user-agent") or None,
    )
    return {"ok": True}


@api_router.post("/cache/prefetch_derivatives")
def prefetch_derivatives(req: PrefetchDerivativesReq, bg: BackgroundTasks, request: Request, user: dict = Depends(get_user)):
    started = time.perf_counter()
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

    ids = list(dict.fromkeys(ids))[:48]
    kinds = ("grid", "overlay") if kind == "both" else (kind,)
    trace_id = getattr(request.state, "trace_id", None) if request is not None else None

    queued_ids: list[int] = []
    skipped_ids: list[int] = []
    for iid in ids:
        try:
            ok = _queue_derivative_request(
                int(iid),
                kinds,
                source="prefetch",
                trace_id=trace_id,
            )
        except sqlite3.OperationalError as exc:
            skipped_ids.append(int(iid))
            log_perf(
                "prefetch_derivatives_enqueue_skipped",
                trace_id=trace_id,
                user_id=int(user["id"]) if user and user.get("id") is not None else None,
                kind=kind,
                image_id=int(iid),
                error_type=exc.__class__.__name__,
                error_message=str(exc),
            )
            continue
        if ok:
            queued_ids.append(int(iid))
        else:
            skipped_ids.append(int(iid))

    log_perf(
        "prefetch_derivatives_enqueue",
        trace_id=trace_id,
        user_id=int(user["id"]) if user and user.get("id") is not None else None,
        kind=kind,
        ids=queued_ids,
        count=len(queued_ids),
        skipped_ids=skipped_ids,
        skipped_count=len(skipped_ids),
        duration_ms=round((time.perf_counter() - started) * 1000.0, 3),
    )
    return {"ok": True, "n": len(queued_ids), "skipped": len(skipped_ids), "kind": kind}

@api_router.get("/images/{image_id}/file")

def download_original(image_id: int, request: Request, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        _assert_image_visible(conn, image_id=int(image_id), viewer=user, bm_list_id=bm_list_id)
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
def view_original(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    """Inline view for full size image (useful for browser zoom)."""
    conn = get_conn()
    try:
        _assert_image_visible(conn, image_id=int(image_id), viewer=user, bm_list_id=bm_list_id)
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
def download_metadata(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        _assert_image_visible(conn, image_id=int(image_id), viewer=user, bm_list_id=bm_list_id)
        row = conn.execute(
            "SELECT prompt_positive_raw, prompt_negative_raw, prompt_character_raw, character_entries_json, main_negative_combined_raw, params_json, software, model_name, metadata_raw, has_potion, uses_potion, uses_precise_reference, sampler FROM images WHERE id=?",
            (image_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="not found")
        uses_potion, uses_precise_reference, sampler = _resolve_generation_usage_fields(row)
        payload = {
            "software": row["software"],
            "model": row["model_name"],
            "prompt_positive_raw": row["prompt_positive_raw"],
            "prompt_negative_raw": row["prompt_negative_raw"],
            "prompt_character_raw": row["prompt_character_raw"],
            "character_entries": json.loads(row["character_entries_json"]) if row["character_entries_json"] else [],
            "main_negative_combined_raw": row["main_negative_combined_raw"],
            "params": json.loads(row["params_json"]) if row["params_json"] else None,
            "metadata_raw": row["metadata_raw"],
            "has_potion": bool(row["has_potion"] or uses_potion),
            "generation_usage": {
                "uses_potion": uses_potion,
                "uses_precise_reference": uses_precise_reference,
                "sampler": sampler,
            },
        }
        data = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        return Response(content=data, media_type="application/json", headers={"Content-Disposition": f'attachment; filename="image_{image_id}_metadata.json"'})
    finally:
        conn.close()

@api_router.get("/images/{image_id}/potion")
def download_potion(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        _assert_image_visible(conn, image_id=int(image_id), viewer=user, bm_list_id=bm_list_id)
        raise HTTPException(status_code=404, detail="disabled")
    finally:
        conn.close()


_GENERIC_CHARACTER_KEYS = {"girl", "girls", "boy", "boys", "1girl", "2girls", "3girls", "1boy", "2boys", "3boys"}


def _parse_caption_lines_server(raw: str | None) -> list[str]:
    from .services.prompt_view import parse_caption_lines
    return parse_caption_lines(raw)


def _extract_character_negative_prompt_raw(params_json_or_obj) -> str:
    from .services.prompt_view import extract_character_negative_prompt_raw
    return extract_character_negative_prompt_raw(params_json_or_obj)


def _canonical_character_name_from_text(conn: sqlite3.Connection, text: str | None) -> str:
    from .services.prompt_view import canonical_character_name_from_text
    return canonical_character_name_from_text(conn, text)


def _parse_character_entries_backend(conn: sqlite3.Connection, pos_raw: str | None, neg_raw: str | None) -> list[dict]:
    from .services.prompt_view import parse_character_entries
    return parse_character_entries(conn, pos_raw, neg_raw)


def _build_prompt_view_payload(conn: sqlite3.Connection, prompt_negative_raw: str | None, prompt_character_raw: str | None, params_json_or_obj) -> tuple[list[dict], str]:
    return build_prompt_view_payload(conn, prompt_negative_raw, prompt_character_raw, params_json_or_obj)


def _parse_prompt_multiline_to_tag_objs(conn, raw: str | None) -> list[dict]:
    return parse_prompt_multiline_to_tag_objs(conn, raw)


def _resolve_generation_usage_fields(row: sqlite3.Row | dict) -> tuple[bool, bool, str | None]:
    uses_potion = bool(row["uses_potion"] if row["uses_potion"] is not None else 0)
    uses_precise_reference = bool(row["uses_precise_reference"] if row["uses_precise_reference"] is not None else 0)
    sampler = row["sampler"] if row["sampler"] else None
    if uses_potion and uses_precise_reference and sampler is not None:
        return uses_potion, uses_precise_reference, sampler

    detected_potion, detected_precise, detected_sampler = detect_generation_usage_from_storage(
        row["params_json"],
        row["metadata_raw"],
    )
    if not uses_potion:
        uses_potion = bool(detected_potion)
    if not uses_precise_reference:
        uses_precise_reference = bool(detected_precise)
    if sampler is None and detected_sampler:
        sampler = detected_sampler
    return uses_potion, uses_precise_reference, sampler


def _normalize_detail_ids(values, *, limit: int = 64) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    for x in (values or []):
        try:
            iid = int(x)
        except Exception:
            continue
        if iid <= 0 or iid in seen:
            continue
        seen.add(iid)
        ids.append(iid)
        if len(ids) >= int(limit):
            break
    return ids


def _get_visible_image_rows(conn: sqlite3.Connection, ids: list[int], viewer: dict, *, bm_list_id: int | None = None) -> dict[int, sqlite3.Row]:
    if not ids:
        return {}
    id_list = [int(x) for x in ids]
    placeholders = ",".join("?" for _ in id_list)
    uid = int(viewer.get("id") or 0)
    lid = _normalize_bm_list_id_for_visibility(conn, viewer=viewer, bm_list_id=bm_list_id)

    sql = (
        "SELECT images.*, users.username AS creator, COALESCE(image_files.size, LENGTH(image_files.bytes)) AS file_bytes "
        "FROM images "
        "JOIN users ON users.id=images.uploader_user_id "
        "JOIN image_files ON image_files.image_id = images.id "
        f"WHERE images.id IN ({placeholders}) "
    )
    params: list = list(id_list)
    if lid is not None:
        sql += (
            "AND EXISTS(SELECT 1 FROM bookmarks b WHERE b.list_id=? AND b.image_id=images.id) "
            "AND (images.uploader_user_id=? "
            "OR (users.disabled=0 AND EXISTS(SELECT 1 FROM user_settings us WHERE us.user_id=users.id AND COALESCE(us.share_works,0)=1)))"
        )
        params.extend([int(lid), uid])
    else:
        sql += (
            "AND (images.uploader_user_id=? "
            "OR EXISTS("
            "SELECT 1 FROM user_creators uc "
            "JOIN users u2 ON u2.id=uc.creator_user_id "
            "LEFT JOIN user_settings us2 ON us2.user_id=uc.creator_user_id "
            "WHERE uc.user_id=? "
            "AND uc.creator_user_id=images.uploader_user_id "
            "AND u2.disabled=0 "
            "AND COALESCE(us2.share_works,0)=1"
            "))"
        )
        params.extend([uid, uid])
    rows = conn.execute(sql, tuple(params)).fetchall()
    return {int(r["id"]): r for r in rows}


def _fetch_tags_by_image_ids(conn: sqlite3.Connection, ids: list[int]) -> tuple[dict[int, list[sqlite3.Row]], bool]:
    if not ids:
        return {}, _table_has_col(conn, "image_tags", "seq")
    has_seq = _table_has_col(conn, "image_tags", "seq")
    cols = "image_id, tag_canonical, tag_text, tag_raw, category, emphasis_type, brace_level, numeric_weight"
    if has_seq:
        cols += ", seq"
    order_sql = " ORDER BY image_id ASC, category ASC, tag_canonical ASC"
    if has_seq:
        order_sql = " ORDER BY image_id ASC, seq ASC, category ASC, tag_canonical ASC"
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        f"SELECT {cols} FROM image_tags WHERE image_id IN ({placeholders})" + order_sql,
        tuple(ids),
    ).fetchall()
    grouped: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        iid = int(row["image_id"])
        grouped.setdefault(iid, []).append(row)
    return grouped, has_seq


def _fetch_favorite_ids(conn: sqlite3.Connection, user_id: int | None, ids: list[int]) -> set[int]:
    if not ids or user_id is None:
        return set()
    placeholders = ",".join("?" for _ in ids)
    rows = conn.execute(
        "SELECT DISTINCT b.image_id "
        "FROM bookmarks b "
        "JOIN bookmark_lists bl ON bl.id=b.list_id "
        f"WHERE bl.user_id=? AND b.image_id IN ({placeholders})",
        (int(user_id), *ids),
    ).fetchall()
    return {int(r["image_id"]) for r in rows}


def _build_image_detail_payloads(conn: sqlite3.Connection, ids: list[int], *, viewer: dict, bm_list_id: int | None = None) -> tuple[dict[int, dict], dict]:
    started = time.perf_counter()
    id_list = _normalize_detail_ids(ids)
    if not id_list:
        return {}, {"count": 0, "total_ms": 0.0}

    t0 = time.perf_counter()
    image_rows = _get_visible_image_rows(conn, id_list, viewer, bm_list_id=bm_list_id)
    visible_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    missing = [iid for iid in id_list if iid not in image_rows]
    if missing:
        raise HTTPException(status_code=404, detail="not found")

    t0 = time.perf_counter()
    tags_by_id, has_seq = _fetch_tags_by_image_ids(conn, id_list)
    tag_query_ms = round((time.perf_counter() - t0) * 1000.0, 3)

    t0 = time.perf_counter()
    favorite_ids = _fetch_favorite_ids(conn, (int(viewer["id"]) if viewer and viewer.get("id") is not None else None), id_list)
    favorite_ms = round((time.perf_counter() - t0) * 1000.0, 3)

    grouped_tags_ms = 0.0
    prompt_cache_ms = 0.0
    uc_tags_ms = 0.0
    payloads: dict[int, dict] = {}
    prompt_cache_meta_by_id: dict[int, dict] = {}
    tag_total = 0
    cached_character_entries_count = 0
    cached_main_negative_count = 0

    for iid in id_list:
        img = image_rows[iid]
        cached_character_entries = bool(str(img["character_entries_json"] or "").strip())
        cached_main_negative = bool(str(img["main_negative_combined_raw"] or "").strip())
        if cached_character_entries:
            cached_character_entries_count += 1
        if cached_main_negative:
            cached_main_negative_count += 1

        t1 = time.perf_counter()
        grouped = {"artist": [], "quality": [], "character": [], "other": []}
        tags = tags_by_id.get(iid, [])
        tag_total += len(tags)
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
        grouped_tags_ms += (time.perf_counter() - t1) * 1000.0

        t1 = time.perf_counter()
        character_entries, main_negative_combined_raw, prompt_cache_meta = ensure_prompt_view_cache(
            conn,
            image_id=int(iid),
            character_entries_json=img["character_entries_json"],
            main_negative_combined_raw=img["main_negative_combined_raw"],
            prompt_negative_raw=img["prompt_negative_raw"],
            prompt_character_raw=img["prompt_character_raw"],
            params_json_or_obj=img["params_json"],
            return_meta=True,
        )
        prompt_cache_ms += (time.perf_counter() - t1) * 1000.0
        prompt_cache_meta_by_id[iid] = prompt_cache_meta

        t1 = time.perf_counter()
        uc_tags = parse_prompt_multiline_to_tag_objs(conn, main_negative_combined_raw)
        uc_tags_ms += (time.perf_counter() - t1) * 1000.0

        uses_potion, uses_precise_reference, sampler = _resolve_generation_usage_fields(img)

        payloads[iid] = {
            "id": img["id"],
            "public_id": img["public_id"],
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
            "favorite": 1 if iid in favorite_ids else 0,
            "is_nsfw": int(img["is_nsfw"] or 0),
            "prompt_positive_raw": img["prompt_positive_raw"],
            "prompt_negative_raw": img["prompt_negative_raw"],
            "prompt_character_raw": img["prompt_character_raw"],
            "character_entries": character_entries,
            "main_negative_combined_raw": main_negative_combined_raw,
            "params_json": img["params_json"],
            "has_potion": bool(img["has_potion"] or uses_potion),
            "uses_potion": uses_potion,
            "uses_precise_reference": uses_precise_reference,
            "sampler": sampler,
            "tags": grouped,
            "uc_tags": uc_tags,
            "thumb": _append_bm_list_id_to_url(_thumb_url(conn, iid, kind="grid"), bm_list_id),
            "overlay": _append_bm_list_id_to_url(_thumb_url(conn, iid, kind="overlay"), bm_list_id),
            "view_full": _append_bm_list_id_to_url(f"/api/images/{iid}/view", bm_list_id),
            "download_file": _append_bm_list_id_to_url(f"/api/images/{iid}/file", bm_list_id),
            "download_meta": _append_bm_list_id_to_url(f"/api/images/{iid}/metadata_json", bm_list_id),
            "download_potion": _append_bm_list_id_to_url(f"/api/images/{iid}/potion", bm_list_id),
        }

    metrics = {
        "count": len(id_list),
        "has_seq": has_seq,
        "visible_ms": visible_ms,
        "tag_query_ms": tag_query_ms,
        "group_tags_ms": round(grouped_tags_ms, 3),
        "prompt_cache_ms": round(prompt_cache_ms, 3),
        "uc_tags_ms": round(uc_tags_ms, 3),
        "favorite_ms": favorite_ms,
        "tag_count": tag_total,
        "cached_character_entries_count": cached_character_entries_count,
        "cached_main_negative_count": cached_main_negative_count,
        "prompt_cache_meta_by_id": prompt_cache_meta_by_id,
        "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }
    return payloads, metrics


@api_router.get("/images/{image_id}/detail")
def image_detail(image_id: int, request: Request, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    trace_id = getattr(request.state, "trace_id", None)
    conn = get_conn()
    try:
        payloads, metrics = _build_image_detail_payloads(conn, [int(image_id)], viewer=user, bm_list_id=bm_list_id)
        payload = payloads.get(int(image_id))
        if not payload:
            raise HTTPException(status_code=404, detail="not found")
        log_perf(
            "image_detail_timing",
            trace_id=trace_id,
            user_id=int(user["id"]) if user and user.get("id") is not None else None,
            image_id=int(image_id),
            detail_source=request.headers.get("x-nim-detail-source") or None,
            detail_page=request.headers.get("x-nim-detail-page") or None,
            detail_mode=request.headers.get("x-nim-detail-mode") or None,
            visible_ms=metrics.get("visible_ms"),
            image_row_ms=metrics.get("visible_ms"),
            tag_query_ms=metrics.get("tag_query_ms"),
            group_tags_ms=metrics.get("group_tags_ms"),
            prompt_cache_ms=metrics.get("prompt_cache_ms"),
            uc_tags_ms=metrics.get("uc_tags_ms"),
            favorite_ms=metrics.get("favorite_ms"),
            total_ms=metrics.get("total_ms"),
            tag_count=metrics.get("tag_count"),
            cached_character_entries=bool(metrics.get("cached_character_entries_count")),
            cached_main_negative=bool(metrics.get("cached_main_negative_count")),
            prompt_cache_meta=(metrics.get("prompt_cache_meta_by_id") or {}).get(int(image_id)),
        )
        return payload
    finally:
        conn.close()


@api_router.post("/images/details")
async def images_detail_batch(request: Request, user: dict = Depends(get_user)):
    data = await _read_json_body_loose(request)
    req = _validate_body_model(ImageDetailsBatchReq, data)
    ids = _normalize_detail_ids(req.ids, limit=64)
    trace_id = getattr(request.state, "trace_id", None)
    conn = get_conn()
    try:
        payloads, metrics = _build_image_detail_payloads(conn, ids, viewer=user, bm_list_id=req.bm_list_id)
        log_perf(
            "image_details_batch_timing",
            trace_id=trace_id,
            user_id=int(user["id"]) if user and user.get("id") is not None else None,
            detail_source=request.headers.get("x-nim-detail-source") or None,
            detail_page=request.headers.get("x-nim-detail-page") or None,
            detail_mode=request.headers.get("x-nim-detail-mode") or None,
            count=metrics.get("count"),
            visible_ms=metrics.get("visible_ms"),
            tag_query_ms=metrics.get("tag_query_ms"),
            group_tags_ms=metrics.get("group_tags_ms"),
            prompt_cache_ms=metrics.get("prompt_cache_ms"),
            uc_tags_ms=metrics.get("uc_tags_ms"),
            favorite_ms=metrics.get("favorite_ms"),
            total_ms=metrics.get("total_ms"),
            tag_count=metrics.get("tag_count"),
            cached_character_entries_count=metrics.get("cached_character_entries_count"),
            cached_main_negative_count=metrics.get("cached_main_negative_count"),
        )
        ordered = {str(iid): payloads[iid] for iid in ids if iid in payloads}
        return {"items": ordered}
    finally:
        conn.close()



class FavReq(BaseModel):
    # Backward-compat: old clients send /images/{id}/favorite with toggle/favorite.
    favorite: int | None = None
    toggle: bool = False


def _ensure_default_bookmark_list(conn: sqlite3.Connection, user_id: int) -> int:
    """Ensure the user has a default bookmark list and return its id."""
    uid = int(user_id)
    row = conn.execute(
        "SELECT id FROM bookmark_lists WHERE user_id=? AND is_default=1 ORDER BY id LIMIT 1",
        (uid,),
    ).fetchone()
    if row:
        return int(row["id"])

    # If lists exist but default is missing, promote the first list.
    row2 = conn.execute(
        "SELECT id FROM bookmark_lists WHERE user_id=? ORDER BY sort_order ASC, id ASC LIMIT 1",
        (uid,),
    ).fetchone()
    if row2:
        lid = int(row2["id"])
        conn.execute("UPDATE bookmark_lists SET is_default=1 WHERE id=?", (lid,))
        conn.commit()
        return lid

    cur = conn.execute(
        "INSERT INTO bookmark_lists(user_id, name, sort_order, is_default) VALUES (?,?,0,1)",
        (uid, "ブックマークバー"),
    )
    conn.commit()
    return int(cur.lastrowid)


def _list_bookmark_lists(conn: sqlite3.Connection, user_id: int) -> tuple[list[dict], int]:
    uid = int(user_id)
    rows = conn.execute(
        """
        SELECT bl.id, bl.name, bl.is_default, bl.sort_order,
               COUNT(b.image_id) AS cnt
        FROM bookmark_lists bl
        LEFT JOIN bookmarks b ON b.list_id = bl.id
        WHERE bl.user_id=?
        GROUP BY bl.id
        ORDER BY bl.is_default DESC, bl.sort_order ASC, bl.id ASC
        """,
        (uid,),
    ).fetchall()

    lists: list[dict] = []
    for r in rows:
        lists.append(
            {
                "id": int(r["id"]),
                "name": str(r["name"] or ""),
                "is_default": int(r["is_default"] or 0),
                "count": int(r["cnt"] or 0),
            }
        )

    any_row = conn.execute(
        """
        SELECT COUNT(DISTINCT b.image_id) AS n
        FROM bookmarks b
        JOIN bookmark_lists bl ON bl.id=b.list_id
        WHERE bl.user_id=?
        """,
        (uid,),
    ).fetchone()
    any_count = int(any_row["n"] if any_row else 0)
    return lists, any_count


def _user_owns_bookmark_list(conn: sqlite3.Connection, user_id: int, list_id: int) -> bool:
    uid = int(user_id)
    lid = int(list_id)
    r = conn.execute(
        "SELECT 1 FROM bookmark_lists WHERE id=? AND user_id=?",
        (lid, uid),
    ).fetchone()
    return bool(r)


def _image_bookmarked(conn: sqlite3.Connection, user_id: int, image_id: int) -> int:
    uid = int(user_id)
    iid = int(image_id)
    r = conn.execute(
        """
        SELECT 1
        FROM bookmarks b
        JOIN bookmark_lists bl ON bl.id=b.list_id
        WHERE bl.user_id=? AND b.image_id=?
        LIMIT 1
        """,
        (uid, iid),
    ).fetchone()
    return 1 if r else 0


def _parse_upload_bookmark_enabled(value) -> int:
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (int, float)):
        return 1 if int(value) != 0 else 0
    txt = str(value or "").strip().lower()
    if not txt:
        return 0
    if txt in {"1", "true", "on", "yes", "y"}:
        return 1
    if txt in {"0", "false", "off", "no", "n"}:
        return 0
    raise HTTPException(status_code=400, detail="invalid bookmark_enabled")


def _normalize_upload_bookmark_list_id(
    conn: sqlite3.Connection,
    *,
    user_id: int,
    bookmark_enabled,
    bookmark_list_id,
    require_owned: bool = True,
) -> int | None:
    enabled = _parse_upload_bookmark_enabled(bookmark_enabled)
    if enabled != 1:
        return None

    uid = int(user_id)
    default_id = _ensure_default_bookmark_list(conn, uid)

    if bookmark_list_id in (None, "", 0, "0"):
        return int(default_id)

    try:
        lid = int(bookmark_list_id)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid bookmark_list_id")
    if lid <= 0:
        return int(default_id)
    if (not require_owned) or _user_owns_bookmark_list(conn, uid, lid):
        return int(lid)
    raise HTTPException(status_code=400, detail="invalid bookmark_list_id")


def _job_upload_bookmark_list_id(conn: sqlite3.Connection, *, user_id: int, bookmark_enabled, bookmark_list_id) -> int | None:
    try:
        return _normalize_upload_bookmark_list_id(
            conn,
            user_id=int(user_id),
            bookmark_enabled=bookmark_enabled,
            bookmark_list_id=bookmark_list_id,
            require_owned=False,
        )
    except HTTPException:
        return _ensure_default_bookmark_list(conn, int(user_id)) if _parse_upload_bookmark_enabled(bookmark_enabled) == 1 else None


def _apply_upload_bookmark(conn: sqlite3.Connection, *, user_id: int, image_id: int, bookmark_list_id: int | None) -> None:
    lid = int(bookmark_list_id or 0)
    if lid <= 0:
        return
    uid = int(user_id)
    iid = int(image_id)
    if not _user_owns_bookmark_list(conn, uid, lid):
        lid = _ensure_default_bookmark_list(conn, uid)
    conn.execute(
        "INSERT OR IGNORE INTO bookmarks(list_id, image_id) VALUES (?,?)",
        (int(lid), iid),
    )


@api_router.get("/bookmarks/lists")
def get_bookmark_lists(user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        _ensure_default_bookmark_list(conn, uid)
        lists, any_count = _list_bookmark_lists(conn, uid)
        return {"lists": lists, "any_count": int(any_count)}
    finally:
        conn.close()


class BookmarkListReq(BaseModel):
    name: str = ""


@api_router.post("/bookmarks/lists")
def create_bookmark_list(req: BookmarkListReq, user: dict = Depends(get_user)):
    name = str(req.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    if len(name) > 80:
        raise HTTPException(status_code=400, detail="name too long")

    conn = get_conn()
    try:
        uid = int(user["id"])
        _ensure_default_bookmark_list(conn, uid)
        try:
            cur = conn.execute(
                "INSERT INTO bookmark_lists(user_id, name, sort_order, is_default) VALUES (?,?,0,0)",
                (uid, name),
            )
            conn.commit()
        except Exception:
            # likely UNIQUE(user_id,name)
            raise HTTPException(status_code=409, detail="name already exists")
        lists, any_count = _list_bookmark_lists(conn, uid)
        return {"ok": True, "list_id": int(cur.lastrowid), "lists": lists, "any_count": int(any_count)}
    finally:
        conn.close()


@api_router.patch("/bookmarks/lists/{list_id}")
def rename_bookmark_list(list_id: int, req: BookmarkListReq, user: dict = Depends(get_user)):
    name = str(req.name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    if len(name) > 80:
        raise HTTPException(status_code=400, detail="name too long")

    conn = get_conn()
    try:
        uid = int(user["id"])
        if not _user_owns_bookmark_list(conn, uid, int(list_id)):
            raise HTTPException(status_code=404, detail="not found")
        try:
            conn.execute("UPDATE bookmark_lists SET name=? WHERE id=? AND user_id=?", (name, int(list_id), uid))
            conn.commit()
        except Exception:
            raise HTTPException(status_code=409, detail="name already exists")
        lists, any_count = _list_bookmark_lists(conn, uid)
        return {"ok": True, "lists": lists, "any_count": int(any_count)}
    finally:
        conn.close()


@api_router.delete("/bookmarks/lists/{list_id}")
def delete_bookmark_list(list_id: int, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        r = conn.execute(
            "SELECT is_default FROM bookmark_lists WHERE id=? AND user_id=?",
            (int(list_id), uid),
        ).fetchone()
        if not r:
            raise HTTPException(status_code=404, detail="not found")
        if int(r["is_default"] or 0):
            raise HTTPException(status_code=400, detail="cannot delete default list")
        conn.execute("DELETE FROM bookmark_lists WHERE id=? AND user_id=?", (int(list_id), uid))
        conn.commit()
        lists, any_count = _list_bookmark_lists(conn, uid)
        return {"ok": True, "lists": lists, "any_count": int(any_count)}
    finally:
        conn.close()


@api_router.get("/bookmarks/images/{image_id}")
def get_image_bookmarks(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        iid = int(image_id)

        _assert_image_visible(conn, image_id=iid, viewer=user, bm_list_id=bm_list_id)

        if not conn.execute("SELECT 1 FROM images WHERE id=?", (iid,)).fetchone():
            raise HTTPException(status_code=404, detail="not found")

        default_id = _ensure_default_bookmark_list(conn, uid)

        lists, any_count = _list_bookmark_lists(conn, uid)
        rows = conn.execute(
            """
            SELECT b.list_id
            FROM bookmarks b
            JOIN bookmark_lists bl ON bl.id=b.list_id
            WHERE bl.user_id=? AND b.image_id=?
            """,
            (uid, iid),
        ).fetchall()
        checked = {int(r["list_id"]) for r in rows}

        out_lists = []
        for l in lists:
            out_lists.append(
                {
                    "id": int(l["id"]),
                    "name": str(l["name"] or ""),
                    "is_default": int(l["is_default"] or 0),
                    "checked": 1 if int(l["id"]) in checked else 0,
                }
            )

        return {
            "image_id": iid,
            "default_list_id": int(default_id),
            "favorite": 1 if checked else 0,
            "lists": out_lists,
            "any_count": int(any_count),
        }
    finally:
        conn.close()


class BookmarkSetReq(BaseModel):
    list_ids: list[int] = Field(default_factory=list)


@api_router.put("/bookmarks/images/{image_id}")
def set_image_bookmarks(image_id: int, req: BookmarkSetReq, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        iid = int(image_id)

        _assert_image_visible(conn, image_id=iid, viewer=user, bm_list_id=bm_list_id)

        if not conn.execute("SELECT 1 FROM images WHERE id=?", (iid,)).fetchone():
            raise HTTPException(status_code=404, detail="not found")

        _ensure_default_bookmark_list(conn, uid)

        # Keep only lists owned by the user.
        wanted = _safe_int_list(req.list_ids, max_n=5000)
        if wanted:
            q = "SELECT id FROM bookmark_lists WHERE user_id=? AND id IN (" + ",".join(["?"] * len(wanted)) + ")"
            rows = conn.execute(q, [uid] + wanted).fetchall()
            allowed = {int(r["id"]) for r in rows}
            wanted = [x for x in wanted if x in allowed]

        # Clear current memberships for this user.
        conn.execute(
            """
            DELETE FROM bookmarks
            WHERE image_id=?
              AND list_id IN (SELECT id FROM bookmark_lists WHERE user_id=?)
            """,
            (iid, uid),
        )

        # Insert new.
        if wanted:
            conn.executemany(
                "INSERT OR IGNORE INTO bookmarks(list_id, image_id) VALUES (?,?)",
                [(int(lid), iid) for lid in wanted],
            )

        conn.commit()
        fav = _image_bookmarked(conn, uid, iid)
        return {"ok": True, "favorite": int(fav), "list_ids": wanted}
    finally:
        conn.close()


@api_router.post("/bookmarks/images/{image_id}/default")
def add_default_bookmark(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        iid = int(image_id)
        _assert_image_visible(conn, image_id=iid, viewer=user, bm_list_id=bm_list_id)
        if not conn.execute("SELECT 1 FROM images WHERE id=?", (iid,)).fetchone():
            raise HTTPException(status_code=404, detail="not found")
        default_id = _ensure_default_bookmark_list(conn, uid)
        conn.execute("INSERT OR IGNORE INTO bookmarks(list_id, image_id) VALUES (?,?)", (default_id, iid))
        conn.commit()
        return {"ok": True, "favorite": 1, "default_list_id": int(default_id)}
    finally:
        conn.close()


@api_router.post("/bookmarks/images/{image_id}/clear")
def clear_bookmarks_for_image(image_id: int, bm_list_id: int | None = None, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        iid = int(image_id)
        _assert_image_visible(conn, image_id=iid, viewer=user, bm_list_id=bm_list_id)
        if not conn.execute("SELECT 1 FROM images WHERE id=?", (iid,)).fetchone():
            raise HTTPException(status_code=404, detail="not found")
        conn.execute(
            """
            DELETE FROM bookmarks
            WHERE image_id=?
              AND list_id IN (SELECT id FROM bookmark_lists WHERE user_id=?)
            """,
            (iid, uid),
        )
        conn.commit()
        return {"ok": True, "favorite": 0}
    finally:
        conn.close()


@api_router.post("/images/{image_id}/favorite")
def set_favorite(image_id: int, req: FavReq, user: dict = Depends(get_user)):
    """Compatibility endpoint. Now acts on *per-user* bookmarks.

    - toggle: toggles between "bookmarked (default list)" and "not bookmarked (clear all)"
    - favorite=1: add to default list
    - favorite=0: clear from all lists
    """
    conn = get_conn()
    try:
        uid = int(user["id"])
        iid = int(image_id)
        if not conn.execute("SELECT 1 FROM images WHERE id=?", (iid,)).fetchone():
            raise HTTPException(status_code=404, detail="not found")

        default_id = _ensure_default_bookmark_list(conn, uid)
        cur = _image_bookmarked(conn, uid, iid)

        if req.toggle:
            if cur:
                conn.execute(
                    """
                    DELETE FROM bookmarks
                    WHERE image_id=?
                      AND list_id IN (SELECT id FROM bookmark_lists WHERE user_id=?)
                    """,
                    (iid, uid),
                )
                conn.commit()
                return {"ok": True, "favorite": 0}
            conn.execute("INSERT OR IGNORE INTO bookmarks(list_id, image_id) VALUES (?,?)", (default_id, iid))
            conn.commit()
            return {"ok": True, "favorite": 1, "default_list_id": int(default_id)}

        if req.favorite is not None and int(req.favorite) == 0:
            conn.execute(
                """
                DELETE FROM bookmarks
                WHERE image_id=?
                  AND list_id IN (SELECT id FROM bookmark_lists WHERE user_id=?)
                """,
                (iid, uid),
            )
            conn.commit()
            return {"ok": True, "favorite": 0}

        # default: add
        conn.execute("INSERT OR IGNORE INTO bookmarks(list_id, image_id) VALUES (?,?)", (default_id, iid))
        conn.commit()
        return {"ok": True, "favorite": 1, "default_list_id": int(default_id)}
    finally:
        conn.close()


class BulkDeleteQuery(BaseModel):
    creator: str = ""
    software: str = ""
    tags: list[str] = Field(default_factory=list)
    tags_not: list[str] = Field(default_factory=list)
    date_from: str = ""   # YYYY-MM-DD
    date_to: str = ""     # YYYY-MM-DD
    dedup_only: int = 0
    bm_any: int = 0
    bm_list_id: int | None = None
    # backward-compat
    fav_only: int = 0


class BulkDeleteReq(BaseModel):
    mode: str
    ids: list[int] = Field(default_factory=list)
    query: BulkDeleteQuery | None = None
    exclude_ids: list[int] = Field(default_factory=list)


class BulkBookmarkSelectionReq(BaseModel):
    mode: str
    ids: list[int] = Field(default_factory=list)
    query: BulkDeleteQuery | None = None
    exclude_ids: list[int] = Field(default_factory=list)
    bm_list_id: int | None = None


class BulkBookmarkApplyReq(BulkBookmarkSelectionReq):
    add_list_ids: list[int] = Field(default_factory=list)
    remove_list_ids: list[int] = Field(default_factory=list)


def _safe_int_list(xs: list[int] | None, *, max_n: int = 200000) -> list[int]:
    out: list[int] = []
    if not xs:
        return out
    seen = set()
    for x in xs:
        try:
            n = int(x)
        except Exception:
            continue
        if n <= 0 or n in seen:
            continue
        seen.add(n)
        out.append(n)
        if len(out) >= max_n:
            break
    return out


def _prepare_bulk_bookmark_scope(
    conn: sqlite3.Connection,
    *,
    viewer: dict,
    mode: str,
    ids: list[int] | None,
    query: BulkDeleteQuery | None,
    exclude_ids: list[int] | None,
    bm_list_id: int | None,
) -> int:
    """Populate tmp_bulk_bm_sel with the selected visible image ids and return its count."""
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bulk_bm_sel(id INTEGER PRIMARY KEY)")
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bulk_bm_excl(id INTEGER PRIMARY KEY)")
    conn.execute("DELETE FROM tmp_bulk_bm_sel")
    conn.execute("DELETE FROM tmp_bulk_bm_excl")

    mode_key = str(mode or "").strip().lower()
    if mode_key not in {"ids", "query"}:
        raise HTTPException(status_code=400, detail="mode must be ids/query")

    excluded = _safe_int_list(exclude_ids, max_n=300000)
    if excluded:
        for i in range(0, len(excluded), 500):
            chunk = excluded[i:i+500]
            conn.executemany("INSERT OR IGNORE INTO tmp_bulk_bm_excl(id) VALUES (?)", [(int(x),) for x in chunk])

    if mode_key == "ids":
        visible_ids: list[int] = []
        excluded_set = set(excluded)
        vis_lid = _normalize_bm_list_id_for_visibility(conn, viewer=viewer, bm_list_id=bm_list_id)
        for iid in _safe_int_list(ids, max_n=300000):
            if excluded_set and int(iid) in excluded_set:
                continue
            try:
                _assert_image_visible(conn, image_id=int(iid), viewer=viewer, bm_list_id=vis_lid)
            except HTTPException:
                continue
            visible_ids.append(int(iid))
        if visible_ids:
            for i in range(0, len(visible_ids), 500):
                chunk = visible_ids[i:i+500]
                conn.executemany("INSERT OR IGNORE INTO tmp_bulk_bm_sel(id) VALUES (?)", [(int(x),) for x in chunk])
        return len(visible_ids)

    q = query or BulkDeleteQuery()
    filters = normalize_gallery_filters(
        creator=q.creator,
        software=q.software,
        tags=",".join(q.tags or []),
        tags_not=",".join(q.tags_not or []),
        date_from=q.date_from,
        date_to=q.date_to,
        dedup_only=q.dedup_only,
        bm_any=q.bm_any,
        bm_list_id=q.bm_list_id,
        fav_only=q.fav_only,
        sort=getattr(q, "sort", "newest"),
        normalize_tag=normalize_tag,
    )
    normalized_lid = normalize_bookmark_list_id(
        conn,
        viewer=viewer,
        bm_list_id=(filters.bm_list_id if filters.bm_list_id is not None else bm_list_id),
        can_view_bookmark_list=lambda c, viewer, lid: _can_view_bookmark_list(c, viewer=viewer, list_id=lid),
    )
    filters.bm_list_id = normalized_lid

    creator_ok, creator_id = resolve_creator_id(conn, filters.creator)
    if not creator_ok:
        return 0

    uid = int(viewer.get("id") or 0)
    join_sql, join_params = build_user_bookmark_join(uid, filters.bm_list_id)
    where: list[str] = []
    params: list = []
    apply_common_filters(
        conn,
        filters=filters,
        creator_id=creator_id,
        where=where,
        params=params,
        viewer=viewer,
        apply_tag_filters=lambda c, w, p, tag_list, tag_not_list, image_alias: _apply_tag_filters(c, w, p, tag_list, tag_not_list, image_alias=image_alias),
        append_visibility_filter=lambda w, p, viewer: _append_visibility_filter(w, p, viewer=viewer, bm_list_id=filters.bm_list_id),
    )
    if excluded:
        where.append("images.id NOT IN (SELECT id FROM tmp_bulk_bm_excl)")
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    from_sql = (
        " FROM images "
        " JOIN users ON users.id = images.uploader_user_id "
        + join_sql
    )
    conn.execute(
        "INSERT OR IGNORE INTO tmp_bulk_bm_sel(id) "
        "SELECT images.id "
        + from_sql
        + where_sql,
        join_params + params,
    )
    row = conn.execute("SELECT COUNT(*) AS n FROM tmp_bulk_bm_sel").fetchone()
    return int(row["n"] if row else 0)


def _apply_bulk_dec(conn: sqlite3.Connection, table: str, key_col: str, key_val: str, dec: int) -> None:
    if not key_val or not dec:
        return
    conn.execute(
        f"UPDATE {table} SET image_count = image_count - ? WHERE {key_col} = ?",
        (int(dec), key_val),
    )
    conn.execute(
        f"DELETE FROM {table} WHERE {key_col} = ? AND image_count <= 0",
        (key_val,),
    )


@api_router.post("/bookmarks/bulk/status")
def bulk_bookmark_status(req: BulkBookmarkSelectionReq, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        selected_count = _prepare_bulk_bookmark_scope(
            conn,
            viewer=user,
            mode=req.mode,
            ids=req.ids,
            query=req.query,
            exclude_ids=req.exclude_ids,
            bm_list_id=req.bm_list_id,
        )
        default_id = _ensure_default_bookmark_list(conn, uid)
        lists, any_count = _list_bookmark_lists(conn, uid)
        matched_rows = conn.execute(
            """
            SELECT bl.id, COUNT(b.image_id) AS matched_count
            FROM bookmark_lists bl
            LEFT JOIN bookmarks b
              ON b.list_id = bl.id
             AND b.image_id IN (SELECT id FROM tmp_bulk_bm_sel)
            WHERE bl.user_id=?
            GROUP BY bl.id
            ORDER BY bl.sort_order ASC, bl.id ASC
            """,
            (uid,),
        ).fetchall()
        matched_map = {int(r["id"]): int(r["matched_count"] or 0) for r in matched_rows}
        out_lists = []
        for l in lists:
            lid = int(l["id"])
            matched = int(matched_map.get(lid, 0))
            if selected_count > 0 and matched == selected_count:
                state = "all"
            elif matched > 0:
                state = "some"
            else:
                state = "none"
            out_lists.append(
                {
                    "id": lid,
                    "name": str(l["name"] or ""),
                    "is_default": int(l["is_default"] or 0),
                    "state": state,
                    "matched_count": matched,
                }
            )
        return {
            "ok": True,
            "selected_count": int(selected_count),
            "default_list_id": int(default_id),
            "any_count": int(any_count),
            "lists": out_lists,
        }
    finally:
        conn.close()


@api_router.post("/bookmarks/bulk/apply")
def bulk_bookmark_apply(req: BulkBookmarkApplyReq, user: dict = Depends(get_user)):
    conn = get_conn()
    try:
        uid = int(user["id"])
        selected_count = _prepare_bulk_bookmark_scope(
            conn,
            viewer=user,
            mode=req.mode,
            ids=req.ids,
            query=req.query,
            exclude_ids=req.exclude_ids,
            bm_list_id=req.bm_list_id,
        )
        if selected_count <= 0:
            return {"ok": True, "selected_count": 0, "add_list_ids": [], "remove_list_ids": []}

        add_list_ids = _safe_int_list(req.add_list_ids, max_n=5000)
        remove_list_ids = _safe_int_list(req.remove_list_ids, max_n=5000)
        if add_list_ids or remove_list_ids:
            all_list_ids = list(dict.fromkeys(add_list_ids + remove_list_ids))
            q = "SELECT id FROM bookmark_lists WHERE user_id=? AND id IN (" + ",".join(["?"] * len(all_list_ids)) + ")"
            rows = conn.execute(q, [uid] + all_list_ids).fetchall()
            allowed = {int(r["id"]) for r in rows}
            add_list_ids = [x for x in add_list_ids if x in allowed]
            remove_list_ids = [x for x in remove_list_ids if x in allowed]

        if add_list_ids:
            remove_set = set(remove_list_ids)
            add_list_ids = [x for x in add_list_ids if x not in remove_set]

        conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bulk_bm_add(id INTEGER PRIMARY KEY)")
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_bulk_bm_remove(id INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM tmp_bulk_bm_add")
        conn.execute("DELETE FROM tmp_bulk_bm_remove")

        if remove_list_ids:
            for i in range(0, len(remove_list_ids), 500):
                chunk = remove_list_ids[i:i+500]
                conn.executemany("INSERT OR IGNORE INTO tmp_bulk_bm_remove(id) VALUES (?)", [(int(x),) for x in chunk])
            conn.execute(
                "DELETE FROM bookmarks WHERE list_id IN (SELECT id FROM tmp_bulk_bm_remove) AND image_id IN (SELECT id FROM tmp_bulk_bm_sel)"
            )

        if add_list_ids:
            for i in range(0, len(add_list_ids), 500):
                chunk = add_list_ids[i:i+500]
                conn.executemany("INSERT OR IGNORE INTO tmp_bulk_bm_add(id) VALUES (?)", [(int(x),) for x in chunk])
            conn.execute(
                "INSERT OR IGNORE INTO bookmarks(list_id, image_id) "
                "SELECT tmp_bulk_bm_add.id, tmp_bulk_bm_sel.id FROM tmp_bulk_bm_add CROSS JOIN tmp_bulk_bm_sel"
            )

        conn.commit()
        return {
            "ok": True,
            "selected_count": int(selected_count),
            "add_list_ids": add_list_ids,
            "remove_list_ids": remove_list_ids,
        }
    finally:
        conn.close()


@api_router.post("/images/bulk_delete")
def bulk_delete_images(req: BulkDeleteReq, user: dict = Depends(get_user)):
    """Delete images in bulk.

    Requirements:
    - user role can delete ONLY their own images.
    - admin/master can delete any images.
    - supports "ids" mode and "query" mode (current gallery filter).
    """

    role = str(user.get("role") or "user")
    user_id = int(user.get("id") or 0)
    username = str(user.get("username") or "")

    mode = (req.mode or "").strip().lower()
    if mode not in {"ids", "query"}:
        raise HTTPException(status_code=400, detail="mode must be ids/query")

    # normalize exclude ids
    exclude_ids = _safe_int_list(req.exclude_ids, max_n=300000)

    conn = get_conn()
    disk_paths: list[str] = []
    try:
        # Prepare temp tables (avoid SQLite variable limits).
        conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_excl(id INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM tmp_excl")
        for i in range(0, len(exclude_ids), 500):
            chunk = exclude_ids[i:i+500]
            conn.executemany("INSERT OR IGNORE INTO tmp_excl(id) VALUES (?)", [(int(x),) for x in chunk])

        conn.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_del_ids(id INTEGER PRIMARY KEY)")
        conn.execute("DELETE FROM tmp_del_ids")

        requested_ids: list[int] = []

        if mode == "ids":
            requested_ids = _safe_int_list(req.ids, max_n=300000)
            if not requested_ids:
                return {"ok": True, "deleted": 0}

            # Insert requested ids.
            for i in range(0, len(requested_ids), 500):
                chunk = requested_ids[i:i+500]
                conn.executemany("INSERT OR IGNORE INTO tmp_del_ids(id) VALUES (?)", [(int(x),) for x in chunk])

            # Permission: user can delete ONLY their own images.
            if role == "user":
                # Keep only ids owned by the current user.
                conn.execute(
                    "DELETE FROM tmp_del_ids WHERE id NOT IN (SELECT id FROM images WHERE uploader_user_id = ?)",
                    (user_id,),
                )
                kept = int(conn.execute("SELECT COUNT(*) AS n FROM tmp_del_ids").fetchone()[0])
                if kept != len(requested_ids):
                    raise HTTPException(status_code=403, detail="can delete only your images")
        else:
            q = req.query or BulkDeleteQuery()

            # For user role: force creator=self, ignore other creator field.
            creator_username = (q.creator or "").strip()
            creator_id: int | None = None
            if role == "user":
                creator_id = user_id
            elif creator_username:
                r = _find_user_by_username(conn, creator_username, "id")
                if not r:
                    return {"ok": True, "deleted": 0}
                creator_id = int(r["id"])

            where: list[str] = []
            params: list = []
            if creator_id is not None:
                where.append("images.uploader_user_id = ?")
                params.append(int(creator_id))
            if q.software:
                where.append("images.software = ?")
                params.append(q.software)
            if q.date_from:
                where.append("images.file_mtime_utc >= ?")
                params.append(q.date_from)
            if q.date_to:
                try:
                    dt = datetime.strptime(q.date_to, "%Y-%m-%d") + timedelta(days=1)
                    where.append("images.file_mtime_utc < ?")
                    params.append(dt.strftime("%Y-%m-%d"))
                except Exception:
                    where.append("substr(images.file_mtime_utc,1,10) <= ?")
                    params.append(q.date_to)
            if int(q.dedup_only or 0):
                where.append("images.dedup_flag = 1")
            bm_any = int((getattr(q, "bm_any", 0) or 0) or (q.fav_only or 0) or 0)
            bm_list_id = getattr(q, "bm_list_id", None)

            # Bookmark filters are per-user (current session user).
            if bm_list_id is not None:
                try:
                    lid = int(bm_list_id)
                except Exception:
                    lid = None
                if lid:
                    ok = conn.execute(
                        "SELECT 1 FROM bookmark_lists WHERE id=? AND user_id=?",
                        (lid, user_id),
                    ).fetchone()
                    if ok:
                        where.append(
                            "EXISTS(SELECT 1 FROM bookmarks b WHERE b.list_id = ? AND b.image_id = images.id)"
                        )
                        params.append(lid)
            elif bm_any:
                where.append(
                    "EXISTS("
                    "SELECT 1 FROM bookmarks b "
                    "JOIN bookmark_lists bl ON bl.id=b.list_id "
                    "WHERE bl.user_id = ? AND b.image_id = images.id"
                    ")"
                )
                params.append(user_id)


            tag_list: list[str] = []
            tag_not_list: list[str] = []
            if q.tags:
                for t in q.tags:
                    tn = normalize_tag(str(t or ""))
                    if tn:
                        tag_list.append(tn)
                tag_list = list(dict.fromkeys(tag_list))

            if q.tags_not:
                for t in q.tags_not:
                    tn = normalize_tag(str(t or ""))
                    if tn:
                        tag_not_list.append(tn)
                tag_not_list = list(dict.fromkeys(tag_not_list))

            if tag_list and tag_not_list:
                inc = set(tag_list)
                tag_not_list = [t for t in tag_not_list if t not in inc]

            _apply_tag_filters(conn, where, params, tag_list, tag_not_list, image_alias="images")

            # Exclusions
            where.append("images.id NOT IN (SELECT id FROM tmp_excl)")

            where_sql = (" WHERE " + " AND ".join(where)) if where else ""

            # Materialize ids into temp table so we can reuse it across stats updates and deletion.
            conn.execute(
                "INSERT OR IGNORE INTO tmp_del_ids(id) SELECT images.id FROM images" + where_sql,
                params,
            )

        to_del = int(conn.execute("SELECT COUNT(*) AS n FROM tmp_del_ids").fetchone()[0])
        if to_del <= 0:
            return {"ok": True, "deleted": 0}

        # Capture aggregates BEFORE deletion.
        # disk paths (best-effort)
        drows = conn.execute(
            "SELECT image_files.disk_path AS p FROM image_files JOIN tmp_del_ids d ON d.id = image_files.image_id"
        ).fetchall()
        for r in drows:
            p = (r["p"] if not isinstance(r, tuple) else r[0])
            if p and str(p).strip():
                disk_paths.append(str(p))

        ddrows = conn.execute(
            "SELECT image_derivatives.disk_path AS p FROM image_derivatives JOIN tmp_del_ids d ON d.id = image_derivatives.image_id"
        ).fetchall()
        for r in ddrows:
            p = (r["p"] if not isinstance(r, tuple) else r[0])
            if p and str(p).strip():
                disk_paths.append(str(p))

        # touched sig hashes for incremental dedup recompute
        hrows = conn.execute(
            "SELECT DISTINCT images.main_sig_hash AS h FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.main_sig_hash IS NOT NULL AND TRIM(images.main_sig_hash) <> ''"
        ).fetchall()
        hashes = [str((r["h"] if not isinstance(r, tuple) else r[0]) or "") for r in hrows]
        hashes = [h for h in hashes if h.strip()]

        # creators
        creators = conn.execute(
            "SELECT users.username AS k, COUNT(*) AS c "
            "FROM images JOIN users ON users.id = images.uploader_user_id "
            "JOIN tmp_del_ids d ON d.id = images.id GROUP BY users.username"
        ).fetchall()

        # software
        softwares = conn.execute(
            "SELECT images.software AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.software IS NOT NULL AND TRIM(images.software) <> '' GROUP BY images.software"
        ).fetchall()

        # per-creator software
        c_softwares = conn.execute(
            "SELECT images.uploader_user_id AS cid, images.software AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.software IS NOT NULL AND TRIM(images.software) <> '' "
            "GROUP BY images.uploader_user_id, images.software"
        ).fetchall()

        # day/month/year
        days = conn.execute(
            "SELECT SUBSTR(images.file_mtime_utc,1,10) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 10 "
            "GROUP BY SUBSTR(images.file_mtime_utc,1,10)"
        ).fetchall()
        months = conn.execute(
            "SELECT SUBSTR(images.file_mtime_utc,1,7) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 7 "
            "GROUP BY SUBSTR(images.file_mtime_utc,1,7)"
        ).fetchall()
        years = conn.execute(
            "SELECT SUBSTR(images.file_mtime_utc,1,4) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 4 "
            "GROUP BY SUBSTR(images.file_mtime_utc,1,4)"
        ).fetchall()

        # per-creator day/month/year for calendar
        c_days = conn.execute(
            "SELECT images.uploader_user_id AS cid, SUBSTR(images.file_mtime_utc,1,10) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 10 "
            "GROUP BY images.uploader_user_id, SUBSTR(images.file_mtime_utc,1,10)"
        ).fetchall()
        c_months = conn.execute(
            "SELECT images.uploader_user_id AS cid, SUBSTR(images.file_mtime_utc,1,7) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 7 "
            "GROUP BY images.uploader_user_id, SUBSTR(images.file_mtime_utc,1,7)"
        ).fetchall()
        c_years = conn.execute(
            "SELECT images.uploader_user_id AS cid, SUBSTR(images.file_mtime_utc,1,4) AS k, COUNT(*) AS c "
            "FROM images JOIN tmp_del_ids d ON d.id = images.id "
            "WHERE images.file_mtime_utc IS NOT NULL AND LENGTH(images.file_mtime_utc) >= 4 "
            "GROUP BY images.uploader_user_id, SUBSTR(images.file_mtime_utc,1,4)"
        ).fetchall()

        # tags (distinct images per tag)
        tags = conn.execute(
            "SELECT it.tag_canonical AS k, COUNT(DISTINCT it.image_id) AS c "
            "FROM image_tags it JOIN tmp_del_ids d ON d.id = it.image_id "
            "GROUP BY it.tag_canonical"
        ).fetchall()

        # Delete rows (cascades to derivatives/image_files/image_tags/etc).
        conn.execute("DELETE FROM images WHERE id IN (SELECT id FROM tmp_del_ids)")

        # Apply stat decrements (best-effort; clamp at zero).
        for r in creators:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_creators", "creator", k, c)

        for r in softwares:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_software", "software", k, c)

        for r in c_softwares:
            cid = int(r["cid"] if not isinstance(r, tuple) else r[0])
            k = str(r["k"] if not isinstance(r, tuple) else r[1])
            c = int(r["c"] if not isinstance(r, tuple) else r[2])
            conn.execute(
                "UPDATE stat_creator_software SET image_count = image_count - ? WHERE creator_id=? AND software=?",
                (c, cid, k),
            )
            conn.execute(
                "DELETE FROM stat_creator_software WHERE creator_id=? AND software=? AND image_count <= 0",
                (cid, k),
            )

        for r in days:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_day_counts", "ymd", k, c)

        for r in months:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_month_counts", "ym", k, c)

        for r in years:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_year_counts", "year", k, c)

        # per-creator calendar stats decrements
        for r in c_days:
            cid = int(r["cid"] if not isinstance(r, tuple) else r[0])
            k = str(r["k"] if not isinstance(r, tuple) else r[1])
            c = int(r["c"] if not isinstance(r, tuple) else r[2])
            conn.execute(
                "UPDATE stat_creator_day_counts SET image_count = image_count - ? WHERE creator_id=? AND ymd=?",
                (c, cid, k),
            )
            conn.execute(
                "DELETE FROM stat_creator_day_counts WHERE creator_id=? AND ymd=? AND image_count <= 0",
                (cid, k),
            )

        for r in c_months:
            cid = int(r["cid"] if not isinstance(r, tuple) else r[0])
            k = str(r["k"] if not isinstance(r, tuple) else r[1])
            c = int(r["c"] if not isinstance(r, tuple) else r[2])
            conn.execute(
                "UPDATE stat_creator_month_counts SET image_count = image_count - ? WHERE creator_id=? AND ym=?",
                (c, cid, k),
            )
            conn.execute(
                "DELETE FROM stat_creator_month_counts WHERE creator_id=? AND ym=? AND image_count <= 0",
                (cid, k),
            )

        for r in c_years:
            cid = int(r["cid"] if not isinstance(r, tuple) else r[0])
            k = str(r["k"] if not isinstance(r, tuple) else r[1])
            c = int(r["c"] if not isinstance(r, tuple) else r[2])
            conn.execute(
                "UPDATE stat_creator_year_counts SET image_count = image_count - ? WHERE creator_id=? AND year=?",
                (c, cid, k),
            )
            conn.execute(
                "DELETE FROM stat_creator_year_counts WHERE creator_id=? AND year=? AND image_count <= 0",
                (cid, k),
            )

        for r in tags:
            k = str(r["k"] if not isinstance(r, tuple) else r[0])
            c = int(r["c"] if not isinstance(r, tuple) else r[1])
            _apply_bulk_dec(conn, "stat_tag_counts", "tag_canonical", k, c)

        # Dedup: recompute only affected signatures.
        try:
            stats_service.recompute_dedup_flags_for_hashes(conn, hashes)
        except Exception:
            # best-effort
            pass

        conn.commit()

        # Best-effort file deletion AFTER commit.
        for p in disk_paths:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        return {"ok": True, "deleted": int(to_del)}
    except HTTPException:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"bulk delete failed: {type(e).__name__}: {e}")
    finally:
        conn.close()