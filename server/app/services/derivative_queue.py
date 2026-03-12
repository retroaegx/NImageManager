from __future__ import annotations

import sqlite3
import threading
import time
from typing import Callable

from ..db import get_queue_conn
from ..logging_utils import log_perf

DerivativeProcessor = Callable[[int, tuple[str, ...], str | None, str | None], None]
UploadProcessor = Callable[[int, str | None, str | None], None]


class ContentQueueWorker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._derivative_processor: DerivativeProcessor | None = None
        self._upload_processor: UploadProcessor | None = None

    def start(self, upload_processor: UploadProcessor, derivative_processor: DerivativeProcessor) -> None:
        with self._lock:
            self._upload_processor = upload_processor
            self._derivative_processor = derivative_processor
            if self._thread and self._thread.is_alive():
                self._wake.set()
                return
            self._reset_running_jobs()
            self._stop.clear()
            self._wake.clear()
            self._thread = threading.Thread(
                target=self._run,
                name="nim-content-worker",
                daemon=True,
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop.set()
            self._wake.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

    def enqueue_derivative(
        self,
        image_id: int,
        kinds: tuple[str, ...],
        *,
        source: str = "unknown",
        trace_id: str | None = None,
    ) -> bool:
        clean_kinds = self._normalize_kinds(kinds)
        if not clean_kinds:
            return False
        need_grid = 1 if "grid" in clean_kinds else 0
        need_overlay = 1 if "overlay" in clean_kinds else 0
        self._execute_with_retry(
            "derivative_queue_enqueue_retry",
            image_id=int(image_id),
            kinds=list(clean_kinds),
            source=str(source or "unknown"),
            trace_id=(trace_id or None),
            op=lambda db: db.execute(
                """
                INSERT INTO derivative_jobs(
                  image_id, need_grid, need_overlay, status,
                  requested_at_utc, started_at_utc, finished_at_utc,
                  last_error, retry_count, request_count, last_source, last_trace_id
                ) VALUES (?,?,?,?,datetime('now'),NULL,NULL,NULL,0,1,?,?)
                ON CONFLICT(image_id) DO UPDATE SET
                  need_grid = CASE WHEN excluded.need_grid=1 THEN 1 ELSE derivative_jobs.need_grid END,
                  need_overlay = CASE WHEN excluded.need_overlay=1 THEN 1 ELSE derivative_jobs.need_overlay END,
                  status = CASE WHEN derivative_jobs.status='running' THEN 'running' ELSE 'queued' END,
                  requested_at_utc = datetime('now'),
                  finished_at_utc = NULL,
                  last_error = NULL,
                  retry_count = 0,
                  request_count = derivative_jobs.request_count + 1,
                  last_source = excluded.last_source,
                  last_trace_id = excluded.last_trace_id
                """,
                (int(image_id), need_grid, need_overlay, "queued", str(source or "unknown"), trace_id),
            ),
        )
        self._wake.set()
        log_perf(
            "derivative_queue_enqueue",
            image_id=int(image_id),
            kinds=list(clean_kinds),
            source=str(source or "unknown"),
            trace_id=(trace_id or None),
        )
        return True

    def enqueue_upload_item(
        self,
        item_id: int,
        *,
        source: str = "unknown",
        trace_id: str | None = None,
    ) -> bool:
        iid = int(item_id)
        if iid <= 0:
            return False
        self._execute_with_retry(
            "upload_item_queue_enqueue_retry",
            item_id=iid,
            source=str(source or "unknown"),
            trace_id=(trace_id or None),
            op=lambda db: db.execute(
                """
                INSERT INTO upload_item_jobs(
                  item_id, status, requested_at_utc, started_at_utc, finished_at_utc,
                  last_error, retry_count, request_count, last_source, last_trace_id
                ) VALUES (?, 'queued', datetime('now'), NULL, NULL, NULL, 0, 1, ?, ?)
                ON CONFLICT(item_id) DO UPDATE SET
                  status = CASE WHEN upload_item_jobs.status='running' THEN 'running' ELSE 'queued' END,
                  requested_at_utc = datetime('now'),
                  finished_at_utc = NULL,
                  last_error = NULL,
                  retry_count = 0,
                  request_count = upload_item_jobs.request_count + 1,
                  last_source = excluded.last_source,
                  last_trace_id = excluded.last_trace_id
                """,
                (iid, str(source or "unknown"), trace_id),
            ),
        )
        self._wake.set()
        log_perf(
            "upload_item_queue_enqueue",
            item_id=iid,
            source=str(source or "unknown"),
            trace_id=(trace_id or None),
        )
        return True

    def _execute_with_retry(self, event: str, *, op: Callable[[sqlite3.Connection], None], **ctx) -> None:
        attempt = 0
        while True:
            attempt += 1
            db = get_queue_conn()
            try:
                op(db)
                db.commit()
                return
            except sqlite3.OperationalError as exc:
                if ("database is locked" not in str(exc).lower()) or attempt >= 8:
                    raise
                log_perf(event, attempt=attempt, error_message=str(exc), **ctx)
                time.sleep(min(0.05 * attempt, 0.4))
            finally:
                try:
                    db.close()
                except Exception:
                    pass

    def _normalize_kinds(self, kinds: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
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

    def _run(self) -> None:
        while not self._stop.is_set():
            job = self._claim_next_job()
            if not job:
                self._wake.wait(timeout=1.0)
                self._wake.clear()
                continue

            started = time.perf_counter()
            try:
                if job["type"] == "upload":
                    processor = self._upload_processor
                    if processor is None:
                        raise RuntimeError("upload processor not configured")
                    processor(int(job["item_id"]), job["source"], job["trace_id"])
                    self._finish_upload_job(item_id=int(job["item_id"]))
                    log_perf(
                        "upload_item_queue_job_done",
                        item_id=int(job["item_id"]),
                        source=job["source"],
                        trace_id=job["trace_id"],
                        duration_ms=round((time.perf_counter() - started) * 1000.0, 3),
                    )
                else:
                    processor = self._derivative_processor
                    if processor is None:
                        raise RuntimeError("derivative processor not configured")
                    processor(int(job["image_id"]), job["kinds"], job["source"], job["trace_id"])
                    self._finish_derivative_job(image_id=int(job["image_id"]), processed_kinds=job["kinds"])
                    log_perf(
                        "derivative_queue_job_done",
                        image_id=int(job["image_id"]),
                        kinds=list(job["kinds"]),
                        source=job["source"],
                        trace_id=job["trace_id"],
                        duration_ms=round((time.perf_counter() - started) * 1000.0, 3),
                    )
            except Exception as exc:
                if job["type"] == "upload":
                    self._fail_upload_job(item_id=int(job["item_id"]), error=exc)
                    log_perf(
                        "upload_item_queue_job_error",
                        item_id=int(job["item_id"]),
                        source=job["source"],
                        trace_id=job["trace_id"],
                        error_type=exc.__class__.__name__,
                        error_message=str(exc),
                        duration_ms=round((time.perf_counter() - started) * 1000.0, 3),
                    )
                else:
                    self._fail_derivative_job(image_id=int(job["image_id"]), error=exc)
                    log_perf(
                        "derivative_queue_job_error",
                        image_id=int(job["image_id"]),
                        kinds=list(job["kinds"]),
                        source=job["source"],
                        trace_id=job["trace_id"],
                        error_type=exc.__class__.__name__,
                        error_message=str(exc),
                        duration_ms=round((time.perf_counter() - started) * 1000.0, 3),
                    )

    def _reset_running_jobs(self) -> None:
        conn = get_queue_conn()
        try:
            conn.execute("UPDATE derivative_jobs SET status='queued', started_at_utc=NULL WHERE status='running' AND (need_grid=1 OR need_overlay=1)")
            conn.execute("UPDATE upload_item_jobs SET status='queued', started_at_utc=NULL WHERE status='running'")
            conn.commit()
        finally:
            conn.close()

    def _claim_next_job(self) -> dict | None:
        upload_job = self._claim_next_upload_job()
        if upload_job:
            return upload_job
        return self._claim_next_derivative_job()

    def _claim_next_upload_job(self) -> dict | None:
        conn = get_queue_conn()
        try:
            row = conn.execute(
                """
                SELECT item_id, last_source, last_trace_id
                FROM upload_item_jobs
                WHERE status='queued'
                ORDER BY requested_at_utc ASC, item_id ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            item_id = int(row["item_id"])
            cur = conn.execute(
                "UPDATE upload_item_jobs SET status='running', started_at_utc=datetime('now') WHERE item_id=? AND status='queued'",
                (item_id,),
            )
            conn.commit()
            if int(cur.rowcount or 0) != 1:
                return None
            return {
                "type": "upload",
                "item_id": item_id,
                "source": str(row["last_source"] or "queue"),
                "trace_id": (str(row["last_trace_id"] or "") or None),
            }
        finally:
            conn.close()

    def _claim_next_derivative_job(self) -> dict | None:
        conn = get_queue_conn()
        try:
            row = conn.execute(
                """
                SELECT image_id, need_grid, need_overlay, last_source, last_trace_id
                FROM derivative_jobs
                WHERE status='queued' AND (need_grid=1 OR need_overlay=1)
                ORDER BY requested_at_utc ASC, image_id ASC
                LIMIT 1
                """
            ).fetchone()
            if not row:
                return None
            image_id = int(row["image_id"])
            cur = conn.execute(
                "UPDATE derivative_jobs SET status='running', started_at_utc=datetime('now') WHERE image_id=? AND status='queued'",
                (image_id,),
            )
            conn.commit()
            if int(cur.rowcount or 0) != 1:
                return None
            kinds: list[str] = []
            if int(row["need_grid"] or 0):
                kinds.append("grid")
            if int(row["need_overlay"] or 0):
                kinds.append("overlay")
            return {
                "type": "derivative",
                "image_id": image_id,
                "kinds": tuple(kinds),
                "source": str(row["last_source"] or "queue"),
                "trace_id": (str(row["last_trace_id"] or "") or None),
            }
        finally:
            conn.close()

    def _finish_upload_job(self, *, item_id: int) -> None:
        conn = get_queue_conn()
        try:
            conn.execute(
                "UPDATE upload_item_jobs SET status='done', finished_at_utc=datetime('now'), last_error=NULL WHERE item_id=?",
                (int(item_id),),
            )
            conn.commit()
        finally:
            conn.close()

    def _finish_derivative_job(self, *, image_id: int, processed_kinds: tuple[str, ...]) -> None:
        processed = set(processed_kinds)
        conn = get_queue_conn()
        try:
            row = conn.execute(
                "SELECT need_grid, need_overlay FROM derivative_jobs WHERE image_id=?",
                (int(image_id),),
            ).fetchone()
            if not row:
                return
            need_grid = int(row["need_grid"] or 0)
            need_overlay = int(row["need_overlay"] or 0)
            if "grid" in processed:
                need_grid = 0
            if "overlay" in processed:
                need_overlay = 0
            status = "queued" if (need_grid or need_overlay) else "done"
            conn.execute(
                """
                UPDATE derivative_jobs
                SET need_grid=?, need_overlay=?, status=?, finished_at_utc=datetime('now'), last_error=NULL
                WHERE image_id=?
                """,
                (int(need_grid), int(need_overlay), status, int(image_id)),
            )
            conn.commit()
        finally:
            conn.close()
        if need_grid or need_overlay:
            self._wake.set()

    def _fail_upload_job(self, *, item_id: int, error: Exception) -> None:
        self._fail_generic(table="upload_item_jobs", key_col="item_id", key_value=int(item_id), error=error)

    def _fail_derivative_job(self, *, image_id: int, error: Exception) -> None:
        self._fail_generic(table="derivative_jobs", key_col="image_id", key_value=int(image_id), error=error)

    def _fail_generic(self, *, table: str, key_col: str, key_value: int, error: Exception) -> None:
        message = str(error or "")
        is_locked = "database is locked" in message.lower()
        conn = get_queue_conn()
        try:
            row = conn.execute(f"SELECT retry_count FROM {table} WHERE {key_col}=?", (int(key_value),)).fetchone()
            retry_count = int((row["retry_count"] if row else 0) or 0) + 1
            should_retry = is_locked and retry_count < 20
            conn.execute(
                f"UPDATE {table} SET status=?, retry_count=?, last_error=?, finished_at_utc=datetime('now') WHERE {key_col}=?",
                ("queued" if should_retry else "error", int(retry_count), f"{error.__class__.__name__}: {message}"[:1000], int(key_value)),
            )
            conn.commit()
        finally:
            conn.close()
        if should_retry:
            time.sleep(min(0.25 * retry_count, 1.0))
            self._wake.set()


_WORKER = ContentQueueWorker()


def enqueue_derivative_job(
    image_id: int,
    kinds: tuple[str, ...],
    *,
    source: str = "unknown",
    trace_id: str | None = None,
) -> bool:
    return _WORKER.enqueue_derivative(int(image_id), kinds, source=source, trace_id=trace_id)


def enqueue_upload_item_job(
    item_id: int,
    *,
    source: str = "unknown",
    trace_id: str | None = None,
) -> bool:
    return _WORKER.enqueue_upload_item(int(item_id), source=source, trace_id=trace_id)


def start_derivative_worker(upload_processor: UploadProcessor, derivative_processor: DerivativeProcessor) -> None:
    _WORKER.start(upload_processor, derivative_processor)


def stop_derivative_worker() -> None:
    _WORKER.stop()
