from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "server" / "data"
LOG_DIR = DATA_DIR / "logs"
PERF_LOG_PATH = LOG_DIR / "perf.log"

_LOGGER_NAME = "nim.perf"
_CONFIGURED = False


def _truthy(v: str | None, default: bool = True) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() not in {"0", "false", "off", "no", ""}


def perf_logging_enabled() -> bool:
    return _truthy(os.getenv("NAI_IM_PERF_LOG_ENABLED"), default=True)


def configure_perf_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return
    if not perf_logging_enabled():
        _CONFIGURED = True
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        max_bytes = int(os.getenv("NAI_IM_PERF_LOG_MAX_BYTES", str(5 * 1024 * 1024)) or (5 * 1024 * 1024))
        backup_count = int(os.getenv("NAI_IM_PERF_LOG_BACKUP_COUNT", "5") or "5")
        handler = RotatingFileHandler(
            PERF_LOG_PATH,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
    _CONFIGURED = True


def perf_log_path() -> str:
    return str(PERF_LOG_PATH)


def new_trace_id() -> str:
    return uuid.uuid4().hex[:16]


def log_perf(event: str, /, **fields: Any) -> None:
    if not perf_logging_enabled():
        return
    configure_perf_logging()
    payload = {
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event": event,
        **fields,
    }
    try:
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        safe_fields: dict[str, Any] = {}
        for k, v in fields.items():
            try:
                json.dumps(v, ensure_ascii=False)
                safe_fields[k] = v
            except Exception:
                safe_fields[k] = repr(v)
        payload = {
            "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "event": event,
            **safe_fields,
        }
        text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    logging.getLogger(_LOGGER_NAME).info(text)
