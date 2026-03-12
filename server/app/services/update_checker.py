from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from ..db import get_conn

ROOT_DIR = Path(__file__).resolve().parents[3]
VERSION_FILE = ROOT_DIR / "VERSION"
STATUS_KV_KEY = "app_update_status_json"
DEFAULT_REPO = "retroaegx/NImageManager"
DEFAULT_INTERVAL_HOURS = 24.0
DEFAULT_TIMEOUT_SEC = 5.0
SEMVER_RE = re.compile(
    r"^v?(?P<core>\d+(?:\.\d+){0,3})(?:-(?P<prerelease>[0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _safe_float(value: str | int | float | None, default: float) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except Exception:
        return float(default)


def _normalize_repo(raw: str | None) -> str:
    text = str(raw or DEFAULT_REPO).strip().strip("/")
    if text.endswith(".git"):
        text = text[:-4]
    lowered = text.lower()
    if lowered.startswith("https://github.com/"):
        text = text[19:]
    elif lowered.startswith("http://github.com/"):
        text = text[18:]
    text = text.strip().strip("/")
    if text.count("/") != 1:
        return DEFAULT_REPO
    owner, repo = text.split("/", 1)
    owner = owner.strip()
    repo = repo.strip()
    if not owner or not repo:
        return DEFAULT_REPO
    return f"{owner}/{repo}"


def _release_page_url(repo: str) -> str:
    return f"https://github.com/{repo}/releases"


def read_current_version() -> str:
    try:
        raw = VERSION_FILE.read_text(encoding="utf-8", errors="ignore").strip()
    except Exception:
        raw = ""
    return raw or "0.0.0"


def _parse_semver_token(token: str) -> tuple[int, int | str]:
    if token.isdigit():
        return (0, int(token))
    return (1, token.lower())


def _parse_semver(value: str | None) -> tuple[tuple[int, ...], tuple[tuple[int, int | str], ...] | None] | None:
    text = str(value or "").strip()
    if not text:
        return None
    m = SEMVER_RE.match(text)
    if not m:
        return None
    core = tuple(int(part) for part in (m.group("core") or "0").split("."))
    pre = m.group("prerelease")
    if not pre:
        return (core, None)
    return (core, tuple(_parse_semver_token(tok) for tok in pre.split(".")))


def compare_versions(current: str | None, latest: str | None) -> int:
    a = _parse_semver(current)
    b = _parse_semver(latest)
    if a is None and b is None:
        sa = str(current or "")
        sb = str(latest or "")
        if sa == sb:
            return 0
        return -1 if sa < sb else 1
    if a is None:
        return -1
    if b is None:
        return 1

    a_core, a_pre = a
    b_core, b_pre = b
    width = max(len(a_core), len(b_core))
    a_core = a_core + (0,) * (width - len(a_core))
    b_core = b_core + (0,) * (width - len(b_core))
    if a_core < b_core:
        return -1
    if a_core > b_core:
        return 1

    if a_pre is None and b_pre is None:
        return 0
    if a_pre is None:
        return 1
    if b_pre is None:
        return -1

    width = max(len(a_pre), len(b_pre))
    for idx in range(width):
        if idx >= len(a_pre):
            return -1
        if idx >= len(b_pre):
            return 1
        left = a_pre[idx]
        right = b_pre[idx]
        if left == right:
            continue
        return -1 if left < right else 1
    return 0


def _env_enabled() -> bool:
    raw = (os.getenv("NAI_IM_UPDATE_CHECK_ENABLED") or "1").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _interval_hours() -> float:
    hours = _safe_float(os.getenv("NAI_IM_UPDATE_CHECK_INTERVAL_HOURS"), DEFAULT_INTERVAL_HOURS)
    return max(1.0, min(hours, 24.0 * 30.0))


def _configured_repo() -> str:
    return _normalize_repo(os.getenv("NAI_IM_UPDATE_REPO") or DEFAULT_REPO)


def _build_state(*, enabled: bool | None = None, repo: str | None = None) -> dict[str, Any]:
    repo_name = _normalize_repo(repo)
    release_page_url = _release_page_url(repo_name)
    return {
        "enabled": _env_enabled() if enabled is None else bool(enabled),
        "repo": repo_name,
        "release_page_url": release_page_url,
        "current_version": read_current_version(),
        "latest_version": None,
        "release_name": None,
        "release_url": release_page_url,
        "published_at": None,
        "checked_at": None,
        "update_available": False,
        "error": None,
        "interval_hours": _interval_hours(),
    }


def _read_cached_state() -> dict[str, Any] | None:
    conn = get_conn()
    try:
        row = conn.execute("SELECT value FROM admin_kv WHERE key=?", (STATUS_KV_KEY,)).fetchone()
        if not row:
            return None
        raw = row[0] if isinstance(row, tuple) else row["value"]
        if not raw:
            return None
        data = json.loads(str(raw))
        if isinstance(data, dict):
            return data
        return None
    except Exception:
        return None
    finally:
        conn.close()


def _write_cached_state(state: dict[str, Any]) -> None:
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True)
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO admin_kv(key, value, updated_at)
            VALUES (?,?,datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
              value=excluded.value,
              updated_at=datetime('now')
            """,
            (STATUS_KV_KEY, payload),
        )
        conn.commit()
    except Exception:
        pass
    finally:
        conn.close()


def _request_latest_release(repo: str) -> dict[str, Any]:
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases/latest",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "NImageManager update checker",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        method="GET",
    )
    with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT_SEC) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        payload = resp.read().decode(charset, errors="replace")
    data = json.loads(payload)
    if not isinstance(data, dict):
        raise RuntimeError("invalid release response")
    return data


class UpdateCheckerService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = _build_state()

    def start(self) -> None:
        repo = _configured_repo()
        enabled = _env_enabled()
        cached = _read_cached_state() or {}
        base = _build_state(enabled=enabled, repo=repo)
        merged = {**cached, **base}
        merged["repo"] = repo
        merged["release_page_url"] = _release_page_url(repo)
        merged["interval_hours"] = _interval_hours()
        merged["current_version"] = read_current_version()
        merged["enabled"] = enabled
        if not merged.get("release_url"):
            merged["release_url"] = merged["release_page_url"]
        with self._lock:
            self._state = merged
        self._persist_current_state()

        if not enabled:
            return

        self.check_now(reason="startup")

        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="nim-update-checker", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop.set()
        if thread and thread.is_alive():
            thread.join(timeout=2.0)

    def get_state(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._state)

    def check_now(self, *, reason: str = "manual") -> dict[str, Any]:
        repo = _configured_repo()
        state = _build_state(enabled=_env_enabled(), repo=repo)
        with self._lock:
            state.update({
                "checked_at": self._state.get("checked_at"),
                "latest_version": self._state.get("latest_version"),
                "release_name": self._state.get("release_name"),
                "release_url": self._state.get("release_url") or state["release_page_url"],
                "published_at": self._state.get("published_at"),
                "update_available": bool(self._state.get("update_available")),
                "error": self._state.get("error"),
            })
        state["current_version"] = read_current_version()
        state["checked_at"] = _utc_now_iso()
        state["interval_hours"] = _interval_hours()

        if not state["enabled"]:
            state["error"] = None
            state["update_available"] = False
            self._replace_state(state)
            return state

        try:
            release = _request_latest_release(repo)
            latest_version = str(release.get("tag_name") or "").strip() or None
            state["latest_version"] = latest_version
            state["release_name"] = str(release.get("name") or "").strip() or latest_version
            state["release_url"] = str(release.get("html_url") or state["release_page_url"]).strip() or state["release_page_url"]
            state["published_at"] = str(release.get("published_at") or "").strip() or None
            state["error"] = None
            state["update_available"] = bool(latest_version) and compare_versions(state["current_version"], latest_version) < 0
        except urllib.error.HTTPError as exc:
            state["update_available"] = False
            state["release_url"] = state["release_page_url"]
            if exc.code == 404:
                state["error"] = "GitHub release がまだ作成されていません"
            elif exc.code in {403, 429}:
                state["error"] = f"GitHub API rate limit ({exc.code})"
            else:
                state["error"] = f"GitHub API error ({exc.code})"
        except Exception as exc:
            state["update_available"] = False
            state["release_url"] = state["release_page_url"]
            state["error"] = f"update check failed: {exc.__class__.__name__}"

        self._replace_state(state)
        return state

    def _replace_state(self, state: dict[str, Any]) -> None:
        with self._lock:
            self._state = dict(state)
        self._persist_current_state()

    def _persist_current_state(self) -> None:
        with self._lock:
            snapshot = dict(self._state)
        _write_cached_state(snapshot)

    def _run(self) -> None:
        while not self._stop.wait(timeout=max(60.0, _interval_hours() * 3600.0)):
            try:
                self.check_now(reason="interval")
            except Exception:
                # Keep worker alive; state persistence already handles errors.
                pass


_SERVICE = UpdateCheckerService()


def start_update_checker() -> None:
    _SERVICE.start()


def stop_update_checker() -> None:
    _SERVICE.stop()


def get_update_status() -> dict[str, Any]:
    return _SERVICE.get_state()


def check_update_now() -> dict[str, Any]:
    return _SERVICE.check_now(reason="manual")
