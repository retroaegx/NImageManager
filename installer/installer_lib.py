from __future__ import annotations

import hashlib
import json
import os
import platform as py_platform
from pathlib import Path
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
import tarfile


# ---- Cloudflare named tunnel discovery (best-effort) ----
#
# If the user already runs a named tunnel that routes to the local app port,
# prefer showing that hostname (stable URL) and DO NOT start a Quick Tunnel.
#
# We intentionally avoid adding a YAML dependency; we only need to parse
# a small subset of cloudflared's config (ingress hostname/service pairs).

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
    # common forms:
    #  - http://127.0.0.1:32287
    #  - http://localhost:32287
    #  - localhost:32287
    #  - 127.0.0.1:32287
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
            # handle inline kv: "- hostname: ..."
            after = s[1:].strip()
            m = _KV_RE.match(after)
            if m:
                cur[m.group(1).lower()] = _strip_yaml_scalar(m.group(2))
            continue

        m = _KV_RE.match(s)
        if not m:
            continue
        cur[m.group(1).lower()] = _strip_yaml_scalar(m.group(2))

    hit = flush_current()
    return hit


def _candidate_cloudflared_config_paths() -> list[Path]:
    """cloudflared default config search locations (plus cwd)."""
    paths: list[Path] = []
    # explicit config path override
    cfg_env = (os.environ.get("NAI_IM_CLOUDFLARED_CONFIG") or "").strip()
    if cfg_env:
        paths.append(Path(cfg_env))

    for name in ("config.yml", "config.yaml"):
        paths.append(Path.cwd() / name)

    home = Path.home()
    for d in (home / ".cloudflared", home / ".cloudflare-warp", home / "cloudflare-warp"):
        for name in ("config.yml", "config.yaml"):
            paths.append(d / name)

    # common Windows fallback
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


def detect_named_tunnel_public_url(port: int) -> str | None:
    """Best-effort discovery of a stable public URL from an existing named tunnel.

    If a cloudflared config contains an ingress rule routing to this app port,
    return the corresponding https URL ("https://<hostname>").
    """
    # explicit override always wins
    env = (os.environ.get("NAI_IM_PUBLIC_BASE_URL") or "").strip()
    if env:
        return env.rstrip("/")

    for cfg in _candidate_cloudflared_config_paths():
        try:
            if not cfg.exists() or cfg.stat().st_size <= 0:
                continue
            txt = cfg.read_text(encoding="utf-8", errors="ignore")
            host = _parse_cloudflared_ingress_hostname(txt, port)
            if not host:
                continue
            # Cloudflare public hostnames are served over https.
            return f"https://{host}".rstrip("/")
        except Exception:
            continue
    return None

def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]

def load_manifest(root: Path) -> dict:
    p = root / "installer" / "manifest.json"
    return json.loads(p.read_text(encoding="utf-8"))

def _server_data_dir(root: Path) -> Path:
    return root / "server" / "data"

def _db_path(root: Path) -> Path:
    return _server_data_dir(root) / "app.db"

def _log_installer_event(root: Path, *, kind: str, payload: dict) -> None:
    db_path = _db_path(root)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS installer_events (
              id INTEGER PRIMARY KEY,
              created_at TEXT NOT NULL DEFAULT (datetime('now')),
              kind TEXT NOT NULL,
              payload_json TEXT NOT NULL
            );
            """
        )
        conn.execute(
            "INSERT INTO installer_events(kind, payload_json) VALUES (?, ?)",
            (kind, json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()

def _download(url: str, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    with urllib.request.urlopen(url) as r, tmp.open("wb") as f:
        shutil.copyfileobj(r, f)
    tmp.replace(dst)

def _platform_key() -> str:
    os_name = "windows" if os.name == "nt" else ("darwin" if sys_platform().startswith("darwin") else "linux")
    arch = py_platform.machine().lower()
    if arch in {"x86_64", "amd64"}:
        arch = "amd64"
    elif arch in {"aarch64", "arm64"}:
        arch = "arm64"
    return f"{os_name}_{arch}"

def sys_platform() -> str:
    return py_platform.system().lower()

def ensure_cloudflared(root: Path) -> Path | None:
    """Return path to cloudflared binary, downloading it if needed."""
    manifest = load_manifest(root)
    urls = manifest.get("cloudflared") or {}
    url = urls.get(f"{_platform_key()}_url")
    if not url:
        print("[installer] cloudflared: no download url for this platform; Quick Tunnel disabled")
        return None

    bin_dir = root / "tools" / "cloudflared"
    bin_dir.mkdir(parents=True, exist_ok=True)
    exe_name = "cloudflared.exe" if os.name == "nt" else "cloudflared"
    dst = bin_dir / exe_name

    if dst.exists() and dst.stat().st_size > 0:
        return dst

    print("[installer] Downloading cloudflared ...")
    try:
        if url.endswith(".tgz"):
            tgz_path = dst.with_suffix(".tgz")
            _download(str(url), tgz_path)
            with tarfile.open(tgz_path, "r:gz") as tf:
                member = next((m for m in tf.getmembers() if m.name.endswith("cloudflared")), None)
                if not member:
                    raise RuntimeError("cloudflared not found in tgz")
                tf.extract(member, path=str(bin_dir))
                extracted = bin_dir / member.name
                extracted.replace(dst)
            try:
                tgz_path.unlink()
            except Exception:
                pass
        else:
            _download(str(url), dst)

        if os.name != "nt":
            dst.chmod(dst.stat().st_mode | 0o111)

        _log_installer_event(root, kind="download", payload={"asset": "cloudflared", "url": url, "path": str(dst)})
        return dst
    except Exception as exc:
        print(f"[installer] cloudflared download failed: {exc}")
        _log_installer_event(root, kind="download_error", payload={"asset": "cloudflared", "url": url, "error": str(exc)})
        return None

_URL_RE = re.compile(r"(https://[a-z0-9\-]+\.trycloudflare\.com)", re.IGNORECASE)

def _read_tunnel_url(proc: subprocess.Popen, *, timeout_s: float, tee_path: Path | None) -> str | None:
    deadline = time.time() + timeout_s
    buf = []
    if tee_path:
        tee_path.parent.mkdir(parents=True, exist_ok=True)
        ftee = tee_path.open("w", encoding="utf-8", errors="replace")
    else:
        ftee = None
    try:
        while time.time() < deadline:
            line = proc.stdout.readline() if proc.stdout else ""
            if not line:
                if proc.poll() is not None:
                    break
                time.sleep(0.05)
                continue
            if ftee:
                ftee.write(line)
                ftee.flush()
            buf.append(line)
            m = _URL_RE.search(line)
            if m:
                url = m.group(1)
                # cloudflared may print a helper API URL; ignore it and keep reading.
                if "api.trycloudflare.com" in url.lower():
                    continue
                return url
        return None
    finally:
        if ftee:
            ftee.close()

def run_quick_tunnel(cloudflared: Path, port: int) -> tuple[subprocess.Popen | None, str | None]:
    """Start Cloudflare Quick Tunnel and extract URL (best-effort)."""

    def _start(args: list[str]) -> subprocess.Popen:
        tmp_home = Path(tempfile.mkdtemp(prefix="nai_cf_"))
        env = os.environ.copy()
        env["HOME"] = str(tmp_home)
        env["USERPROFILE"] = str(tmp_home)
        env["NO_COLOR"] = "1"
        return subprocess.Popen(
            [str(cloudflared), *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=str(tmp_home),
            env=env,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )

    base_args = ["tunnel", "--no-autoupdate", "--url", f"http://127.0.0.1:{port}"]
    log_dir = repo_root() / "server" / "data"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "cloudflared_quick_tunnel.log"

    proc = _start(base_args)
    url = _read_tunnel_url(proc, timeout_s=60.0, tee_path=log_path)
    if url:
        return proc, url

    try:
        proc.terminate()
    except Exception:
        pass

    proc2 = _start(["tunnel", "--no-autoupdate", "--protocol", "http2", "--url", f"http://127.0.0.1:{port}"])
    url2 = _read_tunnel_url(proc2, timeout_s=60.0, tee_path=log_path)
    return proc2, url2
