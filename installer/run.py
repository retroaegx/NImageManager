from __future__ import annotations

import ctypes
import os
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
import venv

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from installer_lib import ensure_cloudflared, repo_root, run_quick_tunnel, detect_named_tunnel_public_url
from shared.dotenv_utils import ensure_dotenv

PORT = 32287
_STARTUP_WAIT_TIMEOUT_S = 180.0
_STARTUP_POLL_INTERVAL_S = 0.25
_PROGRESS_LABEL_WIDTH = 24

_LOCAL_URL_COLOR = "\033[38;5;114m"
_PUBLIC_URL_COLOR = "\033[38;5;221m"
_RESET_COLOR = "\033[0m"
_ANSI_ENABLED: bool | None = None


def _enable_windows_ansi() -> bool:
    if os.name != "nt":
        return True
    try:
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        if handle in (0, -1):
            return False
        mode = ctypes.c_uint()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)) == 0:
            return False
        enable_virtual_terminal_processing = 0x0004
        new_mode = mode.value | enable_virtual_terminal_processing
        if kernel32.SetConsoleMode(handle, new_mode) == 0:
            return False
        return True
    except Exception:
        return False


def _supports_ansi() -> bool:
    global _ANSI_ENABLED
    if _ANSI_ENABLED is not None:
        return _ANSI_ENABLED
    if os.environ.get("NO_COLOR"):
        _ANSI_ENABLED = False
        return False
    stream = sys.stdout
    if not getattr(stream, "isatty", lambda: False)():
        _ANSI_ENABLED = False
        return False
    term = (os.environ.get("TERM") or "").strip().lower()
    if term == "dumb":
        _ANSI_ENABLED = False
        return False
    _ANSI_ENABLED = _enable_windows_ansi()
    return _ANSI_ENABLED


def _colorize(text: str, color: str) -> str:
    if not _supports_ansi():
        return text
    return f"{color}{text}{_RESET_COLOR}"


def _progress(step: str, message: str) -> None:
    print(f"[installer] {step:<{_PROGRESS_LABEL_WIDTH}} {message}", flush=True)


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _ensure_venv(root: Path) -> Path:
    venv_dir = root / ".venv"
    py = _venv_python(venv_dir)
    if py.exists():
        return py
    _progress("venv", f"creating virtual environment: {venv_dir}")
    venv.EnvBuilder(with_pip=True).create(venv_dir)
    return py


def _is_running_in_venv(root: Path) -> bool:
    venv_dir = root / ".venv"
    py = _venv_python(venv_dir)
    try:
        return Path(sys.executable).resolve() == py.resolve()
    except Exception:
        return False


def _rerun_in_venv(py: Path) -> int:
    args = [str(py), str(Path(__file__).resolve())] + sys.argv[1:]
    return subprocess.call(args, cwd=str(repo_root()))


def _pip_install(root: Path) -> bool:
    req = root / "requirements.txt"
    cmd = [sys.executable, "-m", "pip", "install", "-r", str(req)]
    _progress("dependencies", "installing Python requirements ...")
    return subprocess.call(cmd, cwd=str(root)) == 0


def _run_server(root: Path) -> subprocess.Popen:
    cmd = [
        sys.executable, "-m", "uvicorn",
        "server.app.main:app",
        "--host", "127.0.0.1",
        "--port", str(PORT),
        "--log-level", (os.environ.get("NAI_IM_SERVER_LOGLEVEL") or "warning"),
        "--no-access-log",
        "--no-use-colors",
    ]
    if os.environ.get("NAI_IM_RELOAD", "").strip().lower() in {"1", "true", "yes", "on"}:
        cmd.append("--reload")
    _progress("server", f"launching uvicorn on http://localhost:{PORT}")
    return subprocess.Popen(cmd, cwd=str(root))


def _wait_for_local_server_ready(port: int, proc: subprocess.Popen, *, timeout_s: float = _STARTUP_WAIT_TIMEOUT_S) -> bool:
    url = f"http://127.0.0.1:{port}/login.html"
    deadline = time.monotonic() + max(1.0, float(timeout_s))
    next_report = time.monotonic()
    started = time.monotonic()
    last_error = "starting"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            code = proc.returncode if proc.returncode is not None else "unknown"
            _progress("server", f"process exited before ready (code={code})")
            return False
        try:
            req = urllib.request.Request(url, method="GET")
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                status = int(getattr(resp, "status", 200) or 200)
                if 200 <= status < 500:
                    elapsed = time.monotonic() - started
                    _progress("server", f"application ready after {elapsed:.1f}s")
                    return True
                last_error = f"HTTP {status}"
        except urllib.error.HTTPError as exc:
            status = int(getattr(exc, "code", 0) or 0)
            if 200 <= status < 500:
                elapsed = time.monotonic() - started
                _progress("server", f"application ready after {elapsed:.1f}s")
                return True
            last_error = f"HTTP {status}"
        except Exception as exc:
            last_error = str(exc) or exc.__class__.__name__
        now = time.monotonic()
        if now >= next_report:
            elapsed = now - started
            _progress("server", f"waiting for application startup ... {elapsed:.1f}s ({last_error})")
            next_report = now + 1.0
        time.sleep(_STARTUP_POLL_INTERVAL_S)
    _progress("server", f"startup timeout after {timeout_s:.0f}s ({last_error})")
    return False


def main() -> int:
    root = repo_root()
    _progress("startup", "initializing launcher")
    ensure_dotenv(root, log_prefix="[installer]")
    py = _ensure_venv(root)
    if not _is_running_in_venv(root):
        _progress("venv", "restarting inside project virtual environment")
        return _rerun_in_venv(py)

    if not _pip_install(root):
        print("[installer] FATAL: pip install failed", flush=True)
        return 2

    tunnel_flag = (os.environ.get("NAI_IM_TUNNEL") or "1").strip().lower()
    want_tunnel = tunnel_flag not in {"0", "false", "no", "off"}

    cloudflared = None
    if want_tunnel:
        _progress("tunnel", "preparing Cloudflare tunnel support")
        cloudflared = ensure_cloudflared(root)
    else:
        _progress("tunnel", "disabled by NAI_IM_TUNNEL")

    server = _run_server(root)
    if not _wait_for_local_server_ready(PORT, server):
        try:
            rc = server.wait(timeout=3)
            return int(rc or 1)
        except subprocess.TimeoutExpired:
            server.terminate()
            return 1

    tunnel_proc = None
    public_url = None
    if want_tunnel:
        _progress("tunnel", "checking named tunnel / quick tunnel")
        public_url = detect_named_tunnel_public_url(PORT)
        if public_url:
            _progress("tunnel", "named tunnel detected")
        elif cloudflared:
            _progress("tunnel", "starting quick tunnel")
            tunnel_proc, public_url = run_quick_tunnel(cloudflared, PORT)
        else:
            _progress("tunnel", "cloudflared unavailable; public URL disabled")

    if public_url and "api.trycloudflare.com" in public_url.lower():
        public_url = None

    print("\n[installer] startup complete", flush=True)
    print("[installer] URLs", flush=True)
    local_line = f"  local : http://localhost:{PORT}"
    print(_colorize(local_line, _LOCAL_URL_COLOR), flush=True)
    if public_url:
        public_line = f"  public: {public_url}"
        print(_colorize(public_line, _PUBLIC_URL_COLOR), flush=True)
    elif want_tunnel:
        public_line = "  public: (failed)  ※ server/data/cloudflared_quick_tunnel.log を確認してください"
        print(_colorize(public_line, _PUBLIC_URL_COLOR), flush=True)

    try:
        rc = server.wait()
        if tunnel_proc and tunnel_proc.poll() is None:
            tunnel_proc.terminate()
        return int(rc or 0)
    except KeyboardInterrupt:
        print("\n[installer] stopping...", flush=True)
        server.terminate()
        if tunnel_proc and tunnel_proc.poll() is None:
            tunnel_proc.terminate()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
