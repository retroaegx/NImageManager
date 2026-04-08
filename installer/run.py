from __future__ import annotations

import ctypes
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from installer_lib import ensure_cloudflared, repo_root, run_quick_tunnel, detect_named_tunnel_public_url
from shared.dotenv_utils import ensure_dotenv

PORT = 32287
_STARTUP_WAIT_TIMEOUT_S = 180.0
_STARTUP_POLL_INTERVAL_S = 0.25
_PROGRESS_LABEL_WIDTH = 24
_SUPPORTED_PYTHON_VERSIONS: tuple[tuple[int, int], ...] = ((3, 13), (3, 12))
_SUPPORTED_PYTHON_VERSION_SET = set(_SUPPORTED_PYTHON_VERSIONS)
_PYTHON_VERSION_PROBE_CODE = "import sys; print(f'{sys.version_info[0]}.{sys.version_info[1]}')"

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


def _version_text(version: tuple[int, int] | None) -> str:
    if not version:
        return "unknown"
    return f"{version[0]}.{version[1]}"


def _is_supported_python_version(version: tuple[int, int] | None) -> bool:
    return version in _SUPPORTED_PYTHON_VERSION_SET


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _probe_python_command(cmd: list[str]) -> tuple[int, int] | None:
    try:
        completed = subprocess.run(
            cmd + ["-c", _PYTHON_VERSION_PROBE_CODE],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError, subprocess.SubprocessError):
        return None

    output = (completed.stdout or "").strip()
    if not output:
        return None

    head = output.splitlines()[-1].strip()
    parts = head.split(".")
    if len(parts) < 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def _python_command_candidates() -> list[tuple[str, list[str]]]:
    candidates: list[tuple[str, list[str]]] = []
    seen: set[tuple[str, ...]] = set()

    def add(label: str, cmd: list[str]) -> None:
        key = tuple(cmd)
        if key in seen:
            return
        seen.add(key)
        candidates.append((label, cmd))

    add(f"current interpreter ({sys.executable})", [sys.executable])

    if os.name == "nt":
        add("Windows launcher py -3.13", ["py", "-3.13"])
        add("Windows launcher py -3.12", ["py", "-3.12"])
        add("python3.13", ["python3.13"])
        add("python3.12", ["python3.12"])
    else:
        add("python3.13", ["python3.13"])
        add("python3.12", ["python3.12"])

    return candidates


def _select_supported_python_command() -> tuple[list[str], tuple[int, int], str] | None:
    candidates = _python_command_candidates()
    probed: list[tuple[tuple[int, int], str, list[str]]] = []

    current_version = _probe_python_command([sys.executable])
    if _is_supported_python_version(current_version):
        return [sys.executable], current_version, f"current interpreter ({sys.executable})"

    for label, cmd in candidates:
        version = _probe_python_command(cmd)
        if not _is_supported_python_version(version):
            continue
        probed.append((version, label, cmd))

    for wanted in _SUPPORTED_PYTHON_VERSIONS:
        for version, label, cmd in probed:
            if version == wanted:
                return cmd, version, label

    return None


def _print_missing_supported_python_error() -> None:
    current_version = (sys.version_info.major, sys.version_info.minor)
    print(
        f"[installer] FATAL: Python 3.12 または 3.13 が見つかりません。現在の起動 Python は {_version_text(current_version)} です。",
        flush=True,
    )
    print(
        "[installer] FATAL: このアプリは Python 3.12 / 3.13 を使って仮想環境を作成し、その後もそのバージョンで起動します。",
        flush=True,
    )
    print(
        "[installer] FATAL: Python 3.13 または 3.12 をインストールしてから、再度起動してください。",
        flush=True,
    )


def _print_active_unsupported_venv_error(version: tuple[int, int] | None) -> None:
    print(
        f"[installer] FATAL: 現在の .venv は未対応の Python {_version_text(version)} で起動されています。",
        flush=True,
    )
    print(
        "[installer] FATAL: プロジェクト直下の run.bat / run.sh から起動し直してください。必要なら .venv を削除して再実行してください。",
        flush=True,
    )


def _create_venv_with_python(root: Path, creator_cmd: list[str], creator_version: tuple[int, int], source_label: str) -> Path | None:
    venv_dir = root / ".venv"
    py = _venv_python(venv_dir)
    _progress(
        "venv",
        f"creating virtual environment with Python {_version_text(creator_version)} ({source_label})",
    )
    cmd = creator_cmd + ["-m", "venv", str(venv_dir)]
    if subprocess.call(cmd, cwd=str(root)) != 0:
        print("[installer] FATAL: virtual environment creation failed", flush=True)
        return None

    created_version = _probe_python_command([str(py)])
    if not _is_supported_python_version(created_version):
        print(
            f"[installer] FATAL: created virtual environment uses unsupported Python {_version_text(created_version)}",
            flush=True,
        )
        return None
    return py


def _ensure_venv(root: Path) -> Path | None:
    venv_dir = root / ".venv"
    py = _venv_python(venv_dir)
    existing_version = _probe_python_command([str(py)]) if py.exists() else None

    if _is_supported_python_version(existing_version):
        _progress("venv", f"using existing virtual environment (Python {_version_text(existing_version)})")
        return py

    selected = _select_supported_python_command()
    if selected is None:
        _print_missing_supported_python_error()
        return None

    creator_cmd, creator_version, source_label = selected

    if py.exists() or venv_dir.exists():
        if _is_running_in_venv(root):
            _print_active_unsupported_venv_error(existing_version)
            return None
        _progress(
            "venv",
            f"existing virtual environment uses unsupported Python {_version_text(existing_version)}; recreating",
        )
        try:
            shutil.rmtree(venv_dir, ignore_errors=False)
        except OSError as exc:
            print(f"[installer] FATAL: failed to remove existing virtual environment: {exc}", flush=True)
            return None

    return _create_venv_with_python(root, creator_cmd, creator_version, source_label)


def _is_running_in_venv(root: Path) -> bool:
    venv_dir = root / ".venv"
    try:
        if sys.prefix == getattr(sys, "base_prefix", sys.prefix):
            return False
        return Path(sys.prefix).resolve() == venv_dir.resolve()
    except Exception:
        return False


def _ensure_pip_available(py: Path, root: Path) -> bool:
    probe_cmd = [str(py), "-m", "pip", "--version"]
    if subprocess.call(probe_cmd, cwd=str(root)) == 0:
        return True

    _progress("dependencies", "pip is missing in the virtual environment; bootstrapping with ensurepip")
    ensurepip_cmd = [str(py), "-m", "ensurepip", "--upgrade"]
    if subprocess.call(ensurepip_cmd, cwd=str(root)) != 0:
        return False

    return subprocess.call(probe_cmd, cwd=str(root)) == 0


def _rerun_in_venv(py: Path) -> int:
    args = [str(py), str(Path(__file__).resolve())] + sys.argv[1:]
    return subprocess.call(args, cwd=str(repo_root()))


def _pip_install(root: Path, py: Path) -> bool:
    if not _ensure_pip_available(py, root):
        print("[installer] FATAL: pip is not available in the project virtual environment", flush=True)
        return False

    req = root / "requirements.txt"
    cmd = [str(py), "-m", "pip", "install", "-r", str(req)]
    _progress("dependencies", "installing Python requirements ...")
    return subprocess.call(cmd, cwd=str(root)) == 0


def _run_server(root: Path, py: Path) -> subprocess.Popen:
    cmd = [
        str(py), "-m", "uvicorn",
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
    if py is None:
        return 2
    if not _is_running_in_venv(root):
        _progress("venv", "restarting inside project virtual environment")
        return _rerun_in_venv(py)

    if not _pip_install(root, py):
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

    server = _run_server(root, py)
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
