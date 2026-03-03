from __future__ import annotations

import os
import shutil
from pathlib import Path
import subprocess
import sys
import time
import venv

from installer_lib import ensure_cloudflared, repo_root, run_quick_tunnel, detect_named_tunnel_public_url

PORT = 32287
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
            # strip simple quotes
            if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
                v = v[1:-1]
            out[k] = v
    except Exception:
        return out
    return out


def _ensure_dotenv(root: Path) -> None:
    """Ensure .env exists; load its values into os.environ (without overriding existing env vars)."""
    env_path = root / ".env"
    tpl_path = root / ".env.template"
    if (not env_path.exists()) and tpl_path.exists():
        try:
            shutil.copyfile(str(tpl_path), str(env_path))
            print("[installer] created .env from .env.template")
        except Exception:
            # Non-fatal; continue without .env
            pass

    if env_path.exists() and env_path.stat().st_size > 0:
        for k, v in _parse_dotenv(env_path).items():
            if k not in os.environ:
                os.environ[k] = v



def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"

def _ensure_venv(root: Path) -> Path:
    venv_dir = root / ".venv"
    py = _venv_python(venv_dir)
    if py.exists():
        return py
    print(f"[installer] Creating virtual environment: {venv_dir}")
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
    print("[installer] Installing python requirements ...")
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
    if os.environ.get("NAI_IM_RELOAD", "").strip().lower() in {"1","true","yes","on"}:
        cmd.append("--reload")
    print(f"[installer] Starting server: http://localhost:{PORT}")
    return subprocess.Popen(cmd, cwd=str(root))

def main() -> int:
    root = repo_root()
    _ensure_dotenv(root)
    py = _ensure_venv(root)
    if not _is_running_in_venv(root):
        return _rerun_in_venv(py)

    if not _pip_install(root):
        print("[installer] FATAL: pip install failed")
        return 2

    cloudflared = ensure_cloudflared(root)

    server = _run_server(root)
    time.sleep(0.4)

    tunnel_flag = (os.environ.get("NAI_IM_TUNNEL") or "1").strip().lower()
    want_tunnel = tunnel_flag not in {"0","false","no","off"}
    tunnel_proc = None
    public_url = None
    if want_tunnel:
        # Prefer an existing named tunnel (stable hostname) if it already routes to this port.
        public_url = detect_named_tunnel_public_url(PORT)
        if (not public_url) and cloudflared:
            tunnel_proc, public_url = run_quick_tunnel(cloudflared, PORT)

    if public_url and "api.trycloudflare.com" in public_url.lower():
        public_url = None

    print("\n[installer] URLs")
    print(f"  local : http://localhost:{PORT}")
    if public_url:
        print(f"  public: {public_url}")
    elif want_tunnel:
        print("  public: (failed)  ※ server/data/cloudflared_quick_tunnel.log を確認してください")

    try:
        rc = server.wait()
        if tunnel_proc and tunnel_proc.poll() is None:
            tunnel_proc.terminate()
        return int(rc or 0)
    except KeyboardInterrupt:
        print("\n[installer] stopping...")
        server.terminate()
        if tunnel_proc and tunnel_proc.poll() is None:
            tunnel_proc.terminate()
        return 0

if __name__ == "__main__":
    raise SystemExit(main())
