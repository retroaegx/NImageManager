from __future__ import annotations

import os
import shutil
from pathlib import Path


def parse_dotenv(path: Path) -> dict[str, str]:
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


def _append_missing_keys(env_path: Path, missing_items: list[tuple[str, str]]) -> None:
    if not missing_items:
        return

    existing_text = ""
    if env_path.exists():
        try:
            existing_text = env_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            existing_text = ""

    blocks: list[str] = []
    if existing_text and not existing_text.endswith(("\n", "\r")):
        blocks.append("\n")

    blocks.append("\n# Added automatically from .env.template\n")
    for key, value in missing_items:
        blocks.append(f"{key}={value}\n")

    env_path.parent.mkdir(parents=True, exist_ok=True)
    with env_path.open("a", encoding="utf-8", newline="\n") as f:
        f.write("".join(blocks))


def ensure_dotenv(root: Path, *, log_prefix: str) -> dict[str, str]:
    env_path = root / ".env"
    tpl_path = root / ".env.template"

    if (not env_path.exists()) and tpl_path.exists():
        try:
            shutil.copyfile(str(tpl_path), str(env_path))
            print(f"{log_prefix} created .env from .env.template")
        except Exception:
            pass

    env_values = parse_dotenv(env_path) if env_path.exists() else {}
    template_values = parse_dotenv(tpl_path) if tpl_path.exists() else {}

    missing_items = [(key, value) for key, value in template_values.items() if key not in env_values]
    if missing_items:
        try:
            _append_missing_keys(env_path, missing_items)
            env_values.update(dict(missing_items))
            suffix = "y" if len(missing_items) == 1 else "ies"
            print(f"{log_prefix} appended {len(missing_items)} missing .env entr{suffix} from .env.template")
        except Exception:
            pass

    for key, value in env_values.items():
        if key not in os.environ:
            os.environ[key] = value

    return env_values
