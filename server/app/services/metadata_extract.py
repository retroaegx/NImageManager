from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

import re

from PIL import Image
from PIL import ExifTags

@dataclass
class NaiMeta:
    software: Optional[str]
    model: Optional[str]
    prompt: Optional[str]
    negative: Optional[str]
    character_prompt: Optional[str]
    params: Dict[str, Any]
    potion: Optional[Any]
    raw: Dict[str, Any]
    raw_json_str: Optional[str]


def _decode_maybe_bytes(val: Any) -> Any:
    if isinstance(val, (bytes, bytearray, memoryview)):
        b = bytes(val)
        try:
            s = b.decode("utf-8")
            return s.replace("\x00", "").strip()
        except Exception:
            try:
                return b.decode("latin-1").replace("\x00", "").strip()
            except Exception:
                return b
    return val


def _merge_exif(raw: Dict[str, Any], im: Image.Image) -> None:
    """Merge decoded EXIF tags into raw.

    WebP (and sometimes PNG converted from WebP) can store NovelAI metadata
    in EXIF tags.
    """
    try:
        exif = im.getexif()
    except Exception:
        exif = None
    if not exif:
        return

    for tag_id, val in exif.items():
        name = ExifTags.TAGS.get(tag_id, str(tag_id))
        v = _decode_maybe_bytes(val)
        raw.setdefault(f"exif_{tag_id}", v)
        raw.setdefault(name, v)


def _decode_exif_usercomment_bytes(val: Any) -> Optional[str]:
    """Decode EXIF UserComment bytes to text.

    NovelAI WebP exports often store a JSON wrapper in EXIF UserComment.
    The field is commonly prefixed with an encoding marker:
      - b"ASCII\0\0\0" + <utf-8 json>
      - b"UNICODE\0" + <utf-16 json>
    """
    if val is None:
        return None
    if not isinstance(val, (bytes, bytearray, memoryview)):
        if isinstance(val, str):
            s = val.replace("\x00", "").strip()
            return s or None
        return None

    b = bytes(val)
    try:
        if b.startswith(b"ASCII\x00\x00\x00"):
            return b[8:].decode("utf-8", errors="replace").strip() or None
        if b.startswith(b"UNICODE\x00"):
            return b[8:].decode("utf-16", errors="replace").strip() or None
        if b.startswith(b"JIS\x00\x00\x00\x00\x00"):
            return b[8:].decode("shift_jis", errors="replace").strip() or None
        return b.decode("utf-8", errors="replace").replace("\x00", "").strip() or None
    except Exception:
        return None


def _parse_novelai_usercomment_wrapper(text: str) -> Optional[dict]:
    """Parse NovelAI's EXIF UserComment wrapper JSON.

    Typical structure:
      {"Comment": "{...json...}", "Software": "...", "Source": "...", ...}
    """
    if not text:
        return None
    s = text.strip()
    got = _try_parse_json_anywhere(s)
    if got is None:
        return None
    obj, _raw = got
    return obj if isinstance(obj, dict) else None


def _parse_json_str_maybe(val: Any) -> Optional[dict]:
    if not isinstance(val, str):
        return None
    s = val.strip()
    if not s:
        return None
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        obj = json.loads(s)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def _source_name_without_hash(source_like: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Return (source_name, model_hash) from a Source/Software-like string.

    Policy:
    - Do NOT normalize/replace names ("置換なし").
    - Keep the original string (trim whitespace only).
    - Strip a trailing 8-hex model hash token when present.

    Example:
      "NovelAI Diffusion V4.5 1229B44F" -> ("NovelAI Diffusion V4.5", "1229B44F")
    """
    s = (source_like or "").strip()
    if not s:
        return None, None

    # Prefer a trailing 8-hex token as the model hash.
    m = re.search(r"\b([0-9A-Fa-f]{8})\b\s*$", s)
    if m:
        model = m.group(1).upper()
        name = s[: m.start(1)].rstrip()
        return (name or None), model

    # Fallback: expose an 8-hex token if present anywhere.
    try:
        tokens = re.findall(r"\b[0-9A-Fa-f]{8}\b", s)
        model = tokens[-1].upper() if tokens else None
    except Exception:
        model = None
    return s, model


def _try_parse_json_anywhere(val: Any) -> Optional[tuple[dict, str]]:
    """Try to parse JSON even if surrounded by other text."""
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray, memoryview)):
        try:
            val = bytes(val).decode("utf-8", errors="ignore")
        except Exception:
            return None
    if not isinstance(val, str):
        return None

    s = val.strip()
    if not s:
        return None

    got = _try_parse_json(s)
    if got is not None:
        return got

    a = s.find("{")
    b = s.rfind("}")
    if a == -1 or b == -1 or b <= a:
        return None
    inner = s[a : b + 1].strip()
    if not (inner.startswith("{") and inner.endswith("}")):
        return None
    try:
        obj = json.loads(inner)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    return obj, inner


def _split_prompt_sections(text: str) -> tuple[str | None, str | None, str | None]:
    """Split free-form prompt into (positive, negative, character)."""
    t = (text or "").strip()
    if not t:
        return None, None, None

    lines = [ln.strip() for ln in t.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    if len(lines) == 1:
        return t, None, None

    section = "pos"
    pos: list[str] = []
    neg: list[str] = []
    ch: list[str] = []
    for ln in lines:
        if not ln:
            continue
        lnl = ln.lower()
        if lnl.startswith("character prompt") or lnl.startswith("character_prompt"):
            section = "char"
            continue
        if lnl.startswith("undesired content") or lnl.startswith("undesired_content"):
            section = "neg"
            continue
        if lnl.startswith("negative prompt"):
            section = "neg"
            continue
        if lnl == "uc" or lnl.startswith("uc:") or lnl.startswith("uc "):
            section = "neg"
            ln = ln.split(":", 1)[1].strip() if ":" in ln else ""

        if section == "pos":
            pos.append(ln)
        elif section == "neg":
            neg.append(ln)
        else:
            ch.append(ln)

    return (
        " ".join(pos).strip() or None,
        " ".join(neg).strip() or None,
        " ".join(ch).strip() or None,
    )

def _try_parse_json(val: Any) -> Optional[tuple[dict, str]]:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            val = val.decode("utf-8", errors="ignore")
        except Exception:
            return None
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        if not (s.startswith("{") and s.endswith("}")):
            return None
        try:
            return json.loads(s), s
        except Exception:
            return None
    return None

def extract_novelai_metadata(path: str | Path) -> NaiMeta:
    path = Path(path)
    raw: Dict[str, Any] = {}

    # NovelAI WebP exports often store metadata in EXIF UserComment.
    usercomment_wrapper: Optional[dict] = None
    usercomment_comment_payload: Optional[dict] = None
    usercomment_comment_raw: Optional[str] = None
    usercomment_source_raw: Optional[str] = None

    with Image.open(path) as im:
        raw.update(im.info or {})
        _merge_exif(raw, im)
        # capture some common fields
        raw["format"] = im.format
        raw["mode"] = im.mode
        raw["size"] = im.size

        # Prefer the raw EXIF bytes path for UserComment; the generic merge may
        # already strip NULs and lose the encoding marker.
        try:
            exif = im.getexif()
        except Exception:
            exif = None
        if exif:
            uc_text = None
            try:
                exif_ifd = exif.get_ifd(ExifTags.IFD.Exif)
                uc_text = _decode_exif_usercomment_bytes(exif_ifd.get(37510))
            except Exception:
                uc_text = None

            if not uc_text:
                # fallback: merged string value
                uc_text = raw.get("UserComment") if isinstance(raw.get("UserComment"), str) else None

            if uc_text:
                usercomment_wrapper = _parse_novelai_usercomment_wrapper(uc_text)
                if usercomment_wrapper:
                    usercomment_source_raw = (
                        usercomment_wrapper.get("Source")
                        or usercomment_wrapper.get("Software")
                        or usercomment_wrapper.get("source")
                        or usercomment_wrapper.get("software")
                    )
                    c = usercomment_wrapper.get("Comment")
                    if isinstance(c, str):
                        usercomment_comment_raw = c
                        usercomment_comment_payload = _parse_json_str_maybe(c)

    if usercomment_wrapper:
        # Keep a small hint for debugging; avoid stuffing huge blobs.
        raw.setdefault("_usercomment_source", usercomment_source_raw)
        raw.setdefault("_usercomment_keys", sorted(list(usercomment_wrapper.keys()))[:40])

    json_blob = None
    raw_json_str = None

    # Highest priority: NovelAI EXIF UserComment wrapper -> Comment payload.
    if usercomment_comment_payload is not None:
        json_blob = usercomment_comment_payload
        raw_json_str = usercomment_comment_raw
        raw["_json_source_key"] = "EXIF.UserComment.Comment"
    if json_blob is None:
        for k in (
        "Comment",
        "comment",
        "Description",
        "description",
        "parameters",
        "Parameters",
        # JSON wrapper keys seen in some NovelAI exports
        "nai",
        "nai_parameters",
        "naiParams",
        "naiParameters",
        # common EXIF tag names
        "UserComment",
        "ImageDescription",
        "XPComment",
        # some internal keys
        "exif_37510",
    ):
            got = _try_parse_json_anywhere(raw.get(k))
            if got is not None:
                json_blob, raw_json_str = got
                raw["_json_source_key"] = k
                break

    # ---- Source / Software (raw, no replacement) ----
    # Prefer wrapper Source (EXIF UserComment JSON) when present.
    # Otherwise fall back to EXIF/PNG info "Source" or "Software".
    source_raw = None
    if usercomment_wrapper and isinstance(usercomment_wrapper, dict):
        source_raw = (
            usercomment_wrapper.get("Source")
            or usercomment_wrapper.get("source")
            or None
        )
    if not source_raw:
        source_raw = raw.get("Source") or raw.get("source")
    if isinstance(source_raw, (bytes, bytearray, memoryview)):
        source_raw = _decode_maybe_bytes(source_raw)

    software_raw = raw.get("Software") or raw.get("software")
    if not software_raw and usercomment_wrapper and isinstance(usercomment_wrapper, dict):
        software_raw = usercomment_wrapper.get("Software") or usercomment_wrapper.get("software")
    if isinstance(software_raw, (bytes, bytearray, memoryview)):
        software_raw = _decode_maybe_bytes(software_raw)

    # For DB "software" column we store the Source/Software name WITHOUT the trailing hash.
    # No canonicalization: keep the string as-is (whitespace trimmed only).
    name_in = str(source_raw) if source_raw is not None else (str(software_raw) if software_raw is not None else None)
    software, model = _source_name_without_hash(name_in)

    # Keep raw for export/debugging.
    if source_raw:
        raw.setdefault("Source", str(source_raw))
    if software_raw:
        raw.setdefault("Software", str(software_raw))
    prompt = None
    negative = None
    params: Dict[str, Any] = {}
    potion = None

    character_prompt: Optional[str] = None

    if json_blob:
        # Some exports put the actual payload under a wrapper key.
        json_primary: dict = json_blob
        for wrapper_key in ("nai", "nai_parameters", "naiParams", "naiParameters"):
            v = json_blob.get(wrapper_key) if isinstance(json_blob, dict) else None
            if isinstance(v, dict):
                json_primary = v
                raw["_json_wrapper_key"] = wrapper_key
                break
            if isinstance(v, str):
                try:
                    vv = json.loads(v)
                    if isinstance(vv, dict):
                        json_primary = vv
                        raw["_json_wrapper_key"] = wrapper_key
                        break
                except Exception:
                    pass

        # NovelAI v4.5 stores structured prompts under v4_prompt.caption.
        # Some exports store v4_prompt / v4_negative_prompt as JSON strings.
        def _obj_maybe(v: Any) -> Any:
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return v
            return v

        try:
            v4 = _obj_maybe(json_primary.get("v4_prompt") if isinstance(json_primary, dict) else None)
            cap = _obj_maybe(v4.get("caption") if isinstance(v4, dict) else None)
            if isinstance(cap, dict):
                if cap.get("base_caption"):
                    prompt = str(cap.get("base_caption"))
                cc = cap.get("char_captions")
                if isinstance(cc, list):
                    parts: list[str] = []
                    for it in cc:
                        if isinstance(it, dict) and it.get("char_caption"):
                            parts.append(str(it.get("char_caption")).strip())
                        elif isinstance(it, str) and it.strip():
                            parts.append(it.strip())
                    joined = "\n".join([p for p in parts if p])
                    if joined:
                        character_prompt = joined
            elif isinstance(cap, str) and cap.strip():
                prompt = cap.strip()
        except Exception:
            pass

        # Fallback prompt fields
        if not prompt:
            prompt = json_primary.get("prompt") or json_primary.get("Prompt") or json_blob.get("prompt")

        # Negative prompt
        try:
            v4n = _obj_maybe(json_primary.get("v4_negative_prompt") if isinstance(json_primary, dict) else None)
            ncap = _obj_maybe(v4n.get("caption") if isinstance(v4n, dict) else None)
            if isinstance(ncap, dict) and ncap.get("base_caption"):
                negative = str(ncap.get("base_caption"))
            elif isinstance(ncap, str) and ncap.strip():
                negative = ncap.strip()
        except Exception:
            pass
        if not negative:
            negative = (
                json_primary.get("uc")
                or json_primary.get("negative_prompt")
                or json_primary.get("Undesired Content")
                or json_blob.get("uc")
            )

        # model name (when present)
        # Do NOT overwrite the model hash extracted from Source/Software.
        if not model:
            jm = (json_primary.get("model") or json_primary.get("model_name") or json_primary.get("modelName"))
            if isinstance(jm, str):
                jm = jm.strip()
                if re.fullmatch(r"[0-9A-Fa-f]{8}", jm):
                    model = jm.upper()

        for ck in (
            "character_prompt",
            "characterPrompt",
            "character_prompt_raw",
            "Character Prompt",
            "character",
            "char_prompt",
            "characterPromptText",
        ):
            if ck in json_primary and json_primary.get(ck):
                character_prompt = str(json_primary.get(ck))
                break

        params = {
            k: v
            for k, v in json_primary.items()
            if k not in {"prompt", "Prompt", "uc", "negative_prompt", "Undesired Content"}
        }
        vibe_keys = [k for k in json_primary.keys() if "vibe" in k.lower() or "reference" in k.lower()]
        if vibe_keys:
            potion = {k: json_primary.get(k) for k in vibe_keys}

    if not prompt:
        desc = raw.get("ImageDescription") or raw.get("Description") or raw.get("comment") or raw.get("Comment")
        if isinstance(desc, (bytes, bytearray, memoryview)):
            desc = _decode_maybe_bytes(desc)
        if isinstance(desc, str) and desc.strip():
            p, n, ch = _split_prompt_sections(desc)
            prompt = p
            negative = negative or n
            character_prompt = character_prompt or ch

    if character_prompt:
        # Keep it also in params for backward compatibility / exporting,
        # but do NOT merge into positive prompt. Character prompt must be treated separately
        # for dedup signature and UI grouping.
        params.setdefault("_character_prompt_raw", character_prompt)

    # Keep raw Source/Software strings (no replacement) for exporting / debugging.
    # These keys are internal (prefixed by underscore) to avoid collision with NovelAI keys.
    if source_raw is not None:
        params.setdefault("_source_raw", str(source_raw))
    if software_raw is not None:
        params.setdefault("_software_raw", str(software_raw))
    if software is not None:
        params.setdefault("_source_name", str(software))
    if model is not None:
        params.setdefault("_model_hash", str(model))

    return NaiMeta(
        software=str(software) if software is not None else None,
        model=str(model) if model is not None else None,
        prompt=str(prompt) if prompt is not None else None,
        negative=str(negative) if negative is not None else None,
        character_prompt=str(character_prompt) if character_prompt is not None else None,
        params=params,
        potion=potion,
        raw=raw,
        raw_json_str=raw_json_str,
    )
