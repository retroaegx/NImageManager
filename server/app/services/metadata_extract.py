from __future__ import annotations

import io
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

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
    uses_potion: bool
    uses_precise_reference: bool
    sampler: Optional[str]
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

def _has_meaningful_value(val: Any) -> bool:
    if val is None:
        return False
    if isinstance(val, str):
        return val.strip() != ""
    if isinstance(val, dict):
        return any(_has_meaningful_value(v) for v in val.values())
    if isinstance(val, (list, tuple)):
        return any(_has_meaningful_value(item) for item in val)
    return True


def _iter_usage_scopes(payload: Any):
    if not isinstance(payload, dict):
        return

    seen: set[int] = set()
    queue: list[dict[str, Any]] = [payload]
    nested_keys = (
        "params",
        "parameters",
        "nai_parameters",
        "naiParams",
        "naiParameters",
        "parameter",
        "payload",
        "data",
    )

    while queue:
        scope = queue.pop(0)
        sid = id(scope)
        if sid in seen:
            continue
        seen.add(sid)
        yield scope

        for key in nested_keys:
            nested = scope.get(key)
            if isinstance(nested, str):
                try:
                    nested = json.loads(nested)
                except Exception:
                    continue
            if isinstance(nested, dict):
                queue.append(nested)


def _detect_potion_usage(payload: Any) -> bool:
    for scope in _iter_usage_scopes(payload):
        if any(
            _has_meaningful_value(scope.get(key))
            for key in (
                "reference_image_multiple",
                "reference_information_extracted_multiple",
                "reference_strength_multiple",
            )
        ):
            return True
    return False


def _detect_precise_reference_usage(payload: Any) -> bool:
    for scope in _iter_usage_scopes(payload):
        if _has_meaningful_value(scope.get("director_reference_strengths")):
            return True
    return False


def _extract_sampler(payload: Any) -> Optional[str]:
    for scope in _iter_usage_scopes(payload):
        sampler = scope.get("sampler")
        if not isinstance(sampler, str):
            continue
        sampler = sampler.strip()
        if sampler:
            return sampler
    return None


def _parse_json_dict_maybe(val: Any) -> Optional[dict[str, Any]]:
    if val is None:
        return None
    if isinstance(val, dict):
        return val
    if isinstance(val, (bytes, bytearray, memoryview)):
        try:
            val = bytes(val).decode("utf-8", errors="ignore")
        except Exception:
            return None
    if not isinstance(val, str):
        return None
    got = _try_parse_json_anywhere(val)
    if got is None:
        return None
    obj, _raw = got
    return obj if isinstance(obj, dict) else None


def _yield_usage_candidates(payload: Any):
    obj = _parse_json_dict_maybe(payload)
    if not isinstance(obj, dict):
        return

    seen: set[int] = set()
    queue: list[dict[str, Any]] = [obj]
    wrapper_keys = (
        "json",
        "Comment",
        "comment",
        "Description",
        "description",
        "parameters",
        "Parameters",
        "nai",
        "nai_parameters",
        "naiParams",
        "naiParameters",
        "UserComment",
        "ImageDescription",
        "XPComment",
        "exif_37510",
        "info",
    )

    while queue:
        current = queue.pop(0)
        cid = id(current)
        if cid in seen:
            continue
        seen.add(cid)
        yield current

        for key in wrapper_keys:
            nested = current.get(key)
            nested_obj = _parse_json_dict_maybe(nested)
            if isinstance(nested_obj, dict):
                queue.append(nested_obj)


def detect_generation_usage_from_storage(
    params_json_or_obj: Any = None,
    metadata_raw_or_obj: Any = None,
) -> tuple[bool, bool, Optional[str]]:
    uses_potion = False
    uses_precise_reference = False
    sampler: Optional[str] = None

    for source in (params_json_or_obj, metadata_raw_or_obj):
        for candidate in _yield_usage_candidates(source):
            if not uses_potion and _detect_potion_usage(candidate):
                uses_potion = True
            if not uses_precise_reference and _detect_precise_reference_usage(candidate):
                uses_precise_reference = True
            if sampler is None:
                sampler = _extract_sampler(candidate)
            if uses_potion and uses_precise_reference and sampler is not None:
                return uses_potion, uses_precise_reference, sampler

    return uses_potion, uses_precise_reference, sampler


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


def _normalize_nai_choice_syntax(text: Any) -> Optional[str]:
    """Normalize NovelAI choice syntax for fallback prompt extraction.

    Example:
      || A | B | C || -> A, B, C

    This is intentionally used only for fallback/input-side prompt fields.
    Confirmed/actual prompts should be preserved as-is.
    """
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    if "||" not in s:
        return s

    s = s.replace("||", "")
    s = s.replace("|", ",")
    s = re.sub(r"\s*,\s*", ", ", s)
    s = re.sub(r",(?:\s*,)+", ", ", s)
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r"(^|[\s\(\[\{])+,", r"\1", s)
    s = re.sub(r",\s*([\)\]\}])", r"\1", s)
    s = s.strip(" ,")
    return s or None


def _join_char_captions(char_captions: Any, *, normalize_choice_syntax: bool = False) -> Optional[str]:
    if not isinstance(char_captions, list):
        return None
    parts: list[str] = []
    for it in char_captions:
        val: Optional[str] = None
        if isinstance(it, dict) and it.get("char_caption"):
            val = str(it.get("char_caption")).strip()
        elif isinstance(it, str) and it.strip():
            val = it.strip()
        if not val:
            continue
        if normalize_choice_syntax:
            val = _normalize_nai_choice_syntax(val)
        if val:
            parts.append(val)
    joined = "\n".join([p for p in parts if p])
    return joined or None


def _extract_caption_payload(node: Any, *, normalize_choice_syntax: bool = False) -> tuple[Optional[str], Optional[str]]:
    """Extract (base_caption, joined_char_captions) from a prompt-like node."""
    if isinstance(node, str):
        s = node.strip()
        if normalize_choice_syntax:
            s = _normalize_nai_choice_syntax(s) or ""
        return (s or None), None
    if not isinstance(node, dict):
        return None, None

    base = node.get("base_caption")
    if base is None and isinstance(node.get("caption"), str):
        base = node.get("caption")
    base_s: Optional[str] = None
    if base not in (None, ""):
        base_s = str(base).strip()
        if normalize_choice_syntax:
            base_s = _normalize_nai_choice_syntax(base_s)

    chars = _join_char_captions(node.get("char_captions"), normalize_choice_syntax=normalize_choice_syntax)
    return base_s, chars

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

def _extract_novelai_metadata_from_source(source: Any) -> NaiMeta:
    raw: Dict[str, Any] = {}

    # NovelAI WebP exports often store metadata in EXIF UserComment.
    usercomment_wrapper: Optional[dict] = None
    usercomment_comment_payload: Optional[dict] = None
    usercomment_comment_raw: Optional[str] = None
    usercomment_source_raw: Optional[str] = None

    with Image.open(source) as im:
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
    uses_potion = False
    uses_precise_reference = False
    sampler = None

    character_prompt: Optional[str] = None

    character_negative_prompt: Optional[str] = None

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

        # NovelAI stores structured prompts under versioned keys like:
        #   v4_prompt.caption / v4_negative_prompt.caption
        # Future versions may use v5_prompt, v5_5_prompt, etc.
        # Some exports store these values as JSON strings.
        def _obj_maybe(v: Any) -> Any:
            if isinstance(v, dict):
                return v
            if isinstance(v, str):
                try:
                    return json.loads(v)
                except Exception:
                    return v
            return v

        def _gather_prompt_scopes(primary: Any) -> list[tuple[str, dict]]:
            # Prompt-like keys may live under root or nested wrappers like `params`.
            scopes: list[tuple[str, dict]] = []
            if not isinstance(primary, dict):
                return scopes
            scopes.append(("root", primary))

            for key in ("params", "parameters", "nai_parameters", "naiParams", "naiParameters", "parameter", "payload", "data"):
                v = primary.get(key)
                if isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except Exception:
                        pass
                if isinstance(v, dict):
                    scopes.append((key, v))
            return scopes

        def _best_versioned_obj(scopes: list[tuple[str, dict]], suffix: str) -> Tuple[Optional[str], Optional[str], Any]:
            # Pick highest versioned key across scopes.
            best_scope: Optional[str] = None
            best_key: Optional[str] = None
            best_obj: Any = None
            best_v = (-1, -1)

            rx = re.compile(rf"^v(\d+)(?:[._](\d+))?_{re.escape(suffix)}$", re.IGNORECASE)

            for scope_name, d in (scopes or []):
                if not isinstance(d, dict):
                    continue
                for k in d.keys():
                    if not isinstance(k, str):
                        continue
                    m = rx.match(k)
                    if not m:
                        continue
                    maj = int(m.group(1) or 0)
                    minor = int(m.group(2) or 0)
                    v = (maj, minor)
                    if v > best_v:
                        best_v = v
                        best_scope = scope_name
                        best_key = k
                        best_obj = _obj_maybe(d.get(k))

            if not best_key:
                return None, None, None
            return best_scope, best_key, best_obj

        def _find_scope_value(scopes: list[tuple[str, dict]], *keys: str) -> tuple[Optional[str], Optional[str], Any]:
            for scope_name, d in (scopes or []):
                if not isinstance(d, dict):
                    continue
                for key in keys:
                    if key in d:
                        return scope_name, key, _obj_maybe(d.get(key))
            return None, None, None

        scopes = _gather_prompt_scopes(json_primary)

        # Highest priority: NovelAI's resolved/actual prompts.
        # These are post-choice-resolution values and must win over input-side
        # base_caption/char_captions from versioned prompt structures.
        try:
            actual_scope, actual_key, actual_prompts = _find_scope_value(scopes, "actual_prompts")
            if isinstance(actual_prompts, dict):
                raw["_actual_prompts_key_used"] = f"{actual_scope}.{actual_key}" if actual_scope else str(actual_key)

                p_base, p_chars = _extract_caption_payload(_obj_maybe(actual_prompts.get("prompt")), normalize_choice_syntax=False)
                if p_base:
                    prompt = p_base
                    raw["_prompt_key_used"] = raw["_actual_prompts_key_used"] + ".prompt"
                if p_chars:
                    character_prompt = p_chars
                    raw["_character_prompt_key_used"] = raw["_actual_prompts_key_used"] + ".prompt.char_captions"

                n_base, n_chars = _extract_caption_payload(_obj_maybe(actual_prompts.get("negative_prompt")), normalize_choice_syntax=False)
                if n_base:
                    negative = n_base
                    raw["_negative_prompt_key_used"] = raw["_actual_prompts_key_used"] + ".negative_prompt"
                if n_chars:
                    character_negative_prompt = n_chars
                    raw["_character_negative_prompt_key_used"] = raw["_actual_prompts_key_used"] + ".negative_prompt.char_captions"
        except Exception:
            pass

        # Fallback: input-side structured prompts. Normalize NovelAI choice syntax
        # because these may still contain raw `|| ... | ... ||` expressions.
        if not prompt or not character_prompt:
            try:
                scope_used, k_used, vobj = _best_versioned_obj(scopes, "prompt")
                cap = _obj_maybe(vobj.get("caption") if isinstance(vobj, dict) else None)
                p_base, p_chars = _extract_caption_payload(cap, normalize_choice_syntax=True)
                if k_used and (p_base or p_chars):
                    raw["_prompt_fallback_key_used"] = f"{scope_used}.{k_used}" if scope_used else k_used
                if not prompt and p_base:
                    prompt = p_base
                if not character_prompt and p_chars:
                    character_prompt = p_chars
            except Exception:
                pass

        if not negative or not character_negative_prompt:
            try:
                scope_used, k_used, vobj = _best_versioned_obj(scopes, "negative_prompt")
                ncap = _obj_maybe(vobj.get("caption") if isinstance(vobj, dict) else None)
                n_base, n_chars = _extract_caption_payload(ncap, normalize_choice_syntax=True)
                if k_used and (n_base or n_chars):
                    raw["_negative_prompt_fallback_key_used"] = f"{scope_used}.{k_used}" if scope_used else k_used
                if not negative and n_base:
                    negative = n_base
                if not character_negative_prompt and n_chars:
                    character_negative_prompt = n_chars
            except Exception:
                pass

        # Flat/raw fallback fields.
        if not prompt:
            scope_used, key_used, v = _find_scope_value(
                scopes,
                "prompt_positive_raw",
                "promptPositiveRaw",
                "prompt_positive",
                "positive_prompt",
                "positivePrompt",
                "prompt",
                "Prompt",
            )
            if v not in (None, ""):
                prompt = _normalize_nai_choice_syntax(v)
                if prompt and key_used:
                    raw["_prompt_fallback_key_used"] = f"{scope_used}.{key_used}" if scope_used else str(key_used)

        if not negative:
            scope_used, key_used, v = _find_scope_value(
                scopes,
                "prompt_negative_raw",
                "promptNegativeRaw",
                "prompt_negative",
                "negative_prompt_raw",
                "negativePrompt",
                "uc",
                "negative_prompt",
                "Undesired Content",
            )
            if v not in (None, ""):
                negative = _normalize_nai_choice_syntax(v)
                if negative and key_used:
                    raw["_negative_prompt_fallback_key_used"] = f"{scope_used}.{key_used}" if scope_used else str(key_used)

        # Direct character prompt fallback (still input-side, so normalize choice syntax).
        if not character_prompt:
            scope_used, key_used, v = _find_scope_value(
                scopes,
                "character_prompt",
                "characterPrompt",
                "character_prompt_raw",
                "prompt_character_raw",
                "promptCharacterRaw",
                "Character Prompt",
                "character",
                "char_prompt",
                "characterPromptText",
            )
            if v not in (None, ""):
                character_prompt = _normalize_nai_choice_syntax(v)
                if character_prompt and key_used:
                    raw["_character_prompt_fallback_key_used"] = f"{scope_used}.{key_used}" if scope_used else str(key_used)

        # model name (when present)
        # Do NOT overwrite the model hash extracted from Source/Software.
        if not model:
            jm = (json_primary.get("model") or json_primary.get("model_name") or json_primary.get("modelName"))
            if isinstance(jm, str):
                jm = jm.strip()
                if re.fullmatch(r"[0-9A-Fa-f]{8}", jm):
                    model = jm.upper()

        params = {
            k: v
            for k, v in json_primary.items()
            if k not in {"prompt", "Prompt", "uc", "negative_prompt", "Undesired Content"}
        }
        uses_potion = _detect_potion_usage(json_primary)
        uses_precise_reference = _detect_precise_reference_usage(json_primary)
        sampler = _extract_sampler(json_primary)

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

    if character_negative_prompt:
        # Character negative prompt is kept in params only (no DB column).
        params.setdefault("_character_negative_prompt_raw", character_negative_prompt)

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
        uses_potion=bool(uses_potion),
        uses_precise_reference=bool(uses_precise_reference),
        sampler=str(sampler) if sampler is not None else None,
        raw=raw,
        raw_json_str=raw_json_str,
    )


def extract_novelai_metadata(path: str | Path) -> NaiMeta:
    return _extract_novelai_metadata_from_source(Path(path))


def extract_novelai_metadata_bytes(data: bytes) -> NaiMeta:
    return _extract_novelai_metadata_from_source(io.BytesIO(bytes(data or b"")))
