from __future__ import annotations

import json
import re
import sqlite3
from typing import Any

from .tag_parser import normalize_tag, parse_tag_list

_GENERIC_CHARACTER_KEYS = {
    "girl",
    "girls",
    "boy",
    "boys",
    "1girl",
    "2girls",
    "3girls",
    "4girls",
    "5girls",
    "6+girls",
    "1boy",
    "2boys",
    "3boys",
    "4boys",
    "5boys",
    "6+boys",
    "multiple_girls",
    "multiple_boys",
}


def parse_caption_lines(raw: str | None) -> list[str]:
    s = str(raw or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not s:
        return []

    lines: list[str] = []
    st = s.strip()
    if (st.startswith("[") and st.endswith("]")) or (st.startswith("{") and st.endswith("}")):
        try:
            j = json.loads(st)
            if isinstance(j, list):
                for it in j:
                    if isinstance(it, str) and it.strip():
                        lines.append(it.strip())
                    elif isinstance(it, dict):
                        cc = it.get("char_caption") or it.get("caption") or it.get("text")
                        if isinstance(cc, str) and cc.strip():
                            lines.append(cc.strip())
            elif isinstance(j, dict):
                cc = j.get("char_caption") or j.get("caption") or j.get("text")
                if isinstance(cc, str) and cc.strip():
                    lines.append(cc.strip())
        except Exception:
            pass

    if not lines:
        lines = [ln.strip() for ln in s.split("\n") if str(ln or "").strip()]
    return lines


def extract_character_negative_prompt_raw(params_json_or_obj: Any) -> str:
    try:
        if isinstance(params_json_or_obj, str) and params_json_or_obj.strip():
            params = json.loads(params_json_or_obj)
        elif isinstance(params_json_or_obj, dict):
            params = params_json_or_obj
        else:
            params = None
    except Exception:
        params = None
    if isinstance(params, dict) and isinstance(params.get("_character_negative_prompt_raw"), str):
        return str(params.get("_character_negative_prompt_raw") or "")
    return ""


def _get_tag_category(conn: sqlite3.Connection, canonical: str) -> int | None:
    if not canonical:
        return None
    row = conn.execute(
        "SELECT category FROM tags_master WHERE tag=? LIMIT 1",
        (str(canonical or ""),),
    ).fetchone()
    if not row:
        return None
    try:
        return int(row["category"] if not isinstance(row, tuple) else row[0])
    except Exception:
        return None


def canonical_character_name_from_text(conn: sqlite3.Connection, text: str | None) -> str:
    src = str(text or "").strip()
    if not src:
        return ""
    try:
        parsed = parse_tag_list(src)
    except Exception:
        parsed = []

    for t in parsed:
        try:
            tag_norm = normalize_tag(t.tag_text)
        except Exception:
            tag_norm = ""
        if not tag_norm:
            continue
        canonical = tag_norm
        cat = _get_tag_category(conn, canonical)
        if cat != 4:
            continue
        if canonical in _GENERIC_CHARACTER_KEYS:
            continue
        return canonical
    return ""


def parse_character_entries(conn: sqlite3.Connection, pos_raw: str | None, neg_raw: str | None) -> list[dict]:
    pos_lines = parse_caption_lines(pos_raw)
    neg_lines = parse_caption_lines(neg_raw)

    pos_items: list[dict] = []
    unknown_n = 1

    for ln0 in pos_lines:
        ln = str(ln0 or "").strip()
        if not ln:
            continue

        name = ""
        body = ln

        m = re.match(r"^(.+?)\s*:\s*(.+)$", ln)
        if m:
            left = (m.group(1) or "").strip()
            right = (m.group(2) or "").strip()
            canonical = canonical_character_name_from_text(conn, left)
            if canonical:
                name = canonical
                body = right

        if not name:
            name = canonical_character_name_from_text(conn, ln)

        if not name:
            name = f"不明{unknown_n}"
            unknown_n += 1

        pos_items.append({"name": name, "pos": body})

    neg_by_name: dict[str, str] = {}
    neg_list: list[str] = []

    for ln0 in neg_lines:
        ln = str(ln0 or "").strip()
        if not ln:
            continue

        name = ""
        body = ln

        m = re.match(r"^(.+?)\s*(uc|negative|undesired)\s*:\s*(.+)$", ln, flags=re.IGNORECASE)
        if m:
            left = (m.group(1) or "").strip()
            right = (m.group(3) or "").strip()
            canonical = canonical_character_name_from_text(conn, left)
            if canonical:
                name = canonical
                body = right

        if not name:
            m = re.match(r"^(.+?)\s*:\s*(.+)$", ln)
            if m:
                left = (m.group(1) or "").strip()
                right = (m.group(2) or "").strip()
                canonical = canonical_character_name_from_text(conn, left)
                if canonical:
                    name = canonical
                    body = right

        if not name:
            m = re.match(r"^(uc|negative|undesired)\s*:\s*(.+)$", ln, flags=re.IGNORECASE)
            if m:
                body = (m.group(2) or "").strip()

        if name:
            prev = neg_by_name.get(name, "")
            neg_by_name[name] = (prev + "\n" + body).strip() if prev else body
        else:
            neg_list.append(body)

    out: list[dict] = []
    max_n = max(len(pos_items), len(neg_list))

    for i in range(max_n):
        pi = pos_items[i] if i < len(pos_items) else None
        pos = (pi or {}).get("pos", "") if pi else ""
        name = (pi or {}).get("name", "") if pi else ""
        neg = ""

        if name:
            if name in neg_by_name:
                neg = neg_by_name.get(name) or ""
            elif i < len(neg_list):
                neg = neg_list[i] or ""
        else:
            src = neg_list[i] if i < len(neg_list) else ""
            name = canonical_character_name_from_text(conn, src)
            if not name:
                name = f"不明{unknown_n}"
                unknown_n += 1
            neg = src

        out.append({"name": name, "pos": pos, "neg": neg})

    if not pos_items and neg_by_name:
        for name, body in neg_by_name.items():
            out.append({"name": name, "pos": "", "neg": str(body or "")})

    return out


def build_prompt_view_payload(
    conn: sqlite3.Connection,
    prompt_negative_raw: str | None,
    prompt_character_raw: str | None,
    params_json_or_obj: Any,
) -> tuple[list[dict], str]:
    char_neg_raw = extract_character_negative_prompt_raw(params_json_or_obj)
    entries = parse_character_entries(conn, prompt_character_raw, char_neg_raw)

    parts: list[str] = []
    if prompt_negative_raw and str(prompt_negative_raw).strip():
        parts.append(str(prompt_negative_raw).strip())
    for e in entries:
        neg = str((e or {}).get("neg") or "").strip()
        if neg:
            parts.append(neg)
    main_negative = "\n".join(parts).strip()
    return entries, main_negative


def parse_prompt_multiline_to_tag_objs(conn: sqlite3.Connection, raw: str | None) -> list[dict]:
    out: list[dict] = []
    if not raw:
        return out

    s = str(raw).replace("\r\n", "\n").replace("\r", "\n")
    for ln in s.split("\n"):
        ln = (ln or "").strip()
        if not ln:
            continue

        try:
            m = re.match(
                r"^(.+?)\s*(?:uc|negative|negative\s*prompt|undesired|undesired\s*content)\s*:\s*(.+)$",
                ln,
                flags=re.IGNORECASE,
            )
            if m:
                ln = (m.group(2) or "").strip()
            else:
                ln = re.sub(
                    r"^(?:uc|negative|negative\s*prompt|undesired|undesired\s*content)\s*:\s*",
                    "",
                    ln,
                    flags=re.IGNORECASE,
                ).strip()
        except Exception:
            pass
        if not ln:
            continue

        try:
            parsed = parse_tag_list(ln)
        except Exception:
            parsed = []
        for t in parsed:
            try:
                tag_norm = normalize_tag(t.tag_text)
            except Exception:
                tag_norm = ""
            if not tag_norm:
                continue
            canonical = tag_norm
            out.append(
                {
                    "canonical": canonical,
                    "text": t.tag_text or "",
                    "raw_one": t.tag_raw_one or "",
                    "emphasis_type": t.emphasis_type,
                    "brace_level": int(t.brace_level or 0),
                    "numeric_weight": float(t.numeric_weight or 0),
                    "category": _get_tag_category(conn, canonical),
                }
            )
    return out


def _row_get(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        if hasattr(row, "keys") and key in row.keys():
            value = row[key]
            return default if value is None else value
    except Exception:
        pass
    try:
        value = row[index]
        return default if value is None else value
    except Exception:
        return default


def tag_group_for_category(category: Any) -> str:
    try:
        cat = int(category) if category is not None else None
    except Exception:
        cat = None
    if cat == 1:
        return "artist"
    if cat == 4:
        return "character"
    if cat == 5:
        return "quality"
    return "other"


def build_grouped_tags_payload(tag_rows: list[Any] | tuple[Any, ...]) -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {
        "artist": [],
        "quality": [],
        "character": [],
        "other": [],
    }
    for row in tag_rows or []:
        canonical = str(_row_get(row, "tag_canonical", 0, "") or "").strip()
        if not canonical:
            continue
        group = str(_row_get(row, "group", 7, "") or "").strip()
        if not group:
            group = tag_group_for_category(_row_get(row, "category", 3, None))
        if group not in grouped:
            group = "other"
        grouped[group].append(
            {
                "canonical": canonical,
                "text": str(_row_get(row, "tag_text", 1, "") or ""),
                "raw_one": str(_row_get(row, "tag_raw", 2, "") or ""),
                "emphasis_type": _row_get(row, "emphasis_type", 4, "none"),
                "brace_level": int(_row_get(row, "brace_level", 5, 0) or 0),
                "numeric_weight": float(_row_get(row, "numeric_weight", 6, 0) or 0),
            }
        )
    return grouped
