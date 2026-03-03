from __future__ import annotations

from dataclasses import dataclass
import hashlib
import re
from typing import List, Optional

# Numeric emphasis can be negative in some NovelAI exports (e.g. -2::tag::)
_NUMERIC_RE = re.compile(r"^\s*(?P<w>-?[0-9]+(?:\.[0-9]+)?)::(?P<inner>.*)::\s*$", re.DOTALL)

# Empty emphasis wrappers / garbage like "[[[[[[]]]]]]" or "{{}}".
_EMPTY_EMPH_GARBAGE_RE = re.compile(r"^[\s\{\}\[\],]+$")

# Characters that can accidentally leak to a tag edge (typos, separators, etc.)
_TAG_EDGE_STRIP = " \t\r\n,，、:;："

@dataclass(frozen=True)
class ParsedTag:
    tag_text: str
    emphasis_type: str  # none / braces / numeric
    brace_level: Optional[int] = None
    numeric_weight: Optional[float] = None
    tag_raw_one: str = ""  # per-tag raw (reconstructed, emphasis preserved)

def split_top_level_commas(s: str) -> List[str]:
    """Split by commas that are NOT inside braces and NOT inside numeric emphasis blocks.

    Numeric emphasis: <num>:: ... ::  (inner may contain commas)
    """
    parts: List[str] = []
    buf: List[str] = []
    depth = 0
    i = 0
    n = len(s)
    while i < n:
        ch = s[i]

        # numeric block (only when not inside braces)
        if depth == 0:
            m = re.match(r"\s*(-?[0-9]+(?:\.[0-9]+)?)::", s[i:])
            if m:
                start = i
                start_delim_end = i + m.end()
                end = s.find("::", start_delim_end)
                if end != -1:
                    # Treat the numeric block as an atomic token.
                    #
                    # NovelAI tags are comma-separated, and numeric emphasis blocks
                    # are expected to be followed by a comma or EOL. In practice,
                    # users sometimes typo and forget the comma right after the
                    # closing '::' (e.g. "...heart eyes::looking at viewer").
                    # If we keep buffering, the trailing text gets glued into the
                    # same segment and the numeric parser won't run.
                    #
                    # Here we force-split at the closing '::' so the numeric token
                    # is parsed, and any trailing text becomes its own segment.
                    pre = "".join(buf).strip()
                    if pre:
                        parts.append(pre)
                    buf = []

                    token = s[start : end + 2].strip()
                    if token:
                        parts.append(token)

                    i = end + 2
                    continue

        if ch in "{[":
            depth += 1
            buf.append(ch)
            i += 1
            continue
        if ch in "]}":
            depth = max(0, depth - 1)
            buf.append(ch)
            i += 1
            continue
        if ch == "," and depth == 0:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts

def _is_brace_balanced(s: str) -> bool:
    d = 0
    for ch in s:
        if ch == "{":
            d += 1
        elif ch == "}":
            d -= 1
            if d < 0:
                return False
    return d == 0


def _is_wrapped_balanced(s: str, open_ch: str, close_ch: str) -> bool:
    d = 0
    for ch in s:
        if ch == open_ch:
            d += 1
        elif ch == close_ch:
            d -= 1
            if d < 0:
                return False
    return d == 0

def strip_outer_braces(s: str) -> tuple[str, int]:
    s0 = s.strip()
    if not s0:
        return s0, 0

    # Support both {} and [] wrappers (some NovelAI exports use brackets).
    if s0.startswith("{"):
        open_ch, close_ch = "{", "}"
    elif s0.startswith("["):
        open_ch, close_ch = "[", "]"
    else:
        return s0, 0

    lead = 0
    for ch in s0:
        if ch == open_ch:
            lead += 1
        else:
            break
    trail = 0
    for ch in reversed(s0):
        if ch == close_ch:
            trail += 1
        else:
            break
    level = min(lead, trail)
    if level == 0:
        return s0, 0
    if not _is_wrapped_balanced(s0, open_ch, close_ch):
        return s0, 0
    inner = s0[level:len(s0)-level].strip()
    if not inner:
        return s0, 0
    return inner, level


def _strip_tag_edges(s: str) -> str:
    """Strip common separators that may leak into a tag.

    This intentionally does *not* remove separators inside the string
    (e.g. artist:mocamocaink).
    """
    if not s:
        return ""
    return s.strip(_TAG_EDGE_STRIP)

def parse_tag_list(prompt: str) -> List[ParsedTag]:
    return _parse_segment(prompt.strip(), inherited_brace_level=0)

def _parse_segment(seg: str, inherited_brace_level: int) -> List[ParsedTag]:
    seg = seg.strip()
    if not seg:
        return []

    # Drop empty emphasis garbage like "[[[[[[]]]]]]" or "{{}}".
    if _EMPTY_EMPH_GARBAGE_RE.match(seg):
        return []

    # numeric emphasis (explicit) has top priority
    m = _NUMERIC_RE.match(seg)
    if m:
        w = float(m.group("w"))
        inner = (m.group("inner") or "").strip(_TAG_EDGE_STRIP)
        # numeric wins over braces/brackets inside it
        inner2, _ = strip_outer_braces(inner)
        parts = split_top_level_commas(inner2)
        out: List[ParsedTag] = []
        for p in parts:
            t = _strip_tag_edges((p or "").strip())
            if not t:
                continue
            t2, _ = strip_outer_braces(t)
            tag_text = _strip_tag_edges(t2.strip())
            if not tag_text:
                continue
            out.append(ParsedTag(tag_text=tag_text, emphasis_type="numeric", numeric_weight=w, tag_raw_one=f"{m.group('w')}::{tag_text}::"))
        return out

    inner, level = strip_outer_braces(seg)
    if level > 0:
        parts = split_top_level_commas(inner)
        if len(parts) > 1:
            # grouping: braces are inherited and additive
            out: List[ParsedTag] = []
            for p in parts:
                out.extend(_parse_segment(p, inherited_brace_level + level))
            return out
        # single element: explicit braces (override, do NOT add)
        only = parts[0] if parts else inner
        # still allow numeric inside braces
        sub = _parse_segment(only, inherited_brace_level=0)
        if len(sub) == 1 and sub[0].emphasis_type == "numeric":
            return sub
        tag_text = _strip_tag_edges(only.strip())
        return [ParsedTag(tag_text=tag_text, emphasis_type="braces", brace_level=level, tag_raw_one=("{"*level + tag_text + "}"*level))]

    parts = split_top_level_commas(seg)
    if len(parts) > 1:
        out: List[ParsedTag] = []
        for p in parts:
            out.extend(_parse_segment(p, inherited_brace_level))
        return out

    leaf = _strip_tag_edges(parts[0].strip() if parts else seg)
    if not leaf:
        return []

    # Some exports may contain stray trailing '::' without a numeric prefix.
    if leaf.endswith("::") and not re.match(r"^\s*-?[0-9]+(?:\.[0-9]+)?::", leaf):
        leaf = leaf[:-2].rstrip()

    if inherited_brace_level > 0:
        return [ParsedTag(tag_text=leaf, emphasis_type="braces", brace_level=inherited_brace_level, tag_raw_one=("{"*inherited_brace_level + leaf + "}"*inherited_brace_level))]
    return [ParsedTag(tag_text=leaf, emphasis_type="none", tag_raw_one=leaf)]

def normalize_tag(tag_text: str) -> str:
    # NovelAI tags may contain spaces; master dictionary uses underscores.
    t = (tag_text or "").strip().lower()
    # Some sources include UI-oriented separators (e.g. trailing ':'), or stray
    # punctuation from malformed prompts. These must not become part of the
    # canonical tag.
    t = t.strip(" \t\r\n,:;：")
    # Strip namespace prefixes (artist:/character: etc.) during normalization.
    t = re.sub(r"^(?:artist|character|style|quality|general|meta|rating|copyright)\s*[:/]\s*", "", t)
    t = re.sub(r"\s+", "_", t)
    t = re.sub(r"_+", "_", t)
    return t

def main_sig_hash(canonical_tags_without_character: list[str]) -> str:
    tags = sorted(set([t for t in canonical_tags_without_character if t]))
    joined = ",".join(tags)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()
