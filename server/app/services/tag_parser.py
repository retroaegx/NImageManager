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
# Include braces/brackets as edge-strip to be resilient against malformed prompts
# (e.g. missing closing braces causing wrappers to leak into tag text).
_TAG_EDGE_STRIP = " \t\r\n,，、:;：{}[]"

# Emphasis wrappers are often malformed in user prompts (missing/extra closers).
# We repair them for parsing:
# - extra closing brackets are dropped
# - missing closing brackets are appended at EOL
_BRACKET_PAIRS = {"{": "}", "[": "]"}
_BRACKET_OPENERS = set(_BRACKET_PAIRS.keys())
_BRACKET_CLOSERS = set(_BRACKET_PAIRS.values())
_BRACKET_CLOSER_TO_OPENER = {v: k for k, v in _BRACKET_PAIRS.items()}

def _repair_emphasis_brackets(s: str) -> str:
    """Repair emphasis brackets for parsing.

    - Drop extra closing brackets that have no opener.
    - If a wrapper is left open, auto-append the missing closing brackets at EOL.
    - If closing type mismatches, try to recover by auto-closing until we can match.
    """
    if not s:
        return ""
    stack: List[str] = []
    out: List[str] = []
    for ch in s:
        if ch in _BRACKET_OPENERS:
            stack.append(ch)
            out.append(ch)
            continue
        if ch in _BRACKET_CLOSERS:
            if not stack:
                # extra closer -> drop
                continue
            want_open = _BRACKET_CLOSER_TO_OPENER.get(ch)
            if want_open and stack[-1] == want_open:
                stack.pop()
                out.append(ch)
                continue
            if want_open and (want_open in stack):
                # auto-close until match
                while stack and stack[-1] != want_open:
                    op = stack.pop()
                    out.append(_BRACKET_PAIRS.get(op, ""))
                if stack and stack[-1] == want_open:
                    stack.pop()
                    out.append(ch)
                continue
            # no matching opener -> drop
            continue
        out.append(ch)

    while stack:
        op = stack.pop()
        out.append(_BRACKET_PAIRS.get(op, ""))

    return "".join(out)


@dataclass(frozen=True)
class ParsedTag:
    tag_text: str
    emphasis_type: str  # none / braces / numeric
    brace_level: Optional[int] = None
    numeric_weight: Optional[float] = None
    tag_raw_one: str = ""  # per-tag raw (reconstructed, emphasis preserved)

def split_top_level_commas(s: str) -> List[str]:
    """Split by commas that are NOT inside emphasis wrappers.

    Supports:
    - braces/brackets emphasis: {{{tag}}}, [[tag]]
    - numeric emphasis: <num>:: ... ::  (inner may contain commas)

    Error-tolerant behavior for malformed wrappers:
    - Extra closing brackets are dropped.
    - Missing closing brackets are auto-appended at EOL so parsing can proceed.
    """
    s = _repair_emphasis_brackets(s or "")

    parts: List[str] = []
    buf: List[str] = []
    stack: List[str] = []  # '{' or '['

    i = 0
    n = len(s)
    while i < n:
        ch = s[i]

        # numeric block (only when not inside braces/brackets)
        if not stack:
            m = re.match(r"\s*(-?[0-9]+(?:\.[0-9]+)?)::", s[i:])
            if m:
                start = i
                start_delim_end = i + m.end()
                end = s.find("::", start_delim_end)
                if end != -1:
                    # Treat the numeric block as an atomic token.
                    pre = "".join(buf).strip()
                    if pre:
                        parts.append(pre)
                    buf = []

                    token = s[start : end + 2].strip()
                    if token:
                        parts.append(token)

                    i = end + 2
                    continue

        if ch in _BRACKET_OPENERS:
            stack.append(ch)
            buf.append(ch)
            i += 1
            continue

        if ch in _BRACKET_CLOSERS:
            if not stack:
                # extra closer: treat as non-existent
                i += 1
                continue

            want_open = _BRACKET_CLOSER_TO_OPENER.get(ch)

            if want_open and stack[-1] == want_open:
                stack.pop()
                buf.append(ch)
                i += 1
                continue

            # mismatch: try to recover by auto-closing until we can match
            if want_open and (want_open in stack):
                while stack and stack[-1] != want_open:
                    op = stack.pop()
                    buf.append(_BRACKET_PAIRS.get(op, ""))  # auto-close
                if stack and stack[-1] == want_open:
                    stack.pop()
                    buf.append(ch)
                i += 1
                continue

            # no matching opener in stack -> drop
            i += 1
            continue

        if ch == "," and not stack:
            part = "".join(buf).strip()
            if part:
                parts.append(part)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    # auto-close any remaining openers (defensive; s was repaired already)
    while stack:
        op = stack.pop()
        buf.append(_BRACKET_PAIRS.get(op, ""))

    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts
def _split_commas_lenient(s: str) -> List[str]:
    """Lenient comma splitter.

    Always splits at ',' regardless of brace depth, but keeps numeric emphasis
    blocks (<num>::...::) intact so commas inside them won't be split.
    """
    parts: List[str] = []
    buf: List[str] = []
    i = 0
    n = len(s)
    while i < n:
        m = re.match(r"\s*(-?[0-9]+(?:\.[0-9]+)?)::", s[i:])
        if m:
            start = i
            start_delim_end = i + m.end()
            end = s.find("::", start_delim_end)
            if end != -1:
                pre = "".join(buf).strip()
                if pre:
                    parts.append(pre)
                buf = []

                token = s[start : end + 2].strip()
                if token:
                    parts.append(token)

                i = end + 2
                continue

        ch = s[i]
        if ch == ",":
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


def sanitize_prompt_wrappers(s: str) -> str:
    """Best-effort sanitizer for malformed emphasis wrappers.

    - If closing wrappers are missing, we auto-insert them.
    - If there are too many closing wrappers, we ignore the extras.

    Important detail: When we see a comma while wrappers are open, we may need to
    auto-close some wrappers *before the comma* so that later top-level commas
    still split.

    We only auto-close early when it is mathematically impossible for the
    remaining text to close all currently-open wrappers (based on suffix close
    counts). This avoids breaking valid prompts like "{{a,b}}".

    Numeric emphasis blocks (<num>::...::) are treated as atomic to prevent braces
    inside them from affecting outer splitting.
    """
    if not s:
        return s

    # Suffix counts of remaining closing wrappers.
    rem_curly = [0] * (len(s) + 1)
    rem_square = [0] * (len(s) + 1)
    for i in range(len(s) - 1, -1, -1):
        rem_curly[i] = rem_curly[i + 1] + (1 if s[i] == "}" else 0)
        rem_square[i] = rem_square[i + 1] + (1 if s[i] == "]" else 0)

    out: List[str] = []
    stack: List[str] = []  # '{' or '['
    open_curly = 0
    open_square = 0
    i = 0
    n = len(s)
    while i < n:
        # numeric block: <num>:: ... ::  (keep atomic)
        m = re.match(r"\s*(-?[0-9]+(?:\.[0-9]+)?)::", s[i:])
        if m:
            start = i
            start_delim_end = i + m.end()
            end = s.find("::", start_delim_end)
            if end != -1:
                out.append(s[start : end + 2])
                i = end + 2
                continue

        ch = s[i]

        if ch == "{" or ch == "[":
            stack.append(ch)
            if ch == "{":
                open_curly += 1
            else:
                open_square += 1
            out.append(ch)
            i += 1
            continue

        if ch == "}" or ch == "]":
            want = "{" if ch == "}" else "["
            if stack and stack[-1] == want:
                stack.pop()
                if want == "{":
                    open_curly -= 1
                else:
                    open_square -= 1
                out.append(ch)
            # else: extra closing wrapper => ignore
            i += 1
            continue

        if ch == "," and stack:
            # Consider early auto-close BEFORE this comma only if necessary.
            # Use remaining close counts AFTER this comma.
            r_curly = rem_curly[i + 1]
            r_square = rem_square[i + 1]

            # While we have a deficit of closers for the wrapper type at the top
            # of the stack, auto-close it now.
            while stack:
                top = stack[-1]
                if top == "{" and open_curly > r_curly:
                    stack.pop()
                    open_curly -= 1
                    out.append("}")
                    continue
                if top == "[" and open_square > r_square:
                    stack.pop()
                    open_square -= 1
                    out.append("]")
                    continue
                break

            out.append(ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    # Close anything still open at the end.
    while stack:
        top = stack.pop()
        out.append("}" if top == "{" else "]")

    return "".join(out)

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
    # sanitize once at entry to keep splitting and wrapper stripping resilient
    # against malformed prompts.
    p = sanitize_prompt_wrappers((prompt or "").strip())
    return _parse_segment(p, inherited_brace_level=0)

def _parse_segment(seg: str, inherited_brace_level: int) -> List[ParsedTag]:
    seg = seg.strip()
    seg = _repair_emphasis_brackets(seg)
    if not seg:
        return []

    # Drop empty emphasis garbage like "[[[[[[]]]]]]" or "{{}}".
    if _EMPTY_EMPH_GARBAGE_RE.match(seg):
        return []

    # numeric emphasis (explicit) has top priority
    m = _NUMERIC_RE.match(seg)
    if m:
        w = float(m.group("w"))
        inner = sanitize_prompt_wrappers((m.group("inner") or "")).strip(_TAG_EDGE_STRIP)
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