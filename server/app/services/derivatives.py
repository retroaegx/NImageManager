from __future__ import annotations

import io
from typing import Tuple
from PIL import Image, ImageOps

# AVIF write support depends on Pillow build and optional plugins.
# We probe once at runtime and then avoid repeated exceptions.
_AVIF_PROBED = False
_AVIF_AVAILABLE = False
_AVIF_PROBE_ERROR = ""


def probe_avif() -> tuple[bool, str]:
    """Return (available, error).

    This checks whether the *server* can encode AVIF via Pillow.
    (Browser AVIF support is unrelated.)
    """
    global _AVIF_PROBED, _AVIF_AVAILABLE, _AVIF_PROBE_ERROR
    if _AVIF_PROBED:
        return _AVIF_AVAILABLE, _AVIF_PROBE_ERROR
    _AVIF_PROBED = True

    # Best-effort plugin import (the package name is usually pillow_avif).
    try:  # pragma: no cover
        import pillow_avif  # type: ignore  # noqa: F401
    except Exception:
        pass

    try:
        img = Image.new("RGB", (8, 8), (0, 0, 0))
        out = io.BytesIO()
        img.save(out, format="AVIF", quality=50)
        b = out.getvalue()
        if len(b) > 32 and b[4:12] in (b"ftypavif", b"ftypavis"):
            _AVIF_AVAILABLE = True
            _AVIF_PROBE_ERROR = ""
        else:
            _AVIF_AVAILABLE = False
            _AVIF_PROBE_ERROR = "unexpected AVIF output"
    except Exception as e:
        _AVIF_AVAILABLE = False
        _AVIF_PROBE_ERROR = f"{type(e).__name__}: {e}"

    return _AVIF_AVAILABLE, _AVIF_PROBE_ERROR


def avif_available() -> bool:
    return bool(probe_avif()[0])


def avif_probe_error() -> str:
    probe_avif()
    return _AVIF_PROBE_ERROR

def make_webp_derivative(src_bytes: bytes, *, max_side: int, quality: int) -> Tuple[bytes, int, int]:
    with Image.open(io.BytesIO(src_bytes)) as im:
        im = ImageOps.exif_transpose(im)
        w, h = im.size
        scale = min(1.0, max_side / float(max(w, h)))
        nw, nh = int(w * scale), int(h * scale)
        if (nw, nh) != (w, h):
            im = im.resize((nw, nh), Image.Resampling.LANCZOS)
        out = io.BytesIO()
        # NOTE: list thumbnails should be *lossy* (smaller). Make it explicit.
        alpha_q = min(100, max(30, int(quality) + 10))
        im.save(
            out,
            format="WEBP",
            quality=int(quality),
            method=6,
            lossless=False,
            alpha_quality=int(alpha_q),
        )
        return out.getvalue(), im.size[0], im.size[1]


def make_avif_derivative(src_bytes: bytes, *, max_side: int, quality: int) -> Tuple[bytes, int, int]:
    """Best-effort AVIF derivative.

    Pillow can only write AVIF when built with AVIF support (libavif).
    On environments without it, this will raise and callers should fall back.
    """
    ok, err = probe_avif()
    if not ok:
        raise RuntimeError(f"AVIF encode unavailable: {err}")
    with Image.open(io.BytesIO(src_bytes)) as im:
        im = ImageOps.exif_transpose(im)
        w, h = im.size
        scale = min(1.0, max_side / float(max(w, h)))
        nw, nh = int(w * scale), int(h * scale)
        if (nw, nh) != (w, h):
            im = im.resize((nw, nh), Image.Resampling.LANCZOS)
        out = io.BytesIO()
        # Pillow AVIF options vary by build; keep it conservative.
        im.save(out, format="AVIF", quality=int(quality))
        return out.getvalue(), im.size[0], im.size[1]
