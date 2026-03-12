from __future__ import annotations

import io
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Tuple

from PIL import Image, ImageOps

# AVIF write support depends on Pillow build and optional plugins.
# We probe once at runtime and then avoid repeated exceptions.
_AVIF_PROBED = False
_AVIF_AVAILABLE = False
_AVIF_PROBE_ERROR = ""


@dataclass(frozen=True)
class WebpDerivativeSettings:
    quality: int
    method: int
    lossless: bool
    alpha_quality: int


@dataclass(frozen=True)
class AvifDerivativeSettings:
    enabled: bool
    quality: int
    speed: int
    codec: str
    max_threads: int


@dataclass(frozen=True)
class DerivativeTargetSettings:
    max_side: int
    webp: WebpDerivativeSettings
    avif: AvifDerivativeSettings


def _env_int(name: str, default: int, *, lo: int | None = None, hi: int | None = None) -> int:
    raw = os.getenv(name)
    try:
        value = int(str(raw).strip()) if raw is not None and str(raw).strip() != "" else int(default)
    except Exception:
        value = int(default)
    if lo is not None:
        value = max(int(lo), value)
    if hi is not None:
        value = min(int(hi), value)
    return value


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _env_text(name: str, default: str) -> str:
    raw = os.getenv(name)
    text = str(raw).strip() if raw is not None else ""
    return text or str(default)


def _build_target(kind: str, *, default_max_side: int, default_quality: int) -> DerivativeTargetSettings:
    prefix = f"NAI_IM_DERIV_{kind.upper()}"
    webp_quality = _env_int(f"{prefix}_WEBP_QUALITY", default_quality, lo=0, hi=100)
    webp_alpha_default = min(100, max(30, int(webp_quality) + 10))
    avif_quality = _env_int(f"{prefix}_AVIF_QUALITY", default_quality, lo=0, hi=100)
    avif_codec = _env_text(f"{prefix}_AVIF_CODEC", "auto").strip().lower() or "auto"
    if avif_codec not in {"auto", "aom", "rav1e", "svt"}:
        avif_codec = "auto"
    return DerivativeTargetSettings(
        max_side=_env_int(f"{prefix}_MAX_SIDE", default_max_side, lo=1, hi=16384),
        webp=WebpDerivativeSettings(
            quality=webp_quality,
            method=_env_int(f"{prefix}_WEBP_METHOD", 4, lo=0, hi=6),
            lossless=_env_bool(f"{prefix}_WEBP_LOSSLESS", False),
            alpha_quality=_env_int(f"{prefix}_WEBP_ALPHA_QUALITY", webp_alpha_default, lo=0, hi=100),
        ),
        avif=AvifDerivativeSettings(
            enabled=_env_bool(f"{prefix}_AVIF_ENABLED", True),
            quality=avif_quality,
            speed=_env_int(f"{prefix}_AVIF_SPEED", 8, lo=0, hi=10),
            codec=avif_codec,
            max_threads=_env_int(f"{prefix}_AVIF_MAX_THREADS", 0, lo=0, hi=1024),
        ),
    )


@lru_cache(maxsize=1)
def derivative_targets() -> dict[str, DerivativeTargetSettings]:
    return {
        "grid": _build_target("grid", default_max_side=320, default_quality=70),
        "overlay": _build_target("overlay", default_max_side=1400, default_quality=82),
    }


def derivative_target(kind: str) -> DerivativeTargetSettings:
    key = str(kind or "").strip().lower()
    try:
        return derivative_targets()[key]
    except KeyError as exc:  # pragma: no cover - programming error
        raise ValueError(f"unsupported derivative kind: {kind}") from exc


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


def decode_source_image(src_bytes: bytes) -> Image.Image:
    with Image.open(io.BytesIO(src_bytes)) as im:
        im.load()
        decoded = ImageOps.exif_transpose(im)
        decoded.load()
        return decoded.copy()


def make_resized_variant(base_image: Image.Image, *, max_side: int) -> Image.Image:
    im = base_image.copy()
    w, h = im.size
    scale = min(1.0, max_side / float(max(w, h)))
    nw, nh = max(1, int(round(w * scale))), max(1, int(round(h * scale)))
    if (nw, nh) != (w, h):
        im = im.resize((nw, nh), Image.Resampling.LANCZOS)
    return im


def _normalize_image_for_output(image: Image.Image, *, alpha_ok: bool) -> Image.Image:
    mode = str(image.mode or "")
    if alpha_ok:
        if mode in {"RGBA", "LA"}:
            return image
        if "A" in mode:
            return image.convert("RGBA")
        if mode not in {"RGB", "L"}:
            return image.convert("RGBA")
        return image
    if mode == "RGB":
        return image
    return image.convert("RGB")


def encode_webp_image(
    image: Image.Image,
    *,
    quality: int,
    method: int = 4,
    lossless: bool = False,
    alpha_quality: int | None = None,
) -> bytes:
    out = io.BytesIO()
    image = _normalize_image_for_output(image, alpha_ok=True)
    image.save(
        out,
        format="WEBP",
        quality=int(quality),
        method=max(0, min(6, int(method))),
        lossless=bool(lossless),
        alpha_quality=int(alpha_quality if alpha_quality is not None else min(100, max(30, int(quality) + 10))),
    )
    return out.getvalue()


def encode_avif_image(
    image: Image.Image,
    *,
    quality: int,
    speed: int = 8,
    codec: str = "auto",
    max_threads: int = 0,
) -> bytes:
    ok, err = probe_avif()
    if not ok:
        raise RuntimeError(f"AVIF encode unavailable: {err}")
    out = io.BytesIO()
    image = _normalize_image_for_output(image, alpha_ok=False)
    save_kwargs: dict[str, object] = {
        "format": "AVIF",
        "quality": int(quality),
        "speed": max(0, min(10, int(speed))),
        "codec": (str(codec or "auto").strip().lower() or "auto"),
    }
    threads = int(max_threads or 0)
    if threads > 0:
        save_kwargs["max_threads"] = threads
    image.save(out, **save_kwargs)
    return out.getvalue()


def make_webp_derivative(
    src_bytes: bytes,
    *,
    max_side: int,
    quality: int,
    method: int = 4,
    lossless: bool = False,
    alpha_quality: int | None = None,
) -> Tuple[bytes, int, int]:
    base = decode_source_image(src_bytes)
    try:
        im = make_resized_variant(base, max_side=max_side)
        try:
            return (
                encode_webp_image(
                    im,
                    quality=quality,
                    method=method,
                    lossless=lossless,
                    alpha_quality=alpha_quality,
                ),
                im.size[0],
                im.size[1],
            )
        finally:
            im.close()
    finally:
        base.close()


def make_avif_derivative(
    src_bytes: bytes,
    *,
    max_side: int,
    quality: int,
    speed: int = 8,
    codec: str = "auto",
    max_threads: int = 0,
) -> Tuple[bytes, int, int]:
    base = decode_source_image(src_bytes)
    try:
        im = make_resized_variant(base, max_side=max_side)
        try:
            return (
                encode_avif_image(
                    im,
                    quality=quality,
                    speed=speed,
                    codec=codec,
                    max_threads=max_threads,
                ),
                im.size[0],
                im.size[1],
            )
        finally:
            im.close()
    finally:
        base.close()
