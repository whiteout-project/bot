#!/usr/bin/env python3
"""
External OCR handler for the WOS/KS bot.

Runs as a standalone HTTP service that receives image bytes, runs RapidOCR,
and returns the recognised text. The bot offloads all model RAM to this
process by starting with --ext-ocr <url>.

Usage:
    python ocr_handler.py                       # listens on 0.0.0.0:18090
    python ocr_handler.py --port 18090
    python ocr_handler.py --host 127.0.0.1 --port 18090

Install dependencies (once):
    pip install rapidocr pillow numpy aiohttp aiohttp-web

API:
    POST /ocr
    Content-Type: application/json
    {
        "image": "<base64-encoded image bytes>",
        "lang":  "en",          # optional, defaults to "en"
        "boxes": false          # optional — set true for box coordinates
    }

    200 OK (text mode):
    { "text": "recognised text here" }

    200 OK (boxes mode):
    { "results": [ {"text": "foo", "box": [[x1,y1],[x2,y2],[x3,y3],[x4,y4]]}, ... ] }

    503 if OCR is not available (import failed).
    400 if the request is malformed or the image cannot be decoded.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import io
import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("ocr_handler")

# ── RapidOCR setup ──────────────────────────────────────────────────────────

try:
    import numpy as np
    from PIL import Image
    # Cap decoded image size to defend against decompression bombs: a tiny
    # crafted PNG can expand to billions of pixels and OOM the host. Game
    # screenshots are well under 10 MP; 40 MP leaves generous headroom.
    Image.MAX_IMAGE_PIXELS = 40_000_000
    from rapidocr import RapidOCR, LangRec
    try:
        from rapidocr.utils.download_file import DownloadFile
        DownloadFile.check_is_atty = staticmethod(lambda: False)
    except Exception:
        pass
    OCR_AVAILABLE = True
    logger.info("RapidOCR loaded successfully — handler ready.")
except Exception as exc:
    OCR_AVAILABLE = False
    logger.error(f"RapidOCR unavailable: {exc}")

# Optional shared-secret auth. If EXT_OCR_TOKEN is set in this process's env,
# requests must carry a matching `X-OCR-Token` header (or be rejected 401).
# Leave it unset for a trusted LAN; set it when the handler is reachable by
# other tenants on shared infrastructure.
AUTH_TOKEN = os.environ.get("EXT_OCR_TOKEN") or None

# Thread count: 2 is a good balance for a dedicated OCR host.
# Raise it if you have cores to spare; lower to 1 for very constrained boxes.
OCR_NUM_THREADS = int(os.environ.get("OCR_NUM_THREADS", "2"))
MAX_OCR_DIM = int(os.environ.get("MAX_OCR_DIM", "1600"))

# Per-language engine cache (one engine per lang, loaded on demand)
_ENGINES: dict[str, "RapidOCR"] = {}
_ENGINE_LOCK = asyncio.Lock()


def _make_engine(lang: str) -> "RapidOCR":
    return RapidOCR(params={
        "Rec.lang_type": LangRec(lang),
        "EngineConfig.onnxruntime.intra_op_num_threads": OCR_NUM_THREADS,
        "EngineConfig.onnxruntime.inter_op_num_threads": OCR_NUM_THREADS,
    })


async def _get_engine(lang: str) -> "RapidOCR":
    """Return a cached engine for `lang`, creating it if needed (thread-safe)."""
    async with _ENGINE_LOCK:
        if lang not in _ENGINES:
            logger.info(f"Loading OCR engine for lang={lang!r} …")
            engine = await asyncio.to_thread(_make_engine, lang)
            _ENGINES[lang] = engine
            logger.info(f"Engine for lang={lang!r} ready.")
        return _ENGINES[lang]


def _run_ocr(image_bytes: bytes, engine: "RapidOCR") -> "object":
    """Synchronous OCR call — runs in a thread pool."""
    with Image.open(io.BytesIO(image_bytes)) as src:
        image = src.convert("RGB")
    if max(image.size) > MAX_OCR_DIM:
        image.thumbnail((MAX_OCR_DIM, MAX_OCR_DIM), Image.LANCZOS)
    return engine(np.array(image))


def _extract_text(result) -> str:
    if not result:
        return ""
    if hasattr(result, "txts") and result.txts:
        return " ".join(str(t) for t in result.txts)
    if hasattr(result, "__iter__"):
        texts = [str(item[1]) for item in result
                 if isinstance(item, (list, tuple)) and len(item) >= 2]
        return " ".join(texts) if texts else str(result)
    return str(result)


def _to_list(box):
    """RapidOCR boxes are numpy arrays (float32) — not JSON serializable.
    Coerce to nested Python lists. Already-list input passes through."""
    if hasattr(box, "tolist"):
        return box.tolist()
    return [[float(p[0]), float(p[1])] for p in box]


def _extract_boxes(result) -> list:
    """Extract [{"text":..., "box":...}, ...] from a RapidOCR result, with
    boxes coerced to plain nested lists so json.dumps can serialize them."""
    if not result:
        return []
    if hasattr(result, "txts") and hasattr(result, "boxes") and result.txts:
        return [{"text": str(t), "box": _to_list(b)}
                for t, b in zip(result.txts, result.boxes)]
    if hasattr(result, "__iter__"):
        out = []
        for item in result:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                out.append({"text": str(item[1]), "box": _to_list(item[0])})
        return out
    return []


# ── aiohttp web handler ──────────────────────────────────────────────────────

try:
    from aiohttp import web
except ImportError:
    logger.error("aiohttp is not installed. Run: pip install aiohttp")
    sys.exit(1)

VALID_LANGS = {
    "en", "ch", "japan", "korean", "chinese_cht",
    "latin", "arabic", "cyrillic", "devanagari",
}


async def handle_ocr(request: "web.Request") -> "web.Response":
    if AUTH_TOKEN is not None and request.headers.get("X-OCR-Token") != AUTH_TOKEN:
        return web.json_response({"error": "unauthorized"}, status=401)

    if not OCR_AVAILABLE:
        return web.json_response(
            {"error": "RapidOCR not available on this host"},
            status=503,
        )

    try:
        body = await request.json()
    except Exception:
        return web.json_response({"error": "invalid JSON body"}, status=400)

    raw_image = body.get("image")
    if not raw_image:
        return web.json_response({"error": "missing 'image' field"}, status=400)

    lang = str(body.get("lang", "en"))
    if lang not in VALID_LANGS:
        lang = "en"

    want_boxes = bool(body.get("boxes", False))

    try:
        image_bytes = base64.b64decode(raw_image)
    except Exception:
        return web.json_response({"error": "image is not valid base64"}, status=400)

    if not image_bytes:
        if want_boxes:
            return web.json_response({"results": []})
        return web.json_response({"text": ""})

    try:
        engine = await _get_engine(lang)
        result = await asyncio.to_thread(_run_ocr, image_bytes, engine)
    except Image.DecompressionBombError:
        return web.json_response({"error": "image too large"}, status=400)
    except Exception as exc:
        logger.exception("OCR failed")
        return web.json_response({"error": f"OCR error: {exc}"}, status=500)

    if want_boxes:
        return web.json_response({"results": _extract_boxes(result)})
    return web.json_response({"text": _extract_text(result)})


async def handle_health(request: "web.Request") -> "web.Response":
    return web.json_response({
        "ok": OCR_AVAILABLE,
        "engines_loaded": list(_ENGINES.keys()),
        "num_threads": OCR_NUM_THREADS,
        "max_dim": MAX_OCR_DIM,
    })


def build_app() -> "web.Application":
    app = web.Application(client_max_size=20 * 1024 * 1024)  # 20 MB max image
    app.router.add_post("/ocr", handle_ocr)
    app.router.add_get("/health", handle_health)
    return app


# ── Entry point ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="External OCR handler for the WOS/KS bot")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=18090, help="Bind port (default: 18090)")
    args = parser.parse_args()

    app = build_app()
    logger.info(f"Starting OCR handler on {args.host}:{args.port}")
    logger.info(f"  OCR available : {OCR_AVAILABLE}")
    logger.info(f"  Threads/engine: {OCR_NUM_THREADS}")
    logger.info(f"  Max image dim : {MAX_OCR_DIM}px")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
