#!/usr/bin/env python3
"""
External OCR handler for the WOS/KS bot.

Runs as a standalone HTTP service that receives image bytes, runs RapidOCR,
and returns the recognised text. The bot offloads all model RAM to this
process by starting with --ext-ocr <url>.

Multiple bots from multiple users on the same network can share one handler.
Concurrent requests are serialised per language via a semaphore sized to
(cpu_count / OCR_NUM_THREADS) so CPU thrashing is avoided under load.

Usage:
    python ocr_handler.py                       # listens on 0.0.0.0:18090
    python ocr_handler.py --port 18090
    python ocr_handler.py --host 127.0.0.1 --port 18090

Install dependencies (once):
    pip install rapidocr pillow numpy aiohttp

Environment variables:
    EXT_OCR_TOKEN     Shared secret. When set, every POST /ocr must carry a
                      matching X-OCR-Token header or receive 401. Leave unset
                      on a trusted LAN. Never pass as a CLI flag (leaks in ps).
    OCR_NUM_THREADS   onnxruntime intra/inter thread count per engine (default 2).
    MAX_OCR_DIM       Longest image edge after resize (default 1600).
    OCR_MAX_QUEUE     Max requests waiting for inference per language before the
                      handler returns 429. Prevents memory growth under spikes
                      (default 16).

API:
    POST /ocr
    Content-Type: application/json
    {
        "image": "<base64-encoded image bytes>",
        "lang":  "en",          # optional, defaults to "en"
        "boxes": false          # optional — set true for box coordinates
    }

    200 OK (text mode):    { "text": "recognised text here" }
    200 OK (boxes mode):   { "results": [ {"text": "foo", "box": [[x,y],...] }, ... ] }
    400  malformed request or undecodable image
    401  missing or wrong X-OCR-Token
    429  inference queue full (shed load)
    500  OCR runtime error
    503  RapidOCR failed to import on this host

    GET /health
    { "ok": true, "engines_loaded": ["en"], "num_threads": 2, "max_dim": 1600,
      "max_queue": 16, "queue_depth": { "en": 0 } }
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

# ── Config from environment ──────────────────────────────────────────────────

# Optional shared-secret auth. When set, every POST /ocr must carry a matching
# X-OCR-Token header or receive 401. Leave unset on a trusted LAN.
AUTH_TOKEN = os.environ.get("EXT_OCR_TOKEN") or None

# onnxruntime thread count per engine. 2 is a good balance on a dedicated host;
# lower to 1 on a constrained box, raise if you have cores to spare.
OCR_NUM_THREADS = int(os.environ.get("OCR_NUM_THREADS", "2"))

# Longest edge in pixels after thumbnail. Above ~1800 px onnxruntime hits
# bad-allocation errors on the second/third image in a session.
MAX_OCR_DIM = int(os.environ.get("MAX_OCR_DIM", "1600"))

# Maximum number of requests queued waiting for an inference slot per language.
# Requests that arrive when the queue is full receive 429 immediately rather
# than piling up in the thread pool silently. 16 is generous for a handful of
# bots; lower it if RAM is tight.
OCR_MAX_QUEUE = int(os.environ.get("OCR_MAX_QUEUE", "16"))

# Derived: how many inference jobs can run concurrently.
# cpu_count() / OCR_NUM_THREADS — at least 1, at most OCR_MAX_QUEUE.
_cpu = os.cpu_count() or 1
OCR_CONCURRENCY = max(1, _cpu // OCR_NUM_THREADS)

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

# ── Engine cache + per-language semaphores ───────────────────────────────────

# One RapidOCR engine per language, loaded on first use and kept for the
# lifetime of the process (dedicated host; no eviction needed).
_ENGINES: dict[str, "RapidOCR"] = {}
_ENGINE_LOCK = asyncio.Lock()

# Per-language semaphore: caps concurrent inference jobs to OCR_CONCURRENCY.
# A separate semaphore per language means English requests don't block Arabic
# ones when each language has a different engine loaded.
_LANG_SEMAPHORES: dict[str, asyncio.Semaphore] = {}
# Tracks how many requests are currently waiting or running per language,
# so handle_ocr can reject early (429) when the queue is full.
_LANG_QUEUE_DEPTH: dict[str, int] = {}


def _get_lang_semaphore(lang: str) -> asyncio.Semaphore:
    if lang not in _LANG_SEMAPHORES:
        _LANG_SEMAPHORES[lang] = asyncio.Semaphore(OCR_CONCURRENCY)
        _LANG_QUEUE_DEPTH[lang] = 0
    return _LANG_SEMAPHORES[lang]


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

# ── Image processing ─────────────────────────────────────────────────────────

def _run_ocr(image_bytes: bytes, engine: "RapidOCR") -> "object":
    """Synchronous OCR call — runs in the thread pool via to_thread."""
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
    """RapidOCR boxes are numpy float32 ndarrays — not JSON serializable.
    Coerce to plain nested Python lists. Already-list input passes through."""
    if hasattr(box, "tolist"):
        return box.tolist()
    return [[float(p[0]), float(p[1])] for p in box]


def _extract_boxes(result) -> list:
    """Extract [{"text":..., "box":[[x,y],...]},...] from a RapidOCR result."""
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

# ── aiohttp web handlers ─────────────────────────────────────────────────────

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
    # ── Auth ──────────────────────────────────────────────────────────────────
    if AUTH_TOKEN is not None and request.headers.get("X-OCR-Token") != AUTH_TOKEN:
        return web.json_response({"error": "unauthorized"}, status=401)

    if not OCR_AVAILABLE:
        return web.json_response(
            {"error": "RapidOCR not available on this host"}, status=503)

    # ── Parse request ─────────────────────────────────────────────────────────
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
        return web.json_response({"results": []} if want_boxes else {"text": ""})

    # ── Queue management ──────────────────────────────────────────────────────
    sem = _get_lang_semaphore(lang)

    # Reject immediately if the queue for this language is already full.
    # waiting = total slots - available slots = what's currently in flight or waiting.
    waiting = OCR_CONCURRENCY - sem._value + _LANG_QUEUE_DEPTH.get(lang, 0)
    if waiting >= OCR_MAX_QUEUE:
        logger.warning(f"Queue full for lang={lang!r} ({waiting} waiting) — shedding request")
        return web.json_response(
            {"error": f"OCR queue full for lang '{lang}', try again shortly"}, status=429)

    _LANG_QUEUE_DEPTH[lang] = _LANG_QUEUE_DEPTH.get(lang, 0) + 1

    # ── Inference ─────────────────────────────────────────────────────────────
    try:
        engine = await _get_engine(lang)
        async with sem:
            result = await asyncio.to_thread(_run_ocr, image_bytes, engine)
    except Image.DecompressionBombError:
        return web.json_response({"error": "image too large"}, status=400)
    except Exception as exc:
        logger.exception("OCR failed")
        return web.json_response({"error": f"OCR error: {exc}"}, status=500)
    finally:
        _LANG_QUEUE_DEPTH[lang] = max(0, _LANG_QUEUE_DEPTH.get(lang, 1) - 1)

    # ── Respond ───────────────────────────────────────────────────────────────
    if want_boxes:
        return web.json_response({"results": _extract_boxes(result)})
    return web.json_response({"text": _extract_text(result)})


async def handle_health(request: "web.Request") -> "web.Response":
    return web.json_response({
        "ok": OCR_AVAILABLE,
        "engines_loaded": list(_ENGINES.keys()),
        "num_threads": OCR_NUM_THREADS,
        "concurrency": OCR_CONCURRENCY,
        "max_dim": MAX_OCR_DIM,
        "max_queue": OCR_MAX_QUEUE,
        "queue_depth": dict(_LANG_QUEUE_DEPTH),
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
    logger.info(f"  Concurrency   : {OCR_CONCURRENCY} (cpu={_cpu})")
    logger.info(f"  Max image dim : {MAX_OCR_DIM}px")
    logger.info(f"  Max queue/lang: {OCR_MAX_QUEUE}")
    logger.info(f"  Auth token    : {'set' if AUTH_TOKEN else 'not set (open)'}")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
