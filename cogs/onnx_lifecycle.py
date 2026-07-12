"""
Lifecycle manager for ONNX/OCR model sessions. Models are loaded on first
acquire and unloaded after a grace period when no caller holds them, so the
bot only pays for memory while OCR work is actively happening.
"""
from __future__ import annotations

import asyncio
import gc
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

import discord
from discord.ext import commands, tasks

logger = logging.getLogger('bot')

DEFAULT_GRACE_SECONDS = 120
SWEEP_INTERVAL_SECONDS = 30

# Boxes at or below this RAM limit get low-memory behaviour: models load one at
# a time and unload the instant they're released, instead of staying warm for a
# grace period. Trades cold-start latency for survival on a 512 MB free plan.
LOW_MEM_THRESHOLD_MB = 768


def _cgroup_limit_paths():
    """Memory-limit files to try: the generic roots plus this process's own
    cgroup from /proc/self/cgroup, where the real limit lives when a container
    shares the host cgroup namespace (common on Pterodactyl/Docker)."""
    yield "/sys/fs/cgroup/memory.max"                     # cgroup v2 root
    yield "/sys/fs/cgroup/memory/memory.limit_in_bytes"   # cgroup v1 root
    try:
        with open("/proc/self/cgroup") as f:
            lines = f.readlines()
    except OSError:
        return
    for line in lines:
        parts = line.strip().split(":", 2)
        if len(parts) != 3 or not parts[2] or parts[2] == "/":
            continue
        controllers, path = parts[1], parts[2]
        if controllers == "":                              # v2 unified "0::/path"
            yield f"/sys/fs/cgroup{path}/memory.max"
        elif "memory" in controllers.split(","):           # v1 memory line
            yield f"/sys/fs/cgroup/memory{path}/memory.limit_in_bytes"


def _detect_memory_limit_mb() -> "int | None":
    """Smallest readable cgroup memory limit in MB, or None if none is set. Reads
    this process's own cgroup too, so a host-namespace container (whose
    /sys/fs/cgroup/memory.max is the unlimited host root) is still detected."""
    best = None
    for path in _cgroup_limit_paths():
        try:
            with open(path) as f:
                raw = f.read().strip()
        except OSError:
            continue
        if not raw or raw == "max":
            continue
        try:
            val = int(raw)
        except ValueError:
            continue
        # cgroup v1 reports a near-INT64 sentinel when memory is unlimited.
        if 0 < val < (1 << 62):
            mb = val // (1024 * 1024)
            if best is None or mb < best:
                best = mb
    return best


def _resolve_low_mem() -> "tuple[bool, int | None]":
    """Return (low_mem_mode, detected_limit_mb). The BOT_LOW_MEM env var forces
    the mode on (1/true/yes/on) or off (0/false/no/off) for self-hosters who
    know their box better than the cgroup reading does."""
    limit = _detect_memory_limit_mb()
    override = os.environ.get("BOT_LOW_MEM", "").strip().lower()
    if override in ("1", "true", "yes", "on"):
        return True, limit
    if override in ("0", "false", "no", "off"):
        return False, limit
    return (limit is not None and limit <= LOW_MEM_THRESHOLD_MB), limit


LOW_MEM_MODE, MEM_LIMIT_MB = _resolve_low_mem()

if LOW_MEM_MODE:
    logger.info(
        f"Low-memory mode ON (limit={MEM_LIMIT_MB or '?'} MB): OCR engines load "
        "one at a time and unload immediately after use."
    )
    _msg = "OCR engines set for low-memory mode" + (f" ({MEM_LIMIT_MB} MB)" if MEM_LIMIT_MB else "")
    try:
        from . import bot_startup_display as _startup
        _startup.phase_ok(_msg)
    except Exception:
        print(f"  {_msg}", flush=True)
else:
    logger.info(
        f"Low-memory mode OFF (detected limit={MEM_LIMIT_MB or 'none'} MB)."
    )


_REGISTRY: dict[str, "LazyOnnxModel"] = {}


async def _evict_other_idle_models(keep_name: str) -> None:
    """Evict every other non-pinned model that's loaded but unused (refcount 0),
    so at most one OCR engine stays resident on a low-memory box."""
    drained = False
    for name, m in list(_REGISTRY.items()):
        if name == keep_name or m.pinned:
            continue
        async with m._lock:
            if m._model is not None and m._refcount == 0:
                m._model = None
                m._unload_pending_since = None
                logger.info(f"OCR model evicted (low-mem, single-engine): {name}")
                drained = True
    if drained:
        await _drain_and_collect()


async def _drain_and_collect() -> None:
    """Force idle to_thread workers to drop the last engine they touched, then
    collect. Workers cache their previous task's result until they pick up
    another task, so a no-op submission releases the stale reference."""
    try:
        await asyncio.to_thread(lambda: None)
    except Exception:
        pass
    gc.collect()


def get_status_lines() -> list[dict]:
    """Snapshot of every registered model for the Health dashboard."""
    return [m.status() for m in _REGISTRY.values()]


def get_or_create(
    name: str,
    display_name: str,
    factory: Callable[[], Any],
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
    pinned: bool = False,
) -> "LazyOnnxModel":
    """Return the registered model with this name, creating it if missing."""
    if name in _REGISTRY:
        return _REGISTRY[name]
    return LazyOnnxModel(name, display_name, factory, grace_seconds, pinned)


class LazyOnnxModel:
    """Reference-counted lazy-loaded model wrapper.

    First `acquire()` triggers the factory, returning the loaded model.
    Subsequent acquires reuse it. When refcount hits zero, eviction is
    scheduled and runs on the next sweep tick after the grace period.

    When `pinned=True`, the model is exempt from eviction — it loads on
    first use and stays loaded for the bot's lifetime. Use this for small,
    frequently-used models (e.g. the captcha solver) where the cold-start
    cost outweighs the memory savings."""

    def __init__(
        self,
        name: str,
        display_name: str,
        factory: Callable[[], Any],
        grace_seconds: int = DEFAULT_GRACE_SECONDS,
        pinned: bool = False,
    ):
        if name in _REGISTRY:
            raise ValueError(f"Duplicate ONNX model name: {name}")
        self.name = name
        self.display_name = display_name
        self.pinned = pinned
        self._factory = factory
        self._grace = timedelta(seconds=grace_seconds)
        self._model: Any = None
        self._refcount = 0
        self._last_used: datetime | None = None
        self._last_loaded: datetime | None = None
        self._unload_pending_since: datetime | None = None
        self._lock = asyncio.Lock()
        _REGISTRY[name] = self

    async def acquire(self):
        """Increment refcount. Loads the model if it isn't already."""
        async with self._lock:
            if self._model is None:
                logger.info(f"OCR model loading: {self.name}")
                self._model = await asyncio.to_thread(self._factory)
                self._last_loaded = datetime.now(timezone.utc)
            self._refcount += 1
            self._last_used = datetime.now(timezone.utc)
            self._unload_pending_since = None
            model = self._model
        if LOW_MEM_MODE and not self.pinned:
            # Keep only one OCR engine resident on low-memory boxes: drop any
            # other idle engine so a multi-language fallback run (en→arabic→…)
            # runs sequentially in memory instead of stacking 5 engines and
            # OOM-killing the container.
            await _evict_other_idle_models(self.name)
        return model

    async def release(self) -> None:
        """Decrement refcount. When it reaches zero, schedule unload."""
        async with self._lock:
            if self._refcount > 0:
                self._refcount -= 1
            self._last_used = datetime.now(timezone.utc)
            if self._refcount == 0:
                self._unload_pending_since = self._last_used

    @asynccontextmanager
    async def use(self):
        """Short-form auto acquire/release. Use for one-shot calls."""
        model = await self.acquire()
        try:
            yield model
        finally:
            await self.release()

    async def maybe_unload(self) -> bool:
        """Evict if idle past grace. Sweeper calls this periodically.
        Pinned models are never unloaded."""
        if self.pinned:
            return False
        async with self._lock:
            if (
                self._model is None
                or self._refcount > 0
                or self._unload_pending_since is None
            ):
                return False
            elapsed = datetime.now(timezone.utc) - self._unload_pending_since
            if elapsed < self._grace:
                return False
            self._model = None
            self._unload_pending_since = None
            logger.info(f"OCR model unloaded (idle): {self.name}")
        await _drain_and_collect()
        return True

    def status(self) -> dict:
        return {
            'name': self.name,
            'display_name': self.display_name,
            'loaded': self._model is not None,
            'pinned': self.pinned,
            'refcount': self._refcount,
            'last_used': self._last_used,
            'last_loaded': self._last_loaded,
        }


class OnnxLifecycle(commands.Cog):
    """Background sweeper that unloads idle models past their grace window."""

    def __init__(self, bot):
        self.bot = bot
        self.sweeper.start()

    async def cog_unload(self):
        if self.sweeper.is_running():
            self.sweeper.cancel()
        # Force-evict idle models on unload so memory is freed before reload. Active models left alone.
        for model in list(_REGISTRY.values()):
            try:
                async with model._lock:
                    if model._model is not None and model._refcount == 0:
                        model._model = None
                        model._unload_pending_since = None
                        logger.info(f"OCR model force-unloaded on cog unload: {model.name}")
            except Exception as e:
                logger.warning(f"Force-unload error ({model.name}): {e}")
        try:
            await asyncio.to_thread(lambda: None)
        except Exception:
            pass
        gc.collect()

    @tasks.loop(seconds=SWEEP_INTERVAL_SECONDS)
    async def sweeper(self):
        for model in list(_REGISTRY.values()):
            try:
                await model.maybe_unload()
            except Exception as e:
                logger.warning(f"OCR model sweep error ({model.name}): {e}")

    @sweeper.before_loop
    async def _before_sweeper(self):
        await self.bot.wait_until_ready()


async def setup(bot):
    await bot.add_cog(OnnxLifecycle(bot))
