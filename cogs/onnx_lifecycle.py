"""
Lifecycle manager for ONNX/OCR model sessions. Models are loaded on first
acquire and unloaded after a grace period when no caller holds them, so the
bot only pays for memory while OCR work is actively happening.
"""
from __future__ import annotations

import asyncio
import gc
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Any, Callable

import discord
from discord.ext import commands, tasks

logger = logging.getLogger('bot')

DEFAULT_GRACE_SECONDS = 120
SWEEP_INTERVAL_SECONDS = 30


_REGISTRY: dict[str, "LazyOnnxModel"] = {}


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
            return self._model

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
        # asyncio.to_thread workers cache the previous task's result until they
        # pick up another task; submitting a no-op forces them to drop the
        # stale reference so the engine memory is actually released.
        try:
            await asyncio.to_thread(lambda: None)
        except Exception:
            pass
        gc.collect()
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

    def cog_unload(self):
        if self.sweeper.is_running():
            self.sweeper.cancel()

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
