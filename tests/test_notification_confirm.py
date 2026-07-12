import asyncio
import importlib
from types import SimpleNamespace

import discord

ns = importlib.import_module("cogs.notification_system")


def _forbidden():
    resp = SimpleNamespace(status=403, reason="Forbidden")
    return discord.Forbidden(resp, "Missing Access")


def _mk_cog(*, cached=None, fetch_exc=None, fetched=None):
    """Cog wired so _resolve_send_channel can be driven directly.
    `cached` is what get_channel returns; `fetch_exc()` (if given) is raised by
    fetch_channel, else it returns `fetched`. Throttling is disabled."""
    cog = ns.NotificationSystem.__new__(ns.NotificationSystem)
    cog.channel_confirm_state = {}
    cog.CHANNEL_CONFIRM_INTERVAL = 0          # no throttle in the test
    cog.CHANNEL_CONFIRM_REQUIRED = 3
    calls = []

    def get_channel(cid):
        return cached

    async def fetch_channel(cid):
        if fetch_exc is not None:
            exc = fetch_exc()
            if exc is not None:
                raise exc
        return fetched

    async def pause(**kw):
        calls.append(kw)

    cog.bot = SimpleNamespace(get_channel=get_channel, fetch_channel=fetch_channel)
    cog._pause_and_notify = pause
    return cog, calls


def test_cached_channel_returned_without_fetch():
    sentinel = object()
    cog, calls = _mk_cog(cached=sentinel)
    assert asyncio.run(cog._resolve_send_channel(123)) is sentinel
    assert calls == []


def test_uncached_but_reachable_delivers_via_fetch():
    fetched = object()
    cog, calls = _mk_cog(cached=None, fetch_exc=lambda: None, fetched=fetched)
    assert asyncio.run(cog._resolve_send_channel(123)) is fetched
    assert calls == []
    assert 123 not in cog.channel_confirm_state


def test_pauses_only_after_three_forbidden():
    cog, calls = _mk_cog(cached=None, fetch_exc=_forbidden)
    for _ in range(2):
        assert asyncio.run(cog._resolve_send_channel(123)) is None
        assert calls == []                    # no pause on the 1st or 2nd failure
    assert asyncio.run(cog._resolve_send_channel(123)) is None
    assert calls == [{"channel_id": 123, "reason": "channel_forbidden"}]  # paused once on the 3rd


def test_transient_error_is_not_counted():
    cog, calls = _mk_cog(cached=None, fetch_exc=lambda: RuntimeError("network"))
    for _ in range(5):
        assert asyncio.run(cog._resolve_send_channel(123)) is None
    assert calls == []
    assert cog.channel_confirm_state.get(123, {}).get("fails", 0) == 0
