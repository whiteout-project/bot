"""A failed transfer-notification DM must never abort the alliance sync.

The auto-remove path DM'd the global admin with no guard: an owner with DMs
closed (Forbidden) or a deleted account (NotFound) killed check_agslist
mid-roster - the member was already deleted, the rest of the roster went
unchecked, and ProcessQueue marked the sync failed.
"""
import asyncio
import importlib
import logging
import sqlite3
from types import SimpleNamespace

import discord

asy = importlib.import_module("cogs.alliance_sync")


def _forbidden():
    resp = SimpleNamespace(status=403, reason="Forbidden")
    return discord.Forbidden(resp, "Cannot send messages to this user")


def _mk_cog(fetch_exc=None):
    settings = sqlite3.connect(":memory:")
    settings.execute("CREATE TABLE admin (id INTEGER, is_initial INTEGER)")
    settings.execute("INSERT INTO admin VALUES (42, 1)")
    settings.commit()

    cog = asy.AllianceSync.__new__(asy.AllianceSync)
    cog.cursor_settings = settings.cursor()
    cog.logger = logging.getLogger("test")
    sent = []

    async def fetch_user(uid):
        if fetch_exc is not None:
            raise fetch_exc()
        async def send(msg):
            sent.append(msg)
        return SimpleNamespace(send=send)

    cog.bot = SimpleNamespace(fetch_user=fetch_user)
    return cog, sent


def test_dm_failure_does_not_raise():
    cog, sent = _mk_cog(fetch_exc=_forbidden)

    # Must not raise - a closed-DM admin cannot be allowed to kill the sync.
    asyncio.run(cog._notify_transfer_removal(
        fid=1, old_nickname="Alice", alliance_name="TestAlli",
        old_kid=100, new_kid=200,
    ))
    assert sent == []


def test_dm_success_sends_notice():
    cog, sent = _mk_cog()

    asyncio.run(cog._notify_transfer_removal(
        fid=1, old_nickname="Alice", alliance_name="TestAlli",
        old_kid=100, new_kid=200,
    ))
    assert len(sent) == 1
    assert "Alice" in sent[0] and "TestAlli" in sent[0]
