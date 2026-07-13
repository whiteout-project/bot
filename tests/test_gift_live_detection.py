"""Live gift-code detection must see crossposts from followed channels.

The setup tip recommends following the official giftcodes channel, but
crossposted announcements arrive as bot-authored webhook messages and
on_message bailed on author.bot - so codes arriving that way were never
detected live (only by a History Scan). Regular bot messages stay ignored.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

import discord

go = importlib.import_module("cogs.gift_operations")
gr = importlib.import_module("cogs.gift_redemption")


def _mk_cog(channel_configured=True):
    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE giftcode_channel (channel_id INTEGER, alliance_id INTEGER)")
    if channel_configured:
        mc.execute("INSERT INTO giftcode_channel VALUES (77, 5)")
    mc.commit()

    cog = go.GiftOperations.__new__(go.GiftOperations)
    cog.cursor = mc.cursor()
    cog.logger = SimpleNamespace(info=lambda *a: None, exception=lambda *a: None)
    return cog


def _msg(content="", embeds=None, bot=True, crosspost=True):
    # Real MessageFlags so an attribute-name drift fails here, not in production.
    return SimpleNamespace(
        author=SimpleNamespace(bot=bot),
        flags=discord.MessageFlags(is_crossposted=crosspost),
        guild=object(),
        channel=SimpleNamespace(id=77),
        content=content,
        embeds=embeds or [],
    )


def _embed(description):
    return SimpleNamespace(title=None, description=description, fields=[])


def _run(cog, message, monkeypatch):
    enqueued = []

    async def fake_enqueue(cog_, code, source, msg_, chan):
        enqueued.append(code)

    monkeypatch.setattr(go.gift_redemption, "enqueue_validation", fake_enqueue)
    asyncio.run(cog.on_message(message))
    return enqueued


def test_crosspost_content_code_is_detected(monkeypatch):
    cog = _mk_cog()
    enqueued = _run(cog, _msg(content="OFFICIALSTORE0709"), monkeypatch)
    assert enqueued == ["OFFICIALSTORE0709"]


def test_crosspost_embed_code_is_detected(monkeypatch):
    cog = _mk_cog()
    msg = _msg(embeds=[_embed("New rewards!\n**Code:** `SUMMER2026`")])
    enqueued = _run(cog, msg, monkeypatch)
    assert enqueued == ["SUMMER2026"]


def test_regular_bot_message_still_ignored(monkeypatch):
    cog = _mk_cog()
    msg = _msg(content="TOTALLYACODE1", crosspost=False)
    enqueued = _run(cog, msg, monkeypatch)
    assert enqueued == []


def test_human_message_still_detected(monkeypatch):
    cog = _mk_cog()
    msg = _msg(content="PLAYERCODE1", bot=False, crosspost=False)
    enqueued = _run(cog, msg, monkeypatch)
    assert enqueued == ["PLAYERCODE1"]
