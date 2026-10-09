"""Keyword-less uploads must still reach an open bear session.

An upload of 11+ screenshots arrives as several messages and only one carries
the keyword, so the keyword gate only applies when a new session would start.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

bt = importlib.import_module("cogs.bear_track")


def _mk_cog(routed):
    alliance = sqlite3.connect(":memory:")
    alliance.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, bear_score_channel INTEGER, bear_keywords TEXT)")
    alliance.execute("INSERT INTO alliancesettings VALUES (5, 1, 'bear')")
    cog = bt.BearTrack.__new__(bt.BearTrack)
    cog.alliance_cursor = alliance.cursor()

    async def fake_process(message, *, alliance_id=None):
        routed.append(message)

    cog.process_bear_hunt_data = fake_process
    return cog


def _msg(content=""):
    return SimpleNamespace(
        author=SimpleNamespace(bot=False, id=2),
        channel=SimpleNamespace(id=1),
        content=content,
        attachments=[SimpleNamespace(filename="shot.png")],
    )


def test_keyword_less_upload_joins_open_session(monkeypatch):
    routed = []
    cog = _mk_cog(routed)
    monkeypatch.setattr(bt, "_active_sessions", {(1, 2): object()})

    asyncio.run(cog.on_message(_msg()))

    assert len(routed) == 1


def test_keyword_gate_still_blocks_without_session(monkeypatch):
    routed = []
    cog = _mk_cog(routed)
    monkeypatch.setattr(bt, "_active_sessions", {})

    asyncio.run(cog.on_message(_msg()))
    asyncio.run(cog.on_message(_msg(content="Bear 1")))

    assert len(routed) == 1
