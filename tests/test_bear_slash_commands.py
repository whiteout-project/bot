"""/bear_player_history and /bear_damage_view must not die after deferring.

Two hangs, both ending in an eternal "Bot is thinking...": passing file=None
to followup.send crashes inside discord.py (None is not its MISSING sentinel),
and a malformed free-text date raised ValueError after the defer with no
handler.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

bt = importlib.import_module("cogs.bear_track")


def _interaction():
    sent = []
    followups = []

    async def send_message(*a, **k):
        sent.append((a, k))

    async def defer(*a, **k):
        pass

    async def followup_send(*args, **kwargs):
        assert kwargs.get("file", "absent") is not None, \
            "file=None crashes discord.py's message parameter handling"
        followups.append(kwargs)

    return SimpleNamespace(
        user=SimpleNamespace(id=1),
        response=SimpleNamespace(send_message=send_message, defer=defer),
        followup=SimpleNamespace(send=followup_send),
    ), sent, followups


def _mk_cog():
    users = sqlite3.connect(":memory:")
    users.execute("CREATE TABLE users (fid INTEGER, nickname TEXT)")
    alliance = sqlite3.connect(":memory:")
    alliance.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    alliance.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    alliance.commit()
    bear = sqlite3.connect(":memory:")
    bear.execute("CREATE TABLE bear_hunts (id INTEGER PRIMARY KEY, alliance_id INTEGER, hunting_trap INTEGER, date TEXT)")
    bear.execute("CREATE TABLE bear_player_damage (hunt_id INTEGER, fid INTEGER, damage INTEGER, rank INTEGER)")

    cog = bt.BearTrack.__new__(bt.BearTrack)
    cog.users_cursor = users.cursor()
    cog.alliance_cursor = alliance.cursor()
    cog.bear_cursor = bear.cursor()

    async def allow(*a, **k):
        return True

    cog.check_bear_permission = allow
    return cog


def test_player_history_without_records_replies(monkeypatch):
    cog = _mk_cog()
    inter, sent, followups = _interaction()

    asyncio.run(bt.BearTrack.bear_player_history.callback(
        cog, inter, alliance="5", player="7", hunting_trap=None))

    assert followups, "the no-records embed must actually be delivered"
    assert followups[0].get("embed") is not None


def test_damage_view_rejects_malformed_date(monkeypatch):
    cog = _mk_cog()
    inter, sent, followups = _interaction()

    class _StubView:
        def __init__(self, *a, **k):
            pass

    async def fake_process_view(**kwargs):
        raise AssertionError("must not reach data processing with a bad date")

    monkeypatch.setattr(bt, "BearDamageView", _StubView)
    cog.data_submit = SimpleNamespace(process_view=fake_process_view)

    asyncio.run(bt.BearTrack.bear_damage_view.callback(
        cog, inter, alliance="5", hunting_trap=None,
        from_date="yesterday", to_date=None))

    combined = sent + followups
    assert combined, "user must be told the date is invalid, not left on thinking"
    text = str(combined[0])
    assert "date" in text.lower()


def test_damage_view_without_chart_replies(monkeypatch):
    cog = _mk_cog()
    inter, sent, followups = _interaction()

    class _StubView:
        def __init__(self, *a, **k):
            pass

    async def fake_process_view(**kwargs):
        return SimpleNamespace(title="damage"), None  # embed but no chart file

    monkeypatch.setattr(bt, "BearDamageView", _StubView)
    cog.data_submit = SimpleNamespace(process_view=fake_process_view)

    asyncio.run(bt.BearTrack.bear_damage_view.callback(
        cog, inter, alliance="5", hunting_trap=None,
        from_date=None, to_date=None))

    assert followups, "embed without a chart must still be delivered"
