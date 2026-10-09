"""Member transfers honor the target alliance's state lock, and the Manage Members list keeps a bot-token handle."""
import asyncio
import sqlite3
import types
from contextlib import closing

import pytest

import cogs.alliance_member_operations as amo


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT, kid INTEGER, state_locked INTEGER)")
        conn.execute("INSERT INTO alliance_list VALUES (1, 'Source', NULL, 0)")
        conn.execute("INSERT INTO alliance_list VALUES (2, 'Locked', 245, 1)")
        conn.execute("INSERT INTO alliance_list VALUES (3, 'Open', 245, 0)")
        conn.commit()
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("""CREATE TABLE users (
            fid INTEGER PRIMARY KEY, nickname TEXT, furnace_lv INTEGER, kid INTEGER, alliance TEXT,
            power INTEGER, power_updated_at TEXT, combat_power INTEGER, combat_power_updated_at TEXT)""")
        conn.executemany("INSERT INTO users (fid, nickname, furnace_lv, kid, alliance) VALUES (?, ?, 30, ?, '1')",
                         [(100, "Home", 245), (200, "Foreign", 999), (300, "Unknown", None)])
        conn.commit()
    return tmp_path


def _alliance_of(fid):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        return conn.execute("SELECT alliance FROM users WHERE fid = ?", (fid,)).fetchone()[0]


def test_split_keeps_only_members_from_the_locked_state(dbs):
    ok, locked_kid, allowed, skipped = amo._split_by_state_lock(2, [100, 200, 300])
    assert (ok, locked_kid, allowed, skipped) == (True, 245, [100], ["Foreign", "Unknown"])


def test_split_allows_everyone_without_a_lock(dbs):
    assert amo._split_by_state_lock(3, [100, 200]) == (True, None, [100, 200], [])


def test_split_fails_closed_when_the_lock_cannot_be_read(monkeypatch):
    monkeypatch.setattr(amo, "resolve_alliance_kid", lambda aid: (False, None))
    assert amo._split_by_state_lock(2, [100]) == (False, None, [], [])


def _interaction(edits):
    async def edit_message(**kwargs):
        edits.append(kwargs)
    return types.SimpleNamespace(user=types.SimpleNamespace(id=1, name="admin"),
                                 response=types.SimpleNamespace(edit_message=edit_message))


async def _transfer(target_id, selected, monkeypatch):
    async def no_log(*a, **k):
        return None
    monkeypatch.setattr(amo, "_post_alliance_log", no_log)
    members = [(100, "Home", 30, 245), (200, "Foreign", 30, 999), (300, "Unknown", 30, None)]
    alliances = [(1, "Source", 3), (2, "Locked", 0), (3, "Open", 0)]
    view = amo.ManageMembersView(members, 1, "Source", types.SimpleNamespace(bot=None, level_mapping={}), 1,
                                 alliances=alliances)
    view.pending_selections = set(selected)
    sent = {}

    async def send_message(**kwargs):
        sent.update(kwargs)
    await view._on_transfer_selected(types.SimpleNamespace(response=types.SimpleNamespace(send_message=send_message)))
    select = sent["view"].children[0]
    select._values = [str(target_id)]
    edits = []
    await select.callback(_interaction(edits))
    return view, edits[-1]["embed"]


def test_bulk_transfer_skips_members_from_another_state(dbs, monkeypatch):
    view, embed = asyncio.run(_transfer(2, [100, 200, 300], monkeypatch))

    assert [_alliance_of(f) for f in (100, 200, 300)] == ["2", "1", "1"]
    assert "Moved **1**" in embed.description
    assert "Skipped **2**" in embed.description and "Foreign" in embed.description
    assert {m["fid"] for m in view.all_members} == {200, 300}


def test_bulk_transfer_moves_nobody_when_all_are_from_another_state(dbs, monkeypatch):
    _, embed = asyncio.run(_transfer(2, [200, 300], monkeypatch))

    assert [_alliance_of(f) for f in (200, 300)] == ["1", "1"]
    assert "No Members Moved" in embed.title and "Skipped **2**" in embed.description


def test_bulk_transfer_refuses_when_the_lock_cannot_be_read(dbs, monkeypatch):
    monkeypatch.setattr(amo, "resolve_alliance_kid", lambda aid: (False, None))
    _, embed = asyncio.run(_transfer(2, [100], monkeypatch))

    assert _alliance_of(100) == "1"
    assert embed.description == amo.STATE_CHECK_UNAVAILABLE


def test_bulk_transfer_into_an_unlocked_alliance_moves_everyone(dbs, monkeypatch):
    _, embed = asyncio.run(_transfer(3, [100, 200], monkeypatch))

    assert [_alliance_of(f) for f in (100, 200)] == ["3", "3"]
    assert "Skipped" not in embed.description


def _message(ephemeral):
    return types.SimpleNamespace(id=42, flags=types.SimpleNamespace(ephemeral=ephemeral))


def test_public_list_message_is_edited_with_the_bot_token():
    partial = object()
    channel = types.SimpleNamespace(get_partial_message=lambda mid: partial if mid == 42 else None)
    interaction = types.SimpleNamespace(channel=channel)
    assert amo._bot_editable_message(interaction, _message(False)) is partial


def test_ephemeral_list_message_keeps_the_interaction_handle():
    message = _message(True)
    channel = types.SimpleNamespace(get_partial_message=lambda mid: object())
    assert amo._bot_editable_message(types.SimpleNamespace(channel=channel), message) is message
