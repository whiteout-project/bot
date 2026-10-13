"""complete_booking must never lose a member's existing appointment.

The reschedule-conflict path deleted the old booking, then queried the users
table on the svs.sqlite cursor (no such table there) - the OperationalError
skipped the re-add, and the uncommitted DELETE was flushed by the next commit
on the connection, silently erasing the appointment.
"""
import asyncio
import importlib
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from harness import make_player_sync_dbs

mm = importlib.import_module("cogs.minister_menu")
ms = importlib.import_module("cogs.minister_schedule")
member_edit = importlib.import_module("cogs.alliance_member_edit")


def _dbs():
    svs = sqlite3.connect(":memory:")
    svs.execute("""CREATE TABLE appointments (
        fid INTEGER, appointment_type TEXT, time TEXT, alliance INTEGER,
        PRIMARY KEY (fid, appointment_type))""")
    svs.commit()

    users = sqlite3.connect(":memory:")
    users.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance INTEGER)")
    users.executemany("INSERT INTO users VALUES (?,?,?)",
                      [(1, "Alice", 5), (2, "Bob", 5)])
    users.commit()

    alliance = sqlite3.connect(":memory:")
    alliance.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    alliance.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    alliance.commit()
    return svs, users, alliance


def _mk_cog(svs, users, alliance):
    cog = mm.MinisterMenu.__new__(mm.MinisterMenu)
    cog.svs_conn = svs
    cog.svs_cursor = svs.cursor()
    cog.users_cursor = users.cursor()
    cog.alliance_cursor = alliance.cursor()
    cog.bot = SimpleNamespace(get_cog=lambda name: None)
    results = []

    async def fetch_user_data(fid):
        return {"data": None}

    async def show_filtered(interaction, activity, msg, is_error=False):
        results.append((msg, is_error))

    cog.fetch_user_data = fetch_user_data
    cog.show_filtered_user_select_with_message = show_filtered
    return cog, results


def _interaction():
    sent = []

    async def followup_send(*a, **k):
        sent.append(a or k)

    return SimpleNamespace(
        response=SimpleNamespace(is_done=lambda: True),
        followup=SimpleNamespace(send=followup_send),
        user=SimpleNamespace(display_name="Admin", avatar=None),
    ), sent


def test_conflict_on_reschedule_keeps_old_booking():
    svs, users, alliance = _dbs()
    svs.execute("INSERT INTO appointments VALUES (1, 'Construction', '10:00', 5)")
    svs.execute("INSERT INTO appointments VALUES (2, 'Construction', '14:00', 5)")
    svs.commit()
    cog, results = _mk_cog(svs, users, alliance)
    inter, sent = _interaction()

    asyncio.run(cog.complete_booking(inter, "Construction", "1", "14:00"))

    assert results and results[0][1] is True, "must return to selection with an error"
    assert "14:00" in results[0][0]
    # The next commit on svs_conn must not flush a leftover DELETE.
    svs.commit()
    row = svs.execute(
        "SELECT time FROM appointments WHERE fid=1 AND appointment_type='Construction'"
    ).fetchone()
    assert row == ("10:00",), "conflict must leave the old booking untouched"


def test_repick_own_slot_succeeds():
    svs, users, alliance = _dbs()
    svs.execute("INSERT INTO appointments VALUES (1, 'Construction', '10:00', 5)")
    svs.commit()
    cog, results = _mk_cog(svs, users, alliance)
    inter, sent = _interaction()

    asyncio.run(cog.complete_booking(inter, "Construction", "1", "10:00"))

    assert results and results[0][1] is False, f"own slot re-pick must not error: {results}"
    row = svs.execute(
        "SELECT time FROM appointments WHERE fid=1 AND appointment_type='Construction'"
    ).fetchone()
    assert row == ("10:00",)


def test_reschedule_moves_booking():
    svs, users, alliance = _dbs()
    svs.execute("INSERT INTO appointments VALUES (1, 'Construction', '10:00', 5)")
    svs.commit()
    cog, results = _mk_cog(svs, users, alliance)
    inter, sent = _interaction()

    asyncio.run(cog.complete_booking(inter, "Construction", "1", "11:00"))

    assert results and results[0][1] is False
    rows = svs.execute(
        "SELECT time FROM appointments WHERE fid=1 AND appointment_type='Construction'"
    ).fetchall()
    assert rows == [("11:00",)], "old slot must be replaced by the new one"


def test_generate_time_list_survives_deleted_account():
    """A booked FID with no stored nickname must still render (by ID),
    not crash the whole list."""
    svs, users, alliance = _dbs()
    svs.execute("CREATE TABLE reference (context TEXT, context_id INTEGER)")
    svs.commit()

    cog = ms.MinisterSchedule.__new__(ms.MinisterSchedule)
    cog.svs_cursor = svs.cursor()
    cog.users_cursor = users.cursor()
    cog.alliance_cursor = alliance.cursor()

    time_list, booked = cog.generate_time_list({"00:00": (123, 5)})

    joined = "\n".join(time_list)
    assert "123" in joined


def test_unregistered_user_reports_cleanly():
    svs, users, alliance = _dbs()
    cog, results = _mk_cog(svs, users, alliance)
    inter, sent = _interaction()

    asyncio.run(cog.complete_booking(inter, "Construction", "999", "10:00"))

    assert sent, "must inform the admin via followup (response is already deferred)"
    assert "not registered" in str(sent[0])
    assert svs.execute("SELECT COUNT(*) FROM appointments").fetchone()[0] == 0


# --- Player Sync in the minister menus ------------------------------------------------------

@pytest.fixture
def sync_dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make_player_sync_dbs(tmp_path)


def _stored_user(fid, avatar_url=None):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("INSERT INTO users (fid, nickname, avatar_url) VALUES (?, 'N', ?)", (fid, avatar_url))
        conn.commit()


def test_log_avatar_uses_the_synced_picture(sync_dbs):
    _stored_user(5, "https://i.example.com/5.png")
    assert asyncio.run(member_edit.log_avatar_url(5)) == "https://i.example.com/5.png"


def test_log_avatar_falls_back_to_the_default(sync_dbs):
    _stored_user(6)
    assert asyncio.run(member_edit.log_avatar_url(6)) == member_edit.DEFAULT_LOG_AVATAR
    assert asyncio.run(member_edit.log_avatar_url(404)) == member_edit.DEFAULT_LOG_AVATAR


def test_log_avatar_falls_back_when_the_database_errors(sync_dbs, monkeypatch):
    def broken(fid):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(member_edit, "_stored_avatar_url", broken)
    assert asyncio.run(member_edit.log_avatar_url(5)) == member_edit.DEFAULT_LOG_AVATAR


@pytest.mark.parametrize("enabled, name_field, expected", [
    (True, "nickname", "Player Sync fills them in"),
    (True, "", "Manually set"),
    (False, "nickname", "Manually set"),
])
def test_names_guide_promises_names_only_when_auto_sync_fills_them(sync_dbs, enabled, name_field, expected):
    from cogs import alliance_api_sync as sync
    sync.save_setting("profile", {"base_url": "https://api.example", "player_path": "/p/{id}",
                       "fields": {"name": name_field, "level": "lv"}})
    sync.save_setting("enabled", enabled)
    assert expected in asyncio.run(mm.update_names_guide())
