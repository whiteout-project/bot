"""complete_booking must never lose a member's existing appointment.

The reschedule-conflict path deleted the old booking, then queried the users
table on the svs.sqlite cursor (no such table there) - the OperationalError
skipped the re-add, and the uncommitted DELETE was flushed by the next commit
on the connection, silently erasing the appointment.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

mm = importlib.import_module("cogs.minister_menu")
ms = importlib.import_module("cogs.minister_schedule")


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


def test_update_time_list_survives_deleted_account():
    """A booked FID whose game account no longer exists (API returns data=None)
    must render as Unknown, not crash the whole list update."""
    svs, users, alliance = _dbs()
    svs.execute("CREATE TABLE reference (context TEXT, context_id INTEGER)")
    svs.commit()

    cog = ms.MinisterSchedule.__new__(ms.MinisterSchedule)
    cog.svs_cursor = svs.cursor()
    cog.alliance_cursor = alliance.cursor()

    async def fetch(fid):
        return {"data": None}  # not_found account

    cog.fetch_user_data = fetch

    time_list, booked = asyncio.run(cog.update_time_list({"00:00": (123, 5)}))

    joined = "\n".join(time_list)
    assert "Unknown" in joined and "123" in joined


def test_unregistered_user_reports_cleanly():
    svs, users, alliance = _dbs()
    cog, results = _mk_cog(svs, users, alliance)
    inter, sent = _interaction()

    asyncio.run(cog.complete_booking(inter, "Construction", "999", "10:00"))

    assert sent, "must inform the admin via followup (response is already deferred)"
    assert "not registered" in str(sent[0])
    assert svs.execute("SELECT COUNT(*) FROM appointments").fetchone()[0] == 0
