import importlib
import sqlite3

parsers = importlib.import_module("cogs.attendance_ocr_parsers")


def _mk_db(tmp_path, monkeypatch):
    db = tmp_path / "att.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE attendance_sessions (session_id TEXT PRIMARY KEY, "
        "event_type TEXT, event_date TEXT, event_date_confidence TEXT, "
        "event_subtype TEXT, alliance_id INTEGER, awaiting_result INTEGER, "
        "created_at TEXT DEFAULT CURRENT_TIMESTAMP, closed_at TEXT, "
        "origin TEXT, event_time TEXT)"
    )
    conn.execute(
        "CREATE TABLE attendance_records (record_id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "session_id TEXT NOT NULL, session_name TEXT NOT NULL, event_type TEXT NOT NULL, "
        "event_date TEXT, player_id TEXT NOT NULL, player_name TEXT NOT NULL, "
        "alliance_id TEXT NOT NULL, alliance_name TEXT NOT NULL, status TEXT NOT NULL, "
        "points INTEGER DEFAULT 0, event_subtype TEXT, UNIQUE(session_id, player_id))"
    )
    conn.commit()
    conn.close()
    monkeypatch.setattr(parsers, "_ATT_DB", str(db))
    return str(db)


def test_sync_writes_present_rows_points_equal_damage(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, monkeypatch)
    parsers.sync_bear_attendance_event(
        alliance_id=7, hunt_id=42, date="2026-07-05", hunting_trap=2,
        event_time="23:00", alliance_name="SIR",
        participants=[{"fid": 1, "name": "Alice", "damage": 100},
                      {"fid": 2, "name": "Bob", "damage": 50}])
    conn = sqlite3.connect(db)
    rows = conn.execute(
        "SELECT player_id, status, points, event_type FROM attendance_records "
        "WHERE session_id='bear-42' ORDER BY player_id").fetchall()
    assert rows == [("1", "present", 100, "bear"), ("2", "present", 50, "bear")]
    sess = conn.execute(
        "SELECT event_type, event_time, origin, event_subtype, awaiting_result "
        "FROM attendance_sessions WHERE session_id='bear-42'").fetchone()
    assert sess == ("bear", "23:00", "bear", "Trap 2", 0)
    name_row = conn.execute(
        "SELECT DISTINCT session_name FROM attendance_records "
        "WHERE session_id='bear-42'").fetchone()
    assert name_row[0] == "Trap 2 - 2026-07-05"
    conn.close()


def test_sync_is_idempotent_and_rewrites_rows(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, monkeypatch)
    parsers.sync_bear_attendance_event(
        alliance_id=7, hunt_id=42, date="2026-07-05", hunting_trap=2,
        event_time=None, alliance_name="SIR",
        participants=[{"fid": 1, "name": "Alice", "damage": 100}])
    parsers.sync_bear_attendance_event(
        alliance_id=7, hunt_id=42, date="2026-07-05", hunting_trap=2,
        event_time=None, alliance_name="SIR",
        participants=[{"fid": 2, "name": "Bob", "damage": 50}])
    conn = sqlite3.connect(db)
    ids = [r[0] for r in conn.execute(
        "SELECT player_id FROM attendance_records WHERE session_id='bear-42'").fetchall()]
    assert ids == ["2"]   # rewritten to the second call's participants
    n = conn.execute(
        "SELECT COUNT(*) FROM attendance_sessions WHERE session_id='bear-42'").fetchone()[0]
    assert n == 1
    et = conn.execute(
        "SELECT event_time FROM attendance_sessions WHERE session_id='bear-42'").fetchone()[0]
    assert et is None
    conn.close()


def test_delete_removes_session_and_rows(tmp_path, monkeypatch):
    db = _mk_db(tmp_path, monkeypatch)
    parsers.sync_bear_attendance_event(
        alliance_id=7, hunt_id=42, date="2026-07-05", hunting_trap=1,
        event_time=None, alliance_name="SIR",
        participants=[{"fid": 1, "name": "Alice", "damage": 100}])
    parsers.delete_bear_attendance_event(hunt_id=42)
    conn = sqlite3.connect(db)
    assert conn.execute(
        "SELECT COUNT(*) FROM attendance_records WHERE session_id='bear-42'").fetchone()[0] == 0
    assert conn.execute(
        "SELECT COUNT(*) FROM attendance_sessions WHERE session_id='bear-42'").fetchone()[0] == 0
    conn.close()
