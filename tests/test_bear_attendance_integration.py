import importlib
import sqlite3

import pytest

bt = importlib.import_module("cogs.bear_track")


def test_ensure_event_time_column_idempotent(tmp_path):
    db = tmp_path / "bear.sqlite"
    conn = sqlite3.connect(db)
    conn.execute(
        "CREATE TABLE bear_hunts (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "alliance_id INTEGER, date TEXT, hunting_trap INTEGER, rallies INTEGER, "
        "total_damage INTEGER)"
    )
    conn.commit()
    bt._ensure_bear_hunts_event_time(conn)   # adds
    bt._ensure_bear_hunts_event_time(conn)   # no-op, must not raise
    cols = [r[1] for r in conn.execute("PRAGMA table_info(bear_hunts)")]
    assert "event_time" in cols
    conn.close()


attendance = importlib.import_module("cogs.attendance")


def test_bear_event_type_display():
    assert attendance.event_type_display("bear") == ("Bear Trap", "🐻")


def test_normalize_event_time():
    assert bt._normalize_event_time("") is None
    assert bt._normalize_event_time("   ") is None
    assert bt._normalize_event_time("23:00") == "23:00"
    assert bt._normalize_event_time("9:05") == "09:05"
    for bad in ("24:00", "12:60", "abc", "1200", "9:5"):
        with pytest.raises(ValueError):
            bt._normalize_event_time(bad)


def test_bear_participants_matched_only():
    rows = [
        {"fid": 1, "nickname": "Alice", "name": "raw1", "damage": 100},
        {"fid": None, "name": "ghost", "damage": 50},
        {"fid": 2, "nickname": None, "name": "Bob", "damage": 25},
    ]
    assert bt._bear_participants(rows) == [
        {"fid": 1, "name": "Alice", "damage": 100},
        {"fid": 2, "name": "Bob", "damage": 25},
    ]
    assert bt._bear_participants(None) == []
