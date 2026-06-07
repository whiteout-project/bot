"""Per-player attendance history query layer — temp-DB unit tests.

Exercises the read-only queries in `cogs/attendance_history.py` against
throwaway SQLite files (the module's DB paths are monkeypatched), so lookup by
ID, former-member inclusion, unmatched exclusion, timeline ordering, the
ratio-vs-count summary, and the power/CP header all stay correct.
"""
from __future__ import annotations

import sqlite3

import cogs.attendance_history as ah


def _setup_dbs(tmp_path, monkeypatch):
    att = tmp_path / "attendance.sqlite"
    users = tmp_path / "users.sqlite"
    with sqlite3.connect(att) as c:
        c.execute(
            "CREATE TABLE attendance_records ("
            "record_id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, "
            "session_name TEXT, event_type TEXT, event_date TEXT, player_id TEXT, "
            "player_name TEXT, alliance_id TEXT, alliance_name TEXT, status TEXT, "
            "points INTEGER, event_subtype TEXT, alliance_rank INTEGER)"
        )
        rows = [
            ("foundry_battle", "2026-05-01", "100", "Alice", "present", 500, "Legion 1", None),
            ("foundry_battle", "2026-05-08", "100", "Alice", "absent", 0, "Legion 1", None),
            ("canyon_clash", "2026-05-05", "100", "Alice", "present", 300, "Legion 1", None),
            ("alliance_showdown", "2026-05-10", "100", "Alice", "present", 900, None, 3),
            ("foundry_battle", "2026-05-01", "200", "Bob", "present", 400, "Legion 1", None),
            # unmatched placeholder (negative id) — excluded from the picker
            ("foundry_battle", "2026-05-01", "-5", "Ghost", "needs_review", 0, "Legion 1", None),
        ]
        for et, d, pid, nm, st, pts, sub, rk in rows:
            c.execute(
                "INSERT INTO attendance_records (session_id, session_name, event_type, "
                "event_date, player_id, player_name, alliance_id, alliance_name, status, "
                "points, event_subtype, alliance_rank) "
                "VALUES ('s', 's', ?, ?, ?, ?, '1', 'A', ?, ?, ?, ?)",
                (et, d, pid, nm, st, pts, sub, rk),
            )
        c.commit()
    with sqlite3.connect(users) as c:
        c.execute(
            "CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, alliance TEXT, "
            "power INTEGER, power_updated_at TEXT, combat_power INTEGER, "
            "combat_power_updated_at TEXT)"
        )
        c.execute("INSERT INTO users VALUES (100, 'Alice', '1', 120000000, "
                  "'2026-05-10T00:00:00', 45000000, '2026-05-10T00:00:00')")
        # Bob transferred to alliance 2 — still has alliance-1 history.
        c.execute("INSERT INTO users VALUES (200, 'Bob', '2', 90000000, "
                  "'2026-05-01T00:00:00', NULL, NULL)")
        c.commit()
    monkeypatch.setattr(ah, "ATTENDANCE_DB", str(att))
    monkeypatch.setattr(ah, "USERS_DB", str(users))


def test_history_players_includes_former_excludes_unmatched(tmp_path, monkeypatch):
    _setup_dbs(tmp_path, monkeypatch)
    players = ah.history_players(1)
    by = {p["fid"]: p for p in players}
    assert set(by) == {100, 200}          # unmatched -5 excluded
    assert by[100]["current"] is True
    assert by[200]["current"] is False    # transferred → former
    assert by[100]["events"] == 4


def test_player_timeline_is_descending(tmp_path, monkeypatch):
    _setup_dbs(tmp_path, monkeypatch)
    tl = ah.player_timeline(1, 100)
    assert len(tl) == 4
    dates = [r["event_date"] for r in tl]
    assert dates == sorted(dates, reverse=True)


def test_summarize_ratio_vs_count(tmp_path, monkeypatch):
    _setup_dbs(tmp_path, monkeypatch)
    s = ah.summarize(ah.player_timeline(1, 100))
    assert s["foundry_battle"]["present"] == 1
    assert s["foundry_battle"]["absent"] == 1
    assert s["alliance_showdown"]["count"] == 1


def test_player_power_header(tmp_path, monkeypatch):
    _setup_dbs(tmp_path, monkeypatch)
    p = ah.player_power(100)
    assert p["power"] == 120000000
    assert p["combat_power"] == 45000000
    assert p["nickname"] == "Alice"


def test_event_label_other_shows_session_name():
    assert ah._event_label("Other", None, "Test 3") == "Other (Test 3)"
    assert ah._event_label("Other", None, None) == "Other"
    assert ah._event_label("Other", None, "Other") == "Other"   # no redundant bracket
    # Named event types ignore session_name and keep their own label.
    assert ah._event_label("canyon_clash", "Legion 1", "x") == "Canyon Clash · Legion 1"


def test_empty_alliance_returns_nothing(tmp_path, monkeypatch):
    _setup_dbs(tmp_path, monkeypatch)
    assert ah.history_players(999) == []
    assert ah.player_timeline(999, 100) == []
