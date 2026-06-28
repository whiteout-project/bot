import sqlite3
import pytest
from cogs import attendance_ocr_parsers as parsers
from cogs import alliance_power_changes as pc


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    users = tmp_path / "users.sqlite"
    changes = tmp_path / "changes.sqlite"
    with sqlite3.connect(users) as conn:
        conn.execute(
            "CREATE TABLE users (fid INTEGER PRIMARY KEY, power INTEGER, "
            "power_updated_at TEXT, combat_power INTEGER, combat_power_updated_at TEXT)"
        )
        conn.execute("INSERT INTO users (fid, power) VALUES (1, 100)")
        conn.execute("INSERT INTO users (fid) VALUES (2)")  # no prior power
        conn.commit()
    monkeypatch.setattr(parsers, "_USERS_DB", str(users))
    monkeypatch.setattr(pc, "_CHANGES_DB", str(changes))
    pc.ensure_tables()
    return str(users), str(changes)


def test_update_records_change_when_value_differs(dbs):
    parsers.update_users_power(1, 150, "2026-06-27T10:00:00")
    d = pc.latest_delta(1, "power")
    assert d["old"] == 100 and d["new"] == 150


def test_first_ever_power_sets_baseline_no_change_row(dbs):
    parsers.update_users_power(2, 500, "2026-06-27T10:00:00")
    assert pc.latest_delta(2, "power") is None
    with sqlite3.connect(dbs[0]) as conn:
        assert conn.execute("SELECT power FROM users WHERE fid=2").fetchone()[0] == 500


def test_reupload_same_value_no_change_row(dbs):
    parsers.update_users_power(1, 100, "2026-06-27T10:00:00")
    assert pc.latest_delta(1, "power") is None
