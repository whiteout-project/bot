"""Power and Combat Power set by hand through single edit, bulk lines and CSV import."""
import asyncio
import importlib
import sqlite3
from contextlib import closing

import pytest

edit = importlib.import_module("cogs.alliance_member_edit")
ops = importlib.import_module("cogs.alliance_member_operations")
id_channel = importlib.import_module("cogs.alliance_id_channel")

MODAL_LABEL_LIMIT = 45
MODAL_FIELD_LIMIT = 5


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("""CREATE TABLE users (
            fid INTEGER PRIMARY KEY, nickname TEXT, furnace_lv INTEGER, kid INTEGER,
            stove_lv_content TEXT, alliance TEXT, power INTEGER, power_updated_at TEXT,
            combat_power INTEGER, combat_power_updated_at TEXT)""")
        conn.execute("INSERT INTO users (fid, nickname, furnace_lv, alliance, power, combat_power) "
                     "VALUES (100, 'Alpha', 30, '1', 134512345, 20100000)")
        conn.execute("INSERT INTO users (fid, nickname, furnace_lv, alliance) "
                     "VALUES (200, 'Bravo', 30, '1')")
        conn.execute("INSERT INTO users (fid, nickname, furnace_lv, alliance, power) "
                     "VALUES (300, 'Other', 30, '2', 5000000)")
        conn.commit()
    with closing(sqlite3.connect("db/changes.sqlite")) as conn:
        conn.execute("CREATE TABLE nickname_changes (fid, old_nickname, new_nickname, change_date)")
        conn.execute("CREATE TABLE furnace_changes (fid, old_furnace_lv, new_furnace_lv, change_date)")
        conn.commit()
    return tmp_path


def _user(fid, *cols):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        return conn.execute(f"SELECT {', '.join(cols)} FROM users WHERE fid = ?", (fid,)).fetchone()


def _changes(table):
    with closing(sqlite3.connect("db/changes.sqlite")) as conn:
        exists = conn.execute("SELECT 1 FROM sqlite_master WHERE name = ?", (table,)).fetchone()
        return conn.execute(f"SELECT * FROM {table}").fetchall() if exists else []


# --- parse_power ---

@pytest.mark.parametrize("text, expected", [
    ("134512345", 134512345),
    ("134,512,345", 134512345),
    ("134 512 345", 134512345),
    ("134.5M", 134500000),
    ("134.5m", 134500000),
    ("1.2B", 1200000000),
    ("950K", 950000),
    ("20M", 20000000),
])
def test_parse_power_accepts_plain_and_shortened_numbers(text, expected):
    assert edit.parse_power(text) == expected


@pytest.mark.parametrize("text", ["", "abc", "0", "-5", "12X", "1.2.3M", "M"])
def test_parse_power_rejects_unusable_values(text):
    assert edit.parse_power(text) is None


# --- apply_member_edit ---

def test_manual_power_is_saved_with_a_timestamp(dbs):
    changed = edit.apply_member_edit(100, power=140000000, alliance_id=1)

    assert changed == ["power"]
    power, updated_at = _user(100, "power", "power_updated_at")
    assert power == 140000000
    assert updated_at


def test_manual_power_change_is_recorded_in_history(dbs):
    edit.apply_member_edit(100, power=140000000, combat_power=21000000, alliance_id=1)

    assert [(r[1], r[2], r[3]) for r in _changes("power_changes")] == [(100, 134512345, 140000000)]
    assert [(r[1], r[2], r[3]) for r in _changes("combat_power_changes")] == [(100, 20100000, 21000000)]


def test_first_manual_power_is_saved_without_a_history_entry(dbs):
    changed = edit.apply_member_edit(200, power=90000000, alliance_id=1)

    assert changed == ["power"]
    assert _user(200, "power") == (90000000,)
    assert _changes("power_changes") == []


def test_unchanged_power_is_not_rewritten(dbs):
    changed = edit.apply_member_edit(100, power=134512345, combat_power=20100000, alliance_id=1)

    assert changed == []
    assert _user(100, "power_updated_at") == (None,)
    assert _changes("power_changes") == []


def test_power_edit_respects_the_alliance_scope(dbs):
    assert edit.apply_member_edit(300, power=9, alliance_id=1) == []
    assert _user(300, "power") == (5000000,)


# --- bulk lines ---

def test_edit_line_reads_power_and_combat_power():
    assert edit.parse_edit_line("100, Alpha, 30, 245, 140M, 21000000") == \
        ("100", "Alpha", 30, 245, 140000000, 21000000)


def test_edit_line_without_power_keeps_working():
    assert edit.parse_edit_line("100, Alpha, 30, 245") == ("100", "Alpha", 30, 245, None, None)


def test_edit_line_rejects_bad_power_with_a_readable_error():
    result = edit.parse_edit_line("100, Alpha, 30, 245, lots")
    assert isinstance(result, str) and "power" in result.lower()


def test_edit_line_rejects_bad_combat_power_with_a_readable_error():
    result = edit.parse_edit_line("100, Alpha, 30, 245, 140M, lots")
    assert isinstance(result, str) and "combat power" in result.lower()


def test_edit_lines_apply_power_only_lines(dbs):
    updated, skipped, errors, _ = edit.apply_edit_lines("100, , , , 150M\n200, , , , , 5M", 1)

    assert (updated, skipped, errors) == (2, 0, [])
    assert _user(100, "power") == (150000000,)
    assert _user(200, "combat_power") == (5000000,)


def test_bulk_prefill_round_trips_exact_power(dbs):
    members = [{"fid": 100, "nickname": "Alpha", "furnace_lv": 30, "kid": None,
                "power": 134512345, "combat_power": 20100000},
               {"fid": 200, "nickname": "Bravo", "furnace_lv": 30, "kid": None,
                "power": None, "combat_power": None}]

    prefill = edit.build_prefill(members)
    updated, skipped, errors, _ = edit.apply_edit_lines(prefill, 1)

    assert "134512345" in prefill
    assert (updated, errors) == (0, []), "an untouched prefill must not register any change"
    assert _changes("power_changes") == []


# --- modals stay inside Discord's limits ---

class _ParentView:
    author_id = 1


def _build(modal_cls, *args, **kwargs):
    """Some discord.py versions need a running event loop to construct a modal."""
    async def make():
        return modal_cls(*args, **kwargs)
    return asyncio.run(make())


def test_single_edit_modal_fits_discord_limits():
    modal = _build(edit.MemberEditModal, _ParentView(), 100, "Alpha", 30, 1, kid=245,
                   power=134512345, combat_power=20100000)

    assert len(modal.children) <= MODAL_FIELD_LIMIT
    assert all(len(item.label) <= MODAL_LABEL_LIMIT for item in modal.children)
    assert "134,512,345" in [item.default for item in modal.children]


def test_bulk_edit_modal_label_fits_discord_limit():
    modal = _build(edit.BulkMemberEditModal, _ParentView(), 1)
    assert all(len(item.label) <= MODAL_LABEL_LIMIT for item in modal.children)
    assert "power" in modal.children[0].label.lower()


# --- ID channel stays name/level/state only ---

def test_id_channel_post_ignores_power():
    assert id_channel.parse_id_post("100, Alpha, 30, 245, 140M, 21M") == (100, "Alpha", 30, 245)


# --- CSV import ---

def test_csv_profiles_read_the_exports_power_columns():
    text = ("ID,Name,FC Level,State,Power,Power Updated,Combat Power,Combat Power Updated\n"
            '100,Alpha,30,245,"134,512,345",2026-09-01,20100000,2026-09-01\n')

    profiles = ops._extract_profiles_from_csv(text)

    assert profiles["100"] == ("Alpha", "30", "245", "134512345", "20100000")


def test_csv_with_only_power_columns_still_counts_as_profiles():
    profiles = ops._extract_profiles_from_csv("ID,Power\n100,140M\n")
    assert profiles["100"] == ("", "", "", "140000000", "")
