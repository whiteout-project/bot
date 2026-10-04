"""A failed users.sqlite insert must not leave its write lock open.

sqlite3 opens a transaction before an INSERT and holds the write lock until
commit or rollback. A duplicate-ID IntegrityError on a long-lived connection
skipped both, so every other writer (e.g. the kingdom scan saving a found
kingdom) hit "database is locked" until something on that connection committed.
"""
import sqlite3
import types

import pytest

import cogs.alliance_member_operations as amo
import cogs.alliance_registration as ar


@pytest.fixture
def users_db(tmp_path):
    path = tmp_path / "users.sqlite"
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, furnace_lv INTEGER, "
                     "kid INTEGER, stove_lv_content TEXT, alliance TEXT, discord_id INTEGER, "
                     "discord_server_id INTEGER, discord_id_updated_at TEXT, power INTEGER, "
                     "power_updated_at TEXT, combat_power INTEGER, combat_power_updated_at TEXT)")
        conn.execute("INSERT INTO users (fid, nickname) VALUES (1, 'existing')")
    return path


def _other_writer_can_write(path):
    other = sqlite3.connect(path, timeout=0.2)
    try:
        other.execute("UPDATE users SET nickname = 'changed' WHERE fid = 1")
        other.commit()
        return True
    except sqlite3.OperationalError:
        return False
    finally:
        other.close()


def _holder(cls, path):
    obj = cls.__new__(cls)
    obj.conn_users = sqlite3.connect(path)
    obj.c_users = obj.conn_users.cursor()
    return obj


def test_register_duplicate_insert_releases_lock(users_db):
    reg = _holder(ar.AllianceRegistration, users_db)
    with pytest.raises(sqlite3.IntegrityError):
        reg._insert_new_user(1, {"nickname": "dup", "stove_lv": 0, "kid": 259}, 5, 42, 7)
    assert _other_writer_can_write(users_db)


def test_member_add_duplicate_insert_releases_lock(users_db):
    ops = _holder(amo.AllianceMemberOperations, users_db)
    with pytest.raises(sqlite3.IntegrityError):
        ops._insert_member(1, "dup", 0, 259, "5", None, None)
    assert _other_writer_can_write(users_db)


@pytest.mark.parametrize("raw,expected", [
    ("111\n222\n111", ["111", "222"]),
    ("111, 222, 111, 333", ["111", "222", "333"]),
    ("id,name\n111,a\n222,b\n111,c", ["111", "222"]),
])
def test_parse_member_ids_drops_duplicates_keeping_order(raw, expected):
    assert amo.parse_member_ids(raw) == expected
