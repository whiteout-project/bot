"""Gift codes stored with chat markdown (`CODE`) fail every redemption as Sign Error.

The live channel handler used to store a "Code: `X`" message with its backticks, so
the code sat in 'pending' forever and no member ever got it. Startup stores them bare.
"""
import sqlite3

import main


def _db(codes, user_rows=()):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT, validation_status TEXT)")
    conn.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT, PRIMARY KEY (fid, giftcode))")
    conn.executemany("INSERT INTO gift_codes VALUES (?, '2026-09-20', ?)", codes)
    conn.executemany("INSERT INTO user_giftcodes VALUES (?, ?, ?)", user_rows)
    return conn


def _codes(conn):
    return sorted(conn.execute("SELECT giftcode, validation_status FROM gift_codes").fetchall())


def test_wrapped_code_is_renamed_and_revalidated():
    conn = _db([("`HAPPYCATDAY`", "pending"), ("VIP777", "validated")],
               [(1, "`HAPPYCATDAY`", "SIGN_ERROR"), (1, "VIP777", "SUCCESS")])

    assert main.unwrap_markdown_gift_codes(conn) == 1

    assert _codes(conn) == [("HAPPYCATDAY", "pending"), ("VIP777", "validated")]
    assert conn.execute("SELECT giftcode FROM user_giftcodes").fetchall() == [("VIP777",)]


def test_wrapped_duplicate_of_a_clean_code_is_dropped():
    conn = _db([("`MIDAUTUMN26`", "pending"), ("MIDAUTUMN26", "validated")],
               [(1, "`MIDAUTUMN26`", "SIGN_ERROR"), (1, "MIDAUTUMN26", "SUCCESS")])

    assert main.unwrap_markdown_gift_codes(conn) == 1

    assert _codes(conn) == [("MIDAUTUMN26", "validated")]
    assert conn.execute("SELECT giftcode, status FROM user_giftcodes").fetchall() == [("MIDAUTUMN26", "SUCCESS")]


def test_markdown_only_or_non_code_rows_are_dropped():
    conn = _db([("**", "pending"), ("`not-a-code`", "pending")])

    assert main.unwrap_markdown_gift_codes(conn) == 2

    assert _codes(conn) == []


def test_clean_database_is_untouched_on_every_start():
    conn = _db([("VIP777", "validated"), ("Kingshot888", "invalid")])

    assert main.unwrap_markdown_gift_codes(conn) == 0
    assert main.unwrap_markdown_gift_codes(conn) == 0

    assert _codes(conn) == [("Kingshot888", "invalid"), ("VIP777", "validated")]


def test_works_before_the_validation_status_column_exists():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT)")
    conn.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    conn.execute("INSERT INTO gift_codes VALUES ('`CHILLWEEKEND`', '2026-09-20')")

    assert main.unwrap_markdown_gift_codes(conn) == 1

    assert conn.execute("SELECT giftcode FROM gift_codes").fetchall() == [("CHILLWEEKEND",)]
