"""Auto-managed OCR language detection.

`detect_fallback_langs` is pure; `auto_managed_fallbacks` is tested against a
temp alliancesettings DB. No OCR engines load here.
"""
from __future__ import annotations

import sqlite3

import pytest

from harness import bt


# ── detect_fallback_langs ──────────────────────────────────────────────────

@pytest.mark.parametrize("names, expected", [
    (["ксюха", "Bob"], ["cyrillic"]),
    (["ملك الظلام"], ["arabic"]),
    (["नमस्ते"], ["devanagari"]),
    (["홍길동"], ["korean"]),
    (["さくら"], ["japan"]),          # kana → japan
    (["旭東興業"], ["ch"]),           # han-only → ch
    (["ROγAL", "ĎƐΔΗ", "Saeed"], []),  # Greek/decorated Latin → folding, no engine
    (["English", "Names", "Only"], []),
    (["ксюха", "ملك", "홍길동"], ["arabic", "cyrillic", "korean"]),  # sorted, multi
])
def test_detect_fallback_langs(names, expected):
    assert bt.detect_fallback_langs(names, primary="en") == expected


def test_kana_implies_japan_even_with_han():
    # A name with kana + han → japan only (japan engine covers han); not ch.
    assert bt.detect_fallback_langs(["日本さくら"], primary="en") == ["japan"]


def test_primary_excluded():
    assert "cyrillic" not in bt.detect_fallback_langs(["ксюха"], primary="cyrillic")


# ── auto_managed_fallbacks (temp DB) ───────────────────────────────────────

@pytest.fixture
def alliance_db(tmp_path):
    db = tmp_path / "alliance.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute(
            "CREATE TABLE alliancesettings (alliance_id INTEGER PRIMARY KEY, "
            "bear_ocr_auto_manage INTEGER, bear_ocr_fallback_langs TEXT)")
        conn.commit()
    return str(db)


def _row(db, aid):
    with sqlite3.connect(db) as conn:
        return conn.execute(
            "SELECT bear_ocr_fallback_langs FROM alliancesettings WHERE alliance_id=?",
            (aid,)).fetchone()


def test_auto_manage_on_detects_and_persists(alliance_db):
    with sqlite3.connect(alliance_db) as conn:
        conn.execute("INSERT INTO alliancesettings VALUES (1, 1, '')")
        conn.commit()
    got = bt.auto_managed_fallbacks(1, ["ксюха", "Bob"], db_path=alliance_db)
    assert got == ["cyrillic"]
    assert _row(alliance_db, 1)[0] == "cyrillic"   # persisted


def test_auto_manage_prunes_when_member_leaves(alliance_db):
    with sqlite3.connect(alliance_db) as conn:
        conn.execute("INSERT INTO alliancesettings VALUES (1, 1, 'cyrillic')")
        conn.commit()
    # Roster no longer has any Cyrillic name → cyrillic dropped.
    got = bt.auto_managed_fallbacks(1, ["Bob", "Alice"], db_path=alliance_db)
    assert got == []
    assert _row(alliance_db, 1)[0] == ""


def test_auto_manage_off_returns_manual_untouched(alliance_db):
    with sqlite3.connect(alliance_db) as conn:
        conn.execute("INSERT INTO alliancesettings VALUES (1, 0, 'arabic')")
        conn.commit()
    got = bt.auto_managed_fallbacks(1, ["ксюха"], db_path=alliance_db)  # cyrillic present
    assert got == ["arabic"]                       # manual respected, not changed
    assert _row(alliance_db, 1)[0] == "arabic"     # not overwritten


def test_no_write_when_unchanged(alliance_db):
    with sqlite3.connect(alliance_db) as conn:
        conn.execute("INSERT INTO alliancesettings VALUES (1, 1, 'cyrillic')")
        conn.commit()
    # detected == stored → returns it, no exception (idempotent)
    assert bt.auto_managed_fallbacks(1, ["ксюха"], db_path=alliance_db) == ["cyrillic"]
