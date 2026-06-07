"""Tests for the attendance OCR learned manual-match alias cache (Layer 2).

When an admin resolves an unreadable decorated name by hand, the bot remembers
the OCR-text → player mapping so it auto-matches next time. A confident direct
match always wins over a stored alias (collision guard).

Pure logic + a temp SQLite DB — no real OCR, fast and deterministic.
"""
from __future__ import annotations

import pytest

from harness_attendance import parsers


ROSTER = [(1, "ĎƐΔΗ"), (2, "Saeed"), (3, "bella igor"), (4, "MiiKeY")]
ALLIANCE = 7
# OCR garble that fuzzy-matches no roster name — only a learned alias resolves it.
GIBBERISH = "Җҝ ŦŘŁŁ ҂оҩ"


# ---------------------------------------------------------------------------
# Layer 1 — homoglyph folding makes decorated names match directly (no alias)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ocr_text, expected_fid", [
    ("DEAH", 1),            # roster 'ĎƐΔΗ' (Greek/caron lookalikes)
    ("Saeed", 2),
])
def test_decorated_roster_names_match_without_alias(ocr_text, expected_fid):
    # No alliance_id → alias layer is never consulted; this is pure folding.
    fid, status = parsers.fuzzy_match_name(ocr_text, ROSTER)
    assert fid == expected_fid and status == "auto"


def test_fold_collapses_homoglyphs():
    assert parsers._normalize_for_match("ĎƐΔΗ") == parsers._normalize_for_match("DEAH")
    assert parsers._normalize_for_match("ROγAL") == parsers._normalize_for_match("ROYAL")


@pytest.fixture
def temp_alias_db(tmp_path, monkeypatch):
    """Point the alias functions at a throwaway DB with the real schema."""
    db = tmp_path / "attendance.sqlite"
    monkeypatch.setattr(parsers, "_ATT_DB", str(db))
    parsers._init_ocr_alias_table()
    return str(db)


def test_unresolvable_before_learning(temp_alias_db):
    assert parsers.fuzzy_match_candidates(GIBBERISH, ROSTER, alliance_id=ALLIANCE) == []
    assert parsers.fuzzy_match_name(GIBBERISH, ROSTER, alliance_id=ALLIANCE) == (None, "no_match")


def test_learn_then_resolve(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 1)
    cands = parsers.fuzzy_match_candidates(GIBBERISH, ROSTER, alliance_id=ALLIANCE)
    assert cands and cands[0][0] == 1 and cands[0][3] == "auto"
    assert parsers.fuzzy_match_name(GIBBERISH, ROSTER, alliance_id=ALLIANCE) == (1, "auto")


def test_fuzzy_tolerates_ocr_drift(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 2)
    drifted = GIBBERISH + "x"  # one extra char between screenshots
    assert parsers.fuzzy_match_name(drifted, ROSTER, alliance_id=ALLIANCE) == (2, "auto")


def test_strong_direct_match_wins_over_alias(temp_alias_db):
    """Collision guard: a confident read is never overridden by a wrong alias."""
    parsers.learn_name_alias(ALLIANCE, "Saeed", 1)  # deliberately wrong mapping
    fid, _status = parsers.fuzzy_match_name("Saeed", ROSTER, alliance_id=ALLIANCE)
    assert fid == 2  # the real Saeed, not the alias target


def test_alias_ignored_when_player_left_roster(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 1)
    roster_without_1 = [e for e in ROSTER if e[0] != 1]
    assert parsers.fuzzy_match_name(GIBBERISH, roster_without_1, alliance_id=ALLIANCE) == (None, "no_match")


def test_alias_scoped_per_alliance(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 1)
    assert parsers.fuzzy_match_name(GIBBERISH, ROSTER, alliance_id=999) == (None, "no_match")


def test_no_alliance_id_means_no_alias(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 1)
    assert parsers.fuzzy_match_name(GIBBERISH, ROSTER) == (None, "no_match")


def test_blank_or_placeholder_fids_not_learned(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, "", 1)         # blank name
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 0)  # non-real fid
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, -5)  # negative placeholder fid
    import sqlite3
    with sqlite3.connect(temp_alias_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM ocr_name_alias").fetchone()[0]
    assert count == 0


def test_relearn_updates_mapping(temp_alias_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 1)
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 3)  # admin corrects it
    assert parsers.fuzzy_match_name(GIBBERISH, ROSTER, alliance_id=ALLIANCE) == (3, "auto")


# ---------------------------------------------------------------------------
# rematch_unmatched_rows — bulk re-resolve persisted unmatched rows
# ---------------------------------------------------------------------------

import sqlite3


@pytest.fixture
def rematch_db(temp_alias_db, monkeypatch):
    """temp_alias_db + an attendance_records table + a stubbed roster source."""
    with sqlite3.connect(temp_alias_db) as conn:
        conn.execute("""
            CREATE TABLE attendance_records (
                session_id TEXT, session_name TEXT, event_type TEXT, event_date TEXT,
                player_id TEXT, player_name TEXT, alliance_id TEXT, alliance_name TEXT,
                status TEXT, points INTEGER, UNIQUE(session_id, player_id)
            )
        """)
        conn.commit()
    monkeypatch.setattr(parsers, "load_alliance_roster", lambda aid: ROSTER)
    return temp_alias_db


def _insert_rows(db, session_id, rows):
    with sqlite3.connect(db) as conn:
        for pid, name, status in rows:
            conn.execute(
                "INSERT INTO attendance_records "
                "(session_id, player_id, player_name, alliance_id, status, points) "
                "VALUES (?, ?, ?, ?, ?, 0)",
                (session_id, pid, name, str(ALLIANCE), status))
        conn.commit()


def _rows(db, session_id):
    with sqlite3.connect(db) as conn:
        return {r[0]: (r[1], r[2]) for r in conn.execute(
            "SELECT player_id, player_name, status FROM attendance_records WHERE session_id = ?",
            (session_id,))}


def test_rematch_resolves_now_matching_rows(rematch_db):
    _insert_rows(rematch_db, "s1", [
        ("-1", "Saeed", "present"),
        ("-2", "bella igor", "registered"),
        ("-3", "totally unreadable garble", "absent"),
    ])
    n = parsers.rematch_unmatched_rows("s1", ALLIANCE)
    assert n == 2
    rows = _rows(rematch_db, "s1")
    assert "2" in rows and rows["2"][1] == "present"     # Saeed, status preserved
    assert "3" in rows and rows["3"][1] == "registered"  # bella igor
    assert "-3" in rows                                  # genuinely unmatched, untouched


def test_rematch_uses_learned_alias(rematch_db):
    parsers.learn_name_alias(ALLIANCE, GIBBERISH, 4)  # MiiKeY taught earlier
    _insert_rows(rematch_db, "s2", [("-1", GIBBERISH, "present")])
    assert parsers.rematch_unmatched_rows("s2", ALLIANCE) == 1
    assert "4" in _rows(rematch_db, "s2")


def test_rematch_skips_fid_already_in_session(rematch_db):
    # Saeed (fid 2) already present; an unmatched row also reading 'Saeed'
    # must NOT collide onto the same fid (would break UNIQUE).
    _insert_rows(rematch_db, "s3", [
        ("2", "Saeed", "present"),
        ("-1", "Saeed", "present"),
    ])
    assert parsers.rematch_unmatched_rows("s3", ALLIANCE) == 0
    assert "-1" in _rows(rematch_db, "s3")


def test_rematch_no_unmatched_is_noop(rematch_db):
    _insert_rows(rematch_db, "s4", [("200", "Saeed", "present")])
    assert parsers.rematch_unmatched_rows("s4", ALLIANCE) == 0
