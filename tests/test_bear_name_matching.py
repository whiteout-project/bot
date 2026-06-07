"""Name-resolution tests: homoglyph normalization (Layer 1) and the learned
OCR→player alias cache (Layer 2).

Pure logic + a temp SQLite DB for the alias layer — no real OCR runs, fast and
deterministic. Fixtures are the real decorated gamertags from a user's bear
ranking screenshots, which the old plain-fuzzy matcher could not resolve.
"""
from __future__ import annotations

import sqlite3

import pytest
from harness import bt


# Real decorated names from the screenshots. Each player's in-game name (as the
# game API returns it, with Greek/Cyrillic/styled lookalikes) paired with a
# plausible Latin OCR reading of the same rendered glyphs.
DECORATED_ROSTER = [
    (1, "ĎƐΔΗ"),            # D-caron, Latin epsilon, Greek delta, Greek eta
    (2, "ČURSƐ ĵoY"),       # caron C, Latin epsilon, j-circumflex
    (3, "ROγAL Lady AMG"),  # Greek gamma standing in for 'y'
    (4, "Saeed"),           # plain control
    (5, "bella igor"),      # plain control
    (6, "MiiKeY"),          # plain control
]

OCR_READINGS = [
    ("DEAH", 1),
    ("CURSE JOY", 2),
    ("CURSE joY", 2),
    ("ROYAL Lady AMG", 3),
    ("Saeed", 4),
    ("bella igor", 5),
    ("MiiKeY", 6),
]


# ---------------------------------------------------------------------------
# Layer 1 — skeleton folding
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("decorated, expected", [
    ("ĎƐΔΗ", "deah"),
    ("ČURSƐ ĵoY", "curse joy"),
    ("ROγAL Lady AMG", "royal lady amg"),
    ("Saeed", "saeed"),
])
def test_fold_collapses_homoglyphs_to_latin(decorated, expected):
    assert bt._fold(decorated) == expected


def test_skeleton_is_comparison_only_not_for_display():
    """_skeleton must never be used to overwrite a stored name — it strips
    decoration. This guards the intent, not a code path."""
    assert bt._skeleton("ĎƐΔΗ") == "DEAH"
    assert bt._skeleton("ĎƐΔΗ") != "ĎƐΔΗ"


# ---------------------------------------------------------------------------
# Layer 1 — decorated names now match the roster (the core regression guard)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("ocr_text, expected_fid", OCR_READINGS)
def test_decorated_names_match_roster(ocr_text, expected_fid):
    """A Latin OCR reading resolves to the decorated roster name at auto-confirm
    strength. Before homoglyph folding these scored near zero."""
    cands = bt.match_roster(ocr_text, DECORATED_ROSTER)
    assert cands, f"no match for {ocr_text!r}"
    fid, _display, score = cands[0]
    assert fid == expected_fid
    assert score >= bt.MATCH_AUTO_CONFIRM


def test_exact_decorated_text_still_matches():
    """If OCR reproduces the exact decorated codepoints, folding both sides
    still matches (and doesn't regress)."""
    cands = bt.match_roster("ĎƐΔΗ", DECORATED_ROSTER)
    assert cands and cands[0][0] == 1


def test_unrelated_name_does_not_match():
    assert bt.match_roster("totally different", DECORATED_ROSTER) == []


# ---------------------------------------------------------------------------
# Layer 2 — learned alias cache (isolated temp DB)
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_bear_db(tmp_path, monkeypatch):
    """Point the alias functions at a throwaway DB with the real schema."""
    db = tmp_path / "bear_data.sqlite"
    monkeypatch.setattr(bt, "BEAR_DB_PATH", str(db))
    bt.init_bear_database()
    return str(db)


# A garble that does NOT fold to any roster name — only a learned alias can
# resolve it (heavily decorated names OCR can never read cleanly). Long enough
# that a one-character OCR drift stays above the fuzzy-alias threshold.
GIBBERISH = "Җҝ ŦŘŁŁ ҂оҩ"
ALLIANCE = 4242


def test_alias_learn_then_resolve(temp_bear_db):
    roster = DECORATED_ROSTER
    # Unresolvable on its own.
    assert bt.resolve_against_roster(GIBBERISH, roster, ALLIANCE) == []
    # Admin resolves it once.
    bt.learn_alias(ALLIANCE, GIBBERISH, 1)
    cands = bt.resolve_against_roster(GIBBERISH, roster, ALLIANCE)
    assert cands == [(1, "ĎƐΔΗ", bt.MATCH_ALIAS_SCORE)]


def test_alias_fuzzy_tolerates_ocr_drift(temp_bear_db):
    bt.learn_alias(ALLIANCE, GIBBERISH, 2)
    drifted = GIBBERISH + "x"  # one extra char between screenshots
    cands = bt.resolve_against_roster(drifted, DECORATED_ROSTER, ALLIANCE)
    assert cands and cands[0][0] == 2


def test_strong_direct_match_wins_over_alias(temp_bear_db):
    """Collision guard: a confident read is never overridden by a stale/wrong
    learned alias."""
    bt.learn_alias(ALLIANCE, "Saeed", 1)  # deliberately wrong mapping
    cands = bt.resolve_against_roster("Saeed", DECORATED_ROSTER, ALLIANCE)
    assert cands[0][0] == 4  # direct match to the real Saeed, not the alias


def test_alias_ignored_when_player_left_roster(temp_bear_db):
    bt.learn_alias(ALLIANCE, GIBBERISH, 1)
    roster_without_1 = [e for e in DECORATED_ROSTER if e[0] != 1]
    assert bt.resolve_against_roster(GIBBERISH, roster_without_1, ALLIANCE) == []


def test_alias_scoped_per_alliance(temp_bear_db):
    bt.learn_alias(ALLIANCE, GIBBERISH, 1)
    assert bt.resolve_against_roster(GIBBERISH, DECORATED_ROSTER, 9999) == []


def test_blank_or_tiny_keys_are_not_learned(temp_bear_db):
    bt.learn_alias(ALLIANCE, "", 1)
    bt.learn_alias(ALLIANCE, "✦", 1)  # folds to empty/<2 chars
    with sqlite3.connect(temp_bear_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM bear_name_alias").fetchone()[0]
    assert count == 0


# ---------------------------------------------------------------------------
# Bear channel info message — self-heal fingerprint must match the rendered text
# ---------------------------------------------------------------------------

def test_info_message_fingerprints_present_in_render():
    """Every fingerprint used to recognise our own pinned message must appear in
    the rendered text — otherwise self-heal can't find/dedupe it."""
    rendered = bt.render_bear_info_message()
    assert bt._BEAR_INFO_FINGERPRINTS
    for fp in bt._BEAR_INFO_FINGERPRINTS:
        assert fp in rendered
