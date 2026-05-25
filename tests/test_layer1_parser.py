"""Layer-1 tests: pure-text parsing, regex, and clustering logic.

These tests feed pre-recorded OCR text strings directly into the bot's
parsing functions, asserting the structured output. No real OCR runs;
fast and deterministic. Run after every code change.
"""
from __future__ import annotations

import pytest
from harness import bt, parse_text, load_roster


# Real OCR samples captured from log/bot.txt during user testing.
# Each sample is annotated with what scenario it represents.

ARABIC_RANK_TOP = (
    "23:24 59 100 95K 7.8M 39.4M [] GTrololololole 36,961,409,452:b "
    "MIMOUN 31,577,589,708:b Moly 28,942,615,377 : bi Trololololole 4 "
    "27,717,273,878 : Ibrahim 5 26,583,616,026:"
)

ARABIC_RANK_BOTTOM = (
    "23:24 @ G  59 31,577,589,708: Moly 28,942,615,377:b "
    "等Trololololole/ 4 27,717,273,878:b Ibrahim 5 26,583,616,026:b "
    "AlejoRoll 25,829,932,448: HOGERKURDI 7 22,974,884,177 :b "
    "19,975,831,213 Trololololole 14,528,546,977 :l bi JuDy Cat "
    "10,087,366,663:b 2026-05-0821:30:05 ~V"
)

ARABIC_SUMMARY = (
    "23:24 G G e 59 3 2026-04-2421:30:05 l coll aell lgol eelu Jesi Le "
    "proel Slll lei llile [1 53:5g 265,469,476,854 :l i a>! lol ml cKo "
    "00 [1 al ]  ai s yl o bl 27,717,273,878 cus 019    996679689  9 0 099"
)

ENGLISH_RANK = (
    "Mail [Hunting Trap 1] Damage Ranking GTrololololol%e/ "
    "Damage Points:42,691,117,368 GTrololololol%e/ "
    "DamagePoints:37,338,099,863 Lukaku 3 Damage Points:37,273,869,343 "
    "MIMOUN 4 DamagePoints:32,973,560,662 AlejoRoll 5 Damage Points: "
    "31,598,271,109 Ibrahim Damage Points:28,582,817,832 HOGER KURDI 7 "
    "DamagePoints:27,209,593,990 AyJl elL 8 Damage Points: 26,462,701,136 "
    "Numb Little Bug Delete"
)


# ---------------------------------------------------------------------------
# Trap regex (covers Latin-leading and Arabic-OCR digit-leading layouts)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected_trap", [
    ("Mail [Hunting Trap 1] Damage Ranking", "1"),
    ("Battle Overview [Hunting Trap 2]'s [Bear Hunt]", "2"),
    ("メール [ハント 1] ダメージ", "1"),
    ("[사냥 함정 2] 피해 순위", "2"),
    ("郵件 [狩獵陷阱 1] 傷害排名", "1"),
    ("[Охота 2] Рейтинг урона", "2"),
    # Arabic visual-order OCR (digit immediately after `[`)
    ("... lei llile [1 53:5g 265,469,476,854 ... [1 al ] ai s yl ...", "1"),
    # Empty brackets — should NOT match
    ("23:24 59 100 95K 7.8M 39.4M [] GTrololololole 36,961,409,452", ""),
    # No brackets at all
    ("23:24 @ G  59 31,577,589,708: Moly 28,942,615,377", ""),
])
def test_trap_extraction_across_languages(text, expected_trap):
    trap, _, _ = bt.extract_bear_hunt_stats(text)
    assert trap == expected_trap


# ---------------------------------------------------------------------------
# Rallies extraction (the bug that broke when downscale changed neighbouring
# OCR garbage)
# ---------------------------------------------------------------------------

def test_rallies_extracted_when_preceded_by_letter_token():
    """Pre-downscale OCR pattern: 'Wkelbhk 53:g'."""
    _, rallies, _ = bt.extract_bear_hunt_stats(
        "23:24 G G G 三 59 ... !lile ![1  Wkelbhk 53:g 265,469,476,854 ..."
    )
    assert rallies == "53"


def test_rallies_extracted_when_preceded_by_digit_token():
    """Post-downscale OCR pattern: '[1 53:5g'."""
    _, rallies, _ = bt.extract_bear_hunt_stats(
        "23:24 G G e 59 3 ... lei llile [1 53:5g 265,469,476,854 ..."
    )
    assert rallies == "53"


# ---------------------------------------------------------------------------
# Summary screenshot must NOT yield rows — it has bracket-prose with the
# personal-rewards numbers, which previously leaked through as fake rows.
# ---------------------------------------------------------------------------

def test_summary_screenshot_yields_no_rows():
    rows = bt.parse_player_rows(ARABIC_SUMMARY)
    assert rows == [], (
        f"Summary screenshot must produce zero rows; the bracket-rejection "
        f"path is the only thing keeping personal-stats numbers from being "
        f"parsed as fake rows. Got: {rows}"
    )


# ---------------------------------------------------------------------------
# Row-name cleaning: leading short-token garbage stripped, low-alpha names
# blanked
# ---------------------------------------------------------------------------

def test_arabic_rank_top_rows_cleaned():
    rows = bt.parse_player_rows(ARABIC_RANK_TOP)
    by_dmg = {r["damage"]: r for r in rows}
    # MIMOUN row: ":b MIMOUN " should clean to "MIMOUN"
    assert by_dmg[31577589708]["name"] == "MIMOUN"
    # Moly row: ":b Moly " should clean to "Moly"
    assert by_dmg[28942615377]["name"] == "Moly"


def test_status_bar_chunk_blanked():
    """Screenshot 3's first damage (31.58B) had chunk '23:24 @ G  59 ' —
    should be blanked because it has < 3 alpha chars."""
    rows = bt.parse_player_rows(ARABIC_RANK_BOTTOM)
    by_dmg = {r["damage"]: r for r in rows}
    assert by_dmg[31577589708]["name"] == "", (
        f"Status-bar leak should produce empty name. Got: "
        f"{by_dmg[31577589708]['name']!r}"
    )


# ---------------------------------------------------------------------------
# Roster matching: short-name guard prevents 'G' partial-ratio false positives
# ---------------------------------------------------------------------------

def test_short_name_guard_blocks_single_letter_match():
    """Single-letter input should NOT fuzzy-match Gerry/Pgsy etc. via
    partial-ratio. Only an exact case-insensitive roster match is allowed."""
    roster = [(1, "Gerry"), (2, "Pgsy"), (3, "MIMOUN")]
    assert bt.match_roster("G", roster) == []
    assert bt.match_roster("Gx", roster) == []  # 2 letters, still blocked


def test_short_name_guard_allows_exact_match():
    """A genuinely short-named player should still auto-match if their
    nickname is exactly that short string."""
    roster = [(1, "AB"), (2, "Gerry")]
    assert bt.match_roster("AB", roster) == [(1, "AB", 100)]
    assert bt.match_roster("ab", roster) == [(1, "AB", 100)]


def test_likely_threshold_drops_borderline_noise():
    """MATCH_LIKELY_MIN=80 drops below-threshold matches like Jalu(77%) for
    an OCR'd 'Ibrahim' against a roster lacking Ibrahim."""
    assert bt.MATCH_LIKELY_MIN == 80


# ---------------------------------------------------------------------------
# Cluster decision: row-pair status returns 'unknown' when one side has < 3
# letters (the 'G' bug that split events)
# ---------------------------------------------------------------------------

def test_row_pair_status_short_name_unknown():
    roster = [(1, "MIMOUN"), (2, "Gerry")]
    a = {"name": "MIMOUN", "damage": 31577589708}
    b = {"name": "G",      "damage": 31577589708}
    assert bt._row_pair_status(a, b, roster) == "unknown"


def test_event_group_merges_when_compatible():
    """Two screenshots from the same hunt should cluster into one event."""
    roster = load_roster("default")
    rows1 = bt.parse_player_rows(ARABIC_RANK_TOP)
    rows2 = bt.parse_player_rows(ARABIC_RANK_BOTTOM)

    event = bt.EventGroup()
    res1 = bt.ImageResult()
    res1.ok = True
    res1.rows = {r["damage"]: r for r in rows1}
    event.merge(res1, roster=roster)

    res2 = bt.ImageResult()
    res2.ok = True
    res2.rows = {r["damage"]: r for r in rows2}
    assert event.is_compatible(res2, roster), (
        "Top-half and bottom-half rank screenshots must cluster — "
        "is_compatible currently returning False indicates a regression"
    )


# ---------------------------------------------------------------------------
# Rallies extraction edge cases — preference for the explicit "Rallies:"
# marker over scooping random bare ints from OCR garbage.
# ---------------------------------------------------------------------------

def test_rallies_marker_wins_over_bare_int_garbage():
    """Summary text with both 'Trap 1' / leading '0$' garbage AND
    'Rallies: 50' should pick 50 from the marker, not '0' or '1' from
    the bare-int fallback."""
    text = (
        "Mail [Hunting Trap 1] Personal Damage Rewards 0$Trololololf "
        "Battle Overview Rallies: 50 "
        "Total Alliance Damage: 305,722,250,397"
    )
    _, rallies, _ = bt.extract_bear_hunt_stats(text)
    assert rallies == "50"


def test_rallies_skips_zero_from_ocr_noise():
    """'0$Trololololf' was making rallies parse as '0' — falsy in Python,
    indistinguishable from 'no rallies'. Bare-int fallback must drop 0."""
    text = (
        "Mail [Hunting Trap 1] Damage Ranking 0$Trololololol#%e "
        "Damage Points: 42,691,117,368"
    )
    _, rallies, _ = bt.extract_bear_hunt_stats(text)
    # No "Rallies:" marker, no other small ints → empty, NOT "0"
    assert rallies == ""


# ---------------------------------------------------------------------------
# Hunt-date extraction from screenshot timestamps. Earliest date wins
# (expiry dates appear later in the same OCR text).
# ---------------------------------------------------------------------------

def test_hunt_date_picks_earliest_when_expiry_present():
    text = (
        "Mail 2026-04-2021:30:05 Hunt successful! ... "
        "Expires in 2026-05-04 21:30:05"
    )
    assert bt.extract_hunt_date(text) == "2026-04-20"


def test_hunt_date_returns_none_when_no_date():
    assert bt.extract_hunt_date("Mail Damage Points: 42,691,117,368") is None


def test_hunt_date_rejects_implausible_year():
    # 1999 fails the 2020-2099 plausibility window; only 2026 should win.
    assert bt.extract_hunt_date("garbage 1999-01-01 then 2026-04-20") == "2026-04-20"


# ---------------------------------------------------------------------------
# Position-fill must not overwrite a sibling row's clean primary capture
# with the same player's name re-detected from a fallback engine.
# ---------------------------------------------------------------------------

def test_position_fill_doesnt_double_claim_within_one_call():
    """Both candidate rows are unfilled by the primary OCR (English misread
    Arabic name as 'pYWJI slLo'), so the initial claimed_fids is empty.
    The first iteration writes 'ملك الظلام' to row 8; the second iteration
    must see the just-claimed fid and refuse to overwrite row 9."""
    roster = [
        (99999998, "Lukaku"),
        (44348649, "MIMOUN"),
        (99999994, "AlejoRoll"),
        (99999993, "Ibrahim"),
        (42398947, "HOGER KURDI"),
        (99999991, "ملك الظلام"),
        (99999996, "MADO"),
    ]
    img_rows = {
        37273869343: {'name': 'Lukaku', 'damage': 37273869343, 'rank': 3},
        32973560662: {'name': 'MIMOUN', 'damage': 32973560662, 'rank': 4},
        31598271109: {'name': 'AlejoRoll', 'damage': 31598271109, 'rank': 5},
        28582817832: {'name': 'Ibrahim', 'damage': 28582817832, 'rank': 6},
        27209593990: {'name': 'HOGER KURDI', 'damage': 27209593990, 'rank': 7},
        26462701136: {'name': 'pYWJI slLo', 'damage': 26462701136, 'rank': 8},
        13769060743: {'name': 'Numb Little Bug', 'damage': 13769060743, 'rank': 9},
        13492853348: {'name': 'MADO', 'damage': 13492853348, 'rank': 10},
    }
    fb_text = (
        "Mail D 37338,99,86 Lukaku P 37,273,86,33 MIMOUN Dae Pi "
        "32,973,560,662 AlejoRoll Dmage Points: 31,598,271,109 Ibrahim Dae "
        "Pints: 28,582,81783 HOGER KURDI D P 2729,593,99 مالظلا كلم D P "
        "2646201,16 Numb Little Bug Dage Pits: 13,769,6074 MADO 1٥ De Pi "
        "13,492,853,34"
    )
    bt.fill_unfilled_by_position(
        img_rows, fb_text, "arabic", "test.png", roster
    )
    assert img_rows[26462701136]['name'] == 'ملك الظلام'
    assert img_rows[13769060743]['name'] == 'Numb Little Bug'


def test_position_fill_skips_already_claimed_fid():
    """The bug: Arabic OCR finds 'ملك الظلام' once, and position-fill
    assigns it to TWO rows — one is correctly the rank-8 player, the
    other is rank-9 (Numb Little Bug, not in roster, so 'unfilled')."""
    roster = [(99999991, "ملك الظلام")]
    img_rows = {
        # rank 8: cleanly captured by primary, matches roster
        26462701136: {'name': 'ملك الظلام', 'damage': 26462701136, 'rank': 8},
        # rank 9: Numb Little Bug — captured cleanly but not in roster
        13769060743: {'name': 'Numb Little Bug', 'damage': 13769060743, 'rank': 9},
    }
    fb_text = (
        "Mail HOGER KURDI 27,209,593,990 ملك الظلام 26,462,701,136 "
        "Numb Little Bug 13,769,060,743 MADO 13,492,853,348"
    )
    bt.fill_unfilled_by_position(
        img_rows, fb_text, "arabic", "test.png", roster
    )
    assert img_rows[13769060743]['name'] == 'Numb Little Bug', (
        f"Rank-9 row was clobbered with ملك الظلام even though that fid is "
        f"already cleanly captured by rank-8. Got: "
        f"{img_rows[13769060743]['name']!r}"
    )


def test_hunt_date_ignores_expiry_only_text():
    """Bottom-rank screens often only show 'Expires in YYYY-MM-DD' — that
    must NOT be returned as the hunt date (would clobber the real date
    from the summary screen during session merge)."""
    text = "rows... Expires in 2026-05-04 21:30:05 ~V"
    assert bt.extract_hunt_date(text) is None


def test_hunt_date_picks_pre_expires_when_both_present():
    text = "Mail 2026-04-2021:30:05 Hunt successful! ... Expires in 2026-05-04 21:30:05"
    assert bt.extract_hunt_date(text) == "2026-04-20"


def test_damage_merge_skips_already_claimed_fid():
    """Arabic engine misreads Numb Little Bug's row as 'ملك الظلام', so its
    fb_rows have that name at TWO damages. The damage-keyed merge must
    not write the second one onto a row that doesn't belong to that fid."""
    roster = [(99999991, "ملك الظلام")]
    img_rows = {
        26462701136: {'name': 'ملك الظلام', 'damage': 26462701136, 'rank': 8},
        13769060743: {'name': 'Numb Little Bug', 'damage': 13769060743, 'rank': 9},
    }
    fb_rows = [
        {'name': 'ملك الظلام', 'damage': 26462701136, 'rank': 8},
        {'name': 'ملك الظلام', 'damage': 13769060743, 'rank': 9},
    ]
    bt.merge_fallback_rows_by_damage(img_rows, fb_rows, roster, "arabic")
    assert img_rows[13769060743]['name'] == 'Numb Little Bug'


def test_strip_minority_script_drops_isolated_korean_in_latin():
    assert bt._strip_minority_script("008%Trololololol루8/") == "008%Trololololol8/"
    assert bt._strip_minority_script("ᘛᎵTrololoIolol루~") == "ᘛᎵTrololoIolol~"


def test_strip_minority_script_preserves_pure_arabic():
    assert bt._strip_minority_script("ملك الظلام") == "ملك الظلام"


def test_strip_minority_script_preserves_balanced_mix():
    """Legitimate mixed-script names (no clear majority) stay untouched."""
    assert bt._strip_minority_script("Player 한국") == "Player 한국"


def test_match_roster_finds_trololo_through_korean_noise():
    """Korean fallback OCR leaves stray '루' inside a Latin name. The
    matcher must still find a Trololo entry above MATCH_LIKELY_MIN."""
    roster = [
        (45379845, 'Trololololol'),
        (99999998, 'TrololololO'),
        (88888888, 'Trololo'),
    ]
    cands = bt.match_roster('008%Trololololol루8/', roster)
    assert cands, "expected at least one Trololo candidate"
    assert cands[0][2] >= bt.MATCH_LIKELY_MIN
