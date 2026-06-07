"""Layer-1 tests: pure-text parsing for the attendance OCR pipeline.

These feed pre-recorded / synthetic OCR text strings directly into the
parser functions and assert structured output. No real OCR runs; fast
and deterministic. Run after every code change.
"""
from __future__ import annotations

from datetime import date

import pytest

from harness_attendance import parsers, load_roster


# ---------------------------------------------------------------------------
# Event-type classifier — keyword whitelist + fingerprint regex
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,allowed,expected", [
    # Single event type allowed; keyword present
    ("Foundry Battle results page", {"foundry_battle": ["Foundry Battle"]}, "foundry_battle"),
    # Keyword case-insensitive
    ("foundry battle blah blah", {"foundry_battle": ["Foundry Battle"]}, "foundry_battle"),
    # Multiple event types — first keyword match wins
    ("Canyon Clash page", {"foundry_battle": ["Foundry"], "canyon_clash": ["Canyon"]}, "canyon_clash"),
    # No keyword match — returns None
    ("random text with no keywords", {"foundry_battle": ["Foundry"]}, None),
    # Empty whitelist
    ("Foundry Battle", {}, None),
])
def test_classify_by_keywords(text, allowed, expected):
    assert parsers.classify_by_keywords(text, allowed) == expected


@pytest.mark.parametrize("event_type,text,expected", [
    ("foundry_battle", "Personal Arsenal Points scrolling", True),
    ("foundry_battle", "Imperial Foundry Control Duration: 00:43:41", True),
    ("foundry_battle", "Canyon Clash page", False),
    ("foundry_battle", "Congratulations on being selected as a combatant for Foundry Battle",
     True),
    ("canyon_clash", "Total Fuel Used: 31.1M", True),
    ("canyon_clash", "Personal Point Ranking", True),
    ("canyon_clash", "selected as a combatant for Canyon Clash event", True),
    ("power_rankings", "Alliance Ranking Power Rankings", True),
    ("alliance_showdown", "Alliance Showdown Point Ranking", True),
    # Wrong fingerprint for event type
    ("foundry_battle", "Personal Point Ranking (canyon)", False),
])
def test_fingerprint_match(event_type, text, expected):
    assert parsers.fingerprint_match(event_type, text) == expected


@pytest.mark.parametrize("event_type,text,kind,expected", [
    ("foundry_battle", "Personal Arsenal Points", "result",       True),
    ("foundry_battle", "Personal Arsenal Points", "registration", False),
    ("foundry_battle", "Congratulations on being selected as a combatant for Foundry Battle",
     "registration", True),
    ("foundry_battle", "Congratulations on being selected as a combatant for Foundry Battle",
     "result", False),
    ("canyon_clash", "Total Fuel Used: 31.1M", "result", True),
    ("canyon_clash", "selected as a combatant for Canyon Clash", "registration", True),
])
def test_fingerprint_match_by_kind(event_type, text, kind, expected):
    assert parsers.fingerprint_match(event_type, text, kind=kind) == expected


def test_classifier_unknown_event_type():
    assert parsers.fingerprint_match("doesnt_exist", "any text") is False


@pytest.mark.parametrize("event_type,text,expected_kind", [
    ("foundry_battle", "Personal Arsenal Points scrolling", "result"),
    ("foundry_battle", "Imperial Foundry Control Duration", "result"),
    ("foundry_battle", "Congratulations on being selected as a combatant for Foundry Battle",
     "registration"),
    ("canyon_clash", "Total Fuel Used: 31.1M Personal Point Ranking", "result"),
    ("canyon_clash", "selected as a combatant for Canyon Clash", "registration"),
    ("canyon_clash", "[Legion 1] Please get ready to enter the [Canyon Clash]", "registration"),
])
def test_detect_kind(event_type, text, expected_kind):
    assert parsers.detect_kind(event_type, text) == expected_kind


def test_detect_kind_returns_none_for_scroll_page():
    """Continuation pages have no header → no fingerprint match → None."""
    assert parsers.detect_kind("foundry_battle", "just player rows: vtr 123 H&N 456") is None


# ---------------------------------------------------------------------------
# classify_event — the fingerprint-first classifier used in production.
# Keywords are an optional prefilter; fingerprint regex is the deciding factor.
# ---------------------------------------------------------------------------

# Real-ish OCR text snippets — these match the fingerprint regexes in
# attendance_ocr_parsers.EVENT_TYPES.
_TEXT_FOUNDRY_REG = (
    "Congratulations on being selected as a combatant for [Legion 1] "
    "in Foundry Battle. Get ready to fight!"
)
_TEXT_FOUNDRY_RESULT = (
    "Legion 1 Victory! Imperial Foundry Control Duration 00:43:41. "
    "Rewards. Personal Arsenal Points ranking below."
)
_TEXT_CANYON_REG = (
    "You've been selected as a combatant for the upcoming Canyon Clash event."
)
_TEXT_CANYON_RESULT = (
    "Legion 2 ranked No. 2 in [Canyon Clash]. Total Fuel Used 31.1M. "
    "Personal Point Ranking attached."
)
_TEXT_POWER_RANKING = "Alliance Ranking · Power Rankings tab"
_TEXT_SHOWDOWN = "Alliance Showdown Final Standings Point Ranking"
# After the unified-registration refactor, only the four "real" event types
# exist; per-screenshot kind ("registration" / "result") is returned as the
# tuple's second element by classify_event.
_ALL_EVENTS = ["foundry_battle", "canyon_clash", "power_rankings", "alliance_showdown"]


def test_classify_event_no_keywords_fingerprint_alone_works():
    assert parsers.classify_event(_TEXT_FOUNDRY_REG, _ALL_EVENTS, {}) == ("foundry_battle", "registration")
    assert parsers.classify_event(_TEXT_FOUNDRY_RESULT, _ALL_EVENTS, {}) == ("foundry_battle", "result")
    assert parsers.classify_event(_TEXT_CANYON_REG, _ALL_EVENTS, {}) == ("canyon_clash", "registration")
    assert parsers.classify_event(_TEXT_CANYON_RESULT, _ALL_EVENTS, {}) == ("canyon_clash", "result")
    assert parsers.classify_event(_TEXT_POWER_RANKING, _ALL_EVENTS, {}) == ("power_rankings", "result")
    assert parsers.classify_event(_TEXT_SHOWDOWN, _ALL_EVENTS, {}) == ("alliance_showdown", "result")


def test_classify_event_distinguishes_foundry_registration_vs_result():
    """Same event family, different kind — the per-kind fingerprint regexes
    return the right kind tag for each."""
    assert parsers.classify_event(_TEXT_FOUNDRY_REG, _ALL_EVENTS, {}) == ("foundry_battle", "registration")
    assert parsers.classify_event(_TEXT_FOUNDRY_RESULT, _ALL_EVENTS, {}) == ("foundry_battle", "result")


def test_classify_event_distinguishes_canyon_registration_vs_clash():
    assert parsers.classify_event(_TEXT_CANYON_REG, _ALL_EVENTS, {}) == ("canyon_clash", "registration")
    assert parsers.classify_event(_TEXT_CANYON_RESULT, _ALL_EVENTS, {}) == ("canyon_clash", "result")


def test_classify_event_keyword_prefilter_narrows_correctly():
    # When a keyword is configured for an event, the text must contain it
    # OR the event is skipped — even if the fingerprint would otherwise match.
    keywords = {"foundry_battle": ["customword"]}
    # Foundry result text without "customword" — should NOT classify.
    assert parsers.classify_event(_TEXT_FOUNDRY_RESULT, _ALL_EVENTS, keywords) is None
    # Same text with "customword" injected — should classify.
    assert parsers.classify_event(
        _TEXT_FOUNDRY_RESULT + " customword", _ALL_EVENTS, keywords
    ) == ("foundry_battle", "result")


def test_classify_event_disabled_event_never_classified():
    enabled = ["canyon_clash"]  # foundry_battle deliberately omitted
    keywords = {"foundry_battle": ["Foundry"]}
    assert parsers.classify_event(_TEXT_FOUNDRY_RESULT, enabled, keywords) is None


def test_classify_event_empty_inputs_return_none():
    assert parsers.classify_event("", _ALL_EVENTS, {}) is None
    assert parsers.classify_event("any text", [], {}) is None
    assert parsers.classify_event(None or "", _ALL_EVENTS, {}) is None


def test_classify_event_no_match_returns_none():
    assert parsers.classify_event("random unrelated chatter", _ALL_EVENTS, {}) is None


# Regression: "Please get ready to enter the [Canyon Clash]..." invitation mail.

_TEXT_CANYON_REG_INVITATION = (
    "[Legion 1] Please get ready to enter the [Canyon Clash] battlefield "
    "and fight for the glory of your alliance! Here is the list of "
    "Legionnaires of your Legion: Combatants: 18/30 AlejoRoll R5 807.3M"
)
_TEXT_FOUNDRY_REG_INVITATION = (
    "[Legion 2] Please get ready to enter the [Foundry Battle] arena. "
    "Combatants: 24/30 Trololololol R4 925.5M"
)


def test_classify_event_canyon_registration_invitation_wording():
    assert parsers.classify_event(
        _TEXT_CANYON_REG_INVITATION, _ALL_EVENTS, {}
    ) == ("canyon_clash", "registration")


def test_classify_event_foundry_registration_invitation_wording():
    assert parsers.classify_event(
        _TEXT_FOUNDRY_REG_INVITATION, _ALL_EVENTS, {}
    ) == ("foundry_battle", "registration")


def test_classify_event_canyon_registration_not_misclassified_as_result():
    """The bug: 'Canyon Clash' appears in the registration mail header — the
    result-kind regex must NOT match it, so classify_event returns the
    registration kind."""
    result = parsers.classify_event(_TEXT_CANYON_REG_INVITATION, _ALL_EVENTS, {})
    assert result == ("canyon_clash", "registration")


def test_canyon_clash_result_fingerprint_doesnt_match_registration_mail():
    """Result-kind regex must NOT match a registration mail even when the
    event_type is canyon_clash. detect_kind should pick 'registration'."""
    assert parsers.detect_kind("canyon_clash", _TEXT_CANYON_REG_INVITATION) == "registration"
    assert parsers.fingerprint_match("canyon_clash", _TEXT_CANYON_REG_INVITATION,
                                     kind="result") is False


def test_canyon_clash_fingerprint_still_matches_real_result_mails():
    """Tightening canyon_clash mustn't break real result classification."""
    assert parsers.classify_event(_TEXT_CANYON_RESULT, _ALL_EVENTS, {}) == ("canyon_clash", "result")
    text = (
        "Congratulations, [Legion 2] of your alliance ranked No. 2 in [Canyon Clash]! "
        "Here are the battle details: #293 Legion 2 [BRF]Bla 612,477"
    )
    assert parsers.classify_event(text, _ALL_EVENTS, {}) == ("canyon_clash", "result")


# ---------------------------------------------------------------------------
# Header date extraction (matches YYYY-MM-DD anywhere in the OCR text)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("2026-05-03 18:00:02", date(2026, 5, 3)),
    ("Mail header 2026-04-18 22:59:59 results", date(2026, 4, 18)),
    ("dotted 2026.05.03 format", date(2026, 5, 3)),
    ("slash 2026/05/03 format", date(2026, 5, 3)),
    # Real-OCR pattern: no space between date and time
    ("2026-05-0318:00:02", date(2026, 5, 3)),
    ("Mail 2026-04-1822:59:59 Congratulations", date(2026, 4, 18)),
    # First match wins
    ("first 2026-05-03 then 2026-12-31", date(2026, 5, 3)),
    # No date present
    ("no date in this text", None),
    # Invalid date components
    ("2026-13-99", None),
])
def test_extract_header_date(text, expected):
    assert parsers.extract_header_date(text) == expected


# ---------------------------------------------------------------------------
# Event-date weekday snap. Foundry events should snap to Sunday (weekday=6);
# Canyon events to Saturday (weekday=5). ±1 day from target → adjusted;
# anything further → mismatch and the date is returned as-is.
# ---------------------------------------------------------------------------

def test_resolve_event_date_foundry_exact_sunday():
    # 2026-05-03 is a Sunday
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 3), "foundry_battle")
    assert resolved == date(2026, 5, 3)
    assert conf == "exact"


def test_resolve_event_date_foundry_adjusted_from_monday():
    # 2026-05-04 is a Monday — snap back to Sunday 2026-05-03
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 4), "foundry_battle")
    assert resolved == date(2026, 5, 3)
    assert conf == "adjusted"


def test_resolve_event_date_foundry_adjusted_from_saturday():
    # 2026-05-02 is a Saturday — snap forward to Sunday 2026-05-03
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 2), "foundry_battle")
    assert resolved == date(2026, 5, 3)
    assert conf == "adjusted"


def test_resolve_event_date_foundry_mismatch_when_far():
    # 2026-04-29 (Wednesday) is too far from any Sunday → mismatch
    resolved, conf = parsers.resolve_event_date(date(2026, 4, 29), "foundry_battle")
    assert resolved == date(2026, 4, 29)
    assert conf == "mismatch"


def test_resolve_event_date_canyon_exact_saturday():
    # 2026-05-02 is a Saturday
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 2), "canyon_clash")
    assert resolved == date(2026, 5, 2)
    assert conf == "exact"


def test_resolve_event_date_canyon_adjusted_from_sunday():
    # 2026-05-03 is a Sunday — snap back to Saturday 2026-05-02
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 3), "canyon_clash")
    assert resolved == date(2026, 5, 2)
    assert conf == "adjusted"


def test_resolve_event_date_event_without_weekday_returns_exact():
    # Power Rankings has no event_weekday — always exact
    resolved, conf = parsers.resolve_event_date(date(2026, 5, 14), "power_rankings")
    assert resolved == date(2026, 5, 14)
    assert conf == "exact"


# ---------------------------------------------------------------------------
# Legion extraction (matches "Legion 1" / "Legion 2" with optional brackets)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("[Legion 1] of your alliance ranked No. 3", "Legion 1"),
    ("Congratulations, [Legion 2] of your alliance", "Legion 2"),
    ("for [Legion 1] in Foundry Battle", "Legion 1"),
    ("Legion 1 in Foundry", "Legion 1"),
    ("no legion here", None),
])
def test_extract_legion(text, expected):
    assert parsers.extract_legion(text) == expected


# ---------------------------------------------------------------------------
# Player + value row parsing (Foundry/Canyon/Showdown share the same parser
# for "name on the left, big number on the right" rows).
# ---------------------------------------------------------------------------

def test_parse_player_value_rows_basic():
    text = (
        "vtr 512,315\n"
        "H&N 433,354\n"
        "Trololololol 362,242\n"
        "Noura 317,994\n"
    )
    rows = parsers._parse_player_value_rows(text)
    assert rows == [
        {"name": "vtr", "value": 512315},
        {"name": "H&N", "value": 433354},
        {"name": "Trololololol", "value": 362242},
        {"name": "Noura", "value": 317994},
    ]


def test_parse_player_value_rows_strips_rank_marker():
    """OCR often picks up 'R4' / 'R5' alliance-rank stickers before the name."""
    text = (
        "AlejoRoll R5 7,577\n"
        "MIMOUN R3 8,762\n"
    )
    rows = parsers._parse_player_value_rows(text)
    assert rows == [
        {"name": "AlejoRoll", "value": 7577},
        {"name": "MIMOUN", "value": 8762},
    ]


def test_parse_player_value_rows_skips_short_names():
    text = (
        "X 1,234\n"        # 1-char name, dropped
        "ab 5,678\n"       # 2-char ok
        "  10,000\n"       # no name, dropped
    )
    rows = parsers._parse_player_value_rows(text)
    assert rows == [{"name": "ab", "value": 5678}]


def test_parse_player_value_rows_handles_unformatted_number():
    text = "Big Player 1234567\n"
    rows = parsers._parse_player_value_rows(text)
    assert rows == [{"name": "Big Player", "value": 1234567}]


def test_parse_player_value_rows_keeps_four_word_name():
    """Regression: 'Bow to thy Lord' was truncated to 'to thy Lord' by a
    last-3-tokens cap. The full name must survive."""
    rows = parsers._parse_player_value_rows("Bow to thy Lord 259,726,677\n")
    assert rows == [{"name": "Bow to thy Lord", "value": 259726677}]


def test_parse_player_value_rows_splits_merged_rows_at_garbled_number():
    """When a power value is OCR-garbled (letters mixed into the digits) the
    number regex misses it, so the chunk bleeds into the previous row. The real
    name is whatever follows the last garbled number; the leading rank-sticker
    artifact ('Rs') is dropped too."""
    text = "Hobal A36,17o,548 Rs Noura 410,700,498\n"
    rows = parsers._parse_player_value_rows(text)
    assert rows == [{"name": "Noura", "value": 410700498}]


def test_name_from_tokens_keeps_digit_names():
    """A real name with a digit or two is not a number boundary."""
    assert parsers._name_from_tokens(["Antoha28"]) == "Antoha28"
    assert parsers._name_from_tokens(["Just4Fun"]) == "Just4Fun"
    # Garbled number in front of a clean name → only the name survives.
    assert parsers._name_from_tokens(["44эаэа84", "Fifarafa"]) == "Fifarafa"


def test_foundry_rank_from_outcome():
    """Foundry's result mail has no 'ranked No. N' — a 2-alliance head-to-head,
    so a win is rank 1 and a loss rank 2."""
    assert parsers._foundry_rank_from_outcome(
        "Congratulations! Your alliance [Legion 1] prevailed in the [Foundry Battle] VICTORY") == 1
    assert parsers._foundry_rank_from_outcome(
        "Unfortunately your alliance was defeated in the [Foundry Battle]") == 2
    assert parsers._foundry_rank_from_outcome(
        "Personal Arsenal Points MIMOUN 10,585,588") is None


def test_result_header_page_skipped():
    """A Foundry/Canyon result mail's scoreboard/tally/rewards page must not be
    parsed as player rows (its alliance scores + reward totals bled into the
    leaderboard before). Header markers + no leaderboard marker → skip."""
    header = ("Congratulations [Legion 1] prevailed VICTORY [CAT]bluCATs 308,419 "
              "Legion Tally Control Rewards 230,349 Gathering Rewards 39,392 "
              "Loot Rewards 38,678 KO Rewards 20,786,720")
    assert parsers._is_result_header_page(header) is True
    # The leaderboard page itself parses normally.
    assert parsers._is_result_header_page(
        "Personal Arsenal Points MIMOUN 10,585,588 Dracula 1,419,545") is False
    # A continuation leaderboard page (no header markers) parses too.
    assert parsers._is_result_header_page("Lukaku 343,320 Ymir 30,587") is False


def test_default_lord_name_not_read_as_value():
    """Unnamed players default to 'lord<digits>' (no separator). The glued
    digit run must NOT be detected as a power value, the name must survive, and
    the real power must still be captured. Critical on new servers."""
    # Digits glued to letters are not a number; the comma-formatted power is.
    values = [v for _s, _e, v in parsers.find_formatted_numbers("5 lord235342323 211,813,266")]
    assert values == [211813266]
    assert parsers._parse_power_rows("5 lord235342323 211,813,266") == [
        {"rank": 5, "name": "lord235342323", "power": 211813266, "value": 211813266},
    ]
    assert parsers._parse_player_value_rows("lord235342323 211,813,266") == [
        {"name": "lord235342323", "value": 211813266},
    ]
    # A clean letters-then-digits token is always a name, never a boundary.
    assert parsers._looks_like_garbled_number("lord235342323") is False


# ---------------------------------------------------------------------------
# Power-rankings row parser (rank prefix + name + power, only ≥1M)
# ---------------------------------------------------------------------------

def test_parse_power_rows_basic():
    text = (
        "1 R4 vtr 1,142,832,517\n"
        "2 R3 MADO 1,034,240,119\n"
        "3 R4 AlejoCAT 1,027,972,491\n"
    )
    rows = parsers._parse_power_rows(text)
    assert len(rows) == 3
    assert rows[0]["rank"] == 1
    assert rows[0]["power"] == 1_142_832_517
    assert "vtr" in rows[0]["name"]
    assert rows[2]["rank"] == 3
    assert "AlejoCAT" in rows[2]["name"]


def test_parse_power_rows_value_mirrors_power():
    """`value` mirrors `power` so the shared OCR fallback merge/match (keyed on
    name+value, like Showdown/Foundry) works for Power Rankings too."""
    text = "1 R4 vtr 1,142,832,517\n2 R3 MADO 1,034,240,119\n"
    rows = parsers._parse_power_rows(text)
    assert rows
    for r in rows:
        assert r["value"] == r["power"]


def test_parse_power_rows_keeps_four_word_name():
    """Same truncation regression on the power parser."""
    rows = parsers._parse_power_rows("5 Bow to thy Lord 259,726,677\n")
    assert rows and rows[0]["name"] == "Bow to thy Lord"


def test_cleaner_name_dedup_preference():
    """Power Rankings dedup keeps the cleaner name on a power collision (the
    pinned self-row / scroll-overlap duplicate)."""
    # Fewer digit-noise tokens wins (decorated self-row read as '008$Trolol8e').
    assert parsers._cleaner_name("Trololololol", "008$Trolol8e") is True
    # Same noise → longer (more complete) name wins.
    assert parsers._cleaner_name("Trololololol", "Trolo") is True
    assert parsers._cleaner_name("Trolo", "Trololololol") is False


def test_parse_power_rows_drops_sub_million_values():
    """Values under 1,000,000 are not power totals — drop them."""
    text = "999 some_player 999,999\n"
    rows = parsers._parse_power_rows(text)
    assert rows == []


def test_parse_power_rows_allows_optional_rank():
    """Top-3 ranks render as medal icons (no rank digit); parser must still
    extract the row with rank=None."""
    text = "Chief Power Trololololol 925,096,779"
    rows = parsers._parse_power_rows(text)
    assert len(rows) == 1
    assert rows[0]["rank"] is None
    assert rows[0]["power"] == 925_096_779


# ---------------------------------------------------------------------------
# Compact M/B/K suffix values — Canyon/Foundry registration mails show
# combatant power as '807.3M' / '1.0B', not as comma-formatted integers.
# ---------------------------------------------------------------------------

def test_parse_player_value_rows_handles_compact_suffix():
    text = (
        "AlejoRoll R5 807.3M\n"
        "Trololololol R4 1.0B\n"
        "Virlix R4 747.8M\n"
        "Just me 515.0M\n"
    )
    rows = parsers._parse_player_value_rows(text)
    by_name = {r["name"]: r["value"] for r in rows}
    assert by_name["AlejoRoll"] == 807_300_000
    assert by_name["Trololololol"] == 1_000_000_000
    assert by_name["Virlix"] == 747_800_000
    assert by_name["Just me"] == 515_000_000


def test_find_formatted_numbers_picks_up_compact_suffix():
    text = "Power: 807.3M and 1.0B and 3K plus plain 12,345"
    nums = parsers.find_formatted_numbers(text)
    values = [v for _, _, v in nums]
    assert 807_300_000 in values
    assert 1_000_000_000 in values
    assert 3_000 in values
    assert 12_345 in values


def test_compact_suffix_doesnt_swallow_eu_thousand_separator():
    """'12.345' (EU-formatted 12345) must NOT be parsed as 12.345M."""
    values = [v for _, _, v in parsers.find_formatted_numbers("12.345 strict")]
    assert 12_345 in values
    assert 12_345_000 not in values


# ---------------------------------------------------------------------------
# Alliance Showdown final — alliance rank extraction
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    # The actual Showdown header phrasing
    ("Congratulations on ranking No. 30 on Alliance Showdown Point Ranking", 30),
    # Fallback for explicit "Alliance Rank: N" phrasing
    ("Alliance Rank: 5 in this event", 5),
    ("Alliance Ranking: 12 awarded", 12),
    # Word boundary — "ranked" (past tense) doesn't trigger; the cog still
    # captures it via the legion-result regex elsewhere
    ("of your alliance ranked No. 1 in [Canyon Clash]", 1),
])
def test_alliance_rank_regex(text, expected):
    m = parsers._ALLIANCE_RANK_RE.search(text)
    assert m is not None, f"regex didn't match: {text!r}"
    assert int(m.group(1)) == expected


# ---------------------------------------------------------------------------
# Fuzzy roster matching (the OCR-name → fid resolver)
# ---------------------------------------------------------------------------

def test_fuzzy_match_exact():
    roster = load_roster()
    fid, status = parsers.fuzzy_match_name("MIMOUN", roster)
    assert status == "auto"
    assert fid == 10007  # from default.json


def test_fuzzy_match_case_insensitive():
    roster = load_roster()
    fid, status = parsers.fuzzy_match_name("mimoun", roster)
    assert status == "auto"
    assert fid == 10007


def test_fuzzy_match_returns_no_name_on_empty():
    roster = load_roster()
    fid, status = parsers.fuzzy_match_name("", roster)
    assert fid is None
    assert status == "no_name"


def test_fuzzy_match_no_match_on_unknown_name():
    roster = load_roster()
    # All-numeric string has zero char overlap with any Latin/Arabic/Chinese
    # nickname in the roster, so similarity is 0 and status is "no_match".
    fid, status = parsers.fuzzy_match_name("777777777", roster)
    assert fid is None
    assert status == "no_match"


def test_fuzzy_match_doesnt_confuse_similar_letter_sets():
    """Regression for the bug where 'AlejoCAT' fuzzy-matched 'Caramelo' at 0.75
    via the old character-set-overlap metric. SequenceMatcher correctly puts
    them far apart."""
    roster = [(1, "Caramelo"), (2, "AlejoRoll")]  # NB: no AlejoCAT
    fid, status = parsers.fuzzy_match_name("AlejoCAT", roster)
    # Should not auto/likely match either name — AlejoRoll is the closer one
    # but still in review tier at best
    assert status in ("review", "no_match")


def test_fuzzy_match_finds_nick_inside_ocr_decorations():
    """Roster nick appearing as substring of OCR'd name (decorative chars
    around it) should auto-match — e.g. OCR captures '0g$Trololololol8e'
    for the player whose roster nick is 'Trololololol'."""
    roster = [(10006, "Trololololol")]
    fid, status = parsers.fuzzy_match_name("0g$Trololololol8e", roster)
    assert fid == 10006
    assert status in ("auto", "likely")


# ---------------------------------------------------------------------------
# Multi-candidate matching — fuzzy_match_candidates returns top-N for the
# greedy dedup pass in EventReviewView._enrich_rows.
# ---------------------------------------------------------------------------

def test_fuzzy_match_candidates_returns_all_exact_name_collisions():
    """Roster has two players with identical nick — candidates list must
    include BOTH so the dedup pass can assign each to a different row."""
    roster = [(100, "Trololololol"), (101, "Trololololol"), (200, "OtherPlayer")]
    cands = parsers.fuzzy_match_candidates("Trololololol", roster)
    fids = {c[0] for c in cands}
    assert 100 in fids
    assert 101 in fids
    # Both exact matches → both score 1.0
    exact_count = sum(1 for _, _, score, _ in cands if score == 1.0)
    assert exact_count == 2


def test_fuzzy_match_candidates_sorted_score_desc():
    roster = [(1, "Caramelo"), (2, "AlejoCAT"), (3, "AlejoRoll")]
    cands = parsers.fuzzy_match_candidates("AlejoCAT", roster)
    # AlejoCAT should be first (exact match), AlejoRoll second (partial),
    # Caramelo absent (below review threshold).
    assert cands[0][0] == 2
    if len(cands) > 1:
        assert cands[1][0] == 3
        assert cands[0][2] > cands[1][2]


def test_fuzzy_match_candidates_below_threshold_excluded():
    roster = [(1, "Caramelo"), (2, "WildlyDifferent")]
    cands = parsers.fuzzy_match_candidates("AlejoCAT", roster)
    # Caramelo scores 0.25, WildlyDifferent also low — both below 0.65
    # review threshold → empty list.
    assert cands == []


def test_fuzzy_match_candidates_empty_inputs():
    assert parsers.fuzzy_match_candidates("", [(1, "x")]) == []
    assert parsers.fuzzy_match_candidates("foo", []) == []


# ---------------------------------------------------------------------------
# assign_unique_fids — greedy global assignment, dedupes shared-name fids.
# This is the algorithm bear_track has had for ages; we now mirror it.
# ---------------------------------------------------------------------------

def test_assign_unique_fids_two_same_name_get_different_fids():
    """The headline case: roster has TWO 'Trololololol' players, screenshot
    has TWO rows. Each row must end up on a DIFFERENT fid."""
    roster = [(100, "Trololololol"), (101, "Trololololol"), (200, "Other")]
    rows = [
        {"name": "Trololololol", "value": 1_000_000_000},
        {"name": "Trololololol", "value": 925_500_000},
    ]
    result = parsers.assign_unique_fids(rows, roster)
    assert result[0]["fid"] != result[1]["fid"]
    assert {result[0]["fid"], result[1]["fid"]} == {100, 101}
    # Higher-value row gets first pick (by row_priority tiebreak)
    assert result[0]["fid"] in {100, 101}


def test_assign_unique_fids_one_roster_two_rows_unmatched_second():
    """Roster has only ONE matching entry but two rows compete for it.
    Higher-value row wins; the other stays unmatched."""
    roster = [(100, "Trololololol")]
    rows = [
        {"name": "Trololololol", "value": 1_000_000_000},
        {"name": "Trololololol", "value": 925_500_000},
    ]
    result = parsers.assign_unique_fids(rows, roster)
    assert result[0]["fid"] == 100      # higher-value row wins
    assert result[1]["fid"] is None     # loser unmatched
    assert result[1]["status"] == "no_match"


def test_assign_unique_fids_higher_value_wins_priority():
    """Rows are passed in low-then-high order — assignment still gives the
    fid to the higher-value row, not the first one in input."""
    roster = [(100, "Solo")]
    rows = [
        {"name": "Solo", "value": 100_000_000},      # low value, first in input
        {"name": "Solo", "value": 500_000_000},      # high value, second
    ]
    result = parsers.assign_unique_fids(rows, roster)
    assert result[1]["fid"] == 100      # high value got the fid
    assert result[0]["fid"] is None     # low value didn't


def test_assign_unique_fids_falls_back_to_next_best_candidate():
    """Row A scores 1.0 on fid X and 0.7 on fid Y. Row B also scores 1.0
    on fid X but doesn't have Y in its candidate list. Higher-value row
    wins X; loser falls back to Y at 0.7."""
    roster = [(100, "AlejoCAT"), (101, "AlejoCATs")]  # close but distinct
    rows = [
        {"name": "AlejoCATs", "value": 200_000_000},  # exact for 101
        {"name": "AlejoCAT", "value": 500_000_000},   # exact for 100
    ]
    result = parsers.assign_unique_fids(rows, roster)
    # Higher value gets exact match; lower value also gets exact (no collision here)
    assert result[1]["fid"] == 100      # AlejoCAT exact
    assert result[0]["fid"] == 101      # AlejoCATs exact


def test_assign_unique_fids_preserves_input_fields():
    """Extra fields on the input rows (e.g. detected_legion, rank) should
    pass through to the output."""
    roster = [(100, "Solo")]
    rows = [{"name": "Solo", "value": 100, "extra": "preserved"}]
    result = parsers.assign_unique_fids(rows, roster)
    assert result[0]["extra"] == "preserved"


# ---------------------------------------------------------------------------
# _dedup_into — substring-containment dedup across screenshots
# ---------------------------------------------------------------------------

def test_dedup_into_drops_partial_capture_with_same_value():
    """The Moly bug: 'Moly' parsed cleanly from one screenshot, '48Z lII Moly'
    (with leading row-number / artifact) from the next page's partial capture.
    Both have value=913_000_000 — must collapse to one row."""
    target = []
    parsers._dedup_into(target, {"name": "Moly", "value": 913_000_000})
    parsers._dedup_into(target, {"name": "48Z lII Moly", "value": 913_000_000})
    assert len(target) == 1
    assert target[0]["name"] == "Moly"  # shorter cleaner name wins


def test_dedup_into_keeps_shorter_name_when_arrival_reversed():
    """When the noisy capture arrives first, dedup replaces it with the
    later cleaner capture."""
    target = []
    parsers._dedup_into(target, {"name": "48Z lII Moly", "value": 913_000_000})
    parsers._dedup_into(target, {"name": "Moly", "value": 913_000_000})
    assert len(target) == 1
    assert target[0]["name"] == "Moly"


def test_dedup_into_keeps_distinct_when_values_differ():
    """Two 'Trololololol' rows with different values (1B + 925.5M) are two
    real different players — must NOT be deduped."""
    target = []
    parsers._dedup_into(target, {"name": "Trololololol", "value": 1_000_000_000})
    parsers._dedup_into(target, {"name": "Trololololol", "value": 925_500_000})
    assert len(target) == 2


def test_dedup_into_keeps_distinct_when_no_name_overlap():
    """MADO and Trololololol may both score 1.0B by coincidence — keep both
    when names share no substring."""
    target = []
    parsers._dedup_into(target, {"name": "MADO", "value": 1_000_000_000})
    parsers._dedup_into(target, {"name": "Trololololol", "value": 1_000_000_000})
    assert len(target) == 2


def test_dedup_into_handles_ocr_decorations_around_name():
    """OCR captures 'O8$Trololololol#%e' and later cleaner 'Trololololol' —
    same player, same value, must dedup to the cleaner name."""
    target = []
    parsers._dedup_into(target, {"name": "O8$Trololololol#%e", "value": 1_000_000_000})
    parsers._dedup_into(target, {"name": "Trololololol", "value": 1_000_000_000})
    assert len(target) == 1
    assert target[0]["name"] == "Trololololol"


def test_assign_unique_fids_unmatched_row_keeps_status_no_match():
    roster = [(100, "AlejoCAT")]
    rows = [
        {"name": "AlejoCAT", "value": 100},
        {"name": "WildlyDifferentName", "value": 50},
    ]
    result = parsers.assign_unique_fids(rows, roster)
    assert result[0]["fid"] == 100
    assert result[1]["fid"] is None
    assert result[1]["status"] == "no_match"
    assert result[1]["name"] == "WildlyDifferentName"  # preserved


# ---------------------------------------------------------------------------
# find_formatted_numbers — picks up thousand-separated and bare-int numbers
# ---------------------------------------------------------------------------

def test_find_formatted_numbers_picks_up_thousands_and_bare():
    text = "Points: 12,345,678 and bare 999999 plus tiny 99"
    nums = parsers.find_formatted_numbers(text)
    # tiny 99 is below 4-digit threshold → not captured
    values = [v for _, _, v in nums]
    assert 12_345_678 in values
    assert 999_999 in values
    assert 99 not in values


# ---------------------------------------------------------------------------
# Compact-int parser: "31.1M" / "430" / "3.0K" → int
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("token,expected", [
    ("19.9M",   19_900_000),
    ("3.7M",    3_700_000),
    ("3.0K",    3_000),
    ("430",     430),
    ("1,234",   1_234),
    ("invalid", None),
    ("",        None),
])
def test_parse_compact_int(token, expected):
    assert parsers._parse_compact_int(token) == expected


# ---------------------------------------------------------------------------
# Alliance scoreboard: three cards collapsed into one OCR line
# ---------------------------------------------------------------------------

def test_parse_alliance_scoreboard_canyon():
    text = (
        "Congratulations, [Legion 2] of your alliance ranked No. 2 in [Canyon Clash]! "
        "Here are the battle details: "
        "#293 #312 #369 "
        "Legion 2 Legion 2 Legion 1 "
        "[BRF]BestRoyalFamily [CAT]bluCATs [LMN]LMN "
        "612,477 499,037 394,849 "
        "Stats MVP"
    )
    sb = parsers._parse_alliance_scoreboard(text)
    assert len(sb) == 3
    assert sb[0]["rank"] == 293
    assert sb[0]["legion"] == "Legion 2"
    assert sb[0]["tag"] == "BRF"
    assert sb[0]["score"] == 612_477
    assert sb[1]["tag"] == "CAT"
    assert sb[1]["score"] == 499_037
    assert sb[2]["tag"] == "LMN"
    assert sb[2]["score"] == 394_849


def test_parse_alliance_scoreboard_returns_empty_when_no_ids():
    assert parsers._parse_alliance_scoreboard("no card data here") == []


# ---------------------------------------------------------------------------
# Scoreboard name cleaning: wrap-fragments and duplicate-of-tag prefix
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,tag,expected", [
    # Tag == name in game (e.g. [LMN]LMN), wrap 'ly' from BestRoyalFami|ly
    # landed here. Strip duplicate tag, then 'ly' is too short → fall back to tag.
    ("LMN ly",         "LMN", "LMN"),
    # Trailing wrap fragment 'ance' (from WarriorsAlliance) attached to bluCATs
    ("bluCATs ance",   "CAT", "bluCATs"),
    # Clean cases — no modification
    ("BestRoyalFami",  "BRF", "BestRoyalFami"),
    ("bluCATs",        "CAT", "bluCATs"),
    ("Top Brl Union",  "TBU", "Top Brl Union"),
    # Tag fully matches name (no wrap orphan)
    ("LMN",            "LMN", "LMN"),
    # Empty raw → falls back to tag
    ("",               "ABC", "ABC"),
    # All-lowercase short fragment alone → falls back to tag
    ("ly",             "BRF", "BRF"),
    # Name starts with tag letters but isn't tag duplication ([CAT]Caterpillar)
    ("Caterpillar",    "CAT", "Caterpillar"),
])
def test_clean_scoreboard_name(raw, tag, expected):
    assert parsers._clean_scoreboard_name(raw, tag) == expected


# ---------------------------------------------------------------------------
# Spatial scoreboard parser — uses OCR bounding boxes to keep wrapped name
# fragments with their origin alliance card.
# ---------------------------------------------------------------------------

def _bbox(cx: float, cy: float, w: float = 60, h: float = 18) -> list:
    """Convenience: build a 4-corner bbox from a centroid + size."""
    hx, hy = w / 2, h / 2
    return [
        [cx - hx, cy - hy],
        [cx + hx, cy - hy],
        [cx + hx, cy + hy],
        [cx - hx, cy + hy],
    ]


def test_spatial_scoreboard_reassembles_wrapped_name():
    """Mid-word wrap (BestRoyalFami|ly) — the 'ly' chunk sits directly under
    'BestRoyalFami' in the BRF column. Spatial parser must keep them together."""
    blocks = [
        ("#293",                _bbox(100, 100)),
        ("#312",                _bbox(300, 100)),
        ("#369",                _bbox(500, 100)),
        ("Legion 2",            _bbox(100, 130)),
        ("Legion 2",            _bbox(300, 130)),
        ("Legion 1",            _bbox(500, 130)),
        ("[BRF]BestRoyalFami",  _bbox(100, 200)),
        ("ly",                  _bbox(100, 220)),  # ← wrap fragment, mid-word
        ("[CAT]bluCATs",        _bbox(300, 200)),
        ("[LMN]LMN",            _bbox(500, 200)),
        ("612,477",             _bbox(100, 260)),
        ("499,037",             _bbox(300, 260)),
        ("394,849",             _bbox(500, 260)),
    ]
    sb = parsers._parse_alliance_scoreboard_spatial(blocks)
    by_tag = {r["tag"]: r for r in sb}
    assert by_tag["BRF"]["name"] == "BestRoyalFamily"  # ← reassembled
    assert by_tag["CAT"]["name"] == "bluCATs"
    assert by_tag["LMN"]["name"] == "LMN"
    assert by_tag["BRF"]["rank"] == 293
    assert by_tag["BRF"]["legion"] == "Legion 2"
    assert by_tag["BRF"]["score"] == 612_477


def test_spatial_scoreboard_word_boundary_wrap_uses_space():
    """A wrap between two words (lowercase end → uppercase start) joins with
    a space, not concatenation."""
    blocks = [
        ("[TBU]Top Brl",  _bbox(100, 200)),
        ("Union",         _bbox(100, 220)),  # ← starts uppercase, word break
        ("123,456",       _bbox(100, 260)),
    ]
    sb = parsers._parse_alliance_scoreboard_spatial(blocks)
    assert sb[0]["name"] == "Top Brl Union"


def test_spatial_scoreboard_empty_blocks_returns_empty():
    assert parsers._parse_alliance_scoreboard_spatial([]) == []


def test_spatial_scoreboard_skips_legion_anchor_in_body_text():
    """`[Legion 1]` from the 'Congratulations, [Legion 1] of your alliance
    ranked No. 2' header must NOT be treated as a scoreboard anchor —
    otherwise we get a phantom 4th card."""
    blocks = [
        ("Congratulations, [Legion 1] of your alliance ranked No. 2 in",
         _bbox(400, 50)),
        ("#312",            _bbox(100, 100)),
        ("Legion 1",        _bbox(100, 130)),
        ("[CAT]bluCATs",    _bbox(100, 200)),
        ("478,798",         _bbox(100, 260)),
        ("Stats",           _bbox(100, 400)),
        ("Total Fuel Used: 30.8M", _bbox(100, 440)),
    ]
    sb = parsers._parse_alliance_scoreboard_spatial(blocks)
    assert len(sb) == 1
    assert sb[0]["tag"] == "CAT"
    assert sb[0]["score"] == 478_798
    # Stats text must NOT have polluted the name.
    assert "Total Fuel" not in sb[0]["name"]
    assert "Stats" not in sb[0]["name"]


def test_spatial_scoreboard_drops_cards_with_no_score():
    """Anchors that look like alliance tags but have no score nearby are
    body-text artifacts, not real cards. Drop them silently."""
    blocks = [
        ("[FAKE]Something", _bbox(200, 50)),  # no score below this anchor
        ("#312",            _bbox(100, 100)),
        ("[CAT]bluCATs",    _bbox(100, 200)),
        ("478,798",         _bbox(100, 260)),
    ]
    sb = parsers._parse_alliance_scoreboard_spatial(blocks)
    assert len(sb) == 1
    assert sb[0]["tag"] == "CAT"


def test_spatial_scoreboard_name_does_not_eat_stats_or_mvp_text():
    """The user-reported bug: Stats column and MVP column text sat
    in the x-range of the middle/right scoreboard cards and got pulled
    into their names. After the score-bounded fix this no longer happens.
    """
    blocks = [
        # Scoreboard row at top
        ("#449",                _bbox(100, 100)),
        ("Legion 1",            _bbox(100, 130)),
        ("[GML]GreatMoonlight", _bbox(100, 200)),
        ("688,408",             _bbox(100, 260)),
        ("#312",                _bbox(300, 100)),
        ("Legion 1",            _bbox(300, 130)),
        ("[CAT]bluCATs",        _bbox(300, 200)),
        ("478,798",             _bbox(300, 260)),
        ("#463",                _bbox(500, 100)),
        ("Legion 1",            _bbox(500, 130)),
        ("[JFF]Just4Fun",       _bbox(500, 200)),
        ("345,412",             _bbox(500, 260)),
        # Stats column (under left card)
        ("Stats",               _bbox(150, 400)),
        ("Total Fuel Used: 30.8M",    _bbox(150, 440)),
        ("Enemy Squads Defeated: 615", _bbox(150, 480)),
        # MVP column (under right card)
        ("MVP",                 _bbox(550, 400)),
        ("MIMOUN: 3.5M",        _bbox(550, 440)),
        # Footer / personal ranking section
        ("Personal Point Ranking", _bbox(300, 700)),
        ("Delete",              _bbox(300, 1000)),
    ]
    sb = parsers._parse_alliance_scoreboard_spatial(blocks)
    by_tag = {r["tag"]: r for r in sb}
    assert set(by_tag) == {"GML", "CAT", "JFF"}
    assert by_tag["GML"]["name"] == "GreatMoonlight"
    assert by_tag["CAT"]["name"] == "bluCATs"
    assert by_tag["JFF"]["name"] == "Just4Fun"
    for r in sb:
        assert "Total Fuel" not in r["name"]
        assert "MVP" not in r["name"]
        assert "MIMOUN" not in r["name"]
        assert "Personal Point" not in r["name"]
        assert "Delete" not in r["name"]


def test_spatial_scoreboard_malformed_boxes_dont_crash():
    blocks = [("text", None), ("more", "not a box")]
    # No tag anchors found → returns empty
    assert parsers._parse_alliance_scoreboard_spatial(blocks) == []


def test_scoreboard_orphan_wrap_does_not_attach_to_wrong_alliance():
    """The bug: 'BestRoyalFami|ly' wraps in the UI; OCR drops 'ly' between
    the [LMN] tag and the score column, so 'ly' got attached to LMN as
    '[LMN]LMN ly'. After the fix, LMN's name is just 'LMN'."""
    text = (
        "Congratulations, [Legion 2] ranked No. 2 in [Canyon Clash]! "
        "Here are the battle details: "
        "#293 #312 #369 "
        "Legion 2 Legion 2 Legion 1 "
        "[BRF]BestRoyalFami [CAT]bluCATs [LMN]LMN ly "
        "612,477 499,037 394,849 "
        "Stats MVP"
    )
    sb = parsers._parse_alliance_scoreboard(text)
    by_tag = {row["tag"]: row for row in sb}
    assert by_tag["BRF"]["name"] == "BestRoyalFami"
    assert by_tag["CAT"]["name"] == "bluCATs"
    assert by_tag["LMN"]["name"] == "LMN"  # NOT 'LMN ly'


# ---------------------------------------------------------------------------
# Stats panel
# ---------------------------------------------------------------------------

def test_parse_stats_panel_full():
    text = (
        "Total Fuel Used: 19.9M "
        "Enemy Squads Defeated: 388 "
        "Occupied Buildings: 128 "
        "Total Battle Speedups Used: 2 "
        "Total March Accelerator I Used: 59 "
        "Total March Accelerator II Used: 62 "
        "Total Retreats: 0 "
        "Total Advances: 14"
    )
    stats = parsers._parse_stats_panel(text)
    assert stats["fuel_used"] == 19_900_000
    assert stats["squads_defeated"] == 388
    assert stats["buildings"] == 128
    assert stats["speedups"] == 2
    assert stats["march_i"] == 59
    assert stats["march_ii"] == 62
    assert stats["retreats"] == 0
    assert stats["advances"] == 14


def test_parse_stats_panel_partial_when_some_missing():
    text = "Total Fuel Used: 31.1M Enemy Squads Defeated: 430"
    stats = parsers._parse_stats_panel(text)
    assert stats == {"fuel_used": 31_100_000, "squads_defeated": 430}


# ---------------------------------------------------------------------------
# MVPs — paired by proximity to each stat label
# ---------------------------------------------------------------------------

def test_parse_mvps_simple_pairs():
    text = (
        "Total Fuel Used: 19.9M marshmallow: 3.7M "
        "Enemy Squads Defeated: 388 vtr: 122 "
        "Occupied Buildings: 128 AlejoCAT: 25"
    )
    mvps = parsers._parse_mvps(text)
    by_key = {m["stat_key"]: m for m in mvps}
    assert by_key["fuel_used"]["name"] == "marshmallow"
    assert by_key["fuel_used"]["value"] == 3_700_000
    assert by_key["squads_defeated"]["name"] == "vtr"
    assert by_key["squads_defeated"]["value"] == 122
    assert by_key["buildings"]["name"] == "AlejoCAT"
    assert by_key["buildings"]["value"] == 25


# ---------------------------------------------------------------------------
# _dedup_into — same-value name merge prefers the cleaner / more complete name
# ---------------------------------------------------------------------------

def _dedup(names_vals):
    target = []
    for name, val in names_vals:
        parsers._dedup_into(target, {"name": name, "value": val})
    return [r["name"] for r in target]


def test_dedup_keeps_legit_prefix_not_truncated():
    # 'Bow to thy Lord' split across pages must not collapse to 'to thy Lord'.
    assert _dedup([("to thy Lord", 1477213), ("Bow to thy Lord", 1477213)]) == ["Bow to thy Lord"]
    assert _dedup([("Bow to thy Lord", 1477213), ("to thy Lord", 1477213)]) == ["Bow to thy Lord"]


def test_dedup_drops_ocr_noise_prefix():
    # Digit-noise prefix capture is still dropped in favour of the clean name.
    assert _dedup([("48Z lII Moly", 999), ("Moly", 999)]) == ["Moly"]


def test_dedup_keeps_distinct_values():
    assert _dedup([("Alpha", 1), ("Beta", 2)]) == ["Alpha", "Beta"]
