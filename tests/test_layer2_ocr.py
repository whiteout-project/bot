"""Layer-2 tests: end-to-end OCR on real PNG fixtures.

These tests run RapidOCR over actual game screenshots and assert that the
full pipeline (engine → digit repair → trap/rallies/total extraction →
row parsing → roster matching) produces the expected structured output.

Slow (~5-30s per fixture, depending on which engines need warming up) and
not 100% deterministic across hardware, so the assertions are deliberately
loose: damage values and trap numbers are exact, but a row whose name
isn't extractable in this language can be marked `allow_unmatched: true`
to permit a missing match without failing.

Run:  python -m pytest test_layer2_ocr.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from harness import bt, run_ocr_pipeline, load_roster, FIXTURES, discover_fixtures


# Map a fixture's UI language → RapidOCR primary engine code.
#
# RapidOCR doesn't ship a Thai recognition model, but the damage column on
# bear-hunt screenshots is Latin digits regardless of UI language. The 'ch'
# default model gets confused by the surrounding Thai script and garbles
# digits ('19,903,598,834' → '9,903,598,84'); the 'en' model is far more
# stable on the Latin-only digit strip even when Thai chars appear next to
# it. Recommend admins pick 'en' as primary for Thai-speaking alliances.
LANG_TO_OCR = {
    "english":             "en",
    "japanese":            "japan",
    "korean":              "korean",
    "chinese_simplified":  "ch",
    "chinese_traditional": "chinese_cht",
    "thai":                "en",
    "arabic":              "arabic",
}

# Fallbacks help recover Latin names that a non-Latin engine garbles.
# Mirrors what the cog does in production (`fallback_langs` from
# alliance settings, sorted with Latin-only langs last).
LANG_TO_FALLBACKS = {
    "english":             [],
    "japanese":            ["en"],
    "korean":              ["en"],
    "chinese_simplified":  ["en"],
    "chinese_traditional": ["en"],
    "thai":                ["en"],
    "arabic":              ["en"],
}


_STATUS_RANK = {"none": 0, "ambiguous": 1, "likely": 2, "auto": 3}


def _meets_min_status(actual: str, minimum: str) -> bool:
    return _STATUS_RANK.get(actual, 0) >= _STATUS_RANK.get(minimum, 0)


def _row_for_damage(rows: list, damage: int):
    for r in rows:
        if r.get("damage") == damage:
            return r
    return None


def _classify_row_against_roster(row: dict, roster) -> tuple:
    """Re-run roster matching for a row from the OCR pipeline. Returns
    (matched_fid_or_None, status). The pipeline output already has 'name'
    on each row; we redo classification here so the test isn't sensitive
    to whether the harness happens to attach match info.
    """
    name = (row.get("name") or "").strip()
    if not name:
        return None, "none"
    candidates = bt.match_roster(name, roster)
    status = bt.classify_match(candidates)
    fid = candidates[0][0] if candidates else None
    return fid, status


# Discover fixtures at collection time so each shows up as its own test.
_FIXTURES = discover_fixtures()


def _fixture_id(case):
    return case.screenshot_path.stem


@pytest.mark.parametrize("case", _FIXTURES, ids=_fixture_id)
def test_fixture_extraction(case):
    expected = case.expected
    roster = case.roster

    image_bytes = case.screenshot_path.read_bytes()
    ui_lang = expected.get("language", "english")
    primary_lang = LANG_TO_OCR.get(ui_lang, "ch")
    fallback_langs = LANG_TO_FALLBACKS.get(ui_lang, [])

    result = run_ocr_pipeline(
        image_bytes,
        primary_lang=primary_lang,
        fallback_langs=fallback_langs,
        roster=roster,
    )

    if "trap" in expected:
        assert result["trap"] == expected["trap"], (
            f"{case.screenshot_path.name}: expected trap "
            f"{expected['trap']!r}, got {result['trap']!r}"
        )

    if "rallies" in expected:
        assert result["rallies"] == expected["rallies"], (
            f"{case.screenshot_path.name}: expected rallies "
            f"{expected['rallies']!r}, got {result['rallies']!r}"
        )

    if "total_damage" in expected:
        assert result["total_damage"] == expected["total_damage"], (
            f"{case.screenshot_path.name}: expected total_damage "
            f"{expected['total_damage']}, got {result['total_damage']}"
        )

    if "date" in expected:
        assert result["date"] == expected["date"], (
            f"{case.screenshot_path.name}: expected date "
            f"{expected['date']!r}, got {result['date']!r}"
        )

    expected_rows = expected.get("expected_rows", [])
    actual_rows = result["rows"]

    if not expected_rows and expected.get("screen_type") == "summary":
        assert actual_rows == [], (
            f"{case.screenshot_path.name}: summary screen must yield zero "
            f"rows but got: {[(r.get('damage'), r.get('name')) for r in actual_rows]}"
        )
        return

    actual_damages = sorted(r["damage"] for r in actual_rows)
    for spec in expected_rows:
        damage = spec["damage"]
        row = _row_for_damage(actual_rows, damage)
        assert row is not None, (
            f"{case.screenshot_path.name}: expected damage {damage:,} not "
            f"found in extracted rows. Got: {actual_damages}"
        )

        match_spec = spec.get("match")
        if not match_spec:
            continue

        fid, status = _classify_row_against_roster(row, roster)
        allow_unmatched = match_spec.get("allow_unmatched", False)

        if fid is None and allow_unmatched:
            continue

        expected_fid = match_spec.get("fid")
        if expected_fid is not None:
            assert fid == expected_fid, (
                f"{case.screenshot_path.name}: row with damage "
                f"{damage:,} matched fid={fid} (name={row.get('name')!r}) "
                f"but expected fid={expected_fid}"
            )

        min_status = match_spec.get("min_status")
        if min_status:
            assert _meets_min_status(status, min_status), (
                f"{case.screenshot_path.name}: row with damage "
                f"{damage:,} (name={row.get('name')!r}) had status="
                f"{status}, expected at least {min_status}"
            )


def test_layer2_fixtures_present():
    """Sanity check — make sure layer 2 isn't silently empty."""
    assert _FIXTURES, (
        f"No layer-2 fixtures discovered. Looked in {FIXTURES / 'screenshots'}/ "
        f"for PNGs with matching JSON in {FIXTURES / 'expected'}/."
    )
