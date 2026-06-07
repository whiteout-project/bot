"""Layer-2 tests: real OCR on PNG fixtures for the attendance OCR pipeline.

Each fixture in `fixtures/attendance/screenshots/` is OCR'd via the bot's
RapidOCR engine, parsed by the appropriate session class, and checked
against assertions in `fixtures/attendance/expected/<name>.json` if one
exists.

Two layers of assertion:
  • All fixtures: parser doesn't crash and basic shape invariants hold
    (rows is a list, legion regex didn't blow up, etc.).
  • Fixtures with expected JSON: specific values must be extracted
    (legion, header_date, min_rows, named rows with values + roster
    match quality).

Slow (~5-30s per fixture). Run:
  python -m pytest test_attendance_ocr_layer2.py -v
"""
from __future__ import annotations

import pytest

from harness_attendance import (
    parsers, discover_fixtures, ocr_image, load_roster, FIXTURES,
)


_STATUS_RANK = {"no_match": 0, "no_name": 0, "review": 1, "likely": 2, "auto": 3}


def _meets_min_status(actual: str, minimum: str) -> bool:
    return _STATUS_RANK.get(actual, 0) >= _STATUS_RANK.get(minimum, 0)


def _row_for_name_value(rows: list, name: str, value: int) -> dict | None:
    """Find a row by name AND value when both rows of the same value exist,
    falling back to value-only when names don't overlap.
    """
    name_lower = name.lower()
    # Prefer name + value together (handles same-value collisions where
    # MADO and Trololololol both OCR'd at 1.0B).
    for r in rows:
        rn = (r.get("name") or "").lower()
        if r.get("value") == value and (name_lower in rn or rn in name_lower):
            return r
    # Fallback: name match alone (value may have OCR'd slightly differently).
    for r in rows:
        rn = (r.get("name") or "").lower()
        if name_lower in rn or rn in name_lower:
            return r
    # Last resort: any row with that value.
    for r in rows:
        if r.get("value") == value:
            return r
    return None


def _parse_rows_for_event(ocr_text: str, event_type: str) -> list[dict]:
    """Run the right row parser for the event type, normalising rows so
    every row has `name` and `value` keys (power-rankings uses `power`).
    """
    if event_type == "power_rankings":
        return [
            {"name": r["name"], "value": r["power"]}
            for r in parsers._parse_power_rows(ocr_text)
        ]
    return parsers._parse_player_value_rows(ocr_text)


_FIXTURES = discover_fixtures()


def _fixture_id(case):
    return case.screenshot_path.stem


@pytest.mark.parametrize("case", _FIXTURES, ids=_fixture_id)
def test_fixture_extraction(case):
    expected = case.expected
    roster = case.roster

    ocr_text = ocr_image(case.screenshot_path)

    # ── universal invariants (run for every fixture, no expected JSON needed)

    # Legion detector either finds a legion or returns None; never raises
    legion = parsers.extract_legion(ocr_text)
    assert legion is None or legion in ("Legion 1", "Legion 2"), (
        f"{case.screenshot_path.name}: legion regex returned unexpected {legion!r}"
    )

    # Date extractor likewise
    header_date = parsers.extract_header_date(ocr_text)
    if header_date is not None:
        assert header_date.year >= 2024, (
            f"{case.screenshot_path.name}: implausible year {header_date.year}"
        )

    # Row parser shouldn't crash and should return a list
    rows = _parse_rows_for_event(ocr_text, case.event_type)
    assert isinstance(rows, list)

    # ── detail assertions only if expected JSON was provided

    if expected is None:
        return  # universal invariants passed; nothing else to check

    if "header_date" in expected and expected["header_date"] is not None:
        assert header_date is not None, (
            f"{case.screenshot_path.name}: expected date "
            f"{expected['header_date']!r}, got None"
        )
        assert header_date.isoformat() == expected["header_date"], (
            f"{case.screenshot_path.name}: expected date "
            f"{expected['header_date']!r}, got {header_date.isoformat()!r}"
        )

    if "legion" in expected and expected["legion"] is not None:
        assert legion == expected["legion"], (
            f"{case.screenshot_path.name}: expected legion "
            f"{expected['legion']!r}, got {legion!r}"
        )

    if "alliance_rank" in expected and expected["alliance_rank"] is not None:
        m = parsers._ALLIANCE_RANK_RE.search(ocr_text)
        assert m is not None, (
            f"{case.screenshot_path.name}: expected alliance_rank "
            f"{expected['alliance_rank']} but rank regex found nothing"
        )
        assert int(m.group(1)) == expected["alliance_rank"], (
            f"{case.screenshot_path.name}: expected alliance_rank "
            f"{expected['alliance_rank']}, got {m.group(1)}"
        )

    if "min_rows" in expected:
        assert len(rows) >= expected["min_rows"], (
            f"{case.screenshot_path.name}: expected at least "
            f"{expected['min_rows']} rows, got {len(rows)}. Rows: "
            f"{[(r.get('name'), r.get('value')) for r in rows]}"
        )

    for spec in expected.get("expected_rows", []):
        row = _row_for_name_value(rows, spec["name"], spec["value"])
        assert row is not None, (
            f"{case.screenshot_path.name}: expected row "
            f"name={spec['name']!r} value={spec['value']:,} not found. "
            f"Got: {[(r.get('name'), r.get('value')) for r in rows]}"
        )

        # roster match quality
        min_status = spec.get("min_status")
        expected_fid = spec.get("expected_fid")
        if min_status or expected_fid is not None:
            fid, status = parsers.fuzzy_match_name(row["name"], roster)
            if expected_fid is not None:
                assert fid == expected_fid, (
                    f"{case.screenshot_path.name}: row "
                    f"name={row['name']!r} matched fid={fid}, expected "
                    f"fid={expected_fid}"
                )
            if min_status:
                assert _meets_min_status(status, min_status), (
                    f"{case.screenshot_path.name}: row name={row['name']!r} "
                    f"matched with status={status!r}, expected at least "
                    f"{min_status!r}"
                )


def test_layer2_fixtures_present():
    """Sanity check — make sure the suite isn't silently empty."""
    assert _FIXTURES, (
        f"No attendance-OCR fixtures discovered in "
        f"{FIXTURES / 'screenshots'}/."
    )


def test_classify_event_against_real_ocr_fixtures():
    """Verify classify_event picks the right (event_type, kind) for each
    fixture's actual OCR output. Catches fingerprint regressions where
    synthetic-text tests pass but real OCR doesn't match the regex.
    Expected JSON may include an optional `kind` field; when absent, only
    the event_type is checked.
    """
    failures = []
    for case in _FIXTURES:
        if case.expected is None:
            continue
        ocr_text = ocr_image(case.screenshot_path)
        all_events = list(parsers.EVENT_TYPES.keys())
        classified = parsers.classify_event(ocr_text, all_events, {})
        actual_et = classified[0] if classified else None
        actual_kind = classified[1] if classified else None
        expected_et = case.event_type
        expected_kind = (case.expected or {}).get("kind")

        et_ok = (actual_et == expected_et)
        kind_ok = (expected_kind is None or actual_kind == expected_kind)
        if not (et_ok and kind_ok):
            preview = ocr_text[:200].replace("\n", " ")
            failures.append(
                f"  {case.screenshot_path.name}: "
                f"expected ({expected_et!r}, {expected_kind!r}), "
                f"got ({actual_et!r}, {actual_kind!r}). "
                f"OCR preview: {preview!r}"
            )
    assert not failures, (
        "Real-OCR classification mismatches:\n" + "\n".join(failures)
    )


def test_at_least_one_expected_json_per_event_type():
    """For each event type that has fixtures, at least one fixture must
    have an expected JSON. Otherwise layer 2 silently degrades to
    'parser doesn't crash' for that event."""
    covered: dict[str, bool] = {}
    for case in _FIXTURES:
        covered.setdefault(case.event_type, False)
        if case.expected is not None:
            covered[case.event_type] = True
    uncovered = [et for et, has in covered.items() if not has]
    assert not uncovered, (
        f"Event types with screenshots but no expected JSON: {uncovered}. "
        f"Add at least one fixtures/attendance/expected/*.json per event."
    )
