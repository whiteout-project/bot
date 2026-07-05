import importlib
bt = importlib.import_module("cogs.bear_track")


def _box(x_left, y_top, w=40, h=20):
    """Axis-aligned 4-corner box: TL, TR, BR, BL."""
    x_right, y_bottom = x_left + w, y_top + h
    return [[x_left, y_top], [x_right, y_top], [x_right, y_bottom], [x_left, y_bottom]]


def test_boxed_rows_groups_same_line_sorts_by_x():
    # Two tokens on the same y-band, given out of x-order.
    tokens = [("World", _box(120, 100)), ("Hello", _box(10, 102))]
    rows = bt._boxed_rows(tokens)
    assert len(rows) == 1
    assert [t[0] for t in rows[0]["tokens"]] == ["Hello", "World"]


def test_boxed_rows_separates_distinct_lines_top_to_bottom():
    tokens = [("row2", _box(10, 200)), ("row1", _box(10, 100))]
    rows = bt._boxed_rows(tokens)
    assert [r["tokens"][0][0] for r in rows] == ["row1", "row2"]


def test_boxed_rows_skips_missing_boxes():
    tokens = [("keep", _box(10, 100)), ("drop", None), ("drop2", [])]
    rows = bt._boxed_rows(tokens)
    assert len(rows) == 1
    assert rows[0]["tokens"][0][0] == "keep"


def test_box_y_bounds_none_for_malformed():
    assert bt._box_y_bounds(None) is None
    assert bt._box_y_bounds([[1, 2]]) is None  # too few corners


def _r(y_top, y_bottom):
    return {"y_top": y_top, "y_bottom": y_bottom, "y_center": (y_top + y_bottom) / 2.0}


def test_align_matches_by_overlap_in_order():
    p = [_r(100, 120), _r(200, 220), _r(300, 320)]
    f = [_r(198, 220), _r(301, 322), _r(100, 121)]  # shuffled, same bands
    pairs = bt._align_rows_by_y(p, f)
    assert [pf[0] for pf in pairs] == p
    assert [None if pf[1] is None else pf[1]["y_center"] for pf in pairs] == [110.5, 209.0, 311.5]


def test_align_missing_fallback_row_does_not_shift_others():
    p = [_r(100, 120), _r(200, 220), _r(300, 320)]
    f = [_r(101, 121), _r(301, 321)]  # middle fallback row dropped
    pairs = bt._align_rows_by_y(p, f)
    assert pairs[0][1] is not None and pairs[1][1] is None and pairs[2][1] is not None


def test_align_no_overlap_yields_none():
    p = [_r(100, 120)]
    f = [_r(500, 520)]
    assert bt._align_rows_by_y(p, f) == [(p[0], None)]


ROSTER = [(1, "ксюха"), (2, "Numb Little Bug"), (3, "BlackMask")]


def test_row_name_drops_number_and_rank():
    toks = [("15", 5.0), ("[sir]abc", 30.0), ("Damage", 200.0),
            ("Points:2,069,408,586", 260.0)]
    # rank "15" and the number/label are dropped; the name token stays.
    name = bt._row_name_from_tokens(toks)
    assert "2,069,408,586" not in name and "15" not in name.split()


def test_merge_by_boxes_fills_cyrillic_from_aligned_row():
    # Primary read the number well but garbled the Cyrillic name to "kcioxa";
    # the cyrillic fallback read the name on the same Y band.
    primary = [("kcioxa", _box(10, 100)), ("2,069,408,586", _box(300, 100))]
    fallback = [("ксюха", _box(12, 101)), ("2069408586", _box(305, 101))]
    img_rows = {2069408586: {"name": "kcioxa", "damage": 2069408586, "rank": 15}}
    filled = bt.merge_fallback_rows_by_boxes(img_rows, primary, fallback, ROSTER, "cyrillic")
    assert filled is True
    assert img_rows[2069408586]["name"] == "ксюха"


def test_merge_by_boxes_keeps_good_primary_name():
    primary = [("BlackMask", _box(10, 100)), ("9,287,178,025", _box(300, 100))]
    fallback = [("garbage", _box(12, 101)), ("9287178025", _box(305, 101))]
    img_rows = {9287178025: {"name": "BlackMask", "damage": 9287178025, "rank": 12}}
    filled = bt.merge_fallback_rows_by_boxes(img_rows, primary, fallback, ROSTER, "cyrillic")
    assert img_rows[9287178025]["name"] == "BlackMask"  # already matches roster, untouched


def test_boxed_rows_does_not_split_tall_first_place_cell():
    # A rank-1 row can be visually taller; its tokens must stay one row.
    tokens = [("Bruh", _box(120, 100, h=44)), ("Damage", _box(300, 108, h=20)),
              ("Points:40,691,455", _box(360, 108, h=20))]
    rows = bt._boxed_rows(tokens)
    assert len(rows) == 1


def test_align_handles_top3_medal_rows_without_rank_digit():
    # Medal rows have no rank digit; alignment is purely geometric so it holds.
    p = [_r(100, 140), _r(160, 200)]
    f = [_r(102, 142), _r(162, 202)]
    pairs = bt._align_rows_by_y(p, f)
    assert all(pf[1] is not None for pf in pairs)


import asyncio


class _NullCtx:
    async def __aenter__(self): return None
    async def __aexit__(self, *a): return False


def test_attachment_uses_box_align_for_cyrillic(monkeypatch):
    cog = bt.BearTrack.__new__(bt.BearTrack)  # bypass __init__

    # Primary (en) read a good number but garbled the Cyrillic name; the
    # cyrillic fallback read the NAME only (no parseable number), so only box
    # alignment can fill it - the cheap damage-key merge has nothing to match.
    # A header row on its own y-band gives find_ranking_section_start a hit
    # without changing the data row's token shape.
    header_boxed = [("Trap", _box(10, 20)), ("1", _box(60, 20)),
                     ("Damage", _box(90, 20)), ("Ranking", _box(160, 20))]
    data_boxed = [("[sir]kcioxa", _box(10, 100)), ("15", _box(140, 100)),
                  ("Damage", _box(210, 100)), ("Points:2,069,408,586", _box(320, 100))]
    en_boxed = header_boxed + data_boxed
    cyr_boxed = [("ксюха", _box(12, 101))]

    async def fake_boxes(image_bytes, lang, *, session=None):
        return cyr_boxed if lang == "cyrillic" else en_boxed

    monkeypatch.setattr(bt, "ocr_bytes_with_boxes", fake_boxes)
    monkeypatch.setattr(cog, "_acquire_ocr_slot", lambda *a, **k: _NullCtx())

    roster = [(1, "ксюха")]
    res = asyncio.run(cog._ocr_attachment_to_result(
        b"x", "en", ["cyrillic"], filename="t.png", roster=roster))
    assert any(r["name"] == "ксюха" for r in res.rows.values())


def test_row_name_strips_damage_label_and_bracketless_tag():
    # OCR dropped the [CAT] brackets and the "Damage" label word trails the name;
    # the box-align name must be cleaned the same way parse_player_rows cleans it,
    # so it resolves to the real player instead of a wrong roster member.
    toks = [("Cat", 10.0), ("Moonl", 40.0), ("ight", 80.0),
            ("Damage", 130.0), ("Points:31,202,603,220", 190.0)]
    name = bt._row_name_from_tokens(toks)
    assert "Damage" not in name
    assert not name.lower().startswith("cat ")
    assert bt.match_roster(name, [(1, "alki's"), (2, "Moonlight")])[0][1] == "Moonlight"
