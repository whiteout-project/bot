from cogs.attendance_ocr_parsers import _rematch_displaced_fids


def test_edit_to_used_fid_displaces_to_next_free_candidate():
    bucket = [
        {"name": "Trolololo A", "fid": 45379845,
         "candidates": [(45379845, "TA", 90, "matched"), (111, "TA2", 70, "low")]},
        {"name": "Trolololo B", "fid": 45379845,
         "candidates": [(45379845, "TB", 88, "matched"), (222, "TB2", 65, "low")]},
    ]
    out = _rematch_displaced_fids(bucket, 0, 45379845, lambda f: {111: "TA2", 222: "TB2"}.get(f))
    assert bucket[0]["fid"] == 45379845           # the edited row keeps the id
    assert bucket[1]["fid"] == 222                # displaced row -> its next free candidate
    assert bucket[1]["status"] == "low"
    assert out == [("Trolololo B", 222, "TB2")]


def test_displaced_with_no_free_candidate_becomes_unmatched():
    bucket = [
        {"name": "A", "fid": 5, "candidates": [(5, "A", 90, "matched")]},
        {"name": "B", "fid": 5, "candidates": [(5, "B", 88, "matched")]},  # only candidate is the taken id
    ]
    out = _rematch_displaced_fids(bucket, 0, 5, lambda f: None)
    assert bucket[1]["fid"] is None and bucket[1]["status"] == "no_match"
    assert out == [("B", None, None)]


def test_no_collision_is_noop():
    bucket = [{"name": "A", "fid": 1, "candidates": []},
              {"name": "B", "fid": 2, "candidates": []}]
    out = _rematch_displaced_fids(bucket, 0, 1, lambda f: None)
    assert out == [] and bucket[1]["fid"] == 2


def test_skips_all_used_fids_not_just_the_edited_one():
    bucket = [
        {"name": "A", "fid": 5, "candidates": [(5, "A", 90, "m")]},
        {"name": "B", "fid": 5, "candidates": [(5, "B", 88, "m"), (7, "B2", 60, "low"), (9, "B3", 50, "low")]},
        {"name": "C", "fid": 7, "candidates": []},
    ]
    out = _rematch_displaced_fids(bucket, 0, 5, lambda f: None)
    assert bucket[1]["fid"] == 9   # 5 taken by editor, 7 taken by row C -> skip to 9
    assert out == [("B", 9, "B3")]
