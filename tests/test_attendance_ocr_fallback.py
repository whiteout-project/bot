"""Multi-language OCR fallback (step 4) — orchestration tests.

Stubs the OCR engine so the loop logic is deterministic and no models load:
a non-Latin name the primary (English) engine garbles is recovered by a
fallback-language pass and adopted when it matches the roster.
"""
from __future__ import annotations

import asyncio

import pytest

from harness_attendance import parsers, bear_track


def _run(coro):
    return asyncio.run(coro)


def _box(x_left, y_top, w=60, h=20):
    x_right, y_bottom = x_left + w, y_top + h
    return [[x_left, y_top], [x_right, y_top], [x_right, y_bottom], [x_left, y_bottom]]


def test_fallback_recovers_cyrillic_name(monkeypatch):
    async def fake_boxes(img, lang="en", session=None):
        data = {"en": [("KCloxa 1,476,858", _box(10, 100))],
                "cyrillic": [("ксюха 1,476,858", _box(10, 100))]}
        return data.get(lang, [])
    monkeypatch.setattr(bear_track, "ocr_bytes_with_boxes", fake_boxes)
    monkeypatch.setattr(parsers, "_alliance_ocr_langs", lambda aid, names: ("en", ["cyrillic"]))
    monkeypatch.setattr(parsers, "fuzzy_match_name",
                        lambda name, roster, *, alliance_id=None:
                        ((1, "auto") if name == "ксюха" else (None, "no_match")))
    rows, _text = _run(parsers.ocr_value_rows(b"x", roster=[(1, "ксюха")], alliance_id=7))
    assert rows and rows[0]["name"] == "ксюха"


def test_no_fallback_when_primary_matches(monkeypatch):
    calls = []
    async def fake_boxes(img, lang="en", session=None):
        calls.append(lang)
        text = "Saeed 2,071,102" if lang == "en" else "SHOULD_NOT_RUN 2,071,102"
        return [(text, _box(10, 100))]
    monkeypatch.setattr(bear_track, "ocr_bytes_with_boxes", fake_boxes)
    monkeypatch.setattr(parsers, "_alliance_ocr_langs", lambda aid, names: ("en", ["cyrillic", "arabic"]))
    monkeypatch.setattr(parsers, "fuzzy_match_name",
                        lambda name, roster, *, alliance_id=None:
                        ((2, "auto") if name == "Saeed" else (None, "no_match")))
    rows, _ = _run(parsers.ocr_value_rows(b"x", roster=[(2, "Saeed")], alliance_id=7))
    assert rows and rows[0]["name"] == "Saeed"
    assert calls == ["en"]  # fallback engines never loaded


def test_attendance_box_align_fills_cyrillic(monkeypatch):
    async def fake_boxes(img, lang="en", session=None):
        if lang == "cyrillic":
            return [("ксюха", _box(12, 101))]           # name only, no number
        return [("KCloxa", _box(10, 100)), ("1,476,858", _box(200, 100))]
    monkeypatch.setattr(bear_track, "ocr_bytes_with_boxes", fake_boxes)
    monkeypatch.setattr(parsers, "_alliance_ocr_langs", lambda aid, names: ("en", ["cyrillic"]))
    monkeypatch.setattr(parsers, "fuzzy_match_name",
                        lambda name, roster, *, alliance_id=None:
                        ((1, "auto") if name == "ксюха" else (None, "no_match")))
    rows, _ = _run(parsers.ocr_value_rows(b"x", roster=[(1, "ксюха")], alliance_id=7))
    assert rows and rows[0]["name"] == "ксюха"


def test_no_fallback_langs_configured(monkeypatch):
    async def fake_ocr(img, lang="en", session=None):
        return "Whoever 1,000,000"
    monkeypatch.setattr(bear_track, "ocr_bytes", fake_ocr)
    monkeypatch.setattr(parsers, "_alliance_ocr_langs", lambda aid, names: ("en", []))
    rows, _ = _run(parsers.ocr_value_rows(b"x", roster=[(1, "X")], alliance_id=7))
    assert rows and rows[0]["value"] == 1000000
