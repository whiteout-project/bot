"""RTL chart text: matplotlib 3.11+ shapes Arabic itself, so pre-shaping must be version-gated."""
import pytest

from cogs import attendance_report, bear_track

ARABIC = "محمد الفارس"


@pytest.mark.parametrize("version_str,expected", [
    ("3.9.2", False),
    ("3.10.5", False),
    ("3.11.0", True),
    ("3.11.0rc1", True),
    ("3.12.1", True),
    ("4.0.0", True),
    ("3", False),
    ("garbage", False),
])
def test_mpl_shapes_rtl_version_parsing(version_str, expected):
    assert bear_track._mpl_shapes_rtl(version_str) is expected


def test_chart_text_passes_through_on_self_shaping_mpl(monkeypatch):
    monkeypatch.setattr(bear_track, "_MPL_SHAPES_RTL", True)
    assert bear_track._reshape_for_chart(ARABIC) == ARABIC


def test_chart_text_preshaped_on_old_mpl(monkeypatch):
    monkeypatch.setattr(bear_track, "_MPL_SHAPES_RTL", False)
    arabic_reshaper = pytest.importorskip("arabic_reshaper")
    bidi_algorithm = pytest.importorskip("bidi.algorithm")
    expected = bidi_algorithm.get_display(arabic_reshaper.reshape(ARABIC))
    assert expected != ARABIC
    assert bear_track._reshape_for_chart(ARABIC) == expected


@pytest.mark.parametrize("flag", [True, False])
def test_non_rtl_text_never_touched(monkeypatch, flag):
    monkeypatch.setattr(bear_track, "_MPL_SHAPES_RTL", True if flag else False)
    assert bear_track._reshape_for_chart("PlayerOne") == "PlayerOne"
    assert bear_track._reshape_for_chart("") == ""
    assert bear_track._reshape_for_chart(None) == ""


def test_installed_flag_matches_version_parser(monkeypatch):
    monkeypatch.setattr(bear_track, "_MPL_SHAPES_RTL", None)
    from importlib.metadata import version
    assert bear_track._installed_mpl_shapes_rtl() is bear_track._mpl_shapes_rtl(version("matplotlib"))


def test_attendance_table_uses_the_shared_gated_reshaper():
    assert attendance_report._reshape_for_chart is bear_track._reshape_for_chart
