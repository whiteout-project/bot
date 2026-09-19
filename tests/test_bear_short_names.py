"""Short letters-and-digits names like `6m3` survive Bear OCR parsing; status-bar junk still doesn't."""
import pytest

from cogs import bear_track as bt

RANKING = (
    "Mail [Hunting Trap 1] Damage Ranking Thanos Damage Points:42,691,117,368 "
    "6m3 2 Damage Points:37,338,099,863 XTz 3 Damage Points:37,273,869,343 "
    "VAL 4 DamagePoints:32,973,560,662 XT2 5 Damage Points:31,598,271,109 "
    "V4L 6 Damage Points:28,582,817,832 AlejoRoll 7 DamagePoints:27,209,593,990 Delete"
)


@pytest.mark.parametrize("name", ["6m3", "XT2", "V4L", "XTz", "VAL", "Ay Jl", "AlejoRoll", "محمد"])
def test_short_mixed_names_are_readable(name):
    assert bt.is_readable_name(name)


@pytest.mark.parametrize("junk", ["", "23:24 @ G", "4G", "87%", ":b", "12 5G", "21:30:05"])
def test_status_bar_leaks_are_not_readable(junk):
    assert not bt.is_readable_name(junk)


def test_ranking_rows_keep_short_mixed_names():
    names = {r["damage"]: r["name"] for r in bt.parse_player_rows(bt.repair_ocr_digits(RANKING))}

    assert names[37338099863] == "6m3"
    assert names[31598271109] == "XT2"
    assert names[28582817832] == "V4L"
    assert names[37273869343] == "XTz"
    assert names[42691117368] == "Thanos"


def test_short_mixed_name_matches_its_roster_member_exactly():
    roster = [(1, "XTz"), (2, "VAL"), (3, "6m3"), (4, "Valkyrie")]

    candidates = bt.match_roster("6m3", roster)

    assert candidates and candidates[0][0] == 3
    assert bt.classify_match(candidates) == "auto"


def test_short_mixed_name_never_fuzzy_matches_a_different_member():
    roster = [(3, "6m3"), (5, "6m4x")]
    assert [c[0] for c in bt.match_roster("6m4", roster)] == []
