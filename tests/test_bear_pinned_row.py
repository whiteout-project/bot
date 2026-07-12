"""Pinned self-row + resolver-guard tests for the in-game ranking screenshots.

The Damage Ranking / Trap-N-Damage panels repeat the *viewer's own row* pinned
at the bottom, out of rank order (e.g. rank 4 after rank 22). A copy-to-clipboard
icon often clips its last digits. These tests lock in that the pinned row is
dropped on ranking pages (but never on the reward mail), that a short roster
nickname can't absorb a long OCR'd name, and that a digit-only username can be
entered as a name.
"""
from __future__ import annotations

from harness import bt


# Ranking page 2 (ranks 18-22 visible), self-row PinkyCosmoDog pinned at the
# bottom as rank 4; the copy icon clipped the trailing "697" of 27,544,983,697.
RANKING_PAGE2_CLIPPED = (
    "Trap 1 Damage Rewards Damage Ranking Personal Damage Rewards "
    "18 [DOG]Olena Damage Points:10,717,949,096 "
    "19 [DOG]Melly Damage Points:10,441,018,514 "
    "20 [DOG]_DAINKIN_ Damage Points:6,572,639,257 "
    "21 [DOG]Cypis Damage Points:1,113,823,405 "
    "22 [DOG]soaring_eagle Damage Points:4,839,799 "
    "4 [DOG]PinkyCosmoDog Damage Points:27,544,983,69"
)

# Ranking page 1 (ranks 1-5), self-row rank 4 duplicated at the bottom, full value.
RANKING_PAGE1 = (
    "Trap 1 Damage Rewards Damage Ranking "
    "1 [DOG]Alpha Damage Points:99,111,222,333 "
    "2 [DOG]Beta Damage Points:88,111,222,333 "
    "3 [DOG]Gamma Damage Points:77,111,222,333 "
    "4 [DOG]PinkyCosmoDog Damage Points:27,544,983,697 "
    "5 [DOG]Delta Damage Points:20,111,222,333 "
    "4 [DOG]PinkyCosmoDog Damage Points:27,544,983,697"
)

# Reward mail: bare names, no per-row tags, ascending ranks, real last row.
REWARD_MAIL = (
    "Mail [Hunting Trap 1] Total Alliance Damage 13,986,964,733 "
    "Kay 1,118,138,548 Little Auntie 1,053,513,206 axm 800,984,499 "
    "Misos 713,055,479 Auther 322,186,620"
)


def _parse(text):
    return bt.parse_player_rows(bt.repair_ocr_digits(text))


def test_pinned_row_dropped_on_page_with_off_window_rank():
    rows = _parse(RANKING_PAGE2_CLIPPED)
    ranks = [r["rank"] for r in rows]
    damages = [r["damage"] for r in rows]
    # The pinned rank-4 self-row must not survive as a phantom player.
    assert 4 not in ranks, f"pinned self-row leaked: {rows}"
    # And its clipped ~27M value must not appear as a separate row.
    assert 27544983 not in damages, f"clipped self-row damage leaked: {rows}"
    # Only the five real rows (18-22) remain; the last is rank 22.
    assert len(rows) == 5 and ranks[-1] == 22, f"unexpected rows: {rows}"


def test_pinned_row_dropped_keeps_real_row_name_on_page1():
    rows = _parse(RANKING_PAGE1)
    # PinkyCosmoDog appears exactly once, with its name intact (not wiped by the
    # trailing-token stripper that the duplicated pinned row would trigger).
    pinky = [r for r in rows if r["damage"] == 27544983697]
    assert len(pinky) == 1, f"expected one PinkyCosmoDog row, got {rows}"
    assert "PinkyCosmoDog" in (pinky[0]["name"] or ""), f"name wiped: {pinky}"


def test_pinned_drop_never_touches_reward_mail_last_row():
    rows = _parse(REWARD_MAIL)
    names = " ".join((r["name"] or "") for r in rows)
    # The genuine last mail row (Auther) must be preserved.
    assert "Auther" in names, f"mail last row dropped: {rows}"


def test_short_roster_name_does_not_absorb_long_ocr_name():
    roster = [(1, "PinkyCosmoDog"), (2, "⊙~MO")]
    # The real player owns their name; the phantom must not match the 2-char
    # nickname just because "mo" is a substring of "pinkycosmodog".
    cands = bt.match_roster("PinkyCosmoDog", [(2, "⊙~MO")])
    assert cands == [], f"short roster name wrongly matched: {cands}"


def test_short_roster_name_still_matches_exact():
    # A genuine short-name read still resolves by exact (folded) equality.
    cands = bt.match_roster("MO", [(2, "MO"), (3, "Cypis")])
    assert cands and cands[0][0] == 2, f"exact short-name match lost: {cands}"


def test_quote_forces_name_over_id():
    clean, forced = bt._strip_name_quotes('"517"')
    assert (clean, forced) == ("517", True)
    clean, forced = bt._strip_name_quotes("517")
    assert (clean, forced) == ("517", False)


def test_resolve_player_force_name_treats_digits_as_name():
    roster = [(999, "517"), (1000, "Someone")]
    fid, nick, _ = bt._resolve_player("517", roster, force_name=True)
    assert (fid, nick) == (999, "517"), "digit-only username not resolved as name"
