"""A bad API run must not empty a roster.

STATE_MISMATCH for most of an alliance means the Gift Redemption API is
misreporting, not that everyone transferred, so apply_removals holds the
deletions back and flags the members wrong-state instead.
"""
import asyncio
import logging
import sqlite3
import types

import cogs.gift_redemption as gr
from harness import fake_player_api


def _harness(monkeypatch, roster_size, settings_rows=((5, 1),)):
    users = sqlite3.connect(":memory:", check_same_thread=False)
    users.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT)")
    for fid in range(1, roster_size + 1):
        users.execute("INSERT INTO users VALUES (?,?,?)", (fid, f"p{fid}", "5"))
    users.commit()

    alliance = sqlite3.connect(":memory:", check_same_thread=False)
    alliance.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, auto_remove_on_transfer INTEGER)")
    for aid, flag in settings_rows:
        alliance.execute("INSERT INTO alliancesettings VALUES (?,?)", (aid, flag))
    alliance.commit()

    settings = sqlite3.connect(":memory:", check_same_thread=False)
    settings.execute("CREATE TABLE alliance_logs (alliance_id INTEGER, channel_id INTEGER)")
    settings.commit()

    real_connect = sqlite3.connect
    routes = {"db/users.sqlite": users, "db/alliance.sqlite": alliance, "db/settings.sqlite": settings}

    def fake_connect(path, *a, **k):
        conn = routes.get(str(path))
        return conn if conn is not None else real_connect(path, *a, **k)

    monkeypatch.setattr(gr.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(gr.gift_state_resolver, "is_multistate", lambda aid: False)

    flagged = []
    monkeypatch.setattr(gr.gift_state_resolver, "flag_state_mismatch_many", flagged.extend)

    cog = types.SimpleNamespace(
        logger=logging.getLogger("test"),
        bot=types.SimpleNamespace(get_channel=lambda cid: None),
    )
    return cog, users, flagged


def _candidates(n, alliance_id=5):
    return [(alliance_id, fid, f"p{fid}") for fid in range(1, n + 1)]


def test_mass_removal_is_held_back(monkeypatch):
    """The reported incident: the API flags a whole alliance, nobody may be deleted."""
    cog, users, flagged = _harness(monkeypatch, roster_size=100)

    asyncio.run(gr.apply_removals(cog, _candidates(100)))

    assert users.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 100, "roster was wiped"
    assert sorted(flagged) == list(range(1, 101)), "held-back members must be flagged instead"


def test_ordinary_transfer_still_removes(monkeypatch):
    """A couple of genuine transfers must still be removed."""
    cog, users, flagged = _harness(monkeypatch, roster_size=100)

    asyncio.run(gr.apply_removals(cog, _candidates(2)))

    remaining = {r[0] for r in users.execute("SELECT fid FROM users").fetchall()}
    assert remaining == set(range(3, 101))
    assert flagged == []


def test_small_alliance_can_still_remove_one(monkeypatch):
    """One member of four is over the share but under the floor, so it goes through."""
    cog, users, flagged = _harness(monkeypatch, roster_size=4)

    asyncio.run(gr.apply_removals(cog, _candidates(1)))

    assert users.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 3
    assert flagged == []


def test_guard_is_per_alliance(monkeypatch):
    """One alliance being held back must not stop a healthy one."""
    cog, users, flagged = _harness(
        monkeypatch, roster_size=100, settings_rows=((5, 1), (6, 1)))
    users.execute("INSERT INTO users VALUES (?,?,?)", (201, "x", "6"))
    users.execute("INSERT INTO users VALUES (?,?,?)", (202, "y", "6"))
    users.commit()

    removed = asyncio.run(gr.apply_removals(cog, _candidates(50) + [(6, 201, "x")]))

    assert removed == [(6, 201, "x")], "callers learn who was actually removed"

    assert users.execute("SELECT COUNT(*) FROM users WHERE alliance='5'").fetchone()[0] == 100
    assert users.execute("SELECT COUNT(*) FROM users WHERE fid=201").fetchone()[0] == 0
    assert sorted(flagged) == list(range(1, 51))


# --- A rejected state is confirmed through Player Sync's API first ---------------------------

import pytest


@pytest.fixture
def mismatch(tmp_path, monkeypatch):
    """Member 1 in alliance 5 (state 100), stored state 100, which the game just rejected."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, kid INTEGER, alliance TEXT, "
                  "state_mismatch_at TEXT, kingdom_scan_next INTEGER, kingdom_scan_done_at TEXT)")
        c.execute("INSERT INTO users VALUES (1, 'Ann', 100, '5', NULL, NULL, NULL)")
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT, kid INTEGER, "
                  "multistate INTEGER, state_locked INTEGER)")
        c.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, auto_remove_on_transfer INTEGER)")
        c.execute("INSERT INTO alliance_list VALUES (5, 'A', 100, 0, 0)")
        c.execute("INSERT INTO alliancesettings VALUES (5, 0)")

    state = fake_player_api(monkeypatch, lambda fid: {"kid": state.kid} if state.kid is not None else None)
    state.kid, state.queued = None, []
    monkeypatch.setattr(gr, "enqueue_member_redemption", lambda cog, fid: state.queued.append(fid) or 1)
    state.cog = types.SimpleNamespace(logger=logging.getLogger("test"),
                                      bot=types.SimpleNamespace(get_channel=lambda cid: None))
    return state


def _member():
    with sqlite3.connect("db/users.sqlite") as c:
        return c.execute("SELECT kid, state_mismatch_at IS NOT NULL FROM users WHERE fid = 1").fetchone()


def _auto_remove(on):
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("UPDATE alliancesettings SET auto_remove_on_transfer = ?", (1 if on else 0,))


def _mismatch(state, collector=None, fid=1, kid=100):
    """A redemption run's rejection, then the API pass that runs after the run."""
    collector = [] if collector is None else collector
    checks = []
    asyncio.run(gr.handle_state_mismatch(state.cog, fid, kid, collector, checks))
    asyncio.run(gr.confirm_mismatches(state.cog, checks, collector))
    return collector


def test_without_the_api_the_member_is_flagged(mismatch):
    mismatch.available = False
    _mismatch(mismatch)
    assert _member() == (100, 1) and mismatch.asked == []


def test_api_with_the_same_state_keeps_the_flag(mismatch):
    mismatch.kid = 100
    _mismatch(mismatch)
    assert _member() == (100, 1) and mismatch.asked == [1] and mismatch.queued == []


def test_api_with_the_same_state_holds_the_removal(mismatch):
    mismatch.kid = 100                                   # still the alliance's state: no transfer confirmed
    _auto_remove(True)
    collector = _mismatch(mismatch)
    assert collector == [] and _member() == (100, 1)


def test_api_repeating_a_rejected_state_outside_the_alliance_confirms_the_removal(mismatch):
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("UPDATE users SET kid = 250")
    mismatch.kid = 250
    _auto_remove(True)
    collector = _mismatch(mismatch, kid=250)
    assert collector == [("5", 1, "Ann")]


def test_api_state_in_the_alliance_state_is_saved_without_removal(mismatch):
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("UPDATE users SET kid = 90")
    _auto_remove(True)
    mismatch.kid = 100                                   # stored 90 was stale, the alliance is 100
    collector = _mismatch(mismatch, kid=90)
    assert _member() == (100, 0) and collector == [] and mismatch.queued == [1]


def test_api_transfer_goes_to_auto_removal_without_saving_the_new_state(mismatch):
    _auto_remove(True)
    mismatch.kid = 250
    collector = []
    _mismatch(mismatch, collector)
    assert _member() == (100, 0) and collector == [("5", 1, "Ann")] and mismatch.queued == []


def test_api_transfer_without_auto_remove_keeps_the_member_in_the_new_state(mismatch):
    mismatch.kid = 250
    _mismatch(mismatch)
    assert _member() == (250, 0) and mismatch.queued == [1]


def test_flagged_member_rejected_again_on_a_resumed_run_is_checked_before_removal(mismatch):
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("UPDATE users SET state_mismatch_at = '2026-10-01T00:00:00'")
    _auto_remove(True)
    mismatch.kid = None                                  # the API has nothing to confirm
    collector = _mismatch(mismatch)
    assert mismatch.asked == [1] and collector == [] and _member() == (100, 1)


def test_a_member_is_checked_once_per_run(mismatch):
    collector, checks = [], []
    for _ in range(2):
        asyncio.run(gr.handle_state_mismatch(mismatch.cog, 1, 100, collector, checks))
    assert checks == [(1, 100)]


def test_mismatch_note_points_to_the_api_when_it_is_set_up():
    assert "API" in gr.state_mismatch_note(True) and "Members to Fix" in gr.state_mismatch_note(True)
    assert "API" not in gr.state_mismatch_note(False)


def test_held_back_api_transfers_keep_their_old_state_flagged(mismatch):
    with sqlite3.connect("db/users.sqlite") as c:
        c.executemany("INSERT INTO users VALUES (?, ?, 100, '5', NULL, NULL, NULL)",
                      [(fid, f"p{fid}") for fid in range(2, 11)])
    with sqlite3.connect("db/settings.sqlite") as c:
        c.execute("CREATE TABLE alliance_logs (alliance_id INTEGER, channel_id INTEGER)")
    _auto_remove(True)
    mismatch.kid = 250
    collector = []
    for fid in (1, 2, 3):
        _mismatch(mismatch, collector, fid=fid)
    removed = asyncio.run(gr.apply_removals(mismatch.cog, collector))
    with sqlite3.connect("db/users.sqlite") as c:
        rows = c.execute("SELECT fid, kid, state_mismatch_at IS NOT NULL FROM users WHERE fid <= 3").fetchall()
        roster = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    assert removed == [] and roster == 10
    assert rows == [(1, 100, 1), (2, 100, 1), (3, 100, 1)]   # old state, flagged: the next scan places them


def test_the_redeem_loop_never_waits_on_the_api(mismatch):
    mismatch.kid = 250
    collector, checks = [], []
    asyncio.run(gr.handle_state_mismatch(mismatch.cog, 1, 100, collector, checks))
    assert mismatch.asked == [] and checks == [(1, 100)] and _member() == (100, 1)


def test_a_rejection_without_an_api_pass_only_flags(mismatch):
    mismatch.kid = 250
    asyncio.run(gr.handle_state_mismatch(mismatch.cog, 1, 100))
    assert mismatch.asked == [] and _member() == (100, 1)


def test_catchup_stops_at_a_rejected_state_and_asks_the_api_once(mismatch, monkeypatch):
    mismatch.kid = 250
    claimed = []

    async def claim(cog, fid, giftcode, *, removal_collector=None, api_checks=None, **kw):
        claimed.append(giftcode)
        await gr.handle_state_mismatch(cog, fid, 100, removal_collector, api_checks)
        return "STATE_MISMATCH"

    monkeypatch.setattr(gr, "claim_giftcode_rewards_wos", claim)
    mismatch.cog.bot.get_cog = lambda name: None
    asyncio.run(gr.handle_member_redeem_process(mismatch.cog, {'id': 1, 'details': {'fid': 1, 'codes': ['A', 'B']}}))
    assert claimed == ['A'] and mismatch.asked == [1] and _member() == (250, 0)


def test_api_pass_stops_after_the_first_failed_lookup(mismatch, monkeypatch):
    import cogs.alliance_api_sync as sync
    asked = []

    async def failing(bot, fid):
        asked.append(fid)
        return sync.Lookup(reason=sync.FAILED)

    monkeypatch.setattr(sync, "lookup_via_bot", failing)
    flagged = []
    monkeypatch.setattr(gr.gift_state_resolver, "flag_state_mismatch_many", lambda fids: flagged.extend(fids))
    collector = [("5", 1, "Ann"), ("5", 2, "p2")]
    asyncio.run(gr.confirm_mismatches(mismatch.cog, [(1, 100), (2, 100)], collector))
    assert asked == [1] and collector == [] and flagged == [1, 2]


def test_spent_budget_holds_every_candidate(mismatch, monkeypatch):
    import cogs.alliance_api_sync as sync

    async def spent(bot, fid):
        mismatch.asked.append(fid)
        return sync.Lookup(reason=sync.BUDGET_SPENT)

    monkeypatch.setattr(sync, "lookup_via_bot", spent)
    _auto_remove(True)
    collector = _mismatch(mismatch)
    assert collector == [] and _member() == (100, 1)


def test_without_the_api_removal_is_decided_at_once_with_no_hold(mismatch):
    mismatch.available = False
    _auto_remove(True)
    collector, checks = [], []
    asyncio.run(gr.handle_state_mismatch(mismatch.cog, 1, 100, collector, checks))
    asyncio.run(gr.hold_unconfirmed(mismatch.cog, checks, collector))
    asyncio.run(gr.confirm_mismatches(mismatch.cog, checks, collector))
    assert checks == [] and collector == [("5", 1, "Ann")] and mismatch.asked == []
