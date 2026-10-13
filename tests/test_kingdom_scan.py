"""Kingdom scan: settings, targets, resume columns, scan engine handler and auto-scan."""
import asyncio
import logging
import sqlite3
import types
from datetime import datetime, timedelta

import pytest

import cogs.gift_state_resolver as gsr
from harness import fake_player_api, kid_and_flag


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("CREATE TABLE users (fid INTEGER PRIMARY KEY, nickname TEXT, kid INTEGER, "
                  "alliance TEXT, state_mismatch_at TEXT, kingdom_scan_next INTEGER, "
                  "kingdom_scan_done_at TEXT)")
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT, kid INTEGER, "
                  "multistate INTEGER, state_locked INTEGER)")
    with sqlite3.connect("db/settings.sqlite") as c:
        c.execute("CREATE TABLE bot_global_settings (setting_key TEXT PRIMARY KEY, setting_value TEXT)")
    return tmp_path


def _user(fid, kid=None, alliance="5", mismatch=None, done=None, nxt=None, nick=None):
    with sqlite3.connect("db/users.sqlite") as c:
        c.execute("INSERT INTO users VALUES (?,?,?,?,?,?,?)",
                  (fid, nick or f"p{fid}", kid, alliance, mismatch, nxt, done))


def _scan_columns(fid):
    with sqlite3.connect("db/users.sqlite") as c:
        return c.execute("SELECT kingdom_scan_next, kingdom_scan_done_at FROM users WHERE fid = ?",
                         (fid,)).fetchone()


# --- Task 1: settings, targets, resume columns ---------------------------------------


def test_settings_default_off_and_range_from_highest_on_file(dbs):
    _user(1, kid=812)
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("INSERT INTO alliance_list VALUES (5,'A',1500,0,0)")
    assert gsr.get_scan_settings() == {"enabled": False, "min": 1, "max": 1500, "custom": False}


def test_set_range_and_toggle_persist(dbs):
    gsr.set_scan_range(100, 2000)
    gsr.set_scan_enabled(True)
    assert gsr.get_scan_settings() == {"enabled": True, "min": 100, "max": 2000, "custom": True}
    gsr.set_scan_enabled(False)
    assert gsr.get_scan_settings()["enabled"] is False


@pytest.mark.parametrize("lo,hi", [(0, 10), (10, 5), (1, 100000)])
def test_set_range_rejects_bad_bounds(dbs, lo, hi):
    with pytest.raises(ValueError):
        gsr.set_scan_range(lo, hi)


def test_members_to_fix_lists_wrong_then_missing(dbs):
    _user(1, kid=None)
    _user(2, kid=259, mismatch="2026-09-29T10:00:00")
    _user(3, kid=300)
    assert [(f, r) for f, _n, _k, r in gsr.members_to_fix()] == [(2, "wrong"), (1, "missing")]


def test_auto_targets_skip_recently_swept(dbs):
    now = datetime(2026, 10, 3, 12, 0, 0)
    _user(1)
    _user(2, done=(now - timedelta(days=5)).isoformat())
    _user(3, done=(now - timedelta(days=31)).isoformat())
    _user(4, kid=300)                                             # fine, never a target
    assert gsr.scan_targets(auto=True, now=now) == [1, 3]
    assert gsr.scan_targets(auto=False, now=now) == [1, 2, 3]


def test_set_user_kid_clears_scan_columns_and_needs_scan(dbs):
    _user(1, nxt=900, done="2026-09-01T00:00:00")
    assert gsr.needs_scan(1) is True
    gsr.set_user_kid(1, 777)
    with sqlite3.connect("db/users.sqlite") as c:
        assert c.execute("SELECT kid FROM users").fetchone()[0] == 777
    assert _scan_columns(1) == (None, None)
    assert gsr.needs_scan(1) is False


def test_scan_position_round_trip_and_done(dbs):
    _user(1)
    gsr.save_scan_position(1, 1234)
    assert gsr.get_scan_position(1) == 1234
    gsr.mark_scan_done(1)
    nxt, done = _scan_columns(1)
    assert nxt is None and done is not None


# --- Task 2: OCR busy signal ------------------------------------------------------------


def test_any_model_in_use_tracks_refcount(monkeypatch):
    from cogs import onnx_lifecycle as ol
    idle = types.SimpleNamespace(_refcount=0)
    busy = types.SimpleNamespace(_refcount=1)
    monkeypatch.setattr(ol, "_REGISTRY", {"a": idle})
    assert ol.any_model_in_use() is False
    monkeypatch.setattr(ol, "_REGISTRY", {"a": idle, "b": busy})
    assert ol.any_model_in_use() is True


# --- Task 4: scan engine (one member at a time, immediate save, lock retry) ---------------

import cogs.gift_redemption as gr

SETTINGS = {"enabled": True, "min": 1, "max": 99, "custom": True}


def _log_cog():
    return types.SimpleNamespace(logger=logging.getLogger("test"))


async def _no_sleep(_s):
    return None


def _scan(fid, mode="now"):
    return asyncio.run(gr.scan_one_member(_log_cog(), fid, mode=mode, settings=SETTINGS,
                                          wait_until_idle=None, prefer=[], on_progress=None))


def test_save_found_kingdom_retries_lock_then_gives_up(monkeypatch):
    calls = {"n": 0}

    def locked(fid, kid):
        calls["n"] += 1
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(gr.gift_state_resolver, "set_user_kid", locked)
    monkeypatch.setattr(gr.asyncio, "sleep", _no_sleep)
    assert asyncio.run(gr.save_found_kingdom(_log_cog(), 1, 812)) is False
    assert calls["n"] == 3


def test_save_found_kingdom_recovers_after_one_lock(monkeypatch):
    calls = {"n": 0}

    def flaky(fid, kid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(gr.gift_state_resolver, "set_user_kid", flaky)
    monkeypatch.setattr(gr.asyncio, "sleep", _no_sleep)
    assert asyncio.run(gr.save_found_kingdom(_log_cog(), 1, 812)) is True


def test_scan_one_member_resumes_from_saved_position_and_marks_done(monkeypatch, dbs):
    _user(1, nxt=50)
    seen = {}

    async def fake_resolve(cog, fid, **kw):
        seen.update(kw)
        return None

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", fake_resolve)
    assert _scan(1, mode="auto") is None
    assert seen["start_at"] == 50 and seen["scan_range"] == (1, 99) and seen["pace"] == 10.0
    assert _scan_columns(1)[1] is not None


def test_scan_one_member_now_mode_uses_fast_pace(monkeypatch, dbs):
    _user(1)
    seen = {}

    async def fake_resolve(cog, fid, **kw):
        seen.update(kw)
        return None

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", fake_resolve)
    _scan(1, mode="now")
    assert seen["pace"] == 2.2 and seen["start_at"] is None


def test_scan_one_member_skips_member_fixed_meanwhile(monkeypatch, dbs):
    _user(1, kid=812)

    async def must_not_run(*a, **k):
        raise AssertionError("scanned a member who no longer needs it")

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", must_not_run)
    assert _scan(1) is None


def test_scan_one_member_saves_found_kingdom_immediately(monkeypatch, dbs):
    _user(1, nxt=40)
    queued = []

    async def found(cog, fid, **kw):
        return 812

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", found)
    monkeypatch.setattr(gr, "enqueue_member_redemption", lambda cog, fid: queued.append(fid) or 1)
    assert _scan(1) == 812
    with sqlite3.connect("db/users.sqlite") as c:
        assert c.execute("SELECT kid FROM users").fetchone()[0] == 812
    assert _scan_columns(1) == (None, None)
    assert queued == [1]


def test_scan_checkpoint_survives_locked_db(monkeypatch, dbs):
    _user(1)

    async def resolve_with_checkpoint(cog, fid, **kw):
        await kw["on_checkpoint"](60)                  # must not raise even if the save fails
        return None

    def locked(*a):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", resolve_with_checkpoint)
    monkeypatch.setattr(gr.gift_state_resolver, "save_scan_position", locked)
    monkeypatch.setattr(gr.gift_state_resolver, "mark_scan_done", locked)
    assert _scan(1) is None


# --- Task 5: queueing and the auto-scan tick ----------------------------------------------


def _gift_cog(queue_rows=(), busy=False):
    from cogs.gift_operations import GiftOperations
    pq = types.SimpleNamespace(enqueued=[])
    pq.get_queued_processes_by_action = lambda action, statuses=('queued',): list(queue_rows)
    pq.queue_counts = lambda: {'queued': 0, 'active': 1 if busy else 0, 'completed': 0, 'failed': 0}
    pq.enqueue = lambda action, prio, alliance_id=None, details=None: pq.enqueued.append((action, prio, details)) or 1
    cog = GiftOperations.__new__(GiftOperations)
    cog.bot = types.SimpleNamespace(get_cog=lambda name: pq if name == "ProcessQueue" else None)
    cog.logger = logging.getLogger("test")
    return cog, pq


@pytest.fixture
def ocr_idle(monkeypatch):
    from cogs import onnx_lifecycle
    monkeypatch.setattr(onnx_lifecycle, "any_model_in_use", lambda: False)
    return onnx_lifecycle


def test_auto_tick_does_nothing_when_toggle_off(dbs, ocr_idle):
    _user(1)
    cog, pq = _gift_cog()
    assert cog.auto_kingdom_scan_tick() is False and pq.enqueued == []


def test_auto_tick_queues_when_idle_and_on(dbs, ocr_idle):
    _user(1)
    gsr.set_scan_enabled(True)
    cog, pq = _gift_cog()
    assert cog.auto_kingdom_scan_tick() is True
    action, prio, details = pq.enqueued[0]
    assert action == "state_resolve" and details["mode"] == "auto" and details["remaining"] == [1]


def test_auto_tick_waits_while_queue_busy(dbs, ocr_idle):
    _user(1)
    gsr.set_scan_enabled(True)
    cog, pq = _gift_cog(busy=True)
    assert cog.auto_kingdom_scan_tick() is False and pq.enqueued == []


def test_auto_tick_waits_while_ocr_runs(dbs, monkeypatch):
    from cogs import onnx_lifecycle
    monkeypatch.setattr(onnx_lifecycle, "any_model_in_use", lambda: True)
    _user(1)
    gsr.set_scan_enabled(True)
    cog, pq = _gift_cog()
    assert cog.auto_kingdom_scan_tick() is False and pq.enqueued == []


def test_auto_tick_quiet_when_everyone_recently_swept(dbs, ocr_idle, caplog):
    _user(1, done=datetime.now().isoformat(timespec='seconds'))
    gsr.set_scan_enabled(True)
    cog, pq = _gift_cog()
    with caplog.at_level(logging.INFO):
        assert cog.auto_kingdom_scan_tick() is False
    assert pq.enqueued == [] and caplog.records == []


def test_queue_auto_refuses_while_a_scan_is_queued(dbs):
    _user(1)
    cog, pq = _gift_cog(queue_rows=[{'details': {'mode': 'now', 'remaining': [1], 'total': 1},
                                     'status': 'active'}])
    assert cog.queue_kingdom_scan('auto') is None
    assert cog.kingdom_scan_status()['mode'] == 'now'


def test_queue_now_includes_recently_swept(dbs):
    _user(1, done=datetime.now().isoformat(timespec='seconds'))
    cog, pq = _gift_cog()
    assert cog.queue_kingdom_scan('now') == 1
    assert pq.enqueued[0][2]['mode'] == 'now'


# --- final review fixes --------------------------------------------------------------


def test_unanswered_kingdom_stops_member_without_marking_done(monkeypatch, dbs):
    # An API outage must not read as "not in this kingdom" and park the member for 30 days.
    import cogs.gift_state_resolver as res
    _user(1)
    probed = []

    async def down(cog, session, fid, kid, code):
        probed.append(kid)
        return "error"

    monkeypatch.setattr(res, "_probe", down)
    monkeypatch.setattr(res, "_candidate_kids", lambda fid: [])
    monkeypatch.setattr(res, "_reference_center", lambda fid: None)
    monkeypatch.setattr(res, "_make_session", lambda cog: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(res.asyncio, "sleep", _no_sleep)
    with pytest.raises(res.StateResolveUnavailable):
        _scan(1)
    assert set(probed) == {1}                       # never moved past kingdom 1
    assert _scan_columns(1) == (1, None)            # resumes at 1, not marked done


def test_scan_now_supersedes_a_running_auto_scan(dbs):
    from cogs import process_queue
    _user(1)
    cog, pq = _gift_cog(queue_rows=[{'details': {'mode': 'auto', 'remaining': [1], 'total': 1},
                                     'status': 'active', 'priority': process_queue.STATE_RESOLVE}])
    assert cog.queue_kingdom_scan('now') == 1
    action, prio, details = pq.enqueued[0]
    assert details['mode'] == 'now' and prio < process_queue.STATE_RESOLVE   # preempts the auto job


def test_scan_now_refused_while_another_now_scan_runs(dbs):
    _user(1)
    cog, pq = _gift_cog(queue_rows=[{'details': {'mode': 'now', 'remaining': [1], 'total': 1},
                                     'status': 'active'}])
    assert cog.queue_kingdom_scan('now') is None and pq.enqueued == []


def test_auto_job_ends_when_auto_scan_is_switched_off(monkeypatch, dbs):
    _user(1)
    gsr.set_scan_enabled(False)

    _user(2)
    scanned = []

    async def gated_scan(cog, fid, *, wait_until_idle, **kw):
        await wait_until_idle()
        scanned.append(fid)
        return None

    pq = types.SimpleNamespace(should_preempt=lambda: False, update_details=lambda pid, d: None)
    cog = types.SimpleNamespace(logger=logging.getLogger("test"),
                                bot=types.SimpleNamespace(get_cog=lambda name: pq))
    monkeypatch.setattr(gr, "scan_one_member", gated_scan)
    process = {'id': 7, 'details': {'mode': 'auto', 'remaining': [1, 2], 'total': 2}}
    asyncio.run(gr.handle_state_resolve_process(cog, process))   # returns, no PreemptedException
    assert scanned == []                            # nobody probed once auto-scan is off


def test_changing_the_range_restarts_scans(dbs):
    _user(1, nxt=1800, done="2026-09-30T00:00:00")
    gsr.set_scan_range(1000, 2000)
    assert _scan_columns(1) == (None, None)
    _user(2, nxt=1500)
    gsr.set_scan_range(1000, 2000)                  # same range: progress kept
    assert _scan_columns(2) == (1500, None)


def test_bot_health_ignores_a_running_kingdom_scan():
    from cogs import bot_health
    pq = types.SimpleNamespace(
        get_queue_info=lambda: {'queue_size': 1, 'is_processing': True},
        _current_process={'action': 'state_resolve'},
        get_queued_processes_by_action=lambda action, statuses=('queued',): [{}] if action == 'state_resolve' else [],
    )
    bot = types.SimpleNamespace(get_cog=lambda name: pq if name == "ProcessQueue" else None)
    assert bot_health._active_work_summary(bot) is None


# --- review minors -------------------------------------------------------------------


def test_failed_save_is_not_counted_as_found(monkeypatch, dbs):
    _user(1)

    async def found(cog, fid, **kw):
        return 812

    async def save_fails(cog, fid, kid):
        return False

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", found)
    monkeypatch.setattr(gr, "save_found_kingdom", save_fails)
    assert _scan(1) is None
    assert _scan_columns(1)[1] is None                 # not marked done either


def test_save_found_kingdom_does_not_sleep_after_last_attempt(monkeypatch):
    slept = []

    def locked(fid, kid):
        raise sqlite3.OperationalError("database is locked")

    async def record_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(gr.gift_state_resolver, "set_user_kid", locked)
    monkeypatch.setattr(gr.asyncio, "sleep", record_sleep)
    asyncio.run(gr.save_found_kingdom(_log_cog(), 1, 812))
    assert len(slept) == gr.SAVE_ATTEMPTS - 1


def test_hand_fix_mid_scan_stops_the_member(monkeypatch, dbs):
    _user(1)

    async def resolve_then_fixed(cog, fid, **kw):
        gsr.set_user_kid(1, 500)                       # admin sets it while the sweep runs
        await kw["on_checkpoint"](60)
        return 812

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", resolve_then_fixed)
    assert _scan(1) is None
    with sqlite3.connect("db/users.sqlite") as c:
        assert c.execute("SELECT kid FROM users").fetchone()[0] == 500   # hand-set value kept
    assert _scan_columns(1) == (None, None)


def test_hand_fix_just_before_a_match_is_not_overwritten(monkeypatch, dbs):
    _user(1)

    async def match_after_fix(cog, fid, **kw):
        gsr.set_user_kid(1, 500)
        return 812

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", match_after_fix)
    assert _scan(1) is None
    with sqlite3.connect("db/users.sqlite") as c:
        assert c.execute("SELECT kid FROM users").fetchone()[0] == 500


def test_resume_point_outside_the_range_starts_over(monkeypatch, dbs):
    _user(1, nxt=150)                                  # range is 1-99
    seen = {}

    async def fake_resolve(cog, fid, **kw):
        seen.update(kw)
        return None

    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", fake_resolve)
    _scan(1)
    assert seen["start_at"] is None


def test_resume_skips_likely_kingdoms_already_tried(monkeypatch):
    import cogs.gift_state_resolver as res
    probed = []

    async def probe(cog, session, fid, kid, code):
        probed.append(kid)
        return "nomatch"

    monkeypatch.setattr(res, "_probe", probe)
    monkeypatch.setattr(res, "_candidate_kids", lambda fid: [6])
    monkeypatch.setattr(res, "_reference_center", lambda fid: None)
    monkeypatch.setattr(res, "_make_session", lambda cog: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(res.asyncio, "sleep", _no_sleep)
    asyncio.run(res.resolve_state(_log_cog(), 1, scan_range=(5, 8), start_at=7))
    assert probed == [6, 7, 8]                     # 6 is below the resume point: tried as likely first


def test_clearing_the_range_returns_to_the_default(dbs):
    _user(1, kid=812, nxt=None)
    _user(2, nxt=300, done=None)
    gsr.set_scan_range(100, 2000)
    gsr.save_scan_position(2, 300)
    gsr.clear_scan_range()
    s = gsr.get_scan_settings()
    assert s["custom"] is False and (s["min"], s["max"]) == (1, 812)
    assert _scan_columns(2) == (None, None)


def test_handler_marks_started_and_waiting(monkeypatch, dbs):
    _user(1)
    gsr.set_scan_enabled(True)
    saved = []
    waits = {"n": 0}

    def should_wait(cog):
        waits["n"] += 1
        return waits["n"] == 1                         # busy once, then idle

    async def gated_scan(cog, fid, *, wait_until_idle, **kw):
        await wait_until_idle()
        return None

    pq = types.SimpleNamespace(should_preempt=lambda: False, stop_requested=lambda: False,
                               update_details=lambda pid, d: saved.append(dict(d)))
    cog = types.SimpleNamespace(logger=logging.getLogger("test"),
                                bot=types.SimpleNamespace(get_cog=lambda name: pq))
    monkeypatch.setattr(gr, "scan_one_member", gated_scan)
    monkeypatch.setattr(gr, "_auto_should_wait", should_wait)
    monkeypatch.setattr(gr.asyncio, "sleep", _no_sleep)
    asyncio.run(gr.handle_state_resolve_process(
        cog, {'id': 7, 'details': {'mode': 'auto', 'remaining': [1], 'total': 1}}))
    assert saved[0].get("started") is True
    assert any(d.get("waiting") for d in saved)
    assert not saved[-1].get("waiting")



def test_resume_still_sweeps_kingdoms_that_became_likely(monkeypatch):
    import cogs.gift_state_resolver as res
    probed = []

    async def probe(cog, session, fid, kid, code):
        probed.append(kid)
        return "nomatch"

    monkeypatch.setattr(res, "_probe", probe)
    monkeypatch.setattr(res, "_candidate_kids", lambda fid: [7])    # a mate found there since
    monkeypatch.setattr(res, "_reference_center", lambda fid: None)
    monkeypatch.setattr(res, "_make_session", lambda cog: types.SimpleNamespace(close=lambda: None))
    monkeypatch.setattr(res.asyncio, "sleep", _no_sleep)
    asyncio.run(res.resolve_state(_log_cog(), 1, scan_range=(5, 8), start_at=6))
    assert probed == [6, 7, 8]


def test_admin_stop_ends_the_scan_quietly(monkeypatch, dbs):
    _user(1)
    _user(2)
    scanned = []

    async def gated_scan(cog, fid, *, wait_until_idle, **kw):
        await wait_until_idle()
        scanned.append(fid)
        return None

    pq = types.SimpleNamespace(should_preempt=lambda: True, stop_requested=lambda: True,
                               update_details=lambda pid, d: None)
    cog = types.SimpleNamespace(logger=logging.getLogger("test"),
                                bot=types.SimpleNamespace(get_cog=lambda name: pq))
    monkeypatch.setattr(gr, "scan_one_member", gated_scan)
    asyncio.run(gr.handle_state_resolve_process(
        cog, {'id': 7, 'details': {'mode': 'now', 'remaining': [1, 2], 'total': 2}}))
    assert scanned == []


def test_locked_db_at_member_start_keeps_the_member(monkeypatch, dbs):
    _user(1)
    calls = {"n": 0}

    def flaky_needs_scan(fid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise sqlite3.OperationalError("database is locked")
        return True

    async def fake_resolve(cog, fid, **kw):
        return None

    monkeypatch.setattr(gr.gift_state_resolver, "needs_scan", flaky_needs_scan)
    monkeypatch.setattr(gr.gift_state_resolver, "resolve_state", fake_resolve)
    assert _scan(1) is None                       # scanned (done-marked), not dropped by the lock
    assert _scan_columns(1)[1] is not None


# --- Member States: counts and bulk writes without per-row queries ------------------------


def test_member_fix_counts_match_members_to_fix(dbs):
    _user(1, kid=5, mismatch="2026-10-01T00:00:00")
    _user(2)
    _user(3, kid=7)
    _user(4, alliance="")
    _user(5, mismatch="2026-10-02T00:00:00")
    rows = gsr.members_to_fix()
    expected = {"wrong": sum(r[3] == "wrong" for r in rows), "missing": sum(r[3] == "missing" for r in rows)}
    assert gsr.member_fix_counts() == expected == {"wrong": 2, "missing": 1}


def test_scan_counts_match_scan_targets(dbs):
    now = datetime.now()
    _user(1)
    _user(2, done=(now - timedelta(days=1)).isoformat(timespec="seconds"))
    _user(3, done=(now - timedelta(days=40)).isoformat(timespec="seconds"))
    _user(4, kid=9)
    waiting, needing = gsr.scan_counts()
    assert (waiting, needing) == (len(gsr.scan_targets(auto=True)), len(gsr.scan_targets(auto=False)))
    assert (waiting, needing) == (2, 3)


def test_bulk_dismiss_and_set_state(dbs):
    _user(1, kid=5, mismatch="x")
    _user(2, kid=6, mismatch="x")
    _user(3, mismatch="x", nxt=40, done="2026-01-01T00:00:00")
    gsr.clear_state_mismatch_many([1, 2])
    assert [row[0] for row in gsr.fids_with_state_mismatch()] == [3]
    gsr.set_user_kid_many([1, 3], 911)
    with sqlite3.connect("db/users.sqlite") as c:
        rows = c.execute("SELECT fid, kid, state_mismatch_at, kingdom_scan_next, kingdom_scan_done_at "
                         "FROM users ORDER BY fid").fetchall()
    assert rows == [(1, 911, None, None, None), (2, 6, None, None, None), (3, 911, None, None, None)]


def test_survey_matches_per_alliance_binding(dbs):
    for fid, kid, alliance in [(1, 9, "5"), (2, 9, "5"), (3, 9, "5"), (4, 7, "6"), (5, 8, "6"), (6, 3, "7")]:
        _user(fid, kid=kid, alliance=alliance)
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.executemany("INSERT INTO alliance_list VALUES (?,?,?,?,?)",
                      [(5, "A", None, 0, 0), (6, "B", None, 0, 0), (7, "C", None, 1, 0), (8, "D", None, 0, 0)])
    survey = {r["alliance_id"]: r for r in gsr.survey_alliance_bindings()}
    assert survey[5]["proposed_kid"] == gsr.compute_alliance_binding(5)[0] == 9
    assert survey[6]["proposed_kid"] is gsr.compute_alliance_binding(6) is None
    assert survey[7]["proposed_kid"] is None and survey[7]["multistate"]
    assert survey[8]["known"] == 0


# --- API first: Player Sync's API places members before any probing -----------------------


@pytest.fixture
def api(monkeypatch):
    """Player Sync set up; `api.kids` is what the API knows, `api.asked` who was looked up."""
    state = fake_player_api(
        monkeypatch, lambda fid: {"nickname": f"p{fid}", "kid": state.kids[fid]} if fid in state.kids else None)
    state.kids = {}
    return state


def _scan_cog(stop=False, preempt=False, saved=None):
    pq = types.SimpleNamespace(should_preempt=lambda: preempt, stop_requested=lambda: stop,
                               update_details=lambda pid, d: (saved if saved is not None else []).append(dict(d)))
    return types.SimpleNamespace(logger=logging.getLogger("test"),
                                 bot=types.SimpleNamespace(get_cog=lambda name: pq))


def _probed(monkeypatch):
    probed = []

    async def recording_scan(cog, fid, **kw):
        probed.append(fid)
        return None

    monkeypatch.setattr(gr, "scan_one_member", recording_scan)
    return probed


def _run_scan(details, cog=None):
    """Runs the scan job; returns its last saved details (the handler works on a copy)."""
    saved = []
    asyncio.run(gr.handle_state_resolve_process(cog or _scan_cog(saved=saved), {'id': 7, 'details': details}))
    return saved[-1] if saved else None


def test_api_state_is_saved_and_member_skips_the_probe(monkeypatch, dbs, api):
    _user(1)
    _user(2, kid=259, mismatch="2026-10-01T00:00:00")
    _user(3)
    api.kids = {1: 812, 2: 913}
    probed = _probed(monkeypatch)
    queued = []
    monkeypatch.setattr(gr, "enqueue_member_redemption", lambda cog, fid: queued.append(fid) or 1)
    details = _run_scan({'mode': 'now', 'remaining': [1, 2, 3], 'total': 3})
    assert probed == [3]                                  # only the member the API doesn't know
    assert kid_and_flag(1) == (812, None) and kid_and_flag(2) == (913, None)
    assert queued == [1, 2]
    assert details['resolved'] == 2 and details['api_found'] == 2 and details['unresolved'] == 1


def test_without_the_api_every_member_is_probed(monkeypatch, dbs, api):
    _user(1)
    _user(2)
    api.available = False
    probed = _probed(monkeypatch)
    _run_scan({'mode': 'now', 'remaining': [1, 2], 'total': 2})
    assert probed == [1, 2] and api.asked == []


def test_api_state_the_game_just_rejected_is_probed(monkeypatch, dbs, api):
    _user(1, kid=259, mismatch="2026-10-01T00:00:00")
    api.kids = {1: 259}                                   # the API repeats the rejected state
    probed = _probed(monkeypatch)
    _run_scan({'mode': 'now', 'remaining': [1], 'total': 1})
    assert probed == [1] and kid_and_flag(1) == (259, "2026-10-01T00:00:00")


def _real_api(monkeypatch, per_day, used, answers):
    """Real Player Sync lookups against a fake API: `answers` maps fid -> (status, payload)."""
    import cogs.alliance_api_sync as sync
    profile = dict(sync.DEFAULT_PROFILE, base_url="https://api.example", player_path="/p/{id}",
                   per_day=per_day, fields={**sync.DEFAULT_PROFILE["fields"], "kingdom": "kid"})
    sync.save_setting("profile", profile)
    sync.save_setting("budget", {"date": sync.utc_today(), "used": used})
    asked = []

    async def fake_fetch(session, profile, key, fid):
        asked.append(fid)
        status, payload = answers[fid]
        return sync.FetchResult(status, payload)

    monkeypatch.setattr(sync, "fetch_player", fake_fetch)
    api_cog = sync.AllianceApiSync.__new__(sync.AllianceApiSync)
    api_cog.session = lambda: None
    saved = []
    pq = types.SimpleNamespace(should_preempt=lambda: False, stop_requested=lambda: False,
                               update_details=lambda pid, d: saved.append(dict(d)))
    cog = types.SimpleNamespace(logger=logging.getLogger("test"),
                                bot=types.SimpleNamespace(get_cog=lambda name: api_cog
                                                          if name == "AllianceApiSync" else pq))
    return cog, asked, saved


def test_budget_used_up_leaves_members_for_tomorrow(monkeypatch, dbs):
    _user(1)
    _user(2)
    cog, asked, saved = _real_api(monkeypatch, per_day=5, used=5, answers={})
    probed = _probed(monkeypatch)
    sent = []

    async def dm(cog, embed):
        sent.append(embed.description)

    monkeypatch.setattr(gr, "_dm_global_admins", dm)
    asyncio.run(gr.handle_state_resolve_process(cog, {'id': 7, 'details': {'mode': 'now', 'remaining': [1, 2],
                                                                          'total': 2}}))
    assert asked == [] and probed == []
    assert saved[-1]['remaining'] == [1, 2] and saved[-1]['limit_reached'] is True
    assert gsr.scan_targets(auto=True) == [1, 2]            # not marked done: tomorrow's scan asks again
    assert "`2` members weren't looked up yet" in sent[0]


def test_only_members_the_api_was_asked_about_are_probed(monkeypatch, dbs):
    _user(1)
    _user(2)
    cog, asked, saved = _real_api(monkeypatch, per_day=1, used=0, answers={1: (404, None)})
    probed = _probed(monkeypatch)
    gsr.set_scan_enabled(True)
    asyncio.run(gr.handle_state_resolve_process(cog, {'id': 7, 'details': {'mode': 'auto', 'remaining': [1, 2],
                                                                          'total': 2}}))
    assert asked == [1] and probed == [1]
    assert saved[-1]['remaining'] == [2] and saved[-1]['limit_reached'] is True


def test_auto_tick_waits_for_tomorrow_when_the_api_limit_is_used_up(monkeypatch, dbs, ocr_idle):
    _user(1)
    gsr.set_scan_enabled(True)
    _real_api(monkeypatch, per_day=5, used=5, answers={})
    cog, pq = _gift_cog()
    assert cog.auto_kingdom_scan_tick() is False and pq.enqueued == []


def test_db_error_after_the_lookup_leaves_the_member_to_be_asked_again(monkeypatch, dbs, api):
    _user(1)
    api.kids = {1: 812}
    probed = _probed(monkeypatch)

    def locked(fid):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(gr.gift_state_resolver, "scan_check", locked)
    details = _run_scan({'mode': 'now', 'remaining': [1], 'total': 1})
    assert probed == [] and details['api_checked'] == [] and details['remaining'] == [1]


def test_auto_scan_resolves_its_targets_through_the_api(monkeypatch, dbs, api, ocr_idle):
    _user(1)
    _user(2)
    gsr.set_scan_enabled(True)
    api.kids = {1: 812, 2: 813}
    gift_cog, pq = _gift_cog()
    assert gift_cog.auto_kingdom_scan_tick() is True
    details = pq.enqueued[0][2]
    probed = _probed(monkeypatch)
    monkeypatch.setattr(gr, "enqueue_member_redemption", lambda cog, fid: 0)
    monkeypatch.setattr(gr, "_auto_should_wait", lambda cog: True)   # busy bot: lookups still run
    details = _run_scan(details)
    assert probed == [] and details['remaining'] == []
    assert kid_and_flag(1)[0] == 812 and kid_and_flag(2)[0] == 813


def test_api_pass_stops_for_an_admin(monkeypatch, dbs, api):
    _user(1)
    api.kids = {1: 812}
    probed = _probed(monkeypatch)
    saved = []
    _run_scan({'mode': 'now', 'remaining': [1], 'total': 1}, _scan_cog(stop=True, preempt=True, saved=saved))
    assert api.asked == [] and probed == [] and saved[-1]['remaining'] == [1]


def test_api_pass_pauses_and_resumes_without_asking_twice(monkeypatch, dbs, api):
    _user(1)
    _user(2)
    _user(3)
    api.kids = {2: 812}
    probed = _probed(monkeypatch)
    calls = {"n": 0}

    def preempt_on_third_check():
        calls["n"] += 1
        return calls["n"] == 3

    saved = []
    cog = _scan_cog(saved=saved)
    cog.bot.get_cog("ProcessQueue").should_preempt = preempt_on_third_check
    details = {'mode': 'auto', 'remaining': [1, 2, 3], 'total': 3}
    gsr.set_scan_enabled(True)
    with pytest.raises(gr.PreemptedException):
        _run_scan(details, cog)
    assert api.asked == [1, 2] and probed == []
    assert saved[-1]['remaining'] == [1, 3] and saved[-1]['api_checked'] == [1]

    _run_scan(dict(saved[-1]))                             # resumed run
    assert api.asked == [1, 2, 3]                          # member 1 isn't looked up again
    assert probed == [1, 3]


def test_scan_done_dm_counts_api_finds(monkeypatch):
    sent = []

    async def dm(cog, embed):
        sent.append(embed.description)

    monkeypatch.setattr(gr, "_dm_global_admins", dm)
    asyncio.run(gr._notify_state_resolve_done(_log_cog(), {'total': 3, 'resolved': 2, 'unresolved': 1,
                                                           'api_found': 2, 'api_checked': [3]}))
    asyncio.run(gr._notify_state_resolve_done(_log_cog(), {'total': 1, 'resolved': 0, 'unresolved': 1}))
    assert "(2 through your API)" in sent[0] and "Your API and the scan" in sent[0]
    assert "API" not in sent[1]


@pytest.mark.parametrize("mode,expected", [("now", "Press Scan These Now again tomorrow"),
                                           ("auto", "The scan continues tomorrow")])
def test_limit_progress_text_fits_the_kind_of_run(monkeypatch, dbs, mode, expected):
    _user(1)
    gsr.set_scan_enabled(True)
    cog, asked, saved = _real_api(monkeypatch, per_day=5, used=5, answers={})
    _probed(monkeypatch)
    bodies = []

    async def record(self, fid, body):
        bodies.append(body)
        return str(fid)

    async def dm(cog, embed):
        pass

    monkeypatch.setattr(gr._StateScanProgress, "update", record)
    monkeypatch.setattr(gr, "_dm_global_admins", dm)
    asyncio.run(gr.handle_state_resolve_process(cog, {'id': 7, 'details': {'mode': mode, 'remaining': [1],
                                                                          'total': 1}}))
    assert expected in bodies[-1]


def _single_state_alliance(auto_remove):
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("INSERT INTO alliance_list VALUES (5, 'A', 100, 0, 0)")
        c.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, auto_remove_on_transfer INTEGER)")
        c.execute("INSERT INTO alliancesettings VALUES (5, ?)", (auto_remove,))


def test_api_pass_removes_a_member_outside_the_alliance_state_with_auto_remove_on(monkeypatch, dbs, api):
    _single_state_alliance(auto_remove=1)
    _user(1)
    api.kids = {1: 250}
    _probed(monkeypatch)
    applied = []
    real_apply = gr.apply_removals

    async def recording_apply(cog, candidates):
        applied.append(list(candidates))
        return await real_apply(cog, candidates)

    monkeypatch.setattr(gr, "apply_removals", recording_apply)
    details = _run_scan({'mode': 'now', 'remaining': [1], 'total': 1})
    assert applied == [[("5", 1, "p1")]] and kid_and_flag(1) is None
    assert details.get('api_found', 0) == 0 and details['resolved'] == 0
    assert 250 not in details.get('found_states', [])


def test_a_failed_removal_never_hides_the_scan_interruption(monkeypatch, dbs, api):
    _single_state_alliance(auto_remove=1)
    _user(1)
    _user(2)
    api.kids = {1: 250}

    async def broken_apply(cog, candidates):
        raise RuntimeError("log channel gone")

    async def check_yield():
        if api.asked:
            raise gsr.StateResolveInterrupted(None)

    class Progress:
        checked = 0

        async def update(self, *args):
            pass

    monkeypatch.setattr(gr, "apply_removals", broken_apply)
    details = {'resolved': 0}
    with pytest.raises(gsr.StateResolveInterrupted):
        asyncio.run(gr._resolve_through_api(_scan_cog(), details, [1, 2], [], check_yield, lambda: None,
                                            Progress()))


def test_api_pass_saves_a_member_outside_the_alliance_state_with_auto_remove_off(monkeypatch, dbs, api):
    _single_state_alliance(auto_remove=0)
    _user(1)
    api.kids = {1: 250}
    _probed(monkeypatch)
    monkeypatch.setattr(gr, "enqueue_member_redemption", lambda cog, fid: 0)
    _run_scan({'mode': 'now', 'remaining': [1], 'total': 1})
    assert kid_and_flag(1) == (250, None)
