"""Kingdom scan: settings, targets, resume columns, scan engine handler and auto-scan."""
import asyncio
import logging
import sqlite3
import types
from datetime import datetime, timedelta

import pytest

import cogs.gift_state_resolver as gsr


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


def test_scan_position_round_trip_done_and_reset(dbs):
    _user(1)
    gsr.save_scan_position(1, 1234)
    assert gsr.get_scan_position(1) == 1234
    gsr.mark_scan_done(1)
    nxt, done = _scan_columns(1)
    assert nxt is None and done is not None
    gsr.reset_scan(1)
    assert _scan_columns(1) == (None, None)


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
