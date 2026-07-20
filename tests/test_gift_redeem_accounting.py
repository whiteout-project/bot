"""A gift_redeem handler must report the batch outcome that actually happened.
When use_giftcode_for_alliance bails (e.g. no channel, invalid code) it returns
False without raising; the handler used to record success=True regardless, so a
redemption that reached zero members still showed green. The batch result must
follow the return value.
"""
import asyncio
import logging
import types

import cogs.gift_redemption as gr


def _run_handler(monkeypatch, use_return):
    recorded = {}

    async def fake_use(cog, alliance_id, giftcode, process=None):
        return use_return

    async def fake_start(cog, batch_id, alliance_id):
        pass

    async def fake_result(cog, batch_id, alliance_id, success):
        recorded['success'] = success

    monkeypatch.setattr(gr, 'use_giftcode_for_alliance', fake_use)
    monkeypatch.setattr(gr, '_record_batch_start', fake_start)
    monkeypatch.setattr(gr, '_record_batch_result', fake_result)

    cog = types.SimpleNamespace(logger=logging.getLogger('test'), captcha_solver=None)
    process = {'id': 1, 'alliance_id': 5, 'details': {'giftcode': 'CODE', 'batch_id': 'b1'}}
    asyncio.run(gr.handle_gift_redeem_process(cog, process))
    return recorded


def test_bailed_redemption_records_failure(monkeypatch):
    recorded = _run_handler(monkeypatch, use_return=False)
    assert recorded.get('success') is False


def test_successful_redemption_records_success(monkeypatch):
    recorded = _run_handler(monkeypatch, use_return=True)
    assert recorded.get('success') is True


# --- per-member outcome persistence (Redeem History) ---

import sqlite3

real_sleep = asyncio.sleep


def _drive(monkeypatch, status, preseed=None):
    """Run a full one-member redemption with the REAL batch persistence."""
    ac = sqlite3.connect(":memory:")
    ac.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, channel_id INTEGER, redemption_channel_id INTEGER)")
    ac.execute("INSERT INTO alliancesettings VALUES (5, NULL, NULL)")
    ac.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    ac.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    ac.commit()

    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT, validation_status TEXT)")
    mc.execute("""CREATE TABLE user_giftcodes (
        fid INTEGER, giftcode TEXT, status TEXT, last_attempt_at TEXT,
        PRIMARY KEY (fid, giftcode))""")
    for row in (preseed or []):
        mc.execute("INSERT INTO user_giftcodes (fid, giftcode, status) VALUES (?, ?, ?)", row)
    mc.commit()

    cog = types.SimpleNamespace(
        alliance_cursor=ac.cursor(),
        cursor=mc.cursor(),
        conn=mc,
        get_test_fid=lambda: 999,
        captcha_solver=object(),
        bot=types.SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None),
        logger=logging.getLogger("test"),
    )

    users_mem = sqlite3.connect(":memory:")
    users_mem.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT)")
    users_mem.execute("INSERT INTO users VALUES (1, 'member', '5')")
    users_mem.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("users.sqlite"):
            return users_mem
        return real_connect(path, *a, **k)

    claims = {"n": 0}

    async def fake_claim(cog_, fid, giftcode, **kw):
        claims["n"] += 1
        return status

    clock = {"t": 1000.0}

    async def fast_sleep(seconds):
        clock["t"] += max(float(seconds), 0.0)
        await real_sleep(0)

    async def _anoop(*a, **k):
        pass

    monkeypatch.setattr(gr.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(gr.time, "time", lambda: clock["t"])
    monkeypatch.setattr(gr.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(gr.random, "uniform", lambda a, b: 0)
    monkeypatch.setattr(gr, "claim_giftcode_rewards_wos", fake_claim)
    monkeypatch.setattr(gr, "post_redemption_summary", _anoop)

    assert asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE")) is True
    row = mc.execute(
        "SELECT status FROM user_giftcodes WHERE fid = 1 AND giftcode = 'CODE'"
    ).fetchone()
    return claims["n"], (row[0] if row else None)


def test_terminal_failure_is_persisted_for_history(monkeypatch):
    claims, stored = _drive(monkeypatch, "TOO_SMALL_SPEND_MORE")
    assert claims == 1
    assert stored == "TOO_SMALL_SPEND_MORE", "failures must reach Redeem History"


def test_cached_failure_is_retried_and_upgraded(monkeypatch):
    claims, stored = _drive(
        monkeypatch, "SUCCESS", preseed=[(1, "CODE", "CAPTCHA_INVALID")]
    )
    assert claims == 1, "a cached failure must not permanently exclude the member"
    assert stored == "SUCCESS"


def test_cached_success_is_not_retried(monkeypatch):
    claims, stored = _drive(
        monkeypatch, "SUCCESS", preseed=[(1, "CODE", "RECEIVED")]
    )
    assert claims == 0, "conclusive successes stay skipped"
    assert stored == "RECEIVED"


def test_used_count_excludes_persisted_failures():
    """Failures now live in user_giftcodes for Redeem History - the Used by
    counts must only count actual redemptions."""
    import cogs.gift_views as gv

    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT PRIMARY KEY, date TEXT, validation_status TEXT)")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT, PRIMARY KEY (fid, giftcode))")
    mc.execute("INSERT INTO gift_codes VALUES ('CODE', '2026-07-01', 'validated')")
    for fid, status in [(1, "SUCCESS"), (2, "RECEIVED"), (3, "TOO_SMALL_SPEND_MORE"), (4, "CAPTCHA_INVALID")]:
        mc.execute("INSERT INTO user_giftcodes VALUES (?, 'CODE', ?)", (fid, status))
    mc.commit()

    cog = types.SimpleNamespace(cursor=mc.cursor())
    sent = []

    async def send_message(*a, **k):
        sent.append(k.get("embed") or (a[0] if a else None))

    inter = types.SimpleNamespace(response=types.SimpleNamespace(send_message=send_message))
    asyncio.run(gv.list_gift_codes(cog, inter))

    embed = sent[0]
    assert "Used by: 2 users" in embed.fields[0].value, \
        f"failures must not inflate the count: {embed.fields[0].value}"
