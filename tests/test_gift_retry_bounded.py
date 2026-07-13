"""Rate-limited members must not retry forever and wedge the ProcessQueue.

CAPTCHA_TOO_FREQUENT / TIMEOUT_RETRY re-queued the member without ever
advancing the retry-cycle counter, so one FID stuck in a persistent per-FID
captcha penalty looped every 60s indefinitely - and since ProcessQueue runs
one process at a time, every redemption queued behind it starved.
"""
import asyncio
import logging
import sqlite3
import types

import cogs.gift_redemption as gr

real_sleep = asyncio.sleep


def _setup(monkeypatch, status):
    ac = sqlite3.connect(":memory:")
    ac.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, channel_id INTEGER, redemption_channel_id INTEGER)")
    ac.execute("INSERT INTO alliancesettings VALUES (5, NULL, NULL)")
    ac.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    ac.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    ac.commit()

    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT, validation_status TEXT)")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    mc.commit()

    cog = types.SimpleNamespace(
        alliance_cursor=ac.cursor(),
        cursor=mc.cursor(),
        get_test_fid=lambda: 999,
        captcha_solver=object(),
        bot=types.SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None),
        logger=logging.getLogger("test"),
    )

    users_mem = sqlite3.connect(":memory:")
    users_mem.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT)")
    users_mem.execute("INSERT INTO users VALUES (1, 'stuck', '5')")
    users_mem.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("users.sqlite"):
            return users_mem
        return real_connect(path, *a, **k)

    claims = {"n": 0}

    async def fake_claim(cog_, fid, giftcode, **kw):
        claims["n"] += 1
        if claims["n"] >= 50:
            return "SUCCESS"  # escape hatch so a regression fails fast, not hangs
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
    monkeypatch.setattr(gr, "batch_get_user_giftcode_status", lambda cog_, code, ids: {})
    monkeypatch.setattr(gr, "batch_process_alliance_results", lambda cog_, results: None)
    monkeypatch.setattr(gr, "post_redemption_summary", _anoop)
    return cog, claims


def test_captcha_too_frequent_gives_up_after_max_cycles(monkeypatch):
    cog, claims = _setup(monkeypatch, "CAPTCHA_TOO_FREQUENT")

    result = asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE"))

    assert result is True
    assert claims["n"] <= 10, \
        f"rate-limited member must stop after MAX_RETRY_CYCLES, made {claims['n']} attempts"


def test_timeout_retry_gives_up_after_max_cycles(monkeypatch):
    cog, claims = _setup(monkeypatch, "TIMEOUT_RETRY")

    result = asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE"))

    assert result is True
    assert claims["n"] <= 10, \
        f"rate-limited member must stop after MAX_RETRY_CYCLES, made {claims['n']} attempts"
