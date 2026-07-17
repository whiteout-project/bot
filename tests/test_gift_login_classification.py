"""Login failures must be classified, not lumped into terminal LOGIN_FAILED.

A 429 on the player-info call is a transient throttle - it must route to
TIMEOUT_RETRY so the existing retry cycles handle it (a real support case:
FID 21385585 got 429 "Too Many Attempts.", was marked LOGIN_FAILED, and
redeemed fine 60s later). A "role not exist." reply means the account is
gone - that is genuinely terminal and gets its own status so the summary
can say so instead of suggesting the player log off and on.
"""
import asyncio
import logging
import sqlite3
import types

import pytest

import cogs.gift_redemption as gr


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.text = str(payload)

    def json(self):
        return self._payload


def _make_cog():
    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT, validation_status TEXT)")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    mc.commit()
    return types.SimpleNamespace(
        clean_gift_code=lambda code: code,
        cursor=mc.cursor(),
        get_test_fid=lambda: 999,
        captcha_solver=types.SimpleNamespace(save_images_mode=0, reset_run_stats=lambda: None),
        logger=logging.getLogger("test"),
        giftlog=logging.getLogger("test.giftlog"),
        processing_stats={"total_fids_processed": 0, "total_processing_time": 0.0},
    )


def _claim_with_login_response(monkeypatch, response):
    session = types.SimpleNamespace(close=lambda: None)

    async def fake_stove_info(cog_, player_id):
        return session, response

    monkeypatch.setattr(gr, "get_stove_info_wos", fake_stove_info)
    return asyncio.run(gr.claim_giftcode_rewards_wos(_make_cog(), 21385585, "WOS0715", skip_cache=True))


def test_login_429_routes_to_timeout_retry(monkeypatch):
    response = FakeResponse(429, {"message": "Too Many Attempts."})
    assert _claim_with_login_response(monkeypatch, response) == "TIMEOUT_RETRY"


@pytest.mark.parametrize("http_status", [502, 503, 504])
def test_login_5xx_routes_to_timeout_retry(monkeypatch, http_status):
    response = FakeResponse(http_status, {"message": "upstream error"})
    assert _claim_with_login_response(monkeypatch, response) == "TIMEOUT_RETRY"


def test_login_role_not_exist_gets_own_status(monkeypatch):
    response = FakeResponse(200, {"code": 1, "msg": "role not exist.", "data": [], "err_code": 40001})
    assert _claim_with_login_response(monkeypatch, response) == "ROLE_NOT_EXIST"


def test_login_other_failure_stays_login_failed(monkeypatch):
    response = FakeResponse(200, {"code": 1, "msg": "something else", "err_code": 12345})
    assert _claim_with_login_response(monkeypatch, response) == "LOGIN_FAILED"


def test_login_40001_with_unrelated_msg_stays_login_failed(monkeypatch):
    response = FakeResponse(200, {"code": 1, "msg": "params error", "err_code": 40001})
    assert _claim_with_login_response(monkeypatch, response) == "LOGIN_FAILED"


def _setup_alliance_run(monkeypatch, status):
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
    users_mem.execute("INSERT INTO users VALUES (1, 'ghost', '5')")
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

    captured = {}

    async def fake_summary(cog_, channel, alliance_id, alliance_name, giftcode, ok, dup, failed):
        captured["failed"] = failed

    real_sleep = asyncio.sleep

    async def fast_sleep(seconds):
        await real_sleep(0)

    monkeypatch.setattr(gr.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(gr.asyncio, "sleep", fast_sleep)
    monkeypatch.setattr(gr, "claim_giftcode_rewards_wos", fake_claim)
    monkeypatch.setattr(gr, "batch_get_user_giftcode_status", lambda cog_, code, ids: {})
    monkeypatch.setattr(gr, "batch_process_alliance_results", lambda cog_, results: None)
    monkeypatch.setattr(gr, "post_redemption_summary", fake_summary)
    return cog, claims, captured


def test_role_not_exist_is_terminal_after_one_attempt(monkeypatch):
    cog, claims, captured = _setup_alliance_run(monkeypatch, "ROLE_NOT_EXIST")
    result = asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE"))
    assert result is True
    assert claims["n"] == 1
    nickname, reason, cycles = captured["failed"][1]
    assert reason == "Account no longer exists"
    assert cycles == 1
