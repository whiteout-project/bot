"""Redeem-response classification for the 2026 gift API (single POST, no login).

The /api/player login step is gone, so classification now happens on the
/api/gift_code response itself:
 - HTTP 429/5xx is a transient throttle -> TIMEOUT_RETRY so the retry cycles
   handle it (real case: FID 21385585 got 429, retried, redeemed fine 60s later).
 - "role not exist." (err 40001) means the account is gone -> ROLE_NOT_EXIST so
   the summary says so and alliance sync can remove it.
 - A player with no state (kid) on file can't be redeemed for -> NO_STATE, since
   the API now requires kid and it can't be fetched anymore.
"""
import asyncio
import logging
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
    return types.SimpleNamespace(
        wos_encrypt_key="tB87#kPtkxqOS2",
        wos_giftcode_url="https://example.invalid/api/gift_code",
        logger=logging.getLogger("test"),
        giftlog=logging.getLogger("test.giftlog"),
        processing_stats={"redemption_submissions": 0, "server_validation_success": 0, "server_validation_failure": 0},
    )


def _redeem(response):
    session = types.SimpleNamespace(post=lambda url, data=None, timeout=None: response)
    return asyncio.run(gr.redeem_giftcode_once(_make_cog(), 21385585, "WOS0715", 312, session))


def test_http_429_routes_to_timeout_retry():
    assert _redeem(FakeResponse(429, {"message": "Too Many Attempts."})) == "TIMEOUT_RETRY"


@pytest.mark.parametrize("http_status", [502, 503, 504])
def test_http_5xx_routes_to_timeout_retry(http_status):
    assert _redeem(FakeResponse(http_status, {"message": "upstream error"})) == "TIMEOUT_RETRY"


def test_success():
    assert _redeem(FakeResponse(200, {"code": 0, "msg": "SUCCESS", "data": [], "err_code": 20000})) == "SUCCESS"


def test_time_error_is_expired():
    assert _redeem(FakeResponse(200, {"code": 1, "msg": "TIME ERROR.", "data": [], "err_code": 40007})) == "TIME_ERROR"


def test_role_not_exist_gets_own_status():
    assert _redeem(FakeResponse(200, {"code": 1, "msg": "role not exist.", "data": [], "err_code": 40001})) == "ROLE_NOT_EXIST"


def test_user_info_error_is_state_mismatch():
    # Real 2026 response for a wrong/stale kid (seen live: fids 44787419, 49435204, 59932394).
    assert _redeem(FakeResponse(200, {"code": 1, "msg": "USER INFO ERROR.", "data": [], "err_code": 40020})) == "STATE_MISMATCH"


def test_40001_with_unrelated_msg_is_unknown():
    assert _redeem(FakeResponse(200, {"code": 1, "msg": "params error", "err_code": 40001})) == "UNKNOWN_API_RESPONSE"


def test_unknown_response_is_unknown():
    assert _redeem(FakeResponse(200, {"code": 1, "msg": "something else", "err_code": 12345})) == "UNKNOWN_API_RESPONSE"


import sqlite3


def test_get_user_kid_falls_back_to_test_fid_state(monkeypatch):
    users = sqlite3.connect(":memory:", check_same_thread=False)  # get_user_kid reads via to_thread
    users.execute("CREATE TABLE users (fid INTEGER, kid INTEGER)")
    users.execute("INSERT INTO users VALUES (1, 245)")   # a real member with a state
    users.commit()
    settings = sqlite3.connect(":memory:", check_same_thread=False)
    settings.execute("CREATE TABLE test_fid_settings (id INTEGER PRIMARY KEY, test_fid TEXT, kid INTEGER)")
    settings.execute("INSERT INTO test_fid_settings (test_fid, kid) VALUES ('45379845', 312)")
    settings.commit()
    real = sqlite3.connect

    def fake(path, *a, **k):
        if str(path).endswith("users.sqlite"): return users
        if str(path).endswith("settings.sqlite"): return settings
        return real(path, *a, **k)

    monkeypatch.setattr(gr.sqlite3, "connect", fake)
    cog = types.SimpleNamespace(logger=logging.getLogger("test"))
    assert asyncio.run(gr.get_user_kid(cog, 1)) == 245           # member's own state
    assert asyncio.run(gr.get_user_kid(cog, 45379845)) == 312    # test FID (not a member) -> its stored state
    assert asyncio.run(gr.get_user_kid(cog, 99999)) is None      # unknown, not a member or test FID


# --- claim-level guard: no state on file means we can't redeem ---


def test_claim_returns_no_state_when_kid_missing(monkeypatch):
    async def no_kid(cog_, fid):
        return None

    monkeypatch.setattr(gr, "get_user_kid", no_kid)
    cog = types.SimpleNamespace(
        clean_gift_code=lambda code: code,
        logger=logging.getLogger("test"),
        giftlog=logging.getLogger("test.giftlog"),
        processing_stats={"total_fids_processed": 0, "total_processing_time": 0.0},
    )
    result = asyncio.run(gr.claim_giftcode_rewards_wos(cog, 12345, "CODE", skip_cache=True))
    assert result == "NO_STATE"


def test_claim_redeems_when_kid_present(monkeypatch):
    async def has_kid(cog_, fid):
        return 312

    async def fake_redeem(cog_, fid, code, kid, session):
        assert kid == 312
        return "SUCCESS"

    monkeypatch.setattr(gr, "get_user_kid", has_kid)
    monkeypatch.setattr(gr, "redeem_giftcode_once", fake_redeem)
    cog = types.SimpleNamespace(
        clean_gift_code=lambda code: code,
        get_test_fid=lambda: 12345,  # == player_id so the DB write branch is skipped
        retry_config=None,
        wos_giftcode_redemption_url="https://example.invalid",
        logger=logging.getLogger("test"),
        giftlog=logging.getLogger("test.giftlog"),
        processing_stats={"total_fids_processed": 0, "total_processing_time": 0.0, "redemption_submissions": 0},
    )
    result = asyncio.run(gr.claim_giftcode_rewards_wos(cog, 12345, "CODE", skip_cache=True))
    assert result == "SUCCESS"


# --- batch loop: a terminal status must not retry forever ---

import sqlite3


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
        bot=types.SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None),
        logger=logging.getLogger("test"),
    )

    users_mem = sqlite3.connect(":memory:")
    users_mem.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT, kid INTEGER)")
    users_mem.execute("INSERT INTO users VALUES (1, 'ghost', '5', 312)")
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


def test_no_state_is_terminal_after_one_attempt(monkeypatch):
    cog, claims, captured = _setup_alliance_run(monkeypatch, "NO_STATE")
    result = asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE"))
    assert result is True
    assert claims["n"] == 1  # not retried - retrying won't conjure a state
    nickname, reason, cycles = captured["failed"][1]
    assert "NO_STATE" in reason
