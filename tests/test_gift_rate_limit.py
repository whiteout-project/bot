"""Per-IP rate limiting: the whole run waits, per-FID throttling still parks one member.

Also covers member rows with no name, which used to break the run's summary.
"""
import asyncio
import importlib
import time
from types import SimpleNamespace

import pytest

gr = importlib.import_module("cogs.gift_redemption")


def _cog():
    return SimpleNamespace(logger=SimpleNamespace(warning=lambda *a, **k: None,
                                                  info=lambda *a, **k: None))


def _response(status_code=200, **headers):
    return SimpleNamespace(status_code=status_code, headers=headers)


# --- reading the server's numbers ---

def test_retry_after_wins_over_everything():
    assert gr.rate_limit_delay(_response(429, **{"Retry-After": "12"})) == pytest.approx(12, abs=0.1)


def test_reset_as_epoch_and_as_delta():
    epoch = gr.rate_limit_delay(_response(429, **{"X-RateLimit-Reset": str(int(time.time()) + 20)}))
    delta = gr.rate_limit_delay(_response(429, **{"X-RateLimit-Reset": "20"}))

    assert epoch == pytest.approx(20, abs=1.5)
    assert delta == pytest.approx(20, abs=1.5)


def test_delay_falls_back_when_the_server_says_nothing():
    assert gr.rate_limit_delay(_response(429)) == gr.RATE_LIMIT_FALLBACK_COOLDOWN


def test_garbage_headers_fall_back():
    assert gr.rate_limit_delay(_response(429, **{"Retry-After": "soon"})) == gr.RATE_LIMIT_FALLBACK_COOLDOWN


# --- what a response does to the shared gate ---

def test_429_pauses_the_whole_run():
    cog = _cog()
    gr.note_rate_limit(cog, _response(429, **{"Retry-After": "30"}))

    assert gr.rate_limit_wait(cog) == pytest.approx(30, abs=1.0)


def test_a_spent_budget_pauses_before_the_next_request():
    cog = _cog()
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "0",
                                              "X-RateLimit-Reset": "15"}))

    assert gr.rate_limit_wait(cog) == pytest.approx(15, abs=1.5)
    assert cog.rate_limit_remaining == 0
    assert cog.rate_limit_limit == 30


def test_budget_left_means_no_wait():
    cog = _cog()
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "12"}))

    assert gr.rate_limit_wait(cog) == 0
    assert cog.rate_limit_remaining == 12


def test_spent_budget_waits_only_for_the_window_to_roll():
    """The window is fixed from its first request, so a spent budget waits out the
    remainder, not a blanket minute. The endpoint sends no Retry-After or reset."""
    cog = _cog()
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "29"}))
    cog.rate_limit_window_started = time.time() - 50

    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "0"}))

    assert gr.rate_limit_wait(cog) == pytest.approx(gr.RATE_LIMIT_WINDOW - 50, abs=1.5)


def test_a_refilled_budget_starts_a_new_window():
    cog = _cog()
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "3"}))
    first = cog.rate_limit_window_started

    time.sleep(0.01)
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "29"}))

    assert cog.rate_limit_window_started > first


def test_retry_after_still_wins_over_the_tracked_window():
    cog = _cog()
    cog.rate_limit_window_started = time.time() - 50

    gr.note_rate_limit(cog, _response(429, **{"Retry-After": "25"}))

    assert gr.rate_limit_wait(cog) == pytest.approx(25, abs=1.0)


def test_unknown_window_falls_back(monkeypatch):
    cog = _cog()

    gr.note_rate_limit(cog, _response(429))

    assert gr.rate_limit_wait(cog) == pytest.approx(gr.RATE_LIMIT_FALLBACK_COOLDOWN, abs=1.0)


def test_a_later_headerless_response_does_not_re_pause():
    cog = _cog()
    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "15"}))
    cog.rate_limit_pause_until = 0  # window rolled over

    gr.note_rate_limit(cog, _response(200))

    assert gr.rate_limit_wait(cog) == 0, "a stale remaining=0 must not pause later requests"


def test_wait_is_zero_once_the_window_has_passed():
    cog = _cog()
    cog.rate_limit_pause_until = time.time() - 1
    assert gr.rate_limit_wait(cog) == 0


def test_a_fresh_cog_never_waits():
    assert gr.rate_limit_wait(_cog()) == 0


# --- the per-FID throttle is not the per-IP one ---

def test_per_fid_throttle_leaves_the_run_running():
    """TOO FREQUENT (40019) arrives as HTTP 200 with the error in the body, so the
    shared gate must stay open and only that member gets parked by the caller."""
    cog = _cog()

    gr.note_rate_limit(cog, _response(200, **{"X-RateLimit-Limit": "30", "X-RateLimit-Remaining": "24"}))

    assert gr.rate_limit_wait(cog) == 0


# --- the gate is honored before every claim ---

def test_claim_waits_out_an_active_pause(monkeypatch):
    cog = _cog()
    cog.rate_limit_pause_until = time.time() + 5
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)
        cog.rate_limit_pause_until = 0

    monkeypatch.setattr(gr.asyncio, "sleep", fake_sleep)
    asyncio.run(gr.await_rate_limit(cog))

    assert slept and slept[0] == pytest.approx(5, abs=1.0)


def test_members_with_no_name_get_the_placeholder():
    """A NULL nickname reached the run's summary as None and broke its join."""
    assert gr.named_members([(1, "Alpha"), (2, None), (3, "  ")]) == [
        (1, "Alpha"), (2, "Player 2"), (3, "Player 3")]


def test_claim_does_not_wait_without_a_pause(monkeypatch):
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)

    monkeypatch.setattr(gr.asyncio, "sleep", fake_sleep)
    asyncio.run(gr.await_rate_limit(_cog()))

    assert slept == []
