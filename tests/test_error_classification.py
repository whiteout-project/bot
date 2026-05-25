"""API error classification.

`AllianceSync.is_connection_error` decides whether a failed player fetch is a
transient network problem (keep the player) or a real player error (a strike
toward removal). Historically, misclassifying HTTP 403/429/502/503/504 as
"player error" wrongly removed valid players and caused restart loops — so these
are exactly the cases to pin down.

`is_connection_error` ignores `self`, so we call it unbound.
"""
from __future__ import annotations

import pytest

from cogs.alliance_sync import AllianceSync

classify = AllianceSync.is_connection_error


# --- transient/network errors → must be treated as connection issues (keep player) ---
@pytest.mark.parametrize("msg", [
    "HTTP 403 Forbidden",
    "Server returned 429 Too Many Requests",
    "Bad gateway (502)",
    "503 Service Unavailable",
    "Gateway timeout 504",
    "Connection timed out",
    "Connection refused",
    "Connection reset by peer",
    "Host unreachable",
    "DNS lookup failed",
    "Network is down",
    "socket error",
    "SSL handshake failed",
    "certificate verify failed",
    "Read timeout",
])
def test_network_errors_are_connection_errors(msg):
    assert classify(None, msg) is True


# --- genuine player/data errors → must NOT be connection issues (count as strike) ---
@pytest.mark.parametrize("msg", [
    "Invalid FID",
    "role not exist",
    "user does not exist",
    "Player not found",
    "err_code 40004",
    "Sign error",
    "",
])
def test_player_errors_are_not_connection_errors(msg):
    assert classify(None, msg) is False


def test_classification_is_case_insensitive():
    assert classify(None, "TIMEOUT") is True
    assert classify(None, "Forbidden") is True


def test_all_http_transient_codes_covered():
    # Regression guard for the exact codes that caused wrongful removals.
    for code in ("403", "429", "502", "503", "504"):
        assert classify(None, f"request failed with status {code}") is True
