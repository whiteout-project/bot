"""Input sanitization.

`GiftOperations.clean_gift_code` strips invisible/control characters (Unicode
category C*) and trims whitespace — guarding against codes pasted with RLM /
zero-width / control characters that silently fail redemption (commit 46e0092).
It ignores `self`, so we call it unbound.

(The other coercion guards flagged in the analysis — `int('')`, str-vs-int
alliance_id — are inline `try/except`/guards inside methods, not reusable
functions, so they aren't unit-testable without a refactor.)
"""
from __future__ import annotations

import pytest

from cogs.gift_operations import GiftOperations

clean = GiftOperations.clean_gift_code


def test_plain_code_unchanged():
    assert clean(None, "ABCD1234") == "ABCD1234"


def test_trims_surrounding_whitespace():
    assert clean(None, "  ABCD1234  ") == "ABCD1234"


@pytest.mark.parametrize("raw", [
    "ABCD‏1234",   # RLM (right-to-left mark) in the middle
    "‏ABCD1234‏",  # RLM at both ends
    "ABCD​1234",   # zero-width space
    "ABCD\t1234",       # tab (control)
    "ABCD1234\n",       # trailing newline
    "‪ABCD1234‬",  # LTR embedding / pop directional formatting
])
def test_strips_invisible_and_control_chars(raw):
    assert clean(None, raw) == "ABCD1234"


def test_fully_invisible_becomes_empty():
    assert clean(None, "‏​\t\n") == ""


def test_internal_space_is_preserved():
    # Spaces are category Zs (not C*), so only leading/trailing are trimmed.
    assert clean(None, "  AB CD  ") == "AB CD"
