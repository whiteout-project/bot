"""Discord payload-limit chunking.

`MinisterSchedule.split_message_content` splits a long list of lines into
messages that fit Discord's 2000-char limit. A miss here is a user-facing
400 "Invalid Form Body" crash (commit 78aee3a). It ignores `self`, so we call
it unbound.

(The other size guards in the codebase — select-menu `[:25]` clamps, embed
description truncation — are inline expressions, not reusable functions, so
they aren't unit-testable without a refactor.)
"""
from __future__ import annotations

from cogs.minister_schedule import MinisterSchedule

split = MinisterSchedule.split_message_content


def _lines_from(messages, header):
    """Reassemble the original lines from chunked messages (drop the header)."""
    out = []
    for m in messages:
        assert m.startswith(header)
        body = m[len(header):].lstrip("\n")
        if body:
            out.extend(body.split("\n"))
    return out


def test_empty_list_returns_header_only():
    assert split(None, "HEADER", []) == ["HEADER"]


def test_small_list_is_one_message_with_all_lines():
    lines = [f"{h:02}:00" for h in range(5)]
    msgs = split(None, "HEADER", lines)
    assert len(msgs) == 1
    assert _lines_from(msgs, "HEADER") == lines


def test_large_list_splits_under_limit_and_roundtrips():
    lines = [f"{i:04}: minister slot entry number {i}" for i in range(400)]
    msgs = split(None, "Minister schedule", lines, max_length=1900)
    assert len(msgs) > 1                              # actually split
    for m in msgs:
        assert len(m) <= 1900                         # under our margin …
        assert len(m) <= 2000                         # … and Discord's hard limit
        assert m.startswith("Minister schedule")      # header on every chunk
    assert _lines_from(msgs, "Minister schedule") == lines  # nothing lost/dupe/reordered


def test_header_repeats_on_every_chunk():
    lines = ["x" * 100 for _ in range(100)]
    msgs = split(None, "HDR", lines, max_length=500)
    assert len(msgs) > 1
    assert all(m.startswith("HDR") for m in msgs)


def test_each_line_preserved_exactly_once():
    lines = [f"slot-{i}" for i in range(250)]
    msgs = split(None, "H", lines, max_length=300)
    reassembled = _lines_from(msgs, "H")
    assert sorted(reassembled) == sorted(lines)
    assert len(reassembled) == len(lines)
