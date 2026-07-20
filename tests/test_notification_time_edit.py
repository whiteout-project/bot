"""Editing a notification's time must work for any stored UTC offset.

The Time modal hardcoded '+00:00' into its strptime formats: non-UTC
notifications (stored with their timezone's offset) crashed the modal on
open, and when parsing did succeed the write-back rebased the fire time
to UTC, silently shifting it by the offset.
"""
import importlib

ne = importlib.import_module("cogs.notification_editor")


def test_time_edit_preserves_non_utc_offset():
    out = ne.apply_time_edit("2026-07-18T19:00:00+02:00", 20, 30, None)
    assert out == "2026-07-18T20:30:00+02:00"


def test_time_edit_with_date_keeps_utc_offset():
    out = ne.apply_time_edit("2026-07-18T19:00:00+00:00", 5, 0, "01/08/2026")
    assert out == "2026-08-01T05:00:00+00:00"


def test_time_edit_handles_offsetless_legacy_value():
    out = ne.apply_time_edit("2026-07-18T19:00:00", 6, 15, None)
    assert out == "2026-07-18T06:15:00"
