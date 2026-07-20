"""Event date / cycle / schedule math.

The most-fixed area historically (Crazy Joe / Mercenary cycle math, weekday
sentinels, timezone parsing). These are pure functions of a frozen `from_date`,
so they're table-driven and fast — no Discord, no DB.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta

import pytest
import pytz

from cogs.notification_event_types import (
    EVENT_CONFIG,
    calculate_crazy_joe_dates,
    calculate_next_occurrence,
    get_event_config,
    round_to_5min_slot,
    validate_time_slot,
)
from cogs.minister_schedule import MinisterSchedule
from cogs.notification_schedule import NotificationSchedule

# Fixed reference instant (a Thursday, well after every event reference_date).
FROM = datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)
HHMM = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")

# Weekday each event's config explicitly names (Mon=0 … Sun=6).
NAMED_WEEKDAY = {
    "Crazy Joe": 1,       # Tuesday
    "Foundry Battle": 6,  # Sunday
    "Canyon Clash": 5,    # Saturday
    "Frostfire Mine": 1,  # Tuesday
    "Castle Battle": 5,   # Saturday
    "SvS": 5,             # Saturday
}

# Schedule types that key off a reference_date + cycle_weeks.
CYCLE_TYPES = {
    "global_biweekly", "global_monthly", "global_4weekly",
    "global_4weekly_alt", "global_4weekly_multiday", "global_3weekly_multiday",
}


def _reference(cfg) -> datetime:
    return pytz.UTC.localize(datetime.strptime(cfg["reference_date"], "%Y-%m-%d"))


# --- reference_date integrity (catches the Crazy-Joe-style "wrong date" class) ---

@pytest.mark.parametrize("event, weekday", sorted(NAMED_WEEKDAY.items()))
def test_reference_date_falls_on_named_weekday(event, weekday):
    cfg = get_event_config(event)
    assert _reference(cfg).weekday() == weekday, (
        f"{event} reference_date {cfg['reference_date']} is not the claimed weekday"
    )


def test_every_reference_date_parses():
    for name, cfg in EVENT_CONFIG.items():
        if "reference_date" in cfg:
            # Must not raise.
            _reference(cfg)


# --- calculate_next_occurrence ---

def test_custom_schedule_has_no_auto_occurrence():
    assert calculate_next_occurrence("Bear Trap", FROM) is None  # alliance-defined


def test_unknown_event_returns_none():
    assert calculate_next_occurrence("Not A Real Event", FROM) is None


def test_daily_reset_is_next_midnight():
    nxt = calculate_next_occurrence("Daily Reset", FROM)
    assert nxt == datetime(2026, 1, 16, 0, 0, tzinfo=pytz.UTC)


def test_weekly_event_lands_on_friday_and_is_future():
    nxt = calculate_next_occurrence("Fortress Battle", FROM)  # "Every Friday"
    assert nxt is not None and nxt > FROM
    assert nxt.weekday() == 4  # Friday


@pytest.mark.parametrize(
    "event",
    sorted(n for n, c in EVENT_CONFIG.items() if c.get("schedule_type") in CYCLE_TYPES),
)
def test_cycle_event_is_future_and_aligned(event):
    cfg = get_event_config(event)
    nxt = calculate_next_occurrence(event, FROM)
    assert nxt is not None, f"{event} returned no occurrence"
    # Strictly in the future …
    assert nxt > FROM
    ref = _reference(cfg)
    cycle_days = cfg["cycle_weeks"] * 7 if cfg.get("cycle_weeks") else 28
    # … aligned to reference + N whole cycles …
    assert (nxt - ref).days % cycle_days == 0
    # … and on the same weekday as the reference.
    assert nxt.weekday() == ref.weekday()


def test_cycle_event_same_day_is_not_skipped():
    """Occurrences are midnight-anchored - asking ON event day (before the
    slots have run) must return today, not a full cycle later."""
    cfg = get_event_config("Foundry Battle")  # global_biweekly, cycle 2
    ref = _reference(cfg)
    event_day = ref + timedelta(weeks=cfg["cycle_weeks"] * 5)
    asked = event_day.replace(hour=8, minute=0)
    nxt = calculate_next_occurrence("Foundry Battle", asked)
    assert nxt.date() == event_day.date(), "event day itself must not be skipped"


def test_cycle_event_steps_one_cycle_after_event_day():
    """The day after an occurrence, the next one is exactly one cycle on."""
    cfg = get_event_config("Foundry Battle")
    ref = _reference(cfg)
    event_day = ref + timedelta(weeks=cfg["cycle_weeks"] * 5)
    asked = event_day + timedelta(days=1)
    nxt = calculate_next_occurrence("Foundry Battle", asked)
    assert nxt == event_day + timedelta(weeks=cfg["cycle_weeks"])


def test_crazy_joe_same_day_tuesday_kept():
    tue_ref, _ = calculate_crazy_joe_dates(FROM)
    asked = tue_ref.replace(hour=6, minute=0)
    tue, thu = calculate_crazy_joe_dates(asked)
    assert tue.date() == tue_ref.date(), "Crazy Joe Tuesday must survive same-day setup"
    assert thu == tue + timedelta(days=2)


# --- Crazy Joe two-day cycle ---

def test_crazy_joe_tuesday_thursday_pairing():
    tue, thu = calculate_crazy_joe_dates(FROM)
    assert tue is not None and thu is not None
    assert tue > FROM
    assert tue.weekday() == 1        # Tuesday
    assert thu.weekday() == 3        # Thursday
    assert (thu - tue).days == 2     # Thursday is two days after Tuesday
    ref = _reference(get_event_config("Crazy Joe"))
    assert (tue - ref).days % 28 == 0  # 4-week cycle


# --- validate_time_slot ---

@pytest.mark.parametrize("value, ok", [
    ("14:05", True), ("00:00", True), ("23:55", True),
    ("14:03", False),    # not a 5-min increment
    ("24:00", False),    # hour out of range
    ("12:60", False),    # minute out of range
    ("abc", False), ("", False), ("14", False), ("14:5", True),  # "14:5" -> 5 min, %5==0
])
def test_validate_time_slot_5min(value, ok):
    assert validate_time_slot(value, "5min") is ok


def test_validate_time_slot_any_allows_non_5min():
    assert validate_time_slot("14:03", "any") is True
    assert validate_time_slot("24:00", "any") is False  # range still enforced


# --- round_to_5min_slot ---

@pytest.mark.parametrize("minute, expected", [(0, 0), (4, 0), (5, 5), (7, 5), (12, 10), (59, 55)])
def test_round_to_5min_slot(minute, expected):
    out = round_to_5min_slot(datetime(2026, 1, 1, 10, minute, 33, 123))
    assert out.minute == expected and out.second == 0 and out.microsecond == 0


# --- minister time slots ---

def test_minister_slots_mode0_is_48_half_hours():
    slots = MinisterSchedule.get_time_slots(None, 0)
    assert len(slots) == 48
    assert slots[0] == "00:00" and slots[-1] == "23:30"
    assert all(HHMM.match(s) for s in slots)
    assert all(int(s[3:]) in (0, 30) for s in slots)
    assert len(set(slots)) == len(slots)  # unique


def test_minister_slots_mode1_offset():
    slots = MinisterSchedule.get_time_slots(None, 1)
    assert len(slots) == 49
    assert slots[0] == "00:00" and "23:45" in slots
    assert all(HHMM.match(s) for s in slots)
    assert len(set(slots)) == len(slots)


# --- timezone parsing ---

@pytest.mark.parametrize("tz, minutes", [
    ("UTC", 0),
    ("UTC+05:30", 330),
    ("UTC-02:00", -120),
])
def test_timezone_object_offsets(tz, minutes):
    obj = NotificationSchedule._get_timezone_object(None, tz)
    off = obj.utcoffset(datetime(2026, 1, 1))
    assert off.total_seconds() / 60 == minutes


def test_timezone_object_etc_gmt_is_inverted():
    # Etc/GMT-3 is actually UTC+3.
    obj = NotificationSchedule._get_timezone_object(None, "Etc/GMT-3")
    assert obj.utcoffset(datetime(2026, 1, 1)).total_seconds() / 3600 == 3


@pytest.mark.parametrize("zone, shown", [
    ("UTC", "UTC"),
    ("Etc/GMT-3", "UTC+3"),
    ("Etc/GMT+5", "UTC-5"),
    ("UTC+05:30", "UTC+5:30"),
])
def test_timezone_display(zone, shown):
    assert NotificationSchedule._format_timezone_display(None, zone) == shown
