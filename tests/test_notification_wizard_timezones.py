import asyncio
import importlib
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
import pytz

from cogs.notification_event_types import (
    calculate_crazy_joe_dates,
    first_future_occurrence,
    get_timezone,
    utc_offset_zone_name,
)
from cogs.notification_schedule import NotificationSchedule

wz = importlib.import_module("cogs.notification_wizard")
nsys = importlib.import_module("cogs.notification_system")

FROM = datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)


# --- UTC-labeled events are stored as UTC ---

def _mk_system_cog():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE bear_notifications (
        id INTEGER PRIMARY KEY, guild_id INTEGER, channel_id INTEGER,
        hour INTEGER, minute INTEGER, timezone TEXT, description TEXT,
        notification_type INTEGER, mention_type TEXT, repeat_enabled INTEGER,
        repeat_minutes INTEGER, created_by INTEGER, is_enabled INTEGER DEFAULT 1,
        next_notification TEXT, event_type TEXT, wizard_batch_id TEXT,
        instance_identifier TEXT, channel_name TEXT)""")
    conn.commit()
    cog = nsys.NotificationSystem.__new__(nsys.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    cog.bot = SimpleNamespace(get_cog=lambda name: None, get_channel=lambda cid: None)
    return cog


def _create(event_name, instance_id, hour, start_date, session_tz):
    view = wz.WizardPreviewView.__new__(wz.WizardPreviewView)
    view.session = SimpleNamespace(
        original_instance_states={}, timezone=session_tz, notification_type=2,
        mention_type="none", wizard_batch_id="b1", channel_id=77,
    )
    cog = _mk_system_cog()
    inter = SimpleNamespace(guild_id=9, user=SimpleNamespace(id=42))
    asyncio.run(view._create_or_update_notification(
        cog, inter, event_name, instance_id, hour, 0, start_date,
        14 * 24 * 60, "EMBED_MESSAGE:x", {},
    ))
    return cog.conn.execute("SELECT timezone, next_notification FROM bear_notifications").fetchone()


def test_utc_labeled_event_ignores_session_timezone():
    start = datetime(2026, 10, 11, tzinfo=pytz.UTC)
    tz, next_notification = _create("Foundry Battle", "legion1", 12, start, "America/New_York")
    assert tz == "UTC"
    assert datetime.fromisoformat(next_notification) == datetime(2026, 10, 11, 12, 0, tzinfo=pytz.UTC)


def test_bear_trap_keeps_session_timezone():
    start = datetime(2026, 10, 11, tzinfo=pytz.UTC)
    tz, next_notification = _create("Bear Trap", "bt1", 12, start, "America/New_York")
    assert tz == "America/New_York"
    assert datetime.fromisoformat(next_notification).astimezone(pytz.UTC).hour == 16


# --- Fractional UTC offsets ---

@pytest.mark.parametrize("entered, stored", [
    ("UTC", "UTC"),
    ("UTC+3", "Etc/GMT-3"),
    ("UTC-5", "Etc/GMT+5"),
    ("UTC+5:30", "UTC+05:30"),
    ("UTC+5.5", "UTC+05:30"),
    ("UTC+5:45", "UTC+05:45"),
    ("UTC-3:30", "UTC-03:30"),
    ("UTC+0", "UTC"),
])
def test_offset_zone_name(entered, stored):
    assert utc_offset_zone_name(entered) == stored


@pytest.mark.parametrize("entered", ["UTC+15", "UTC-13", "UTC+5:60", "UTC+-5", "UTCX"])
def test_offset_zone_name_rejects(entered):
    with pytest.raises(ValueError):
        utc_offset_zone_name(entered)


@pytest.mark.parametrize("entered, minutes", [
    ("UTC+5:30", 330), ("UTC-3:30", -210), ("UTC+5:45", 345), ("UTC+3", 180), ("UTC-5", -300),
])
def test_stored_zone_is_readable_with_offset(entered, minutes):
    tz = get_timezone(utc_offset_zone_name(entered))
    local = tz.localize(datetime(2026, 1, 1, 12, 0))
    assert local.utcoffset() == timedelta(minutes=minutes)
    assert NotificationSchedule._get_timezone_object(None, utc_offset_zone_name(entered)).utcoffset(
        datetime(2026, 1, 1)) == timedelta(minutes=minutes)


def test_fractional_zone_saves_through_notification_system():
    cog = _mk_system_cog()
    asyncio.run(cog.save_notification(
        guild_id=9, channel_id=77, start_date=datetime(2026, 10, 11), hour=20, minute=0,
        timezone="UTC+05:30", description="d", created_by=1, notification_type=2,
        mention_type="none", repeat_enabled=False, skip_board_update=True,
    ))
    next_notification = cog.conn.execute("SELECT next_notification FROM bear_notifications").fetchone()[0]
    assert datetime.fromisoformat(next_notification) == datetime(2026, 10, 11, 14, 30, tzinfo=pytz.UTC)


# --- Bear Trap start date rolls forward instead of a year out ---

NOW = datetime(2026, 10, 9, 15, 0)  # a Friday


def test_future_start_is_kept():
    start = datetime(2026, 10, 10, 14, 0)
    assert first_future_occurrence(start, NOW, repeat_days=2) == start


def test_past_start_rolls_by_repeat_interval():
    start = datetime(2026, 10, 1, 14, 0)
    assert first_future_occurrence(start, NOW, repeat_days=2) == datetime(2026, 10, 11, 14, 0)


def test_past_start_same_day_later_time_kept_in_cycle():
    start = datetime(2026, 10, 7, 16, 0)
    assert first_future_occurrence(start, NOW, repeat_days=2) == datetime(2026, 10, 9, 16, 0)


def test_past_start_rolls_to_next_selected_weekday():
    start = datetime(2026, 10, 1, 14, 0)
    # Mon=0, Wed=2 -> next is Monday 12 Oct.
    assert first_future_occurrence(start, NOW, weekdays=[0, 2]) == datetime(2026, 10, 12, 14, 0)


def test_past_start_without_repeat_moves_to_next_year():
    start = datetime(2026, 10, 1, 14, 0)
    assert first_future_occurrence(start, NOW) == datetime(2027, 10, 1, 14, 0)


# --- Crazy Joe Thursday inside the current cycle ---

def _cycle_tuesday():
    tue, _ = calculate_crazy_joe_dates(FROM)
    return tue


@pytest.mark.parametrize("days_after_tuesday", [1, 2])
def test_crazy_joe_thursday_kept_mid_week(days_after_tuesday):
    cycle_tue = _cycle_tuesday()
    asked = cycle_tue + timedelta(days=days_after_tuesday, hours=20)
    tue, thu = calculate_crazy_joe_dates(asked)
    assert thu == cycle_tue + timedelta(days=2), "this week's Thursday must not be skipped"
    assert tue == cycle_tue + timedelta(weeks=4)


def test_crazy_joe_after_thursday_moves_both_to_next_cycle():
    cycle_tue = _cycle_tuesday()
    tue, thu = calculate_crazy_joe_dates(cycle_tue + timedelta(days=3))
    assert tue == cycle_tue + timedelta(weeks=4)
    assert thu == cycle_tue + timedelta(weeks=4, days=2)
