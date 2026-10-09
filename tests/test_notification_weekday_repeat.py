"""Weekday-repeat scheduling: today's later occurrence counts, and the local hour survives DST changes."""
import asyncio
import importlib
import sqlite3
from datetime import datetime
from types import SimpleNamespace

import pytz

ns = importlib.import_module("cogs.notification_system")

BERLIN = pytz.timezone("Europe/Berlin")
MON, TUE, WED, THU, FRI, SAT, SUN = range(7)


def _local(tz, *args):
    return tz.localize(datetime(*args))


def test_today_still_upcoming_is_picked():
    after = _local(BERLIN, 2026, 10, 7, 9, 0)  # Wednesday 09:00
    result = ns.next_weekday_time(after, {WED}, 18, 30, BERLIN)
    assert result == _local(BERLIN, 2026, 10, 7, 18, 30)


def test_today_already_passed_moves_to_next_week():
    after = _local(BERLIN, 2026, 10, 7, 19, 0)
    result = ns.next_weekday_time(after, {WED}, 18, 30, BERLIN)
    assert result == _local(BERLIN, 2026, 10, 14, 18, 30)


def test_just_sent_occurrence_is_not_rescheduled():
    sent = _local(BERLIN, 2026, 10, 7, 18, 30)
    result = ns.next_weekday_time(sent, {WED, FRI}, 18, 30, BERLIN)
    assert result == _local(BERLIN, 2026, 10, 9, 18, 30)


def test_spring_forward_keeps_local_hour():
    after = _local(BERLIN, 2026, 3, 27, 20, 0)  # Friday, CET; DST starts Sunday 29 March
    result = ns.next_weekday_time(after, {MON}, 20, 0, BERLIN)
    assert result.astimezone(BERLIN).replace(tzinfo=None) == datetime(2026, 3, 30, 20, 0)
    assert result.utcoffset().total_seconds() == 2 * 3600


def test_fall_back_keeps_local_hour():
    after = _local(BERLIN, 2026, 10, 23, 20, 0)  # Friday, CEST; DST ends Sunday 25 October
    result = ns.next_weekday_time(after, {SUN}, 20, 0, BERLIN)
    assert result.astimezone(BERLIN).replace(tzinfo=None) == datetime(2026, 10, 25, 20, 0)
    assert result.utcoffset().total_seconds() == 1 * 3600


def test_no_weekdays_returns_none():
    assert ns.next_weekday_time(_local(BERLIN, 2026, 10, 7, 9, 0), set(), 18, 30, BERLIN) is None


def test_fixed_offset_timezone():
    tz = ns.get_timezone("UTC+05:30")
    after = tz.localize(datetime(2026, 10, 7, 9, 0))
    assert ns.next_weekday_time(after, {WED}, 18, 30, tz) == tz.localize(datetime(2026, 10, 7, 18, 30))


def _mk_cog():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE bear_notifications (id INTEGER PRIMARY KEY, next_notification TEXT)")
    conn.execute("CREATE TABLE notification_days (notification_id INTEGER, weekday TEXT)")
    cog = ns.NotificationSystem.__new__(ns.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    cog.bot = SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None)
    return cog


def _frozen_datetime(fixed):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    return _Frozen


def test_catch_up_after_downtime_picks_today(monkeypatch):
    now = _local(BERLIN, 2026, 10, 7, 9, 0)  # Wednesday morning
    monkeypatch.setattr(ns, "datetime", _frozen_datetime(now))
    cog = _mk_cog()
    missed = _local(BERLIN, 2026, 10, 5, 18, 30)  # Monday's send, missed while offline
    cog.cursor.execute("INSERT INTO bear_notifications VALUES (1, ?)", (missed.isoformat(),))
    cog.cursor.execute("INSERT INTO notification_days VALUES (1, ?)", (f"{MON}|{WED}",))
    row = (1, 10, 20, 18, 30, "Europe/Berlin", "Bear", 5, "none", 1, -1,
           1, None, 1, None, missed.isoformat(), "Bear Trap", None, None)

    asyncio.run(cog.process_notification(row))

    stored = cog.cursor.execute("SELECT next_notification FROM bear_notifications WHERE id = 1").fetchone()[0]
    assert datetime.fromisoformat(stored) == _local(BERLIN, 2026, 10, 7, 18, 30)
