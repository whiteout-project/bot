"""The wizard's Bear Trap custom-weekday answer must produce a weekday repeat.

Answering no to repeat-every-2-days opens a weekday picker whose selection was
stored and never read - the notification was created one-shot (repeat 0) and
auto-disabled after firing. The update path additionally had to sync
repeat_enabled and clean up notification_days when switching modes, or the
startup repair migration flipped the notification back to weekday mode.
"""
import asyncio
import importlib
import sqlite3
from datetime import datetime
from types import SimpleNamespace

import pytz

wz = importlib.import_module("cogs.notification_wizard")
nsys = importlib.import_module("cogs.notification_system")


def _mk_system_cog():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE bear_notifications (
        id INTEGER PRIMARY KEY, guild_id INTEGER, channel_id INTEGER,
        hour INTEGER, minute INTEGER, timezone TEXT, description TEXT,
        notification_type INTEGER, mention_type TEXT, repeat_enabled INTEGER,
        repeat_minutes INTEGER, is_enabled INTEGER DEFAULT 1,
        next_notification TEXT, event_type TEXT, instance_identifier TEXT)""")
    conn.execute("CREATE TABLE notification_days (notification_id INTEGER, weekday TEXT)")
    conn.commit()
    cog = nsys.NotificationSystem.__new__(nsys.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    cog.bot = SimpleNamespace(get_cog=lambda name: None)
    return cog


def _update(cog, repeat_minutes, selected_weekdays=None):
    return asyncio.run(cog.update_notification(
        notification_id=1, hour=20, minute=0, timezone="UTC", description="d",
        notification_type=1, mention_type="none", repeat_minutes=repeat_minutes,
        selected_weekdays=selected_weekdays, skip_board_update=True,
        start_date=datetime(2026, 7, 27, tzinfo=pytz.UTC),
    ))


def test_update_to_weekday_mode_enables_repeat():
    cog = _mk_system_cog()
    cog.conn.execute("INSERT INTO bear_notifications (id, repeat_enabled, repeat_minutes) VALUES (1, 0, 0)")
    cog.conn.commit()

    assert _update(cog, -1, [0, 2]) is True

    enabled, minutes = cog.conn.execute(
        "SELECT repeat_enabled, repeat_minutes FROM bear_notifications WHERE id = 1").fetchone()
    assert (enabled, minutes) == (1, -1), "weekday mode must enable repeat on update"
    days = cog.conn.execute("SELECT weekday FROM notification_days WHERE notification_id = 1").fetchall()
    assert days == [("0|2",)]


def test_update_away_from_weekday_mode_cleans_up():
    cog = _mk_system_cog()
    cog.conn.execute("INSERT INTO bear_notifications (id, repeat_enabled, repeat_minutes) VALUES (1, 1, -1)")
    cog.conn.execute("INSERT INTO notification_days VALUES (1, '0|2')")
    cog.conn.commit()

    assert _update(cog, 0) is True

    enabled, minutes = cog.conn.execute(
        "SELECT repeat_enabled, repeat_minutes FROM bear_notifications WHERE id = 1").fetchone()
    assert (enabled, minutes) == (0, 0), "no-repeat must disable repeat on update"
    days = cog.conn.execute("SELECT weekday FROM notification_days WHERE notification_id = 1").fetchall()
    assert days == [], "stale day rows would be flipped back by the startup migration"


def test_bear_trap_repeat_mapping():
    f = wz.WizardPreviewView._bear_trap_repeat
    assert f({"repeat_days": 2}) == (2 * 24 * 60, None)
    assert f({"repeat_days": None, "repeat_weekdays": [0, 2, 5]}) == (-1, [0, 2, 5])
    assert f({}) == (0, None)


def test_create_passes_weekdays_through():
    view = wz.WizardPreviewView.__new__(wz.WizardPreviewView)
    view.session = SimpleNamespace(
        original_instance_states={}, timezone="UTC", notification_type=1,
        mention_type="none", wizard_batch_id="b1", channel_id=77,
    )
    captured = {}

    async def save_notification(**kw):
        captured.update(kw)
        return 1

    cog = SimpleNamespace(save_notification=save_notification)
    inter = SimpleNamespace(guild_id=9, user=SimpleNamespace(id=42))

    asyncio.run(view._create_or_update_notification(
        cog, inter, "Bear Trap", "bt1", 20, 0,
        datetime(2026, 7, 20, tzinfo=pytz.UTC), -1, "desc", {},
        selected_weekdays=[0, 2],
    ))

    assert captured["selected_weekdays"] == [0, 2]
    assert captured["repeat_minutes"] == -1
    assert captured["repeat_enabled"] is True, "weekday repeat (-1) counts as repeating"
