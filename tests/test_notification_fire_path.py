"""process_notification fire-path regressions.

Dedupe: the 30s duplicate-send check must compare UTC against UTC. It used the
notification's own timezone for the threshold while sent_at is stored in UTC,
so UTC+ timezones re-sent the same pre-warning dozens of times per due window
and UTC- timezones silently suppressed legitimate repeats.

Forbidden: losing send permission on a still-cached channel must route into
the pause/DM quarantine machinery instead of retrying every 0.1s forever with
no admin feedback.
"""
import asyncio
import importlib
import sqlite3
from datetime import datetime, timedelta
from types import SimpleNamespace

import discord
import pytz

ns = importlib.import_module("cogs.notification_system")


def _forbidden():
    resp = SimpleNamespace(status=403, reason="Forbidden")
    return discord.Forbidden(resp, "Missing Access")


class _Chan:
    def __init__(self, fail=False):
        self.sent = []
        self.attempts = 0
        self._fail = fail
        self.guild = None

    async def send(self, *args, **kwargs):
        self.attempts += 1
        if self._fail:
            raise _forbidden()
        self.sent.append(args or kwargs)
        return SimpleNamespace(id=self.attempts)


def _mk_cog(channel):
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE notification_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, notification_id INTEGER,
        notification_time INTEGER, message_id INTEGER, channel_id INTEGER,
        scheduled_delete_at TEXT, sent_at TEXT, deleted_at TEXT)""")
    conn.execute("""CREATE TABLE bear_notifications (
        id INTEGER PRIMARY KEY, is_enabled INTEGER DEFAULT 1,
        last_notification TEXT, next_notification TEXT, auto_disabled_at TEXT)""")
    conn.commit()

    cog = ns.NotificationSystem.__new__(ns.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    cog.bot = SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None)
    cog.channel_confirm_state = {}
    cog.send_forbidden_state = {}
    cog.CHANNEL_CONFIRM_INTERVAL = 0
    cog.CHANNEL_CONFIRM_REQUIRED = 3
    cog.calculate_delete_time = lambda *a, **k: None
    pauses = []

    async def resolve(cid):
        return channel

    async def pause(**kw):
        pauses.append(kw)

    cog._resolve_send_channel = resolve
    cog._pause_and_notify = pause
    return cog, pauses


def _row(tz_name, minutes_ahead=30):
    tz = pytz.timezone(tz_name)
    next_at = (datetime.now(tz) + timedelta(minutes=minutes_ahead)).isoformat()
    # (id, guild_id, channel_id, hour, minute, timezone, description, type,
    #  mention, repeat_enabled, repeat_minutes, is_enabled, created_at,
    #  created_by, last_notification, next_notification, event_type,
    #  instance_identifier, custom_delete_delay_minutes)
    return (1, 10, 20, 12, 0, tz_name, "Test event", 1, "none", 0, 0, 1,
            None, None, None, next_at, None, None, None)


def test_dedupe_suppresses_duplicate_in_non_utc_timezone():
    chan = _Chan()
    cog, _ = _mk_cog(chan)
    cog.cursor.execute("INSERT INTO bear_notifications (id) VALUES (1)")
    cog.conn.commit()

    row = _row("Europe/Berlin")
    asyncio.run(cog.process_notification(row))
    asyncio.run(cog.process_notification(row))

    assert len(chan.sent) == 1, "second pass inside 30s must be deduped"


def test_dedupe_does_not_block_after_window_in_utc_minus_timezone():
    chan = _Chan()
    cog, _ = _mk_cog(chan)
    cog.cursor.execute("INSERT INTO bear_notifications (id) VALUES (1)")
    # A record from 40s ago (outside the 30s window) must not suppress the send.
    old = (datetime.now(pytz.UTC) - timedelta(seconds=40)).strftime('%Y-%m-%d %H:%M:%S')
    cog.cursor.execute(
        "INSERT INTO notification_history (notification_id, notification_time, sent_at)"
        " VALUES (1, 30, ?)", (old,))
    cog.conn.commit()

    asyncio.run(cog.process_notification(_row("America/New_York")))

    assert len(chan.sent) == 1, "a 40s-old record must not suppress the send"


def test_forbidden_send_pauses_after_confirmations():
    chan = _Chan(fail=True)
    cog, pauses = _mk_cog(chan)
    cog.cursor.execute("INSERT INTO bear_notifications (id) VALUES (1)")
    cog.conn.commit()

    row = _row("UTC")
    for _ in range(3):
        asyncio.run(cog.process_notification(row))

    assert pauses, "repeated Forbidden sends must pause via quarantine"
    assert pauses[0]["channel_id"] == 20


def test_forbidden_send_throttles_retry_hammering():
    chan = _Chan(fail=True)
    cog, _ = _mk_cog(chan)
    cog.CHANNEL_CONFIRM_INTERVAL = 300
    cog.cursor.execute("INSERT INTO bear_notifications (id) VALUES (1)")
    cog.conn.commit()

    row = _row("UTC")
    asyncio.run(cog.process_notification(row))
    asyncio.run(cog.process_notification(row))

    assert chan.attempts == 1, "a recent confirmed Forbidden must skip re-sends"
