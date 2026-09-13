"""The mine event runs every 2 weeks, not every 4.

It shipped on a 4-week cycle, so computed dates, the repeat interval the wizard
stores, and notifications already created all skipped every other occurrence.
"""
import importlib
import json
import sqlite3
from datetime import datetime, timedelta

import pytz

net = importlib.import_module("cogs.notification_event_types")
ns = importlib.import_module("cogs.notification_system")
templates = importlib.import_module("cogs.notification_templates")

EVENT = "Frostfire Mine" if "Frostfire Mine" in net.EVENT_CONFIG else "Eternity's Reach"
FOUR_WEEKLY = "Castle Battle"
FROM = datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)
WEEK = 7 * 24 * 60


def test_event_config_is_a_two_week_cycle():
    cfg = net.get_event_config(EVENT)
    assert cfg["cycle_weeks"] == 2
    assert cfg["schedule_type"] == "global_biweekly"


def test_consecutive_occurrences_are_two_weeks_apart():
    first = net.calculate_next_occurrence(EVENT, FROM)
    second = net.calculate_next_occurrence(EVENT, first + timedelta(days=1))
    assert (second - first).days == 14


def test_occurrences_stay_on_the_reference_weekday():
    cfg = net.get_event_config(EVENT)
    reference = pytz.UTC.localize(datetime.strptime(cfg["reference_date"], "%Y-%m-%d"))
    nxt = net.calculate_next_occurrence(EVENT, FROM)
    assert nxt.weekday() == reference.weekday() == 1  # Tuesday
    assert (nxt - reference).days % 14 == 0


def test_cycle_repeat_minutes_follows_each_events_own_cycle():
    assert net.cycle_repeat_minutes(EVENT) == 2 * WEEK
    assert net.cycle_repeat_minutes(FOUR_WEEKLY) == 4 * WEEK
    assert net.cycle_repeat_minutes("Bear Trap") is None  # alliance-defined, no fixed cycle


def test_template_default_repeat_is_a_two_week_interval():
    cfg = net.get_event_config(EVENT)
    repeat = json.loads(templates.build_default_repeat_config(EVENT, cfg))
    assert repeat == {"type": "interval", "minutes": 2 * WEEK}


def _mk_cog():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE bear_notifications (
        id INTEGER PRIMARY KEY, event_type TEXT, repeat_minutes INTEGER)""")
    conn.commit()

    cog = ns.NotificationSystem.__new__(ns.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    return cog


def _add(cog, notif_id, event_type, repeat_minutes):
    cog.cursor.execute(
        "INSERT INTO bear_notifications (id, event_type, repeat_minutes) VALUES (?,?,?)",
        (notif_id, event_type, repeat_minutes))
    cog.conn.commit()


def _repeat(cog, notif_id):
    return cog.cursor.execute(
        "SELECT repeat_minutes FROM bear_notifications WHERE id = ?", (notif_id,)).fetchone()[0]


def test_repair_corrects_notifications_left_on_the_four_week_repeat():
    cog = _mk_cog()
    _add(cog, 1, EVENT, 4 * WEEK)

    cog._realign_biweekly_cycle()

    assert _repeat(cog, 1) == 2 * WEEK


def test_repair_leaves_genuinely_four_weekly_events_alone():
    cog = _mk_cog()
    _add(cog, 1, FOUR_WEEKLY, 4 * WEEK)

    cog._realign_biweekly_cycle()

    assert _repeat(cog, 1) == 4 * WEEK


def test_repair_leaves_other_intervals_of_the_same_event_alone():
    cog = _mk_cog()
    _add(cog, 1, EVENT, 3 * WEEK)  # hand-picked by an admin, not the shipped default

    cog._realign_biweekly_cycle()

    assert _repeat(cog, 1) == 3 * WEEK


def test_repair_runs_only_once():
    cog = _mk_cog()
    _add(cog, 1, EVENT, 4 * WEEK)
    cog._realign_biweekly_cycle()

    # An admin deliberately choosing 4 weeks afterwards must survive a restart.
    cog.cursor.execute("UPDATE bear_notifications SET repeat_minutes = ? WHERE id = 1", (4 * WEEK,))
    cog.conn.commit()
    cog._realign_biweekly_cycle()

    assert _repeat(cog, 1) == 4 * WEEK
