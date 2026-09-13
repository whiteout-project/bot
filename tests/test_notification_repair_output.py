"""One-time repairs announce themselves only when they actually changed rows.

A repair that reports "0 rows" on every fresh install is console noise that
buries the startup output admins actually need to read.
"""
import importlib
import sqlite3

ns = importlib.import_module("cogs.notification_system")
net = importlib.import_module("cogs.notification_event_types")

PHASE_EVENT = "SvS" if "SvS" in net.EVENT_CONFIG else "KvK"
PHASE = next(iter(net.get_instance_defaults(PHASE_EVENT)))
WEEK = 7 * 24 * 60


def _mk_cog():
    conn = sqlite3.connect(":memory:")
    conn.execute("""CREATE TABLE bear_notifications (
        id INTEGER PRIMARY KEY, event_type TEXT, instance_identifier TEXT,
        repeat_minutes INTEGER)""")
    conn.execute("""CREATE TABLE bear_notification_embeds (
        id INTEGER PRIMARY KEY AUTOINCREMENT, notification_id INTEGER, description TEXT)""")
    conn.commit()

    cog = ns.NotificationSystem.__new__(ns.NotificationSystem)
    cog.conn = conn
    cog.cursor = conn.cursor()
    return cog


def test_instance_descriptions_repair_is_silent_when_nothing_to_fix(capsys):
    _mk_cog()._apply_instance_descriptions()

    assert capsys.readouterr().out == "", "a no-op repair must not print on every fresh install"


def test_instance_descriptions_repair_reports_when_it_changes_rows(capsys):
    cog = _mk_cog()
    generic = net.EVENT_CONFIG[PHASE_EVENT]["description"]
    cog.cursor.execute(
        "INSERT INTO bear_notifications (id, event_type, instance_identifier) VALUES (1, ?, ?)",
        (PHASE_EVENT, PHASE))
    cog.cursor.execute(
        "INSERT INTO bear_notification_embeds (notification_id, description) VALUES (1, ?)",
        (generic,))
    cog.conn.commit()

    cog._apply_instance_descriptions()

    assert "1" in capsys.readouterr().out
    stored = cog.cursor.execute(
        "SELECT description FROM bear_notification_embeds WHERE notification_id = 1").fetchone()[0]
    assert stored == net.get_instance_defaults(PHASE_EVENT)[PHASE]


def test_biweekly_repair_is_silent_when_nothing_to_fix(capsys):
    _mk_cog()._realign_biweekly_cycle()

    assert capsys.readouterr().out == ""


def test_biweekly_repair_reports_when_it_changes_rows(capsys):
    cog = _mk_cog()
    event = "Frostfire Mine" if "Frostfire Mine" in net.EVENT_CONFIG else "Eternity's Reach"
    cog.cursor.execute(
        "INSERT INTO bear_notifications (id, event_type, repeat_minutes) VALUES (1, ?, ?)",
        (event, 4 * WEEK))
    cog.conn.commit()

    cog._realign_biweekly_cycle()

    assert "1" in capsys.readouterr().out
