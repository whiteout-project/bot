"""Per-server event reference dates: servers on a different rotation can
override an event's reference date, shifting every computed occurrence.
"""
import importlib
import sqlite3
from datetime import datetime, timedelta

import pytz

net = importlib.import_module("cogs.notification_event_types")

FROM = datetime(2026, 1, 15, 10, 30, tzinfo=pytz.UTC)


def _override_db(tmp_path, monkeypatch):
    db = tmp_path / "beartime.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE event_reference_overrides (
        guild_id INTEGER, event_type TEXT, reference_date TEXT,
        PRIMARY KEY (guild_id, event_type))""")
    conn.commit()
    conn.close()
    monkeypatch.setattr(net, "_OVERRIDES_DB", str(db))
    return db


def test_override_shifts_occurrence(tmp_path, monkeypatch):
    _override_db(tmp_path, monkeypatch)
    cfg = net.get_event_config("Foundry Battle")
    default_next = net.calculate_next_occurrence("Foundry Battle", FROM)

    # Shift the rotation by one week (still the same weekday).
    shifted_ref = (
        datetime.strptime(cfg["reference_date"], "%Y-%m-%d") + timedelta(weeks=1)
    ).strftime("%Y-%m-%d")
    net.set_reference_override(9, "Foundry Battle", shifted_ref)

    overridden = net.calculate_next_occurrence("Foundry Battle", FROM, guild_id=9)
    assert overridden != default_next
    assert abs((overridden - default_next).days) == 7

    # Other guilds and guildless callers keep the default rotation.
    assert net.calculate_next_occurrence("Foundry Battle", FROM, guild_id=8) == default_next
    assert net.calculate_next_occurrence("Foundry Battle", FROM) == default_next


def test_clearing_override_restores_default(tmp_path, monkeypatch):
    _override_db(tmp_path, monkeypatch)
    cfg = net.get_event_config("Foundry Battle")
    shifted_ref = (
        datetime.strptime(cfg["reference_date"], "%Y-%m-%d") + timedelta(weeks=1)
    ).strftime("%Y-%m-%d")
    net.set_reference_override(9, "Foundry Battle", shifted_ref)
    assert net.get_reference_override(9, "Foundry Battle") == shifted_ref

    net.set_reference_override(9, "Foundry Battle", None)
    assert net.get_reference_override(9, "Foundry Battle") is None
    assert net.calculate_next_occurrence("Foundry Battle", FROM, guild_id=9) == \
        net.calculate_next_occurrence("Foundry Battle", FROM)


def test_crazy_joe_respects_override(tmp_path, monkeypatch):
    _override_db(tmp_path, monkeypatch)
    cfg = net.get_event_config("Crazy Joe")
    default_tue, _ = net.calculate_crazy_joe_dates(FROM)
    shifted_ref = (
        datetime.strptime(cfg["reference_date"], "%Y-%m-%d") + timedelta(weeks=1)
    ).strftime("%Y-%m-%d")
    net.set_reference_override(9, "Crazy Joe", shifted_ref)

    tue, thu = net.calculate_crazy_joe_dates(FROM, guild_id=9)
    assert tue != default_tue
    assert thu == tue + timedelta(days=2)


def test_missing_table_degrades_to_default(tmp_path, monkeypatch):
    monkeypatch.setattr(net, "_OVERRIDES_DB", str(tmp_path / "empty.sqlite"))
    assert net.get_reference_override(9, "Foundry Battle") is None
    assert net.calculate_next_occurrence("Foundry Battle", FROM, guild_id=9) == \
        net.calculate_next_occurrence("Foundry Battle", FROM)
