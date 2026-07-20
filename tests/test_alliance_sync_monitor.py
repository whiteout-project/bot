"""The sync scheduler must survive settings rows with a NULL interval.

Setting only a Sync Log / Redemption Log channel creates an alliancesettings
row with interval NULL; `None > 0` raised TypeError in the minute-monitor,
aborting the whole iteration - every alliance after the bad row stopped
responding to schedule changes, silently, forever.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

asy = importlib.import_module("cogs.alliance_sync")


def test_monitor_survives_null_interval_row():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, channel_id INTEGER, interval INTEGER, start_time TEXT)")
    conn.execute("INSERT INTO alliancesettings VALUES (1, 10, NULL, NULL)")
    conn.execute("INSERT INTO alliancesettings VALUES (2, 20, 60, NULL)")
    conn.commit()

    cog = asy.AllianceSync.__new__(asy.AllianceSync)
    cog.cursor_alliance = conn.cursor()
    cog.db_lock = asyncio.Lock()
    cog.alliance_tasks = {}
    cog.current_task_settings = {}
    cog.is_running = {}
    errors = []
    cog.logger = SimpleNamespace(
        info=lambda *a: None, warning=lambda *a: None,
        error=lambda m: errors.append(m),
    )
    cog.bot = SimpleNamespace(get_channel=lambda cid: object())

    started = []

    async def fake_check(alliance_id):
        started.append(alliance_id)

    cog.schedule_alliance_check = fake_check

    async def run():
        await asy.AllianceSync.monitor_alliance_changes.coro(cog)
        for t in cog.alliance_tasks.values():
            t.cancel()

    asyncio.run(run())

    assert errors == [], f"NULL interval must not abort the monitor: {errors}"
    assert 2 in cog.alliance_tasks, "alliances after the NULL row must still get their task"
    assert 1 not in cog.alliance_tasks, "NULL interval means sync disabled"
