"""Reload-gap regression: a hot cog reload (reload_extension) reruns setup()
but never re-fires on_ready, so the processor on_ready normally starts would
never run and the queue wedges. cog_load must start it when the bot is already
connected (a reload), and stay a no-op on a cold start (on_ready handles that).
"""
import asyncio
import importlib
import sqlite3
import types

pqmod = importlib.import_module("cogs.process_queue")


def _mk_pq(tmp_path):
    pq = pqmod.ProcessQueue.__new__(pqmod.ProcessQueue)  # bypass __init__ (no bot/DB file)
    conn = sqlite3.connect(tmp_path / "settings.sqlite")
    conn.execute(
        "CREATE TABLE process_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', priority INTEGER NOT NULL, "
        "alliance_id INTEGER, details TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, "
        "completed_at TEXT)"
    )
    conn.commit()
    pq.conn = conn
    pq.cursor = conn.cursor()
    pq._handlers = {}
    pq._processor_task = None
    pq._shutting_down = False
    pq._current_process = None
    pq._runtime_contexts = {}
    pq.HANDLER_GRACE_SECONDS = 0  # skip the handler-registration wait in tests
    return pq


async def _stop(pq):
    pq._shutting_down = True
    if pq._wake_event:
        pq._wake_event.set()
    if pq._processor_task:
        pq._processor_task.cancel()
        try:
            await pq._processor_task
        except BaseException:
            pass


def test_cog_load_starts_processor_when_bot_ready(tmp_path):
    pq = _mk_pq(tmp_path)
    pq.bot = types.SimpleNamespace(is_ready=lambda: True)

    async def run():
        pq._wake_event = asyncio.Event()
        await pq.cog_load()
        for _ in range(200):  # ensure runs in a background task; wait for it
            if pq._processor_task is not None and not pq._processor_task.done():
                break
            await asyncio.sleep(0.01)
        assert pq._processor_task is not None and not pq._processor_task.done()
        await _stop(pq)

    asyncio.run(run())


def test_cog_load_noop_when_bot_not_ready(tmp_path):
    pq = _mk_pq(tmp_path)
    pq.bot = types.SimpleNamespace(is_ready=lambda: False)

    async def run():
        pq._wake_event = asyncio.Event()
        await pq.cog_load()
        await asyncio.sleep(0.05)
        assert pq._processor_task is None  # left for on_ready to start
        await _stop(pq)

    asyncio.run(run())
