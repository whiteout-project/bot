"""Hot cog reloads must re-wire queue handlers and loops, and a Discord outage at
startup must never delete gift code or ID channel config."""
import asyncio
import importlib
import logging
import sqlite3
import types

import discord

pqmod = importlib.import_module("cogs.process_queue")
go = importlib.import_module("cogs.gift_operations")
ops = importlib.import_module("cogs.alliance_member_operations")
id_channel = importlib.import_module("cogs.alliance_id_channel")
bot_health = importlib.import_module("cogs.bot_health")


def _http_error(cls, status):
    return cls(types.SimpleNamespace(status=status, reason="x"), "x")


def _queue(tmp_path, bot=None):
    pq = pqmod.ProcessQueue.__new__(pqmod.ProcessQueue)
    conn = sqlite3.connect(tmp_path / "settings.sqlite")
    conn.execute(
        "CREATE TABLE process_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', priority INTEGER NOT NULL, "
        "alliance_id INTEGER, details TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, "
        "completed_at TEXT)"
    )
    pq.bot = bot or types.SimpleNamespace(is_ready=lambda: False, cogs={})
    pq.conn = conn
    pq.cursor = conn.cursor()
    pq._handlers = {}
    pq._processor_task = None
    pq._wake_event = asyncio.Event()
    pq._current_process = None
    pq._current_started = None
    pq._cancel_requested = set()
    pq._unhandled_warned = set()
    pq._shutting_down = False
    pq._runtime_contexts = {}
    pq.HANDLER_GRACE_SECONDS = 0
    return pq


def _status(pq, pid):
    return pq.conn.execute("SELECT status FROM process_queue WHERE id = ?", (pid,)).fetchone()[0]


def test_job_without_handler_does_not_block_others(tmp_path):
    pq = _queue(tmp_path)
    orphan = pq.enqueue('gift_validate', pqmod.GIFT_VALIDATE)
    runnable = pq.enqueue('member_add', pqmod.MEMBER_ADD)
    ran = []

    async def handler(process):
        ran.append(process['id'])

    async def run():
        pq.register_handler('member_add', handler)
        task = asyncio.create_task(pq._processor_loop())
        for _ in range(100):
            await asyncio.sleep(0.01)
            if ran:
                break
        pq._shutting_down = True
        pq._wake_event.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run())
    assert ran == [runnable]
    assert _status(pq, orphan) == 'queued'
    assert orphan in pq._unhandled_warned


def test_unhandled_job_does_not_preempt_running_work(tmp_path):
    pq = _queue(tmp_path)
    pq.enqueue('gift_validate', pqmod.GIFT_VALIDATE)
    pq.register_handler('member_add', lambda p: None)
    assert pq.has_higher_priority_waiting(pqmod.MEMBER_ADD) is False
    pq.register_handler('gift_validate', lambda p: None)
    assert pq.has_higher_priority_waiting(pqmod.MEMBER_ADD) is True


def test_reloaded_queue_collects_handlers_from_sibling_cogs(tmp_path):
    seen = []
    sibling = types.SimpleNamespace(register_queue_handlers=lambda queue: seen.append(queue))
    bot = types.SimpleNamespace(is_ready=lambda: True, cogs={'Sibling': sibling, 'Other': object()})
    pq = _queue(tmp_path, bot)

    async def run():
        await pq.cog_load()
        pq._shutting_down = True
        await asyncio.sleep(0.05)

    asyncio.run(run())
    assert seen == [pq]


class _FakeLoop:
    def __init__(self):
        self.starts = 0

    def is_running(self):
        return self.starts > 0

    def start(self):
        self.starts += 1


class _FakeQueue:
    def __init__(self):
        self.actions = []

    def register_handler(self, action, handler):
        self.actions.append(action)


def _gift_cog(ready, queue):
    cog = go.GiftOperations.__new__(go.GiftOperations)
    cog.bot = types.SimpleNamespace(is_ready=lambda: ready, get_cog=lambda name: queue)
    cog.logger = logging.getLogger("test")
    cog.periodic_validation_loop = _FakeLoop()
    cog.auto_kingdom_scan_loop = _FakeLoop()
    return cog


def test_gift_cog_reload_restarts_loops_and_handlers_once():
    queue = _FakeQueue()
    cog = _gift_cog(True, queue)
    asyncio.run(cog.cog_load())
    cog._start_background_work()  # a later reconnect on_ready must not double-start loops
    assert cog.periodic_validation_loop.starts == 1
    assert cog.auto_kingdom_scan_loop.starts == 1
    assert {'gift_validate', 'gift_redeem', 'gift_redeem_member', 'state_resolve'} <= set(queue.actions)


def test_gift_cog_cold_start_waits_for_on_ready():
    queue = _FakeQueue()
    cog = _gift_cog(False, queue)
    asyncio.run(cog.cog_load())
    assert cog.periodic_validation_loop.starts == 0
    assert queue.actions == []


def test_member_ops_reload_registers_member_add():
    queue = _FakeQueue()
    cog = ops.AllianceMemberOperations.__new__(ops.AllianceMemberOperations)
    cog.bot = types.SimpleNamespace(is_ready=lambda: True, get_cog=lambda name: queue)
    asyncio.run(cog.cog_load())
    assert queue.actions == ['member_add']


def _gift_cog_with_channels(tmp_path, fetch_error):
    conn = sqlite3.connect(tmp_path / "giftcode.sqlite")
    conn.execute("CREATE TABLE giftcode_channel (alliance_id INTEGER, channel_id INTEGER)")
    conn.execute("INSERT INTO giftcode_channel VALUES (1, 111)")
    conn.commit()

    async def fetch_channel(channel_id):
        raise fetch_error

    cog = go.GiftOperations.__new__(go.GiftOperations)
    cog.bot = types.SimpleNamespace(get_channel=lambda cid: None, fetch_channel=fetch_channel)
    cog.logger = logging.getLogger("test")
    cog.conn = conn
    cog.cursor = conn.cursor()
    return cog


def test_gift_channel_kept_when_discord_is_down(tmp_path):
    cog = _gift_cog_with_channels(tmp_path, _http_error(discord.DiscordServerError, 503))
    asyncio.run(cog._prune_deleted_gift_channels())
    assert cog.conn.execute("SELECT COUNT(*) FROM giftcode_channel").fetchone()[0] == 1


def test_gift_channel_removed_when_discord_confirms_deletion(tmp_path):
    cog = _gift_cog_with_channels(tmp_path, _http_error(discord.NotFound, 404))
    asyncio.run(cog._prune_deleted_gift_channels())
    assert cog.conn.execute("SELECT COUNT(*) FROM giftcode_channel").fetchone()[0] == 0


def _id_channel_cog(guild, fetch_error):
    async def fetch_channel(channel_id):
        raise fetch_error

    cog = id_channel.AllianceIDChannel.__new__(id_channel.AllianceIDChannel)
    cog.bot = types.SimpleNamespace(get_guild=lambda gid: guild, fetch_channel=fetch_channel)
    return cog


def test_id_channel_kept_when_guild_unavailable():
    guild = types.SimpleNamespace(unavailable=True, get_channel=lambda cid: None)
    cog = _id_channel_cog(guild, _http_error(discord.NotFound, 404))
    assert asyncio.run(cog._channel_confirmed_deleted(1, 111)) is False


def test_id_channel_kept_when_guild_not_cached():
    cog = _id_channel_cog(None, _http_error(discord.NotFound, 404))
    assert asyncio.run(cog._channel_confirmed_deleted(1, 111)) is False


def test_id_channel_kept_when_lookup_fails():
    guild = types.SimpleNamespace(unavailable=False, get_channel=lambda cid: None)
    cog = _id_channel_cog(guild, _http_error(discord.Forbidden, 403))
    assert asyncio.run(cog._channel_confirmed_deleted(1, 111)) is False


def test_id_channel_deleted_only_on_confirmed_not_found():
    guild = types.SimpleNamespace(unavailable=False, get_channel=lambda cid: None)
    cog = _id_channel_cog(guild, _http_error(discord.NotFound, 404))
    assert asyncio.run(cog._channel_confirmed_deleted(1, 111)) is True


def test_onnx_lifecycle_is_never_hot_reloaded():
    reloaded = []

    async def reload_extension(name):
        reloaded.append(name)

    cog = bot_health.BotHealth.__new__(bot_health.BotHealth)
    cog.bot = types.SimpleNamespace(extensions={'cogs.onnx_lifecycle': None}, reload_extension=reload_extension)
    cog.logger = logging.getLogger("test")
    results = asyncio.run(cog.reload_cogs(["onnx_lifecycle"]))
    assert reloaded == []
    assert results['failed'] == ["onnx_lifecycle"]
