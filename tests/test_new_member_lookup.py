"""New members added through the ID channel or Add Members take their name, level and state from Player Sync."""
import asyncio
import importlib
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from harness import fake_player_api

id_channel = importlib.import_module("cogs.alliance_id_channel")
member_ops = importlib.import_module("cogs.alliance_member_operations")
sync = importlib.import_module("cogs.alliance_api_sync")
gift_state_resolver = importlib.import_module("cogs.gift_state_resolver")

API_PLAYER = {"nickname": "Delta", "furnace_lv": 45, "kid": 245, "power": 5000,
              "avatar_url": "https://i.example.com/9.png"}


def _alliance(kid=None, multistate=0, locked=0):
    with closing(sqlite3.connect("db/alliance.sqlite")) as conn:
        conn.execute("INSERT INTO alliance_list (alliance_id, name, kid, multistate, state_locked) "
                     "VALUES (1, 'A1', ?, ?, ?)", (kid, multistate, locked))
        conn.commit()


def _member(fid):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        row = conn.execute("SELECT nickname, furnace_lv, kid, power, avatar_url, api_synced_at "
                           "FROM users WHERE fid = ?", (fid,)).fetchone()
    return row


def _probe(monkeypatch, answer):
    probed = []

    async def verify(gift_cog, fid, alliance_id):
        probed.append(fid)
        return answer
    monkeypatch.setattr(gift_state_resolver, "verify_add_state", verify)
    return probed


# --- ID channel ------------------------------------------------------------------------

class _Message:
    def __init__(self, message_id=1, content=""):
        self.id, self.content = message_id, content
        self.reactions, self.replies, self.shown = [], [], []
        self.author = SimpleNamespace(id=7, bot=False)
        self.guild = SimpleNamespace(id=3)

    async def add_reaction(self, emoji):
        self.reactions.append(emoji)
        self.shown.append(emoji)

    async def remove_reaction(self, emoji, member):
        self.reactions.remove(emoji)

    async def reply(self, content=None, embed=None, **kwargs):
        self.replies.append(content if embed is None else embed.description)


def _id_channel_cog():
    cog = id_channel.AllianceIDChannel.__new__(id_channel.AllianceIDChannel)
    cog.bot = SimpleNamespace(get_cog=lambda name: object(), user=SimpleNamespace(id=99))
    cog.get_guild_settings = lambda guild_id: {"delete_after": 5, "scan_enabled": 1, "scan_limit": 50}
    cog._seen, cog._backlog_task = set(), None

    async def log_action(*args, **kwargs):
        pass
    cog.log_action = log_action
    return cog


def _post(fid, state=None, name=None, level=None):
    message = _Message()
    asyncio.run(_id_channel_cog().process_fid(message, fid, 1, state, given_name=name, given_level=level))
    return message


def test_id_channel_uses_the_api_and_skips_the_probe(dbs, monkeypatch):
    _alliance(kid=245)
    fake_player_api(monkeypatch, API_PLAYER)
    probed = _probe(monkeypatch, (245, True))
    message = _post(9)
    assert probed == []
    assert _member(9)[:5] == ("Delta", 45, 245, 5000, "https://i.example.com/9.png")
    assert _member(9)[5], "the new member is marked synced"
    assert "Delta" in message.replies[-1]


def test_id_channel_keeps_a_typed_name(dbs, monkeypatch):
    _alliance(kid=245)
    fake_player_api(monkeypatch, API_PLAYER)
    _post(9, name="Typed")
    assert _member(9)[:3] == ("Typed", 45, 245)


def test_id_channel_multistate_needs_no_state_when_the_api_has_one(dbs, monkeypatch):
    _alliance(multistate=1)
    fake_player_api(monkeypatch, API_PLAYER)
    _post(9)
    assert _member(9)[2] == 245


@pytest.mark.parametrize("available", [True, False])
def test_id_channel_multistate_without_an_api_state_still_asks(dbs, monkeypatch, available):
    _alliance(multistate=1)
    fake_player_api(monkeypatch, None, available=available)
    message = _post(9)
    assert _member(9) is None
    assert "state together" in message.replies[-1]


def test_id_channel_lock_uses_the_api_state(dbs, monkeypatch):
    _alliance(kid=100, locked=1)
    fake_player_api(monkeypatch, API_PLAYER)
    probed = _probe(monkeypatch, (100, True))
    message = _post(9)
    assert probed == [] and _member(9) is None
    assert "State #100" in message.replies[-1]


def test_id_channel_without_the_api_probes_and_uses_the_placeholder(dbs, monkeypatch):
    _alliance(kid=245)
    looked_up = fake_player_api(monkeypatch, API_PLAYER, available=False).asked
    probed = _probe(monkeypatch, (245, True))
    _post(9, level=30)
    assert looked_up == [] and probed == [9]
    assert _member(9) == ("Player 9", 30, 245, None, None, None)


def test_id_channel_api_without_the_player_probes(dbs, monkeypatch):
    _alliance(kid=245)
    fake_player_api(monkeypatch, None)
    probed = _probe(monkeypatch, (245, True))
    _post(9)
    assert probed == [9] and _member(9)[:3] == ("Player 9", 0, 245)


def test_id_channel_typed_state_refused_by_the_lock_names_the_api_state(dbs, monkeypatch):
    _alliance(kid=100, locked=1)
    fake_player_api(monkeypatch, API_PLAYER)
    message = _post(9, state=100)
    assert _member(9) is None
    assert message.replies[-1].endswith(
        "Player Sync shows this player in State #245. This alliance only accepts members from State #100.")


def test_id_channel_handles_a_post_once_while_it_is_in_flight(dbs, monkeypatch):
    import asyncio as aio
    _alliance(kid=245)
    looked_up = []

    async def slow_lookup(bot, fid):
        looked_up.append(fid)
        await aio.sleep(0.05)
        return sync.Lookup(dict(API_PLAYER))
    monkeypatch.setattr(sync, "lookup_via_bot", slow_lookup)
    cog, message = _id_channel_cog(), _Message()

    async def twice():
        await aio.gather(cog.process_fid(message, 9, 1), cog.process_fid(message, 9, 1))
        await cog.process_fid(message, 9, 1)
    asyncio.run(twice())

    assert looked_up == [9] and len(message.replies) == 1
    assert cog._seen == {message.id}


class _Channel:
    def __init__(self, messages):
        self.messages = messages

    async def history(self, **kwargs):
        for message in self.messages:
            yield message


def test_backlog_stops_asking_the_api_after_a_failure(dbs, monkeypatch):
    _alliance(kid=245)
    _probe(monkeypatch, (245, True))
    looked_up = []

    async def failing_lookup(bot, fid):
        looked_up.append(fid)
        return sync.Lookup(reason=sync.FAILED)
    monkeypatch.setattr(sync, "lookup_via_bot", failing_lookup)
    posts = [_Message(1, "9"), _Message(2, "8")]

    asyncio.run(_id_channel_cog().process_backlog([(_Channel(posts), 1, 3)]))

    assert looked_up == [9]
    assert _member(9)[:3] == ("Player 9", 0, 245) and _member(8)[:3] == ("Player 8", 0, 245)


def test_on_ready_starts_the_loop_before_the_backlog_finishes(dbs, monkeypatch):
    import asyncio as aio
    with closing(sqlite3.connect("db/id_channel.sqlite")) as conn:
        conn.execute("CREATE TABLE id_channels (channel_id INTEGER, alliance_id INTEGER, guild_id INTEGER)")
        conn.execute("INSERT INTO id_channels VALUES (5, 1, 3)")
        conn.commit()
    cog, steps = _id_channel_cog(), []
    cog.bot = SimpleNamespace(get_channel=lambda channel_id: _Channel([]), get_cog=lambda name: None)
    cog.check_channels_loop = SimpleNamespace(is_running=lambda: False, start=lambda: steps.append("loop"))

    async def reconcile():
        steps.append("reconcile")
    cog.reconcile_info_messages = reconcile
    release = None

    async def backlog(channels):
        steps.append("backlog")
        await release.wait()

    cog.process_backlog = backlog

    async def scenario():
        nonlocal release
        release = aio.Event()
        await cog.on_ready()
        await aio.sleep(0)
        assert steps == ["reconcile", "loop", "backlog"] and not cog._backlog_task.done()
        release.set()
        await cog._backlog_task
    asyncio.run(scenario())


def test_id_channel_info_skips_the_state_when_the_api_supplies_it(dbs, monkeypatch):
    _alliance(multistate=1)
    fake_player_api(monkeypatch, API_PLAYER, available=False)
    assert "your state together" in id_channel.render_info_message(1, "A1")
    fake_player_api(monkeypatch, API_PLAYER)
    text = id_channel.render_info_message(1, "A1")
    assert "your state together" not in text and "`12345678`" in text


# --- Add Members (bulk add and import) ------------------------------------------------

def _ops():
    ops = member_ops.AllianceMemberOperations.__new__(member_ops.AllianceMemberOperations)
    ops.bot = SimpleNamespace(get_cog=lambda name: object())
    return ops


def _resolve(fid, kid_ok=True, locked_kid=None, profile=None):
    return asyncio.run(_ops()._resolve_new_member(fid, 1, kid_ok, locked_kid, profile))


def test_bulk_add_uses_the_api_and_skips_the_probe(dbs, monkeypatch):
    fake_player_api(monkeypatch, API_PLAYER)
    probed = _probe(monkeypatch, (245, True))
    member, error = _resolve(9, profile=("", "30", "", "100", ""))
    assert probed == [] and error is None
    assert (member.nickname, member.furnace_lv, member.kid) == ("Delta", 45, 245)


def test_bulk_add_keeps_a_csv_name(dbs, monkeypatch):
    fake_player_api(monkeypatch, API_PLAYER)
    member, _ = _resolve(9, profile=("Typed", "", "", "", ""))
    assert member.nickname == "Typed"


def test_bulk_add_lock_uses_the_api_state(dbs, monkeypatch):
    fake_player_api(monkeypatch, API_PLAYER)
    _, error = _resolve(9, locked_kid=100)
    assert "State #100" in error
    _, error = _resolve(9, locked_kid=245)
    assert error is None


@pytest.mark.parametrize("available", [True, False])
def test_bulk_add_without_api_values_probes(dbs, monkeypatch, available):
    fake_player_api(monkeypatch, None, available=available)
    probed = _probe(monkeypatch, (None, False))
    member, error = _resolve(9, locked_kid=245, profile=("", "30", "", "", ""))
    assert probed == [9]
    assert (member.nickname, member.furnace_lv, member.kid) == ("Player 9", 30, None)
    assert "State #245" in error


def test_bulk_add_keeps_placeholders_once_the_budget_runs_out(dbs, monkeypatch):
    sync.save_setting("profile", sync.normalise({
        "base_url": "https://api.example.com", "player_path": "/p/{fid}", "per_minute": 600, "per_day": 2,
        "fields": {"name": "name", "level": "level", "kingdom": "kid", "power": "", "avatar": ""}}))
    sync.save_setting("key", "k")
    fetched = []

    async def fetch(session, profile, key, fid):
        fetched.append(fid)
        return sync.FetchResult(200, {"name": f"Api{fid}", "level": 45, "kid": 245})
    monkeypatch.setattr(sync, "fetch_player", fetch)
    probed = _probe(monkeypatch, (None, False))
    api_cog = sync.AllianceApiSync(None)
    api_cog.session = lambda: None
    ops = _ops()
    ops.bot = SimpleNamespace(get_cog=lambda name: api_cog if name == "AllianceApiSync" else object())

    async def add_three():
        return [await ops._resolve_new_member(fid, 1, True, None, None) for fid in (1, 2, 3)]
    members = [member for member, _ in asyncio.run(add_three())]

    assert fetched == [1, 2] and probed == [3]
    assert [m.nickname for m in members] == ["Api1", "Api2", "Player 3"]
    assert [m.kid for m in members] == [245, 245, None]


@pytest.mark.parametrize("ready", [True, False])
def test_member_list_hints_mention_player_sync_only_when_ready(ready):
    members = [(1, "", 30, 245)]
    view = member_ops.ManageMembersView.__new__(member_ops.ManageMembersView)
    view.all_members = [{"fid": 1, "nickname": "", "furnace_lv": 30, "kid": 245, "power": 5, "combat_power": None}]
    view.sync_ready = ready
    view.sort_idx = view.current_page = 0
    view.cog = SimpleNamespace(level_mapping={})
    view.pending_selections = set()
    view.filter_name = view.filter_id = view.filter_state = ""
    view.filter_unnamed = False
    view.alliance_name = "A"
    view.alliance_id = 1
    view._sorted_filtered = lambda: []
    view._unnamed = lambda: view.all_members
    text = view.build_embed().description
    assert ("Player Sync" in text) == ready


def test_id_channel_info_says_name_and_level_are_filled_in_when_the_api_is_on(dbs, monkeypatch):
    id_channel = importlib.import_module("cogs.alliance_id_channel")
    fake_player_api(monkeypatch, None, available=True)
    assert "filled in for you" in id_channel.render_info_message(1, "A1")
    fake_player_api(monkeypatch, None, available=False)
    assert "To fill in your name and level too" in id_channel.render_info_message(1, "A1")


def test_id_channel_shows_an_hourglass_only_while_it_looks_the_player_up(dbs, monkeypatch):
    from cogs.pimp_my_bot import theme
    _alliance(kid=245)
    fake_player_api(monkeypatch, API_PLAYER)
    message = _post(9)
    assert message.shown[0] == theme.hourglassIcon
    assert theme.hourglassIcon not in message.reactions and theme.verifiedIcon in message.reactions
