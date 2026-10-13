"""Player Sync menu: hub layout, gating until set up, Test report and the Alliances entry."""
import asyncio
import types
from datetime import datetime, timezone

import pytest

import cogs.alliance_api_menu as menu
import cogs.alliance_api_sync as sync
from cogs.alliance_api_sync import FetchResult, export_profile, normalise
from cogs.pimp_my_bot import theme

PROFILE = normalise({"base_url": "https://api.example.com", "player_path": "/p/{id}",
                     "fields": {"name": "player.name", "level": "player.tc", "power": "player.power"}})


def _cog(job=None):
    return types.SimpleNamespace(sync_job=lambda: job)


def _build(view_job=None):
    async def build():
        view = menu.PlayerSyncView(_cog(view_job), 1)
        embed = await view.build_embed()
        return view, embed
    return asyncio.run(build())


def _rows(view):
    rows = {}
    for item in view.children:
        rows.setdefault(item.row, []).append(item.label)
    return [rows[r] for r in sorted(rows)]


def test_hub_rows_are_even(dbs):
    view, _ = _build()
    assert _rows(view) == [
        ["Auto Sync: Off", "Sync Now", "Sync Alliance", "Interval"],
        ["Connection", "API Key", "Fields", "Limits", "Channels"],
        ["Test", "Import", "Export", "Back"],
    ]


def test_actions_disabled_until_set_up(dbs):
    view, embed = _build()
    disabled = {item.label for item in view.children if item.disabled}
    assert disabled == {"Auto Sync: Off", "Sync Now", "Sync Alliance", "Test"}
    assert "base URL" in embed.description


def test_actions_enabled_once_set_up(dbs):
    sync.save_setting("profile", PROFILE)
    view, embed = _build()
    assert not any(item.disabled for item in view.children)
    assert "Still needs" not in embed.description


def test_hub_text_names_no_site_and_mentions_backups(dbs):
    _, embed = _build()
    text = embed.description
    assert "from a player-data API" in text and "backups include your API key" in text
    for button in ("**Connection**", "**API Key**", "**Fields**", "**Channels**", "**Test**", "**Import**",
                   "**Export**"):
        assert button in text


def test_hub_shows_running_job(dbs):
    _, embed = _build({"status": "active", "details": {}})
    assert "Syncing right now" in embed.description


def test_test_report_reads_each_field():
    answer = FetchResult(200, {"player": {"name": "Alpha", "tc": 45, "power": "nope"}})
    text = menu.test_report(answer, PROFILE)
    assert "Name: `Alpha` → Alpha" in text
    assert "Furnace level: `45` → FC 3" in text
    assert "Power: `nope` → nothing found at this path" in text
    assert "State: skipped" in text


def test_test_report_groups_power_digits():
    answer = FetchResult(200, {"player": {"name": "Alpha", "tc": 45, "power": 9695312}})
    assert "Power: `9695312` → 9,695,312" in menu.test_report(answer, PROFILE)


def test_connection_modal_names_the_id_placeholder():
    async def build():
        return menu.ConnectionModal(_cog(), 1, PROFILE).player_path.to_component_dict()
    field = asyncio.run(build())
    assert field["label"] == "Player path ({id} = player ID)" and field["placeholder"] == "/v1/players/{id}"


@pytest.mark.parametrize("answer, fragment", [
    (FetchResult(0), "didn't answer"),
    (FetchResult(401), "key"),
    (FetchResult(404), "doesn't know this player"),
    (FetchResult(200), "not with player data"),
])
def test_test_report_explains_failures(answer, fragment):
    assert fragment in menu.test_report(answer, PROFILE)


def test_alliances_entry_has_player_sync_and_even_rows():
    import cogs.bot_main_menu as mm

    async def build():
        view = mm.AllianceManagementEntryView(types.SimpleNamespace(), [], mm.TIER_GLOBAL)
        return [(item.row, getattr(item, "label", None)) for item in view.children
                if getattr(item, "custom_id", "").startswith("alliance_entry_") and item.row >= 2]

    rows = {}
    for row, label in asyncio.run(build()):
        rows.setdefault(row, []).append(label)
    assert rows[2] == ["Add Alliance", "Self-Registration", "Transfer Members", "Export Members"]
    assert rows[3] == ["Member States", "Player Sync", "Main Menu"]


def _apply(make_modal, **values):
    async def run():
        modal = make_modal()
        for name, value in values.items():
            getattr(modal, name)._value = value
        return await modal.apply()
    return asyncio.run(run())


def _connection(base_url):
    return _apply(lambda: menu.ConnectionModal(_cog(), 1, sync.load_setting("profile")),
                  base_url=base_url, player_path="/p/{id}")


def _import(base_url):
    return _apply(lambda: menu.ImportModal(_cog(), 1),
                  text=export_profile(normalise({**PROFILE, "base_url": base_url})))


@pytest.mark.parametrize("save", [_connection, _import])
def test_new_site_clears_the_key(dbs, save):
    sync.save_setting("profile", PROFILE)
    sync.save_setting("key", "Bearer secret-key-1234")
    error, (icon, text) = save("https://other.example.com")
    assert error is None and sync.load_setting("key") == ""
    assert "the old key was removed" in text and icon == theme.infoIcon


@pytest.mark.parametrize("save", [_connection, _import])
def test_same_site_keeps_the_key(dbs, save):
    sync.save_setting("profile", PROFILE)
    sync.save_setting("key", "Bearer secret-key-1234")
    error, (icon, text) = save("https://api.example.com/v2")
    assert error is None and sync.load_setting("key") == "Bearer secret-key-1234"
    assert "cleared" not in text and icon == theme.verifiedIcon


def test_hub_shows_the_result_icon(dbs):
    async def build():
        view = menu.PlayerSyncView(_cog(), 1, (theme.deniedIcon, "Something failed."))
        return await view.build_embed()
    assert f"{theme.deniedIcon} Something failed." in asyncio.run(build()).description


def test_field_paths_show_the_result_line(dbs):
    async def build():
        view = menu.FieldPickView(_cog(), 1, (theme.verifiedIcon, "Name path saved."))
        return await view.build_embed()
    description = asyncio.run(build()).description
    assert description.endswith(f"{theme.verifiedIcon} Name path saved.")


def test_queue_results_only_queued_is_success():
    assert menu.queue_result("queued")[0] == theme.verifiedIcon
    assert menu.queue_result("busy")[0] != theme.verifiedIcon
    assert menu.queue_result("unavailable")[0] != theme.verifiedIcon


@pytest.mark.parametrize("answer, icon", [(FetchResult(200, {"player": {}}), "verifiedIcon"),
                                          (FetchResult(404), "deniedIcon"), (FetchResult(0), "deniedIcon")])
def test_test_result_icon(dbs, monkeypatch, answer, icon):
    sync.save_setting("profile", PROFILE)
    shown = []

    async def allow(*args):
        return True

    async def fetch(*args):
        return answer

    async def show(cog, interaction, last_result=None):
        shown.append(last_result)

    async def defer():
        pass

    monkeypatch.setattr(menu, "require_global_admin", allow)
    monkeypatch.setattr(menu, "check_interaction_user", allow)
    monkeypatch.setattr(sync, "fetch_player", fetch)
    monkeypatch.setattr(menu, "show_player_sync", show)
    inter = types.SimpleNamespace(response=types.SimpleNamespace(defer=defer))
    cog = types.SimpleNamespace(session=lambda: None)

    async def run():
        modal = menu.TestModal(cog, 1)
        modal.fid._value = "123"
        await modal.on_submit(inter)
    asyncio.run(run())
    assert shown[0][0] == getattr(theme, icon)


def test_auto_sync_can_be_turned_off_when_not_set_up(dbs):
    sync.save_setting("enabled", True)
    view, _ = _build()
    toggle = next(item for item in view.children if item.label.startswith("Auto Sync"))
    assert not toggle.disabled


def _toggle_text(last_queued):
    if last_queued:
        sync.save_setting("last_queued", last_queued)
    sync.save_setting("profile", PROFILE)
    seen = []

    async def run():
        view = menu.PlayerSyncView(_cog(), 1)

        async def show(interaction):
            seen.append(view.last_result)
        view.show = show
        await view._on_toggle(None)
    asyncio.run(run())
    return seen[0][1]


def test_toggle_on_says_first_run_soon_when_due(dbs):
    assert "within 5 minutes" in _toggle_text(None)


def test_toggle_on_names_next_due_time(dbs):
    last = datetime.now(timezone.utc).replace(microsecond=0)
    text = _toggle_text(last)
    due = int(last.timestamp()) + 24 * 3600
    assert "within 5 minutes" not in text and f"<t:{due}:R>" in text


def test_modal_errors_are_reported(monkeypatch):
    reported = []

    async def report(interaction, where, error):
        reported.append(where)

    monkeypatch.setattr(menu, "report_failed_click", report)

    async def run():
        await menu.TestModal(_cog(), 1).on_error(None, RuntimeError("boom"))
    asyncio.run(run())
    assert reported == ["TestModal"]


@pytest.mark.parametrize("last_run", [{"finished_at": "2026-10-10T08:00:00+00:00"}, {"scope": "All"},
                                      {"finished_at": "garbage", "checked": "x"}])
def test_last_run_line_survives_odd_summaries(last_run):
    assert isinstance(menu._last_run_line(last_run), str)


def _hub_calls(monkeypatch, is_global):
    rendered, denied = [], []

    async def render(cog, interaction, last_result=None):
        rendered.append(last_result)

    async def send_message(text, ephemeral):
        denied.append(text)

    monkeypatch.setattr(menu.PermissionManager, "is_admin", staticmethod(lambda user_id: (True, is_global)))
    monkeypatch.setattr(menu, "show_player_sync", render)
    return rendered, denied, types.SimpleNamespace(user=types.SimpleNamespace(id=1),
                                                   response=types.SimpleNamespace(send_message=send_message))


@pytest.mark.parametrize("is_global", [True, False])
def test_opening_the_hub_needs_a_global_admin(monkeypatch, is_global):
    rendered, denied, inter = _hub_calls(monkeypatch, is_global)
    asyncio.run(menu.open_player_sync(_cog(), inter))
    assert (rendered == [None]) is is_global and bool(denied) is not is_global


def test_interval_modal_shows_the_given_hours():
    async def build():
        return menu.IntervalModal(_cog(), 1, 6).number.default
    assert asyncio.run(build()) == "6"


def test_rendering_the_hub_skips_the_admin_check(dbs, monkeypatch):
    edits = []

    async def edit(interaction, **kwargs):
        edits.append(kwargs)

    monkeypatch.setattr(menu.PermissionManager, "is_admin", staticmethod(lambda user_id: (False, False)))
    monkeypatch.setattr(menu, "safe_edit_message", edit)
    inter = types.SimpleNamespace(user=types.SimpleNamespace(id=1))
    asyncio.run(menu.show_player_sync(_cog(), inter, (theme.verifiedIcon, "Saved.")))
    assert len(edits) == 1 and "Saved." in edits[0]["embed"].description


def test_export_reply_has_a_title_and_no_key(dbs):
    sync.save_setting("profile", PROFILE)
    sync.save_setting("key", "Bearer secret-key-1234")
    sent = []

    async def send_message(**kwargs):
        sent.append(kwargs)

    async def run():
        view = menu.PlayerSyncView(_cog(), 1)
        await view._on_export(types.SimpleNamespace(response=types.SimpleNamespace(send_message=send_message)))
    asyncio.run(run())
    embed, file = sent[0]["embed"], sent[0]["file"]
    assert sent[0]["ephemeral"] and embed.title and file.filename == "api-profile.json"
    assert b"secret-key" not in file.fp.read()


def test_hub_guide_lines_follow_the_button_order(dbs):
    _, embed = _build()
    text = embed.description
    order = ["**Interval**", "**Connection**", "**API Key**", "**Fields**", "**Limits**", "**Channels**", "**Test**"]
    assert [text.index(name) for name in order] == sorted(text.index(name) for name in order)


# --- Channels: the Sync Log per alliance, auto-delete and how long results stay -----------

ALLIANCES = [(10, "Wolves"), (20, "Bears")]


@pytest.fixture
def alliances(dbs, monkeypatch):
    monkeypatch.setattr(menu.PermissionManager, "list_alliances", staticmethod(lambda: list(ALLIANCES)))
    return dbs


def _channels(alliance_id=None, last_result=None, run=None):
    async def build():
        view = menu.ChannelsView(_cog(), 1, alliance_id, last_result)
        if run:
            await run(view)
        return view, await view.build_embed()
    return asyncio.run(build())


def _channel_rows(view):
    rows = {}
    for item in view.children:
        rows.setdefault(item.row, []).append(getattr(item, "label", None) or item.placeholder)
    return [rows[r] for r in sorted(rows)]


def test_channels_layout_before_an_alliance_is_picked(alliances):
    view, embed = _channels()
    assert _channel_rows(view) == [["Pick an alliance to set its Sync Log"], ["Set Channel", "Clear"],
                                   ["Auto-Delete: On", "Keep Results", "Back"]]
    disabled = {item.label for item in view.children if getattr(item, "disabled", False)}
    assert disabled == {"Set Channel", "Clear"}
    assert "**Wolves:** Not set" in embed.description and "**Bears:** Not set" in embed.description
    assert "On, results stay for 5 minutes" in embed.description


def test_channels_shows_the_picked_alliance(alliances):
    sync.set_sync_channel(10, 555)
    view, embed = _channels(10)
    assert "**Wolves:** <#555>" in embed.description and "**Picked:** Wolves, Sync Log <#555>" in embed.description
    assert not any(getattr(item, "disabled", False) for item in view.children)
    select = view.children[0]
    assert [o.default for o in select.options] == [True, False]


def test_channels_single_alliance_is_picked_for_you(dbs, monkeypatch):
    monkeypatch.setattr(menu.PermissionManager, "list_alliances", staticmethod(lambda: [(10, "Wolves")]))
    view, _ = _channels()
    assert view.alliance_id == 10


def test_channels_pick_and_clear(alliances, monkeypatch):
    async def no_edit(interaction, **kwargs):
        pass
    monkeypatch.setattr(menu, "safe_edit_message", no_edit)

    async def pick(view):
        await view.build_embed()
        await view.apply_pick(None, "sync", 555)
    view, embed = _channels(20, run=pick)
    assert sync.sync_channels().get("20") == 555
    assert f"{theme.verifiedIcon} Sync results for Bears now go to <#555>." in embed.description

    async def clear(view):
        await view.build_embed()
        await view._on_clear(None)
    view, embed = _channels(20, run=clear)
    assert sync.sync_channels().get("20") is None and "Sync Log cleared for Bears." in embed.description


def test_channels_auto_delete_toggle(alliances, monkeypatch):
    async def no_edit(interaction, **kwargs):
        pass
    monkeypatch.setattr(menu, "safe_edit_message", no_edit)

    async def toggle(view):
        await view._on_autodelete(None)
    view, embed = _channels(run=toggle)
    assert sync.load_setting("autodelete") is False
    toggle_button = next(item for item in view.children if getattr(item, "label", "").startswith("Auto-Delete"))
    assert toggle_button.label == "Auto-Delete: Off" and toggle_button.style.name == "secondary"
    assert "Off, results stay in the channel" in embed.description


@pytest.mark.parametrize("value, saved", [("30", 30), ("0", 5), ("1441", 5), ("abc", 5)])
def test_keep_results_modal(alliances, value, saved):
    error, done = _apply(lambda: menu.KeepResultsModal(_cog(), 1, 5, 10), number=value)
    assert sync.load_setting("keep_minutes") == saved
    if saved == 30:
        assert error is None and "30 minutes" in done[1]
    else:
        assert "from 1 to 1440" in error


def test_keep_results_modal_returns_to_channels_with_the_alliance(alliances, monkeypatch):
    shown = []

    async def edit(interaction, **kwargs):
        shown.append(kwargs["embed"].description)
    monkeypatch.setattr(menu, "safe_edit_message", edit)

    async def run():
        await menu.KeepResultsModal(_cog(), 1, 5, 10).show_saved(None, (theme.verifiedIcon, "Saved."))
    asyncio.run(run())
    assert "**Picked:** Wolves" in shown[0] and shown[0].endswith("Saved.")


# --- Channel Setup: the alliance's Sync Log row edits the same setting ---------------------

def _channel_setup(alliance_id=10):
    import cogs.alliance_channels as channels

    async def build():
        return channels.ChannelSetupView(alliance_id, "Wolves", types.SimpleNamespace())
    return asyncio.run(build())


def test_channel_setup_has_a_sync_log_row(dbs):
    view = _channel_setup()
    rows = {}
    for item in view.children:
        rows.setdefault(item.row, []).append(item.label)
    assert rows == {0: ["Set ID Channel", "Clear"], 1: ["Set Activity Log", "Clear"],
                    2: ["Set Redemption Log", "Clear"], 3: ["Set Sync Log", "Clear"], 4: ["Back"]}


def test_channel_setup_sync_log_shares_the_player_sync_setting(dbs, monkeypatch):
    view = _channel_setup()
    monkeypatch.setattr(view, "_get_id_channel", lambda: None)
    monkeypatch.setattr(view, "_get_activity_log_channel", lambda: None)
    inter = types.SimpleNamespace(guild_id=1, user=types.SimpleNamespace(id=1))
    asyncio.run(view.apply_pick(inter, "sync", 555))
    assert sync.sync_channels().get("10") == 555
    assert "**Sync Log**\n└ <#555>\n└ Posts the changes Player Sync finds" in asyncio.run(view.build_embed(None)).description
    view._clear_sync_log(guild_id=1)
    assert "**Sync Log**\n└ _(none)_" in asyncio.run(view.build_embed(None)).description


def test_hub_edit_name_renames_off_the_event_loop(dbs, monkeypatch):
    import threading
    import cogs.alliance as alliance
    renamed = []
    monkeypatch.setattr(alliance, "rename_alliance",
                        lambda aid, name: renamed.append((aid, name, threading.current_thread() is threading.main_thread())))

    async def run():
        modal = alliance.EditNameModal(10, "Old")
        modal.name_input._value = " New "

        async def edit_message(**kw):
            pass
        await modal.on_submit(types.SimpleNamespace(response=types.SimpleNamespace(edit_message=edit_message)))
    asyncio.run(run())
    assert renamed == [(10, "New", False)]
