"""Player Sync menu: hub layout, gating until set up, Test report and the Alliances entry."""
import asyncio
import types
from datetime import datetime, timezone

import pytest

import cogs.alliance_api_menu as menu
import cogs.alliance_api_sync as sync
from cogs.alliance_api_sync import FetchResult, export_profile, normalise
from cogs.pimp_my_bot import theme
from harness import make_player_sync_dbs

PROFILE = normalise({"base_url": "https://api.example.com", "player_path": "/p/{fid}",
                     "fields": {"name": "player.name", "level": "player.tc", "power": "player.power"}})


@pytest.fixture
def dbs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    make_player_sync_dbs(tmp_path)
    return tmp_path


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
        ["Connection", "API Key", "Fields", "Limits"],
        ["Test", "Import", "Export", "Back"],
    ]


def test_actions_disabled_until_set_up(dbs):
    view, embed = _build()
    disabled = {item.label for item in view.children if item.disabled}
    assert disabled == {"Auto Sync: Off", "Sync Now", "Sync Alliance", "Test"}
    assert "base URL" in embed.description


def test_actions_enabled_once_set_up(dbs):
    sync.save_profile(PROFILE)
    view, embed = _build()
    assert not any(item.disabled for item in view.children)
    assert "Still needs" not in embed.description


def test_hub_text_names_no_site_and_mentions_backups(dbs):
    _, embed = _build()
    text = embed.description
    assert "player-data API you use" in text and "backups include your API key" in text
    for button in ("**Connection**", "**API Key**", "**Fields**", "**Test**", "**Import**", "**Export**"):
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
    return _apply(lambda: menu.ConnectionModal(_cog(), 1, sync.get_profile()),
                  base_url=base_url, player_path="/p/{fid}")


def _import(base_url):
    return _apply(lambda: menu.ImportModal(_cog(), 1),
                  text=export_profile(normalise({**PROFILE, "base_url": base_url})))


@pytest.mark.parametrize("save", [_connection, _import])
def test_new_site_clears_the_key(dbs, save):
    sync.save_profile(PROFILE)
    sync.save_key("Bearer secret-key-1234")
    error, (icon, text) = save("https://other.example.com")
    assert error is None and sync.get_key() == ""
    assert "the old key was removed" in text and icon == theme.infoIcon


@pytest.mark.parametrize("save", [_connection, _import])
def test_same_site_keeps_the_key(dbs, save):
    sync.save_profile(PROFILE)
    sync.save_key("Bearer secret-key-1234")
    error, (icon, text) = save("https://api.example.com/v2")
    assert error is None and sync.get_key() == "Bearer secret-key-1234"
    assert "cleared" not in text and icon == theme.verifiedIcon


def test_hub_shows_the_result_icon(dbs):
    async def build():
        view = menu.PlayerSyncView(_cog(), 1, (theme.deniedIcon, "Something failed."))
        return await view.build_embed()
    assert f"{theme.deniedIcon} Something failed." in asyncio.run(build()).description


def test_queue_results_only_queued_is_success():
    assert menu.queue_result("queued")[0] == theme.verifiedIcon
    assert menu.queue_result("busy")[0] != theme.verifiedIcon
    assert menu.queue_result("unavailable")[0] != theme.verifiedIcon


@pytest.mark.parametrize("answer, icon", [(FetchResult(200, {"player": {}}), "verifiedIcon"),
                                          (FetchResult(404), "deniedIcon"), (FetchResult(0), "deniedIcon")])
def test_test_result_icon(dbs, monkeypatch, answer, icon):
    sync.save_profile(PROFILE)
    shown = []

    async def allow(*args):
        return True

    async def fetch(*args):
        return answer

    async def show(cog, interaction, last_result=None):
        shown.append(last_result)

    async def defer():
        pass

    monkeypatch.setattr(menu, "_is_global_admin", allow)
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
    sync.set_enabled(True)
    view, _ = _build()
    toggle = next(item for item in view.children if item.label.startswith("Auto Sync"))
    assert not toggle.disabled


def _toggle_text(last_queued):
    if last_queued:
        sync.set_last_queued(last_queued)
    sync.save_profile(PROFILE)
    seen = []

    async def run():
        view = menu.PlayerSyncView(_cog(), 1)

        async def refresh(interaction, last_result=None):
            seen.append(last_result)
        view.refresh = refresh
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
        return menu.IntervalModal(_cog(), 1, 6).hours.default
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
    sync.save_profile(PROFILE)
    sync.save_key("Bearer secret-key-1234")
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
