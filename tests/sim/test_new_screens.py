"""Member States, Running Now, timeout settings, who may edit an uploaded screenshot review, and Player Sync."""
import sqlite3
from contextlib import closing

import pytest
import pytest_asyncio

from sim_harness import ALLIANCE_ID, labels, modal_fields, title

pytestmark = pytest.mark.asyncio


def _description(message):
    return message.embeds[0].description or ""


# --- Member States -----------------------------------------------------------------------


async def test_member_states_hub_and_submenus(sim):
    hub = await sim.walk("Alliances", "Member States")
    assert labels(hub) == ["Members to Fix (0)", "Alliance States", "State Scan: Off", "Back"]
    for submenu in ("Members to Fix (0)", "Alliance States", "State Scan: Off"):
        click = await sim.click(sim.current(hub), submenu)
        assert not click.problems(), (submenu, click.problems())
        back = await sim.click(click.screen[0], "Back")
        assert not back.problems() and title(back.screen[0]).endswith("Member States")


async def test_state_scan_toggle_and_range(sim):
    scan = await sim.walk("Alliances", "Member States", "State Scan: Off")
    toggled = await sim.click(scan, "Auto-scan: Off")
    assert not toggled.problems()
    assert "Auto-scan: On" in labels(toggled.screen[0])

    shown = await sim.click(toggled.screen[0], "Set Range")
    assert shown.result.modal is not None
    saved = await sim.submit_modal(shown, {"Lowest state": "100", "Highest state": "2000"})
    assert not saved.problems()
    assert "states 100-2000" in _description(sim.current(scan))


async def test_state_scan_range_rejects_bad_input(sim):
    scan = await sim.walk("Alliances", "Member States", "State Scan: Off")
    shown = await sim.click(scan, "Set Range")
    refused = await sim.submit_modal(shown, {"Lowest state": "20", "Highest state": "10"})
    message, ephemeral = refused.screen
    assert ephemeral and "can't be higher" in message.content


# --- Bot Health: Running Now and the timeout settings ---------------------------------------


async def test_running_now_lists_and_stops_a_waiting_job(sim):
    queue = sim.bot.get_cog("ProcessQueue")
    job = queue.enqueue("sim_waiting_job", 900, details={})   # no handler, so it stays queued
    running = await sim.walk("Maintenance", "Bot Health", "Running Now")
    assert "sim_waiting_job" in _description(running)

    picked = await sim.select(running, "Pick a job to stop", "sim_waiting_job")
    assert not picked.problems()
    confirm, ephemeral = picked.screen
    assert ephemeral and title(confirm).endswith("Stop This Job?")
    stopped = await sim.click(confirm, "Stop")
    assert not stopped.problems()
    assert "Removed it from the queue" in _description(sim.current(running))
    assert all(p["id"] != job for p in queue.queued_processes())


async def test_running_now_cancel_leaves_the_job(sim):
    queue = sim.bot.get_cog("ProcessQueue")
    job = queue.enqueue("sim_waiting_job", 900, details={})
    running = await sim.walk("Maintenance", "Bot Health", "Running Now")
    confirm = (await sim.select(running, "Pick a job to stop", "sim_waiting_job")).screen[0]
    cancelled = await sim.click(confirm, "Cancel")
    assert not cancelled.problems()
    assert any(p["id"] == job for p in queue.queued_processes())


async def test_health_settings_save_both_timeouts(sim):
    from cogs.pimp_my_bot import confirm_timeout, menu_timeout
    health = await sim.walk("Maintenance", "Bot Health")
    shown = await sim.click(health, "Settings")
    fields = modal_fields(shown)
    assert "Menu timeout (minutes, 0 = never)" in fields
    assert "Confirm dialog timeout (seconds, 0 = never)" in fields
    saved = await sim.submit_modal(shown, {
        "Daily Cleanup Time (HH:MM UTC)": "03:00",
        "Monthly Deep Cleanup Day (1-28, 0=off)": "0",
        "Menu timeout (minutes, 0 = never)": "45",
        "Confirm dialog timeout (seconds, 0 = never)": "90",
    })
    assert not saved.problems()
    assert menu_timeout() == 2700.0 and confirm_timeout() == 90.0


# --- Who may edit an uploaded review -----------------------------------------------------------


async def test_bear_settings_cycle_edit_permission(sim):
    settings = await sim.walk("Bear Tracking", "Settings")
    picked = await sim.select(settings, "Select an alliance", "Test Alliance")
    assert not picked.problems()
    cycled = await sim.click(picked.screen[0], "Editors: Uploader only")
    assert not cycled.problems()
    assert "Editors set to **Uploader + admins**" in _description(cycled.screen[0])
    assert sim.bot.get_cog("BearTrack").get_bear_settings(ALLIANCE_ID)["review_editors"] == "admins"


async def _post_hunt_review(sim, uploader="owner"):
    from cogs.bear_track import BearHuntReviewView
    cog = sim.bot.get_cog("BearTrack")
    review = BearHuntReviewView(
        cog=cog, data_submit=cog.data_submit,
        hunt_meta={"date": "2026-10-04", "hunting_trap": 1, "rallies": 3,
                   "total_damage": 1000, "event_time": None},
        rows=[{"name": "Frosty", "damage": 1000, "rank": 1}],
        roster=cog.get_match_roster(ALLIANCE_ID), alliance_id=ALLIANCE_ID,
        alliance_name="Test Alliance", original_user_id=sim.users[uploader].id)
    review.message = await sim.bot.get_channel(sim.channel.id).send(embed=review.build_embed(), view=review)
    await sim.env.settle()
    return sim.current(review.message)


def _first_edit_button(message):
    return next(label for label in labels(message, enabled_only=True)
                if label not in ("Submit", "Cancel", "Back", "Main Menu"))


def _set_bear_editors(sim, mode):
    sim.bot.get_cog("BearTrack").update_bear_setting(ALLIANCE_ID, "bear_review_editors", mode)


def _refused(click):
    return click.screen is not None and click.screen[1] and "Only the person who uploaded" in click.screen[0].content


async def test_hunt_review_is_uploader_only_by_default(sim):
    review = await _post_hunt_review(sim)
    click = await sim.click(review, _first_edit_button(review), user="member")
    assert _refused(click)


async def test_hunt_review_admin_mode_lets_alliance_admins_in(sim):
    _set_bear_editors(sim, "admins")
    review = await _post_hunt_review(sim)
    button = _first_edit_button(review)
    assert not _refused(await sim.click(review, button, user="alliance"))
    assert _refused(await sim.click(sim.current(review), button, user="member"))


async def test_hunt_review_anyone_mode_lets_a_member_in(sim):
    _set_bear_editors(sim, "anyone")
    review = await _post_hunt_review(sim)
    click = await sim.click(review, _first_edit_button(review), user="member")
    assert not _refused(click) and not click.problems()


def _configure_upload_channel(sim):
    with closing(sqlite3.connect("db/settings.sqlite")) as db, db:
        db.execute("INSERT INTO ocr_channel_settings (channel_id, alliance_id) VALUES (?, ?)",
                   (sim.channel.id, ALLIANCE_ID))


async def test_attendance_channel_cycles_editors(sim):
    _configure_upload_channel(sim)
    channels = await sim.walk("Attendance", "Screenshot Upload")
    menu = next(item for row in channels.components for item in row.children
                if getattr(item, "placeholder", None) == "Edit a configured channel…")
    edit = await sim.select(channels, "Edit a configured channel…", menu.options[0].label)
    assert not edit.problems()
    assert "Editors: Uploader only" in labels(edit.screen[0])
    cycled = await sim.click(edit.screen[0], "Editors: Uploader only")
    assert not cycled.problems()
    assert "Editors: Uploader + admins" in labels(cycled.screen[0])


async def _post_upload_panel(sim, uploader="owner"):
    from types import SimpleNamespace
    from cogs.attendance_ocr_parsers import _ProgressView
    cancelled = []

    async def cancel(**_kwargs):
        cancelled.append(True)

    session = SimpleNamespace(uploader_id=sim.users[uploader].id,
                              channel=sim.bot.get_channel(sim.channel.id), cancel=cancel)
    view = _ProgressView(session)
    message = await session.channel.send(content="upload panel", view=view)
    await sim.env.settle()
    return sim.current(message), cancelled


async def test_attendance_upload_panel_follows_editors_setting(sim):
    from cogs.attendance_ocr_setup import set_ocr_review_editors
    _configure_upload_channel(sim)
    panel, cancelled = await _post_upload_panel(sim)
    refused = await sim.click(panel, "Cancel", user="member")
    assert refused.screen[1] and "Only the uploader" in refused.screen[0].content and not cancelled

    set_ocr_review_editors(ALLIANCE_ID, "anyone")
    await sim.click(sim.current(panel), "Cancel", user="member")
    assert cancelled


# --- Player Sync -------------------------------------------------------------------------------

API_KEY = "Bearer test-key"
SYNC_SETUP_BUTTONS = {"Auto Sync: Off", "Sync Now", "Sync Alliance", "Test"}


@pytest_asyncio.fixture
async def fake_api():
    """A local player API: needs the test key, answers 404 for player IDs ending in 0."""
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    async def player(request):
        if request.headers.get("Authorization") != API_KEY:
            return web.json_response({"ok": False}, status=401)
        fid = request.match_info["fid"]
        if fid.endswith("0"):
            return web.json_response({"ok": False}, status=404)
        return web.json_response({"player": {"nick_name": f"Synced{fid[-3:]}", "furnace_level": 30,
                                             "power": 12345678}})

    app = web.Application()
    app.router.add_get("/v1/players/{fid}", player)
    server = TestServer(app, host="127.0.0.1")
    await server.start_server()
    yield str(server.make_url("/")).rstrip("/")
    await server.close()


async def _player_sync(sim, user="owner"):
    return await sim.walk("Alliances", "Player Sync", user=user)


async def _submit(sim, hub, button, values):
    shown = await sim.click(sim.current(hub), button)
    assert shown.result.modal is not None, button
    return await sim.submit_modal(shown, values)


async def _saved(sim, hub, button, values):
    saved = await _submit(sim, hub, button, values)
    assert not saved.problems(), (button, saved.problems())
    return sim.current(hub)


async def _set_field(sim, hub, field, path):
    fields = await sim.click(sim.current(hub), "Fields")
    assert not fields.problems()
    shown = await sim.select(fields.screen[0], "Choose a field to edit", field)
    assert list(modal_fields(shown)) == [f"{field} path (leave empty to skip)"]
    saved = await sim.submit_modal(shown, {f"{field} path (leave empty to skip)": path})
    assert not saved.problems(), saved.problems()
    return sim.current(hub)


async def _set_up(sim, hub, base_url, key=API_KEY):
    await _saved(sim, hub, "Connection", {"Base URL": base_url,
                                          "Player path ({fid} = player ID)": "/v1/players/{fid}"})
    await _saved(sim, hub, "API Key", {"Header name": "Authorization",
                                       "Key (include Bearer if the API needs it)": key})
    await _set_field(sim, hub, "Name", "player.nick_name")
    return await _set_field(sim, hub, "Furnace level", "player.furnace_level")


def _hold_sync_jobs(sim):
    """Keeps queued syncs waiting, so the menu's queued state is stable while it is checked."""
    sim.bot.get_cog("ProcessQueue")._handlers.pop("api_sync", None)


async def test_player_sync_opens_from_alliances_not_set_up(sim):
    alliances = await sim.walk("Alliances")
    assert "Player Sync" in labels(alliances, enabled_only=True)
    opened = await sim.click(alliances, "Player Sync")
    assert not opened.problems(), opened.problems()
    hub = opened.screen[0]
    assert title(hub).endswith("Player Sync")
    disabled = set(labels(hub)) - set(labels(hub, enabled_only=True))
    assert disabled == SYNC_SETUP_BUTTONS


async def test_player_sync_connection_saves_in_place(sim):
    hub = await _player_sync(sim)
    assert "base URL" in _description(hub)
    hub = await _saved(sim, hub, "Connection", {"Base URL": "https://api.example.com",
                                                "Player path ({fid} = player ID)": "/v1/players/{fid}"})
    assert "Connection saved." in _description(hub)
    assert "base URL" not in _description(hub)


async def test_player_sync_connection_refuses_a_bad_url(sim):
    hub = await _player_sync(sim)
    refused = await _submit(sim, hub, "Connection", {"Base URL": "ftp://api.example.com"})
    assert not refused.problems(), refused.problems()
    message, ephemeral = refused.screen
    assert ephemeral and "must start with http:// or https://" in message.content
    assert "Back" in labels(sim.current(hub), enabled_only=True)


async def test_player_sync_key_is_masked(sim):
    hub = await _player_sync(sim)
    hub = await _saved(sim, hub, "API Key", {"Key (include Bearer if the API needs it)": API_KEY})
    assert "API key saved." in _description(hub)
    assert "••••-key" in _description(hub)
    assert API_KEY not in str(hub.embeds[0].to_dict())


async def test_player_sync_fields_set_one_path(sim):
    hub = await _player_sync(sim)
    fields = await sim.click(hub, "Fields")
    assert not fields.problems() and title(fields.screen[0]).endswith("Field Paths")
    back = await sim.click(fields.screen[0], "Back")
    assert not back.problems() and title(back.screen[0]).endswith("Player Sync")
    hub = await _set_field(sim, hub, "Name", "player.nick_name")
    assert title(hub).endswith("Player Sync")
    assert "Name path saved." in _description(hub)


async def test_player_sync_limits_and_interval(sim):
    from cogs import alliance_api_sync as sync
    hub = await _player_sync(sim)
    hub = await _saved(sim, hub, "Limits", {"Requests per minute": "60", "Requests per day": "5000"})
    assert "up to 60 requests a minute and 5000 a day." in _description(hub)
    hub = await _saved(sim, hub, "Interval", {"Hours between syncs (at least 1)": "12"})
    assert "every 12 hours" in _description(hub)
    refused = await _submit(sim, hub, "Interval", {"Hours between syncs (at least 1)": "0"})
    message, ephemeral = refused.screen
    assert ephemeral and "Enter a whole number of hours" in message.content
    assert sync.get_interval_hours() == 12


async def test_player_sync_import_and_export(sim):
    hub = await _player_sync(sim)
    refused = await _submit(sim, hub, "Import", {"Paste the whole setup file": "not json"})
    message, ephemeral = refused.screen
    assert ephemeral and "look like a Player Sync setup file" in message.content

    profile = ('{"base_url": "https://api.example.com", "player_path": "/v1/players/{fid}", '
               '"fields": {"name": "player.nick_name"}}')
    hub = await _saved(sim, hub, "Import", {"Paste the whole setup file": profile})
    assert "Setup imported." in _description(hub)
    assert "Setup:** Ready" in _description(hub)

    await _saved(sim, hub, "API Key", {"Key (include Bearer if the API needs it)": API_KEY})
    exported = await sim.click(sim.current(hub), "Export")
    assert not exported.problems(), exported.problems()
    message, ephemeral = exported.screen
    assert ephemeral and [a.filename for a in message.attachments] == ["api-profile.json"]
    data = sim.env.backend.cdn.get(message.attachments[0].url).decode("utf-8")
    assert '"base_url": "https://api.example.com"' in data
    assert "test-key" not in data


async def test_player_sync_test_reads_each_field(sim, fake_api):
    from cogs.pimp_my_bot import theme
    hub = await _set_up(sim, await _player_sync(sim), fake_api)
    assert not SYNC_SETUP_BUTTONS - set(labels(hub, enabled_only=True))
    tested = await _submit(sim, hub, "Test", {"Player ID": "10001"})
    assert not tested.problems(), tested.problems()
    text = _description(sim.current(hub))
    assert f"{theme.verifiedIcon} Test with player `10001`" in text
    assert "• Name: `Synced001` → Synced001" in text
    assert "• Power: skipped" in text


async def test_player_sync_test_shows_key_rejected(sim, fake_api):
    from cogs.alliance_api_menu import KEY_REJECTED
    from cogs.pimp_my_bot import theme
    hub = await _set_up(sim, await _player_sync(sim), fake_api, key="Bearer wrong-key")
    tested = await _submit(sim, hub, "Test", {"Player ID": "10001"})
    assert not tested.problems(), tested.problems()
    text = _description(sim.current(hub))
    assert f"{theme.deniedIcon} Test with player `10001`" in text
    assert "error 401" in text and KEY_REJECTED in text


async def test_player_sync_now_queues_once(sim, fake_api):
    _hold_sync_jobs(sim)
    hub = await _set_up(sim, await _player_sync(sim), fake_api)
    queued = await sim.click(hub, "Sync Now")
    assert not queued.problems(), queued.problems()
    assert "The sync is queued" in _description(queued.screen[0])
    assert "**Waiting to start**" in _description(queued.screen[0])
    again = await sim.click(queued.screen[0], "Sync Now")
    assert not again.problems()
    assert "already running or waiting to start" in _description(again.screen[0])


async def test_player_sync_one_alliance(sim, fake_api):
    _hold_sync_jobs(sim)
    hub = await _set_up(sim, await _player_sync(sim), fake_api)
    picker = await sim.click(hub, "Sync Alliance")
    assert not picker.problems(), picker.problems()
    back = await sim.click(picker.screen[0], "Back")
    assert not back.problems() and title(back.screen[0]).endswith("Player Sync")

    picker = await sim.click(back.screen[0], "Sync Alliance")
    picked = await sim.select(picker.screen[0], "Pick an alliance to sync now", "Test Alliance")
    assert not picked.problems(), picked.problems()
    assert "The sync is queued" in _description(picked.screen[0])
    jobs = sim.bot.get_cog("ProcessQueue").get_queued_processes_by_action("api_sync")
    assert [job["details"]["alliance_id"] for job in jobs] == [ALLIANCE_ID]


async def test_player_sync_auto_sync_toggles(sim, fake_api):
    _hold_sync_jobs(sim)
    hub = await _set_up(sim, await _player_sync(sim), fake_api)
    on = await sim.click(hub, "Auto Sync: Off")
    assert not on.problems(), on.problems()
    assert "Auto Sync: On" in labels(on.screen[0], enabled_only=True)
    assert "Automatic sync is on." in _description(on.screen[0])
    off = await sim.click(on.screen[0], "Auto Sync: On")
    assert not off.problems(), off.problems()
    assert "Auto Sync: Off" in labels(off.screen[0])
    assert "Automatic sync is off." in _description(off.screen[0])


async def test_player_sync_back_returns_to_alliances(sim):
    hub = await _player_sync(sim)
    back = await sim.click(hub, "Back")
    assert not back.problems(), back.problems()
    assert title(back.screen[0]).endswith("Alliances")


async def test_player_sync_refuses_non_global_admins(sim):
    alliances = await sim.walk("Alliances", user="server")
    assert "Player Sync" in labels(alliances)
    assert "Player Sync" not in labels(alliances, enabled_only=True)

    hub = await _player_sync(sim)
    refused = await sim.click(hub, "Connection", user="server")
    assert refused.result.modal is None
    message, ephemeral = refused.screen
    assert ephemeral and "Only global administrators" in message.content
