"""Member States, Running Now, timeout settings, and who may edit an uploaded screenshot review."""
import sqlite3
from contextlib import closing

import pytest

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
    cycled = await sim.click(picked.screen[0], "Toggle Edit Permission")
    assert not cycled.problems()
    assert "Edit permission is now: Uploader + admins" in _description(cycled.screen[0])
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
