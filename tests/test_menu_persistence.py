"""Menu persistence: a global menu timeout (0 = never) and reopening the main menu on a dead click."""
import asyncio
import sqlite3
import types

import pytest

from cogs import pimp_my_bot as pmb


@pytest.fixture
def settings_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with sqlite3.connect("db/settings.sqlite") as c:
        c.execute("CREATE TABLE bot_global_settings (setting_key TEXT PRIMARY KEY, setting_value TEXT)")
    monkeypatch.setattr(pmb, "_timeout_cache", {})   # drop the in-process cache
    return tmp_path


def test_default_menu_timeout_is_two_hours(settings_db):
    assert pmb.get_menu_timeout_minutes() == 120
    assert pmb.menu_timeout() == 7200.0


def test_zero_means_menus_never_time_out(settings_db):
    pmb.set_menu_timeout_minutes(0)
    assert pmb.menu_timeout() is None


def test_setting_persists_and_survives_a_cache_reset(settings_db, monkeypatch):
    pmb.set_menu_timeout_minutes(45)
    assert pmb.menu_timeout() == 2700.0
    monkeypatch.setattr(pmb, "_timeout_cache", {})
    assert pmb.get_menu_timeout_minutes() == 45


@pytest.mark.parametrize("bad", [-1, 1441])
def test_out_of_range_timeout_is_rejected(settings_db, bad):
    with pytest.raises(ValueError):
        pmb.set_menu_timeout_minutes(bad)


def test_unreadable_setting_falls_back_to_default(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)                                  # no db/ folder at all
    monkeypatch.setattr(pmb, "_timeout_cache", {})
    assert pmb.menu_timeout() == 7200.0


def test_confirm_timeout_defaults_to_a_minute(settings_db):
    assert pmb.get_confirm_timeout_seconds() == 60
    assert pmb.confirm_timeout() == 60.0


def test_confirm_timeout_is_separate_from_the_menu_timeout(settings_db, monkeypatch):
    pmb.set_confirm_timeout_seconds(90)
    pmb.set_menu_timeout_minutes(30)
    monkeypatch.setattr(pmb, "_timeout_cache", {})
    assert pmb.confirm_timeout() == 90.0 and pmb.menu_timeout() == 1800.0


def test_confirm_timeout_zero_means_never(settings_db):
    pmb.set_confirm_timeout_seconds(0)
    assert pmb.confirm_timeout() is None


@pytest.mark.parametrize("bad", [-1, 3601])
def test_out_of_range_confirm_timeout_is_rejected(settings_db, bad):
    with pytest.raises(ValueError):
        pmb.set_confirm_timeout_seconds(bad)


# --- dead-click detection ---------------------------------------------------------------


class _Store:
    def __init__(self, views=None, dynamic=None):
        self._views = views or {}
        self._dynamic_items = dynamic or {}


def _click(message_id=10, custom_id="abc", component_type=2):
    return types.SimpleNamespace(
        type=__import__("discord").InteractionType.component,
        message=types.SimpleNamespace(id=message_id, interaction_metadata=None),
        data={"custom_id": custom_id, "component_type": component_type},
    )


def test_click_on_a_tracked_menu_is_live():
    store = _Store({10: {(2, "abc"): object()}})
    assert pmb.has_live_handler(store, _click()) is True


def test_click_on_an_untracked_menu_is_dead():
    store = _Store({99: {(2, "abc"): object()}})
    assert pmb.has_live_handler(store, _click()) is False


def test_click_on_a_persistent_view_is_live():
    store = _Store({None: {(2, "abc"): object()}})
    assert pmb.has_live_handler(store, _click()) is True


def test_click_matching_a_dynamic_item_is_live():
    import re
    store = _Store(dynamic={re.compile(r"ticket:\d+"): object()})
    assert pmb.has_live_handler(store, _click(custom_id="ticket:42")) is True


def test_unknown_store_internals_never_hijack_a_click():
    assert pmb.has_live_handler(object(), _click()) is True


def _dead_click(user_id=1, opener_id=1, command="settings", done=False, answered_meanwhile=False):
    sent, opened = [], []
    state = {"done": done}

    async def send_message(*a, **k):
        sent.append(k)

    origin = (types.SimpleNamespace(name=command, user=types.SimpleNamespace(id=opener_id))
              if command else None)
    interaction = types.SimpleNamespace(
        type=__import__("discord").InteractionType.component,
        user=types.SimpleNamespace(id=user_id),
        message=types.SimpleNamespace(id=10, interaction_metadata=None, _interaction=origin),
        data={"custom_id": "abc", "component_type": 2},
        response=types.SimpleNamespace(send_message=send_message, is_done=lambda: state["done"]),
    )
    interaction.answered_meanwhile = lambda: state.update(done=answered_meanwhile)
    return interaction, sent, opened


def _menu_cog(opened, live=False):
    from cogs import bot_main_menu
    cog = bot_main_menu.MainMenu.__new__(bot_main_menu.MainMenu)
    store = _Store({10: {(2, "abc"): object()}} if live else {})
    cog.bot = types.SimpleNamespace(_connection=types.SimpleNamespace(_view_store=store))

    async def show_main_menu(interaction):
        opened.append(interaction)

    cog.show_main_menu = show_main_menu
    return cog


def _admin(monkeypatch, is_admin=True, inter=None):
    from cogs import bot_main_menu
    monkeypatch.setattr(bot_main_menu.PermissionManager, "is_admin",
                        staticmethod(lambda uid: (is_admin, is_admin)))

    async def grace(_seconds):
        if inter is not None:
            inter.answered_meanwhile()           # a raw listener or stop()-ing view answers now

    monkeypatch.setattr(bot_main_menu.asyncio, "sleep", grace)


def test_dead_click_by_the_menu_owner_reopens_the_main_menu(monkeypatch):
    _admin(monkeypatch)
    inter, sent, opened = _dead_click()
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [inter] and sent == []


def test_dead_click_on_a_public_message_never_replaces_it(monkeypatch):
    _admin(monkeypatch)
    inter, sent, opened = _dead_click(command=None)             # e.g. a schedule board
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [] and sent[0]["ephemeral"] is True


def test_dead_click_on_someone_elses_menu_is_left_alone(monkeypatch):
    _admin(monkeypatch)
    inter, sent, opened = _dead_click(user_id=2, opener_id=1)
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [] and sent[0]["ephemeral"] is True


def test_dead_click_by_a_non_admin_gets_a_note(monkeypatch):
    _admin(monkeypatch, is_admin=False)
    inter, sent, opened = _dead_click()
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [] and sent[0]["ephemeral"] is True


def test_live_or_answered_clicks_are_ignored(monkeypatch):
    _admin(monkeypatch)
    inter, sent, opened = _dead_click()
    asyncio.run(_menu_cog(opened, live=True).on_interaction(inter))
    inter2, sent2, opened2 = _dead_click(done=True)
    asyncio.run(_menu_cog(opened2).on_interaction(inter2))
    assert opened == opened2 == [] and sent == sent2 == []


def test_dead_click_on_another_commands_public_post_is_left_alone(monkeypatch):
    _admin(monkeypatch)
    inter, sent, opened = _dead_click(command="bear_damage_view")   # a public chart its owner clicks
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [] and sent[0]["ephemeral"] is True


def test_click_answered_during_the_grace_wait_is_left_alone(monkeypatch):
    inter, sent, opened = _dead_click(answered_meanwhile=True)
    _admin(monkeypatch, inter=inter)
    asyncio.run(_menu_cog(opened).on_interaction(inter))
    assert opened == [] and sent == []


def test_menus_no_longer_wipe_or_grey_out_on_timeout():
    import inspect
    from cogs import pimp_my_bot
    assert not hasattr(pimp_my_bot, "disable_expired_view")
    from pathlib import Path
    for path in Path(pimp_my_bot.__file__).parent.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        for call in __import__("re").findall(r'notify_view_expired\(self, "([^"]*)"\)', text):
            assert "confirm" in call, f"{path.name}: menu '{call}' still expires; only confirmations should"


def test_health_settings_modal_saves_the_menu_timeout(settings_db, monkeypatch):
    from cogs import bot_health
    saved = {}
    cog = types.SimpleNamespace(update_config=lambda **kw: saved.update(kw))
    config = {'cleanup_hour': 3, 'cleanup_minute': 0, 'monthly_optimization_day': 0, 'notify_user_id': None}
    edits = []

    async def edit_message(**kw):
        edits.append(kw)

    inter = types.SimpleNamespace(response=types.SimpleNamespace(edit_message=edit_message))
    monkeypatch.setattr(bot_health, "HealthMenuView", lambda cog: "view")

    async def submit():
        modal = bot_health.HealthSettingsModal(cog, config)
        assert modal.menu_timeout.default == "120"
        assert modal.confirm_timeout.default == "60"
        modal.menu_timeout._value = "0"
        modal.confirm_timeout._value = "120"
        await modal.on_submit(inter)

    asyncio.run(submit())
    assert pmb.menu_timeout() is None and pmb.confirm_timeout() == 120.0
    assert "never" in edits[0]["embed"].description
