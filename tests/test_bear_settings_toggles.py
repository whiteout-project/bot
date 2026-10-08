"""Bear Settings buttons show their state ("Auto-Delete: On") and refresh after a toggle."""
import asyncio
import sqlite3
from types import SimpleNamespace

import discord

from cogs import bear_track

ALLIANCE = 7


class _Cog:
    def __init__(self):
        self.alliance_conn = sqlite3.connect(":memory:")
        self.alliance_conn.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
        self.alliance_conn.execute("INSERT INTO alliance_list VALUES (?, 'Test Alliance')", (ALLIANCE,))
        self.settings = {
            "session_timeout_min": 15, "auto_delete_screenshots": 1, "match_all_history": 0,
            "post_info_message": 0, "pin_info_message": 1, "admin_only_add": 1, "admin_only_view": 0,
            "review_editors": "uploader", "channel_id": None,
        }

    def get_bear_settings(self, alliance_id):
        return dict(self.settings)

    def update_bear_setting(self, alliance_id, column, value):
        self.settings[column.removeprefix("bear_")] = value

    async def check_bear_permission(self, interaction, alliance_id, action):
        return True


def _view(monkeypatch, alliance_id=ALLIANCE):
    monkeypatch.setattr(bear_track, "menu_timeout", lambda: 60.0)
    cog = _Cog()

    async def build():
        view = bear_track.BearSettingsView(cog, original_user_id=1)
        view.alliance_id = alliance_id
        view._build_components()
        return view

    return cog, asyncio.run(build())


def _buttons(view):
    return {item.label: item for item in view.children if isinstance(item, discord.ui.Button)}


def test_buttons_show_state_and_highlight_what_is_on(monkeypatch):
    _, view = _view(monkeypatch)
    buttons = _buttons(view)

    on = discord.ButtonStyle.success
    off = discord.ButtonStyle.secondary
    assert buttons["Auto-Delete: On"].style == on
    assert buttons["Name History: Off"].style == off
    assert buttons["Uploaders: Admins only"].style == on
    assert buttons["Viewers: Everyone"].style == off
    assert buttons["Editors: Uploader only"].style == off
    assert buttons["Info Message: Off"].style == off
    assert buttons["Pin Info: On"].style == on
    assert "Session Timeout" in buttons and "Back" in buttons


def test_without_alliance_buttons_show_no_state_and_are_disabled(monkeypatch):
    _, view = _view(monkeypatch, alliance_id=None)
    buttons = _buttons(view)

    for name in ("Auto-Delete", "Name History", "Uploaders", "Viewers", "Editors", "Info Message", "Pin Info"):
        assert buttons[name].disabled
    assert not buttons["Back"].disabled


def test_toggle_refreshes_the_button_label(monkeypatch):
    cog, view = _view(monkeypatch)

    async def allow(interaction, user_id):
        return True

    async def edited(interaction, **kwargs):
        pass

    monkeypatch.setattr(bear_track, "check_interaction_user", allow)
    monkeypatch.setattr(bear_track, "safe_edit_message", edited)
    interaction = SimpleNamespace(user=SimpleNamespace(id=1))

    asyncio.run(view._toggle_auto_delete_callback(interaction))
    asyncio.run(view._toggle_permission(interaction, "view"))
    asyncio.run(view._toggle_edit_callback(interaction))

    labels = set(_buttons(view))
    assert {"Auto-Delete: Off", "Viewers: Admins only", "Editors: Uploader + admins"} <= labels


def test_rows_fit_on_one_line_with_the_longest_values(monkeypatch):
    from test_view_rules import BUTTON_CHROME, ROW_WIDTH_LIMIT

    cog, view = _view(monkeypatch)
    cog.settings.update(admin_only_add=1, admin_only_view=1, review_editors="admins",
                        auto_delete_screenshots=0, match_all_history=0, post_info_message=0, pin_info_message=0)
    view._build_components()

    rows = {}
    for item in view.children:
        if isinstance(item, discord.ui.Button):
            rows.setdefault(item.row, []).append(len(item.label))
    for row, labels in rows.items():
        assert sum(labels) + BUTTON_CHROME * len(labels) <= ROW_WIDTH_LIMIT, (row, labels)
