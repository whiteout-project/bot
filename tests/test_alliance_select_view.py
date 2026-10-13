"""AllianceSelectView keeps extra options (e.g. All alliances) on every page without dropping alliances."""
import asyncio

import discord

from cogs.alliance_member_operations import AllianceSelectView


def _alliances(n):
    return [(i, f"Alliance {i}", 10) for i in range(1, n + 1)]


def _extras(*values):
    return [discord.SelectOption(label=v.upper(), value=v) for v in values]


def _values(view):
    return [o.value for o in view.current_select.options]


def _build(alliances, extras=None):
    async def build():
        return AllianceSelectView(alliances, context="giftcode", extra_options=extras)
    return asyncio.run(build())


def test_no_extras_pages_by_25():
    view = _build(_alliances(30))
    assert len(view.current_select.options) == 25
    assert view.max_page == 1


def test_extras_fit_without_dropping_alliances():
    view = _build(_alliances(30), _extras("enable_all", "disable_all"))
    shown = []
    for page in range(view.max_page + 1):
        view.page = page
        view.update_select_menu()
        values = _values(view)
        assert values[:2] == ["enable_all", "disable_all"]
        assert len(values) <= 25
        shown += values[2:]
    assert shown == [str(i) for i in range(1, 31)]


def test_extras_stay_after_changing_page():
    view = _build(_alliances(30), _extras("all"))
    view.page = 1
    view.update_select_menu()
    assert _values(view)[0] == "all"


def test_page_count_accounts_for_extras():
    view = _build(_alliances(24), _extras("enable_all", "disable_all"))
    assert view.max_page == 1
    view = _build(_alliances(23), _extras("enable_all", "disable_all"))
    assert view.max_page == 0
