"""Member States menu: hub layout, sub-menus, range input and WOS wording."""
import asyncio
import inspect
import re
import types

import pytest

import cogs.alliance_member_states as ams
from cogs.permission_handler import PermissionManager

SETTINGS_OFF = {"enabled": False, "min": 1, "max": 900, "custom": False}


def _labels(view):
    return [getattr(c, "label", None) for c in view.children]


def _cog(status=None):
    return types.SimpleNamespace(kingdom_scan_status=lambda: status, member_catchup_status=lambda: None)


@pytest.fixture(autouse=True)
def api_limit(monkeypatch):
    state = types.SimpleNamespace(spent=False)

    monkeypatch.setattr(ams, "budget_spent", lambda: state.spent)
    return state


@pytest.mark.parametrize("lo,hi,expected", [("1", "2000", (1, 2000)), (" 50 ", "60", (50, 60))])
def test_parse_scan_range_ok(lo, hi, expected):
    assert ams.parse_scan_range(lo, hi) == expected


@pytest.mark.parametrize("lo,hi", [("0", "10"), ("20", "10"), ("abc", "10"), ("1", "100000"), ("", "5")])
def test_parse_scan_range_rejects(lo, hi):
    assert isinstance(ams.parse_scan_range(lo, hi), str)


def test_hub_has_three_submenus_and_back(monkeypatch):
    monkeypatch.setattr(ams, "_hub_counts",
                        lambda: {"wrong": 2, "missing": 3, "unbound": 0, "bindable": 0})
    monkeypatch.setattr(ams.gsr, "get_scan_settings", lambda: SETTINGS_OFF)

    async def build():
        view = ams.StateManagementView(_cog(), 1)
        embed = await view.build_embed()
        return _labels(view), embed.description

    labels, text = asyncio.run(build())
    assert labels == ["Members to Fix (5)", "Alliance States", "State Scan: Off", "Back"]
    assert "**Members to Fix**" in text and "**State Scan**" in text


def test_members_to_fix_buttons_follow_what_is_listed(monkeypatch):
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: [(1, "Ann", None, "missing")])

    async def build():
        view = ams.MembersToFixView(_cog(), 1)
        embed = await view.build_embed()
        return {c.label: c.disabled for c in view.children if getattr(c, "label", None)}, embed.description

    buttons, text = asyncio.run(build())
    assert buttons["Dismiss Warnings"] is True        # nothing flagged as wrong
    assert buttons["Use Alliance State"] is False
    assert "Back" in buttons
    assert "no state on file" in ams.MembersToFixView.option_description(1, None, "missing")
    assert "259" in ams.MembersToFixView.option_description(1, 259, "wrong")


def test_kingdom_scan_view_shows_toggle_state_and_range(monkeypatch):
    monkeypatch.setattr(ams.gsr, "get_scan_settings",
                        lambda: {"enabled": True, "min": 100, "max": 2000, "custom": True})
    monkeypatch.setattr(ams.gsr, "scan_counts", lambda: (2, 3))

    async def build():
        view = ams.KingdomScanView(_cog(), 1)
        embed = await view.build_embed()
        return view, embed.description

    view, text = asyncio.run(build())
    toggle = next(c for c in view.children if getattr(c, "label", "").startswith("Auto-scan"))
    assert toggle.label == "Auto-scan: On"
    assert "100-2000" in text and "set by you" in text
    assert "`2`" in text and "`1`" in text           # waiting 2, not found 1


def test_scan_lines_report_paused_and_progress():
    paused = ams._scan_lines(_cog({"mode": "auto", "total": 4, "remaining": [3, 4],
                                   "resolved": 1, "status": "queued", "started": True}))
    assert any("Paused" in line for line in paused)
    running = ams._scan_lines(_cog({"mode": "now", "total": 4, "remaining": [4], "resolved": 2,
                                    "status": "active", "probe": 30, "probe_total": 900}))
    assert any("30/900" in line for line in running)
    assert ams._scan_lines(_cog(None)) == []


def test_wos_menu_strings_have_no_kingdom_wording():
    strings = re.findall(r'"([^"]*)"', inspect.getsource(ams))
    offenders = [s for s in strings if re.search(r"\b(kingdom|kingdoms|multi-kingdom)\b", s, re.I)]
    assert offenders == []


def test_every_click_rechecks_global_admin(monkeypatch):
    sent = []

    async def send_message(*a, **k):
        sent.append(k)

    inter = types.SimpleNamespace(user=types.SimpleNamespace(id=5),
                                  response=types.SimpleNamespace(send_message=send_message))

    async def check(is_global):
        monkeypatch.setattr(PermissionManager, "is_admin", staticmethod(lambda uid: (True, is_global)))
        return await ams.KingdomScanView(_cog(), 5).interaction_check(inter)

    assert asyncio.run(check(False)) is False and sent[0]["ephemeral"] is True
    assert asyncio.run(check(True)) is True


def test_set_all_only_covers_members_with_a_wrong_kingdom(monkeypatch):
    rows = [(1, "Ann", 259, "wrong"), (2, "Bob", None, "missing")]
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: rows)
    sent = {}

    async def send_modal(modal):
        sent["modal"] = modal

    async def run():
        view = ams.MembersToFixView(_cog(), 1)
        await view.build_embed()
        await view._on_set_all(types.SimpleNamespace(response=types.SimpleNamespace(send_modal=send_modal)))
        return view

    asyncio.run(run())
    assert sent["modal"].fids == [1]


def test_parse_scan_range_both_blank_means_default():
    assert ams.parse_scan_range("", "  ") is None


def test_scan_lines_tell_queued_paused_and_waiting_apart():
    queued = ams._scan_lines(_cog({"mode": "auto", "total": 2, "remaining": [1, 2], "status": "queued"}))
    assert any("Waiting for its turn" in line for line in queued)
    paused = ams._scan_lines(_cog({"mode": "auto", "total": 2, "remaining": [2], "status": "queued",
                                   "started": True}))
    assert any("Paused" in line for line in paused)
    waiting = ams._scan_lines(_cog({"mode": "auto", "total": 2, "remaining": [2], "status": "active",
                                    "started": True, "waiting": True}))
    assert any("bot is busy" in line for line in waiting)


def test_modals_recheck_global_admin(monkeypatch):
    sent = []

    async def send_message(*a, **k):
        sent.append(k)

    inter = types.SimpleNamespace(user=types.SimpleNamespace(id=1),
                                  response=types.SimpleNamespace(send_message=send_message))
    monkeypatch.setattr(PermissionManager, "is_admin", staticmethod(lambda uid: (True, False)))

    async def submit():
        parent = types.SimpleNamespace(original_user_id=1)
        modal = ams.ScanRangeModal(parent, {"enabled": False, "min": 1, "max": 99, "custom": True})
        await modal.on_submit(inter)

    asyncio.run(submit())
    assert sent and sent[0]["ephemeral"] is True


def test_member_modal_uses_id_when_name_missing(monkeypatch):
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: [(7, None, None, "missing")])
    sent = {}

    async def send_modal(modal):
        sent["modal"] = modal

    async def run():
        view = ams.MembersToFixView(_cog(), 1)
        await view.build_embed()
        inter = types.SimpleNamespace(user=types.SimpleNamespace(id=1), data={"values": ["7"]},
                                      response=types.SimpleNamespace(send_modal=send_modal))
        monkeypatch.setattr(ams, "check_interaction_user", _allow)
        await view._on_select(inter)

    asyncio.run(run())
    assert sent["modal"].nickname == "7"


async def _allow(*a, **k):
    return True



def test_range_modal_is_blank_while_on_the_default_range():
    async def build(custom):
        parent = types.SimpleNamespace(original_user_id=1)
        settings = {"enabled": False, "min": 1 if not custom else 100, "max": 900 if not custom else 2000,
                    "custom": custom}
        modal = ams.ScanRangeModal(parent, settings)
        return modal.lo.default, modal.hi.default

    assert asyncio.run(build(False)) == (None, None)
    assert asyncio.run(build(True)) == ("100", "2000")


def test_page_flip_reuses_loaded_rows():
    loads = []

    def load():
        loads.append(1)
        return list(range(60))

    view = ams._PagedView.__new__(ams._PagedView)
    view.rows, view._keep_rows = [], False
    asyncio.run(view._rows(load))
    view._keep_rows = True                      # what Prev/Next set
    asyncio.run(view._rows(load))
    assert len(loads) == 1
    asyncio.run(view._rows(load))               # any other render reloads
    assert len(loads) == 2


# --- Player Sync's API: Members to Fix looks states up there first --------------------------


def _fix_text(monkeypatch, api_on):
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: [(1, "Ann", 259, "wrong")])

    async def api():
        return api_on

    monkeypatch.setattr(ams, "api_available_async", api)

    async def build():
        return (await ams.MembersToFixView(_cog(), 1).build_embed()).description
    return asyncio.run(build())


def test_members_to_fix_says_the_scan_is_quick_with_the_api(monkeypatch):
    text = _fix_text(monkeypatch, True)
    assert "through your API" in text and "take hours" in text      # unknown members still probe
    assert "Try state numbers" not in text


def test_members_to_fix_without_the_api_keeps_the_probe_text(monkeypatch):
    text = _fix_text(monkeypatch, False)
    assert "Try state numbers" in text and "API" not in text


def test_hub_points_to_the_api_when_it_is_set_up(monkeypatch):
    monkeypatch.setattr(ams, "_hub_counts",
                        lambda: {"wrong": 0, "missing": 1, "unbound": 0, "bindable": 0})
    monkeypatch.setattr(ams.gsr, "get_scan_settings", lambda: SETTINGS_OFF)

    async def build(api_on):
        async def api():
            return api_on
        monkeypatch.setattr(ams, "api_available_async", api)
        return (await ams.StateManagementView(_cog(), 1).build_embed()).description

    assert "your API" in asyncio.run(build(True))
    assert "API" not in asyncio.run(build(False))


def test_scan_these_now_with_the_api_queues_the_scan_and_says_so(monkeypatch):
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: [(1, "Ann", None, "missing")])
    queued = []
    cog = _cog()
    cog.queue_kingdom_scan = lambda mode: queued.append(mode) or 1

    async def api():
        return True

    async def edit(interaction, **kw):
        pass

    monkeypatch.setattr(ams, "api_available_async", api)
    monkeypatch.setattr(ams, "safe_edit_message", edit)
    view = ams.MembersToFixView(cog, 1)
    asyncio.run(view._on_scan(types.SimpleNamespace()))
    assert queued == ["now"]
    assert "through your API" in view.last_result


def test_scan_these_now_waits_for_tomorrow_when_the_api_limit_is_used_up(monkeypatch, api_limit):
    monkeypatch.setattr(ams.gsr, "members_to_fix", lambda: [(1, "Ann", None, "missing")])
    queued = []
    cog = _cog()
    cog.queue_kingdom_scan = lambda mode: queued.append(mode) or 1
    api_limit.spent = True

    async def edit(interaction, **kw):
        pass

    monkeypatch.setattr(ams, "safe_edit_message", edit)
    view = ams.MembersToFixView(cog, 1)
    asyncio.run(view._on_scan(types.SimpleNamespace()))
    assert queued == [] and "daily request limit" in view.last_result


def test_state_scan_screen_mentions_auto_sync_and_the_api_limit(monkeypatch, api_limit):
    monkeypatch.setattr(ams.gsr, "get_scan_settings",
                        lambda: {"enabled": True, "min": 1, "max": 900, "custom": False})
    monkeypatch.setattr(ams.gsr, "scan_counts", lambda: (2, 3))

    async def build(spent, api_on=True):
        async def api():
            return api_on
        monkeypatch.setattr(ams, "api_available_async", api)
        cog = _cog()
        api_limit.spent = spent
        return (await ams.KingdomScanView(cog, 1).build_embed()).description

    assert "Auto Sync" in asyncio.run(build(False))
    assert "Auto Sync" not in asyncio.run(build(False, api_on=False))
    assert "daily request limit" in asyncio.run(build(True))
    assert "daily request limit" not in asyncio.run(build(False))
