"""Bot Health -> Running Now: long-running jobs listed in plain language, and stopping them."""
import asyncio
import types
from datetime import datetime, timedelta

from cogs import bot_health, onnx_lifecycle


def _job(pid, action, details=None, alliance_id=None):
    return {'id': pid, 'action': action, 'details': details or {}, 'alliance_id': alliance_id,
            'status': 'queued', 'priority': 200}


NAMES = {5: "Wolves"}


def test_describe_job_wording():
    d = bot_health.describe_job
    assert d(_job(1, 'gift_validate', {'giftcode': 'ABC'}), NAMES) == "Checking gift code `ABC`"
    assert d(_job(2, 'gift_redeem', {'giftcode': 'ABC'}, 5), NAMES) == "Redeeming gift code `ABC` for Wolves"
    assert d(_job(3, 'gift_redeem_member', {'fid': 9, 'nickname': 'Ann', 'codes': ['A', 'B']}),
             NAMES) == "Redeeming 2 missed codes for Ann"
    assert d(_job(4, 'member_add', {'alliance_name': 'Wolves'}), NAMES) == "Adding members to Wolves"
    assert d(_job(5, 'state_resolve', {'mode': 'auto', 'total': 40, 'remaining': list(range(37))}),
             NAMES) == "State auto-scan (3/40 members)"
    assert d(_job(6, 'state_resolve', {'total': 2, 'remaining': [1, 2]}), NAMES) == "State scan (0/2 members)"
    assert d(_job(7, 'something_new'), NAMES) == "something_new"


def _pq(running=None, queued=(), cancelled=None):
    return types.SimpleNamespace(
        running_info=lambda: running,
        queued_processes=lambda: list(queued),
        cancel_process=lambda pid: (cancelled.append(pid) if cancelled is not None else None) or 'removed',
        queue_counts=lambda: {'queued': len(queued), 'active': 1 if running else 0, 'completed': 0, 'failed': 0},
    )


def _cog(pq):
    bot = types.SimpleNamespace(get_cog=lambda name: pq if name == "ProcessQueue" else None)
    return types.SimpleNamespace(bot=bot, logger=__import__("logging").getLogger("test"))


def _build(view):
    async def run():
        return await view.build_embed()
    return asyncio.run(run())


def test_lists_running_and_queued_and_offers_them_for_stopping(monkeypatch):
    monkeypatch.setattr(bot_health, "_alliance_names", lambda: NAMES)
    monkeypatch.setattr(onnx_lifecycle, "models_in_use", lambda: 0)
    running = {**_job(1, 'gift_redeem', {'giftcode': 'ABC'}, 5),
               'started': datetime.now() - timedelta(minutes=12), 'stopping': False}
    pq = _pq(running, [_job(2, 'member_add', {'alliance_name': 'Wolves'})])

    async def run():
        view = bot_health.RunningNowView(_cog(pq))
        embed = await view.build_embed()
        select = next(c for c in view.children if hasattr(c, "options"))
        return embed.description, [o.value for o in select.options]

    text, values = asyncio.run(run())
    assert "Redeeming gift code `ABC` for Wolves" in text and "12 min" in text
    assert "Adding members to Wolves" in text
    assert values == ["1", "2"]


def test_job_that_cannot_stop_mid_run_is_not_offered(monkeypatch):
    monkeypatch.setattr(bot_health, "_alliance_names", lambda: NAMES)
    monkeypatch.setattr(onnx_lifecycle, "models_in_use", lambda: 0)
    running = {**_job(1, 'gift_validate', {'giftcode': 'ABC'}), 'started': datetime.now(), 'stopping': False}

    async def run():
        view = bot_health.RunningNowView(_cog(_pq(running)))
        embed = await view.build_embed()
        return embed.description, [c for c in view.children if hasattr(c, "options")]

    text, selects = asyncio.run(run())
    assert "finishes on its own" in text
    assert selects == []


def test_confirmed_stop_cancels_the_job(monkeypatch):
    monkeypatch.setattr(bot_health, "_alliance_names", lambda: NAMES)
    monkeypatch.setattr(onnx_lifecycle, "models_in_use", lambda: 0)
    cancelled = []
    pq = _pq(None, [_job(2, 'member_add', {'alliance_name': 'Wolves'})], cancelled)
    edits = []

    async def edit_message(**kw):
        edits.append(kw)

    inter = types.SimpleNamespace(response=types.SimpleNamespace(edit_message=edit_message,
                                                                 is_done=lambda: False))

    async def run():
        view = bot_health.RunningNowView(_cog(pq))
        await view.build_embed()
        await view.stop_job(2)
        return view.last_result

    assert asyncio.run(run()) == "Removed it from the queue."
    assert cancelled == [2]



def test_stopping_the_auto_scan_switches_it_off(monkeypatch):
    monkeypatch.setattr(bot_health, "_alliance_names", lambda: NAMES)
    monkeypatch.setattr(onnx_lifecycle, "models_in_use", lambda: 0)
    switched = []
    monkeypatch.setattr(bot_health.gift_state_resolver, "set_scan_enabled", lambda on: switched.append(on))
    running = {**_job(1, 'state_resolve', {'mode': 'auto', 'total': 3, 'remaining': [1, 2, 3]}),
               'started': datetime.now(), 'stopping': False}
    pq = _pq(running)
    pq.cancel_process = lambda pid: 'stopping'
    edits = []

    async def edit_message(**kw):
        edits.append(kw)

    inter = types.SimpleNamespace(response=types.SimpleNamespace(edit_message=edit_message))

    async def run():
        view = bot_health.RunningNowView(_cog(pq))
        await view.build_embed()
        await view.stop_job(1)
        return view.last_result

    result = asyncio.run(run())
    assert switched == [False]
    assert "Auto-scan" in result


def test_health_settings_rejects_a_bad_menu_timeout_without_saving_anything(monkeypatch):
    saved = {}
    cog = types.SimpleNamespace(update_config=lambda **kw: saved.update(kw))
    config = {'cleanup_hour': 3, 'cleanup_minute': 0, 'monthly_optimization_day': 0, 'notify_user_id': None}
    sent = []

    async def send_message(*a, **k):
        sent.append(k)

    inter = types.SimpleNamespace(response=types.SimpleNamespace(send_message=send_message))
    monkeypatch.setattr(bot_health, "get_menu_timeout_minutes", lambda: 120)
    monkeypatch.setattr(bot_health, "get_confirm_timeout_seconds", lambda: 60)

    async def submit():
        modal = bot_health.HealthSettingsModal(cog, config)
        modal.menu_timeout._value = "5000"
        await modal.on_submit(inter)

    asyncio.run(submit())
    assert saved == {} and sent[0]["ephemeral"] is True



def test_health_settings_rejects_a_bad_confirm_timeout_without_saving_anything(monkeypatch):
    saved = {}
    cog = types.SimpleNamespace(update_config=lambda **kw: saved.update(kw))
    config = {'cleanup_hour': 3, 'cleanup_minute': 0, 'monthly_optimization_day': 0, 'notify_user_id': None}
    sent = []

    async def send_message(*a, **k):
        sent.append(k)

    inter = types.SimpleNamespace(response=types.SimpleNamespace(send_message=send_message))
    monkeypatch.setattr(bot_health, "get_menu_timeout_minutes", lambda: 120)
    monkeypatch.setattr(bot_health, "get_confirm_timeout_seconds", lambda: 60)

    async def submit():
        modal = bot_health.HealthSettingsModal(cog, config)
        modal.confirm_timeout._value = "abc"
        await modal.on_submit(inter)

    asyncio.run(submit())
    assert saved == {} and sent[0]["ephemeral"] is True
