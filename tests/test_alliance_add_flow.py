"""Add Alliance ends on a channel-setup prompt that routes correctly."""
import asyncio
import importlib
from types import SimpleNamespace

al = importlib.import_module("cogs.alliance")


def _mk(routes):
    async def show_channel_setup_for(interaction, alliance_id):
        routes.append(("channels", alliance_id))

    async def show_alliance_hub(interaction, alliance_id):
        routes.append(("hub", alliance_id))

    cogs = {
        "AllianceChannels": SimpleNamespace(show_channel_setup_for=show_channel_setup_for),
        "MainMenu": SimpleNamespace(show_alliance_hub=show_alliance_hub),
    }
    cog = SimpleNamespace(bot=SimpleNamespace(get_cog=cogs.get))
    return al.PostCreateChannelPromptView(cog, 5, "TestAlli")


def _run(label, routes):
    async def scenario():
        view = _mk(routes)
        btn = next(c for c in view.children if getattr(c, "label", None) == label)
        await btn.callback(SimpleNamespace())
        return view

    return asyncio.run(scenario())


def test_prompt_routes_to_channel_setup():
    routes = []
    view = _run("Set Up Channels", routes)
    assert routes == [("channels", 5)]
    assert view.is_finished(), "prompt must stop() before routing away"


def test_prompt_skip_routes_to_hub():
    routes = []
    view = _run("Skip for Now", routes)
    assert routes == [("hub", 5)]
    assert view.is_finished()


def test_prompt_rejects_non_admin_clicks(monkeypatch):
    """The prompt is non-ephemeral with a 2h timeout - every click must re-verify."""
    monkeypatch.setattr(al.PermissionManager, "is_admin", staticmethod(lambda uid: (False, False)))
    sent = []

    async def send_message(*a, **k):
        sent.append((a, k))

    inter = SimpleNamespace(
        user=SimpleNamespace(id=99),
        response=SimpleNamespace(send_message=send_message),
    )

    async def scenario():
        view = _mk([])
        return await view.interaction_check(inter)

    allowed = asyncio.run(scenario())
    assert allowed is False
    assert sent, "non-admin click must get an ephemeral denial"


def test_prompt_allows_admin_clicks(monkeypatch):
    monkeypatch.setattr(al.PermissionManager, "is_admin", staticmethod(lambda uid: (True, False)))
    inter = SimpleNamespace(user=SimpleNamespace(id=1), response=SimpleNamespace())

    async def scenario():
        view = _mk([])
        return await view.interaction_check(inter)

    assert asyncio.run(scenario()) is True
