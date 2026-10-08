"""MenuView / ConfirmView: the shared timeout, opener check, failed-click reply and expiry notice."""
import asyncio
import types

import cogs.pimp_my_bot as pmb


class _Response:
    def __init__(self, done=False):
        self.sent, self._done = [], done

    def is_done(self):
        return self._done

    async def send_message(self, text, **kwargs):
        self.sent.append((text, kwargs))


def _interaction(user_id, done=False):
    followups = []

    async def send(text, **kwargs):
        followups.append((text, kwargs))

    return types.SimpleNamespace(user=types.SimpleNamespace(id=user_id), response=_Response(done),
                                 followup=types.SimpleNamespace(send=send), followups=followups)


def test_menu_view_uses_menu_timeout_and_lets_only_the_opener_click(monkeypatch):
    monkeypatch.setattr(pmb, "menu_timeout", lambda: 1234.0)

    async def run():
        view = pmb.MenuView(owner_id=7)
        stranger, opener = _interaction(8), _interaction(7)
        return view.timeout, await view.interaction_check(stranger), stranger, await view.interaction_check(opener)

    timeout, stranger_ok, stranger, opener_ok = asyncio.run(run())
    assert timeout == 1234.0 and stranger_ok is False and opener_ok is True
    assert stranger.response.sent[0][1]["ephemeral"] is True


def test_menu_view_without_opener_lets_anyone_click():
    async def run():
        return await pmb.MenuView().interaction_check(_interaction(8))

    assert asyncio.run(run()) is True


def test_failed_click_is_answered_privately_and_logged(caplog):
    async def run(done):
        interaction = _interaction(7, done=done)
        await pmb.MenuView(owner_id=7).on_error(interaction, RuntimeError("boom"), None)
        return interaction

    fresh, deferred = asyncio.run(run(False)), asyncio.run(run(True))
    assert fresh.response.sent and fresh.response.sent[0][1]["ephemeral"] is True
    assert deferred.followups and deferred.followups[0][1]["ephemeral"] is True
    assert any("boom" in record.getMessage() for record in caplog.records)


def test_confirm_view_uses_confirm_timeout_and_shows_the_expired_notice(monkeypatch):
    monkeypatch.setattr(pmb, "confirm_timeout", lambda: 60.0)
    edits = []

    async def edit(**kwargs):
        edits.append(kwargs)

    async def run():
        view = pmb.ConfirmView("stop confirmation")
        view.message = types.SimpleNamespace(edit=edit)
        await view.on_timeout()
        return view.timeout

    assert asyncio.run(run()) == 60.0
    assert edits and edits[0]["view"] is None and "stop confirmation" in edits[0]["embed"].description.lower()
