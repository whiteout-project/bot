"""External OCR Service setting must re-verify the CLICKER on every use.

MaintenanceView is persistent (timeout=None) and gated by the opener's
is_global flag, so anyone in the channel could click a Global admin's live
button and the modal wrote remote_ocr_url with no permission check at all -
redirecting all OCR screenshot traffic (plus the API key) to an arbitrary URL.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

bm = importlib.import_module("cogs.bot_main_menu")


def _interaction(user_id=42):
    sent = []
    modals = []

    async def send_message(*a, **k):
        sent.append(a or k)

    async def send_modal(m):
        modals.append(m)

    return SimpleNamespace(
        user=SimpleNamespace(id=user_id),
        response=SimpleNamespace(
            send_message=send_message,
            send_modal=send_modal,
            is_done=lambda: False,
        ),
    ), sent, modals


def _set_admin(monkeypatch, is_global):
    monkeypatch.setattr(
        bm.PermissionManager, "is_admin",
        staticmethod(lambda uid: (is_global, is_global)),
    )


def test_non_global_clicker_does_not_get_modal(monkeypatch):
    _set_admin(monkeypatch, is_global=False)
    view = bm.MaintenanceView(SimpleNamespace(bot=None), is_global=True)
    btn = next(c for c in view.children if c.custom_id == "toggle_remote_ocr")
    inter, sent, modals = _interaction()

    asyncio.run(btn.callback(inter))

    assert modals == [], "non-global clicker must not receive the OCR modal"
    assert sent, "clicker must get an ephemeral denial"


def test_non_global_submitter_cannot_write_setting(monkeypatch):
    modal = bm.ExternalOcrModal(SimpleNamespace(bot=None), is_global=True)
    _set_admin(monkeypatch, is_global=False)
    calls = []

    def spy_connect(*a, **k):
        calls.append(a)
        raise RuntimeError("blocked")

    monkeypatch.setattr(bm.sqlite3, "connect", spy_connect)
    inter, sent, modals = _interaction()

    asyncio.run(modal.on_submit(inter))

    assert calls == [], "on_submit must not touch the DB for non-global users"
    assert sent, "submitter must get an ephemeral denial"


def test_global_submitter_still_writes(monkeypatch):
    shows = []

    async def show_maintenance(interaction):
        shows.append(interaction)

    modal = bm.ExternalOcrModal(
        SimpleNamespace(bot=None, show_maintenance=show_maintenance), is_global=True
    )
    modal.url_input._value = "https://ocr.example.com/"
    _set_admin(monkeypatch, is_global=True)

    mem = sqlite3.connect(":memory:")
    mem.execute("CREATE TABLE bot_global_settings (setting_key TEXT PRIMARY KEY, setting_value TEXT)")
    mem.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("settings.sqlite"):
            return mem
        return real_connect(path, *a, **k)

    monkeypatch.setattr(bm.sqlite3, "connect", fake_connect)
    inter, sent, modals = _interaction()

    asyncio.run(modal.on_submit(inter))

    row = mem.execute(
        "SELECT setting_value FROM bot_global_settings WHERE setting_key='remote_ocr_url'"
    ).fetchone()
    assert row == ("https://ocr.example.com",), "global admin write must still work"
    assert shows, "must return to the maintenance menu"
