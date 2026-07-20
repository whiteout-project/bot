"""/register must reject alliance IDs that don't exist.

Discord autocomplete is a suggestion UI, not validation - typing any integer
submitted fine, the user got a success embed, and the orphan row was later
silently deleted by the self-heal sweeps.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

ar = importlib.import_module("cogs.alliance_registration")


def _mk_cog(monkeypatch):
    alliance = sqlite3.connect(":memory:")
    alliance.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    alliance.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    alliance.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("alliance.sqlite"):
            return alliance
        return real_connect(path, *a, **k)

    monkeypatch.setattr(ar.sqlite3, "connect", fake_connect)

    cog = ar.AllianceRegistration.__new__(ar.AllianceRegistration)
    cog.is_registration_enabled = lambda: True
    cog._get_user_row = lambda fid: None
    inserted = []
    cog._insert_new_user = lambda *a, **k: inserted.append(a)

    async def send_success(*a, **k):
        pass

    cog._send_register_success = send_success

    async def fetch_user(fid):
        return {"msg": "success", "data": {"nickname": "X", "stove_lv": 30, "kid": 100}}

    cog.fetch_user = fetch_user
    return cog, inserted


def _interaction():
    sent = []

    async def send_message(*a, **k):
        sent.append((a, k))

    async def defer(*a, **k):
        pass

    async def followup_send(*a, **k):
        sent.append((a, k))

    return SimpleNamespace(
        user=SimpleNamespace(id=42),
        guild=object(),
        guild_id=9,
        response=SimpleNamespace(send_message=send_message, defer=defer),
        followup=SimpleNamespace(send=followup_send),
    ), sent


def test_register_rejects_unknown_alliance(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    inter, sent = _interaction()

    asyncio.run(ar.AllianceRegistration.register.callback(cog, inter, fid=7, alliance=999))

    assert inserted == [], "no user row may be written for a nonexistent alliance"
    assert sent and "doesn't exist" in str(sent[0])


def test_register_accepts_known_alliance(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    inter, sent = _interaction()

    asyncio.run(ar.AllianceRegistration.register.callback(cog, inter, fid=7, alliance=5))

    assert len(inserted) == 1, "valid alliance must register normally"
