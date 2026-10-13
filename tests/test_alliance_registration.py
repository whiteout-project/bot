"""/register must reject alliance IDs that don't exist.

Discord autocomplete is a suggestion UI, not validation - typing any integer
submitted fine, the user got a success embed, and the orphan row was later
silently deleted by the self-heal sweeps.
"""
import asyncio
import importlib
import sqlite3
from types import SimpleNamespace

from harness import fake_player_api

ar = importlib.import_module("cogs.alliance_registration")
gift_state_resolver = importlib.import_module("cogs.gift_state_resolver")

API_PLAYER = {"nickname": "Delta", "furnace_lv": 45, "kid": 245, "power": 5000}


def _mk_cog(monkeypatch, home_kid=100):
    fake_player_api(monkeypatch, None, available=False)
    alliance = sqlite3.connect(":memory:")
    alliance.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT, kid INTEGER)")
    alliance.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli', ?)", (home_kid,))
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

    cog.bot = SimpleNamespace(get_cog=lambda name: object())

    cog.probed = []

    async def _verify_add_state(gift_cog, fid, alliance):
        cog.probed.append(fid)
        return (100, True) if home_kid else (None, False)

    monkeypatch.setattr(gift_state_resolver, "verify_add_state", _verify_add_state)
    cog.stored = []

    async def store(member, fid):
        cog.stored.append((fid, member.from_api, member.power, member.avatar_url))
    monkeypatch.setattr(ar, "store_new_member", store)
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


def _register(cog, **options):
    inter, sent = _interaction()
    asyncio.run(ar.AllianceRegistration.register.callback(cog, inter, fid=7, alliance=5, **options))
    return sent


def test_register_takes_name_level_and_state_from_the_api(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    fake_player_api(monkeypatch, API_PLAYER)
    _register(cog)
    assert cog.probed == []
    assert inserted[0][1] == {"nickname": "Delta", "stove_lv": 45, "kid": 245}
    assert cog.stored == [(7, True, API_PLAYER["power"], None)]


def test_register_keeps_the_typed_name(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    fake_player_api(monkeypatch, API_PLAYER)
    _register(cog, name="Typed")
    assert inserted[0][1]["nickname"] == "Typed"


def test_register_multistate_needs_no_state_when_the_api_has_one(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch, home_kid=None)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    fake_player_api(monkeypatch, API_PLAYER)
    _register(cog)
    assert inserted[0][1]["kid"] == 245


def test_register_lock_uses_the_api_state(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state",
                        lambda a, k: None if k == 100 else "This alliance only accepts members from State #100.")
    fake_player_api(monkeypatch, API_PLAYER)
    sent = _register(cog)
    assert inserted == [] and "State #100" in str(sent[-1])


def test_register_without_api_values_probes_and_uses_the_placeholder(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    fake_player_api(monkeypatch, None)
    _register(cog)
    assert cog.probed == [7]
    assert inserted[0][1] == {"nickname": "Player 7", "stove_lv": 0, "kid": 100}


def test_register_multistate_without_an_api_state_still_asks_for_it(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch, home_kid=None)
    monkeypatch.setattr(ar, "check_alliance_state", lambda a, k: None)
    monkeypatch.setattr(ar, "get_alliance_kid", lambda alliance: None)
    fake_player_api(monkeypatch, None)
    sent = _register(cog)
    assert inserted == [] and "**state** option" in str(sent[-1])


def test_register_typed_state_refused_by_the_lock_names_the_api_state(monkeypatch):
    cog, inserted = _mk_cog(monkeypatch)
    monkeypatch.setattr(ar, "check_alliance_state",
                        lambda a, k: None if k == 100 else "This alliance only accepts members from State #100.")
    fake_player_api(monkeypatch, API_PLAYER)
    sent = _register(cog, state=100)
    assert inserted == []
    assert ("Player Sync shows this player in State #245. "
            "This alliance only accepts members from State #100.") in str(sent[-1])


def test_register_option_descriptions_are_plain_and_mark_name_and_level_optional():
    described = {p.name: p.description for p in ar.AllianceRegistration.register.parameters}
    assert "optional" in described["name"] and "optional" in described["level"]
    assert "Player Sync" not in described["name"] + described["level"]
