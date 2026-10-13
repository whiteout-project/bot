"""/w shows the stored profile, or a live Player Sync lookup for admins."""
import asyncio
import importlib
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from harness import fake_player_api

w_cmd = importlib.import_module("cogs.alliance_w_command")
permissions = importlib.import_module("cogs.permission_handler")

API_PLAYER = {"nickname": "Delta", "furnace_lv": 80, "kid": 245, "power": 1234567,
              "avatar_url": "https://i.example.com/9.png"}


@pytest.fixture
def dbs(dbs, monkeypatch):
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute("ALTER TABLE users ADD COLUMN discord_id INTEGER")
        conn.execute("ALTER TABLE users ADD COLUMN discord_id_updated_at TEXT")
        conn.commit()
    monkeypatch.setattr(permissions.PermissionManager, "is_admin", staticmethod(lambda user_id: (True, True)))
    return dbs


def _user(fid, **columns):
    columns = {"fid": fid, **columns}
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        conn.execute(f"INSERT INTO users ({', '.join(columns)}) VALUES ({', '.join('?' * len(columns))})",
                     list(columns.values()))
        conn.commit()


def _run_w(fid, user_id=1):
    sent = []

    async def send(*args, **kwargs):
        sent.append(kwargs.get("embed") or args[0])

    async def defer(**kwargs):
        sent.append("deferred")
    interaction = SimpleNamespace(user=SimpleNamespace(id=user_id), response=SimpleNamespace(defer=defer), followup=SimpleNamespace(send=send))
    cog = w_cmd.WCommand.__new__(w_cmd.WCommand)
    cog.bot = SimpleNamespace(get_cog=lambda name: object())
    asyncio.run(cog.fetch_user_info(interaction, fid))
    assert sent[0] == "deferred"
    return sent[-1]


# --- /w ---------------------------------------------------------------------------------

def test_w_shows_everything_for_a_synced_member(dbs):
    _user(7, nickname="Ana", furnace_lv=80, kid=245, power=1234567,
          avatar_url="https://i.example.com/7.png", api_synced_at="2026-10-11T10:00:00+00:00")
    embed = _run_w("7")
    assert "FC 10" in embed.description
    assert "1,234,567" in embed.description
    assert "Last synced:** <t:" in embed.description
    assert embed.thumbnail.url == "https://i.example.com/7.png"


def test_w_leaves_out_what_a_member_has_no_data_for(dbs):
    _user(8, nickname="Ben", furnace_lv=0, kid=245)
    embed = _run_w("8")
    assert "Last synced" not in embed.description and "Power" not in embed.description
    assert not embed.thumbnail


def test_w_looks_up_an_unknown_id_live_when_the_api_is_on(dbs, monkeypatch):
    looked_up = fake_player_api(monkeypatch, API_PLAYER).asked
    embed = _run_w("9")
    assert looked_up == ["9"]
    assert "Delta" in embed.title and "FC 10" in embed.description and "1,234,567" in embed.description
    assert embed.thumbnail.url == "https://i.example.com/9.png"
    with closing(sqlite3.connect("db/users.sqlite")) as conn:
        assert conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0


def test_w_explains_an_unknown_id_when_the_api_is_off(dbs, monkeypatch):
    looked_up = fake_player_api(monkeypatch, API_PLAYER, available=False).asked
    embed = _run_w("9")
    assert looked_up == []
    assert "isn't in any of your alliances yet" in embed.description
    assert "removed" not in embed.description


def test_w_explains_an_id_the_api_does_not_know(dbs, monkeypatch):
    fake_player_api(monkeypatch, None)
    assert "isn't in any of your alliances yet" in _run_w("9").description


def test_w_live_lookup_without_an_avatar_has_no_thumbnail(dbs, monkeypatch):
    fake_player_api(monkeypatch, {**API_PLAYER, "avatar_url": None})
    assert not _run_w("9").thumbnail


def test_w_ignores_an_avatar_that_is_not_a_web_address(dbs, monkeypatch):
    fake_player_api(monkeypatch, {**API_PLAYER, "avatar_url": "not a url"})
    assert not _run_w("9").thumbnail


def test_w_live_lookup_is_for_admins_only(dbs, monkeypatch):
    looked_up = fake_player_api(monkeypatch, API_PLAYER).asked
    monkeypatch.setattr(permissions.PermissionManager, "is_admin", staticmethod(lambda user_id: (False, False)))
    embed = _run_w("9", user_id=2)
    assert looked_up == []
    assert "isn't in any of your alliances yet" in embed.description


def test_w_shows_a_member_on_file_to_non_admins(dbs, monkeypatch):
    _user(7, nickname="Ana", furnace_lv=80, kid=245)
    monkeypatch.setattr(permissions.PermissionManager, "is_admin", staticmethod(lambda user_id: (False, False)))
    assert "FC 10" in _run_w("7", user_id=2).description


def test_w_tells_non_admins_without_pointing_at_admin_menus(dbs, monkeypatch):
    fake_player_api(monkeypatch, API_PLAYER)
    monkeypatch.setattr(permissions.PermissionManager, "is_admin", staticmethod(lambda user_id: (False, False)))
    text = _run_w("9", user_id=2).description
    assert "isn't in any of your alliances yet" in text and "Add them" not in text and "Player Sync" not in text


def test_w_tells_admins_when_the_lookup_found_nothing(dbs, monkeypatch):
    fake_player_api(monkeypatch, None)
    text = _run_w("9").description
    assert "Add them from Alliances" in text and "Player Sync couldn't look them up right now." in text


def test_w_without_the_api_only_explains_how_to_add(dbs, monkeypatch):
    fake_player_api(monkeypatch, API_PLAYER, available=False)
    text = _run_w("9").description
    assert "Add them from Alliances" in text and "Player Sync" not in text
