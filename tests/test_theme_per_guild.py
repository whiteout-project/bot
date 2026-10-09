"""A server's applied theme must only show in that server.

Applying a theme used to overwrite the shared `theme` object, so every server
(and background posts) rendered the last applied theme until a restart.
"""
import asyncio
import importlib
import sqlite3

pmb = importlib.import_module("cogs.pimp_my_bot")


def _manager(tmp_path, monkeypatch):
    monkeypatch.setattr(pmb, "THEME_DB_PATH", str(tmp_path / "pimpmybot.sqlite"))
    manager = object.__new__(pmb.ThemeManager)
    manager._initialized = False
    manager.__init__()
    with sqlite3.connect(pmb.THEME_DB_PATH) as conn:
        conn.execute("INSERT INTO pimpsettings (themeName, is_active, emColorString1) VALUES ('red', 0, '#FF0000')")
        conn.execute("INSERT INTO server_themes (guild_id, theme_name) VALUES (1, 'red')")
    manager.load()
    return manager


def _color_in(manager, guild_id):
    with pmb.use_guild_theme(guild_id):
        return manager.emColorString1


def test_server_theme_only_applies_to_that_server(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    default_color = manager.emColorString1
    assert default_color != "#FF0000"
    assert _color_in(manager, 1) == "#FF0000"
    assert _color_in(manager, 2) == default_color
    assert manager.emColorString1 == default_color, "outside a server context the bot-wide theme applies"


def test_tasks_inherit_the_server_theme(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)

    async def read_color():
        return manager.emColorString1

    async def run():
        with pmb.use_guild_theme(1):
            task = asyncio.ensure_future(read_color())
        return await task

    assert asyncio.run(run()) == "#FF0000"


def test_clearing_the_override_falls_back_after_reload(tmp_path, monkeypatch):
    manager = _manager(tmp_path, monkeypatch)
    assert _color_in(manager, 1) == "#FF0000"
    with sqlite3.connect(pmb.THEME_DB_PATH) as conn:
        conn.execute("DELETE FROM server_themes WHERE guild_id = 1")
    manager.load()
    assert _color_in(manager, 1) == manager.emColorString1
