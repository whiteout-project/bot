"""Runs the real WOS cogs inside SimCord's in-memory Discord."""
from __future__ import annotations

import ast
import re
import sqlite3
import sys
from contextlib import asynccontextmanager, closing
from dataclasses import dataclass, field
from pathlib import Path

import discord
from discord.ext import commands

import simcord

REPO = Path(__file__).resolve().parents[2]
MAIN_SOURCE = (REPO / "main.py").read_text(encoding="utf-8")

WAY_OUT_LABELS = ("Back", "Main Menu")
SETTINGS_TITLE = "Settings Menu"
ALLIANCE_ID = 1
MEMBERS = [(10001, "Frosty", 30), (10002, "Glacier", 25), (10003, "Blizzard", 18)]


def production_cogs() -> list[str]:
    match = re.search(r"cogs\s*=\s*(\[[^\]]*\])", MAIN_SOURCE)
    return ast.literal_eval(match.group(1))


def _main_node(predicate):
    return next(n for n in ast.walk(ast.parse(MAIN_SOURCE)) if predicate(n))


def build_schema() -> None:
    """Runs main.py's own create_tables() against db/ in the current folder."""
    databases = ast.literal_eval(_main_node(
        lambda n: isinstance(n, ast.Assign) and getattr(n.targets[0], "id", None) == "databases").value)
    create_tables = _main_node(lambda n: isinstance(n, ast.FunctionDef) and n.name == "create_tables")
    Path("db").mkdir(exist_ok=True)
    connections = {name: sqlite3.connect(path) for name, path in databases.items()}
    namespace = {"sqlite3": sqlite3, "connections": connections}
    exec(compile(ast.Module(body=[create_tables], type_ignores=[]), "main.py", "exec"), namespace)
    try:
        namespace["create_tables"]()
    finally:
        for conn in connections.values():
            conn.close()


def seed_alliance(guild_id: int) -> None:
    with closing(sqlite3.connect("db/alliance.sqlite")) as db, db:
        db.execute("INSERT INTO alliance_list (alliance_id, name, discord_server_id) VALUES (?, ?, ?)",
                   (ALLIANCE_ID, "Test Alliance", guild_id))
    with closing(sqlite3.connect("db/users.sqlite")) as db, db:
        db.executemany("INSERT INTO users (fid, nickname, furnace_lv, kid, alliance) VALUES (?, ?, ?, 1, ?)",
                       [(fid, name, level, str(ALLIANCE_ID)) for fid, name, level in MEMBERS])


@dataclass
class Sim:
    env: simcord.Env
    bot: commands.Bot
    guild: simcord.GuildHandle
    channel: simcord.ChannelHandle
    users: dict[str, simcord.MemberActor] = field(default_factory=dict)
    logged_errors: list[str] = field(default_factory=list)

    async def open_settings(self, user: str = "owner") -> discord.Message:
        """Opens /settings fresh. Earlier menus are dropped, as a user walking away would,
        because every live view slows SimCord's settle down."""
        self.drop_open_menus()
        result = await self.users[user].slash(self.channel, "settings")
        return result.response.message

    def drop_open_menus(self) -> None:
        store = self.bot._connection._view_store
        views = {item.view for items in store._views.values() for item in items.values()}
        for view in views:
            if view is not None and view.timeout is not None:
                view.stop()

    def current(self, message: discord.Message) -> discord.Message:
        stored = self.env.backend.get_message(message.channel.id, message.id)
        return simcord.results.to_discord_message(self.env, stored)

    async def click(self, message: discord.Message, label: str, user: str = "owner") -> Click:
        return await self._interact(self.users[user].click(message, label=label))

    async def select(self, message: discord.Message, placeholder: str, option: str, user: str = "owner") -> Click:
        menu = next(item for row in message.components for item in row.children
                    if getattr(item, "placeholder", None) == placeholder)
        value = next(o.value for o in menu.options if o.label == option)
        return await self._interact(self.users[user].select(message, [value], custom_id=menu.custom_id))

    async def _interact(self, action) -> Click:
        error_mark, log_mark = self.env.error_cursor, len(self.logged_errors)
        result = await action
        raised = list(self.env.errors_since(error_mark))
        self.env.errors  # reported per click, so SimCord must not re-raise them at shutdown
        return Click(result, raised, self.logged_errors[log_mark:])


@dataclass
class Click:
    result: simcord.InteractionResult
    raised: list[BaseException]
    logged: list[str]

    @property
    def screen(self) -> tuple[discord.Message, bool] | None:
        """The message the click left on screen and whether it is ephemeral; None for a modal."""
        if self.result.modal is not None:
            return None
        reply = self.result.followups[-1] if self.result.followups else self.result.response
        return (reply.message, reply.ephemeral) if reply else None

    def problems(self) -> list[str]:
        found = [f"raised {error!r}" for error in self.raised] + [f"logged {line}" for line in self.logged]
        if not self.result.acknowledged:
            return found + ["click was never answered (This interaction failed)"]
        if self.screen is None:
            return found
        message, ephemeral = self.screen
        return found + screen_problems(message, ephemeral)


def screen_problems(message: discord.Message, ephemeral: bool) -> list[str]:
    found = []
    if message.embeds and message.content:
        found.append(f"text above the embed: {message.content[:60]!r}")
    if not ephemeral and not message.embeds:
        found.append(f"public message without an embed: {message.content[:60]!r}")
    if message.embeds and not title(message):
        found.append("embed has no title")
    if ephemeral and way_out(message):
        found.append(f"ephemeral message has a {way_out(message)} button")
    if not ephemeral and not way_out(message) and not title(message).endswith(SETTINGS_TITLE):
        found.append(f"no Back or Main Menu on {title(message)!r}, the user is stranded")
    return found


def labels(message: discord.Message, *, enabled_only: bool = False) -> list[str]:
    return [item.label for row in message.components for item in row.children
            if getattr(item, "label", None) and not (enabled_only and item.disabled)]


def way_out(message: discord.Message) -> str | None:
    return next((label for label in labels(message) if label in WAY_OUT_LABELS), None)


def title(message: discord.Message) -> str:
    return message.embeds[0].title or "" if message.embeds else ""


def grant_bot_administrator(env: simcord.Env, guild: simcord.GuildHandle, bot: commands.Bot) -> None:
    """The bot runs without the members intent, so admin rights go on its managed role.
    SimCord has no public API for this; env.backend is internal."""
    role = guild.roles[bot.user.name]
    env.backend.edit_role(guild.id, role.id, {"permissions": discord.Permissions(administrator=True).value})


def seed_admins(sim: Sim, scenario: str) -> None:
    from cogs.permission_handler import PermissionManager, TIER_GLOBAL, TIER_OWNER, TIER_SERVER, TIER_ALLIANCE
    tiers = {"owner": TIER_OWNER, "global": TIER_GLOBAL, "server": TIER_SERVER}
    if scenario == "populated":
        tiers["alliance"] = TIER_ALLIANCE
    for name, tier in tiers.items():
        sim.users[name] = sim.guild.add_member(sim.env.create_user(f"{name}-admin"))
        alliances = [ALLIANCE_ID] if tier == TIER_ALLIANCE else None
        PermissionManager.add_admin(sim.users[name].id, tier=tier, alliance_ids=alliances)
    sim.users["member"] = sim.guild.add_member(sim.env.create_user("plain-member"))


def _forget_cogs() -> None:
    for name in [m for m in sys.modules if m == "cogs" or m.startswith("cogs.")]:
        del sys.modules[name]


@asynccontextmanager
async def running(scenario: str = "populated", *, bot_admin: bool = True):
    """A fresh bot with every production cog, in the current folder's db/."""
    build_schema()
    intents = discord.Intents.default()
    intents.message_content = True
    bot = commands.Bot(command_prefix="/", intents=intents)
    try:
        async with simcord.run(bot) as env:
            for cog in production_cogs():
                await bot.load_extension(f"cogs.{cog}")
            await bot.tree.sync()
            guild = env.create_guild("WOS Test")
            sim = Sim(env, bot, guild, guild.create_text_channel("general"))
            if bot_admin:
                grant_bot_administrator(env, guild, bot)
            if scenario == "populated":
                seed_alliance(guild.id)
            seed_admins(sim, scenario)
            await env.settle()
            yield sim
            for extension in list(bot.extensions):
                await bot.unload_extension(extension)
    finally:
        _forget_cogs()
