"""The /w "who is" command. Shows the stored profile for an in-game ID, or a live Player Sync lookup."""
import discord
from discord.ext import commands
import sqlite3
from contextlib import closing
import asyncio
import logging
from datetime import datetime, timezone
from .pimp_my_bot import theme
from .alliance_member_edit import new_member_name
from .bot_level_mapping import LEVEL_MAPPING, format_furnace_level
from . import alliance_api_sync
from .permission_handler import PermissionManager

logger = logging.getLogger('alliance')


def _relative_age(iso_ts: str | None) -> str:
    if not iso_ts:
        return ""
    try:
        dt = datetime.fromisoformat(iso_ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return f"<t:{int(dt.timestamp())}:R>"
    except (ValueError, OSError):
        return ""


def _format_big_int(n) -> str:
    try:
        return f"{int(n):,}"
    except (TypeError, ValueError):
        return str(n)

class WCommand(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/changes.sqlite', timeout=30.0, check_same_thread=False)
        self.c = self.conn.cursor()
        
        self.level_mapping = LEVEL_MAPPING

    async def cog_unload(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    @discord.app_commands.command(name='w', description='Fetches user info using ID.')
    @discord.app_commands.rename(fid='id')
    async def w(self, interaction: discord.Interaction, fid: str):
        await self.fetch_user_info(interaction, fid)

    @w.autocomplete('fid')
    async def autocomplete_fid(self, interaction: discord.Interaction, current: str):
        try:
            def _read():
                with sqlite3.connect('db/users.sqlite', timeout=30.0) as users_db:
                    return users_db.execute("SELECT fid, nickname FROM users").fetchall()
            users = await asyncio.to_thread(_read)

            choices = [
                discord.app_commands.Choice(name=f"{nickname} ({fid})", value=str(fid)) 
                for fid, nickname in users
            ]

            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = choices[:25]

            return filtered_choices
        
        except Exception as e:
            logger.error(f"Autocomplete could not be loaded: {e}")
            print(f"Autocomplete could not be loaded: {e}")
            return []


    async def fetch_user_info(self, interaction: discord.Interaction, fid: str):
        try:
            await interaction.response.defer(thinking=True)
            stored = await asyncio.to_thread(self._read_stored, fid)
            if stored:
                embed = stored_profile_embed(fid, *stored)
            else:
                embed = await self._live_profile_embed(fid, interaction.user.id)
            await interaction.followup.send(embed=embed)
        except Exception as e:
            logger.error(f"Error fetching user info for ID {fid}: {e}")
            await interaction.followup.send("An error occurred while fetching user info.")

    def _read_stored(self, fid: str):
        with closing(sqlite3.connect('db/users.sqlite', timeout=30.0)) as users_db:
            row = users_db.execute(
                "SELECT nickname, kid, alliance, power, power_updated_at, combat_power, "
                "combat_power_updated_at, discord_id, discord_id_updated_at, furnace_lv, "
                "avatar_url, api_synced_at FROM users WHERE fid=?",
                (fid,),
            ).fetchone()
        if not row:
            return None
        alliance_name = None
        if row[2]:
            with closing(sqlite3.connect('db/alliance.sqlite', timeout=30.0)) as alliance_db:
                found = alliance_db.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id=?", (row[2],)
                ).fetchone()
            alliance_name = found[0] if found else None
        return row, alliance_name

    async def _live_profile_embed(self, fid: str, user_id: int) -> discord.Embed:
        is_admin, _ = await asyncio.to_thread(PermissionManager.is_admin, user_id)
        if not is_admin:
            return _not_found_embed(NOT_IN_AN_ALLIANCE)
        api_on = await alliance_api_sync.api_available_async()
        found = (await alliance_api_sync.lookup_via_bot(self.bot, fid)).values if api_on else None
        if found:
            return live_profile_embed(fid, found)
        text = f"{NOT_IN_AN_ALLIANCE} {ADD_THEM}"
        return _not_found_embed(f"{text}\n{LOOKUP_FAILED}" if api_on else text)


NOT_IN_AN_ALLIANCE = "This player isn't in any of your alliances yet."
ADD_THEM = "Add them from Alliances, then Manage Members, then Add Members, and /w will show their profile."
LOOKUP_FAILED = "Player Sync couldn't look them up right now."


def _not_found_embed(text) -> discord.Embed:
    return discord.Embed(description=f"{theme.infoIcon} {text}", color=theme.emColor1)


def render_fields(entries) -> str:
    return "".join(f"**{label}:** {value}\n" for label, value in entries if value is not None)


def power_text(power, age="") -> str | None:
    if power is None:
        return None
    return f"`{_format_big_int(power)}`" + (f" · updated {age}" if age else "")


def api_entries(furnace_lv, power, power_age="") -> list:
    """The fields Player Sync can fill, in display order. Add new API fields here."""
    return [
        (f"{theme.levelIcon} Furnace", f"`{format_furnace_level(furnace_lv)}`" if furnace_lv else None),
        (f"{theme.boltIcon} Power", power_text(power, power_age)),
    ]


def profile_embed(title: str, fid, state, entries, footer: str, avatar_url=None) -> discord.Embed:
    header = (
        f"{theme.upperDivider}\n"
        f"**{theme.fidIcon} ID:** `{fid}`\n"
        f"**{theme.globeIcon} State:** `{state if state is not None else 'unknown'}`\n"
        f"{theme.middleDivider}\n"
    )
    embed = discord.Embed(
        title=title,
        description=f"{header}{render_fields(entries)}{theme.lowerDivider}\n",
        color=theme.emColor1,
    )
    if avatar_url and avatar_url.startswith(("http://", "https://")):
        embed.set_thumbnail(url=avatar_url)
    embed.set_footer(text=footer)
    return embed


def live_profile_embed(fid, found: dict) -> discord.Embed:
    entries = api_entries(found.get("furnace_lv"), found.get("power"))
    return profile_embed(
        f"{theme.userIcon} {new_member_name(found.get('nickname'), fid)}", fid, found.get("kid"),
        entries, f"Live lookup {theme.verifiedIcon}", found.get("avatar_url"),
    )


def stored_profile_embed(fid, row, alliance_name) -> discord.Embed:
    (nickname, kid, _alliance, power_val, power_ts, combat_power_val, combat_power_ts,
     discord_id_val, discord_id_ts, furnace_lv, avatar_url, synced_at) = row
    power_age = _relative_age(power_ts)
    combat_age = _relative_age(combat_power_ts)
    linked_age = _relative_age(discord_id_ts)
    synced = _relative_age(synced_at)
    entries = [(f"{theme.allianceIcon} Alliance", f"`{alliance_name}`" if alliance_name else None)]
    entries += api_entries(furnace_lv, power_val, power_age)
    entries += [
        (f"{theme.shieldIcon} Combat Power", power_text(combat_power_val, combat_age)),
        (f"{theme.chatIcon} Discord",
         f"<@{discord_id_val}>" + (f" · linked {linked_age}" if linked_age else "")
         if discord_id_val else None),
        (f"{theme.timeIcon} Last synced", synced or None),
    ]
    footer = f"Stored profile {theme.verifiedIcon}"
    return profile_embed(f"{theme.userIcon} {new_member_name(nickname, fid)}", fid, kid, entries,
                         footer, avatar_url)


async def setup(bot):
    await bot.add_cog(WCommand(bot))
