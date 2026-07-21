"""The /w "who is" command. Shows the stored profile for an in-game ID."""
import discord
from discord.ext import commands
import sqlite3
import asyncio
import logging
from datetime import datetime, timezone
from .pimp_my_bot import theme
from .login_handler import LoginHandler
from .bot_level_mapping import LEVEL_MAPPING

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

            # Stored profile only; no live lookup.
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute(
                    "SELECT nickname, kid, alliance, power, power_updated_at, combat_power, "
                    "combat_power_updated_at, discord_id, discord_id_updated_at "
                    "FROM users WHERE fid=?",
                    (fid,),
                )
                row = cursor.fetchone()

            if not row:
                await interaction.followup.send(
                    f"User with ID {fid} is not on the list. Live lookups are no longer "
                    f"available (the game removed the player API), so only added members can be shown."
                )
                return

            (nickname, kid, user_alliance, power_val, power_ts,
             combat_power_val, combat_power_ts, discord_id_val, discord_id_ts) = row

            alliance_info = None
            if user_alliance:
                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                    acursor = alliance_db.cursor()
                    acursor.execute(
                        "SELECT name FROM alliance_list WHERE alliance_id=?",
                        (user_alliance,),
                    )
                    alliance_info = acursor.fetchone()

            embed = discord.Embed(
                title=f"{theme.userIcon} {nickname or f'Player {fid}'}",
                description=(
                    f"{theme.upperDivider}\n"
                    f"**{theme.fidIcon} ID:** `{fid}`\n"
                    f"**{theme.globeIcon} State:** `{kid if kid is not None else 'unknown'}`\n"
                    f"{theme.middleDivider}\n"
                ),
                color=theme.emColor1
            )

            if alliance_info:
                embed.description += f"**{theme.allianceIcon} Alliance:** `{alliance_info[0]}`\n"

            if power_val is not None:
                age = _relative_age(power_ts)
                age_suffix = f" · updated {age}" if age else ""
                embed.description += (
                    f"**{theme.boltIcon} Power:** `{_format_big_int(power_val)}`{age_suffix}\n"
                )
            if combat_power_val is not None:
                age = _relative_age(combat_power_ts)
                age_suffix = f" · updated {age}" if age else ""
                embed.description += (
                    f"**{theme.shieldIcon} Combat Power:** "
                    f"`{_format_big_int(combat_power_val)}`{age_suffix}\n"
                )

            if discord_id_val:
                linked_mention = f"<@{discord_id_val}>"
                age = _relative_age(discord_id_ts)
                age_suffix = f" · linked {age}" if age else ""
                embed.description += (
                    f"**{theme.chatIcon} Discord:** {linked_mention}{age_suffix}\n"
                )

            embed.description += f"{theme.lowerDivider}\n"
            embed.set_footer(text=f"Stored profile {theme.verifiedIcon}")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Error fetching user info for ID {fid}: {e}")
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching user info.")


async def setup(bot):
    await bot.add_cog(WCommand(bot))
