"""
The /w whisper command. Sends anonymous messages to registered alliance members.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from .pimp_my_bot import theme
from .login_handler import LoginHandler
from .bot_level_mapping import LEVEL_MAPPING

logger = logging.getLogger('alliance')

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
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT fid, nickname FROM users")
                users = cursor.fetchall()

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

            result = await LoginHandler().fetch_player_data(str(fid))

            if result['status'] == 'rate_limited':
                await interaction.followup.send("API limit reached, please try again later.")
                return

            if result['status'] == 'not_found':
                await interaction.followup.send(f"User with ID {fid} not found.")
                return

            if result['status'] == 'error':
                await interaction.followup.send(f"An error occurred: {result.get('error_message', 'Unknown error')}")
                return

            player = result['data']
            nickname = player['nickname']
            stove_level = player['stove_lv']
            kid = player['kid']
            avatar_image = player['avatar_image']
            stove_lv_content = player.get('stove_lv_content')

            if stove_level > 30:
                stove_level_name = self.level_mapping.get(stove_level, f"Level {stove_level}")
            else:
                stove_level_name = f"Level {stove_level}"

            user_info = None
            alliance_info = None

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT *, alliance FROM users WHERE fid=?", (fid,))
                user_info = cursor.fetchone()

                if user_info and user_info[-1]:
                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                        cursor = alliance_db.cursor()
                        cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (user_info[-1],))
                        alliance_info = cursor.fetchone()

            embed = discord.Embed(
                title=f"{theme.userIcon} {nickname}",
                description=(
                    f"{theme.upperDivider}\n"
                    f"**{theme.fidIcon} ID:** `{fid}`\n"
                    f"**{theme.levelIcon} Furnace Level:** `{stove_level_name}`\n"
                    f"**{theme.globeIcon} State:** `{kid}`\n"
                    f"{theme.middleDivider}\n"
                ),
                color=theme.emColor1
            )

            if alliance_info:
                embed.description += f"**{theme.allianceIcon} Alliance:** `{alliance_info[0]}`\n{theme.lowerDivider}\n"

            registration_status = f"Registered on the List {theme.verifiedIcon}" if user_info else f"Not on the List {theme.deniedIcon}"
            embed.set_footer(text=registration_status)

            if avatar_image:
                embed.set_image(url=avatar_image)
            if isinstance(stove_lv_content, str) and stove_lv_content.startswith("http"):
                embed.set_thumbnail(url=stove_lv_content)

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Error fetching user info for ID {fid}: {e}")
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching user info.")


async def setup(bot):
    await bot.add_cog(WCommand(bot))
