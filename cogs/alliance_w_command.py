"""
The /w whisper command. Sends anonymous messages to registered alliance members.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from .pimp_my_bot import theme
from .login_handler import LoginHandler

logger = logging.getLogger('alliance')

class WCommand(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/changes.sqlite', timeout=30.0, check_same_thread=False)
        self.c = self.conn.cursor()
        
        self.level_mapping = {
            31: "30-1", 32: "30-2", 33: "30-3", 34: "30-4",
            35: "FC 1", 36: "FC 1 - 1", 37: "FC 1 - 2", 38: "FC 1 - 3", 39: "FC 1 - 4",
            40: "FC 2", 41: "FC 2 - 1", 42: "FC 2 - 2", 43: "FC 2 - 3", 44: "FC 2 - 4",
            45: "FC 3", 46: "FC 3 - 1", 47: "FC 3 - 2", 48: "FC 3 - 3", 49: "FC 3 - 4",
            50: "FC 4", 51: "FC 4 - 1", 52: "FC 4 - 2", 53: "FC 4 - 3", 54: "FC 4 - 4",
            55: "FC 5", 56: "FC 5 - 1", 57: "FC 5 - 2", 58: "FC 5 - 3", 59: "FC 5 - 4",
            60: "FC 6", 61: "FC 6 - 1", 62: "FC 6 - 2", 63: "FC 6 - 3", 64: "FC 6 - 4",
            65: "FC 7", 66: "FC 7 - 1", 67: "FC 7 - 2", 68: "FC 7 - 3", 69: "FC 7 - 4",
            70: "FC 8", 71: "FC 8 - 1", 72: "FC 8 - 2", 73: "FC 8 - 3", 74: "FC 8 - 4",
            75: "FC 9", 76: "FC 9 - 1", 77: "FC 9 - 2", 78: "FC 9 - 3", 79: "FC 9 - 4",
            80: "FC 10", 81: "FC 10 - 1", 82: "FC 10 - 2", 83: "FC 10 - 3", 84: "FC 10 - 4"
        }

    def cog_unload(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    @discord.app_commands.command(name='w', description='Fetches user info using fid.')
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
            logger.error(f"Error fetching user info for FID {fid}: {e}")
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching user info.")


async def setup(bot):
    await bot.add_cog(WCommand(bot))
