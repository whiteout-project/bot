"""
Alliance registration flow. Lets users link their game account to Discord.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from .pimp_my_bot import theme
from .login_handler import LoginHandler

logger = logging.getLogger('alliance')


class AllianceRegistration(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.conn_alliance = sqlite3.connect("db/alliance.sqlite", timeout=30.0, check_same_thread=False)
        self.c_alliance = self.conn_alliance.cursor()

        self.conn_users = sqlite3.connect("db/users.sqlite", timeout=30.0, check_same_thread=False)
        self.c_users = self.conn_users.cursor()

    def cog_unload(self):
        self.conn_alliance.close()
        self.conn_users.close()

    def is_already_in_users(self, fid: int) -> bool:
        """Check if a user with the given fid is already registered."""
        self.c_users.execute("SELECT 1 FROM users WHERE fid = ?", (fid,))
        return self.c_users.fetchone() is not None
        
    def set_registration_enabled(self, enabled: bool) -> None:
        """Persist the global self-registration toggle to settings.sqlite."""
        try:
            with sqlite3.connect("db/settings.sqlite") as conn:
                cursor = conn.cursor()
                cursor.execute("CREATE TABLE IF NOT EXISTS register_settings (enabled BOOLEAN)")
                cursor.execute("SELECT COUNT(*) FROM register_settings")
                exists = cursor.fetchone()[0] > 0
                if exists:
                    cursor.execute(
                        "UPDATE register_settings SET enabled = ? WHERE rowid = 1",
                        (enabled,),
                    )
                else:
                    cursor.execute(
                        "INSERT INTO register_settings VALUES (?)", (enabled,)
                    )
                conn.commit()
        except Exception as e:
            logger.error(f"Error updating register settings: {e}")
            print(f"Error updating register settings: {e}")

    def is_registration_enabled(self) -> bool:
        """Check if registration is enabled in the settings database."""
        try:
            conn = sqlite3.connect("db/settings.sqlite")
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='register_settings'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                conn.close()
                return False
            
            cursor.execute("SELECT enabled FROM register_settings WHERE rowid = 1")
            result = cursor.fetchone()
            
            conn.close()
            
            return bool(result[0]) if result else False
            
        except Exception as e:
            logger.error(f"Error checking registration status: {e}")
            print(f"Error checking registration status: {e}")
            return False
        
    async def alliance_autocomplete(self, interaction: discord.Interaction, current: str):
        self.c_alliance.execute("SELECT alliance_id, name FROM alliance_list")
        alliances = self.c_alliance.fetchall()
        
        return [
            discord.app_commands.Choice(name=name, value=alliance_id)
            for alliance_id, name in alliances if current.lower() in name.lower()
        ][:25]
        
    async def fetch_user(self, fid: int):
        result = await LoginHandler().fetch_player_data(str(fid))

        if result['status'] == 'success':
            return {"msg": "success", "data": result['data']}
        elif result['status'] == 'rate_limited':
            raise Exception("RATE_LIMITED")
        elif result['status'] == 'not_found':
            return {"msg": "role not exist"}
        else:
            raise Exception(result.get('error_message', 'Failed to fetch user data'))
         
    @discord.app_commands.command(
        name="register",
        description="Registers yourself into the bot's database.",
    )
    @discord.app_commands.describe(
        fid="Your In-Game ID",
        alliance="Your Alliance Name"
    )
    @discord.app_commands.rename(fid="id")
    @discord.app_commands.autocomplete(alliance=alliance_autocomplete)
    async def register(self, interaction: discord.Interaction, fid: int, alliance: int):
        if not self.is_registration_enabled():
            await interaction.response.send_message(
                f"{theme.deniedIcon} Registration is currently disabled.",
                ephemeral=True
            )
            return
        
        if self.is_already_in_users(fid):
            await interaction.response.send_message(
                f"{theme.deniedIcon} You are already registered in the bot's database.",
                ephemeral=True
            )
            return
        
        try:
            api_response = await self.fetch_user(fid)
            
            if api_response.get("msg") != "success":
                error_msg = api_response.get("msg", "Unknown error")
                
                if "role not exist" in error_msg.lower():
                    display_msg = f"{theme.deniedIcon} Invalid ID. Please try again."
                else:
                    display_msg = f"{theme.deniedIcon} Invalid ID: {error_msg}"
                
                await interaction.response.send_message(
                    display_msg,
                    ephemeral=True
                )
                return
            
            if "data" not in api_response:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid response from server. Please try again later.",
                    ephemeral=True
                )
                return
                
            user_data = api_response["data"]
            
        except Exception as e:
            if str(e) == "RATE_LIMITED":
                await interaction.response.send_message(
                    "⏳ Rate limit reached. Please wait a minute before trying again.",
                    ephemeral=True
                )
            else:
                logger.error(f"Error fetching user data for ID {fid}: {e}")
                print(f"Error fetching user data for ID {fid}: {e}")
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Failed to fetch user data. Please try again later.",
                    ephemeral=True
                )
            return

        nickname = user_data["nickname"]
        furnace_lv = user_data["stove_lv"]
        kid = user_data["kid"]
        stove_lv_content = user_data.get("stove_lv_content")

        self.c_users.execute(
            "INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance) VALUES (?, ?, ?, ?, ?, ?)", 
            (fid, nickname, furnace_lv, kid, stove_lv_content, alliance)
        )
        
        self.conn_users.commit()    
        
        await interaction.response.send_message("Registration successful! You are now in the bot's database.", ephemeral=True)
        
async def setup(bot):
    await bot.add_cog(AllianceRegistration(bot))