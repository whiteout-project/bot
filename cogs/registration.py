import discord
from discord.ext import commands
import hashlib
import sqlite3
import aiohttp
import time
import ssl

class RegisterSettingsView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        
    def change_settings(self, enabled: bool):
        try:
            conn = sqlite3.connect("db/settings.sqlite")
            cursor = conn.cursor()
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='register_settings'")
            table_exists = cursor.fetchone()
            
            if not table_exists:
                cursor.execute("CREATE TABLE register_settings (enabled BOOLEAN)")
                cursor.execute("INSERT INTO register_settings VALUES (?)", (enabled,))
            else:
                cursor.execute("UPDATE register_settings SET enabled = ? WHERE rowid = 1", (enabled,))
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error updating register settings: {e}")

    @discord.ui.button(
        label="Enable",
        emoji="✅",
        style=discord.ButtonStyle.success,
        custom_id="enable_register",
        row=0
    )
    async def enable_register_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.change_settings(True)
            await interaction.response.send_message("✅ Registration has been enabled.", ephemeral=True)
        except Exception as _:
            await interaction.response.send_message("❌ An error occurred while enabling registration.", ephemeral=True)
            
    @discord.ui.button(
        label="Disable",
        emoji="❌",
        style=discord.ButtonStyle.danger,
        custom_id="disable_register",
        row=0
    )
    async def disable_register_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.change_settings(False)
            await interaction.response.send_message("❌ Registration has been disabled.", ephemeral=True)
        except Exception as _:
            await interaction.response.send_message("❌ An error occurred while disabling registration.", ephemeral=True)

class Register(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        self.conn_alliance = sqlite3.connect("db/alliance.sqlite")
        self.c_alliance = self.conn_alliance.cursor()
        
        self.conn_users = sqlite3.connect("db/users.sqlite")
        self.c_users = self.conn_users.cursor()
    
    def cog_unload(self):
        self.conn_alliance.close()
        self.conn_users.close()

    async def show_settings_menu(self, interaction: discord.Interaction):
        if not self.is_global_admin(interaction.user.id):
            await interaction.response.send_message(
                "❌ You do not have permission to access this command.",
                ephemeral=True
            )
            return
        
        view = RegisterSettingsView(self)
        
        await interaction.response.send_message(
            "Choose an option to enable or disable the registration system:",
            view=view,
            ephemeral=True
        )
        
    def is_global_admin(self, user_id: int) -> bool:
        with sqlite3.connect("db/settings.sqlite") as settings_db:
            cursor = settings_db.cursor()
            cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (user_id,))
            result = cursor.fetchone()
            
            return not (not result or result[0] == 1)
        
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
        URL = "https://wos-giftcode-api.centurygame.com/api/player"
        HEADERS = {"Content-Type": "application/x-www-form-urlencoded"}
        
        ssl_context = ssl.create_default_context()
        session = aiohttp.ClientSession()
        
        data_nosign = f"fid={fid}&time={time.time_ns()}"
        sign = hashlib.md5((data_nosign + "tB87#kPtkxqOS2").encode()).hexdigest()
        data = f"sign={sign}&{data_nosign}"

        try:
            async with session.post(
                url=URL,
                data=data,
                headers=HEADERS,
                ssl=ssl_context
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    raise Exception(f"Failed to fetch user data: {response.status}")
        finally:
            await session.close()
         
    @discord.app_commands.command(
        name="register",
        description="Registers yourself into the bot's database.",
    )
    @discord.app_commands.describe(
        fid="Your In-Game ID",
        alliance="Your Alliance Name"
    )
    @discord.app_commands.autocomplete(alliance=alliance_autocomplete)
    async def register(self, interaction: discord.Interaction, fid: int, alliance: int):
        if not self.is_registration_enabled():
            await interaction.response.send_message(
                "❌ Registration is currently disabled.",
                ephemeral=True
            )
            return
        
        try:
            user_data = (await self.fetch_user(fid))["data"]
        except Exception as _:
            await interaction.response.send_message(
                "❌ Failed to fetch user data. Please try again later.",
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
    await bot.add_cog(Register(bot))