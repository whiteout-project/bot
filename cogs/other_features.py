import discord
from discord.ext import commands
import sqlite3
from cogs import prettification_is_my_purpose as pimp
class OtherFeatures(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
    async def show_other_features_menu(self, interaction: discord.Interaction):
        try:
            embed = discord.Embed(
                title=f"{pimp.pinIcon} Other Features",
                description=(
                    f"This section was created according to users' requests:\n\n"
                    f"**Available Operations**\n"
                    f"{pimp.divider1}\n\n"
                    f"{pimp.anounceIcon} **Notification System**\n"
                    f"└ Event notification system\n"
                    f"└ Not just for Bear! Use it for any event:\n"
                    f"   Bear - KE - Frostfire - CJ and everything else\n"
                    f"└ Add unlimited notifications\n\n"
                    f"{pimp.fidIcon} **ID Channel**\n"
                    f"└ Create and manage ID channels\n"
                    f"└ Automatic ID verification system\n"
                    f"└ Custom channel settings\n\n"
                    f"{pimp.listIcon} **Registration System**\n"
                    f"└ Enable/disable user self-registration (Global Admin only)\n"
                    f"└ Users can /register to add themselves based on ID\n\n"
                    f"{pimp.listIcon} **Attendance System**\n"
                    f"└ Manage event attendance records\n"
                    f"└ View detailed attendance reports\n"
                    f"└ Export attendance data to CSV, TSV, HTML\n\n"
                    f"{pimp.hourglassIcon} **Minister Scheduling**\n"
                    f"└ Manage your state minister appointments\n"
                    f"└ Schedule Construction, Research, Training days\n"
                    f"└ Configure minister log channels\n\n"
                    f"{pimp.listIcon} **Backup System**\n"
                    f"└ Automatic database backup\n"
                    f"└ Send backups to your DMs\n"
                    f"└ Only for Global Admin\n\n"
                    f"{pimp.divider1}"
                ),
                color=discord.Color.blue()
            )
            
            view = OtherFeaturesView(self)
            
            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                pass
                
        except Exception as e:
            print(f"Error in show_other_features_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} An error occurred. Please try again.",
                    ephemeral=True
                )

class OtherFeaturesView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Notification System",
        emoji=f"{pimp.anounceIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="bear_trap",
        row=0
    )
    async def bear_trap_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            bear_trap_cog = self.cog.bot.get_cog("BearTrap")
            if bear_trap_cog:
                await bear_trap_cog.show_bear_trap_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} Bear Trap module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Bear Trap menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading Bear Trap menu.",
                ephemeral=True
            )

    @discord.ui.button(
        label="ID Channel",
        emoji=f"{pimp.fidIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="id_channel",
        row=0
    )
    async def id_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            id_channel_cog = self.cog.bot.get_cog("IDChannel")
            if id_channel_cog:
                await id_channel_cog.show_id_channel_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} ID Channel module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading ID Channel menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading ID Channel menu.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Registration System",
        emoji=f"{pimp.listIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="registration_system",
        row=1
    )
    async def registration_system_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            register_cog = self.cog.bot.get_cog("Register")
            if register_cog:
                await register_cog.show_settings_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} Registration System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Registration System menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading Registration System menu.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Attendance System",
        emoji=f"{pimp.listIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="attendance_system",
        row=1
    )
    async def attendance_system_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            attendance_cog = self.cog.bot.get_cog("Attendance")
            if attendance_cog:
                await attendance_cog.show_attendance_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} Attendance System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Attendance System menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading Attendance System menu.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Minister Scheduling",
        emoji=f"{pimp.hourglassIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="minister_channels",
        row=2
    )
    async def minister_channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            minister_menu_cog = self.cog.bot.get_cog("MinisterMenu")
            if minister_menu_cog:
                await minister_menu_cog.show_minister_channel_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} Minister Scheduling module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Minister Scheduling menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading Minister Scheduling menu.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Backup System",
        emoji=f"{pimp.listIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="backup_system",
        row=2
    )
    async def backup_system_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            backup_cog = self.cog.bot.get_cog("BackupOperations")
            if backup_cog:
                await backup_cog.show_backup_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} Backup System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Backup System menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while loading Backup System menu.",
                ephemeral=True
            )
            
    @discord.ui.button(
        label="Main Menu",
        emoji=f"{pimp.homeIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu",
        row=3
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            alliance_cog = self.cog.bot.get_cog("Alliance")
            if alliance_cog:
                await alliance_cog.show_main_menu(interaction)
        except Exception as e:
            print(f"Error returning to main menu: {e}")
            await interaction.response.send_message(
                f"{pimp.deniedIcon} An error occurred while returning to main menu.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(OtherFeatures(bot))