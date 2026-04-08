"""
Centralized menu system that handles all main menu logic and routing.
"""

import discord
from discord.ext import commands
import logging
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message

logger = logging.getLogger('bot')


class MainMenu(commands.Cog):
    """Centralized main menu cog for bot navigation."""

    def __init__(self, bot):
        self.bot = bot

    async def show_main_menu(self, interaction: discord.Interaction):
        """Display the main settings menu - entry point for all navigation."""
        try:
            embed = discord.Embed(
                title=f"{theme.settingsIcon} Settings Menu",
                description=(
                    f"Welcome to the bot settings. Select a category to get started:\n\n"
                    f"**Menu Categories**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance Management**\n"
                    f"└ Manage alliances, members, and registration\n\n"
                    f"{theme.giftIcon} **Gift Codes**\n"
                    f"└ Manage gift codes and rewards\n\n"
                    f"{theme.bellIcon} **Notifications**\n"
                    f"└ Event notification system for Bear, KE, and more\n\n"
                    f"{theme.listIcon} **Attendance**\n"
                    f"└ Track and export event attendance\n\n"
                    f"{theme.chartIcon} **Bear Tracking**\n"
                    f"└ Track bear hunt damage and view statistics\n\n"
                    f"{theme.ministerIcon} **Minister Scheduling**\n"
                    f"└ Manage state minister appointments\n\n"
                    f"{theme.paletteIcon} **Themes**\n"
                    f"└ Customize bot icons and colors\n\n"
                    f"{theme.settingsIcon} **Settings**\n"
                    f"└ Bot configuration and permissions\n\n"
                    f"{theme.robotIcon} **Maintenance**\n"
                    f"└ Updates, backups, and support\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = MainMenuView(self)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_main_menu: {e}")
            print(f"Error in show_main_menu: {e}")

    async def show_alliance_management(self, interaction: discord.Interaction):
        """Display the Alliance Management sub-menu."""
        try:
            embed = discord.Embed(
                title=f"{theme.allianceIcon} Alliance Management",
                description=(
                    f"Manage your alliances and members:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance Setup**\n"
                    f"└ Add, edit, delete, and view alliances\n\n"
                    f"{theme.userIcon} **Self-Registration**\n"
                    f"└ ID Channel and user registration settings\n\n"
                    f"{theme.membersIcon} **Member Management**\n"
                    f"└ Add, remove, transfer, and view members\n\n"
                    f"{theme.listIcon} **Member History**\n"
                    f"└ View furnace and nickname changes\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = AllianceManagementView(self)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_alliance_management: {e}")
            print(f"Error in show_alliance_management: {e}")

    async def show_self_registration(self, interaction: discord.Interaction):
        """Display the Self-Registration sub-menu (ID Channel + Registration)."""
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.userIcon} Self-Registration",
                description=(
                    f"Configure user self-registration and ID verification:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.fidIcon} **ID Channel**\n"
                    f"└ Create and manage ID verification channels\n"
                    f"└ Automatic ID verification system\n\n"
                    f"{theme.editListIcon} **Registration System**\n"
                    f"└ Enable/disable user self-registration\n"
                    f"└ Users can /register to add themselves\n"
                    f"└ Global Admin only\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = SelfRegistrationView(self, is_global)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_self_registration: {e}")
            print(f"Error in show_self_registration: {e}")

    async def show_settings(self, interaction: discord.Interaction):
        """Display the Settings sub-menu."""
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.settingsIcon} Settings",
                description=(
                    f"Configure bot settings and permissions:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.crownIcon} **Permissions**\n"
                    f"└ Manage bot administrators (Global Admin only)\n\n"
                    f"{theme.messageIcon} **Alliance Control Messages**\n"
                    f"└ Toggle control information messages\n\n"
                    f"{theme.settingsIcon} **Control Settings**\n"
                    f"└ Configure alliance control behaviors\n\n"
                    f"{theme.documentIcon} **Log System**\n"
                    f"└ Configure log channels for alliances\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = SettingsView(self, is_global)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_settings: {e}")
            print(f"Error in show_settings: {e}")

    async def show_permissions(self, interaction: discord.Interaction):
        """Display the Permissions sub-menu (admin management)."""
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)

            if not is_global:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only global administrators can access permissions management.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"{theme.crownIcon} Permissions",
                description=(
                    f"Manage bot administrators and their permissions:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.addIcon} **Add Admin**\n"
                    f"└ Add a new administrator\n\n"
                    f"{theme.deleteIcon} **Remove Admin**\n"
                    f"└ Remove an administrator\n\n"
                    f"{theme.listIcon} **View Administrators**\n"
                    f"└ List all current administrators\n\n"
                    f"{theme.linkIcon} **Assign Alliance to Admin**\n"
                    f"└ Assign specific alliances to an admin\n\n"
                    f"{theme.trashIcon} **Delete Admin Permissions**\n"
                    f"└ Remove alliance assignments from admin\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = PermissionsView(self)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_permissions: {e}")
            print(f"Error in show_permissions: {e}")

    async def show_maintenance(self, interaction: discord.Interaction):
        """Display the Maintenance sub-menu."""
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.robotIcon} Maintenance",
                description=(
                    f"Bot maintenance and support options:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.refreshIcon} **Check for Updates**\n"
                    f"└ Check for and manage bot updates (Global Admin only)\n\n"
                    f"{theme.archiveIcon} **Backup System**\n"
                    f"└ Backup and restore database (Global Admin only)\n\n"
                    f"{theme.cleanIcon} **DB Maintenance**\n"
                    f"└ Database cleanup and optimization (Global Admin only)\n\n"
                    f"{theme.supportIcon} **Request Support**\n"
                    f"└ Get help and support information\n\n"
                    f"{theme.infoIcon} **About Project**\n"
                    f"└ View project information\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = MaintenanceView(self, is_global)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_maintenance: {e}")
            print(f"Error in show_maintenance: {e}")


# ============================================================================
# Main Menu View
# ============================================================================

class MainMenuView(discord.ui.View):
    """Main menu with 8 category buttons."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Alliance Management",
        emoji=theme.allianceIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_management",
        row=0
    )
    async def alliance_management_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_alliance_management(interaction)

    @discord.ui.button(
        label="Gift Codes",
        emoji=theme.giftIcon,
        style=discord.ButtonStyle.primary,
        custom_id="gift_codes",
        row=0
    )
    async def gift_codes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            gift_cog = self.cog.bot.get_cog("GiftOperations")
            if gift_cog:
                await gift_cog.show_gift_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Gift Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Gift Codes menu: {e}")
            print(f"Error loading Gift Codes menu: {e}")

    @discord.ui.button(
        label="Notifications",
        emoji=theme.bellIcon,
        style=discord.ButtonStyle.primary,
        custom_id="notifications",
        row=0
    )
    async def notifications_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            notification_cog = self.cog.bot.get_cog("NotificationSystem")
            if notification_cog:
                await notification_cog.show_notification_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Notification System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Notifications menu: {e}")
            print(f"Error loading Notifications menu: {e}")

    @discord.ui.button(
        label="Attendance",
        emoji=theme.listIcon,
        style=discord.ButtonStyle.primary,
        custom_id="attendance_tracking",
        row=1
    )
    async def attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            attendance_cog = self.cog.bot.get_cog("Attendance")
            if attendance_cog:
                await attendance_cog.show_attendance_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Attendance System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Attendance menu: {e}")
            print(f"Error loading Attendance menu: {e}")

    @discord.ui.button(
        label="Bear Tracking",
        emoji=theme.chartIcon,
        style=discord.ButtonStyle.primary,
        custom_id="bear_tracking",
        row=1
    )
    async def bear_tracking_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            bear_cog = self.cog.bot.get_cog("BearTrack")
            if bear_cog:
                await bear_cog.show_bear_track_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Bear Tracking module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Bear Tracking menu: {e}")
            print(f"Error loading Bear Tracking menu: {e}")

    @discord.ui.button(
        label="Minister Scheduling",
        emoji=theme.ministerIcon,
        style=discord.ButtonStyle.primary,
        custom_id="minister_scheduling",
        row=1
    )
    async def minister_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            minister_cog = self.cog.bot.get_cog("MinisterMenu")
            if minister_cog:
                await minister_cog.show_minister_channel_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Minister Scheduling module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Minister menu: {e}")
            print(f"Error loading Minister menu: {e}")

    @discord.ui.button(
        label="Themes",
        emoji=theme.paletteIcon,
        style=discord.ButtonStyle.primary,
        custom_id="themes",
        row=2
    )
    async def themes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            theme_cog = self.cog.bot.get_cog("Theme")
            if theme_cog:
                await theme_cog.show_theme_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Theme module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Theme menu: {e}")
            print(f"Error loading Theme menu: {e}")

    @discord.ui.button(
        label="Settings",
        emoji=theme.settingsIcon,
        style=discord.ButtonStyle.primary,
        custom_id="settings",
        row=2
    )
    async def settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_settings(interaction)

    @discord.ui.button(
        label="Maintenance",
        emoji=theme.robotIcon,
        style=discord.ButtonStyle.primary,
        custom_id="maintenance",
        row=2
    )
    async def maintenance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_maintenance(interaction)


# ============================================================================
# Alliance Management View
# ============================================================================

class AllianceManagementView(discord.ui.View):
    """Alliance Management sub-menu."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Alliance Setup",
        emoji=theme.allianceIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_setup",
        row=0
    )
    async def alliance_setup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            alliance_cog = self.cog.bot.get_cog("Alliance")
            if alliance_cog:
                # Show alliance operations menu (add/edit/delete/view)
                await alliance_cog.show_alliance_operations(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Alliance Setup: {e}")
            print(f"Error loading Alliance Setup: {e}")

    @discord.ui.button(
        label="Self-Registration",
        emoji=theme.userIcon,
        style=discord.ButtonStyle.primary,
        custom_id="self_registration",
        row=0
    )
    async def self_registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_self_registration(interaction)

    @discord.ui.button(
        label="Member Management",
        emoji=theme.membersIcon,
        style=discord.ButtonStyle.primary,
        custom_id="member_management",
        row=1
    )
    async def member_management_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            member_ops_cog = self.cog.bot.get_cog("AllianceMemberOperations")
            if member_ops_cog:
                await member_ops_cog.handle_member_operations(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Member Management module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Member Management: {e}")
            print(f"Error loading Member Management: {e}")

    @discord.ui.button(
        label="Member History",
        emoji=theme.listIcon,
        style=discord.ButtonStyle.primary,
        custom_id="member_history",
        row=1
    )
    async def member_history_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            history_cog = self.cog.bot.get_cog("AllianceHistory")
            if history_cog:
                await history_cog.show_alliance_history_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance History module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Member History: {e}")
            print(f"Error loading Member History: {e}")

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu_from_alliance",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


# ============================================================================
# Self-Registration View
# ============================================================================

class SelfRegistrationView(discord.ui.View):
    """Self-Registration sub-menu (ID Channel + Registration)."""

    def __init__(self, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.is_global = is_global

        # Disable Registration System button for non-global admins
        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.label == "Registration System":
                child.disabled = not is_global

    @discord.ui.button(
        label="ID Channel",
        emoji=theme.fidIcon,
        style=discord.ButtonStyle.primary,
        custom_id="id_channel",
        row=0
    )
    async def id_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            id_channel_cog = self.cog.bot.get_cog("AllianceIDChannel")
            if id_channel_cog:
                await id_channel_cog.show_id_channel_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} ID Channel module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading ID Channel menu: {e}")
            print(f"Error loading ID Channel menu: {e}")

    @discord.ui.button(
        label="Registration System",
        emoji=theme.editListIcon,
        style=discord.ButtonStyle.primary,
        custom_id="registration_system",
        row=0
    )
    async def registration_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            register_cog = self.cog.bot.get_cog("AllianceRegistration")
            if register_cog:
                await register_cog.show_settings_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Registration System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Registration System menu: {e}")
            print(f"Error loading Registration System menu: {e}")

    @discord.ui.button(
        label="Back",
        emoji=theme.backIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="back_from_registration",
        row=1
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_alliance_management(interaction)


# ============================================================================
# Settings View
# ============================================================================

class SettingsView(discord.ui.View):
    """Settings sub-menu."""

    def __init__(self, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.is_global = is_global

        # Disable Permissions and Alliance Control Messages for non-global admins
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.label == "Permissions":
                    child.disabled = not is_global
                elif child.label == "Alliance Control Messages":
                    child.disabled = not is_global

    @discord.ui.button(
        label="Permissions",
        emoji=theme.crownIcon,
        style=discord.ButtonStyle.primary,
        custom_id="permissions",
        row=0
    )
    async def permissions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_permissions(interaction)

    @discord.ui.button(
        label="Alliance Control Messages",
        emoji=theme.messageIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_control_messages",
        row=0
    )
    async def control_messages_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Control Settings",
        emoji=theme.settingsIcon,
        style=discord.ButtonStyle.primary,
        custom_id="control_settings",
        row=1
    )
    async def control_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Log System",
        emoji=theme.documentIcon,
        style=discord.ButtonStyle.primary,
        custom_id="log_system",
        row=1
    )
    async def log_system_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by alliance_logs.py on_interaction listener
        pass

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu_from_settings",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


# ============================================================================
# Permissions View
# ============================================================================

class PermissionsView(discord.ui.View):
    """Permissions sub-menu (admin management)."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Add Admin",
        emoji=theme.addIcon,
        style=discord.ButtonStyle.success,
        custom_id="add_admin",
        row=0
    )
    async def add_admin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Remove Admin",
        emoji=theme.deleteIcon,
        style=discord.ButtonStyle.danger,
        custom_id="remove_admin",
        row=0
    )
    async def remove_admin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="View Administrators",
        emoji=theme.listIcon,
        style=discord.ButtonStyle.primary,
        custom_id="view_administrators",
        row=0
    )
    async def view_admins_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Assign Alliance to Admin",
        emoji=theme.linkIcon,
        style=discord.ButtonStyle.primary,
        custom_id="assign_alliance",
        row=1
    )
    async def assign_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Delete Admin Permissions",
        emoji=theme.trashIcon,
        style=discord.ButtonStyle.danger,
        custom_id="view_admin_permissions",
        row=1
    )
    async def delete_permissions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Back",
        emoji=theme.backIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_settings",
        row=2
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_settings(interaction)


# ============================================================================
# Maintenance View
# ============================================================================

class MaintenanceView(discord.ui.View):
    """Maintenance sub-menu."""

    def __init__(self, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.is_global = is_global

        # Disable Global Admin only buttons for non-global admins
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.label in ["Check for Updates", "Backup System", "DB Maintenance"]:
                    child.disabled = not is_global

    @discord.ui.button(
        label="Check for Updates",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.primary,
        custom_id="check_updates",
        row=0
    )
    async def check_updates_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Backup System",
        emoji=theme.archiveIcon,
        style=discord.ButtonStyle.primary,
        custom_id="backup_system",
        row=0
    )
    async def backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            backup_cog = self.cog.bot.get_cog("BotBackup")
            if backup_cog:
                await backup_cog.show_backup_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Backup System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Backup System: {e}")
            print(f"Error loading Backup System: {e}")

    @discord.ui.button(
        label="Bot Health",
        emoji=theme.heartIcon,
        style=discord.ButtonStyle.primary,
        custom_id="bot_health",
        row=0
    )
    async def bot_health_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            health_cog = self.cog.bot.get_cog("BotHealth")
            if health_cog:
                await health_cog.show_health_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Bot Health module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Bot Health: {e}")
            print(f"Error loading Bot Health: {e}")

    @discord.ui.button(
        label="Request Support",
        emoji=theme.supportIcon,
        style=discord.ButtonStyle.primary,
        custom_id="request_support",
        row=1
    )
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            support_cog = self.cog.bot.get_cog("SupportOperations")
            if support_cog:
                await support_cog.show_support_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Support Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Support menu: {e}")
            print(f"Error loading Support menu: {e}")

    @discord.ui.button(
        label="About Project",
        emoji=theme.infoIcon,
        style=discord.ButtonStyle.primary,
        custom_id="about_project",
        row=1
    )
    async def about_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            support_cog = self.cog.bot.get_cog("BotSupport")
            if support_cog:
                await support_cog.show_about_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Support Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading About menu: {e}")
            print(f"Error loading About menu: {e}")

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu_from_maintenance",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


async def setup(bot):
    await bot.add_cog(MainMenu(bot))
