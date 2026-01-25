import discord
from discord.ext import commands
import sqlite3
import time
import hashlib
import aiohttp
from aiohttp_socks import ProxyConnector
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

SECRET = 'tB87#kPtkxqOS2'

class UserFilterModal(discord.ui.Modal, title="Filter Users"):
    def __init__(self, parent_view):
        super().__init__()
        self.parent_view = parent_view
        
        self.filter_input = discord.ui.TextInput(
            label="Filter by ID or Name",
            placeholder="Enter ID or nickname (partial match supported)",
            required=False,
            max_length=100,
            default=self.parent_view.filter_text
        )
        self.add_item(self.filter_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        self.parent_view.filter_text = self.filter_input.value.strip()
        self.parent_view.page = 0  # Reset to first page when filtering
        self.parent_view.apply_filter()
        self.parent_view.update_select_menu()
        self.parent_view.update_navigation_buttons()
        await self.parent_view.update_embed(interaction)

class FilteredUserSelectView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, users, booked_times, page=0):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.users = users  # List of (fid, nickname, alliance_id) tuples
        self.booked_times = booked_times  # Dict of time: (fid, alliance) for this activity
        self.page = page
        self.filter_text = ""
        self.filtered_users = self.users.copy()
        self.max_page = (len(self.filtered_users) - 1) // 25 if self.filtered_users else 0

        # Get list of IDs that are already booked for this activity
        self.booked_fids = {fid for time, (fid, alliance) in self.booked_times.items() if fid}
        
        self.update_select_menu()
        self.update_navigation_buttons()
    
    def apply_filter(self):
        """Apply the filter to the users list"""
        if not self.filter_text:
            self.filtered_users = self.users.copy()
        else:
            filter_lower = self.filter_text.lower()
            self.filtered_users = []
            
            for fid, nickname, alliance_id in self.users:
                # Check if filter matches ID or nickname (partial, case-insensitive)
                if filter_lower in str(fid).lower() or filter_lower in nickname.lower():
                    self.filtered_users.append((fid, nickname, alliance_id))
        
        # Update max page based on filtered results
        self.max_page = (len(self.filtered_users) - 1) // 25 if self.filtered_users else 0
        
        # Ensure current page is valid
        if self.page > self.max_page:
            self.page = self.max_page
    
    def update_navigation_buttons(self):
        """Update the state of navigation and filter buttons"""
        # Update navigation button states
        prev_button = next((item for item in self.children if hasattr(item, 'custom_id') and item.custom_id == 'prev_page'), None)
        next_button = next((item for item in self.children if hasattr(item, 'custom_id') and item.custom_id == 'next_page'), None)
        clear_button = next((item for item in self.children if hasattr(item, 'custom_id') and item.custom_id == 'clear_filter'), None)
        
        if prev_button:
            prev_button.disabled = self.page == 0
        if next_button:
            next_button.disabled = self.page >= self.max_page
        if clear_button:
            clear_button.disabled = not bool(self.filter_text)
    
    def update_select_menu(self):
        """Update the user selection dropdown"""
        # Remove existing select menu
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)
        
        # Calculate page boundaries
        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.filtered_users))
        current_users = self.filtered_users[start_idx:end_idx]
        
        if not current_users:
            # No users to display
            placeholder = "No users found" if self.filter_text else "No users available"
            select = discord.ui.Select(
                placeholder=placeholder,
                options=[discord.SelectOption(label="No users", value="none")],
                disabled=True
            )
        else:
            # Create options for users
            options = []
            for fid, nickname, alliance_id in current_users:
                # Check if user is already booked
                emoji = "📅" if fid in self.booked_fids else ""
                # Avoid nested f-strings for Python 3.9+ compatibility
                if emoji:
                    label = f"{emoji} {nickname} ({fid})"
                else:
                    label = f"{nickname} ({fid})"

                option = discord.SelectOption(
                    label=label[:100],  # Discord limit
                    value=str(fid)
                )
                options.append(option)
            
            select = discord.ui.Select(
                placeholder=f"Select a user... (Page {self.page + 1}/{self.max_page + 1})",
                options=options,
                min_values=1,
                max_values=1
            )
            
            select.callback = self.user_select_callback
        
        self.add_item(select)
    
    async def user_select_callback(self, interaction: discord.Interaction):
        """Handle user selection"""
        selected_fid = int(interaction.data['values'][0])
        
        # Find the selected user's data
        user_data = next((user for user in self.users if user[0] == selected_fid), None)
        if not user_data:
            await interaction.response.send_message(f"{theme.deniedIcon} User not found.", ephemeral=True)
            return
        
        fid, nickname, alliance_id = user_data
        
        # Check if user is already booked
        if fid in self.booked_fids:
            # Find their current time slot
            current_time = next((time for time, (booked_fid, _) in self.booked_times.items() if booked_fid == fid), None)
            await self.cog.show_time_selection(interaction, self.activity_name, str(fid), current_time)
        else:
            await self.cog.show_time_selection(interaction, self.activity_name, str(fid), None)
    
    @discord.ui.button(label="", style=discord.ButtonStyle.secondary, emoji=f"{theme.prevIcon}", custom_id="prev_page", row=1)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        self.update_navigation_buttons()
        await self.update_embed(interaction)

    @discord.ui.button(label="", style=discord.ButtonStyle.secondary, emoji=f"{theme.nextIcon}", custom_id="next_page", row=1)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        self.update_navigation_buttons()
        await self.update_embed(interaction)
    
    @discord.ui.button(label="Filter", style=discord.ButtonStyle.secondary, emoji=f"{theme.searchIcon}", custom_id="filter", row=1)
    async def filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = UserFilterModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="Clear", style=discord.ButtonStyle.danger, emoji=f"{theme.deniedIcon}", custom_id="clear_filter", row=1, disabled=True)
    async def clear_filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.filter_text = ""
        self.page = 0
        self.apply_filter()
        self.update_select_menu()
        self.update_navigation_buttons()
        await self.update_embed(interaction)
    
    @discord.ui.button(label="List", style=discord.ButtonStyle.secondary, emoji=f"{theme.listIcon}", custom_id="list", row=1)
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_current_schedule_list(interaction, self.activity_name)
    
    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_channel_menu(interaction)
    
    async def update_embed(self, interaction: discord.Interaction):
        """Update the main embed with current information"""
        # Get current stats
        total_booked = len(self.booked_fids)
        available_slots = 48 - total_booked
        
        # Create description based on filter status
        description = f"Select a user to manage their {self.activity_name} appointment.\n\n"
        
        if self.filter_text:
            description += f"**Filter:** `{self.filter_text}`\n"
            description += f"**Filtered Users:** {len(self.filtered_users)}/{len(self.users)}\n\n"
        
        description += (
            f"**Current Status**\n"
            f"{theme.upperDivider}\n"
            f"📅 **Booked Slots:** `{total_booked}/48`\n"
            f"{theme.timeIcon} **Available Slots:** `{available_slots}/48`\n"
            f"{theme.lowerDivider}\n\n"
            f"📅 = User already has a booking"
        )
        
        embed = discord.Embed(
            title=f"🧑‍💼 {self.activity_name} Management",
            description=description,
            color=theme.emColor1
        )
        
        try:
            await interaction.response.edit_message(embed=embed, view=self)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=self)

class ClearConfirmationView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, is_global_admin, alliance_ids):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.is_global_admin = is_global_admin
        self.alliance_ids = alliance_ids
    
    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji=f"{theme.verifiedIcon}")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        
        minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
        
        if self.is_global_admin:
            # Get all appointments to log before clearing
            self.cog.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (self.activity_name,))
            cleared_fids = {row[0]: (row[1], row[2]) for row in self.cog.svs_cursor.fetchall()}

            time_list, _ = minister_schedule_cog.generate_time_list(cleared_fids)

            # Split into chunks if too long for embed description (4096 char limit)
            header = f"**Previous {self.activity_name} schedule** (before clearing):"
            message_chunks = minister_schedule_cog.split_message_content(header, time_list, max_length=4000)

            for i, chunk in enumerate(message_chunks):
                title = f"Cleared {self.activity_name}" if i == 0 else f"Cleared {self.activity_name} (continued)"
                clear_list_embed = discord.Embed(
                    title=title,
                    description=chunk,
                    color=discord.Color.orange()
                )
                await minister_schedule_cog.send_embed_to_channel(clear_list_embed)

            # Clear all appointments
            self.cog.svs_cursor.execute("DELETE FROM appointments WHERE appointment_type=?", (self.activity_name,))
            self.cog.svs_conn.commit()
            
            cleared_count = len(cleared_fids)
            message = f"Cleared all {cleared_count} appointments for {self.activity_name}"
        else:
            # Get appointments for allowed alliances
            placeholders = ','.join('?' for _ in self.alliance_ids)
            query = f"SELECT fid FROM appointments WHERE appointment_type=? AND alliance IN ({placeholders})"
            self.cog.svs_cursor.execute(query, [self.activity_name] + self.alliance_ids)
            cleared_fids = [row[0] for row in self.cog.svs_cursor.fetchall()]
            
            # Clear alliance appointments
            query = f"DELETE FROM appointments WHERE appointment_type=? AND alliance IN ({placeholders})"
            self.cog.svs_cursor.execute(query, [self.activity_name] + self.alliance_ids)
            self.cog.svs_conn.commit()
            
            cleared_count = len(cleared_fids)
            message = f"Cleared {cleared_count} alliance appointments for {self.activity_name}"
        
        # Send log
        if minister_schedule_cog and cleared_count > 0:
            embed = discord.Embed(
                title=f"Appointments Cleared - {self.activity_name}",
                description=f"{cleared_count} appointments were cleared",
                color=theme.emColor2
            )
            embed.set_author(name=f"Cleared by {interaction.user.display_name}", icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
            await minister_schedule_cog.send_embed_to_channel(embed)
            await self.cog.update_channel_message(self.activity_name)
        
        # Return to settings menu with success message
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Minister Settings",
            description=(
                f"{theme.verifiedIcon} **{message}**\n\n"
                f"Administrative settings for minister scheduling:\n\n"
                f"Available Actions\n"
                f"{theme.upperDivider}\n\n"
                f"{theme.editListIcon} **Update Names**\n"
                f"└ Update nicknames from API for booked users\n\n"
                f"{theme.listIcon} **Schedule List Type**\n"
                f"└ Change the type of schedule list message when adding/removing people\n\n"
                f"{theme.calendarIcon} **Delete All Reservations**\n"
                f"└ Clear appointments for a specific day\n\n"
                f"{theme.announceIcon} **Clear Channels**\n"
                f"└ Clear channel configurations\n\n"
                f"{theme.fidIcon} **Delete Server ID**\n"
                f"└ Remove configured server from database\n\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor3
        )
        
        view = MinisterSettingsView(self.cog.bot, self.cog, self.is_global_admin)
        await interaction.followup.send(embed=embed, view=view)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji=f"{theme.deniedIcon}")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_filtered_user_select(interaction, self.activity_name)

class ActivitySelectView(discord.ui.View):
    def __init__(self, bot, cog, action_type):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.action_type = action_type  # "update_names" or "clear_reservations"
    
    @discord.ui.select(
        placeholder="Select an activity day...",
        options=[
            discord.SelectOption(label="Construction Day", value="Construction Day", emoji=theme.constructionIcon),
            discord.SelectOption(label="Research Day", value="Research Day", emoji=theme.researchIcon),
            discord.SelectOption(label="Troops Training Day", value="Troops Training Day", emoji=theme.trainingIcon)
        ]
    )
    async def activity_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        activity_name = select.values[0]
        
        if self.action_type == "update_names":
            await self.cog.update_minister_names(interaction, activity_name)
        elif self.action_type == "clear_reservations":
            await self.cog.show_clear_confirmation(interaction, activity_name)
    
    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}")
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_settings_menu(interaction)

class MinisterSettingsView(discord.ui.View):
    def __init__(self, bot, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog
        self.is_global = is_global

        # Disable global-admin-only buttons for non-global admins
        if not is_global:
            for child in self.children:
                if isinstance(child, discord.ui.Button) and child.label in [
                    "Schedule List Type", "Time Slot Mode",
                    "Delete All Reservations", "Clear Channels", "Delete Server ID"
                ]:
                    child.disabled = True

    @discord.ui.button(label="Update Names", style=discord.ButtonStyle.secondary, emoji=f"{theme.editListIcon}", row=1)
    async def update_names(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is admin
        if not await self.cog.is_admin(interaction.user.id):
            await interaction.response.send_message(f"{theme.deniedIcon} You do not have permission to update names.", ephemeral=True)
            return

        await self.cog.show_activity_selection_for_update(interaction)

    @discord.ui.button(label="Schedule List Type", style=discord.ButtonStyle.secondary, emoji=f"{theme.listIcon}", row=1)
    async def list_type(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is admin
        if not await self.cog.is_admin(interaction.user.id):
            await interaction.response.send_message(f"{theme.deniedIcon} You do not have permission to update names.", ephemeral=True)
            return

        await self.cog.show_activity_selection_for_list_type(interaction)

    @discord.ui.button(label="Time Slot Mode", style=discord.ButtonStyle.secondary, emoji=f"{theme.timeIcon}", row=1)
    async def time_slot_mode(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is admin
        if not await self.cog.is_admin(interaction.user.id):
            await interaction.response.send_message(f"{theme.deniedIcon} You do not have permission to change time slot mode.", ephemeral=True)
            return

        await self.cog.show_time_slot_mode_menu(interaction)
    
    @discord.ui.button(label="Delete All Reservations", style=discord.ButtonStyle.danger, emoji=f"{theme.calendarIcon}", row=2)
    async def clear_reservations(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is global admin
        is_admin, is_global_admin, _ = await self.cog.get_admin_permissions(interaction.user.id)
        if not is_global_admin:
            await interaction.response.send_message(f"{theme.deniedIcon} Only global administrators can clear reservations.", ephemeral=True)
            return
        
        await self.cog.show_activity_selection_for_clear(interaction)
    
    @discord.ui.button(label="Clear Channels", style=discord.ButtonStyle.danger, emoji=f"{theme.announceIcon}", row=2)
    async def clear_channels(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is global admin
        is_admin, is_global_admin, _ = await self.cog.get_admin_permissions(interaction.user.id)
        if not is_global_admin:
            await interaction.response.send_message(f"{theme.deniedIcon} Only global administrators can clear channel configurations.", ephemeral=True)
            return
        
        await self.cog.show_clear_channels_selection(interaction)
    
    @discord.ui.button(label="Delete Server ID", style=discord.ButtonStyle.danger, emoji=f"{theme.fidIcon}", row=3)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is global admin
        is_admin, is_global_admin, _ = await self.cog.get_admin_permissions(interaction.user.id)
        if not is_global_admin:
            await interaction.response.send_message(f"{theme.deniedIcon} Only global administrators can delete server configuration.", ephemeral=True)
            return
        
        try:
            svs_conn = sqlite3.connect("db/svs.sqlite")
            svs_cursor = svs_conn.cursor()
            svs_cursor.execute("DELETE FROM reference WHERE context=?", ("minister guild id",))
            svs_conn.commit()
            svs_conn.close()
            await interaction.response.send_message(f"{theme.verifiedIcon} Server ID deleted from the database.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"{theme.deniedIcon} Failed to delete server ID: {e}", ephemeral=True)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}", row=3)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_channel_menu(interaction)

class MinisterChannelView(discord.ui.View):
    def __init__(self, bot, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog
        self.is_global = is_global

        # Disable global-admin-only buttons for non-global admins
        # Note: Channel Setup is server-specific, so it's allowed for server admins
        if not is_global:
            for child in self.children:
                if isinstance(child, discord.ui.Button) and child.label in [
                    "Event Archive"
                ]:
                    child.disabled = True

    @discord.ui.button(label="Construction Day", style=discord.ButtonStyle.primary, emoji=f"{theme.constructionIcon}")
    async def construction_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Construction Day")

    @discord.ui.button(label="Research Day", style=discord.ButtonStyle.primary, emoji=f"{theme.researchIcon}")
    async def research_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Research Day")

    @discord.ui.button(label="Troops Training Day", style=discord.ButtonStyle.primary, emoji=f"{theme.trainingIcon}")
    async def troops_training_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Troops Training Day")

    @discord.ui.button(label="Channel Setup", style=discord.ButtonStyle.success, emoji=f"{theme.editListIcon}", row=1)
    async def channel_setup(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Channel setup is server-specific, so any admin can configure it
        if not await self.cog.is_admin(interaction.user.id):
            await interaction.response.send_message(f"{theme.deniedIcon} You do not have permission to configure channels.", ephemeral=True)
            return

        await self.cog.show_channel_setup_menu(interaction)

    @discord.ui.button(label="Event Archive", style=discord.ButtonStyle.secondary, emoji=f"{theme.archiveIcon}", row=1)
    async def event_archive(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Check if user is global admin
        is_admin, is_global_admin, _ = await self.cog.get_admin_permissions(interaction.user.id)
        if not is_global_admin:
            await interaction.response.send_message(f"{theme.deniedIcon} Only global administrators can access archives.", ephemeral=True)
            return

        # Get archive cog
        archive_cog = self.bot.get_cog("MinisterArchive")
        if not archive_cog:
            await interaction.response.send_message(f"{theme.deniedIcon} Minister Archive module not found.", ephemeral=True)
            return

        await archive_cog.show_archive_menu(interaction)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.secondary, emoji=f"{theme.settingsIcon}", row=1)
    async def settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_settings_menu(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            other_features_cog = self.cog.bot.get_cog("OtherFeatures")
            if other_features_cog:
                await other_features_cog.show_other_features_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Other Features module not found.",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while returning to Other Features menu: {e}",
                ephemeral=True
            )

    async def _handle_activity_selection(self, interaction: discord.Interaction, activity_name: str):
        minister_schedule_cog = self.cog.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.response.send_message(f"{theme.deniedIcon} Minister Schedule module not found.", ephemeral=True)
            return

        channel_context = f"{activity_name} channel"
        log_context = "minister log channel"

        channel_id = await minister_schedule_cog.get_channel_id(channel_context)
        log_channel_id = await minister_schedule_cog.get_channel_id(log_context)
        log_guild = await minister_schedule_cog.get_log_guild(interaction.guild)

        channel = log_guild.get_channel(channel_id)
        log_channel = log_guild.get_channel(log_channel_id)

        if not log_guild:
            await interaction.response.send_message(
                "Could not find the minister log server. Make sure the bot is in that server.\n\nIf issue persists, run the `/settings` command --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server",
                ephemeral=True
            )
            return

        if not channel or not log_channel:
            await interaction.response.send_message(
                f"Could not find {activity_name} channel or log channel. Make sure to select a channel for each minister type for the bot to send the updated list, and a log channel.\n\nYou can do so by running the `/settings` command --> Other Features --> Minister Scheduling --> Channel Setup",
                ephemeral=True
            )
            return


        if interaction.guild.id != log_guild.id:
            await interaction.response.send_message(
                f"This menu must be used in the configured server: `{log_guild}`.\n\n"
                "If you want to change the server, run `/settings` command --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server",
                ephemeral=True
            )
            return

        # Show the filtered user selection menu for this activity
        await self.cog.show_filtered_user_select(interaction, activity_name)

class ChannelConfigurationView(discord.ui.View):
    def __init__(self, bot, cog):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog

    @discord.ui.button(label="Construction Channel", style=discord.ButtonStyle.secondary, emoji=f"{theme.constructionIcon}")
    async def construction_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Construction Day channel", "Construction Day")

    @discord.ui.button(label="Research Channel", style=discord.ButtonStyle.secondary, emoji=f"{theme.researchIcon}")
    async def research_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Research Day channel", "Research Day")

    @discord.ui.button(label="Training Channel", style=discord.ButtonStyle.secondary, emoji=f"{theme.trainingIcon}")
    async def training_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Troops Training Day channel", "Troops Training Day")

    @discord.ui.button(label="Log Channel", style=discord.ButtonStyle.secondary, emoji=f"{theme.documentIcon}")
    async def log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "minister log channel", "general logging")

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_channel_menu(interaction)

    async def _handle_channel_selection(self, interaction: discord.Interaction, channel_context: str, activity_name: str):
        minister_schedule_cog = self.cog.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.response.send_message(f"{theme.deniedIcon} Minister Schedule module not found.", ephemeral=True)
            return

        import sys
        minister_module = minister_schedule_cog.__class__.__module__
        ChannelSelect = getattr(sys.modules[minister_module], 'ChannelSelect')
        
        # Create a custom view with a back button
        class ChannelSelectWithBackView(discord.ui.View):
            def __init__(self, bot, context, cog):
                super().__init__(timeout=None)
                self.bot = bot
                self.context = context
                self.cog = cog
                self.add_item(ChannelSelect(bot, context))
                
            @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}", row=1)
            async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
                # Restore the menu with embed
                embed = discord.Embed(
                    title=f"{theme.listIcon} Channel Setup",
                    description=(
                        f"Configure channels for minister scheduling:\n\n"
                        f"Channel Types\n"
                        f"{theme.upperDivider}\n\n"
                        f"{theme.constructionIcon} **Construction Channel**\n"
                        f"└ Shows available Construction Day slots\n\n"
                        f"{theme.researchIcon} **Research Channel**\n"
                        f"└ Shows available Research Day slots\n\n"
                        f"{theme.trainingIcon} **Training Channel**\n"
                        f"└ Shows available Training Day slots\n\n"
                        f"{theme.listIcon} **Log Channel**\n"
                        f"└ Receives add/remove notifications\n\n"
                        f"{theme.lowerDivider}\n\n"
                        f"Select a channel type to configure:"
                    ),
                    color=theme.emColor1
                )

                import sys
                minister_menu_module = self.cog.__class__.__module__
                ChannelConfigurationView = getattr(sys.modules[minister_menu_module], 'ChannelConfigurationView')
                
                view = ChannelConfigurationView(self.bot, self.cog)
                
                await interaction.response.edit_message(
                    content=None, # Clear the "Select a channel for..." content
                    embed=embed,
                    view=view
                )

        await interaction.response.edit_message(
            content=f"Select a channel for {activity_name}:",
            view=ChannelSelectWithBackView(self.bot, channel_context, self.cog),
            embed=None
        )

class TimeSelectView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, fid, available_times, current_time=None, page=0):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.fid = fid
        self.available_times = available_times
        self.current_time = current_time
        self.page = page
        self.max_page = (len(available_times) - 1) // 25 if available_times else 0

        self.update_components()

    def update_components(self):
        # Clear existing components
        self.clear_items()

        # Calculate page boundaries
        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.available_times))
        page_times = self.available_times[start_idx:end_idx]

        # Add time select dropdown
        self.add_item(TimeSelect(page_times, self.page, self.max_page))

        # Add pagination buttons if needed
        if self.max_page > 0:
            # Previous page button
            prev_button = discord.ui.Button(
                label="",
                emoji=f"{theme.prevIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="prev_page",
                row=1,
                disabled=self.page == 0
            )
            prev_button.callback = self.prev_page_callback
            self.add_item(prev_button)

            # Next page button
            next_button = discord.ui.Button(
                label="",
                emoji=f"{theme.nextIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next_page",
                row=1,
                disabled=self.page >= self.max_page
            )
            next_button.callback = self.next_page_callback
            self.add_item(next_button)

        # Add clear reservation button if user has existing booking
        if self.current_time:
            clear_button = discord.ui.Button(
                label="Clear Reservation",
                style=discord.ButtonStyle.danger,
                emoji=f"{theme.trashIcon}",
                row=2 if self.max_page > 0 else 1
            )
            clear_button.callback = self.clear_reservation_callback
            self.add_item(clear_button)

        # Add back button
        back_button = discord.ui.Button(
            label="Back",
            style=discord.ButtonStyle.secondary,
            emoji=f"{theme.backIcon}",
            row=2 if self.max_page > 0 else 1
        )
        back_button.callback = self.back_button_callback
        self.add_item(back_button)

    async def prev_page_callback(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self.update_components()
        await interaction.response.edit_message(view=self)

    async def next_page_callback(self, interaction: discord.Interaction):
        self.page = min(self.max_page, self.page + 1)
        self.update_components()
        await interaction.response.edit_message(view=self)

    async def back_button_callback(self, interaction: discord.Interaction):
        await self.cog.show_filtered_user_select(interaction, self.activity_name)
    
    async def clear_reservation_callback(self, interaction: discord.Interaction):
        await self.cog.clear_user_reservation(interaction, self.activity_name, self.fid, self.current_time)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class TimeSelect(discord.ui.Select):
    def __init__(self, available_times, page=0, max_page=0):
        options = []
        for time_slot in available_times:  # Already sliced in TimeSelectView
            options.append(discord.SelectOption(
                label=time_slot,
                value=time_slot
            ))

        placeholder = "Select an available time slot..."
        if max_page > 0:
            placeholder = f"Select time... (Page {page + 1}/{max_page + 1})"

        super().__init__(
            placeholder=placeholder,
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_time = self.values[0]
        
        minister_cog = self.view.cog
        await minister_cog.complete_booking(interaction, self.view.activity_name, self.view.fid, selected_time)

class MinisterMenu(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.users_conn = sqlite3.connect('db/users.sqlite')
        self.users_cursor = self.users_conn.cursor()
        self.alliance_conn = sqlite3.connect('db/alliance.sqlite')
        self.alliance_cursor = self.alliance_conn.cursor()
        self.svs_conn = sqlite3.connect("db/svs.sqlite")
        self.svs_cursor = self.svs_conn.cursor()
        self.original_interaction = None

    async def fetch_user_data(self, fid, proxy=None):
        url = 'https://wos-giftcode-api.centurygame.com/api/player'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        current_time = int(time.time() * 1000)
        form = f"fid={fid}&time={current_time}"
        sign = hashlib.md5((form + SECRET).encode('utf-8')).hexdigest()
        form = f"sign={sign}&{form}"

        try:
            connector = ProxyConnector.from_url(proxy) if proxy else None
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, headers=headers, data=form, ssl=False) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return response.status
        except Exception as e:
            return None

    async def is_admin(self, user_id: int) -> bool:
        settings_conn = sqlite3.connect('db/settings.sqlite')
        settings_cursor = settings_conn.cursor()
        
        if user_id == self.bot.owner_id:
            settings_conn.close()
            return True
        
        settings_cursor.execute("SELECT 1 FROM admin WHERE id=?", (user_id,))
        result = settings_cursor.fetchone() is not None
        settings_conn.close()
        return result

    async def show_minister_channel_menu(self, interaction: discord.Interaction):
        # Store the original interaction for later updates
        self.original_interaction = interaction

        # Get channel status and permissions
        channel_status, embed_color = await self.get_channel_status_display()
        _, is_global, _ = await self.get_admin_permissions(interaction.user.id)

        embed = discord.Embed(
            title="🏛️ Minister Scheduling",
            description=(
                f"Manage your minister appointments here:\n\n"
                f"**Channel Status**\n"
                f"{theme.upperDivider}\n"
                f"{channel_status}\n"
                f"{theme.middleDivider}\n\n"
                f"**Available Operations**\n"
                f"{theme.middleDivider}\n"
                f"{theme.constructionIcon} **Construction Day**\n"
                f"└ Manage Construction Day appointments\n\n"
                f"{theme.researchIcon} **Research Day**\n"
                f"└ Manage Research Day appointments\n\n"
                f"{theme.trainingIcon} **Training Day**\n"
                f"└ Manage Troops Training Day appointments\n\n"
                f"{theme.editListIcon} **Channel Setup**\n"
                f"└ Configure channels for appointments and logging\n\n"
                f"{theme.archiveIcon} **Event Archive**\n"
                f"└ Save and view past SvS minister schedules\n\n"
                f"{theme.settingsIcon} **Settings**\n"
                f"└ Update names, clear reservations and more\n"
                f"{theme.lowerDivider}"
            ),
            color=embed_color
        )

        view = MinisterChannelView(self.bot, self, is_global)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            pass

    async def show_channel_setup_menu(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title=f"{theme.listIcon} Channel Setup",
            description=(
                f"Configure channels for minister scheduling:\n\n"
                f"Channel Types\n"
                f"{theme.upperDivider}\n\n"
                f"{theme.constructionIcon} **Construction Channel**\n"
                f"└ Shows available Construction Day slots\n\n"
                f"{theme.researchIcon} **Research Channel**\n"
                f"└ Shows available Research Day slots\n\n"
                f"{theme.trainingIcon} **Training Channel**\n"
                f"└ Shows available Training Day slots\n\n"
                f"{theme.listIcon} **Log Channel**\n"
                f"└ Receives all change notifications\n\n"
                f"{theme.lowerDivider}\n\n"
                f"Select a channel type to configure:"
            ),
            color=theme.emColor1
        )

        view = ChannelConfigurationView(self.bot, self)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)
    
    async def get_channel_status_display(self) -> tuple[str, discord.Color]:
        """
        Generate channel status display for main menu.
        Returns (status_text, embed_color)
        """
        minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            return f"{theme.warnIcon} **Minister Schedule module not loaded**\n", discord.Color.red()

        # Get the log guild to check channels
        try:
            log_guild = await minister_schedule_cog.get_log_guild(None)
        except:
            log_guild = None

        # Define channels to check
        channels_config = [
            ("Construction Day channel", f"{theme.constructionIcon} Construction"),
            ("Research Day channel", f"{theme.researchIcon} Research"),
            ("Troops Training Day channel", f"{theme.trainingIcon} Training"),
            ("minister log channel", f"{theme.listIcon} Log Channel")
        ]

        status_lines = []
        configured_count = 0
        invalid_count = 0

        for context, label in channels_config:
            channel_id = await minister_schedule_cog.get_channel_id(context)

            if not channel_id:
                status_lines.append(f"{label}: {theme.warnIcon} Not Configured")
            else:
                # Try to get the channel
                channel = None
                if log_guild:
                    channel = log_guild.get_channel(channel_id)

                if channel:
                    status_lines.append(f"{label}: {theme.verifiedIcon} {channel.mention}")
                    configured_count += 1
                else:
                    status_lines.append(f"{label}: {theme.deniedIcon} Invalid Channel")
                    invalid_count += 1

        # Determine embed color based on status
        total_channels = len(channels_config)
        if configured_count == total_channels:
            embed_color = discord.Color.green()
        elif configured_count > 0:
            embed_color = discord.Color.orange()
        else:
            embed_color = discord.Color.red()

        status_text = "\n".join(status_lines)
        return status_text, embed_color

    async def get_admin_permissions(self, user_id: int):
        """Get admin permissions - delegates to centralized PermissionManager"""
        is_admin, is_global = PermissionManager.is_admin(user_id)
        if not is_admin:
            return False, False, []
        if is_global:
            return True, True, []
        # Get alliance-specific permissions for server admin
        with sqlite3.connect('db/settings.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("SELECT alliances_id FROM adminserver WHERE admin=?", (user_id,))
            alliance_ids = [row[0] for row in cursor.fetchall()]
        return True, False, alliance_ids

    async def show_filtered_user_select(self, interaction: discord.Interaction, activity_name: str):
        """Show the filtered user selection view"""
        # Check admin permissions
        is_admin, is_global_admin, alliance_ids = await self.get_admin_permissions(interaction.user.id)

        if not is_admin:
            await interaction.response.send_message(f"{theme.deniedIcon} You do not have permission to manage minister appointments.", ephemeral=True)
            return

        # Get users based on permissions
        guild_id = interaction.guild.id if interaction.guild else None
        users = PermissionManager.get_admin_users(interaction.user.id, guild_id)
        
        if not users:
            await interaction.response.send_message(f"{theme.deniedIcon} No users found in your allowed alliances.", ephemeral=True)
            return
        
        # Get current bookings for this activity
        self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
        
        # Create the view
        view = FilteredUserSelectView(self.bot, self, activity_name, users, booked_times)
        
        # Initial embed
        await view.update_embed(interaction)
    
    async def show_current_schedule_list(self, interaction: discord.Interaction, activity_name: str):
        """Show a paginated list of current bookings"""
        await interaction.response.defer()
        
        # Get bookings
        self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=? ORDER BY time", (activity_name,))
        bookings = self.svs_cursor.fetchall()
        
        if not bookings:
            embed = discord.Embed(
                title=f"{theme.listIcon} {activity_name} Schedule",
                description="No appointments currently booked.",
                color=theme.emColor1
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        # Build booking list with user info
        booking_lines = []
        for time, fid, alliance_id in bookings:
            # Get user info
            self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
            user_result = self.users_cursor.fetchone()
            nickname = user_result[0] if user_result else f"Unknown ({fid})"
            
            # Get alliance info
            self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
            alliance_result = self.alliance_cursor.fetchone()
            alliance_name = alliance_result[0] if alliance_result else "Unknown"
            
            booking_lines.append(f"`{time}` - [{alliance_name}] {nickname} ({fid})")
        
        # Create embed with all bookings
        embed = discord.Embed(
            title=f"{theme.listIcon} {activity_name} Schedule",
            description="\n".join(booking_lines),
            color=theme.emColor1
        )
        embed.set_footer(text=f"Total bookings: {len(bookings)}/48")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    async def show_filtered_user_select_with_message(self, interaction: discord.Interaction, activity_name: str, message: str, is_error: bool = False):
        """Show the filtered user selection view with a status message"""
        # Get users based on permissions
        guild_id = interaction.guild.id if interaction.guild else None
        users = PermissionManager.get_admin_users(interaction.user.id, guild_id)
        
        if not users:
            await interaction.response.send_message(f"{theme.deniedIcon} No users found in your allowed alliances.", ephemeral=True)
            return
        
        # Get current bookings for this activity
        self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
        
        # Create the view
        view = FilteredUserSelectView(self.bot, self, activity_name, users, booked_times)
        
        # Get current stats
        total_booked = len({fid for time, (fid, alliance) in booked_times.items() if fid})
        available_slots = 48 - total_booked
        
        # Create description with message
        status_emoji = f"{theme.deniedIcon}" if is_error else f"{theme.verifiedIcon}"
        description = f"{status_emoji} **{message}**\n\n"
        description += f"Select a user to manage their {activity_name} appointment.\n\n"
        
        if view.filter_text:
            description += f"**Filter:** `{view.filter_text}`\n"
            description += f"**Filtered Users:** {len(view.filtered_users)}/{len(view.users)}\n\n"
        
        description += (
            f"**Current Status**\n"
            f"{theme.upperDivider}\n"
            f"📅 **Booked Slots:** `{total_booked}/48`\n"
            f"{theme.timeIcon} **Available Slots:** `{available_slots}/48`\n"
            f"{theme.lowerDivider}\n\n"
            f"📅 = User already has a booking"
        )
        
        embed = discord.Embed(
            title=f"🧑‍💼 {activity_name} Management",
            description=description,
            color=theme.emColor2 if is_error else discord.Color.green()
        )
        
        try:
            await interaction.edit_original_response(embed=embed, view=view)
        except:
            await interaction.followup.send(embed=embed, view=view)
    
    async def update_minister_names(self, interaction: discord.Interaction, activity_name: str):
        """Update nicknames from API for all booked users"""
        await interaction.response.defer()
        
        # Get all bookings for this activity
        self.svs_cursor.execute("SELECT DISTINCT fid FROM appointments WHERE appointment_type=?", (activity_name,))
        fids = [row[0] for row in self.svs_cursor.fetchall()]
        
        if not fids:
            await interaction.followup.send(f"{theme.deniedIcon} No appointments to update.", ephemeral=True)
            return
        
        updated_count = 0
        failed_count = 0
        
        for fid in fids:
            try:
                # Fetch user data from API
                data = await self.fetch_user_data(fid)
                if data and isinstance(data, dict) and "data" in data:
                    new_nickname = data["data"].get("nickname", "")
                    if new_nickname:
                        # Update in database
                        self.users_cursor.execute("UPDATE users SET nickname=? WHERE fid=?", (new_nickname, fid))
                        self.users_conn.commit()
                        updated_count += 1
                    else:
                        failed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                print(f"Error updating nickname for ID {fid}: {e}")
                failed_count += 1
        
        # Show result
        result_msg = f"Updated {updated_count} nicknames for {activity_name}"
        if failed_count > 0:
            result_msg += f" ({failed_count} failed)"
        
        # Return to settings menu with success message
        _, is_global, _ = await self.get_admin_permissions(interaction.user.id)
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Minister Settings",
            description=(
                f"{theme.verifiedIcon} **{result_msg}**\n\n"
                f"Administrative settings for minister scheduling:\n\n"
                f"Available Actions\n"
                f"{theme.upperDivider}\n\n"
                f"{theme.editListIcon} **Update Names**\n"
                f"└ Update nicknames from API for booked users\n\n"
                f"{theme.listIcon} **Schedule List Type**\n"
                f"└ Change the type of schedule list message when adding/removing people\n\n"
                f"{theme.calendarIcon} **Delete All Reservations**\n"
                f"└ Clear appointments for a specific day\n\n"
                f"{theme.announceIcon} **Clear Channels**\n"
                f"└ Clear channel configurations\n\n"
                f"{theme.fidIcon} **Delete Server ID**\n"
                f"└ Remove configured server from database\n\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor3
        )

        view = MinisterSettingsView(self.bot, self, is_global)
        await interaction.followup.send(embed=embed, view=view)
    
    async def show_clear_confirmation(self, interaction: discord.Interaction, activity_name: str):
        """Show confirmation for clearing appointments"""
        # Check permissions
        is_admin, is_global_admin, alliance_ids = await self.get_admin_permissions(interaction.user.id)
        
        if is_global_admin:
            # Count all appointments
            self.svs_cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_type=?", (activity_name,))
            count = self.svs_cursor.fetchone()[0]
            
            embed = discord.Embed(
                title=f"{theme.warnIcon} Clear All Appointments",
                description=f"Are you sure you want to clear **ALL {count} appointments** for {activity_name}?\n\nThis action cannot be undone.",
                color=theme.emColor2
            )
        else:
            # Count appointments for allowed alliances
            if not alliance_ids:
                await interaction.response.send_message(f"{theme.deniedIcon} You don't have permission to clear appointments.", ephemeral=True)
                return
            
            placeholders = ','.join('?' for _ in alliance_ids)
            query = f"SELECT COUNT(*) FROM appointments WHERE appointment_type=? AND alliance IN ({placeholders})"
            self.svs_cursor.execute(query, [activity_name] + alliance_ids)
            count = self.svs_cursor.fetchone()[0]
            
            embed = discord.Embed(
                title=f"{theme.warnIcon} Clear Alliance Appointments",
                description=f"Are you sure you want to clear **{count} appointments** for your alliance(s) in {activity_name}?\n\nThis action cannot be undone.",
                color=theme.emColor2
            )
        
        view = ClearConfirmationView(self.bot, self, activity_name, is_global_admin, alliance_ids)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_time_selection(self, interaction: discord.Interaction, activity_name: str, fid: str, current_time: str = None):
        # Get current slot mode
        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", ("slot_mode",))
        row = self.svs_cursor.fetchone()
        slot_mode = int(row[0]) if row else 0

        # Get MinisterSchedule cog to access get_time_slots
        minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.response.send_message(f"{theme.deniedIcon} Minister Schedule module not found.", ephemeral=True)
            return

        # Get available time slots
        self.svs_cursor.execute("SELECT time FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_times = {row[0] for row in self.svs_cursor.fetchall()}

        # Generate time slots based on mode
        time_slots = minister_schedule_cog.get_time_slots(slot_mode)
        available_times = [time_slot for time_slot in time_slots if time_slot not in booked_times or time_slot == current_time]

        if not available_times:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No available time slots for {activity_name}.",
                ephemeral=True
            )
            return

        # Get user info
        self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
        user_data = self.users_cursor.fetchone()
        nickname = user_data[0] if user_data else f"ID: {fid}"

        description = f"Choose an available time slot for **{nickname}** in {activity_name}:"
        if current_time:
            description += f"\n\n**Current booking:** `{current_time}`"
            description += "\n\nSelecting a new time will move the booking."

        embed = discord.Embed(
            title=f"{theme.timeIcon} Select Time for {nickname}",
            description=description,
            color=theme.emColor1
        )

        view = TimeSelectView(self.bot, self, activity_name, fid, available_times, current_time)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def complete_booking(self, interaction: discord.Interaction, activity_name: str, fid: str, selected_time: str):
        try:
            # Defer to prevent timeout
            if not interaction.response.is_done():
                await interaction.response.defer()

            # Check if the user is already booked for this activity type
            self.svs_cursor.execute("SELECT time FROM appointments WHERE fid=? AND appointment_type=?", (fid, activity_name))
            existing_booking = self.svs_cursor.fetchone()
            
            # If user already has a booking, we'll remove it and add the new one
            if existing_booking:
                old_time = existing_booking[0]
                self.svs_cursor.execute("DELETE FROM appointments WHERE fid=? AND appointment_type=?", (fid, activity_name))

            # Check if the time slot is already taken by someone else
            self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=? AND time=?", (activity_name, selected_time))
            conflicting_booking = self.svs_cursor.fetchone()
            if conflicting_booking:
                booked_fid = conflicting_booking[0]
                self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (booked_fid,))
                booked_user = self.users_cursor.fetchone()
                booked_nickname = booked_user[0] if booked_user else "Unknown"
                
                # Re-add the old booking if we had removed it
                if existing_booking:
                    self.svs_cursor.execute("SELECT alliance FROM users WHERE fid=?", (fid,))
                    user_alliance = self.svs_cursor.fetchone()
                    if user_alliance:
                        self.svs_cursor.execute(
                            "INSERT INTO appointments (fid, appointment_type, time, alliance) VALUES (?, ?, ?, ?)",
                            (fid, activity_name, old_time, user_alliance[0])
                        )
                        self.svs_conn.commit()
                
                error_msg = f"The time {selected_time} for {activity_name} is already taken by {booked_nickname}"
                # Return to user selection with error in embed
                await self.show_filtered_user_select_with_message(interaction, activity_name, error_msg, is_error=True)
                return

            # Get user and alliance info
            self.users_cursor.execute("SELECT alliance, nickname FROM users WHERE fid=?", (fid,))
            user_data = self.users_cursor.fetchone()

            if not user_data:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} User {fid} is not registered.",
                    ephemeral=True
                )
                return

            alliance_id, nickname = user_data

            # Get alliance name
            self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
            alliance_result = self.alliance_cursor.fetchone()
            alliance_name = alliance_result[0] if alliance_result else "Unknown"

            # Book the slot
            self.svs_cursor.execute(
                "INSERT INTO appointments (fid, appointment_type, time, alliance) VALUES (?, ?, ?, ?)",
                (fid, activity_name, selected_time, alliance_id)
            )
            self.svs_conn.commit()

            # Get avatar
            try:
                data = await self.fetch_user_data(fid)
                if isinstance(data, int) and data == 429:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
                elif data and "data" in data and "avatar_image" in data["data"]:
                    avatar_image = data["data"]["avatar_image"]
                else:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
            except Exception:
                avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"

            # Send log embed and log change
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                if existing_booking:
                    # This was a reschedule
                    embed = discord.Embed(
                        title=f"Player rescheduled in {activity_name}",
                        description=f"{nickname} ({fid}) from **{alliance_name}** moved from {old_time} to {selected_time}",
                        color=theme.emColor1
                    )
                    # Log reschedule
                    await minister_schedule_cog.log_change(
                        action_type="reschedule",
                        user=interaction.user,
                        appointment_type=activity_name,
                        fid=int(fid),
                        nickname=nickname,
                        old_time=old_time,
                        new_time=selected_time,
                        alliance_name=alliance_name
                    )
                else:
                    # This was a new booking
                    embed = discord.Embed(
                        title=f"Player added to {activity_name}",
                        description=f"{nickname} ({fid}) from **{alliance_name}** at {selected_time}",
                        color=theme.emColor3
                    )
                    # Log add
                    await minister_schedule_cog.log_change(
                        action_type="add",
                        user=interaction.user,
                        appointment_type=activity_name,
                        fid=int(fid),
                        nickname=nickname,
                        old_time=None,
                        new_time=selected_time,
                        alliance_name=alliance_name
                    )
                embed.set_thumbnail(url=avatar_image)
                embed.set_author(name=f"Added by {interaction.user.display_name}", icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
                await minister_schedule_cog.send_embed_to_channel(embed)
                await self.update_channel_message(activity_name)

            if existing_booking:
                success_msg = f"Successfully moved {nickname} from {old_time} to {selected_time}"
            else:
                success_msg = f"Successfully added {nickname} to {activity_name} at {selected_time}"
            
            await self.show_filtered_user_select_with_message(interaction, activity_name, success_msg)

        except Exception as e:
            try:
                error_msg = f"{theme.deniedIcon} Error booking appointment: {e}"
                await interaction.followup.send(error_msg, ephemeral=True)
            except:
                print(f"Failed to show error message for booking: {e}")

    async def update_channel_message(self, activity_name: str):
        """Update the channel message with current available slots"""
        try:
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                return

            # Get current booked times
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            # Generate time list
            list_type = await minister_schedule_cog.get_channel_id("list type")
            if list_type == 3:
                time_list, _ = minister_schedule_cog.generate_time_list(booked_times)
                message_content = f"**{activity_name}** slots:\n" + "\n".join(time_list)
            elif list_type == 2:
                time_list = minister_schedule_cog.generate_booked_time_list(booked_times)
                message_content = f"**{activity_name}** booked slots:\n" + "\n".join(time_list)
            else:
                time_list = minister_schedule_cog.generate_available_time_list(booked_times)
                available_slots = len(time_list) > 0
                message_content = f"**{activity_name}** available slots:\n" + "\n".join(
                    time_list) if available_slots else f"All appointment slots are filled for {activity_name}"

            context = f"{activity_name}"
            channel_context = f"{activity_name} channel"

            # Get channel
            channel_id = await minister_schedule_cog.get_channel_id(channel_context)
            if channel_id:
                log_guild = await minister_schedule_cog.get_log_guild(None)
                if log_guild:
                    channel = log_guild.get_channel(channel_id)
                    if channel:
                        await minister_schedule_cog.get_or_create_message(context, message_content, channel)

        except Exception as e:
            print(f"Error updating channel message: {e}")
    
    async def clear_user_reservation(self, interaction: discord.Interaction, activity_name: str, fid: str, current_time: str):
        """Clear a user's reservation and return to the day management page"""
        try:
            # Defer to prevent timeout
            if not interaction.response.is_done():
                await interaction.response.defer()
            
            # Get user info for logging
            self.users_cursor.execute("SELECT nickname, alliance FROM users WHERE fid=?", (fid,))
            user_data = self.users_cursor.fetchone()
            
            if not user_data:
                await interaction.followup.send(f"{theme.deniedIcon} User not found.", ephemeral=True)
                return
            
            nickname, alliance_id = user_data
            
            # Get alliance name
            self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
            alliance_result = self.alliance_cursor.fetchone()
            alliance_name = alliance_result[0] if alliance_result else "Unknown"
            
            # Delete the reservation
            self.svs_cursor.execute("DELETE FROM appointments WHERE fid=? AND appointment_type=? AND time=?", 
                                  (fid, activity_name, current_time))
            self.svs_conn.commit()
            
            # Get avatar for log
            try:
                data = await self.fetch_user_data(fid)
                if isinstance(data, int) and data == 429:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
                elif data and "data" in data and "avatar_image" in data["data"]:
                    avatar_image = data["data"]["avatar_image"]
                else:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
            except Exception:
                avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
            
            # Send log embed and log change
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                embed = discord.Embed(
                    title=f"Player removed from {activity_name}",
                    description=f"{nickname} ({fid}) from **{alliance_name}** at {current_time}",
                    color=theme.emColor2
                )
                embed.set_thumbnail(url=avatar_image)
                embed.set_author(name=f"Removed by {interaction.user.display_name}",
                               icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
                await minister_schedule_cog.send_embed_to_channel(embed)

                # Log the change
                await minister_schedule_cog.log_change(
                    action_type="remove",
                    user=interaction.user,
                    appointment_type=activity_name,
                    fid=int(fid),
                    nickname=nickname,
                    old_time=None,
                    new_time=None,
                    alliance_name=alliance_name
                )

                await self.update_channel_message(activity_name)
            
            # Return to day management with confirmation
            success_msg = f"Successfully cleared {nickname}'s reservation at {current_time}"
            await self.show_filtered_user_select_with_message(interaction, activity_name, success_msg)
            
        except Exception as e:
            try:
                error_msg = f"{theme.deniedIcon} Error clearing reservation: {e}"
                await interaction.followup.send(error_msg, ephemeral=True)
            except:
                print(f"Failed to show error message for clearing reservation: {e}")
    
    async def show_clear_channels_selection(self, interaction: discord.Interaction):
        """Show channel selection menu for clearing configurations"""
        class ClearChannelsConfirmView(discord.ui.View):
            def __init__(self, parent_cog):
                super().__init__(timeout=7200)
                self.parent_cog = parent_cog
                
            @discord.ui.select(
                placeholder="Select channels to clear...",
                options=[
                    discord.SelectOption(label="Construction Channel", value="Construction Day", emoji=theme.constructionIcon),
                    discord.SelectOption(label="Research Channel", value="Research Day", emoji=theme.researchIcon),
                    discord.SelectOption(label="Training Channel", value="Troops Training Day", emoji=theme.trainingIcon),
                    discord.SelectOption(label="Log Channel", value="minister log", emoji=theme.documentIcon),
                    discord.SelectOption(label="All Channels", value="ALL", emoji=theme.trashIcon, description="Clear all channel configurations")
                ],
                min_values=1,
                max_values=5
            )
            async def select_channels(self, interaction: discord.Interaction, select: discord.ui.Select):
                try:
                    await interaction.response.defer()
                    
                    cleared_channels = []
                    svs_conn = sqlite3.connect("db/svs.sqlite")
                    svs_cursor = svs_conn.cursor()
                    
                    for value in select.values:
                        if value == "ALL":
                            # Clear all minister channels
                            for activity in ["Construction Day", "Research Day", "Troops Training Day"]:
                                await self._clear_channel_config(svs_cursor, activity, interaction.guild)
                                cleared_channels.append(f"{activity} channel")
                            
                            # Clear log channel
                            svs_cursor.execute("DELETE FROM reference WHERE context=?", ("minister log channel",))
                            cleared_channels.append("Log channel")
                        else:
                            if value == "minister log":
                                svs_cursor.execute("DELETE FROM reference WHERE context=?", ("minister log channel",))
                                cleared_channels.append("Log channel")
                            else:
                                await self._clear_channel_config(svs_cursor, value, interaction.guild)
                                cleared_channels.append(f"{value} channel")
                    
                    svs_conn.commit()
                    svs_conn.close()
                    
                    # Show success message
                    success_message = "Successfully cleared the following configurations:\n" + "\n".join([f"• {ch}" for ch in cleared_channels])
                    
                    # Return to settings menu with success message
                    embed = discord.Embed(
                        title="⚙️ Minister Settings",
                        description=(
                            f"{theme.verifiedIcon} **{success_message}**\n\n"
                            f"Administrative settings for minister scheduling:\n\n"
                            f"Available Actions\n"
                            f"{theme.upperDivider}\n\n"
                            f"{theme.editListIcon} **Update Names**\n"
                            f"└ Update nicknames from API for booked users\n\n"
                            f"{theme.listIcon} **Schedule List Type**\n"
                            f"└ Change the type of schedule list message when adding/removing people\n\n"
                            f"📅 **Delete All Reservations**\n"
                            f"└ Clear appointments for a specific day\n\n"
                            f"{theme.announceIcon} **Clear Channels**\n"
                            f"└ Clear channel configurations\n\n"
                            f"{theme.fidIcon} **Delete Server ID**\n"
                            f"└ Remove configured server from database\n\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor3
                    )
                    
                    view = MinisterSettingsView(self.parent_cog.bot, self.parent_cog, is_global=True)
                    await interaction.followup.edit_message(
                        message_id=interaction.message.id,
                        embed=embed,
                        view=view
                    )

                except Exception as e:
                    await interaction.followup.send(f"{theme.deniedIcon} Error clearing channels: {e}", ephemeral=True)
            
            async def _clear_channel_config(self, svs_cursor, activity_name, guild):
                """Clear channel configuration and delete associated message - preserves appointment records"""
                # Get the channel and message IDs
                channel_context = f"{activity_name} channel"
                svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (channel_context,))
                channel_row = svs_cursor.fetchone()
                
                if channel_row and guild:
                    channel_id = int(channel_row[0])
                    channel = guild.get_channel(channel_id)
                    
                    # Get the message ID
                    svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (activity_name,))
                    message_row = svs_cursor.fetchone()
                    
                    if message_row and channel:
                        message_id = int(message_row[0])
                        try:
                            message = await channel.fetch_message(message_id)
                            await message.delete()
                        except:
                            pass  # Message might already be deleted
                    
                    # Delete the message reference
                    svs_cursor.execute("DELETE FROM reference WHERE context=?", (activity_name,))
                
                # Delete the channel reference
                svs_cursor.execute("DELETE FROM reference WHERE context=?", (channel_context,))
                # NOTE: We do NOT delete appointment records - only channel configuration
            
            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji=f"{theme.deniedIcon}")
            async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
                await self.parent_cog.show_settings_menu(interaction)
        
        embed = discord.Embed(
            title="🗑️ Clear Channel Configurations",
            description="Select which channel configurations you want to clear.\n\n**Warning:** This will remove the channel configuration and delete any existing appointment messages in those channels.\n\n**Note:** Appointment records will be preserved.",
            color=theme.emColor2
        )
        
        await interaction.response.edit_message(embed=embed, view=ClearChannelsConfirmView(self))
    
    async def show_settings_menu(self, interaction: discord.Interaction):
        """Show the minister settings menu"""
        _, is_global, _ = await self.get_admin_permissions(interaction.user.id)
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Minister Settings",
            description=(
                f"Administrative settings for minister scheduling:\n\n"
                f"Available Actions\n"
                f"{theme.upperDivider}\n\n"
                f"{theme.editListIcon} **Update Names**\n"
                f"└ Update nicknames from API for booked users\n\n"
                f"{theme.listIcon} **Schedule List Type**\n"
                f"└ Change the type of schedule list message when adding/removing people\n\n"
                f"{theme.timeIcon} **Time Slot Mode**\n"
                f"└ Toggle between standard (00:00/00:30) and offset (00:00/00:15/00:45) time slots\n\n"
                f"{theme.calendarIcon} **Delete All Reservations**\n"
                f"└ Clear appointments for a specific day\n\n"
                f"{theme.announceIcon} **Clear Channels**\n"
                f"└ Clear channel configurations\n\n"
                f"{theme.fidIcon} **Delete Server ID**\n"
                f"└ Remove configured server from database\n\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        view = MinisterSettingsView(self.bot, self, is_global)

        try:
            await interaction.response.edit_message(content=None, embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(content=None, embed=embed, view=view)
    
    async def show_activity_selection_for_update(self, interaction: discord.Interaction):
        """Show activity selection for updating names"""
        embed = discord.Embed(
            title=f"{theme.editListIcon} Update Names",
            description="Select which activity day you want to update names for:",
            color=theme.emColor1
        )
        
        view = ActivitySelectView(self.bot, self, "update_names")
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)
    
    async def show_activity_selection_for_clear(self, interaction: discord.Interaction):
        """Show activity selection for clearing reservations"""

        minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.followup.send("Couldn't load minister_schedule.py cog")
            return

        log_guild = await minister_schedule_cog.get_log_guild(interaction.guild)
        log_channel_id = await minister_schedule_cog.get_channel_id("minister log channel")
        log_channel = log_guild.get_channel(log_channel_id)

        if not log_channel:
            await interaction.response.send_message(
                f"[Warning] Could not find a log channel. Log channel is needed before clearing the appointment \n\nRun the `/settings` command --> Other Features --> Minister Scheduling --> Channel Setup and choose a log channel", ephemeral=True)
            return

        embed = discord.Embed(
            title="📅 Delete All Reservations",
            description="Select which activity day you want to clear reservations for:",
            color=theme.emColor2
        )
        
        view = ActivitySelectView(self.bot, self, "clear_reservations")
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_time_slot_mode_menu(self, interaction: discord.Interaction):
        """Show time slot mode selection menu"""
        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", ("slot_mode",))
        row = self.svs_cursor.fetchone()
        current_mode = int(row[0]) if row else 0

        mode_labels = {
            0: "Standard (00:00, 00:30, 01:00...)",
            1: "Offset (00:00, 00:15, 00:45, 01:15...)"
        }
        current_label = mode_labels[current_mode]

        embed = discord.Embed(
            title=f"{theme.timeIcon} Time Slot Mode",
            description=(
                f"**Current Mode:** {current_label}\n\n"
                "**Mode 0 (Standard):**\n"
                "└ 48 slots: 00:00, 00:30, 01:00, 01:30... 23:30\n"
                "└ Each slot is 30 minutes\n\n"
                "**Mode 1 (Offset):**\n"
                "└ 48 slots: 00:00 (15min), 00:15, 00:45, 01:15... 23:45 (15min to midnight)\n"
                "└ First slot: 00:00-00:15 (15 min)\n"
                "└ Middle slots: 30 min each\n"
                "└ Last slot: 23:45-00:00 (15 min, ends at daily reset)\n\n"
                f"{theme.warnIcon} **Warning:** Changing modes will automatically migrate all existing reservations to the new time slots."
            ),
            color=theme.emColor1
        )

        view = discord.ui.View(timeout=60)

        select = discord.ui.Select(
            placeholder="Choose a time slot mode:",
            options=[
                discord.SelectOption(label="Standard", description="00:00, 00:30, 01:00... (30min slots)", value="0"),
                discord.SelectOption(label="Offset", description="00:00, 00:15, 00:45... (offset 15min)", value="1")
            ]
        )

        async def select_callback(interaction: discord.Interaction):
            new_mode = int(select.values[0])

            if new_mode == current_mode:
                await interaction.response.send_message(f"{theme.infoIcon} Already using this mode.", ephemeral=True)
                return

            # Migrate reservations
            await self.migrate_time_slots(interaction, current_mode, new_mode)

        select.callback = select_callback
        view.add_item(select)

        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}")

        async def back_callback(interaction: discord.Interaction):
            await self.show_settings_menu(interaction)

        back_button.callback = back_callback
        view.add_item(back_button)

        try:
            await interaction.response.edit_message(content=None, embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(content=None, embed=embed, view=view)

    async def migrate_time_slots(self, interaction: discord.Interaction, old_mode: int, new_mode: int):
        """Migrate all reservations from old mode to new mode"""
        try:
            await interaction.response.defer()

            # Get all appointments
            self.svs_cursor.execute("SELECT fid, appointment_type, time, alliance FROM appointments")
            appointments = self.svs_cursor.fetchall()

            if not appointments:
                # No appointments to migrate, just update mode
                self.svs_cursor.execute("UPDATE reference SET context_id=? WHERE context=?", (new_mode, "slot_mode"))
                self.svs_conn.commit()

                embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Time Slot Mode Updated",
                    description=f"Successfully switched to **Mode {new_mode}** (no reservations to migrate).",
                    color=theme.emColor3
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                await self.show_settings_menu(interaction)
                return

            # Build migration mapping
            migrations = []
            for fid, appointment_type, old_time, alliance in appointments:
                new_time = self.convert_time_slot(old_time, old_mode, new_mode)
                migrations.append((fid, appointment_type, old_time, new_time, alliance))

            # Update database atomically
            for fid, appointment_type, old_time, new_time, alliance in migrations:
                self.svs_cursor.execute(
                    "UPDATE appointments SET time=? WHERE fid=? AND appointment_type=?",
                    (new_time, fid, appointment_type)
                )

            # Update slot mode
            self.svs_cursor.execute("UPDATE reference SET context_id=? WHERE context=?", (new_mode, "slot_mode"))
            self.svs_conn.commit()

            # Log to minister log channel and change history
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                migration_text = "\n".join([f"`{old}` → `{new}` - {atype}" for _, atype, old, new, _ in migrations[:20]])
                if len(migrations) > 20:
                    migration_text += f"\n... and {len(migrations) - 20} more"

                embed = discord.Embed(
                    title=f"Time Slot Mode Changed: Mode {old_mode} → Mode {new_mode}",
                    description=f"**Migrated {len(migrations)} reservations:**\n\n{migration_text}",
                    color=discord.Color.orange()
                )
                embed.set_author(name=f"Changed by {interaction.user.display_name}",
                               icon_url=interaction.user.avatar.url if interaction.user.avatar else None)
                await minister_schedule_cog.send_embed_to_channel(embed)

                # Log the time slot mode change
                import json
                additional_data = json.dumps({
                    "old_mode": old_mode,
                    "new_mode": new_mode,
                    "migrations_count": len(migrations)
                })
                await minister_schedule_cog.log_change(
                    action_type="time_slot_mode_change",
                    user=interaction.user,
                    appointment_type=None,
                    fid=None,
                    nickname=None,
                    old_time=None,
                    new_time=None,
                    alliance_name=None,
                    additional_data=additional_data
                )

                # Update all channel messages
                for activity_name in ["Construction Day", "Research Day", "Troops Training Day"]:
                    await self.update_channel_message(activity_name)

            # Show success
            mode_labels = {0: "Standard", 1: "Offset"}
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Time Slot Mode Updated",
                description=f"Successfully switched to **{mode_labels[new_mode]}** mode.\n\n{len(migrations)} reservations were migrated.",
                color=theme.emColor3
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            await self.show_settings_menu(interaction)

        except Exception as e:
            await interaction.followup.send(f"{theme.deniedIcon} Error migrating time slots: {e}", ephemeral=True)

    def convert_time_slot(self, time_str: str, old_mode: int, new_mode: int) -> str:
        """Convert a time slot from old mode to new mode"""
        hour, minute = map(int, time_str.split(":"))
        total_minutes = hour * 60 + minute

        if old_mode == 0 and new_mode == 1:
            # Standard → Offset
            if total_minutes == 0:
                return "00:00"
            new_minutes = total_minutes - 15
            new_hour = new_minutes // 60
            new_min = new_minutes % 60
            return f"{new_hour:02}:{new_min:02}"
        elif old_mode == 1 and new_mode == 0:
            # Offset → Standard
            # Special case: 23:45 → 23:30 (no 00:00 available as it's first slot)
            if total_minutes == 0:
                return "00:00"
            if time_str == "23:45":
                return "23:30"
            new_minutes = total_minutes + 15
            new_hour = new_minutes // 60
            new_min = new_minutes % 60
            return f"{new_hour:02}:{new_min:02}"

        return time_str

    async def show_activity_selection_for_list_type(self, interaction: discord.Interaction):
        """Show activity selection for changing the list type"""

        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", ("list type",))
        row = self.svs_cursor.fetchone()
        current_value = row[0]

        labels = {1: "Available", 2: "Booked", 3: "All"}
        current_label = labels[current_value]

        embed = discord.Embed(
            title="📋 Schedule List Type",
            description=f"Select the type of generated minister list message when adding/removing people:\n\n**Currently showing:** {current_label}",
            color=theme.emColor3
        )

        view = discord.ui.View(timeout=60)

        select = discord.ui.Select(
            placeholder=f"Choose a schedule list type:",
            options=[
                discord.SelectOption(label="Available", description="Show only available slots", value="1"),
                discord.SelectOption(label="Booked", description="Show only booked slots", value="2"),
                discord.SelectOption(label="All", description="Show all slots", value="3")
            ]
        )

        async def select_callback(interaction: discord.Interaction):
            value = int(select.values[0])

            self.svs_cursor.execute(
                "UPDATE reference SET context_id=? WHERE context=?", (value, "list type")
            )
            self.svs_conn.commit()

            updated_embed = discord.Embed(
                title="📋 Schedule List Type",
                description=f"{theme.verifiedIcon} Schedule list type updated successfully!\n\n**Now showing:** {labels[value]}\n\nNew changes will take effect when you add/remove a person to/from the minister schedule.",
                color=theme.emColor3
            )

            await interaction.response.edit_message(
                content=None,
                embed=updated_embed,
                view=view
            )

        select.callback = select_callback
        view.add_item(select)

        back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji=f"{theme.backIcon}")

        async def back_callback(interaction: discord.Interaction):
            await self.show_settings_menu(interaction)

        back_button.callback = back_callback
        view.add_item(back_button)

        try:
            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )
        except discord.InteractionResponded:
            await interaction.edit_original_response(
                content=None,
                embed=embed,
                view=view
            )

async def setup(bot):
    await bot.add_cog(MinisterMenu(bot))