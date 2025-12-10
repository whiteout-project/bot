import discord
from discord.ext import commands
import sqlite3
from datetime import datetime, timedelta
import pytz
import os
from typing import Dict
import uuid
import sys
sys.path.insert(0, os.path.dirname(__file__))
from bear_event_types import (
    get_event_icon, get_event_config, calculate_next_occurrence, validate_time_slot,
    calculate_crazy_joe_dates
)

class BearTrapWizard(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = 'db/beartime.sqlite'
        os.makedirs('db', exist_ok=True)

        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Enable WAL mode
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.commit()

        # Create wizard tracking table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS wizard_notifications (
                notification_id INTEGER PRIMARY KEY,
                guild_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                created_by_wizard INTEGER DEFAULT 1,
                wizard_run_id TEXT,
                FOREIGN KEY (notification_id) REFERENCES bear_notifications(id) ON DELETE CASCADE
            )
        """)
        self.conn.commit()

    async def check_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user is an admin"""
        conn = sqlite3.connect('db/settings.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM admin WHERE id = ?", (interaction.user.id,))
        is_admin = cursor.fetchone() is not None
        conn.close()

        if not is_admin:
            await interaction.response.send_message(
                "❌ You don't have permission to use this command!",
                ephemeral=True
            )
        return is_admin

    async def show_wizard(self, interaction: discord.Interaction):
        """Launch the notification setup wizard"""
        if not await self.check_admin(interaction):
            return

        wizard_session = WizardSession(self, interaction.guild_id, interaction.user.id)
        view = WizardWelcomeView(self, wizard_session)

        embed = discord.Embed(
            title="🧙 The Wizard",
            description=(
                "*Welcome, oh seeker of convenient event notifications.*\n\n"
                "**You shall not pass without reading the following instructions carefully!**\n\n"
                "I'll help you set up notifications for all common alliance events and more in a channel of your choice, "
                "so that your members never forget another event. It works just like magic! ✨\n\n"
                "**Important:**\n"
                "- Make sure you've created a channel where you want the notifications to appear.\n"
                "- If you want to use a separate role for alerts, set that up in advance too.\n"
                "- Event Templates will be applied to simplify the setup process.\n"
                "- Resulting notifications can be adjusted manually as needed.\n"
                "- Re-run the wizard on the same channel to modify the existing set of notifications there.\n\n"
                "**The events you can configure include:**\n"
                "• Bear Trap (Trap 1 & 2)\n"
                "• Crazy Joe\n"
                "• Mercenary Bosses\n"
                "• Foundry Battle\n"
                "• Canyon Clash\n"
                "• Fortress Battle\n"
                "• Castle Battle & SvS\n"
                "• Frostfire Mine\n"
                "• Daily Reset\n\n"
                "**Are you ready to get started?**"
            ),
            color=discord.Color.gold()
        )

        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class WizardSession:
    """Stores wizard session data"""
    def __init__(self, cog, guild_id: int, user_id: int):
        self.cog = cog
        self.guild_id = guild_id
        self.user_id = user_id
        self.wizard_run_id = str(uuid.uuid4())
        self.wizard_batch_id = None
        self.is_update = False
        self.existing_notifications = {}
        # For tracking original state when updating
        self.originally_configured_events = set()  # Events that existed before wizard run
        self.existing_notifications_raw = {}  # {event_type: [notification_dicts]}
        self.original_instance_states = {}  # {(event_type, instance): notification_dict}
        # Common settings
        self.channel_id = None
        self.mention_type = None
        self.notification_type = None  # e.g., 1, 2, 3, 4, 5, 6 (custom)
        self.custom_times = None  # For notification_type 6
        self.timezone = "UTC"
        # Event tracking
        self.selected_events = []  # Events that have been configured
        self.configured_events = set()  # Track which events are fully configured
        # Event-specific data
        self.bear_trap_data = {}
        self.crazy_joe_data = {}
        self.foundry_data = {}
        self.canyon_data = {}
        self.stronghold_data = {}
        self.frostfire_data = {}
        self.sunfire_data = {}
        self.svs_data = {}
        self.mercenary_bosses_data = {}
        self.daily_reset_data = {}

    def is_event_configured(self, event_type: str) -> bool:
        """Check if an event has been configured"""
        return event_type in self.configured_events

    def mark_event_configured(self, event_type: str):
        """Mark an event as configured"""
        if event_type not in self.selected_events:
            self.selected_events.append(event_type)
        self.configured_events.add(event_type)

    def unconfigure_event(self, event_type: str):
        """Remove event configuration"""
        if event_type in self.configured_events:
            self.configured_events.remove(event_type)
        if event_type in self.selected_events:
            self.selected_events.remove(event_type)
        # Clear event data
        event_data = self.get_event_data(event_type)
        if event_data is not None:
            event_data.clear()

    def get_event_data(self, event_type: str) -> Dict:
        """Get the data dict for a specific event"""
        mapping = {
            "Bear Trap": self.bear_trap_data,
            "Crazy Joe": self.crazy_joe_data,
            "Foundry Battle": self.foundry_data,
            "Canyon Clash": self.canyon_data,
            "Fortress Battle": self.stronghold_data,
            "Frostfire Mine": self.frostfire_data,
            "Castle Battle": self.sunfire_data,
            "SvS": self.svs_data,
            "Mercenary Bosses": self.mercenary_bosses_data,
            "Daily Reset": self.daily_reset_data
        }
        return mapping.get(event_type, {})

    def load_existing_notifications(self, channel_id: int):
        """Load existing wizard notifications and reconstruct session state"""
        bear_trap_cog = self.cog.bot.get_cog("BearTrap")
        if not bear_trap_cog:
            return

        self.wizard_batch_id = f"wizard_{self.guild_id}_{channel_id}"

        # Get ALL notifications (not just one per event_type)
        notifications = bear_trap_cog.get_all_wizard_notifications_for_channel(self.guild_id, channel_id)

        if not notifications:
            return

        self.is_update = True
        self.channel_id = channel_id

        # Group notifications by event_type
        self.existing_notifications_raw = {}
        for notif in notifications:
            event_type = notif.get("event_type")
            if not event_type:
                continue
            if event_type not in self.existing_notifications_raw:
                self.existing_notifications_raw[event_type] = []
            self.existing_notifications_raw[event_type].append(notif)

            # Store by (event_type, instance_identifier) for easy lookup
            instance = notif.get("instance_identifier") or "default"
            self.original_instance_states[(event_type, instance)] = notif

        # Load global settings from first notification
        first_notif = notifications[0]
        self.timezone = first_notif.get("timezone", "UTC")
        self.mention_type = first_notif.get("mention_type")

        # Only set notification_type if it's not the default (2)
        loaded_notif_type = first_notif.get("notification_type")
        if loaded_notif_type and loaded_notif_type != 2:
            self.notification_type = loaded_notif_type

        # Check for custom times in description
        desc = first_notif.get("description", "")
        if desc.startswith("CUSTOM_TIMES:"):
            parts = desc.split("|")
            if parts:
                self.custom_times = parts[0].replace("CUSTOM_TIMES:", "")
                self.notification_type = 6  # Custom type

        # Reconstruct event-specific data and mark as configured
        for event_type, notifs in self.existing_notifications_raw.items():
            # Only count as configured if at least one instance is enabled
            has_enabled = any(n.get("is_enabled", 1) for n in notifs)
            if has_enabled:
                self._reconstruct_event_data(event_type, notifs)
                self.mark_event_configured(event_type)
                self.originally_configured_events.add(event_type)

        # Keep legacy dict for backward compatibility
        self.existing_notifications = bear_trap_cog.get_wizard_notifications_for_channel(self.guild_id, channel_id)

    def _reconstruct_event_data(self, event_type: str, notifications: list):
        """Reconstruct event-specific data from existing notifications"""
        if event_type == "Bear Trap":
            for notif in notifications:
                instance = notif.get("instance_identifier", "")
                if instance == "bt1" or (not instance and "bt1_hour" not in self.bear_trap_data):
                    self.bear_trap_data["bt1_hour"] = notif["hour"]
                    self.bear_trap_data["bt1_minute"] = notif["minute"]
                    if notif.get("repeat_minutes") and notif["repeat_minutes"] > 0:
                        self.bear_trap_data["repeat_days"] = notif["repeat_minutes"] // (24 * 60)
                elif instance == "bt2" or (not instance and "bt1_hour" in self.bear_trap_data):
                    self.bear_trap_data["bt2_hour"] = notif["hour"]
                    self.bear_trap_data["bt2_minute"] = notif["minute"]

        elif event_type == "Crazy Joe":
            for notif in notifications:
                instance = notif.get("instance_identifier", "")
                if instance == "tuesday" or (not instance and "tuesday_hour" not in self.crazy_joe_data):
                    self.crazy_joe_data["tuesday_hour"] = notif["hour"]
                    self.crazy_joe_data["tuesday_minute"] = notif["minute"]
                elif instance == "thursday" or (not instance and "tuesday_hour" in self.crazy_joe_data):
                    self.crazy_joe_data["thursday_hour"] = notif["hour"]
                    self.crazy_joe_data["thursday_minute"] = notif["minute"]

        elif event_type == "Foundry Battle":
            for notif in notifications:
                instance = notif.get("instance_identifier", "legion1")
                if instance == "legion1":
                    self.foundry_data["legion1_hour"] = notif["hour"]
                    self.foundry_data["legion1_minute"] = notif["minute"]
                elif instance == "legion2":
                    self.foundry_data["legion2_hour"] = notif["hour"]
                    self.foundry_data["legion2_minute"] = notif["minute"]

        elif event_type == "Canyon Clash":
            for notif in notifications:
                instance = notif.get("instance_identifier", "legion1")
                if instance == "legion1":
                    self.canyon_data["legion1_hour"] = notif["hour"]
                    self.canyon_data["legion1_minute"] = notif["minute"]
                elif instance == "legion2":
                    self.canyon_data["legion2_hour"] = notif["hour"]
                    self.canyon_data["legion2_minute"] = notif["minute"]

        elif event_type == "Fortress Battle":
            if "times" not in self.stronghold_data:
                self.stronghold_data["times"] = []
            for notif in notifications:
                self.stronghold_data["times"].append({
                    "hour": notif["hour"],
                    "minute": notif["minute"],
                    "phase": notif.get("instance_identifier")
                })

        elif event_type == "Frostfire Mine":
            if "times" not in self.frostfire_data:
                self.frostfire_data["times"] = []
            for notif in notifications:
                self.frostfire_data["times"].append({
                    "hour": notif["hour"],
                    "minute": notif["minute"],
                    "phase": notif.get("instance_identifier")
                })

        elif event_type == "Castle Battle":
            if "times" not in self.sunfire_data:
                self.sunfire_data["times"] = []
            for notif in notifications:
                self.sunfire_data["times"].append({
                    "hour": notif["hour"],
                    "minute": notif["minute"],
                    "phase": notif.get("instance_identifier")
                })

        elif event_type == "SvS":
            if "times" not in self.svs_data:
                self.svs_data["times"] = []
            for notif in notifications:
                self.svs_data["times"].append({
                    "hour": notif["hour"],
                    "minute": notif["minute"],
                    "phase": notif.get("instance_identifier")
                })

        elif event_type == "Mercenary Bosses":
            if "bosses" not in self.mercenary_bosses_data:
                self.mercenary_bosses_data["bosses"] = []
            for notif in notifications:
                self.mercenary_bosses_data["bosses"].append({
                    "hour": notif["hour"],
                    "minute": notif["minute"],
                    "instance": notif.get("instance_identifier")
                })

        elif event_type == "Daily Reset":
            self.daily_reset_data["configured"] = True
            self.daily_reset_data["hour"] = notifications[0]["hour"] if notifications else 0
            self.daily_reset_data["minute"] = notifications[0]["minute"] if notifications else 0

class WizardWelcomeView(discord.ui.View):
    def __init__(self, cog: BearTrapWizard, session: WizardSession):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session

    @discord.ui.button(label="Start Wizard", emoji="✨", style=discord.ButtonStyle.success, row=0)
    async def start_wizard(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Start the wizard - begin with Common Settings"""
        view = CommonSettingsHubView(self.cog, self.session)
        await view.show(interaction)

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.danger, row=0)
    async def cancel_wizard(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the wizard"""
        embed = discord.Embed(
            title="Wizard Cancelled",
            description="The notification setup wizard has been cancelled.",
            color=discord.Color.red()
        )
        await interaction.response.edit_message(embed=embed, view=None)

class CommonSettingsHubView(discord.ui.View):
    """Step 1: Configure common settings (channel, mention, notification times, timezone)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session

    async def show(self, interaction: discord.Interaction):
        """Display the common settings hub"""
        # Build status display
        channel_status = "✅ Configured" if self.session.channel_id else "⚠️ Required"
        mention_status = "✅ Configured" if self.session.mention_type else "⚠️ Required"
        notif_status = "✅ Configured" if self.session.notification_type else "⚙️ Default (10m, 5m, Time)"
        timezone_status = f"✅ {self.session.timezone}" if self.session.timezone != "UTC" else "⚙️ UTC (Default)"

        # Get channel name if configured
        channel_name = ""
        if self.session.channel_id:
            channel = interaction.guild.get_channel(self.session.channel_id)
            if channel:
                channel_name = f" - #{channel.name}"

        # Get mention description
        mention_desc = ""
        if self.session.mention_type:
            if self.session.mention_type == "everyone":
                mention_desc = " - @everyone"
            elif self.session.mention_type.startswith("role_"):
                role_id = int(self.session.mention_type.split("_")[1])
                role = interaction.guild.get_role(role_id)
                mention_desc = f" - @{role.name}" if role else " - Role"
            elif self.session.mention_type.startswith("member_"):
                member_id = int(self.session.mention_type.split("_")[1])
                member = interaction.guild.get_member(member_id)
                mention_desc = f" - @{member.name}" if member else " - Member"
            elif self.session.mention_type == "none":
                mention_desc = " - No Mention"

        # Get notification type description
        notif_desc = ""
        if self.session.notification_type:
            notif_map = {
                1: " - 30m, 10m, 5m & Time",
                2: " - 10m, 5m & Time",
                3: " - 5m & Time",
                4: " - Only 5m",
                5: " - Only Time",
                6: f" - Custom: {self.session.custom_times}" if self.session.custom_times else " - Custom"
            }
            notif_desc = notif_map.get(self.session.notification_type, "")

        embed = discord.Embed(
            title="⚙️ Global Settings",
            description=(
                "First let's configure settings that will apply to all of the event notifications that we are going to set up.\n\n"
                "**You need to do at least two things here:**\n"
                "- Specify a channel where you want the bot to post the notifications.\n"
                "- Specify who should be mentioned in the notifications.\n\n"
                "**You might also want to adjust some optional settings:**\n"
                "- When the bot will send notifications before an event. 10m and 5m before and at the event time by default.\n"
                "- The timezone for the event times. UTC by default.\n\n"
                "**Required Settings:**\n"
                f"📍 **Channel:** {channel_status}{channel_name}\n"
                f"💬 **Mention:** {mention_status}{mention_desc}\n\n"
                "**Optional Settings:**\n"
                f"⏰ **Notification Times:** {notif_status}{notif_desc}\n"
                f"🌍 **Timezone:** {timezone_status}\n\n"
                "Click the buttons below to configure each setting.\n"
                "When ready, click **Continue** to select events."
            ),
            color=discord.Color.blue()
        )

        # Check if updating existing batch
        if self.session.is_update:
            embed.set_footer(text=f"ℹ️ Updating existing batch with {len(self.session.existing_notifications)} notifications")

        self.clear_items()

        # Required settings buttons
        channel_button = discord.ui.Button(
            label="Set Channel",
            emoji="📍",
            style=discord.ButtonStyle.success if self.session.channel_id else discord.ButtonStyle.danger,
            row=0
        )
        channel_button.callback = self.configure_channel
        self.add_item(channel_button)

        mention_button = discord.ui.Button(
            label="Set Mention",
            emoji="💬",
            style=discord.ButtonStyle.success if self.session.mention_type else discord.ButtonStyle.danger,
            row=0
        )
        mention_button.callback = self.configure_mention
        self.add_item(mention_button)

        # Optional settings buttons
        notif_button = discord.ui.Button(
            label="Notification Times",
            emoji="⏰",
            style=discord.ButtonStyle.success if self.session.notification_type else discord.ButtonStyle.secondary,
            row=1
        )
        notif_button.callback = self.configure_notification_times
        self.add_item(notif_button)

        timezone_button = discord.ui.Button(
            label="Timezone",
            emoji="🌍",
            style=discord.ButtonStyle.success if self.session.timezone != "UTC" else discord.ButtonStyle.secondary,
            row=1
        )
        timezone_button.callback = self.configure_timezone
        self.add_item(timezone_button)

        # Continue button (disabled if required settings not configured)
        can_continue = self.session.channel_id and self.session.mention_type
        continue_button = discord.ui.Button(
            label="Continue to Event Selection",
            emoji="➡️",
            style=discord.ButtonStyle.primary,
            disabled=not can_continue,
            row=2
        )
        continue_button.callback = self.continue_to_events
        self.add_item(continue_button)

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def configure_channel(self, interaction: discord.Interaction):
        """Show channel selection"""
        view = WizardChannelSelectView(self.cog, self.session, self)
        await view.show(interaction)

    async def configure_mention(self, interaction: discord.Interaction):
        """Show mention type selection"""
        view = WizardMentionSelectView(self.cog, self.session, self)
        await view.show(interaction)

    async def configure_notification_times(self, interaction: discord.Interaction):
        """Show notification times selection"""
        view = WizardNotificationTypeView(self.cog, self.session, self)
        await view.show(interaction)

    async def configure_timezone(self, interaction: discord.Interaction):
        """Show timezone modal"""
        modal = WizardTimezoneModal(self.session, self)
        await interaction.response.send_modal(modal)

    async def continue_to_events(self, interaction: discord.Interaction):
        """Proceed to event selection hub"""
        view = EventSelectionHubView(self.cog, self.session)
        await view.show(interaction)

class WizardChannelSelectView(discord.ui.View):
    """Channel selection for wizard"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, parent_view: CommonSettingsHubView):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.parent_view = parent_view

        # Add channel select dropdown
        channel_select = discord.ui.ChannelSelect(
            placeholder="Select notification channel",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text]
        )
        channel_select.callback = self.channel_selected
        self.add_item(channel_select)

    async def show(self, interaction: discord.Interaction):
        """Display channel selection"""
        embed = discord.Embed(
            title="📍 Select Notification Channel",
            description="Choose the channel where notifications will be posted.",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def channel_selected(self, interaction: discord.Interaction):
        """Handle channel selection"""
        channel_id = int(interaction.data["values"][0])
        self.session.channel_id = channel_id

        # Load existing wizard notifications for this channel
        self.session.load_existing_notifications(channel_id)

        # Return to common settings hub
        await self.parent_view.show(interaction)

class WizardMentionSelectView(discord.ui.View):
    """Mention type selection for wizard"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, parent_view: CommonSettingsHubView):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.parent_view = parent_view

    async def show(self, interaction: discord.Interaction):
        """Display mention selection"""
        embed = discord.Embed(
            title="💬 Select Mention Type",
            description=(
                "Choose how to mention users:\n\n"
                "1️⃣ @everyone\n"
                "2️⃣ Specific Role\n"
                "3️⃣ Specific Member\n"
                "4️⃣ No Mention"
            ),
            color=discord.Color.blue()
        )

        self.clear_items()

        everyone_button = discord.ui.Button(
            label="@everyone",
            emoji="📢",
            style=discord.ButtonStyle.danger,
            row=0
        )
        everyone_button.callback = lambda i: self.mention_selected(i, "everyone")
        self.add_item(everyone_button)

        role_button = discord.ui.Button(
            label="Select Role",
            emoji="👥",
            style=discord.ButtonStyle.success,
            row=0
        )
        role_button.callback = self.select_role
        self.add_item(role_button)

        member_button = discord.ui.Button(
            label="Select Member",
            emoji="👤",
            style=discord.ButtonStyle.primary,
            row=0
        )
        member_button.callback = self.select_member
        self.add_item(member_button)

        no_mention_button = discord.ui.Button(
            label="No Mention",
            emoji="🔕",
            style=discord.ButtonStyle.secondary,
            row=0
        )
        no_mention_button.callback = lambda i: self.mention_selected(i, "none")
        self.add_item(no_mention_button)

        await interaction.response.edit_message(embed=embed, view=self)

    async def mention_selected(self, interaction: discord.Interaction, mention_type: str):
        """Handle mention selection"""
        self.session.mention_type = mention_type
        await self.parent_view.show(interaction)

    async def select_role(self, interaction: discord.Interaction):
        """Show role selector"""
        role_select = discord.ui.RoleSelect(
            placeholder="Select a role to mention",
            min_values=1,
            max_values=1
        )

        async def role_callback(select_interaction):
            role_id = select_interaction.data["values"][0]
            self.session.mention_type = f"role_{role_id}"
            await self.parent_view.show(select_interaction)

        role_select.callback = role_callback
        view = discord.ui.View(timeout=3600)
        view.add_item(role_select)

        embed = discord.Embed(
            title="👥 Select Role",
            description="Choose a role to mention:",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def select_member(self, interaction: discord.Interaction):
        """Show member selector"""
        member_select = discord.ui.UserSelect(
            placeholder="Select a member to mention",
            min_values=1,
            max_values=1
        )

        async def member_callback(select_interaction):
            member_id = select_interaction.data["values"][0]
            self.session.mention_type = f"member_{member_id}"
            await self.parent_view.show(select_interaction)

        member_select.callback = member_callback
        view = discord.ui.View(timeout=3600)
        view.add_item(member_select)

        embed = discord.Embed(
            title="👤 Select Member",
            description="Choose a member to mention:",
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)

class WizardNotificationTypeView(discord.ui.View):
    """Notification times selection for wizard"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, parent_view: CommonSettingsHubView):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.parent_view = parent_view

    async def show(self, interaction: discord.Interaction):
        """Display notification type selection"""
        embed = discord.Embed(
            title="⏰ Select Notification Times",
            description="Choose when to send notifications before each event:",
            color=discord.Color.blue()
        )

        self.clear_items()

        # Type 1: 30m, 10m, 5m & Time
        type1_btn = discord.ui.Button(
            label="30m, 10m, 5m & Time",
            style=discord.ButtonStyle.primary,
            row=0
        )
        type1_btn.callback = lambda i: self.type_selected(i, 1)
        self.add_item(type1_btn)

        # Type 2: 10m, 5m & Time
        type2_btn = discord.ui.Button(
            label="10m, 5m & Time",
            style=discord.ButtonStyle.primary,
            row=0
        )
        type2_btn.callback = lambda i: self.type_selected(i, 2)
        self.add_item(type2_btn)

        # Type 3: 5m & Time
        type3_btn = discord.ui.Button(
            label="5m & Time",
            style=discord.ButtonStyle.primary,
            row=1
        )
        type3_btn.callback = lambda i: self.type_selected(i, 3)
        self.add_item(type3_btn)

        # Type 4: Only 5m
        type4_btn = discord.ui.Button(
            label="Only 5m",
            style=discord.ButtonStyle.primary,
            row=1
        )
        type4_btn.callback = lambda i: self.type_selected(i, 4)
        self.add_item(type4_btn)

        # Type 5: Only Time
        type5_btn = discord.ui.Button(
            label="Only Time",
            style=discord.ButtonStyle.primary,
            row=1
        )
        type5_btn.callback = lambda i: self.type_selected(i, 5)
        self.add_item(type5_btn)

        # Type 6: Custom
        type6_btn = discord.ui.Button(
            label="Custom Times",
            style=discord.ButtonStyle.success,
            row=2
        )
        type6_btn.callback = self.show_custom_modal
        self.add_item(type6_btn)

        await interaction.response.edit_message(embed=embed, view=self)

    async def type_selected(self, interaction: discord.Interaction, notification_type: int):
        """Handle notification type selection"""
        self.session.notification_type = notification_type
        self.session.custom_times = None
        await self.parent_view.show(interaction)

    async def show_custom_modal(self, interaction: discord.Interaction):
        """Show custom times modal"""
        modal = WizardCustomTimesModal(self.session, self.parent_view)
        await interaction.response.send_modal(modal)

class WizardCustomTimesModal(discord.ui.Modal, title="Set Custom Notification Times"):
    """Modal for custom notification times"""
    def __init__(self, session: WizardSession, parent_view: CommonSettingsHubView):
        super().__init__()
        self.session = session
        self.parent_view = parent_view

        self.custom_times_input = discord.ui.TextInput(
            label="Custom Notification Times",
            placeholder="Enter times in minutes (e.g., 60-20-15-4-2 or 60-20-15-4-2-0)",
            min_length=1,
            max_length=50,
            required=True,
            style=discord.TextStyle.short
        )
        self.add_item(self.custom_times_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Validate and save custom times"""
        try:
            times_str = self.custom_times_input.value.strip()
            times = [int(t) for t in times_str.split('-')]

            # Validation
            if not all(isinstance(t, int) and t >= 0 for t in times):
                raise ValueError("All times must be non-negative integers")
            if not times:
                raise ValueError("At least one time must be specified")
            if not all(times[i] > times[i + 1] for i in range(len(times) - 1)):
                raise ValueError("Times must be in descending order")

            # Save to session
            self.session.notification_type = 6
            self.session.custom_times = times_str

            # Return to hub using followup since modal consumed the interaction
            await interaction.response.defer()
            # We need to edit the original message
            await interaction.edit_original_response(embed=None, view=self.parent_view)
            await self.parent_view.show(interaction)

        except ValueError as e:
            await interaction.response.send_message(
                f"❌ Invalid input: {str(e)}",
                ephemeral=True
            )

class WizardTimezoneModal(discord.ui.Modal, title="Set Timezone"):
    """Modal for timezone selection"""
    def __init__(self, session: WizardSession, parent_view: CommonSettingsHubView):
        super().__init__()
        self.session = session
        self.parent_view = parent_view

        self.timezone_input = discord.ui.TextInput(
            label="Timezone",
            placeholder="e.g., UTC, America/New_York, UTC+2, UTC-5",
            default=session.timezone,
            required=True,
            max_length=50
        )
        self.add_item(self.timezone_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Validate and save timezone"""
        try:
            tz_input = self.timezone_input.value.strip()

            # Convert UTC+X or UTC-X to appropriate timezone format
            if tz_input.upper() == "UTC":
                tz_name = "UTC"
            elif tz_input.upper().startswith("UTC+") or tz_input.upper().startswith("UTC-"):
                # Extract offset
                offset_str = tz_input[3:]  # Remove "UTC"

                # Parse offset - support both formats
                if ':' in offset_str:
                    # HH:MM format
                    parts = offset_str.split(':')
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    offset = hours + (minutes / 60.0 if hours >= 0 else -minutes / 60.0)
                else:
                    # Decimal format
                    offset = float(offset_str)

                # Convert to Etc/GMT timezone (note: Etc/GMT has inverted signs)
                if offset >= 0:
                    tz_name = f"Etc/GMT-{int(offset)}"
                else:
                    tz_name = f"Etc/GMT+{int(abs(offset))}"
            else:
                # Try as pytz timezone
                tz_name = tz_input

            # Validate timezone
            pytz.timezone(tz_name)
            self.session.timezone = tz_name

            # Return to hub
            await interaction.response.defer()
            await self.parent_view.show(interaction)

        except Exception as e:
            await interaction.response.send_message(
                f"❌ Invalid timezone! Please use a valid timezone name (e.g., UTC, America/New_York, UTC+2).",
                ephemeral=True
            )

class EventSelectionHubView(discord.ui.View):
    """Step 2: Select and configure events - returns here after each event config"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session

        # Event types
        self.event_types = [
            "Bear Trap",
            "Crazy Joe",
            "Mercenary Bosses",
            "Foundry Battle",
            "Canyon Clash",
            "Fortress Battle",
            "Frostfire Mine",
            "Castle Battle",
            "SvS",
            "Daily Reset"
        ]

    async def show(self, interaction: discord.Interaction):
        """Display event selection hub"""
        # Build event list with status
        event_list = []
        for event in self.event_types:
            icon = get_event_icon(event)
            if self.session.is_event_configured(event):
                event_list.append(f"{icon} **{event}** ✅")
            else:
                event_list.append(f"{icon} {event}")

        configured_count = len(self.session.configured_events)

        embed = discord.Embed(
            title="📋 Step 2: Configure Events",
            description=(
                f"**Events Configured: {configured_count}/{len(self.event_types)}**\n\n"
                "Click an event to configure it. Configured events show ✅\n"
                "Click a configured event again to unconfigure it.\n\n"
                "**Available Events:**\n" + "\n".join(event_list) + "\n\n"
                "When finished configuring events, click **Continue to Preview**."
            ),
            color=discord.Color.blue()
        )

        self.clear_items()

        # Add event buttons (5 per row)
        for idx, event in enumerate(self.event_types):
            icon = get_event_icon(event)
            is_configured = self.session.is_event_configured(event)

            button = discord.ui.Button(
                label=event,
                emoji=icon,
                style=discord.ButtonStyle.success if is_configured else discord.ButtonStyle.secondary,
                row=idx // 5
            )
            button.callback = lambda i, e=event: self.event_clicked(i, e)
            self.add_item(button)

        # Back button
        back_button = discord.ui.Button(
            label="Back to Settings",
            emoji="⬅️",
            style=discord.ButtonStyle.secondary,
            row=2
        )
        back_button.callback = self.back_to_settings
        self.add_item(back_button)

        # Continue button (only enabled if at least one event configured)
        continue_button = discord.ui.Button(
            label="Continue to Preview",
            emoji="➡️",
            style=discord.ButtonStyle.primary,
            disabled=(configured_count == 0),
            row=2
        )
        continue_button.callback = self.continue_to_preview
        self.add_item(continue_button)

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def event_clicked(self, interaction: discord.Interaction, event_type: str):
        """Handle event click - configure or unconfigure"""
        if self.session.is_event_configured(event_type):
            # Unconfigure event
            self.session.unconfigure_event(event_type)
            await self.show(interaction)
        else:
            # Route to appropriate config view
            router = EventConfigRouter(self.cog, self.session, event_type, self)
            await router.route(interaction)

    async def back_to_settings(self, interaction: discord.Interaction):
        """Return to common settings"""
        view = CommonSettingsHubView(self.cog, self.session)
        await view.show(interaction)

    async def continue_to_preview(self, interaction: discord.Interaction):
        """Proceed to preview"""
        try:
            view = WizardPreviewView(self.cog, self.session)
            await view.show(interaction)
        except Exception as e:
            import traceback
            traceback.print_exc()
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"An error occurred while loading the preview: {type(e).__name__}: {e}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"An error occurred while loading the preview: {type(e).__name__}: {e}",
                    ephemeral=True
                )


class EventConfigRouter:
    """Routes to appropriate event configuration view"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, event_type: str, hub_view: EventSelectionHubView):
        self.cog = cog
        self.session = session
        self.event_type = event_type
        self.hub_view = hub_view

    async def route(self, interaction: discord.Interaction):
        """Route to correct config view"""
        view_mapping = {
            "Bear Trap": BearTrapConfigView(self.cog, self.session, self.hub_view),
            "Crazy Joe": CrazyJoeConfigView(self.cog, self.session, self.hub_view),
            "Foundry Battle": FoundryConfigView(self.cog, self.session, self.hub_view),
            "Canyon Clash": CanyonConfigView(self.cog, self.session, self.hub_view),
            "Fortress Battle": StrongholdConfigView(self.cog, self.session, self.hub_view),
            "Frostfire Mine": FrostfireConfigView(self.cog, self.session, self.hub_view),
            "Castle Battle": SunfireConfigView(self.cog, self.session, self.hub_view),
            "SvS": SvSConfigView(self.cog, self.session, self.hub_view),
            "Mercenary Bosses": MercenaryBossesConfigView(self.cog, self.session, self.hub_view),
            "Daily Reset": DailyResetConfigView(self.cog, self.session, self.hub_view)
        }

        view = view_mapping.get(self.event_type)
        if view:
            await view.show(interaction)


class BearTrapConfigView:
    """Configuration for Bear Trap events"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        self.cog = cog
        self.session = session
        self.hub_view = hub_view

    async def show(self, interaction: discord.Interaction):
        """Show Bear Trap configuration - directly show modal"""
        modal = BearTrapModal(self.cog, self.session, self.hub_view)
        await interaction.response.send_modal(modal)

class BearTrapModal(discord.ui.Modal):
    """Modal for Bear Trap configuration"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(title="Bear Trap Configuration")
        self.cog = cog
        self.session = session
        self.hub_view = hub_view

        # Pre-populate from existing data if available
        existing = session.bear_trap_data
        bt1_time_default = ""
        bt2_time_default = ""
        repeat_default = ""

        if existing.get("bt1_hour") is not None:
            bt1_time_default = f"{existing['bt1_hour']:02d}:{existing['bt1_minute']:02d}"
        if existing.get("bt2_hour") is not None:
            bt2_time_default = f"{existing['bt2_hour']:02d}:{existing['bt2_minute']:02d}"
        if existing.get("repeat_days"):
            repeat_default = "yes" if existing["repeat_days"] == 2 else "no"

        self.bt1_date = discord.ui.TextInput(
            label="Next Bear Trap 1 Date (DD/MM)",
            placeholder="e.g., 25/11",
            max_length=5
        )
        self.add_item(self.bt1_date)

        self.bt1_time = discord.ui.TextInput(
            label="Bear Trap 1 Time (HH:MM)",
            placeholder="e.g., 14:00",
            default=bt1_time_default,
            max_length=5
        )
        self.add_item(self.bt1_time)

        self.bt2_date = discord.ui.TextInput(
            label="Bear Trap 2 Date (DD/MM, optional)",
            placeholder="Leave blank if same as Trap 1",
            required=False,
            max_length=5
        )
        self.add_item(self.bt2_date)

        self.bt2_time = discord.ui.TextInput(
            label="Bear Trap 2 Time (HH:MM)",
            placeholder="e.g., 18:00",
            default=bt2_time_default,
            max_length=5
        )
        self.add_item(self.bt2_time)

        self.repeat_config = discord.ui.TextInput(
            label="Run every 2 days? (yes/no)",
            placeholder="yes = every 2 days, no = custom weekdays",
            default=repeat_default,
            required=True,
            max_length=3
        )
        self.add_item(self.repeat_config)

    async def on_submit(self, interaction: discord.Interaction):
        """Process Bear Trap configuration"""
        try:
            # Validate and parse dates/times
            tz = pytz.timezone(self.session.timezone)
            now = datetime.now(tz)
            current_year = now.year

            # Parse DD/MM format and add year
            bt1_day, bt1_month = map(int, self.bt1_date.value.split("/"))
            bt1_hour, bt1_minute = map(int, self.bt1_time.value.split(":"))

            # Determine year - if date is in the past, use next year
            bt1_datetime = tz.localize(datetime(current_year, bt1_month, bt1_day, bt1_hour, bt1_minute))
            if bt1_datetime < now:
                bt1_datetime = tz.localize(datetime(current_year + 1, bt1_month, bt1_day, bt1_hour, bt1_minute))

            # Parse Bear 2 date (use Bear 1 date if not provided)
            if self.bt2_date.value.strip():
                bt2_day, bt2_month = map(int, self.bt2_date.value.split("/"))
            else:
                # Same day as Bear 1
                bt2_day, bt2_month = bt1_day, bt1_month

            bt2_hour, bt2_minute = map(int, self.bt2_time.value.split(":"))

            # Determine year for Bear 2
            bt2_datetime = tz.localize(datetime(current_year, bt2_month, bt2_day, bt2_hour, bt2_minute))
            if bt2_datetime < now:
                bt2_datetime = tz.localize(datetime(current_year + 1, bt2_month, bt2_day, bt2_hour, bt2_minute))

            # Validate 5-minute slots
            if bt1_minute % 5 != 0 or bt2_minute % 5 != 0:
                await interaction.response.send_message(
                    "❌ Times must be in 5-minute increments (e.g., :00, :05, :10)!",
                    ephemeral=True
                )
                return

            # Check overlap
            if abs((bt1_datetime - bt2_datetime).total_seconds()) < 1800:  # 30 minutes
                await interaction.response.send_message(
                    "❌ Bear 1 and Bear 2 must be at least 30 minutes apart!",
                    ephemeral=True
                )
                return

            # Parse repeat configuration
            repeat_answer = self.repeat_config.value.lower().strip()
            if repeat_answer not in ["yes", "no"]:
                await interaction.response.send_message(
                    "❌ Please answer 'yes' or 'no' for the repeat question!",
                    ephemeral=True
                )
                return

            # Save data
            self.session.bear_trap_data = {
                "bt1_datetime": bt1_datetime,
                "bt1_hour": bt1_hour,
                "bt1_minute": bt1_minute,
                "bt2_datetime": bt2_datetime,
                "bt2_hour": bt2_hour,
                "bt2_minute": bt2_minute,
                "repeat_days": 2 if repeat_answer == "yes" else None
            }

            # If answer is "no", show custom weekday selection
            if repeat_answer == "no":
                view = BearTrapWeekdayView(self.cog, self.session, self.hub_view)
                await view.show(interaction)
            else:
                # Mark as configured and return to hub
                self.session.mark_event_configured("Bear Trap")
                await self.hub_view.show(interaction)

        except Exception as e:
            await interaction.response.send_message(
                f"❌ Invalid input! Please check your dates and times.\nError: {str(e)}",
                ephemeral=True
            )

class BearTrapWeekdayView(discord.ui.View):
    """Select custom weekdays for Bear Trap repeat schedule"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.selected_days = []
    async def show(self, interaction: discord.Interaction):
        """Show weekday selection"""
        icon = get_event_icon("Bear Trap")
        embed = discord.Embed(
            title=f"{icon} Select Repeat Days",
            description=(
                "Select which days of the week Bear Trap should repeat.\n\n"
                "Click on the days to toggle selection, then click Continue."
            ),
            color=discord.Color.blue()
        )
        if self.selected_days:
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            selected_names = [day_names[d] for d in sorted(self.selected_days)]
            embed.add_field(
                name="Selected Days",
                value=", ".join(selected_names),
                inline=False
            )
        self.clear_items()
        # Add day buttons
        days = [
            ("Monday", 0), ("Tuesday", 1), ("Wednesday", 2), ("Thursday", 3),
            ("Friday", 4), ("Saturday", 5), ("Sunday", 6)
        ]
        for day_name, day_num in days:
            button = discord.ui.Button(
                label=day_name,
                style=discord.ButtonStyle.success if day_num in self.selected_days else discord.ButtonStyle.secondary,
                row=day_num // 4
            )
            button.callback = lambda i, d=day_num: self.toggle_day(i, d)
            self.add_item(button)
        # Add continue button
        continue_button = discord.ui.Button(
            label="Continue",
            emoji="➡️",
            style=discord.ButtonStyle.primary,
            disabled=len(self.selected_days) == 0,
            row=2
        )
        continue_button.callback = self.continue_to_next
        self.add_item(continue_button)
        await interaction.response.edit_message(embed=embed, view=self)
    async def toggle_day(self, interaction: discord.Interaction, day: int):
        """Toggle day selection"""
        if day in self.selected_days:
            self.selected_days.remove(day)
        else:
            self.selected_days.append(day)
        await self.show(interaction)
    async def continue_to_next(self, interaction: discord.Interaction):
        """Save selected days and continue"""
        # Update session data with selected weekdays
        self.session.bear_trap_data["repeat_weekdays"] = sorted(self.selected_days)
        # Mark as configured and return to hub
        self.session.mark_event_configured("Bear Trap")
        await self.hub_view.show(interaction)

class CrazyJoeConfigView:
    """Configuration for Crazy Joe"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        self.cog = cog
        self.session = session
        self.hub_view = hub_view

    async def show(self, interaction: discord.Interaction):
        """Show Crazy Joe configuration - directly show modal"""
        modal = CrazyJoeModal(self.cog, self.session, self.hub_view)
        await interaction.response.send_modal(modal)

class CrazyJoeModal(discord.ui.Modal):
    """Modal for Crazy Joe configuration"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(title="Crazy Joe Configuration")
        self.cog = cog
        self.session = session
        self.hub_view = hub_view

        # Pre-populate from existing data if available
        existing = session.crazy_joe_data
        tuesday_default = ""
        thursday_default = ""

        if existing.get("tuesday_hour") is not None:
            tuesday_default = f"{existing['tuesday_hour']:02d}:{existing['tuesday_minute']:02d}"
        if existing.get("thursday_hour") is not None:
            thursday_default = f"{existing['thursday_hour']:02d}:{existing['thursday_minute']:02d}"

        self.tuesday_time = discord.ui.TextInput(
            label="Tuesday Time (HH:MM)",
            placeholder="e.g., 19:00",
            default=tuesday_default,
            max_length=5,
            required=True
        )
        self.add_item(self.tuesday_time)

        self.thursday_time = discord.ui.TextInput(
            label="Thursday Time (HH:MM, optional)",
            placeholder="Leave blank for same as Tuesday",
            default=thursday_default,
            max_length=5,
            required=False
        )
        self.add_item(self.thursday_time)

    async def on_submit(self, interaction: discord.Interaction):
        """Process Crazy Joe configuration"""
        try:
            tue_hour, tue_minute = map(int, self.tuesday_time.value.split(":"))

            # If Thursday time is blank or "same", use Tuesday's time
            if not self.thursday_time.value or self.thursday_time.value.strip() == "":
                thu_hour, thu_minute = tue_hour, tue_minute
            else:
                thu_hour, thu_minute = map(int, self.thursday_time.value.split(":"))

            # Validate 5-minute slots
            if tue_minute % 5 != 0 or thu_minute % 5 != 0:
                await interaction.response.send_message(
                    "❌ Times must be in 5-minute increments!",
                    ephemeral=True
                )
                return

            # Save data
            self.session.crazy_joe_data = {
                "tuesday_hour": tue_hour,
                "tuesday_minute": tue_minute,
                "thursday_hour": thu_hour,
                "thursday_minute": thu_minute
            }

            # Mark as configured and return to hub
            self.session.mark_event_configured("Crazy Joe")
            await self.hub_view.show(interaction)

        except Exception as e:
            await interaction.response.send_message(
                f"❌ Invalid time format! Use HH:MM (e.g., 19:00).\nError: {str(e)}",
                ephemeral=True
            )

class DualLegionConfigView(discord.ui.View):
    """Base class for dual-legion event configuration (Foundry Battle, Canyon Clash)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView, event_name: str, session_data_attr: str):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.event_name = event_name
        self.session_data_attr = session_data_attr

        config = get_event_config(event_name)
        time_slots = config.get("available_times", [])

        # Pre-populate from existing data if available
        existing_data = getattr(self.session, session_data_attr, {})
        if existing_data.get("legion1_hour") is not None:
            self.legion1_time = f"{existing_data['legion1_hour']:02d}:{existing_data['legion1_minute']:02d}"
        if existing_data.get("legion2_hour") is not None:
            self.legion2_time = f"{existing_data['legion2_hour']:02d}:{existing_data['legion2_minute']:02d}"

        # Build options with "None" option first
        legion1_options = [discord.SelectOption(label="None (Disable)", value="none", description="Disable Legion 1 notifications")]
        legion2_options = [discord.SelectOption(label="None (Disable)", value="none", description="Disable Legion 2 notifications")]

        for time in time_slots:
            legion1_options.append(discord.SelectOption(
                label=f"{time} UTC",
                value=time,
                default=(hasattr(self, 'legion1_time') and self.legion1_time == time)
            ))
            legion2_options.append(discord.SelectOption(
                label=f"{time} UTC",
                value=time,
                default=(hasattr(self, 'legion2_time') and self.legion2_time == time)
            ))

        # Add time selection for Legion 1
        legion1_select = discord.ui.Select(
            placeholder="Select Legion 1 time...",
            options=legion1_options,
            row=0
        )
        legion1_select.callback = lambda i: self.set_legion1_time(i, legion1_select.values[0])
        self.add_item(legion1_select)

        # Add time selection for Legion 2
        legion2_select = discord.ui.Select(
            placeholder="Select Legion 2 time...",
            options=legion2_options,
            row=1
        )
        legion2_select.callback = lambda i: self.set_legion2_time(i, legion2_select.values[0])
        self.add_item(legion2_select)

        # Add continue button
        continue_button = discord.ui.Button(
            label="Continue",
            emoji="➡️",
            style=discord.ButtonStyle.success,
            row=2
        )
        continue_button.callback = self.continue_to_next
        self.add_item(continue_button)

    async def show(self, interaction: discord.Interaction):
        """Show event configuration"""
        icon = get_event_icon(self.event_name)
        next_date = calculate_next_occurrence(self.event_name)
        config = get_event_config(self.event_name)

        # Get schedule description from config
        schedule_desc = config.get("fixed_days", "Scheduled event")
        time_slots_str = ", ".join(config.get("available_times", []))

        description = (
            f"{self.event_name} occurs **{schedule_desc}**.\n\n"
            f"**Next Date:** {next_date.strftime('%B %d, %Y') if next_date else 'N/A'}\n\n"
            f"**Available Times (UTC):** {time_slots_str}\n\n"
            "Select times for Legion 1 and Legion 2.\n"
            "Select 'None' to disable a legion's notifications."
        )

        # Show current configuration
        legion1_display = getattr(self, 'legion1_time', None)
        legion2_display = getattr(self, 'legion2_time', None)

        if legion1_display:
            if legion1_display == "none":
                description += f"\n\n**Legion 1:** Disabled"
            else:
                description += f"\n\n**Legion 1:** {legion1_display} UTC"
        if legion2_display:
            if legion2_display == "none":
                description += f"\n**Legion 2:** Disabled"
            else:
                description += f"\n**Legion 2:** {legion2_display} UTC"

        embed = discord.Embed(
            title=f"{icon} Configure {self.event_name}",
            description=description,
            color=discord.Color.blue()
        )

        # Update continue button state
        legion1 = getattr(self, 'legion1_time', None)
        legion2 = getattr(self, 'legion2_time', None)

        # Can continue if at least one legion has an actual time selected (not "none" and not unset)
        has_legion1_time = legion1 is not None and legion1 != "none"
        has_legion2_time = legion2 is not None and legion2 != "none"
        can_continue = has_legion1_time or has_legion2_time

        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label == "Continue":
                item.disabled = not can_continue

        await interaction.response.edit_message(embed=embed, view=self)

    async def set_legion1_time(self, interaction: discord.Interaction, time: str):
        """Set Legion 1 time"""
        self.legion1_time = time
        await self.show(interaction)

    async def set_legion2_time(self, interaction: discord.Interaction, time: str):
        """Set Legion 2 time"""
        self.legion2_time = time
        await self.show(interaction)

    async def continue_to_next(self, interaction: discord.Interaction):
        """Save data and proceed to next event"""
        legion1 = getattr(self, 'legion1_time', None)
        legion2 = getattr(self, 'legion2_time', None)

        # Check that at least one legion has an actual time
        has_legion1_time = legion1 is not None and legion1 != "none"
        has_legion2_time = legion2 is not None and legion2 != "none"

        if not has_legion1_time and not has_legion2_time:
            await interaction.response.send_message(
                "❌ At least one Legion must have a time configured!",
                ephemeral=True
            )
            return

        # Save data - use None for disabled/unset legions
        data = {}
        if has_legion1_time:
            hour1, minute1 = map(int, legion1.split(":"))
            data["legion1_hour"] = hour1
            data["legion1_minute"] = minute1
        else:
            data["legion1_hour"] = None
            data["legion1_minute"] = None

        if has_legion2_time:
            hour2, minute2 = map(int, legion2.split(":"))
            data["legion2_hour"] = hour2
            data["legion2_minute"] = minute2
        else:
            data["legion2_hour"] = None
            data["legion2_minute"] = None

        setattr(self.session, self.session_data_attr, data)

        # Mark as configured and return to hub
        self.session.mark_event_configured(self.event_name)
        await self.hub_view.show(interaction)

class FoundryConfigView(DualLegionConfigView):
    """Configuration for Foundry Battle"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(cog, session, hub_view, "Foundry Battle", "foundry_data")

class CanyonConfigView(DualLegionConfigView):
    """Configuration for Canyon Clash"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(cog, session, hub_view, "Canyon Clash", "canyon_data")

class MultiTimeSelectView(discord.ui.View):
    """Base class for multi-time selection events (Fortress Battle, Frostfire Mine)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView,
                 event_name: str, session_data_attr: str, buttons_per_row: int = 5):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.event_name = event_name
        self.session_data_attr = session_data_attr
        self.selected_times = []

        config = get_event_config(event_name)
        self.time_slots = config.get("available_times", [])

        # Pre-populate from existing data if available
        existing_data = getattr(self.session, session_data_attr, {})
        if existing_data.get("times"):
            for t in existing_data["times"]:
                time_str = f"{t['hour']:02d}:{t['minute']:02d}"
                if time_str in self.time_slots:
                    self.selected_times.append(time_str)

        # Add time buttons with dynamic row assignment
        for idx, time in enumerate(self.time_slots):
            row = idx // buttons_per_row
            button = discord.ui.Button(
                label=time,
                style=discord.ButtonStyle.secondary,
                row=row
            )
            button.callback = lambda i, t=time: self.toggle_time(i, t)
            self.add_item(button)

        # Add continue button on the next available row
        continue_row = (len(self.time_slots) - 1) // buttons_per_row + 1
        continue_button = discord.ui.Button(
            label="Continue",
            emoji="➡️",
            style=discord.ButtonStyle.success,
            row=continue_row
        )
        continue_button.callback = self.continue_to_next
        self.add_item(continue_button)

    async def show(self, interaction: discord.Interaction):
        """Show event configuration"""
        icon = get_event_icon(self.event_name)
        next_date = calculate_next_occurrence(self.event_name)
        config = get_event_config(self.event_name)

        # Get schedule description from config
        schedule_desc = config.get("fixed_days", "Scheduled event")
        time_slots_str = ", ".join(self.time_slots)

        description = f"{self.event_name} occurs **{schedule_desc}**.\n\n"

        if next_date:
            description += f"**Next Date:** {next_date.strftime('%B %d, %Y')}\n\n"

        description += (
            f"**Available Times (UTC):** {time_slots_str}\n\n"
            f"Select one or more times."
        )

        if self.selected_times:
            description += f"\n\n**Selected:** {', '.join(self.selected_times)}"

        embed = discord.Embed(
            title=f"{icon} Configure {self.event_name}",
            description=description,
            color=discord.Color.blue()
        )

        # Update button styles
        for item in self.children:
            if isinstance(item, discord.ui.Button) and item.label in self.time_slots:
                item.style = discord.ButtonStyle.success if item.label in self.selected_times else discord.ButtonStyle.secondary
            elif isinstance(item, discord.ui.Button) and item.label == "Continue":
                item.disabled = len(self.selected_times) == 0

        await interaction.response.edit_message(embed=embed, view=self)

    async def toggle_time(self, interaction: discord.Interaction, time: str):
        """Toggle time selection"""
        if time in self.selected_times:
            self.selected_times.remove(time)
        else:
            self.selected_times.append(time)
        await self.show(interaction)

    async def continue_to_next(self, interaction: discord.Interaction):
        """Save selected times and proceed"""
        if not self.selected_times:
            await interaction.response.send_message(
                "❌ Please select at least one time!",
                ephemeral=True
            )
            return

        setattr(self.session, self.session_data_attr, {
            "times": [{"hour": int(t.split(":")[0]), "minute": int(t.split(":")[1])} for t in self.selected_times]
        })

        # Mark as configured and return to hub
        self.session.mark_event_configured(self.event_name)
        await self.hub_view.show(interaction)

class StrongholdConfigView(MultiTimeSelectView):
    """Configuration for Fortress Battle"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(cog, session, hub_view, "Fortress Battle", "stronghold_data", buttons_per_row=5)

class FrostfireConfigView(MultiTimeSelectView):
    """Configuration for Frostfire Mine"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(cog, session, hub_view, "Frostfire Mine", "frostfire_data", buttons_per_row=4)

class PhaseToggleConfigView(discord.ui.View):
    """Base class for phase-based toggle configuration (Castle Battle, SvS)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView,
                 event_name: str, session_data_attr: str, phases: list):
        """
        phases: List of dicts with keys: 'name', 'emoji', 'time', 'phase_key', 'hour', 'minute'
        Example: [{"name": "Borders Open", "emoji": "🌍", "time": "10:00 UTC", "phase_key": "borders_open", "hour": 10, "minute": 0}]
        """
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.event_name = event_name
        self.session_data_attr = session_data_attr
        self.phases = phases
        self.selected_phases = {phase["phase_key"]: False for phase in phases}

        # Pre-populate from existing data if available
        existing_data = getattr(self.session, session_data_attr, {})
        if existing_data.get("times"):
            for t in existing_data["times"]:
                phase_key = t.get("phase")
                if phase_key and phase_key in self.selected_phases:
                    self.selected_phases[phase_key] = True

    async def show(self, interaction: discord.Interaction):
        """Show event configuration with toggles"""
        icon = get_event_icon(self.event_name)
        next_date = calculate_next_occurrence(self.event_name)
        config = get_event_config(self.event_name)

        # Build description showing selected notifications
        notifications = []
        phase_descriptions = []
        for phase in self.phases:
            phase_descriptions.append(f"• {phase['time']} - {phase['name']}")
            if self.selected_phases[phase["phase_key"]]:
                notifications.append(f"**{phase['time']}** - {phase['name']} ✅")

        schedule_desc = config.get("fixed_days", "Scheduled event")
        description = (
            f"{self.event_name} occurs **{schedule_desc}**.\n\n"
            f"**Next Date:** {next_date.strftime('%B %d, %Y') if next_date else 'N/A'}\n"
        )

        duration = config.get("duration_minutes")
        if duration:
            description += f"**Duration:** {duration // 60} hours\n\n"
        else:
            description += "\n"

        description += "**Select Notifications:**\nToggle buttons below to select which notifications you want:\n"
        description += "\n".join(phase_descriptions) + "\n"

        if notifications:
            description += "\n**Selected:**\n" + "\n".join(notifications)
        else:
            description += "\n⚠️ Select at least one notification to proceed."

        embed = discord.Embed(
            title=f"{icon} Configure {self.event_name}",
            description=description,
            color=discord.Color.blue()
        )

        self.clear_items()

        # Add toggle buttons for each phase
        for phase in self.phases:
            button = discord.ui.Button(
                label=phase["name"],
                emoji=phase["emoji"],
                style=discord.ButtonStyle.success if self.selected_phases[phase["phase_key"]] else discord.ButtonStyle.secondary,
                row=0
            )
            button.callback = lambda i, pk=phase["phase_key"]: self.toggle_phase(i, pk)
            self.add_item(button)

        # Add confirm button (only enabled if at least one is selected)
        confirm_button = discord.ui.Button(
            label="Confirm",
            emoji="✅",
            style=discord.ButtonStyle.primary,
            row=1,
            disabled=not any(self.selected_phases.values())
        )
        confirm_button.callback = self.confirm_selection
        self.add_item(confirm_button)

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def toggle_phase(self, interaction: discord.Interaction, phase_key: str):
        """Toggle phase notification"""
        self.selected_phases[phase_key] = not self.selected_phases[phase_key]
        await self.show(interaction)

    async def confirm_selection(self, interaction: discord.Interaction):
        """Save selected phases and proceed"""
        times = []
        for phase in self.phases:
            if self.selected_phases[phase["phase_key"]]:
                times.append({
                    "hour": phase["hour"],
                    "minute": phase["minute"],
                    "phase": phase["phase_key"]
                })

        setattr(self.session, self.session_data_attr, {"times": times})
        self.session.mark_event_configured(self.event_name)
        await self.hub_view.show(interaction)

class SunfireConfigView(PhaseToggleConfigView):
    """Configuration for Castle Battle"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        phases = [
            {"name": "Teleport Window", "emoji": "🚪", "time": "11:00 UTC", "phase_key": "teleport_window", "hour": 11, "minute": 0},
            {"name": "Battle Starts", "emoji": "⚔️", "time": "12:00 UTC", "phase_key": "battle_start", "hour": 12, "minute": 0}
        ]
        super().__init__(cog, session, hub_view, "Castle Battle", "sunfire_data", phases)

class SvSConfigView(PhaseToggleConfigView):
    """Configuration for SvS with three toggle buttons"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        phases = [
            {"name": "Borders Open", "emoji": "🌍", "time": "10:00 UTC", "phase_key": "borders_open", "hour": 10, "minute": 0},
            {"name": "Teleport Window", "emoji": "🚪", "time": "11:00 UTC", "phase_key": "teleport_window", "hour": 11, "minute": 0},
            {"name": "Battle Starts", "emoji": "⚔️", "time": "12:00 UTC", "phase_key": "battle_start", "hour": 12, "minute": 0}
        ]
        super().__init__(cog, session, hub_view, "SvS", "svs_data", phases)

class MercenaryBossesConfigView(discord.ui.View):
    """Configuration for Mercenary Bosses (up to 5 instances during 3-day window)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session
        self.hub_view = hub_view
        self.boss_times = []  # List of {"day": 0-2, "hour": int, "minute": int}

        # Pre-populate from existing data if available
        existing_data = self.session.mercenary_bosses_data
        if existing_data.get("bosses"):
            for boss in existing_data["bosses"]:
                self.boss_times.append({
                    "day": boss.get("day", 0),
                    "hour": boss.get("hour", 0),
                    "minute": boss.get("minute", 0)
                })

    async def show(self, interaction: discord.Interaction):
        """Show Mercenary Bosses configuration"""
        icon = get_event_icon("Mercenary Bosses")
        config = get_event_config("Mercenary Bosses")
        next_date = calculate_next_occurrence("Mercenary Bosses")

        # Calculate the 3-day window
        window_text = "N/A"
        if next_date:
            day1 = next_date.strftime('%B %d')
            day2 = (next_date + timedelta(days=1)).strftime('%B %d')
            day3 = (next_date + timedelta(days=2)).strftime('%B %d, %Y')
            window_text = f"{day1} - {day3}"

        # Build list of configured bosses
        boss_list = ""
        if self.boss_times:
            for idx, boss in enumerate(self.boss_times, 1):
                day_name = ["Saturday", "Sunday", "Monday"][boss["day"]]
                boss_list += f"{idx}. {day_name} at {boss['hour']:02d}:{boss['minute']:02d} UTC\n"
        else:
            boss_list = "*No bosses scheduled yet*"

        embed = discord.Embed(
            title=f"{icon} Configure Mercenary Bosses",
            description=(
                f"Mercenary Bosses occur **every 4 weeks during a 3-day window**.\n\n"
                f"**Next Event Window:** {window_text}\n"
                f"**Duration:** 3 consecutive days\n\n"
                f"You can schedule **up to 5 mercenary bosses** at any time during the 3-day window.\n"
                f"Some alliances run several bosses early, others spread them out - it's up to you!\n\n"
                f"**Scheduled Bosses ({len(self.boss_times)}/5):**\n{boss_list}"
            ),
            color=discord.Color.blue()
        )

        self.clear_items()

        # All At Once button (disabled if any bosses already added)
        configure_all_button = discord.ui.Button(
            label="All At Once",
            emoji="⚡",
            style=discord.ButtonStyle.primary,
            disabled=len(self.boss_times) > 0,
            row=0
        )
        configure_all_button.callback = self.configure_all_bosses
        self.add_item(configure_all_button)

        # Separate Events button (disabled if 5 bosses already)
        add_button = discord.ui.Button(
            label="Separate Events",
            emoji="➕",
            style=discord.ButtonStyle.secondary,
            disabled=len(self.boss_times) >= 5,
            row=0
        )
        add_button.callback = self.add_boss
        self.add_item(add_button)

        # Remove last boss button (disabled if no bosses) - row 0 right of Separate Events
        remove_button = discord.ui.Button(
            label="Remove Last",
            emoji="➖",
            style=discord.ButtonStyle.secondary,
            disabled=len(self.boss_times) == 0,
            row=0
        )
        remove_button.callback = self.remove_last_boss
        self.add_item(remove_button)

        # Clear all button (disabled if no bosses) - row 1 left side
        clear_button = discord.ui.Button(
            label="Clear All",
            emoji="🗑️",
            style=discord.ButtonStyle.danger,
            disabled=len(self.boss_times) == 0,
            row=1
        )
        clear_button.callback = self.clear_all_bosses
        self.add_item(clear_button)

        # Continue button (disabled if no bosses) - row 1 right side
        continue_button = discord.ui.Button(
            label="Continue",
            emoji="➡️",
            style=discord.ButtonStyle.success,
            disabled=len(self.boss_times) == 0,
            row=1
        )
        continue_button.callback = self.continue_to_next
        self.add_item(continue_button)

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    async def configure_all_bosses(self, interaction: discord.Interaction):
        """Show modal to configure all 5 bosses at once"""
        modal = MercenaryAllBossesModal(self)
        await interaction.response.send_modal(modal)

    async def add_boss(self, interaction: discord.Interaction):
        """Show modal to add a single boss time"""
        modal = MercenaryBossTimeModal(self)
        await interaction.response.send_modal(modal)

    async def remove_last_boss(self, interaction: discord.Interaction):
        """Remove the last boss from the list"""
        if self.boss_times:
            self.boss_times.pop()
        await self.show(interaction)

    async def clear_all_bosses(self, interaction: discord.Interaction):
        """Clear all configured bosses"""
        self.boss_times.clear()
        await self.show(interaction)

    async def continue_to_next(self, interaction: discord.Interaction):
        """Save boss times and proceed"""
        if not self.boss_times:
            await interaction.response.send_message(
                "❌ Please add at least one mercenary boss!",
                ephemeral=True
            )
            return

        self.session.mercenary_bosses_data = {
            "bosses": self.boss_times  # List of {day, hour, minute}
        }
        # Mark as configured and return to hub
        self.session.mark_event_configured("Mercenary Bosses")
        await self.hub_view.show(interaction)

class MercenaryBossTimeModal(discord.ui.Modal, title="Add Mercenary Boss"):
    def __init__(self, config_view: 'MercenaryBossesConfigView'):
        super().__init__()
        self.config_view = config_view

        self.day_input = discord.ui.TextInput(
            label="Day (Saturday/Sunday/Monday)",
            placeholder="Saturday, Sunday, or Monday",
            max_length=9,
            required=True
        )
        self.add_item(self.day_input)

        self.time_input = discord.ui.TextInput(
            label="Time (HH:MM UTC)",
            placeholder="Example: 14:30",
            max_length=5,
            required=True
        )
        self.add_item(self.time_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Process the boss time input"""
        try:
            # Validate day
            day_str = self.day_input.value.strip().lower()
            day_mapping = {
                "saturday": 0,
                "sat": 0,
                "sunday": 1,
                "sun": 1,
                "monday": 2,
                "mon": 2
            }

            if day_str not in day_mapping:
                await interaction.response.send_message(
                    "❌ Day must be Saturday, Sunday, or Monday!",
                    ephemeral=True
                )
                return

            day = day_mapping[day_str]

            # Validate time format
            time_str = self.time_input.value.strip()
            if not validate_time_slot(time_str, "5min"):
                await interaction.response.send_message(
                    "❌ Invalid time format! Use HH:MM in 5-minute increments (e.g., 14:30, 16:00)",
                    ephemeral=True
                )
                return

            hour, minute = map(int, time_str.split(":"))

            # Add to boss times
            self.config_view.boss_times.append({
                "day": day,
                "hour": hour,
                "minute": minute
            })

            # Refresh the view
            await self.config_view.show(interaction)

        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid input! Day must be Saturday/Sunday/Monday and time must be in HH:MM format.",
                ephemeral=True
            )

class MercenaryAllBossesModal(discord.ui.Modal, title="All Bosses At Once"):
    def __init__(self, config_view: 'MercenaryBossesConfigView'):
        super().__init__()
        self.config_view = config_view

        self.day_input = discord.ui.TextInput(
            label="Day (Saturday/Sunday/Monday)",
            placeholder="Saturday, Sunday, or Monday",
            max_length=9,
            required=True
        )
        self.add_item(self.day_input)

        self.start_time_input = discord.ui.TextInput(
            label="Start Time (HH:MM UTC)",
            placeholder="Example: 14:00",
            max_length=5,
            required=True
        )
        self.add_item(self.start_time_input)

    async def on_submit(self, interaction: discord.Interaction):
        """Process the all bosses configuration"""
        try:
            # Validate day
            day_str = self.day_input.value.strip().lower()
            day_mapping = {
                "saturday": 0,
                "sat": 0,
                "sunday": 1,
                "sun": 1,
                "monday": 2,
                "mon": 2
            }

            if day_str not in day_mapping:
                await interaction.response.send_message(
                    "❌ Day must be Saturday, Sunday, or Monday!",
                    ephemeral=True
                )
                return

            day = day_mapping[day_str]

            # Validate start time format
            time_str = self.start_time_input.value.strip()
            if not validate_time_slot(time_str, "5min"):
                await interaction.response.send_message(
                    "❌ Invalid time format! Use HH:MM in 5-minute increments (e.g., 14:00, 16:30)",
                    ephemeral=True
                )
                return

            start_hour, start_minute = map(int, time_str.split(":"))

            # Create a single boss time entry (all 5 bosses at the same time)
            self.config_view.boss_times.clear()
            self.config_view.boss_times.append({
                "day": day,
                "hour": start_hour,
                "minute": start_minute
            })

            # Refresh the view
            await self.config_view.show(interaction)

        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid input! Day must be Saturday/Sunday/Monday and time must be in HH:MM format.",
                ephemeral=True
            )

class DailyResetConfigView:
    """Configuration for Daily Reset (auto-configured)"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession, hub_view: EventSelectionHubView):
        self.cog = cog
        self.session = session
        self.hub_view = hub_view

    async def show(self, interaction: discord.Interaction):
        """Auto-configure Daily Reset and return to hub"""
        self.session.daily_reset_data = {
            "hour": 0,
            "minute": 0
        }
        # Mark as configured and return to hub
        self.session.mark_event_configured("Daily Reset")
        await self.hub_view.show(interaction)

class WizardPreviewView(discord.ui.View):
    """Preview all notifications before creation"""
    def __init__(self, cog: BearTrapWizard, session: WizardSession):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session

    async def show(self, interaction: discord.Interaction):
        """Show preview of all notifications to be created"""
        embed = discord.Embed(
            title="📋 Preview: Notifications to Create",
            description=(
                "Review the notifications that will be created.\n\n"
                f"**Channel:** <#{self.session.channel_id}>\n"
                f"**Timezone:** {self.session.timezone}\n\n"
                "**Events:**"
            ),
            color=discord.Color.gold()
        )

        # List all configured events
        for event in self.session.selected_events:
            icon = get_event_icon(event)
            data = self.session.get_event_data(event)

            if event == "Bear Trap" and data:
                # Handle both fresh config (with datetime) and reconstructed (hour/minute only)
                if data.get('bt1_datetime'):
                    bt1_str = data['bt1_datetime'].strftime('%d/%m/%Y %H:%M')
                else:
                    bt1_str = f"{data.get('bt1_hour', 0):02d}:{data.get('bt1_minute', 0):02d}"

                if data.get('bt2_datetime'):
                    bt2_str = data['bt2_datetime'].strftime('%d/%m/%Y %H:%M')
                else:
                    bt2_str = f"{data.get('bt2_hour', 0):02d}:{data.get('bt2_minute', 0):02d}"

                embed.add_field(
                    name=f"{icon} Bear Trap",
                    value=(
                        f"└ Trap 1: {bt1_str}\n"
                        f"└ Trap 2: {bt2_str}"
                    ),
                    inline=False
                )
            elif event == "Crazy Joe" and data:
                tue_hour = data.get('tuesday_hour')
                tue_min = data.get('tuesday_minute')
                thu_hour = data.get('thursday_hour')
                thu_min = data.get('thursday_minute')

                # Only show if we have valid data
                if tue_hour is not None and tue_min is not None:
                    if tue_hour == thu_hour and tue_min == thu_min:
                        # Same time for both days
                        value = f"└ Tuesday & Thursday: {tue_hour:02d}:{tue_min:02d}"
                    elif thu_hour is not None and thu_min is not None:
                        # Different times
                        value = f"└ Tuesday: {tue_hour:02d}:{tue_min:02d}\n└ Thursday: {thu_hour:02d}:{thu_min:02d}"
                    else:
                        # Only Tuesday configured
                        value = f"└ Tuesday: {tue_hour:02d}:{tue_min:02d}"

                    embed.add_field(
                        name=f"{icon} Crazy Joe",
                        value=value,
                        inline=False
                    )
            elif event in ["Foundry Battle", "Canyon Clash"] and data:
                times_str = ""
                if data.get("legion1_hour") is not None:
                    times_str += f"└ Legion 1: {data['legion1_hour']:02d}:{data['legion1_minute']:02d}\n"
                if data.get("legion2_hour") is not None:
                    times_str += f"└ Legion 2: {data['legion2_hour']:02d}:{data['legion2_minute']:02d}"
                if times_str.strip():
                    embed.add_field(
                        name=f"{icon} {event}",
                        value=times_str.strip(),
                        inline=False
                    )
            elif event in ["Fortress Battle", "Frostfire Mine", "Castle Battle", "SvS"] and data:
                times = data.get("times", [])
                if times:
                    times_list = " ".join([f"{t['hour']:02d}:{t['minute']:02d}" for t in times])
                    embed.add_field(
                        name=f"{icon} {event}",
                        value=f"└ Times: {times_list}",
                        inline=False
                    )
            elif event == "Mercenary Bosses" and data:
                bosses = data.get("bosses", [])
                if bosses:
                    boss_lines = []
                    for i, b in enumerate(bosses):
                        hour = b.get('hour', 0)
                        minute = b.get('minute', 0)
                        if 'day' in b:
                            boss_lines.append(f"└ Day {b['day']}: {hour:02d}:{minute:02d}")
                        else:
                            # Reconstructed from DB - no day info, show as Boss N
                            boss_lines.append(f"└ Boss {i + 1}: {hour:02d}:{minute:02d}")
                    embed.add_field(
                        name=f"{icon} {event}",
                        value="\n".join(boss_lines),
                        inline=False
                    )
            elif event == "Daily Reset" and data:
                embed.add_field(
                    name=f"{icon} Daily Reset",
                    value="└ Time: 00:00",
                    inline=False
                )

        embed.set_footer(text="Click 'Create All' to create these notifications.")

        # Add buttons
        create_button = discord.ui.Button(
            label="Create All",
            emoji="✨",
            style=discord.ButtonStyle.success,
            row=0
        )
        create_button.callback = self.create_all_notifications
        self.add_item(create_button)

        cancel_button = discord.ui.Button(
            label="Cancel",
            emoji="❌",
            style=discord.ButtonStyle.danger,
            row=0
        )
        cancel_button.callback = self.cancel_wizard
        self.add_item(cancel_button)

        await interaction.response.edit_message(embed=embed, view=self)

    def _get_instance_display_name(self, event_name: str, instance_id: str, hour: int = None, minute: int = None) -> str:
        """Convert instance_id to human-readable display name for completion message."""
        display_map = {
            "Bear Trap": {"bt1": "Bear 1", "bt2": "Bear 2"},
            "Crazy Joe": {"tuesday": "Tuesday", "thursday": "Thursday"},
            "Foundry Battle": {"legion1": "Legion 1", "legion2": "Legion 2"},
            "Canyon Clash": {"legion1": "Legion 1", "legion2": "Legion 2"},
            "Castle Battle": {"teleport_window": "Teleport Window", "battle_start": "Battle Start"},
            "SvS": {"borders_open": "Borders Open", "teleport_window": "Teleport Window", "battle_start": "Battle Start"},
        }

        # Check if event has predefined display names
        if event_name in display_map and instance_id in display_map[event_name]:
            return display_map[event_name][instance_id]

        # Fortress Battle and Frostfire Mine use actual times as display
        if event_name in ["Fortress Battle", "Frostfire Mine"]:
            if hour is not None and minute is not None:
                return f"{hour:02d}:{minute:02d}"
            return instance_id

        # Mercenary Bosses: boss_0 -> "Boss 1"
        if event_name == "Mercenary Bosses" and instance_id.startswith("boss_"):
            try:
                idx = int(instance_id.split("_")[1])
                return f"Boss {idx + 1}"
            except (ValueError, IndexError):
                return instance_id

        # Daily Reset has no instance display
        if event_name == "Daily Reset":
            return None

        return instance_id

    async def _create_or_update_notification(self, bear_trap_cog, interaction, event_name: str,
                                             instance_id: str, hour: int, minute: int,
                                             start_date, repeat_minutes: int, description: str,
                                             embed_data: dict) -> tuple:
        """
        Create or update a single notification instance.
        Returns (created, updated, disabled, action) where action is "added", "updated", or "enabled".
        """
        # Look for existing notification with this event_type and instance_identifier
        existing = self.session.original_instance_states.get((event_name, instance_id))

        if existing:
            # UPDATE existing notification
            await bear_trap_cog.update_notification(
                notification_id=existing["id"],
                hour=hour,
                minute=minute,
                timezone=self.session.timezone,
                description=description,
                notification_type=self.session.notification_type,
                mention_type=self.session.mention_type,
                repeat_minutes=repeat_minutes,
                event_type=event_name,
                embed_data=embed_data,
                instance_identifier=instance_id,
                skip_board_update=True
            )
            # Re-enable if it was disabled
            if not existing.get("is_enabled", 1):
                await bear_trap_cog.toggle_notification(existing["id"], enabled=True, skip_board_update=True)
                return (0, 1, 0, "enabled")
            return (0, 1, 0, "updated")
        else:
            # CREATE new notification
            bear_trap_cog.current_embed_data = embed_data
            await bear_trap_cog.save_notification(
                guild_id=interaction.guild_id,
                channel_id=self.session.channel_id,
                skip_board_update=True,
                start_date=start_date,
                hour=hour,
                minute=minute,
                timezone=self.session.timezone,
                description=description,
                created_by=interaction.user.id,
                notification_type=self.session.notification_type,
                mention_type=self.session.mention_type,
                repeat_enabled=repeat_minutes > 0,
                repeat_minutes=repeat_minutes,
                event_type=event_name,
                wizard_batch_id=self.session.wizard_batch_id,
                instance_identifier=instance_id
            )
            return (1, 0, 0, "added")

    async def _disable_instance(self, bear_trap_cog, event_name: str, instance_id: str) -> int:
        """Disable a specific instance if it exists and is enabled. Returns 1 if disabled, 0 otherwise."""
        existing = self.session.original_instance_states.get((event_name, instance_id))
        if existing and existing.get("is_enabled", 1):
            await bear_trap_cog.toggle_notification(existing["id"], enabled=False, skip_board_update=True)
            return 1
        return 0

    async def create_all_notifications(self, interaction: discord.Interaction):
        """Create, update, or disable notifications based on configuration changes"""
        try:
            await interaction.response.defer()

            # Show progress indication
            progress_embed = discord.Embed(
                title="⏳ Processing Notifications...",
                description="Please wait while notifications are being updated.",
                color=discord.Color.blue()
            )
            await interaction.edit_original_response(embed=progress_embed, view=None)

            bear_trap_cog = self.cog.bot.get_cog("BearTrap")
            if not bear_trap_cog:
                await interaction.edit_original_response(
                    embed=discord.Embed(
                        title="❌ Error",
                        description="BearTrap cog not found. Cannot create notifications.",
                        color=discord.Color.red()
                    )
                )
                return

            created_count = 0
            updated_count = 0
            disabled_count = 0
            event_changes = {}
            from datetime import datetime, timedelta
            import pytz

            tz = pytz.timezone(self.session.timezone)
            now = datetime.now(tz)

            # Set default notification_type if not set (type 2 = 10, 5, 0 minutes before)
            if self.session.notification_type is None:
                self.session.notification_type = 2

            # Determine notification times based on notification_type
            if self.session.notification_type == 6 and self.session.custom_times:
                custom_times_str = self.session.custom_times
                description_prefix = f"CUSTOM_TIMES:{custom_times_str}|EMBED_MESSAGE:"
            else:
                description_prefix = "EMBED_MESSAGE:"

            # First, disable events that were previously configured but now unconfigured
            for event_type in self.session.originally_configured_events:
                if event_type not in self.session.configured_events:
                    # Disable all instances of this event - record as whole event disabled
                    existing_notifs = self.session.existing_notifications_raw.get(event_type, [])
                    any_disabled = False
                    for notif in existing_notifs:
                        if notif.get("is_enabled", 1):
                            await bear_trap_cog.toggle_notification(notif["id"], enabled=False, skip_board_update=True)
                            disabled_count += 1
                            any_disabled = True
                    if any_disabled:
                        event_changes[event_type] = [(None, "disabled")]

            # Create notifications for each configured event
            for event_name in self.session.selected_events:
                event_data = self.session.get_event_data(event_name)

                # Get event config for image/thumbnail URLs and calculate next occurrence
                from .bear_event_types import get_event_config, calculate_next_occurrence
                event_config = get_event_config(event_name)

                # Calculate next occurrence for global events (returns None for custom events like Bear Trap)
                event_next_occurrence = calculate_next_occurrence(event_name)
                if not event_next_occurrence:
                    event_next_occurrence = now  # Fallback to now for custom events

                # Prepare embed data with image and thumbnail URLs
                embed_data = {
                    'title': f"{event_name} Notification",
                    'description': event_config.get('description', ''),
                    'color': discord.Color.blue().value,
                    'image_url': event_config.get('image_url', ''),
                    'thumbnail_url': event_config.get('thumbnail_url', ''),
                    'footer': None,
                    'author': None,
                    'mention_message': None
                }

                # Set embed data on the bear_trap_cog so save_notification can use it
                bear_trap_cog.current_embed_data = embed_data

                # Create description for this event
                description = f"{description_prefix}{event_name}"

                if event_name == "Bear Trap":
                    repeat_minutes = 0
                    if event_data.get("repeat_days"):
                        repeat_minutes = event_data["repeat_days"] * 24 * 60

                    # Bear Trap 1
                    bt1_datetime = event_data.get("bt1_datetime")
                    if bt1_datetime:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, "Bear Trap", "bt1",
                            event_data["bt1_hour"], event_data["bt1_minute"],
                            bt1_datetime, repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":  # Only track non-update changes
                            display = self._get_instance_display_name("Bear Trap", "bt1")
                            event_changes.setdefault(event_name, []).append((display, action))

                    # Bear Trap 2
                    bt2_datetime = event_data.get("bt2_datetime")
                    if bt2_datetime:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, "Bear Trap", "bt2",
                            event_data["bt2_hour"], event_data["bt2_minute"],
                            bt2_datetime, repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name("Bear Trap", "bt2")
                            event_changes.setdefault(event_name, []).append((display, action))

                elif event_name == "Crazy Joe":
                    # Tuesday and Thursday every 4 weeks
                    next_tuesday, next_thursday = calculate_crazy_joe_dates(now)
                    repeat_minutes = 28 * 24 * 60  # 4-week repeat

                    tuesday_hour = event_data.get("tuesday_hour")
                    tuesday_minute = event_data.get("tuesday_minute")
                    if tuesday_hour is not None and next_tuesday:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, "Crazy Joe", "tuesday",
                            tuesday_hour, tuesday_minute, next_tuesday,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name("Crazy Joe", "tuesday")
                            event_changes.setdefault(event_name, []).append((display, action))

                    thursday_hour = event_data.get("thursday_hour")
                    thursday_minute = event_data.get("thursday_minute")
                    if thursday_hour is not None and next_thursday:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, "Crazy Joe", "thursday",
                            thursday_hour, thursday_minute, next_thursday,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name("Crazy Joe", "thursday")
                            event_changes.setdefault(event_name, []).append((display, action))

                elif event_name == "Foundry Battle":
                    # Every 2 weeks on Sunday
                    repeat_minutes = 14 * 24 * 60

                    legion1_hour = event_data.get("legion1_hour")
                    legion1_minute = event_data.get("legion1_minute")
                    if legion1_hour is not None:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, event_name, "legion1",
                            legion1_hour, legion1_minute, event_next_occurrence,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name(event_name, "legion1")
                            event_changes.setdefault(event_name, []).append((display, action))
                    else:
                        # Legion 1 disabled - disable if existed
                        d = await self._disable_instance(bear_trap_cog, event_name, "legion1")
                        disabled_count += d
                        if d > 0:
                            display = self._get_instance_display_name(event_name, "legion1")
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                    legion2_hour = event_data.get("legion2_hour")
                    legion2_minute = event_data.get("legion2_minute")
                    if legion2_hour is not None:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, event_name, "legion2",
                            legion2_hour, legion2_minute, event_next_occurrence,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name(event_name, "legion2")
                            event_changes.setdefault(event_name, []).append((display, action))
                    else:
                        # Legion 2 disabled - disable if existed
                        d = await self._disable_instance(bear_trap_cog, event_name, "legion2")
                        disabled_count += d
                        if d > 0:
                            display = self._get_instance_display_name(event_name, "legion2")
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                elif event_name == "Canyon Clash":
                    # Every 4 weeks on Saturday
                    repeat_minutes = 28 * 24 * 60

                    legion1_hour = event_data.get("legion1_hour")
                    legion1_minute = event_data.get("legion1_minute")
                    if legion1_hour is not None:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, event_name, "legion1",
                            legion1_hour, legion1_minute, event_next_occurrence,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name(event_name, "legion1")
                            event_changes.setdefault(event_name, []).append((display, action))
                    else:
                        d = await self._disable_instance(bear_trap_cog, event_name, "legion1")
                        disabled_count += d
                        if d > 0:
                            display = self._get_instance_display_name(event_name, "legion1")
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                    legion2_hour = event_data.get("legion2_hour")
                    legion2_minute = event_data.get("legion2_minute")
                    if legion2_hour is not None:
                        c, u, _, action = await self._create_or_update_notification(
                            bear_trap_cog, interaction, event_name, "legion2",
                            legion2_hour, legion2_minute, event_next_occurrence,
                            repeat_minutes, description, embed_data
                        )
                        created_count += c
                        updated_count += u
                        if action != "updated":
                            display = self._get_instance_display_name(event_name, "legion2")
                            event_changes.setdefault(event_name, []).append((display, action))
                    else:
                        d = await self._disable_instance(bear_trap_cog, event_name, "legion2")
                        disabled_count += d
                        if d > 0:
                            display = self._get_instance_display_name(event_name, "legion2")
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                elif event_name == "Fortress Battle":
                    # Every Friday - handle times with phase as instance_id
                    repeat_minutes = 7 * 24 * 60
                    times = event_data.get("times", [])
                    processed_phases = set()

                    for idx, time_data in enumerate(times):
                        hour = time_data.get("hour")
                        minute = time_data.get("minute")
                        phase = time_data.get("phase") or f"time_{idx}"
                        processed_phases.add(phase)

                        if hour is not None:
                            phase_description = description
                            phase_embed = embed_data.copy()
                            if phase and event_config.get("descriptions"):
                                phase_desc = event_config["descriptions"].get(phase)
                                if phase_desc:
                                    phase_embed['description'] = phase_desc
                                    phase_description = f"{description_prefix}{event_name} - {phase}"

                            c, u, _, action = await self._create_or_update_notification(
                                bear_trap_cog, interaction, event_name, phase,
                                hour, minute, event_next_occurrence,
                                repeat_minutes, phase_description, phase_embed
                            )
                            created_count += c
                            updated_count += u
                            if action != "updated":
                                # For Fortress Battle, use the time as display name
                                display = self._get_instance_display_name(event_name, phase, hour, minute)
                                event_changes.setdefault(event_name, []).append((display, action))

                    # Disable any previously existing phases that are no longer selected
                    existing_notifs = self.session.existing_notifications_raw.get(event_name, [])
                    for notif in existing_notifs:
                        old_phase = notif.get("instance_identifier") or "default"
                        if old_phase not in processed_phases and notif.get("is_enabled", 1):
                            await bear_trap_cog.toggle_notification(notif["id"], enabled=False, skip_board_update=True)
                            disabled_count += 1
                            # Get the time from the old notification for display
                            display = self._get_instance_display_name(event_name, old_phase, notif.get("hour"), notif.get("minute"))
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                elif event_name in ["Frostfire Mine", "Castle Battle", "SvS"]:
                    # Every 4 weeks - handle times with phase as instance_id
                    repeat_minutes = 28 * 24 * 60
                    times = event_data.get("times", [])
                    processed_phases = set()

                    for idx, time_data in enumerate(times):
                        hour = time_data.get("hour")
                        minute = time_data.get("minute")
                        phase = time_data.get("phase") or f"time_{idx}"
                        processed_phases.add(phase)

                        if hour is not None:
                            phase_description = description
                            phase_embed = embed_data.copy()
                            if phase and event_config.get("descriptions"):
                                phase_desc = event_config["descriptions"].get(phase)
                                if phase_desc:
                                    phase_embed['description'] = phase_desc
                                    phase_description = f"{description_prefix}{event_name} - {phase}"

                            c, u, _, action = await self._create_or_update_notification(
                                bear_trap_cog, interaction, event_name, phase,
                                hour, minute, event_next_occurrence,
                                repeat_minutes, phase_description, phase_embed
                            )
                            created_count += c
                            updated_count += u
                            if action != "updated":
                                # Frostfire uses times, Castle/SvS use phase names
                                display = self._get_instance_display_name(event_name, phase, hour, minute)
                                event_changes.setdefault(event_name, []).append((display, action))

                    # Disable any previously existing phases that are no longer selected
                    existing_notifs = self.session.existing_notifications_raw.get(event_name, [])
                    for notif in existing_notifs:
                        old_phase = notif.get("instance_identifier") or "default"
                        if old_phase not in processed_phases and notif.get("is_enabled", 1):
                            await bear_trap_cog.toggle_notification(notif["id"], enabled=False, skip_board_update=True)
                            disabled_count += 1
                            display = self._get_instance_display_name(event_name, old_phase, notif.get("hour"), notif.get("minute"))
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                elif event_name == "Mercenary Bosses":
                    # Boss times across 3-day window (repeats every 4 weeks)
                    repeat_minutes = 28 * 24 * 60
                    bosses = event_data.get("bosses", [])
                    processed_instances = set()

                    for idx, boss in enumerate(bosses):
                        day = boss.get("day")  # 0=Saturday, 1=Sunday, 2=Monday
                        hour = boss.get("hour")
                        minute = boss.get("minute")
                        instance_id = f"boss_{idx}"
                        processed_instances.add(instance_id)

                        if day is not None and hour is not None:
                            start_date = event_next_occurrence + timedelta(days=day)
                            c, u, _, action = await self._create_or_update_notification(
                                bear_trap_cog, interaction, event_name, instance_id,
                                hour, minute, start_date,
                                repeat_minutes, description, embed_data
                            )
                            created_count += c
                            updated_count += u
                            if action != "updated":
                                display = self._get_instance_display_name(event_name, instance_id)
                                event_changes.setdefault(event_name, []).append((display, action))

                    # Disable any previously existing boss instances no longer needed
                    existing_notifs = self.session.existing_notifications_raw.get(event_name, [])
                    for notif in existing_notifs:
                        old_instance = notif.get("instance_identifier") or "default"
                        if old_instance not in processed_instances and notif.get("is_enabled", 1):
                            await bear_trap_cog.toggle_notification(notif["id"], enabled=False, skip_board_update=True)
                            disabled_count += 1
                            display = self._get_instance_display_name(event_name, old_instance)
                            event_changes.setdefault(event_name, []).append((display, "disabled"))

                elif event_name == "Daily Reset":
                    c, u, _, action = await self._create_or_update_notification(
                        bear_trap_cog, interaction, "Daily Reset", "daily",
                        0, 0, event_next_occurrence,
                        24 * 60, description, embed_data
                    )
                    created_count += c
                    updated_count += u
                    if action != "updated":
                        # Daily Reset uses None as display (single-instance event)
                        event_changes.setdefault(event_name, []).append((None, action))

            # Update schedule boards once after all notifications are processed
            schedule_cog = self.cog.bot.get_cog("BearTrapSchedule")
            if schedule_cog:
                await schedule_cog.on_notification_created(interaction.guild_id, self.session.channel_id)

            # Build completion message with per-event changes
            embed = discord.Embed(
                title="✅ Wizard Complete!",
                description=f"**Channel:** <#{self.session.channel_id}>\n**Timezone:** {self.session.timezone}",
                color=discord.Color.green()
            )

            # Build event list with change annotations
            event_lines = []

            # First, add all configured events
            for event in self.session.selected_events:
                icon = get_event_icon(event)
                changes = event_changes.get(event, [])

                if not changes:
                    # No changes - just show event name (implied unchanged)
                    event_lines.append(f"• {icon} {event}")
                elif len(changes) == 1 and changes[0][0] is None:
                    # Single-instance event (Daily Reset) or entire event action
                    event_lines.append(f"• {icon} {event} - {changes[0][1]}")
                else:
                    # Multiple instance changes
                    change_strs = [f"{inst} {action}" for inst, action in changes]
                    event_lines.append(f"• {icon} {event} - {', '.join(change_strs)}")

            # Then, add disabled events (previously configured but now unconfigured)
            for event_type in self.session.originally_configured_events:
                if event_type not in self.session.configured_events:
                    icon = get_event_icon(event_type)
                    event_lines.append(f"• {icon} {event_type} - disabled")

            embed.add_field(
                name="📋 Events",
                value="\n".join(event_lines) if event_lines else "No events configured",
                inline=False
            )

            embed.add_field(
                name="📅 Next Step",
                value="Would you like to set up a schedule board that automatically shows the upcoming events in the channel?",
                inline=False
            )

            view = WizardCompletionView(self.cog, self.session)
            await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            print(f"Error creating notifications: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            await interaction.edit_original_response(
                embed=discord.Embed(
                    title="❌ Error Creating Notifications",
                    description=f"An error occurred: {str(e)}",
                    color=discord.Color.red()
                ),
                view=None
            )

    async def cancel_wizard(self, interaction: discord.Interaction):
        """Cancel wizard"""
        embed = discord.Embed(
            title="Wizard Cancelled",
            description="No notifications were created.",
            color=discord.Color.red()
        )
        await interaction.response.edit_message(embed=embed, view=None)

class WizardCompletionView(discord.ui.View):
    def __init__(self, cog: BearTrapWizard, session: WizardSession):
        super().__init__(timeout=3600)
        self.cog = cog
        self.session = session

    @discord.ui.button(label="Create Schedule Board", emoji="📋", style=discord.ButtonStyle.success, row=0)
    async def create_board(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Create a schedule board for the channel"""
        try:
            schedule_cog = self.cog.bot.get_cog("BearTrapSchedule")
            if not schedule_cog:
                await interaction.response.send_message(
                    "❌ Schedule board module not found.",
                    ephemeral=True
                )
                return
            await interaction.response.defer()
            bear_trap_cog = self.cog.bot.get_cog("BearTrap")
            if not bear_trap_cog:
                await interaction.followup.send(
                    "❌ BearTrap module not found.",
                    ephemeral=True
                )
                return
            wizard_batch_id = f"wizard_{self.session.guild_id}_{self.session.channel_id}"
            notifications = bear_trap_cog.get_wizard_notifications_for_channel(
                self.session.guild_id,
                self.session.channel_id
            )
            if not notifications:
                await interaction.followup.send(
                    "❌ No wizard notifications found to create board.",
                    ephemeral=True
                )
                return
            channel = self.cog.bot.get_channel(self.session.channel_id)
            if not channel:
                await interaction.followup.send(
                    "❌ Could not access the channel.",
                    ephemeral=True
                )
                return

            # Check if a channel-specific board already exists for this channel
            schedule_cog.cursor.execute("""
                SELECT id, board_type FROM notification_schedule_boards
                WHERE guild_id = ? AND channel_id = ? AND board_type = 'channel' AND target_channel_id = ?
            """, (interaction.guild.id, self.session.channel_id, self.session.channel_id))
            existing_channel_board = schedule_cog.cursor.fetchone()

            # Also check for existing server boards in this channel (to warn user)
            schedule_cog.cursor.execute("""
                SELECT COUNT(*) FROM notification_schedule_boards
                WHERE guild_id = ? AND channel_id = ? AND board_type = 'server'
            """, (interaction.guild.id, self.session.channel_id))
            server_boards_count = schedule_cog.cursor.fetchone()[0]

            if existing_channel_board:
                # Update existing channel board
                board_id = existing_channel_board[0]
                await schedule_cog.update_schedule_board(board_id)
                embed = discord.Embed(
                    title="✅ Schedule Board Updated!",
                    description=f"The existing channel-specific schedule board has been updated with your new notifications.",
                    color=discord.Color.green()
                )
                if server_boards_count > 0:
                    embed.add_field(
                        name="⚠️ Note",
                        value=f"This channel also has {server_boards_count} server-wide schedule board(s). You may want to remove them to avoid confusion.",
                        inline=False
                    )
                await interaction.edit_original_response(embed=embed, view=None)
            else:
                # Create new channel-specific schedule board
                board_id, error = await schedule_cog.create_schedule_board(
                    guild_id=interaction.guild.id,
                    channel_id=self.session.channel_id,
                    board_type="channel",
                    target_channel_id=self.session.channel_id,
                    creator_id=interaction.user.id,
                    settings={}
                )

                if error:
                    await interaction.followup.send(
                        f"❌ Error creating schedule board: {error}",
                        ephemeral=True
                    )
                    return

                if board_id:
                    embed = discord.Embed(
                        title="✅ Schedule Board Created!",
                        description=f"A channel-specific schedule board has been created and pinned in <#{self.session.channel_id}>.\n\nThis board will only show notifications for this channel.",
                        color=discord.Color.green()
                    )
                    if server_boards_count > 0:
                        embed.add_field(
                            name="⚠️ Note",
                            value=f"This channel also has {server_boards_count} server-wide schedule board(s). You may want to remove them using `/schedule manage` to avoid confusion.",
                            inline=False
                        )
                    await interaction.edit_original_response(embed=embed, view=None)
                else:
                    await interaction.followup.send(
                        "❌ Failed to create schedule board (no board ID returned)",
                        ephemeral=True
                    )
        except Exception as e:
            print(f"Error creating schedule board: {e}")
            await interaction.followup.send(
                f"❌ Error creating schedule board: {str(e)}",
                ephemeral=True
            )

    @discord.ui.button(label="Done", emoji="✅", style=discord.ButtonStyle.secondary, row=0)
    async def done(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Close the wizard"""
        embed = discord.Embed(
            title="✅ Wizard Complete!",
            description="All done! Your notifications are ready.",
            color=discord.Color.green()
        )
        await interaction.response.edit_message(embed=embed, view=None)

async def setup(bot):
    await bot.add_cog(BearTrapWizard(bot))