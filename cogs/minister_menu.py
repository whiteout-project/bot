import discord
from discord.ext import commands
import sqlite3
import asyncio
import time
import hashlib
import aiohttp
from aiohttp_socks import ProxyConnector

SECRET = 'tB87#kPtkxqOS2'

class MinisterChannelView(discord.ui.View):
    def __init__(self, bot, cog):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog

    @discord.ui.button(label="Construction Day", style=discord.ButtonStyle.primary, emoji="🔨")
    async def construction_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Construction Day")

    @discord.ui.button(label="Research Day", style=discord.ButtonStyle.primary, emoji="🔬")
    async def research_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Research Day")

    @discord.ui.button(label="Troops Training Day", style=discord.ButtonStyle.primary, emoji="⚔️")
    async def troops_training_day(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_activity_selection(interaction, "Troops Training Day")

    @discord.ui.button(label="Channel Configuration", style=discord.ButtonStyle.secondary, emoji="⚙️", row=1)
    async def channel_configuration(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_channel_configuration_menu(interaction)

    @discord.ui.button(label="Delete Server ID", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            svs_conn = sqlite3.connect("db/svs.sqlite")
            svs_cursor = svs_conn.cursor()
            svs_cursor.execute("DELETE FROM reference WHERE context=?", ("minister guild id",))
            svs_conn.commit()
            svs_conn.close()
            await interaction.response.send_message("✅ Server ID deleted from the database.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to delete server ID: {e}", ephemeral=True)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            other_features_cog = self.cog.bot.get_cog("OtherFeatures")
            if other_features_cog:
                await other_features_cog.show_other_features_menu(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Other Features module not found.",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ An error occurred while returning to Other Features menu: {e}",
                ephemeral=True
            )

    async def _handle_activity_selection(self, interaction: discord.Interaction, activity_name: str):
        minister_schedule_cog = self.cog.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.response.send_message("❌ Minister Schedule module not found.", ephemeral=True)
            return

        log_guild = await minister_schedule_cog.get_log_guild(interaction.guild)

        if not log_guild:
            await interaction.response.send_message(
                "Could not find the minister log server. Make sure the bot is in that server.\n\nIf issue persists, run the `/settings` command --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server",
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

        # Show the minister management menu for this activity
        await self.cog.show_minister_activity_menu(interaction, activity_name)

class ChannelConfigurationView(discord.ui.View):
    def __init__(self, bot, cog):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog

    @discord.ui.button(label="Construction Channel", style=discord.ButtonStyle.secondary, emoji="🔨")
    async def construction_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Construction Day channel", "Construction Day")

    @discord.ui.button(label="Research Channel", style=discord.ButtonStyle.secondary, emoji="🔬")
    async def research_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Research Day channel", "Research Day")

    @discord.ui.button(label="Training Channel", style=discord.ButtonStyle.secondary, emoji="⚔️")
    async def training_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "Troops Training Day channel", "Troops Training Day")

    @discord.ui.button(label="Log Channel", style=discord.ButtonStyle.secondary, emoji="📄")
    async def log_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._handle_channel_selection(interaction, "minister log channel", "general logging")

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_channel_menu(interaction)

    async def _handle_channel_selection(self, interaction: discord.Interaction, channel_context: str, activity_name: str):
        minister_schedule_cog = self.cog.bot.get_cog("MinisterSchedule")
        if not minister_schedule_cog:
            await interaction.response.send_message("❌ Minister Schedule module not found.", ephemeral=True)
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
                
            @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️", row=1)
            async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
                # Properly restore the menu with embed (fix issue with "Select a channel for..." leftover)
                embed = discord.Embed(
                    title="⚙️ Channel Configuration",
                    description=(
                        "Configure channels for minister scheduling:\n\n"
                        "**Channel Types**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🔨 **Construction Channel** - Shows available Construction Day slots\n"
                        "🔬 **Research Channel** - Shows available Research Day slots\n"
                        "⚔️ **Training Channel** - Shows available Training Day slots\n"
                        "📄 **Log Channel** - Receives add/remove notifications\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Select a channel type to configure:"
                    ),
                    color=discord.Color.blue()
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

class MinisterActivityView(discord.ui.View):
    def __init__(self, bot, cog, activity_name):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name

    @discord.ui.button(label="Add", style=discord.ButtonStyle.success, emoji="➕")
    async def add_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_add_minister_menu(interaction, self.activity_name)

    @discord.ui.button(label="Remove", style=discord.ButtonStyle.danger, emoji="➖")
    async def remove_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_remove_minister_menu(interaction, self.activity_name)

    @discord.ui.button(label="List", style=discord.ButtonStyle.secondary, emoji="📋")
    async def list_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_list_minister_menu(interaction, self.activity_name)

    @discord.ui.button(label="Clear All", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def clear_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_clear_confirmation(interaction, self.activity_name)

    @discord.ui.button(label="Update Names", style=discord.ButtonStyle.secondary, emoji="🔄")
    async def update_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.update_minister_names(interaction, self.activity_name)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_channel_menu(interaction)

class UserSelectView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, available_users):
        super().__init__(timeout=300)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.available_users = available_users
        self.selected_user = None

        self.add_item(UserSelect(available_users))
        self.add_item(BackToActivityButton(cog, activity_name))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class UserSelect(discord.ui.Select):
    def __init__(self, available_users):
        options = []
        for fid, nickname in available_users[:25]: # Discord limit
            options.append(discord.SelectOption(
                label=f"{nickname}",
                value=str(fid),
                description=f"ID: {fid}"
            ))
        
        super().__init__(
            placeholder="Select a user to add...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_fid = self.values[0]
        self.view.selected_user = selected_fid
        
        minister_cog = self.view.cog
        await minister_cog.show_time_selection(interaction, self.view.activity_name, selected_fid)

class TimeSelectView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, fid, available_times):
        super().__init__(timeout=300)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.fid = fid
        self.available_times = available_times
        
        self.add_item(TimeSelect(available_times))
        self.add_item(BackToUserSelectButton(cog, activity_name))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class TimeSelect(discord.ui.Select):
    def __init__(self, available_times):
        options = []
        for time_slot in available_times[:25]: # Discord limit
            options.append(discord.SelectOption(
                label=time_slot,
                value=time_slot
            ))
        
        super().__init__(
            placeholder="Select an available time slot...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_time = self.values[0]
        
        minister_cog = self.view.cog
        await minister_cog.complete_booking(interaction, self.view.activity_name, self.view.fid, selected_time)

class RemoveUserSelectView(discord.ui.View):
    def __init__(self, bot, cog, activity_name, booked_users):
        super().__init__(timeout=300)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name
        self.booked_users = booked_users
        
        self.add_item(RemoveUserSelect(booked_users))
        self.add_item(BackToActivityButton(cog, activity_name))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class RemoveUserSelect(discord.ui.Select):
    def __init__(self, booked_users):
        options = []
        for fid, nickname in booked_users[:25]: # Discord limit
            options.append(discord.SelectOption(
                label=f"{nickname}",
                value=str(fid),
                description=f"ID: {fid}"
            ))
        
        super().__init__(
            placeholder="Select a user to remove...",
            options=options,
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        selected_fid = self.values[0]
        
        minister_cog = self.view.cog
        await minister_cog.complete_removal(interaction, self.view.activity_name, selected_fid)

class ListTypeView(discord.ui.View):
    def __init__(self, bot, cog, activity_name):
        super().__init__(timeout=300)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name

    @discord.ui.button(label="Full Schedule", style=discord.ButtonStyle.primary, emoji="📋")
    async def full_schedule_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_full_schedule_above_buttons(interaction, self.activity_name, update=False)

    @discord.ui.button(label="Available Slots Only", style=discord.ButtonStyle.secondary, emoji="⏰")
    async def available_only_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_available_schedule_above_buttons(interaction, self.activity_name)

    @discord.ui.button(label="Booked Slots Only", style=discord.ButtonStyle.secondary, emoji="📅")
    async def booked_only_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_booked_schedule_above_buttons(interaction, self.activity_name)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_minister_activity_menu_from_schedule(interaction, self.activity_name)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class ClearConfirmationView(discord.ui.View):
    def __init__(self, bot, cog, activity_name):
        super().__init__(timeout=60)
        self.bot = bot
        self.cog = cog
        self.activity_name = activity_name

    @discord.ui.button(label="Yes, Clear All", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.execute_clear_all(interaction, self.activity_name)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Clear operation cancelled.", ephemeral=True)
        await self.cog.update_original_settings_message(self.activity_name)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

class BackToActivityButton(discord.ui.Button):
    def __init__(self, cog, activity_name):
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, emoji="◀️")
        self.cog = cog
        self.activity_name = activity_name

    async def callback(self, interaction: discord.Interaction):
        await self.cog.show_minister_activity_menu_from_schedule(interaction, self.activity_name)

class BackToUserSelectButton(discord.ui.Button):
    def __init__(self, cog, activity_name):
        super().__init__(label="Back", style=discord.ButtonStyle.secondary, emoji="◀️")
        self.cog = cog
        self.activity_name = activity_name

    async def callback(self, interaction: discord.Interaction):
        await self.cog.show_add_minister_menu(interaction, self.activity_name)

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
        
        embed = discord.Embed(
            title="🏛️ Minister Scheduling",
            description=(
                "Manage your minister appointments here:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🔨 Manage Construction Day appointments\n"
                "🔬 Manage Research Day appointments\n"
                "⚔️ Manage Training Day appointments\n"
                "⚙️ Configure log channels\n"
                "🗑️ Delete server ID\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )

        view = MinisterChannelView(self.bot, self)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            pass

    async def show_channel_configuration_menu(self, interaction: discord.Interaction):
        embed = discord.Embed(
            title="⚙️ Channel Configuration",
            description=(
                "Configure channels for minister scheduling:\n\n"
                "**Channel Types**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🔨 **Construction Channel** - Shows available Construction Day slots\n"
                "🔬 **Research Channel** - Shows available Research Day slots\n"
                "⚔️ **Training Channel** - Shows available Training Day slots\n"
                "📄 **Log Channel** - Receives add/remove notifications\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "Select a channel type to configure:"
            ),
            color=discord.Color.blue()
        )

        view = ChannelConfigurationView(self.bot, self)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)
            
    async def show_minister_activity_menu_from_schedule(self, interaction: discord.Interaction, activity_name: str):
        """Return from schedule view to activity menu"""
        # Get current stats
        self.svs_cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_count = self.svs_cursor.fetchone()[0]
        available_count = 48 - booked_count # 48 total 30-minute slots in 24 hours

        embed = discord.Embed(
            title=f"🏛️ {activity_name} Management",
            description=(
                f"Manage appointments for **{activity_name}**:\n\n"
                f"**Current Status**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 **Booked Slots:** `{booked_count}/48`\n"
                f"⏰ **Available Slots:** `{available_count}/48`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**Available Actions**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"➕ Add new appointment\n"
                f"➖ Remove existing appointment\n"
                f"📋 View schedule\n"
                f"🗑️ Clear all appointments\n"
                f"🔄 Update names from API\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )

        view = MinisterActivityView(self.bot, self, activity_name)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except:
            pass

    async def update_original_settings_message(self, activity_name: str):
        """Update the original settings message back to the activity menu"""
        if not self.original_interaction:
            return
            
        # Get current stats
        self.svs_cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_count = self.svs_cursor.fetchone()[0]
        available_count = 48 - booked_count # 48 total 30-minute slots in 24 hours

        embed = discord.Embed(
            title=f"🏛️ {activity_name} Management",
            description=(
                f"Manage appointments for **{activity_name}**:\n\n"
                f"**Current Status**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 **Booked Slots:** `{booked_count}/48`\n"
                f"⏰ **Available Slots:** `{available_count}/48`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**Available Actions**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"➕ Add new appointment\n"
                f"➖ Remove existing appointment\n"
                f"📋 View schedule\n"
                f"🗑️ Clear all appointments\n"
                f"🔄 Update names from API\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )

        view = MinisterActivityView(self.bot, self, activity_name)

        try:
            await self.original_interaction.edit_original_response(embed=embed, view=view)
        except:
            pass

    async def show_minister_activity_menu_edit(self, interaction: discord.Interaction, activity_name: str, success_message: str = None):
        """Edit the original message to show the activity menu - used after operations"""
        # Get current stats
        self.svs_cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_count = self.svs_cursor.fetchone()[0]
        available_count = 48 - booked_count

        description = f"Manage appointments for **{activity_name}**:\n\n"
        
        # Add success message if provided
        if success_message:
            description = f"✅ **{success_message}**\n\n{description}"
        
        description += (
            f"**Current Status**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 **Booked Slots:** `{booked_count}/48`\n"
            f"⏰ **Available Slots:** `{available_count}/48`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"**Available Actions**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"➕ Add new appointment\n"
            f"➖ Remove existing appointment\n"
            f"📋 View schedule\n"
            f"🗑️ Clear all appointments\n"
            f"🔄 Update names from API\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )

        embed = discord.Embed(
            title=f"🏛️ {activity_name} Management",
            description=description,
            color=discord.Color.blue()
        )

        view = MinisterActivityView(self.bot, self, activity_name)
        
        try:
            await interaction.edit_original_response(embed=embed, view=view)
        except:
            pass

    async def show_minister_activity_menu(self, interaction: discord.Interaction, activity_name: str):
        # Get current stats
        self.svs_cursor.execute("SELECT COUNT(*) FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_count = self.svs_cursor.fetchone()[0]
        available_count = 48 - booked_count # 48 total 30-minute slots in 24 hours

        embed = discord.Embed(
            title=f"🧑‍💼 {activity_name} Management",
            description=(
                f"Manage appointments for **{activity_name}**:\n\n"
                f"**Current Status**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 **Booked Slots:** `{booked_count}/48`\n"
                f"⏰ **Available Slots:** `{available_count}/48`\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"**Available Actions**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"➕ Add new appointment\n"
                f"➖ Remove existing appointment\n"
                f"📋 View schedule\n"
                f"🗑️ Clear all appointments\n"
                f"🔄 Update names from API\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )

        view = MinisterActivityView(self.bot, self, activity_name)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_add_minister_menu(self, interaction: discord.Interaction, activity_name: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        # Get available users (not already booked for this activity)
        self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_fids = {row[0] for row in self.svs_cursor.fetchall()}

        self.users_cursor.execute("SELECT fid, nickname FROM users")
        all_users = self.users_cursor.fetchall()
        
        available_users = [(fid, nickname) for fid, nickname in all_users if fid not in booked_fids]

        if not available_users:
            await interaction.response.send_message(
                f"❌ No available users to add to {activity_name}. All registered users are already booked.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"➕ Add to {activity_name}",
            description=f"Select a user to add to **{activity_name}**:",
            color=discord.Color.green()
        )

        view = UserSelectView(self.bot, self, activity_name, available_users)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_time_selection(self, interaction: discord.Interaction, activity_name: str, fid: str):
        # Get available time slots
        self.svs_cursor.execute("SELECT time FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_times = {row[0] for row in self.svs_cursor.fetchall()}

        available_times = []
        for hour in range(24):
            for minute in (0, 30):
                time_slot = f"{hour:02}:{minute:02}"
                if time_slot not in booked_times:
                    available_times.append(time_slot)

        if not available_times:
            await interaction.response.send_message(
                f"❌ No available time slots for {activity_name}.",
                ephemeral=True
            )
            return

        # Get user info
        self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
        user_data = self.users_cursor.fetchone()
        nickname = user_data[0] if user_data else f"ID: {fid}"

        embed = discord.Embed(
            title=f"⏰ Select Time for {nickname}",
            description=f"Choose an available time slot for **{nickname}** in {activity_name}:",
            color=discord.Color.blue()
        )

        view = TimeSelectView(self.bot, self, activity_name, fid, available_times)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_booked_schedule_above_buttons(self, interaction: discord.Interaction, activity_name: str):
        try:
            # Get booked times
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.response.send_message("❌ Minister Schedule module not found.", ephemeral=True)
                return

            if not booked_times:
                embed = discord.Embed(
                    title=f"Booked slots for {activity_name}",
                    description="No appointments scheduled",
                    color=discord.Color.blue()
                )
            else:
                time_list, _ = minister_schedule_cog.generate_time_list(booked_times)
                # Filter to only show booked slots
                booked_only = [slot for slot in time_list if " - " in slot and not slot.endswith(" - ")]
                
                if booked_only:
                    time_list_text = "\n".join(booked_only)
                    embed = discord.Embed(
                        title=f"Booked slots for {activity_name}",
                        description=time_list_text,
                        color=discord.Color.blue()
                    )
                else:
                    embed = discord.Embed(
                        title=f"Booked slots for {activity_name}",
                        description="No appointments scheduled",
                        color=discord.Color.blue()
                    )

            view = ListTypeView(self.bot, self, activity_name)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error showing booked schedule: {e}", ephemeral=True)

    async def complete_booking(self, interaction: discord.Interaction, activity_name: str, fid: str, selected_time: str):
        try:
            # Defer to prevent timeout
            if not interaction.response.is_done():
                await interaction.response.defer()

            # Check if the user is already booked for this activity type
            self.svs_cursor.execute("SELECT time FROM appointments WHERE fid=? AND appointment_type=?", (fid, activity_name))
            existing_booking = self.svs_cursor.fetchone()
            if existing_booking:
                self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
                user_data = self.users_cursor.fetchone()
                nickname = user_data[0] if user_data else f"ID: {fid}"
                
                error_msg = f"❌ {nickname} already has an appointment for {activity_name} at {existing_booking[0]}"
                await self.show_minister_activity_menu_edit(interaction, activity_name, error_msg)
                return

            # Check if the time slot is already taken
            self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=? AND time=?", (activity_name, selected_time))
            conflicting_booking = self.svs_cursor.fetchone()
            if conflicting_booking:
                booked_fid = conflicting_booking[0]
                self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (booked_fid,))
                booked_user = self.users_cursor.fetchone()
                booked_nickname = booked_user[0] if booked_user else "Unknown"
                
                error_msg = f"❌ The time {selected_time} for {activity_name} is already taken by {booked_nickname}"
                await self.show_minister_activity_menu_edit(interaction, activity_name, error_msg)
                return

            # Get user and alliance info
            self.users_cursor.execute("SELECT alliance, nickname FROM users WHERE fid=?", (fid,))
            user_data = self.users_cursor.fetchone()

            if not user_data:
                await interaction.response.send_message(
                    f"❌ User {fid} is not registered.",
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

            # Send log embed
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                embed = discord.Embed(
                    title=f"Player added to {activity_name}",
                    description=f"{nickname} ({fid}) from **{alliance_name}** at {selected_time}",
                    color=discord.Color.green()
                )
                embed.set_thumbnail(url=avatar_image)
                embed.set_author(name=f"Added by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)
                await minister_schedule_cog.send_embed_to_channel(embed)
                await self.update_channel_message(activity_name)

            success_msg = f"Successfully added {nickname} to {activity_name} at {selected_time}"
            await self.show_minister_activity_menu_edit(interaction, activity_name, success_msg)

        except Exception as e:
            try:
                error_msg = f"❌ Error booking appointment: {e}"
                await self.show_minister_activity_menu_edit(interaction, activity_name, error_msg)
            except:
                print(f"Failed to show error message for booking: {e}")

    async def show_remove_minister_menu(self, interaction: discord.Interaction, activity_name: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        # Get booked users for this activity
        self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=?", (activity_name,))
        booked_fids = [row[0] for row in self.svs_cursor.fetchall()]

        if not booked_fids:
            await interaction.response.send_message(
                f"❌ No users are currently booked for {activity_name}.",
                ephemeral=True
            )
            return

        # Get user info
        placeholders = ",".join("?" for _ in booked_fids)
        query = f"SELECT fid, nickname FROM users WHERE fid IN ({placeholders})"
        self.users_cursor.execute(query, booked_fids)
        booked_users = self.users_cursor.fetchall()

        embed = discord.Embed(
            title=f"➖ Remove from {activity_name}",
            description=f"Select a user to remove from **{activity_name}**:",
            color=discord.Color.red()
        )

        view = RemoveUserSelectView(self.bot, self, activity_name, booked_users)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def complete_removal(self, interaction: discord.Interaction, activity_name: str, fid: str):
        try:
            # Defer to prevent timeout
            if not interaction.response.is_done():
                await interaction.response.defer()

            # Check if the user is actually booked for this activity type
            self.svs_cursor.execute("SELECT * FROM appointments WHERE fid=? AND appointment_type=?", (fid, activity_name))
            booking = self.svs_cursor.fetchone()

            # Get user info
            self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
            user_data = self.users_cursor.fetchone()
            nickname = user_data[0] if user_data else f"ID: {fid}"

            if not booking:
                error_msg = f"❌ {nickname} is not on the minister list for {activity_name}"
                await self.show_minister_activity_menu_edit(interaction, activity_name, error_msg)
                return

            # Remove the appointment
            self.svs_cursor.execute("DELETE FROM appointments WHERE fid=? AND appointment_type=?", (fid, activity_name))
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

            # Send log embed
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                embed = discord.Embed(
                    title=f"Player removed from {activity_name}",
                    description=f"{nickname} ({fid})",
                    color=discord.Color.red()
                )
                embed.set_thumbnail(url=avatar_image)
                embed.set_author(name=f"Removed by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)
                await minister_schedule_cog.send_embed_to_channel(embed)
                await self.update_channel_message(activity_name)

            success_msg = f"Successfully removed {nickname} from {activity_name}"
            await self.show_minister_activity_menu_edit(interaction, activity_name, success_msg)

        except Exception as e:
            try:
                error_msg = f"❌ Error removing appointment: {e}"
                await self.show_minister_activity_menu_edit(interaction, activity_name, error_msg)
            except:
                print(f"Failed to show error message for removal: {e}")

    async def show_list_minister_menu(self, interaction: discord.Interaction, activity_name: str):
        embed = discord.Embed(
            title=f"📋 View {activity_name} Schedule",
            description="Choose how you'd like to view the schedule:",
            color=discord.Color.blue()
        )

        view = ListTypeView(self.bot, self, activity_name)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def show_full_schedule_above_buttons(self, interaction: discord.Interaction, activity_name: str, update: bool = False):
        try:
            if update:
                await interaction.response.defer()
            
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                if update:
                    await interaction.followup.send("❌ Minister Schedule module not found.", ephemeral=True)
                else:
                    await interaction.response.send_message("❌ Minister Schedule module not found.", ephemeral=True)
                return

            if update:
                async def update_progress(checked, total, waiting):
                    if checked % 1 == 0:
                        if waiting:
                            color = discord.Color.orange()
                            title = "⏳ Rate limit hit - waiting 60 seconds"
                            description = f"Checked {checked}/{total} minister appointees\n\n⚠️ **Please wait** - this will take some time!"
                        elif checked >= total:
                            color = discord.Color.green()
                            title = "✅ Update Complete!"
                            description = f"Successfully updated {total}/{total} minister appointees"
                        else:
                            color = discord.Color.blue()
                            title = "🔄 Updating names..."
                            description = f"Checked {checked}/{total} minister appointees"
                            
                        embed = discord.Embed(
                            title=title,
                            description=description,
                            color=color
                        )
                        try:
                            await interaction.edit_original_response(embed=embed)
                        except discord.NotFound:
                            print("Interaction expired during name update. This is normal for long operations.\nPlease try to re-open the Settings menu.")
                        except discord.HTTPException as e:
                            if "Unknown interaction" in str(e):
                                print("Interaction expired during name update. This is normal for long operations.\nPlease try to re-open the Settings menu.")
                            else:
                                raise

                time_list, _ = await minister_schedule_cog.update_time_list(booked_times, update_progress)
            else:
                time_list, _ = minister_schedule_cog.generate_time_list(booked_times)

            time_list_text = "\n".join(time_list)

            if time_list_text:
                embed = discord.Embed(
                    title=f"Schedule for {activity_name}",
                    description=time_list_text,
                    color=discord.Color.blue()
                )
            else:
                embed = discord.Embed(
                    title=f"Schedule for {activity_name}",
                    description="No appointments scheduled",
                    color=discord.Color.blue()
                )
                
            view = ListTypeView(self.bot, self, activity_name)
            
            if update:
                try:
                    await interaction.edit_original_response(embed=embed, view=view)
                except discord.NotFound:
                    print("Interaction expired before final update.")
            else:
                await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            error_msg = f"❌ Error showing schedule: {e}"
            if update:
                await interaction.followup.send(error_msg, ephemeral=True)
            else:
                await interaction.response.send_message(error_msg, ephemeral=True)

    async def show_available_schedule_above_buttons(self, interaction: discord.Interaction, activity_name: str):
        try:
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.response.send_message("❌ Minister Schedule module not found.", ephemeral=True)
                return

            available_slots = minister_schedule_cog.generate_available_time_list(booked_times)
            
            if available_slots:
                time_list = "\n".join(available_slots)
                embed = discord.Embed(
                    title=f"Available slots for {activity_name}",
                    description=time_list,
                    color=discord.Color.green()
                )
            else:
                embed = discord.Embed(
                    title=f"Available slots for {activity_name}",
                    description="All appointment slots are filled",
                    color=discord.Color.red()
                )

            view = ListTypeView(self.bot, self, activity_name)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error showing available schedule: {e}", ephemeral=True)

    async def show_full_schedule(self, interaction: discord.Interaction, activity_name: str, update: bool = False):
        try:
            await interaction.response.defer()

            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.followup.send(
                    "❌ Minister Schedule module not found.",
                    ephemeral=True
                )
                return

            if update:
                async def update_progress(checked, total, waiting):
                    if checked % 1 == 0:
                        color = discord.Color.orange() if waiting else discord.Color.green()
                        title = "Waiting 60 seconds before continuing" if waiting else "Updating names"
                        embed = discord.Embed(
                            title=title,
                            description=f"Checked {checked}/{total} minister appointees",
                            color=color
                        )
                        try:
                            await interaction.edit_original_response(embed=embed)
                        except discord.NotFound:
                            print("Interaction expired before progress update.")

                time_list, _ = await minister_schedule_cog.update_time_list(booked_times, update_progress)
            else:
                time_list, _ = minister_schedule_cog.generate_time_list(booked_times)

            time_list_text = "\n".join(time_list)

            if time_list_text:
                embed = discord.Embed(
                    title=f"Schedule for {activity_name}",
                    description=time_list_text,
                    color=discord.Color.blue()
                )
                
                view = discord.ui.View()
                back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️")
                
                async def back_callback(button_interaction):
                    await self.show_minister_activity_menu_from_schedule(button_interaction, activity_name)
                
                back_button.callback = back_callback
                view.add_item(back_button)
                
                try:
                    await interaction.edit_original_response(embed=embed, view=view)
                except discord.NotFound:
                    print("Interaction expired before final update.")

        except Exception as e:
            await interaction.followup.send(f"❌ Error showing schedule: {e}", ephemeral=True)

    async def show_available_schedule(self, interaction: discord.Interaction, activity_name: str):
        try:
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.response.send_message(
                    "❌ Minister Schedule module not found.",
                    ephemeral=True
                )
                return

            available_slots = minister_schedule_cog.generate_available_time_list(booked_times)
            
            if available_slots:
                time_list = "\n".join(available_slots)
                embed = discord.Embed(
                    title=f"Available slots for {activity_name}",
                    description=time_list,
                    color=discord.Color.green()
                )
            else:
                embed = discord.Embed(
                    title=f"Available slots for {activity_name}",
                    description="All appointment slots are filled",
                    color=discord.Color.red()
                )

            view = discord.ui.View()
            back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji="◀️")
            
            async def back_callback(button_interaction):
                await self.show_minister_activity_menu_from_schedule(button_interaction, activity_name)
            
            back_button.callback = back_callback
            view.add_item(back_button)

            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error showing available schedule: {e}",
                ephemeral=True
            )

    async def show_clear_confirmation(self, interaction: discord.Interaction, activity_name: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"⚠️ Confirm clearing {activity_name} list",
            description=(
                f"Are you sure you want to remove all minister appointment slots for: **{activity_name}**?\n\n"
                f"🚨 **This action cannot be undone and all names will be removed** 🚨"
            ),
            color=discord.Color.orange()
        )

        view = ClearConfirmationView(self.bot, self, activity_name)
        
        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def execute_clear_all(self, interaction: discord.Interaction, activity_name: str):
        try:
            # Get current schedule before clearing
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                time_list, _ = minister_schedule_cog.generate_time_list(booked_times)
                previous_schedule = "\n".join(time_list)

                # Clear all appointments
                self.svs_cursor.execute("DELETE FROM appointments WHERE appointment_type=?", (activity_name,))
                self.svs_conn.commit()

                # Send log embed
                embed = discord.Embed(
                    title=f"Cleared {activity_name} list",
                    description=f"All appointments for {activity_name} have been successfully removed.",
                    color=discord.Color.red()
                )
                embed.set_author(name=f"Cleared by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)
                await minister_schedule_cog.send_embed_to_channel(embed)
                await self.update_channel_message(activity_name)

            # Return to activity menu with success message
            success_msg = f"Successfully cleared all appointments for {activity_name}"
            await self.show_minister_activity_menu_edit(interaction, activity_name, success_msg)

        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error clearing appointments: {e}",
                ephemeral=True
            )

    async def update_minister_names(self, interaction: discord.Interaction, activity_name: str):
        try:
            # Send initial ephemeral response immediately
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="🔄 Starting name update...",
                    description=f"Initializing update process for **{activity_name}**",
                    color=discord.Color.blue()
                ),
                ephemeral=True
            )

            # Get booked times
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            if not booked_times:
                await interaction.edit_original_response(
                    embed=discord.Embed(
                        title="❌ No appointments found",
                        description=f"No appointments found for **{activity_name}**",
                        color=discord.Color.red()
                    )
                )
                return

            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.edit_original_response(
                    embed=discord.Embed(
                        title="❌ Error",
                        description="Minister Schedule module not found.",
                        color=discord.Color.red()
                    )
                )
                return

            async def update_progress(checked, total, waiting):
                if checked % 1 == 0:
                    if waiting:
                        color = discord.Color.orange()
                        title = "⏳ Rate limit hit - waiting 60 seconds"
                        description = f"Checked {checked}/{total} minister appointees\n\n⚠️ **Please wait** - This is normal when updating many names\n\n✅ **You can use other bot functions while this runs**"
                    elif checked >= total:
                        color = discord.Color.green()
                        title = "✅ Update Complete!"
                        description = f"Successfully updated {total}/{total} minister appointees for **{activity_name}**"
                    else:
                        color = discord.Color.blue()
                        title = "🔄 Updating names..."
                        description = f"Checked {checked}/{total} minister appointees\n\n✅ **You can use other bot functions while this runs**"
                        
                    embed = discord.Embed(
                        title=title,
                        description=description,
                        color=color
                    )
                    try:
                        await interaction.edit_original_response(embed=embed)
                    except discord.NotFound:
                        print("Ephemeral interaction expired during name update. This is normal for long operations.")
                    except discord.HTTPException as e:
                        if "Unknown interaction" in str(e):
                            print("Ephemeral interaction expired during name update. This is normal for long operations.")
                        else:
                            print(f"HTTP error during progress update: {e}")

            # Update names via API
            time_list, _ = await minister_schedule_cog.update_time_list(booked_times, update_progress)

            # Final success message
            embed = discord.Embed(
                title="✅ Names Updated Successfully!",
                description=f"All minister names have been updated for **{activity_name}**\n\n🔄 Channel messages have been refreshed with updated names",
                color=discord.Color.green()
            )
            try:
                await interaction.edit_original_response(embed=embed)
            except (discord.NotFound, discord.HTTPException):
                # If interaction expired, that's normal for long operations
                print(f"Update completed for {activity_name} but ephemeral interaction expired. This is normal.")

            # Update the channel message with fresh data
            await self.update_channel_message(activity_name)

        except Exception as e:
            try:
                await interaction.edit_original_response(
                    embed=discord.Embed(
                        title="❌ Error updating names",
                        description=f"An error occurred: {e}",
                        color=discord.Color.red()
                    )
                )
            except:
                print(f"Error updating names for {activity_name}: {e}")

    async def update_channel_message(self, activity_name: str):
        """Update the channel message with current available slots"""
        try:
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                return

            # Get current booked times
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (activity_name,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
            
            # Generate available time list
            time_list = minister_schedule_cog.generate_available_time_list(booked_times)

            context = f"{activity_name}"
            channel_context = f"{activity_name} channel"

            message_content = f"**{activity_name}** available slots:\n" + "\n".join(time_list)

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

async def setup(bot):
    await bot.add_cog(MinisterMenu(bot))