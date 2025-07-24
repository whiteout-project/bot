import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from datetime import datetime
import os
import re
from io import BytesIO

try: # Matplotlib imports (optional - fallback to text if not available)
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import arabic_reshaper
    from bidi.algorithm import get_display
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Matplotlib not available - using text reports only")

FC_LEVEL_MAPPING = {
    31: "30-1", 32: "30-2", 33: "30-3", 34: "30-4",
    35: "FC 1", 36: "FC 1-1", 37: "FC 1-2", 38: "FC 1-3", 39: "FC 1-4",
    40: "FC 2", 41: "FC 2-1", 42: "FC 2-2", 43: "FC 2-3", 44: "FC 2-4",
    45: "FC 3", 46: "FC 3-1", 47: "FC 3-2", 48: "FC 3-3", 49: "FC 3-4",
    50: "FC 4", 51: "FC 4-1", 52: "FC 4-2", 53: "FC 4-3", 54: "FC 4-4",
    55: "FC 5", 56: "FC 5-1", 57: "FC 5-2", 58: "FC 5-3", 59: "FC 5-4",
    60: "FC 6", 61: "FC 6-1", 62: "FC 6-2", 63: "FC 6-3", 64: "FC 6-4",
    65: "FC 7", 66: "FC 7-1", 67: "FC 7-2", 68: "FC 7-3", 69: "FC 7-4",
    70: "FC 8", 71: "FC 8-1", 72: "FC 8-2", 73: "FC 8-3", 74: "FC 8-4",
    75: "FC 9", 76: "FC 9-1", 77: "FC 9-2", 78: "FC 9-3", 79: "FC 9-4",
    80: "FC 10", 81: "FC 10-1", 82: "FC 10-2", 83: "FC 10-3", 84: "FC 10-4"
}

def get_best_unicode_font():
    """Get the best available font for Unicode/Arabic text"""
    if not MATPLOTLIB_AVAILABLE:
        return None
        
    font_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "fonts")
    roboto_arabic_path = os.path.join(font_dir, "RobotoArabic-Regular.ttf")
    roboto_path = os.path.join(font_dir, "Roboto-Regular.ttf")
    
    if os.path.exists(roboto_arabic_path):
        return fm.FontProperties(fname=roboto_arabic_path)
    if os.path.exists(roboto_path):
        return fm.FontProperties(fname=roboto_path)
    return fm.FontProperties(family='DejaVu Sans')

def parse_points(points_str):
    try:
        points_str = points_str.strip().upper()
        points_str = points_str.replace(',', '')
        if points_str.endswith('M'):
            number = float(points_str[:-1])
            return int(number * 1_000_000)
        elif points_str.endswith('K'):
            number = float(points_str[:-1])
            return int(number * 1_000)
        else:
            return int(float(points_str))
    except (ValueError, TypeError):
        raise ValueError("Invalid points format")

class AttendanceSettingsView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=1800)
        self.cog = cog

    @discord.ui.button(
        label="Report Type",
        emoji="📊",
        style=discord.ButtonStyle.primary,
        custom_id="report_type"
    )
    async def report_type_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle between text and matplotlib reports"""
        try:
            # Get current setting
            current_setting = await self.cog.get_user_report_preference(interaction.user.id)
            
            # Create selection view
            select_view = ReportTypeSelectView(self.cog, current_setting)
            
            embed = discord.Embed(
                title="📊 Report Type Settings",
                description=(
                    f"**Current Setting:** {current_setting.title()}\n\n"
                    "**Available Options:**\n"
                    "• **Text** - Traditional text-based reports (faster, always available)\n"
                    "• **Matplotlib** - Visual table reports (requires matplotlib)\n\n"
                    f"**Matplotlib Status:** {'✅ Available' if MATPLOTLIB_AVAILABLE else '❌ Not Available'}\n\n"
                    "Select your preferred report type below:"
                ),
                color=discord.Color.blue()
            )
            
            await interaction.response.edit_message(embed=embed, view=select_view)
            
        except Exception as e:
            print(f"Error in report type settings: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "An error occurred while loading settings."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_main"
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)

class ReportTypeSelectView(discord.ui.View):
    def __init__(self, cog, current_setting):
        super().__init__(timeout=1800)
        self.cog = cog
        self.current_setting = current_setting

    @discord.ui.button(
        label="Text Reports",
        emoji="📝",
        style=discord.ButtonStyle.secondary,
        custom_id="text_reports"
    )
    async def text_reports_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.set_report_preference(interaction, "text")

    @discord.ui.button(
        label="Matplotlib Reports",
        emoji="📊",
        style=discord.ButtonStyle.primary,
        custom_id="matplotlib_reports"
    )
    async def matplotlib_reports_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not MATPLOTLIB_AVAILABLE:
            await interaction.response.send_message(
                "❌ Matplotlib is not available on this system.",
                ephemeral=True
            )
            return
        await self.set_report_preference(interaction, "matplotlib")

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_settings"
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings_view = AttendanceSettingsView(self.cog)
        embed = discord.Embed(
            title="⚙️ Attendance Settings",
            description=(
                "Configure your attendance system preferences:\n\n"
                "**Available Settings**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📊 **Report Type**\n"
                "└ Choose between text or visual reports\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=settings_view)

    async def set_report_preference(self, interaction: discord.Interaction, preference: str):
        """Set user's report preference"""
        try:
            await self.cog.set_user_report_preference(interaction.user.id, preference)
            
            embed = discord.Embed(
                title="✅ Settings Updated",
                description=f"Report type has been set to: **{preference.title()}**",
                color=discord.Color.green()
            )
            
            back_view = self.cog._create_back_view(
                lambda i: self.cog.show_attendance_menu(i)
            )
            
            await interaction.response.edit_message(embed=embed, view=back_view)
            
        except Exception as e:
            print(f"Error setting report preference: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "Failed to update settings."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

class AttendanceView(discord.ui.View):
    def __init__(self, cog, user_id, guild_id):
        super().__init__(timeout=1800)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.admin_result = None
        self.alliances = None
    
    async def initialize_permissions_and_alliances(self):
        """Initialize permissions and alliances at the view level."""
        self.admin_result = await self.cog._check_admin_permissions(self.user_id)
        
        if self.admin_result:
            self.alliances, _, _ = await self.cog.get_admin_alliances(self.user_id, self.guild_id)

    async def _handle_permission_check(self, interaction):
        """Consolidated permission checking using cached results."""
        if not self.admin_result:
            error_embed = self.cog._create_error_embed(
                "❌ Access Denied", 
                "You do not have permission to use this command."
            )
            back_view = self.cog._create_back_view(lambda i: self.cog.show_attendance_menu(i))
            await interaction.response.edit_message(embed=error_embed, view=back_view)
            return None
            
        if not self.alliances:
            error_embed = self.cog._create_error_embed(
                "❌ No Alliances Found",
                "No alliances found for your permissions."
            )
            back_view = self.cog._create_back_view(lambda i: self.cog.show_attendance_menu(i))
            await interaction.response.edit_message(embed=error_embed, view=back_view)
            return None
            
        return self.alliances, self.admin_result[0]

    def _get_alliances_with_counts(self, alliances):
        """Get alliance member counts with optimized single query"""
        alliance_ids = [aid for aid, _ in alliances]
        alliances_with_counts = []
        
        # Validate that all alliance IDs are integers to prevent SQL injection
        if alliance_ids and not all(isinstance(aid, int) for aid in alliance_ids):
            raise ValueError("Invalid alliance IDs detected - all IDs must be integers")
        
        if alliance_ids:
            with sqlite3.connect('db/users.sqlite') as db:
                cursor = db.cursor()
                placeholders = ','.join('?' * len(alliance_ids))
                cursor.execute(f"""
                    SELECT alliance, COUNT(*) 
                    FROM users 
                    WHERE alliance IN ({placeholders}) 
                    GROUP BY alliance
                """, [str(aid) for aid in alliance_ids]) # Convert to strings to match database
                counts = dict(cursor.fetchall())
            
            alliances_with_counts = [
                (aid, name, counts.get(str(aid), 0)) # Use string key for lookup
                for aid, name in alliances
            ]
        
        return alliances_with_counts

    @discord.ui.button(
        label="Mark Attendance",
        emoji="📋",
        style=discord.ButtonStyle.primary,
        custom_id="mark_attendance"
    )
    async def mark_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            result = await self._handle_permission_check(interaction)
            if not result:
                return
                
            alliances, is_initial = result
            
            # Create alliance selection embed
            select_embed = discord.Embed(
                title="📋 Attendance - Alliance Selection",
                description=(
                    "Please select an alliance to mark attendance:\n\n"
                    "**Permission Details**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 **Access Level:** `{'Global Admin' if is_initial == 1 else 'Server Admin'}`\n"
                    f"🔍 **Access Type:** `{'All Alliances' if is_initial == 1 else 'Server + Special Access'}`\n"
                    f"📊 **Available Alliances:** `{len(alliances)}`\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )

            # Get alliance member counts with optimized query
            alliances_with_counts = self._get_alliances_with_counts(alliances)
            view = AllianceSelectView(alliances_with_counts, self.cog, is_marking=True)
            
            await interaction.response.edit_message(embed=select_embed, view=view)

        except Exception as e:
            print(f"Error in mark_attendance_button: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "An error occurred while processing your request."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    @discord.ui.button(
        label="View Attendance",
        emoji="👀",
        style=discord.ButtonStyle.secondary,
        custom_id="view_attendance"
    )
    async def view_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            result = await self._handle_permission_check(interaction)
            if not result:
                return
                
            alliances, _ = result

            # Get alliance member counts with optimized query
            alliances_with_counts = self._get_alliances_with_counts(alliances)
            view = AllianceSelectView(alliances_with_counts, self.cog, is_marking=False)
            
            select_embed = discord.Embed(
                title="👀 View Attendance - Alliance Selection",
                description="Please select an alliance to view attendance records:",
                color=discord.Color.green()
            )
            
            await interaction.response.edit_message(embed=select_embed, view=view)

        except Exception as e:
            print(f"Error in view_attendance_button: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "An error occurred while processing your request."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    @discord.ui.button(
        label="Settings",
        emoji="⚙️",
        style=discord.ButtonStyle.secondary,
        custom_id="attendance_settings"
    )
    async def settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user has admin permissions
            admin_result = await self.cog._check_admin_permissions(interaction.user.id)
            
            if not admin_result:
                error_embed = self.cog._create_error_embed(
                    "❌ Access Denied", 
                    "You do not have permission to access settings."
                )
                await interaction.response.edit_message(embed=error_embed, view=None)
                return

            settings_view = AttendanceSettingsView(self.cog)
            
            embed = discord.Embed(
                title="⚙️ Attendance Settings",
                description=(
                    "Configure your attendance system preferences:\n\n"
                    "**Available Settings**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "📊 **Report Type**\n"
                    "└ Choose between text or visual reports\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )
            
            await interaction.response.edit_message(embed=embed, view=settings_view)

        except Exception as e:
            print(f"Error in settings button: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "An error occurred while loading settings."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_other_features"
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            other_features_cog = self.cog.bot.get_cog("OtherFeatures")
            if other_features_cog:
                await other_features_cog.show_other_features_menu(interaction)
        except Exception as e:
            print(f"Error returning to other features: {e}")
            error_embed = self.cog._create_error_embed(
                "❌ Error",
                "An error occurred while returning to other features."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

class SessionNameModal(discord.ui.Modal, title="Attendance Session"):
    def __init__(self, alliance_id, cog):
        super().__init__()
        self.alliance_id = alliance_id
        self.cog = cog
        
        self.session_name = discord.ui.TextInput(
            label="Session Name",
            placeholder="Enter a name for this attendance session",
            required=True,
            max_length=50
        )
        self.add_item(self.session_name)

    async def on_submit(self, interaction: discord.Interaction):
        session_name = self.session_name.value.strip()
        if not session_name:
            error_embed = discord.Embed(
                title="❌ Error",
                description="Session name cannot be empty.",
                color=discord.Color.red()
            )
            await interaction.response.edit_message(embed=error_embed, view=None)
            return
            
        await self.cog.show_attendance_marking(
            interaction, 
            self.alliance_id,
            session_name
        )

class AllianceSelectView(discord.ui.View):
    def __init__(self, alliances_with_counts, cog, page=0, is_marking=False):
        super().__init__(timeout=1800)
        self.alliances = alliances_with_counts
        self.cog = cog
        self.page = page
        self.max_page = (len(alliances_with_counts) - 1) // 25 if alliances_with_counts else 0
        self.current_select = None
        self.is_marking = is_marking
        self.update_select_menu()

    def update_select_menu(self):
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.alliances))
        current_alliances = self.alliances[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"🏰 Select an alliance... (Page {self.page + 1}/{self.max_page + 1})",
            options=[
                discord.SelectOption(
                    label=f"{name[:50]}",
                    value=str(alliance_id),
                    description=f"ID: {alliance_id} | Members: {count}",
                    emoji="🏰"
                ) for alliance_id, name, count in current_alliances
            ]
        )
        
        async def select_callback(interaction: discord.Interaction):
            self.current_select = select
            alliance_id = int(select.values[0])
            
            if self.is_marking:
                # For marking: ask for session name
                modal = SessionNameModal(alliance_id, self.cog)
                await interaction.response.send_modal(modal)
            else:
                # For viewing: show session selection
                await self.cog.show_session_selection(interaction, alliance_id)

        select.callback = select_callback
        self.add_item(select)
        self.current_select = select

        # Update navigation button states
        prev_button = next((item for item in self.children if hasattr(item, 'label') and item.label == "◀️"), None)
        next_button = next((item for item in self.children if hasattr(item, 'label') and item.label == "▶️"), None)
        
        if prev_button:
            prev_button.disabled = self.page == 0
        if next_button:
            next_button.disabled = self.page == self.max_page

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary, row=0)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary, row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        row=0
    )
    async def back_to_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)

class PlayerSelectView(discord.ui.View):
    def __init__(self, players, alliance_name, session_name, cog, page=0):
        super().__init__(timeout=1800)
        self.players = players
        self.alliance_name = alliance_name
        self.session_name = session_name
        self.cog = cog
        self.selected_players = {}
        self.page = page
        self.max_page = (len(players) - 1) // 25 if players else 0
        self.current_select = None
        self.update_select_menu()

    def update_select_menu(self):
        # Remove existing select menu
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.players))
        current_players = self.players[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"👥 Select a player to mark attendance... (Page {self.page + 1}/{self.max_page + 1})",
            options=[
                discord.SelectOption(
                    label=f"{nickname[:50]}",
                    value=str(fid),
                    description=f"FID: {fid} | FC: {FC_LEVEL_MAPPING.get(furnace_lv, str(furnace_lv))}",
                    emoji="👤"
                ) for fid, nickname, furnace_lv in current_players
            ]
        )
        
        async def select_callback(interaction: discord.Interaction):
            self.current_select = select
            selected_fid = int(select.values[0])
            # Find the selected player
            selected_player = next((p for p in self.players if p[0] == selected_fid), None)
            if selected_player:
                await self.show_player_attendance_options(interaction, selected_player)
        
        select.callback = select_callback
        self.add_item(select)
        self.current_select = select

        # Update navigation button states
        prev_button = next((item for item in self.children if hasattr(item, 'label') and item.label == "◀️"), None)
        next_button = next((item for item in self.children if hasattr(item, 'label') and item.label == "▶️"), None)
        
        if prev_button:
            prev_button.disabled = self.page == 0
        if next_button:
            next_button.disabled = self.page == self.max_page

    async def show_player_attendance_options(self, interaction: discord.Interaction, player):
        fid, nickname, furnace_lv = player
        
        # Create new view with attendance options for this player
        attendance_view = PlayerAttendanceView(player, self)
        
        embed = discord.Embed(
            title=f"📋 Mark Attendance - {nickname}",
            description=(
                f"**Player:** {nickname}\n"
                f"**FID:** {fid}\n"
                f"**FC:** {FC_LEVEL_MAPPING.get(furnace_lv, str(furnace_lv))}\n"
                f"**Session:** {self.session_name}\n\n"
                "Please select the attendance status for this player:"
            ),
            color=discord.Color.blue()
        )
        
        await interaction.response.edit_message(embed=embed, view=attendance_view)

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary, row=1)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await self.update_main_embed(interaction)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary, row=1)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await self.update_main_embed(interaction)

    @discord.ui.button(label="📊 View Summary", style=discord.ButtonStyle.primary, row=1)
    async def view_summary_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_players:
            # Show error in the same message
            error_embed = discord.Embed(
                title="❌ No Data",
                description="No attendance has been marked yet.",
                color=discord.Color.orange()
            )
            back_view = discord.ui.View()
            back_button = discord.ui.Button(
                label="⬅️ Close",
                style=discord.ButtonStyle.secondary
            )
            back_button.callback = lambda i: self.update_main_embed(i)
            back_view.add_item(back_button)
            
            await interaction.response.edit_message(embed=error_embed, view=back_view)
            return
        
        await self.show_summary(interaction)

    @discord.ui.button(label="✅ Finish Attendance", style=discord.ButtonStyle.success, row=1)
    async def finish_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_players:
            error_embed = discord.Embed(
                title="❌ No Data",
                description="No attendance has been marked yet.",
                color=discord.Color.orange()
            )
            back_view = discord.ui.View()
            back_button = discord.ui.Button(
                label="⬅️ Close",
                style=discord.ButtonStyle.secondary
            )
            back_button.callback = lambda i: self.update_main_embed(i)
            back_view.add_item(back_button)
            
            await interaction.response.edit_message(embed=error_embed, view=back_view)
            return
        
        # Use defer then call existing method with defer flag
        await interaction.response.defer()
        await self.cog.process_attendance_results(interaction, self.selected_players, self.alliance_name, self.session_name, use_defer=True)

    @discord.ui.button(label="⬅️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_to_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)

    async def update_main_embed(self, interaction: discord.Interaction):
        marked_count = len(self.selected_players)
        total_count = len(self.players)
        
        embed = discord.Embed(
            title=f"📋 Marking Attendance - {self.alliance_name}",
            description=(
                f"**Session:** {self.session_name}\n"
                f"**Progress:** {marked_count}/{total_count} players marked\n"
                f"**Current Page:** {self.page + 1}/{self.max_page + 1}\n\n"
                "Select a player from the dropdown to mark their attendance.\n"
                "Use the buttons below to navigate, view summary, or finish."
            ),
            color=discord.Color.blue()
        )
        
        if marked_count > 0:
            present = sum(1 for p in self.selected_players.values() if p['attendance_type'] == 'present')
            absent = sum(1 for p in self.selected_players.values() if p['attendance_type'] == 'absent')
            not_signed = sum(1 for p in self.selected_players.values() if p['attendance_type'] == 'not_signed')
            
            embed.add_field(
                name="📊 Current Stats",
                value=f"Present: {present}\nAbsent: {absent}\nNot Signed: {not_signed}",
                inline=True
            )
        
        await interaction.response.edit_message(embed=embed, view=self)

    async def show_summary(self, interaction: discord.Interaction):
        # Check user's report preference
        report_type = await self.cog.get_user_report_preference(interaction.user.id)
        
        # If matplotlib is not available, force text mode
        if report_type == "matplotlib" and not MATPLOTLIB_AVAILABLE:
            report_type = "text"
            
        if report_type == "matplotlib":
            await self.show_matplotlib_summary(interaction)
        else:
            await self.show_text_summary(interaction)

    async def show_matplotlib_summary(self, interaction: discord.Interaction):
        """Show summary using matplotlib"""
        try:
            if not self.selected_players:
                await self.show_text_summary(interaction)
                return
                
            font_prop = get_best_unicode_font()
            
            # Sort by points (highest to lowest)
            sorted_players = sorted(
                self.selected_players.items(),
                key=lambda x: x[1]['points'],
                reverse=True
            )
            
            # Prepare data for matplotlib table
            headers = ["Player", "Status", "Points"]
            table_data = []
            
            def fix_arabic(text):
                if text and re.search(r'[\u0600-\u06FF]', text):
                    try:
                        reshaped = arabic_reshaper.reshape(text)
                        return get_display(reshaped)
                    except Exception:
                        return text
                return text
                
            def wrap_text(text, width=25):
                if not text:
                    return ""
                lines = []
                for part in str(text).split('\n'):
                    while len(part) > width:
                        lines.append(part[:width])
                        part = part[width:]
                    lines.append(part)
                return '\n'.join(lines)

            for fid, data in sorted_players:
                status_display = {
                    "present": "Present",
                    "absent": "Absent",
                    "not_signed": "Not Signed"
                }.get(data['attendance_type'], data['attendance_type'])
                
                table_data.append([
                    wrap_text(fix_arabic(data['nickname'])),
                    wrap_text(fix_arabic(status_display)),
                    wrap_text(f"{data['points']:,}" if data['points'] > 0 else "0")
                ])

            fig, ax = plt.subplots(figsize=(10, min(1 + len(table_data) * 0.4, 15)))
            ax.axis('off')
            
            table = ax.table(
                cellText=table_data,
                colLabels=headers,
                cellLoc='left',
                loc='center',
                colColours=['#28a745']*len(headers)  # Green color for summary
            )
            table.auto_set_font_size(False)
            table.set_fontsize(11)
            table.scale(1, 1.3)

            # Set font for all cells
            for key, cell in table.get_celld().items():
                if hasattr(cell, 'set_fontproperties'):
                    cell.set_fontproperties(font_prop)
                elif hasattr(cell, 'set_font_properties'):
                    cell.set_font_properties(font_prop)

            plt.title(f'Attendance Summary - {self.alliance_name} | Session: {self.session_name}', 
                    fontsize=14, color='#28a745', pad=15, fontproperties=font_prop)

            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            plt.close(fig)
            img_buffer.seek(0)

            file = discord.File(img_buffer, filename="attendance_summary.png")

            embed = discord.Embed(
                title=f"📊 Attendance Summary - {self.alliance_name}",
                description=f"**Session:** {self.session_name}\n**Total Marked:** {len(self.selected_players)} players",
                color=discord.Color.green()
            )
            embed.set_image(url="attachment://attendance_summary.png")
            
            back_view = self.cog._create_back_view(lambda i: self.update_main_embed(i))
            await interaction.response.edit_message(embed=embed, view=back_view, attachments=[file])

        except Exception as e:
            print(f"Matplotlib summary error: {e}")
            # Fallback to text summary
            await self.show_text_summary(interaction)

    async def show_text_summary(self, interaction: discord.Interaction):
        """Show summary using text format"""
        report_sections = []
        report_sections.append("📊 **SUMMARY**")
        report_sections.append(f"**Session:** {self.session_name}")
        report_sections.append(f"**Alliance:** {self.alliance_name}")
        report_sections.append("")
        report_sections.append("👥 **PLAYER DETAILS**")
        report_sections.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        # Sort by points (highest to lowest)
        sorted_players = sorted(
            self.selected_players.items(),
            key=lambda x: x[1]['points'],
            reverse=True
        )
        
        for fid, data in sorted_players:
            status_emoji = self.cog._get_status_emoji(data['attendance_type'])
            points_display = f"{data['points']:,}" if data['points'] > 0 else "0"
            
            player_line = f"{status_emoji} **{data['nickname']}**"
            if data['points'] > 0:
                player_line += f" | **{points_display}** points"
            
            report_sections.append(player_line)
        
        embed = discord.Embed(
            title=f"📊 Attendance Summary - {self.alliance_name}",
            description="\n".join(report_sections),
            color=discord.Color.green()
        )
        
        back_view = self.cog._create_back_view(lambda i: self.update_main_embed(i))
        await interaction.response.edit_message(embed=embed, view=back_view)

    def add_player_attendance(self, fid, nickname, attendance_type, points, last_event_attendance):
        self.selected_players[fid] = {
            'nickname': nickname,
            'attendance_type': attendance_type,
            'points': points,
            'last_event_attendance': last_event_attendance
        }

class AttendanceModal(discord.ui.Modal):
    def __init__(self, fid, nickname, attendance_type, parent_view, last_attendance):
        super().__init__(title=f"Attendance Details - {nickname}")
        self.fid = fid
        self.nickname = nickname
        self.attendance_type = attendance_type
        self.parent_view = parent_view
        self.last_attendance = last_attendance
        
        # Only show points input for "present" attendance
        if attendance_type == "present":
            self.points_input = discord.ui.TextInput(
                label="Points",
                placeholder="Enter points (e.g., 100, 4.3K, 2.5M), default is 0",
                required=False, # Not mandatory anymore, default to 0
                max_length=15
            )
            self.add_item(self.points_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Handle points based on attendance type
            points = 0
            if self.attendance_type == "present" and hasattr(self, 'points_input'):
                points_value = self.points_input.value.strip()
                if points_value:
                    points = parse_points(points_value)

            # Single transaction for all database operations
            with sqlite3.connect('db/attendance.sqlite', timeout=10.0) as attendance_db, \
                sqlite3.connect('db/users.sqlite') as users_db, \
                sqlite3.connect('db/alliance.sqlite') as alliance_db:
                
                # Get user alliance
                user_cursor = users_db.cursor()
                user_cursor.execute("SELECT alliance FROM users WHERE fid = ?", (self.fid,))
                user_result = user_cursor.fetchone()
                if not user_result:
                    raise ValueError(f"User with FID {self.fid} not found in database")
                alliance_id = user_result[0]
                
                # Get alliance name
                alliance_cursor = alliance_db.cursor()
                alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = alliance_cursor.fetchone()
                alliance_name = alliance_result[0] if alliance_result else "Unknown Alliance"

                # We use INSERT OR REPLACE to handle existing records automatically
                attendance_cursor = attendance_db.cursor()
                attendance_cursor.execute("""
                    INSERT OR REPLACE INTO attendance_records
                    (fid, nickname, alliance_id, alliance_name, attendance_status, points,
                    last_event_attendance, marked_date, marked_by, marked_by_username, session_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (self.fid, self.nickname, alliance_id, alliance_name, self.attendance_type,
                    points, self.last_attendance, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    interaction.user.id, interaction.user.name, self.parent_view.session_name))
                
                attendance_db.commit()
            
            self.parent_view.add_player_attendance(self.fid, self.nickname, self.attendance_type, points, self.last_attendance)
            await self.update_main_embed_with_confirmation(interaction)
            
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"Error: {str(e)[:100]}",
                color=discord.Color.red()
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    async def update_main_embed_with_confirmation(self, interaction: discord.Interaction):
        """Update main embed with confirmation message instead of showing success page"""
        marked_count = len(self.parent_view.selected_players)
        total_count = len(self.parent_view.players)
        
        # Create status display
        status_display = {
            "present": "Present",
            "absent": "Absent",
            "not_signed": "Not Signed"
        }.get(self.attendance_type, self.attendance_type)
        
        # Get the points for display
        player_data = self.parent_view.selected_players[self.fid]
        points = player_data['points']
        
        embed = discord.Embed(
            title=f"📋 Marking Attendance - {self.parent_view.alliance_name}",
            description=(
                f"**Session:** {self.parent_view.session_name}\n"
                f"**Progress:** {marked_count}/{total_count} players marked\n"
                f"**Current Page:** {self.parent_view.page + 1}/{self.parent_view.max_page + 1}\n\n"
                f"✅ **{self.nickname}** marked as **{status_display}** with **{points:,} points**\n\n"
                "Select a player from the dropdown to mark their attendance.\n"
                "Use the buttons below to navigate, view summary, or finish."
            ),
            color=discord.Color.green()
        )
        
        if marked_count > 0:
            present = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'present')
            absent = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'absent')
            not_signed = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'not_signed')
            
            embed.add_field(
                name="📊 Current Stats",
                value=f"Present: {present}\nAbsent: {absent}\nNot Signed: {not_signed}",
                inline=True
            )
        
        # Defer first, then edit
        await interaction.response.defer()
        await interaction.edit_original_response(embed=embed, view=self.parent_view)

class PlayerAttendanceView(discord.ui.View):
    def __init__(self, player, parent_view):
        super().__init__(timeout=1800)
        self.player = player
        self.parent_view = parent_view
        self.fid, self.nickname, self.furnace_lv = player

    async def fetch_last_attendance(self, fid):
        def query():
            with sqlite3.connect('db/attendance.sqlite') as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT attendance_status, marked_date FROM attendance_records "
                    "WHERE fid = ? "
                    "ORDER BY marked_date DESC LIMIT 1",
                    (fid,)
                )
                result = cursor.fetchone()
                return f"{result[0]} ({result[1][:10]})" if result else "N/A"
        try:
            return await self.parent_view.cog.bot.loop.run_in_executor(None, query)
        except:
            return "Error"

    @discord.ui.button(label="Present", style=discord.ButtonStyle.success, custom_id="present")
    async def present_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._mark_attendance(interaction, "present")

    @discord.ui.button(label="Absent", style=discord.ButtonStyle.danger, custom_id="absent")
    async def absent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._mark_attendance(interaction, "absent")

    @discord.ui.button(label="Not Signed", style=discord.ButtonStyle.secondary, custom_id="not_signed")
    async def not_signed_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._mark_attendance(interaction, "not_signed")

    @discord.ui.button(label="⬅️ Back to List", style=discord.ButtonStyle.secondary, custom_id="back_to_list")
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.parent_view.update_main_embed(interaction)

    async def _mark_attendance(self, interaction, attendance_type):
        """Unified attendance marking method"""
        last_attendance = await self.fetch_last_attendance(self.fid)
        
        if attendance_type == "present":
            modal = AttendanceModal(self.fid, self.nickname, attendance_type, self.parent_view, last_attendance)
            await interaction.response.send_modal(modal)
        else:
            await interaction.response.defer()
            await self.mark_attendance_direct_deferred(interaction, attendance_type, 0, last_attendance)

    async def mark_attendance_direct_deferred(self, interaction: discord.Interaction, attendance_type: str, points: int, last_attendance: str):
        """Mark attendance directly with deferred interaction for absent/not_signed"""
        try:
            # Single transaction for all database operations
            with sqlite3.connect('db/attendance.sqlite', timeout=10.0) as attendance_db, \
                sqlite3.connect('db/users.sqlite') as users_db, \
                sqlite3.connect('db/alliance.sqlite') as alliance_db:
                
                # Get user alliance
                user_cursor = users_db.cursor()
                user_cursor.execute("SELECT alliance FROM users WHERE fid = ?", (self.fid,))
                user_result = user_cursor.fetchone()
                if not user_result:
                    raise ValueError(f"User with FID {self.fid} not found in database")
                alliance_id = user_result[0]
                
                # Get alliance name
                alliance_cursor = alliance_db.cursor()
                alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = alliance_cursor.fetchone()
                alliance_name = alliance_result[0] if alliance_result else "Unknown Alliance"

                # Use INSERT OR REPLACE to handle existing records automatically
                attendance_cursor = attendance_db.cursor()
                attendance_cursor.execute("""
                    INSERT OR REPLACE INTO attendance_records
                    (fid, nickname, alliance_id, alliance_name, attendance_status, points,
                    last_event_attendance, marked_date, marked_by, marked_by_username, session_name)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (self.fid, self.nickname, alliance_id, alliance_name, attendance_type,
                    points, last_attendance, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    interaction.user.id, interaction.user.name, self.parent_view.session_name))
                
                attendance_db.commit()
            
            # Add to parent view's selected players
            self.parent_view.add_player_attendance(self.fid, self.nickname, attendance_type, points, last_attendance)
            
            # Update the main embed with confirmation message
            marked_count = len(self.parent_view.selected_players)
            total_count = len(self.parent_view.players)
            
            # Create status display
            status_display = {
                "present": "Present",
                "absent": "Absent", 
                "not_signed": "Not Signed"
            }.get(attendance_type, attendance_type)
            
            embed = discord.Embed(
                title=f"📋 Marking Attendance - {self.parent_view.alliance_name}",
                description=(
                    f"**Session:** {self.parent_view.session_name}\n"
                    f"**Progress:** {marked_count}/{total_count} players marked\n"
                    f"**Current Page:** {self.parent_view.page + 1}/{self.parent_view.max_page + 1}\n\n"
                    f"✅ **{self.nickname}** marked as **{status_display}**\n\n"
                    "Select a player from the dropdown to mark their attendance.\n"
                    "Use the buttons below to navigate, view summary, or finish."
                ),
                color=discord.Color.green()
            )
            
            if marked_count > 0:
                present = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'present')
                absent = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'absent')
                not_signed = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'not_signed')
                
                embed.add_field(
                    name="📊 Current Stats",
                    value=f"Present: {present}\nAbsent: {absent}\nNot Signed: {not_signed}",
                    inline=True
                )
            
            await interaction.edit_original_response(embed=embed, view=self.parent_view)
            
        except Exception as e:
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"Error: {str(e)[:100]}",
                color=discord.Color.red()
            )
            await interaction.edit_original_response(embed=error_embed, view=None)

class SessionSelectView(discord.ui.View):
    def __init__(self, sessions, alliance_id, cog):
        super().__init__(timeout=1800)
        self.sessions = sessions
        self.alliance_id = alliance_id
        self.cog = cog
 
        select = discord.ui.Select(
            placeholder="📋 Select a session...",
            options=[
                discord.SelectOption(
                    label=session[:100],
                    value=session,
                    description=f"Session: {session}"
                ) for session in sessions
            ]
        )
        select.callback = self.on_select
        self.add_item(select)

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        row=1
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)
 
    async def on_select(self, interaction: discord.Interaction):
        session_name = interaction.data['values'][0]
        await self.cog.show_attendance_report(interaction, self.alliance_id, session_name)

class Attendance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.setup_database()

    def _get_status_emoji(self, status):
        """Helper to get status emoji"""
        return {"present": "✅", "absent": "❌", "not_signed": "⚪"}.get(status, "❓")

    def _format_last_attendance(self, last_attendance):
        """Helper to format last attendance with emojis"""
        if last_attendance == "N/A" or "(" not in last_attendance:
            return last_attendance
        
        replacements = [
            ("present", "✅"), ("Present", "✅"),
            ("absent", "❌"), ("Absent", "❌"),
            ("not_signed", "⚪"), ("Not Signed", "⚪"), ("not signed", "⚪")
        ]
        
        for old, new in replacements:
            last_attendance = last_attendance.replace(old, new)
        return last_attendance

    def _create_error_embed(self, title, description, color=discord.Color.red()):
        """Helper to create error embeds"""
        return discord.Embed(title=title, description=description, color=color)

    def _create_back_view(self, callback):
        """Helper to create back button view"""
        view = discord.ui.View()
        back_button = discord.ui.Button(label="⬅️ Back", style=discord.ButtonStyle.secondary)
        back_button.callback = callback
        view.add_item(back_button)
        return view

    async def _check_admin_permissions(self, user_id):
        """Helper to check admin permissions"""
        with sqlite3.connect('db/settings.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (user_id,))
            return cursor.fetchone()

    async def _get_alliance_name(self, alliance_id):
        """Helper to get alliance name"""
        with sqlite3.connect('db/alliance.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
            result = cursor.fetchone()
            return result[0] if result else "Unknown Alliance"

    async def get_user_report_preference(self, user_id):
        """Get user's report preference"""
        try:
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT report_type FROM user_preferences 
                    WHERE user_id = ?
                """, (user_id,))
                result = cursor.fetchone()
                return result[0] if result else "text"
        except Exception:
            return "text"

    async def set_user_report_preference(self, user_id, preference):
        """Set user's report preference"""
        try:
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO user_preferences (user_id, report_type)
                    VALUES (?, ?)
                """, (user_id, preference))
                db.commit()
        except Exception as e:
            print(f"Error setting user preference: {e}")
            raise

    def setup_database(self):
        """Set up dedicated attendance database"""
        try:
            # Create attendance database if it doesn't exist
            if not os.path.exists("db/attendance.sqlite"):
                sqlite3.connect("db/attendance.sqlite").close()
                print("✓ Created and initialized new attendance database")
            
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                
                # Create attendance records table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attendance_records (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fid INTEGER,
                        nickname TEXT,
                        alliance_id INTEGER,
                        alliance_name TEXT,
                        attendance_status TEXT,
                        points INTEGER,
                        last_event_attendance TEXT,
                        marked_date TEXT,
                        marked_by INTEGER,
                        marked_by_username TEXT,
                        session_name TEXT
                    )
                """)
                
                # Check and add session_name column if missing
                cursor.execute("PRAGMA table_info(attendance_records)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'session_name' not in columns:
                    cursor.execute("ALTER TABLE attendance_records ADD COLUMN session_name TEXT")
                    print("✓ Added session_name column to attendance_records")
                
                # Create attendance sessions table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attendance_sessions (
                        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        alliance_id INTEGER,
                        alliance_name TEXT,
                        session_date TEXT,
                        created_by INTEGER,
                        created_by_username TEXT,
                        total_players INTEGER,
                        present_count INTEGER,
                        absent_count INTEGER,
                        not_signed_count INTEGER,
                        session_name TEXT
                    )
                """)
                
                # Check and add session_name column to sessions table
                cursor.execute("PRAGMA table_info(attendance_sessions)")
                columns = [col[1] for col in cursor.fetchall()]
                if 'session_name' not in columns:
                    cursor.execute("ALTER TABLE attendance_sessions ADD COLUMN session_name TEXT")
                    print("✓ Added session_name column to attendance_sessions")
                
                # Create session_records junction table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS session_records (
                        session_id INTEGER,
                        record_id INTEGER,
                        FOREIGN KEY (session_id) REFERENCES attendance_sessions(session_id),
                        FOREIGN KEY (record_id) REFERENCES attendance_records(id)
                    )
                """)

                # Create user preferences table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_preferences (
                        user_id INTEGER PRIMARY KEY,
                        report_type TEXT DEFAULT 'text'
                    )
                """)

                attendance_db.commit()
                
        except Exception as e:
            print(f"Error setting up attendance database: {e}")

    async def show_attendance_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show attendance records with user's preferred format"""
        try:
            # Get user's report preference
            report_type = await self.get_user_report_preference(interaction.user.id)
            
            # If matplotlib is not available, force text mode
            if report_type == "matplotlib" and not MATPLOTLIB_AVAILABLE:
                report_type = "text"
                
            if report_type == "matplotlib":
                await self.show_matplotlib_report(interaction, alliance_id, session_name)
            else:
                await self.show_text_report(interaction, alliance_id, session_name)
                
        except Exception as e:
            print(f"Error showing attendance report: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while generating attendance report.",
                ephemeral=True
            )

    async def show_matplotlib_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show attendance records as a Matplotlib table image"""
        try:
            font_prop = get_best_unicode_font()
            
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Get attendance records - sorted by points descending
            records = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                cursor.execute("""
                    SELECT nickname, attendance_status, last_event_attendance, points, marked_date, marked_by_username
                    FROM attendance_records
                    WHERE alliance_id = ? AND session_name = ?
                    ORDER BY points DESC, marked_date DESC
                """, (alliance_id, session_name))
                records = cursor.fetchall()

            if not records:
                await interaction.response.edit_message(
                    content=f"❌ No attendance records found for session '{session_name}' in {alliance_name}.",
                    embed=None,
                    view=None
                )
                return

            # Generate Matplotlib table image
            headers = ["Player", "Status", "Last Event", "Points", "Date", "Marked By"]
            table_data = []
            
            def fix_arabic(text):
                if text and re.search(r'[\u0600-\u06FF]', text):
                    try:
                        reshaped = arabic_reshaper.reshape(text)
                        return get_display(reshaped)
                    except Exception:
                        return text
                return text
                
            def wrap_text(text, width=20):
                if not text:
                    return ""
                lines = []
                for part in str(text).split('\n'):
                    while len(part) > width:
                        lines.append(part[:width])
                        part = part[width:]
                    lines.append(part)
                return '\n'.join(lines)

            for row in records:
                table_data.append([
                    wrap_text(fix_arabic(row[0] or "Unknown")),
                    wrap_text(fix_arabic(row[1].replace('_', ' ').title())),
                    wrap_text(fix_arabic(row[2] if row[2] else "N/A"), width=40),
                    wrap_text(f"{row[3]:,}" if row[3] else "0"),
                    wrap_text(fix_arabic(row[4].split()[0] if row[4] else "N/A")),
                    wrap_text(fix_arabic(row[5] or "Unknown"))
                ])

            fig, ax = plt.subplots(figsize=(13, min(1 + len(table_data) * 0.5, 20)))
            ax.axis('off')
            table = ax.table(
                cellText=table_data,
                colLabels=headers,
                cellLoc='left',
                loc='center',
                colColours=['#1f77b4']*len(headers)
            )
            table.auto_set_font_size(False)
            table.set_fontsize(12)
            table.scale(1, 1.5)
            
            # Set larger width for the 'Last Event' column (index 2)
            nrows = len(table_data) + 1
            for row in range(nrows):
                cell = table[(row, 2)]
                cell.set_width(0.35)

            # Set font for all cells
            for key, cell in table.get_celld().items():
                if hasattr(cell, 'set_fontproperties'):
                    cell.set_fontproperties(font_prop)
                elif hasattr(cell, 'set_font_properties'):
                    cell.set_font_properties(font_prop)

            plt.title(f'Attendance Report - {alliance_name} | Session: {session_name}', 
                     fontsize=16, color='#1f77b4', pad=20, fontproperties=font_prop)

            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            plt.close(fig)
            img_buffer.seek(0)

            file = discord.File(img_buffer, filename="attendance_report.png")

            embed = discord.Embed(
                title=f"📊 Attendance Report - {alliance_name}",
                description=f"**Session:** {session_name}\n**Total Players:** {len(records)}\n**Sorted by Points (Highest to Lowest)**",
                color=discord.Color.blue()
            )
            embed.set_image(url="attachment://attendance_report.png")

            # Add back button
            back_view = discord.ui.View(timeout=1800)
            back_button = discord.ui.Button(
                label="⬅️ Back to Sessions",
                style=discord.ButtonStyle.secondary
            )
            
            async def back_callback(back_interaction: discord.Interaction):
                await self.show_session_selection(back_interaction, alliance_id)
            
            back_button.callback = back_callback
            back_view.add_item(back_button)

            await interaction.response.edit_message(embed=embed, view=back_view, attachments=[file])

        except Exception as e:
            print(f"Matplotlib error: {e}")
            # Fallback to text report
            await self.show_text_report(interaction, alliance_id, session_name)

    async def show_text_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show attendance records for a specific session with emoji-based formatting (your original implementation)"""
        try:
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Get attendance records - sorted by points descending
            records = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                cursor.execute("""
                    SELECT nickname, attendance_status, last_event_attendance, points, marked_date, marked_by_username
                    FROM attendance_records
                    WHERE alliance_id = ? AND session_name = ?
                    ORDER BY points DESC, marked_date DESC
                """, (alliance_id, session_name))
                records = cursor.fetchall()

            if not records:
                await interaction.response.edit_message(
                    content=f"❌ No attendance records found for session '{session_name}' in {alliance_name}.",
                    embed=None,
                    view=None
                )
                return

            # Count attendance types
            present_count = sum(1 for r in records if r[1] == 'present')
            absent_count = sum(1 for r in records if r[1] == 'absent')
            not_signed_count = sum(1 for r in records if r[1] == 'not_signed')

            # Get session ID if available
            session_id = None
            try:
                with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                    cursor = attendance_db.cursor()
                    cursor.execute("""
                        SELECT session_id FROM attendance_sessions
                        WHERE session_name = ? AND alliance_id = ?
                        LIMIT 1
                    """, (session_name, alliance_id))
                    result = cursor.fetchone()
                    if result:
                        session_id = result[0]
            except:
                pass

            # Build the report sections
            report_sections = []
            
            # Summary section
            report_sections.append("📊 **SUMMARY**")
            report_sections.append(f"**Session:** {session_name}")
            report_sections.append(f"**Alliance:** {alliance_name}")
            report_sections.append(f"**Date:** {records[0][4].split()[0] if records else 'N/A'}")
            report_sections.append(f"**Total Players:** {len(records)}")
            report_sections.append(f"**Present:** {present_count} | **Absent:** {absent_count} | **Not Signed:** {not_signed_count}")
            if session_id:
                report_sections.append(f"**Session ID:** {session_id}")
            report_sections.append("")
            
            # Player details section
            report_sections.append("👥 **PLAYER DETAILS**")
            report_sections.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            
            # Sort: Present (by points desc) → Absent → Not Signed
            def sort_key(record):
                attendance_type = record[1]
                points = record[3] or 0
                
                type_priority = {
                    "present": 1,
                    "absent": 2, 
                    "not_signed": 3
                }.get(attendance_type, 4)
                
                return (type_priority, -points)
            
            sorted_records = sorted(records, key=sort_key)
            
            for record in sorted_records:
                nickname = record[0] or "Unknown"
                attendance_status = record[1]
                last_event_attendance = record[2] or "N/A"
                points = record[3] or 0
                
                # Get status emoji
                status_emoji = self._get_status_emoji(attendance_status)
                
                # Convert last attendance status to relevant emoji
                last_event_display = self._format_last_attendance(last_event_attendance)
                
                points_display = f"{points:,}" if points > 0 else "0"
                
                player_line = f"{status_emoji} **{nickname}**"
                if points > 0:
                    player_line += f" | **{points_display}** points"
                if last_event_attendance != "N/A":
                    player_line += f" | Last: {last_event_display}"
                
                report_sections.append(player_line)

            # Join all sections and create final embed
            report_description = "\n".join(report_sections)
            
            embed = discord.Embed(
                title=f"📊 Attendance Report - {alliance_name}",
                description=report_description,
                color=discord.Color.blue()
            )
            
            if session_id:
                embed.set_footer(text=f"Session ID: {session_id} | Sorted by Points (Highest to Lowest)")
            else:
                embed.set_footer(text="Sorted by Points (Highest to Lowest)")
            
            # Add back button
            back_view = discord.ui.View(timeout=1800)
            back_button = discord.ui.Button(
                label="⬅️ Back to Sessions",
                style=discord.ButtonStyle.secondary
            )
            
            async def back_callback(back_interaction: discord.Interaction):
                await self.show_session_selection(back_interaction, alliance_id)
            
            back_button.callback = back_callback
            back_view.add_item(back_button)
            
            await interaction.response.edit_message(embed=embed, view=back_view)

        except Exception as e:
            print(f"Error showing text attendance report: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while generating attendance report.",
                ephemeral=True
            )

    async def show_attendance_menu(self, interaction: discord.Interaction):
        """Show the main attendance menu"""
        embed = discord.Embed(
            title="📋 Attendance System",
            description=(
                "Please select an operation:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 **Mark Attendance**\n"
                "└ Mark attendance for alliance members\n\n"
                "👀 **View Attendance**\n"
                "└ View attendance records and reports\n\n"
                "⚙️ **Settings**\n"
                "└ Configure attendance preferences\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        
        view = AttendanceView(self, interaction.user.id, interaction.guild_id)
        await view.initialize_permissions_and_alliances()
        
        try:
            await interaction.response.edit_message(embed=embed, view=view, attachments=[])
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view, attachments=[])

    async def get_admin_alliances(self, user_id: int, guild_id: int):
        """Get alliances that the user has admin access to"""
        try:
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (user_id,))
                admin_result = cursor.fetchone()
                
                if not admin_result:
                    return [], [], False
                    
                is_initial = admin_result[0]
                
            if is_initial == 1:
                # Global admin - can access all alliances
                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                    cursor = alliance_db.cursor()
                    cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name")
                    alliances = cursor.fetchall()
                    return alliances, [], True
            
            # Server admin - get server and special access alliances
            server_alliances = []
            special_alliances = []
            
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("""
                    SELECT DISTINCT alliance_id, name 
                    FROM alliance_list 
                    WHERE discord_server_id = ?
                    ORDER BY name
                """, (guild_id,))
                server_alliances = cursor.fetchall()
            
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("""
                    SELECT alliances_id 
                    FROM adminserver 
                    WHERE admin = ?
                """, (user_id,))
                special_alliance_ids = cursor.fetchall()
                
            if special_alliance_ids:
                # Validate that all special alliance IDs are integers to prevent SQL injection
                validated_ids = []
                for aid_tuple in special_alliance_ids:
                    if isinstance(aid_tuple[0], int):
                        validated_ids.append(aid_tuple[0])
                    else:
                        print(f"Warning: Skipping invalid alliance ID: {aid_tuple[0]}")
                
                if validated_ids:
                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                        cursor = alliance_db.cursor()
                        placeholders = ','.join('?' * len(validated_ids))
                        cursor.execute(f"""
                            SELECT DISTINCT alliance_id, name
                            FROM alliance_list
                            WHERE alliance_id IN ({placeholders})
                            ORDER BY name
                        """, validated_ids)
                    special_alliances = cursor.fetchall()
            
            all_alliances = list({(aid, name) for aid, name in (server_alliances + special_alliances)})
            return all_alliances, special_alliances, False
                
        except Exception as e:
            print(f"Error getting admin alliances: {e}")
            return [], [], False

    async def show_attendance_marking(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show the attendance marking interface for selected alliance"""
        try:
            # Get alliance name
            alliance_name = "Unknown Alliance"
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = cursor.fetchone()
                if alliance_result:
                    alliance_name = alliance_result[0]

            # Get alliance members - sort by FC level (highest to lowest)
            players = []
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv 
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY furnace_lv DESC, nickname
                """, (alliance_id,))
                players = cursor.fetchall()

            if not players:
                await interaction.response.send_message(
                    f"❌ No players found in alliance {alliance_name}.",
                    ephemeral=True
                )
                return

            # Calculate alliance statistics with proper FC levels
            max_fl = max(player[2] for player in players) if players else 0
            avg_fl = sum(player[2] for player in players) / len(players) if players else 0
            
            # Start attendance marking process with player selection
            embed = discord.Embed(
                title=f"📋 Marking Attendance - {alliance_name}",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Total Players:** {len(players)}\n"
                    f"**Highest FC:** {FC_LEVEL_MAPPING.get(max_fl, str(max_fl))}\n"
                    f"**Average FC:** {FC_LEVEL_MAPPING.get(int(avg_fl), str(int(avg_fl)))}\n"
                    f"**Progress:** 0/{len(players)} players marked\n\n"
                    "Select a player from the dropdown to mark their attendance.\n"
                    "Players are sorted by FC level (highest to lowest)."
                ),
                color=discord.Color.blue()
            )

            view = PlayerSelectView(players, alliance_name, session_name, self)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"Error showing attendance marking: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading attendance marking.",
                ephemeral=True
            )

    async def process_attendance_results(self, interaction: discord.Interaction, selected_players: dict, alliance_name: str, session_name: str, use_defer: bool = True):
        """Process and display final attendance results"""
        try:
            # Count attendance types
            present_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'present')
            absent_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'absent')
            not_signed_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'not_signed')
            
            # Create attendance session in database
            session_id = None
            try:
                # Get alliance ID
                alliance_id = None
                for fid, data in selected_players.items():
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                        result = cursor.fetchone()
                        if result:
                            alliance_id = result[0]
                            break
                
                if alliance_id:
                    with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                        cursor = attendance_db.cursor()
                        
                        # Create session with session name
                        cursor.execute("""
                            INSERT INTO attendance_sessions 
                            (alliance_id, alliance_name, session_date, created_by, created_by_username,
                            total_players, present_count, absent_count, not_signed_count, session_name)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (alliance_id, alliance_name, datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            interaction.user.id, interaction.user.name, len(selected_players),
                            present_count, absent_count, not_signed_count, session_name))
                        
                        session_id = cursor.lastrowid
                        
                        # Link records to session
                        for fid in selected_players.keys():
                            cursor.execute("""
                                SELECT id FROM attendance_records 
                                WHERE fid = ? AND marked_by = ? AND session_name = ?
                                ORDER BY marked_date DESC LIMIT 1
                            """, (fid, interaction.user.id, session_name))
                            
                            record_result = cursor.fetchone()
                            if record_result:
                                cursor.execute("""
                                    INSERT INTO session_records (session_id, record_id)
                                    VALUES (?, ?)
                                """, (session_id, record_result[0]))
                        
                        attendance_db.commit()
                        print(f"✓ Created attendance session {session_id} for {alliance_name} - {session_name}")
                        
            except Exception as e:
                print(f"Warning: Could not create attendance session: {e}")

            # Check user's report preference
            report_type = await self.get_user_report_preference(interaction.user.id)
            
            # If matplotlib is not available, force text mode
            if report_type == "matplotlib" and not MATPLOTLIB_AVAILABLE:
                report_type = "text"
                
            if report_type == "matplotlib":
                await self.show_matplotlib_completion_report(interaction, selected_players, alliance_name, session_name, session_id, present_count, absent_count, not_signed_count)
            else:
                await self.show_text_completion_report(interaction, selected_players, alliance_name, session_name, session_id, present_count, absent_count, not_signed_count)

        except Exception as e:
            print(f"Error processing attendance results: {e}")
            error_embed = discord.Embed(
                title="❌ Error",
                description="An error occurred while generating the attendance report.",
                color=discord.Color.red()
            )
            
            if use_defer:
                await interaction.edit_original_response(embed=error_embed, view=None)
            else:
                await interaction.response.edit_message(embed=error_embed, view=None)

    async def show_matplotlib_completion_report(self, interaction, selected_players, alliance_name, session_name, session_id, present_count, absent_count, not_signed_count):
        """Show completion report using matplotlib"""
        try:
            font_prop = get_best_unicode_font()
            
            # Sort: Present (by points desc) → Absent → Not Signed
            def sort_key(item):
                fid, data = item
                attendance_type = data['attendance_type']
                points = data['points']
                
                type_priority = {
                    "present": 1,
                    "absent": 2, 
                    "not_signed": 3
                }.get(attendance_type, 4)
                
                return (type_priority, -points)
            
            sorted_players = sorted(selected_players.items(), key=sort_key)
            
            # Prepare data for matplotlib table
            headers = ["Player", "Status", "Points", "Last Event"]
            table_data = []
            
            def fix_arabic(text):
                if text and re.search(r'[\u0600-\u06FF]', text):
                    try:
                        reshaped = arabic_reshaper.reshape(text)
                        return get_display(reshaped)
                    except Exception:
                        return text
                return text
                
            def wrap_text(text, width=20):
                if not text:
                    return ""
                lines = []
                for part in str(text).split('\n'):
                    while len(part) > width:
                        lines.append(part[:width])
                        part = part[width:]
                    lines.append(part)
                return '\n'.join(lines)

            for fid, data in sorted_players:
                status_display = {
                    "present": "Present",
                    "absent": "Absent",
                    "not_signed": "Not Signed"
                }.get(data['attendance_type'], data['attendance_type'])
                
                # Format last event attendance
                last_event_display = data['last_event_attendance']
                if last_event_display != "N/A" and "(" in last_event_display:
                    if "present" in last_event_display.lower():
                        last_event_display = last_event_display.replace("present", "✅").replace("Present", "✅")
                    elif "absent" in last_event_display.lower():
                        last_event_display = last_event_display.replace("absent", "❌").replace("Absent", "❌")
                    elif "not_signed" in last_event_display.lower() or "not signed" in last_event_display.lower():
                        last_event_display = last_event_display.replace("not_signed", "⚪").replace("Not Signed", "⚪").replace("not signed", "⚪")
                
                table_data.append([
                    wrap_text(fix_arabic(data['nickname'])),
                    wrap_text(fix_arabic(status_display)),
                    wrap_text(f"{data['points']:,}" if data['points'] > 0 else "0"),
                    wrap_text(fix_arabic(last_event_display), width=30)
                ])

            fig, ax = plt.subplots(figsize=(14, min(2 + len(table_data) * 0.5, 20)))
            ax.axis('off')
            
            table = ax.table(
                cellText=table_data,
                colLabels=headers,
                cellLoc='left',
                loc='center',
                colColours=['#28a745']*len(headers)  # Green color for completion
            )
            table.auto_set_font_size(False)
            table.set_fontsize(11)
            table.scale(1, 1.4)
            
            # Set larger width for the 'Last Event' column (index 3)
            nrows = len(table_data) + 1
            for row in range(nrows):
                cell = table[(row, 3)]
                cell.set_width(0.3)

            # Set font for all cells
            for key, cell in table.get_celld().items():
                if hasattr(cell, 'set_fontproperties'):
                    cell.set_fontproperties(font_prop)
                elif hasattr(cell, 'set_font_properties'):
                    cell.set_font_properties(font_prop)

            plt.title(f'Attendance Report Completed - {alliance_name} | Session: {session_name}', 
                    fontsize=16, color='#28a745', pad=20, fontproperties=font_prop)

            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            plt.close(fig)
            img_buffer.seek(0)

            file = discord.File(img_buffer, filename="attendance_completion_report.png")

            embed = discord.Embed(
                title=f"✅ Attendance Report Completed",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Alliance:** {alliance_name}\n"
                    f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"**Total Players:** {len(selected_players)}\n"
                    f"**Present:** {present_count} | **Absent:** {absent_count} | **Not Signed:** {not_signed_count}\n"
                    f"**Session ID:** {session_id if session_id else 'N/A'}"
                ),
                color=discord.Color.green()
            )
            embed.set_image(url="attachment://attendance_completion_report.png")
            embed.set_footer(text=f"Marked by {interaction.user.name} | Saved to database")
            
            # Return to the attendance menu in the main message
            await self.show_attendance_menu_from_defer(interaction)
            
            # Send the detailed report as an ephemeral follow-up
            await interaction.followup.send(
                embed=embed,
                files=[file],
                ephemeral=True
            )

        except Exception as e:
            print(f"Matplotlib completion report error: {e}")
            # Fallback to text report
            await self.show_text_completion_report(interaction, selected_players, alliance_name, session_name, session_id, present_count, absent_count, not_signed_count)

    async def show_text_completion_report(self, interaction, selected_players, alliance_name, session_name, session_id, present_count, absent_count, not_signed_count):
        """Show completion report using text format"""
        # Sort: Present (by points desc) → Absent → Not Signed
        def sort_key(item):
            fid, data = item
            attendance_type = data['attendance_type']
            points = data['points']
            
            type_priority = {
                "present": 1,
                "absent": 2, 
                "not_signed": 3
            }.get(attendance_type, 4)
            
            # Sort by type priority first, then by points descending
            return (type_priority, -points)
        
        sorted_players = sorted(selected_players.items(), key=sort_key)
        
        report_sections = []
        
        # Report summary section
        report_sections.append("📊 **SUMMARY**")
        report_sections.append(f"**Session:** {session_name}")
        report_sections.append(f"**Alliance:** {alliance_name}")
        report_sections.append(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_sections.append(f"**Total Players:** {len(selected_players)}")
        report_sections.append(f"**Present:** {present_count} | **Absent:** {absent_count} | **Not Signed:** {not_signed_count}")
        if session_id:
            report_sections.append(f"**Session ID:** {session_id}")
        report_sections.append("")
        
        # Player details section
        report_sections.append("👥 **PLAYER DETAILS**")
        report_sections.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
        
        for fid, data in sorted_players:
            status_emoji = {
                "present": "✅",
                "absent": "❌", 
                "not_signed": "⚪"
            }.get(data['attendance_type'], "❓")
            
            # Convert last attendance status to relevant emoji
            last_event_display = data['last_event_attendance']
            if last_event_display != "N/A" and "(" in last_event_display:
                if "present" in last_event_display.lower():
                    last_event_display = last_event_display.replace("present", "✅").replace("Present", "✅")
                elif "absent" in last_event_display.lower():
                    last_event_display = last_event_display.replace("absent", "❌").replace("Absent", "❌")
                elif "not_signed" in last_event_display.lower() or "not signed" in last_event_display.lower():
                    last_event_display = last_event_display.replace("not_signed", "⚪").replace("Not Signed", "⚪").replace("not signed", "⚪")
            
            points_display = f"{data['points']:,}" if data['points'] > 0 else "0"
            
            player_line = f"{status_emoji} **{data['nickname']}**"
            if data['points'] > 0:
                player_line += f" | **{points_display}** points"
            if data['last_event_attendance'] != "N/A":
                player_line += f" | Last: {last_event_display}"
            
            report_sections.append(player_line)
        
        # Join all sections and create final embed for the ephemeral report
        report_description = "\n".join(report_sections)
        report_embed = discord.Embed(
            title=f"✅ Attendance Report Completed",
            description=report_description,
            color=discord.Color.green()
        )
        
        report_embed.set_footer(text=f"Marked by {interaction.user.name} | Saved to database")
        
        # Return to the attendance menu in the main message
        await self.show_attendance_menu_from_defer(interaction)
        
        # Send the detailed report as an ephemeral follow-up
        await interaction.followup.send(
            embed=report_embed,
            ephemeral=True
        )

    async def show_attendance_menu_from_defer(self, interaction: discord.Interaction):
        """Show the main attendance menu using edit_original_response (for deferred interactions)"""
        embed = discord.Embed(
            title="📋 Attendance System",
            description=(
                "Please select an operation:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 **Mark Attendance**\n"
                "└ Mark attendance for alliance members\n\n"
                "👀 **View Attendance**\n"
                "└ View attendance records and reports\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        
        view = AttendanceView(self, interaction.user.id, interaction.guild_id)
        await view.initialize_permissions_and_alliances()
        await interaction.edit_original_response(embed=embed, view=view)

    async def show_session_selection(self, interaction: discord.Interaction, alliance_id: int):
        """Show available attendance sessions for an alliance"""
        try:
            # Get alliance name
            alliance_name = "Unknown Alliance"
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = cursor.fetchone()
                if alliance_result:
                    alliance_name = alliance_result[0]
        
            # Get distinct session names from attendance records
            sessions = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                cursor.execute("""
                    SELECT DISTINCT session_name 
                    FROM attendance_records
                    WHERE alliance_id = ? 
                    AND session_name IS NOT NULL
                    AND TRIM(session_name) <> ''
                    ORDER BY marked_date DESC
                """, (alliance_id,))
                sessions = [row[0] for row in cursor.fetchall() if row[0]]

            if not sessions:
                # Create embed for no sessions found
                embed = discord.Embed(
                    title=f"📋 Attendance Sessions - {alliance_name}",
                    description=f"❌ **No attendance sessions found for {alliance_name}.**\n\nTo create attendance records, use the 'Mark Attendance' option from the main menu.",
                    color=discord.Color.orange()
                )
                
                # Add back button
                back_view = discord.ui.View(timeout=1800)
                back_button = discord.ui.Button(
                    label="⬅️ Back to Alliance Selection",
                    style=discord.ButtonStyle.secondary
                )
                
                async def back_callback(back_interaction: discord.Interaction):
                    await self.show_attendance_menu(back_interaction)
                
                back_button.callback = back_callback
                back_view.add_item(back_button)
                
                await interaction.response.edit_message(
                    content=None,
                    embed=embed,
                    view=back_view,
                    attachments=[]
                )
                return
        
            # Create session selection view
            view = SessionSelectView(sessions, alliance_id, self)
            
            embed = discord.Embed(
                title=f"📋 Attendance Sessions - {alliance_name}",
                description="Please select a session to view attendance records:",
                color=discord.Color.blue()
            )
            
            await interaction.response.edit_message(embed=embed, view=view, attachments=[])
    
        except Exception as e:
            print(f"Error showing session selection: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading sessions.",
                ephemeral=True
            )

    async def show_attendance_menu(self, interaction: discord.Interaction):
        """Show the main attendance menu"""
        embed = discord.Embed(
            title="📋 Attendance System",
            description=(
                "Please select an operation:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 **Mark Attendance**\n"
                "└ Mark attendance for alliance members\n\n"
                "👀 **View Attendance**\n"
                "└ View attendance records and reports\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        
        view = AttendanceView(self, interaction.user.id, interaction.guild_id)
        await view.initialize_permissions_and_alliances()
        
        try:
            await interaction.response.edit_message(embed=embed, view=view, attachments=[])
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view, attachments=[])

async def setup(bot):
    try:
        cog = Attendance(bot)
        await bot.add_cog(cog)
    except Exception as e:
        print(f"❌ Failed to load Attendance cog: {e}")