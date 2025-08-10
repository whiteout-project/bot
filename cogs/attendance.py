import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from datetime import datetime
import os
import re
from io import BytesIO
import uuid

try:
    import matplotlib.pyplot as plt
    import matplotlib.font_manager as fm
    import arabic_reshaper
    from bidi.algorithm import get_display
    
    # Load Unifont if available
    font_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "fonts")
    unifont_path = os.path.join(font_dir, "unifont-16.0.04.otf")
    if os.path.exists(unifont_path):
        fm.fontManager.addfont(unifont_path)
    
    # Simple font configuration
    plt.rcParams['font.sans-serif'] = ['Unifont', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

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

EVENT_TYPES = ["Foundry", "Canyon Clash", "Crazy Joe", "Bear Trap", "Castle Battle", "Frostdragon Tyrant", "Other"]

EVENT_TYPE_ICONS = {
    "Foundry": "🏭",
    "Canyon Clash": "⚔️",
    "Crazy Joe": "🤪",
    "Bear Trap": "🐻",
    "Castle Battle": "🏰",
    "Frostdragon Tyrant": "🐉",
    "Other": "📋"
}

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
        super().__init__(timeout=7200)
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
                    "• **Text** - Text-based reports (faster, no requirements)\n"
                    "• **Matplotlib** - Visual table reports (requires matplotlib)\n\n"
                    f"**Matplotlib Status:** {'✅ Available' if MATPLOTLIB_AVAILABLE else '❌ Not Available'}\n\n"
                    "Select your preferred report type below:"
                ),
                color=discord.Color.blue()
            )
            
            await interaction.response.edit_message(embed=embed, view=select_view)
            
        except Exception as e:
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
        super().__init__(timeout=7200)
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
            error_embed = self.cog._create_error_embed(
                "❌ Error", 
                "Failed to update settings."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

class AttendanceView(discord.ui.View):
    def __init__(self, cog, user_id, guild_id):
        super().__init__(timeout=7200)
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
        await self.cog.show_alliance_selection_for_marking(interaction)

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
            error_embed = self.cog._create_error_embed(
                "❌ Error",
                "An error occurred while returning to other features."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

class EventTypeSelectView(discord.ui.View):
    def __init__(self, session_data, cog, alliance_id, alliance_name):
        super().__init__(timeout=1800)
        self.session_data = session_data
        self.cog = cog
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        
        # Add the dropdown
        self.add_item(self.create_event_type_select())
        
        # Add back button
        back_button = discord.ui.Button(label="⬅️ Back", style=discord.ButtonStyle.secondary)
        back_button.callback = self.back_to_sessions
        self.add_item(back_button)
    
    def create_event_type_select(self):
        select = discord.ui.Select(
            placeholder="🎯 Select Event Type...",
            options=[
                discord.SelectOption(label="Foundry", value="Foundry", emoji="🏭"),
                discord.SelectOption(label="Canyon Clash", value="Canyon Clash", emoji="⚔️"),
                discord.SelectOption(label="Crazy Joe", value="Crazy Joe", emoji="🤪"),
                discord.SelectOption(label="Bear Trap", value="Bear Trap", emoji="🐻"),
                discord.SelectOption(label="Castle Battle", value="Castle Battle", emoji="🏰"),
                discord.SelectOption(label="Frostdragon Tyrant", value="Frostdragon Tyrant", emoji="🐉"),
                discord.SelectOption(label="Other", value="Other", emoji="📋", default=True)
            ]
        )
        
        async def select_callback(interaction: discord.Interaction):
            event_type = select.values[0]
            self.session_data['event_type'] = event_type
            
            # Proceed to player marking
            await self.cog.show_attendance_marking(
                interaction,
                self.alliance_id,
                self.alliance_name,
                self.session_data['name'],
                session_id=None,
                is_edit=False,
                event_type=event_type,
                event_date=self.session_data.get('event_date')
            )
        
        select.callback = select_callback
        return select
    
    async def back_to_sessions(self, interaction: discord.Interaction):
        await self.cog.show_session_selection_for_marking(interaction, self.alliance_id)

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
        
        self.event_date = discord.ui.TextInput(
            label="Event Date/Time (UTC)",
            placeholder="YYYY-MM-DD HH:MM (Leave empty for current time)",
            required=False,
            max_length=16
        )
        self.add_item(self.event_date)

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
        
        # Parse event date if provided
        event_date = None
        if self.event_date.value.strip():
            try:
                event_date = datetime.strptime(self.event_date.value.strip(), "%Y-%m-%d %H:%M")
            except ValueError:
                error_embed = discord.Embed(
                    title="❌ Invalid Date Format",
                    description="Please use the format: YYYY-MM-DD HH:MM (e.g., 2024-03-15 14:30)",
                    color=discord.Color.red()
                )
                await interaction.response.edit_message(embed=error_embed, view=None)
                return
            
        # Get alliance name
        alliance_name = await self.cog._get_alliance_name(self.alliance_id)
        
        # Show event type selection
        session_data = {
            'name': session_name,
            'event_date': event_date
        }
        
        event_view = EventTypeSelectView(session_data, self.cog, self.alliance_id, alliance_name)
        embed = discord.Embed(
            title="🎯 Select Event Type",
            description=f"**Session:** {session_name}\n**Alliance:** {alliance_name}\n\nPlease select the event type for this attendance session:",
            color=discord.Color.blue()
        )
        
        await interaction.response.edit_message(embed=embed, view=event_view)

class AllianceSelectView(discord.ui.View):
    def __init__(self, alliances_with_counts, cog, page=0, is_marking=False):
        super().__init__(timeout=7200)
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
            ],
            row=0  # Explicitly set row 0 for dropdown
        )
        
        async def select_callback(interaction: discord.Interaction):
            self.current_select = select
            alliance_id = int(select.values[0])

            if self.is_marking:
                # For marking: show session selection
                await self.cog.show_session_selection_for_marking(interaction, alliance_id)
            else:
                # For viewing: show session selection without defer
                report_cog = self.cog.bot.get_cog("AttendanceReport")
                if report_cog:
                    await report_cog.show_session_selection(interaction, alliance_id)

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

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary, row=1)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary, row=1)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(
        label="⬅️ Back",
        style=discord.ButtonStyle.secondary,
        row=1
    )
    async def back_to_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)

class EditEventDetailsView(discord.ui.View):
    def __init__(self, session_id, session_name, current_event_type, current_event_date, parent_view, is_edit=True):
        super().__init__(timeout=1800)
        self.session_id = session_id
        self.session_name = session_name
        self.current_event_type = current_event_type
        self.current_event_date = current_event_date
        self.parent_view = parent_view
        self.selected_event_type = current_event_type
        self.new_event_date = None
        self.is_edit = is_edit
        
        # Create event type dropdown
        self.event_type_select = discord.ui.Select(
            placeholder=f"Event Type: {current_event_type or 'Select...'}",
            options=[
                discord.SelectOption(label="Foundry", value="Foundry", emoji="🏭", default=(current_event_type == "Foundry")),
                discord.SelectOption(label="Canyon Clash", value="Canyon Clash", emoji="⚔️", default=(current_event_type == "Canyon Clash")),
                discord.SelectOption(label="Crazy Joe", value="Crazy Joe", emoji="🤪", default=(current_event_type == "Crazy Joe")),
                discord.SelectOption(label="Bear Trap", value="Bear Trap", emoji="🐻", default=(current_event_type == "Bear Trap")),
                discord.SelectOption(label="Castle Battle", value="Castle Battle", emoji="🏰", default=(current_event_type == "Castle Battle")),
                discord.SelectOption(label="Frostdragon Tyrant", value="Frostdragon Tyrant", emoji="🐉", default=(current_event_type == "Frostdragon Tyrant")),
                discord.SelectOption(label="Other", value="Other", emoji="📋", default=(current_event_type == "Other" or not current_event_type))
            ],
            row=1
        )
        self.event_type_select.callback = self.on_event_type_select
        self.add_item(self.event_type_select)
        
        # Remove delete button if not in edit mode
        if not self.is_edit:
            for item in self.children:
                if hasattr(item, 'label') and item.label == "🗑️ Delete Event":
                    item.disabled = True
        
    async def on_event_type_select(self, interaction: discord.Interaction):
        self.selected_event_type = self.event_type_select.values[0]
        await interaction.response.defer()
        
    @discord.ui.button(label="✏️ Rename Session", style=discord.ButtonStyle.secondary, row=0)
    async def rename_session_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        rename_modal = RenameSessionModal(self.session_id, self.session_name, self)
        await interaction.response.send_modal(rename_modal)
    
    @discord.ui.button(label="📅 Edit Date", style=discord.ButtonStyle.secondary, row=0)
    async def edit_date_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        date_modal = EventDateModal(self.current_event_date, self)
        await interaction.response.send_modal(date_modal)

    @discord.ui.button(label="💾 Save", style=discord.ButtonStyle.primary, row=2)
    async def save_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Use the selected event type
            event_type = self.selected_event_type
            
            # Use new date if set, otherwise keep current
            event_date = self.new_event_date if self.new_event_date else self.current_event_date
            if isinstance(event_date, str):
                try:
                    event_date = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                except:
                    event_date = datetime.utcnow()
            elif event_date is None:
                # If no date is set, use current datetime
                event_date = datetime.utcnow()
            
            # Update the database
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    UPDATE attendance_records
                    SET event_type = ?, event_date = ?
                    WHERE session_id = ?
                """, (event_type, event_date.isoformat(), self.session_id))
                db.commit()
            
            # Update parent view and refresh
            self.parent_view.event_type = event_type
            self.parent_view.event_date = event_date
            await self.parent_view.update_main_embed(interaction)
            
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error updating event details: {str(e)}",
                ephemeral=True
            )
            
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.danger, row=2)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.parent_view.update_main_embed(interaction)
    
    @discord.ui.button(label="🗑️ Delete Event", style=discord.ButtonStyle.danger, row=3)
    async def delete_event_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only show for edit mode
        if not self.is_edit:
            button.disabled = True
            return
            
        # Confirm deletion
        confirm_embed = discord.Embed(
            title="⚠️ Confirm Deletion",
            description=f"Are you sure you want to delete the session **{self.session_name}**?\n\nThis action cannot be undone.",
            color=discord.Color.orange()
        )
        
        # Get alliance_id from parent_view (PlayerSelectView)
        alliance_id = self.parent_view.alliance_id if hasattr(self.parent_view, 'alliance_id') else None
        confirm_view = ConfirmDeleteView(self.session_id, self.parent_view, alliance_id)
        await interaction.response.edit_message(embed=confirm_embed, view=confirm_view)

class EventDateModal(discord.ui.Modal, title="Edit Event Date"):
    def __init__(self, current_event_date, parent_view):
        super().__init__()
        self.current_event_date = current_event_date
        self.parent_view = parent_view
        
        # Add event date input
        current_date_str = ""
        if current_event_date:
            if isinstance(current_event_date, str):
                # Parse ISO format to display format
                try:
                    dt = datetime.fromisoformat(current_event_date.replace('Z', '+00:00'))
                    current_date_str = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    current_date_str = current_event_date
            elif isinstance(current_event_date, datetime):
                current_date_str = current_event_date.strftime("%Y-%m-%d %H:%M")
                
        self.event_date_input = discord.ui.TextInput(
            label="Event Date/Time (UTC)",
            placeholder="YYYY-MM-DD HH:MM (Leave empty to keep current)",
            default=current_date_str,
            required=False,
            max_length=16
        )
        self.add_item(self.event_date_input)
        
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse event date if provided
            if self.event_date_input.value.strip():
                try:
                    event_date = datetime.strptime(self.event_date_input.value.strip(), "%Y-%m-%d %H:%M")
                    self.parent_view.new_event_date = event_date
                    await interaction.response.send_message(
                        f"✅ Date updated to: {event_date.strftime('%Y-%m-%d %H:%M')} UTC",
                        ephemeral=True
                    )
                except ValueError:
                    await interaction.response.send_message(
                        "❌ Invalid date format. Please use: YYYY-MM-DD HH:MM",
                        ephemeral=True
                    )
            else:
                await interaction.response.send_message(
                    "ℹ️ Date unchanged.",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error: {str(e)}",
                ephemeral=True
            )

class RenameSessionModal(discord.ui.Modal, title="Rename Session"):
    def __init__(self, session_id, current_name, parent_view):
        super().__init__()
        self.session_id = session_id
        self.current_name = current_name
        self.parent_view = parent_view
        
        self.new_name = discord.ui.TextInput(
            label="New Session Name",
            placeholder="Enter new name for the session",
            default=current_name,
            required=True,
            max_length=50
        )
        self.add_item(self.new_name)
        
    async def on_submit(self, interaction: discord.Interaction):
        try:
            new_name = self.new_name.value.strip()
            if not new_name:
                await interaction.response.send_message(
                    "❌ Session name cannot be empty.",
                    ephemeral=True
                )
                return
                
            # Update session name in database
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    UPDATE attendance_records 
                    SET session_name = ? 
                    WHERE session_id = ?
                """, (new_name, self.session_id))
                db.commit()
                
            # Update parent views
            self.parent_view.session_name = new_name
            if hasattr(self.parent_view, 'parent_view'):
                self.parent_view.parent_view.session_name = new_name
                
            await interaction.response.send_message(
                f"✅ Session renamed to: **{new_name}**",
                ephemeral=True
            )
            
            # Refresh the view
            await self.parent_view.parent_view.update_main_embed(interaction)
            
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error renaming session: {str(e)}",
                ephemeral=True
            )

class ConfirmDeleteView(discord.ui.View):
    def __init__(self, session_id, parent_view, alliance_id):
        super().__init__(timeout=300)
        self.session_id = session_id
        self.parent_view = parent_view
        self.alliance_id = alliance_id
        
    @discord.ui.button(label="🗑️ Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Delete session and all associated records
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                # Delete all attendance records for this session
                cursor.execute("DELETE FROM attendance_records WHERE session_id = ?", (self.session_id,))
                db.commit()
                
            # Show success message
            success_embed = discord.Embed(
                title="✅ Session Deleted",
                description="The attendance session has been permanently deleted.",
                color=discord.Color.green()
            )
            
            # Create back button to return to session list
            back_view = discord.ui.View(timeout=7200)
            back_button = discord.ui.Button(
                label="⬅️ Back",
                style=discord.ButtonStyle.secondary
            )
            async def back_callback(i: discord.Interaction):
                # Get cog directly from parent_view (PlayerSelectView)
                cog = self.parent_view.cog
                await cog.show_attendance_menu(i)
            back_button.callback = back_callback
            back_view.add_item(back_button)
            
            await interaction.response.edit_message(embed=success_embed, view=back_view)
            
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Error deleting session: {str(e)}",
                ephemeral=True
            )
    
    @discord.ui.button(label="❌ Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.parent_view.parent_view.update_main_embed(interaction)

class PlayerFilterModal(discord.ui.Modal, title="Filter Players"):
    def __init__(self, parent_view):
        super().__init__()
        self.parent_view = parent_view
        
        self.filter_input = discord.ui.TextInput(
            label="Filter by ID or Name",
            placeholder="Enter player ID or name (partial match supported)",
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
        self.parent_view.update_clear_button_visibility()
        await self.parent_view.update_main_embed(interaction)

class PlayerSelectView(discord.ui.View):
    def __init__(self, players, alliance_name, session_name, cog, alliance_id=None, session_id=None, is_edit=False, page=0, event_type="Other", event_date=None):
        super().__init__(timeout=7200)
        self.players = players
        self.alliance_name = alliance_name
        self.session_name = session_name
        self.cog = cog
        self.alliance_id = alliance_id if alliance_id is not None else 0  # Default for backward compat
        self.session_id = session_id
        self.is_edit = is_edit
        self.event_type = event_type
        self.event_date = event_date
        self.selected_players = {}
        
        # Pre-populate selected_players if in edit mode
        if is_edit and players:
            for player in players:
                if len(player) >= 5:
                    fid, nickname, furnace_lv, status, points = player[:5]
                    if status in ['present', 'absent', 'not_recorded']:
                        self.selected_players[fid] = {
                            'nickname': nickname,
                            'attendance_type': status,
                            'points': points,
                            'last_event_attendance': 'N/A'  # This will be fetched if needed
                        }
        
        self.page = page
        self.max_page = (len(players) - 1) // 25 if players else 0
        self.current_select = None
        
        # Filter-related attributes
        self.filter_text = ""
        self.filtered_players = self.players.copy()
        
        self.update_select_menu()
        self.update_clear_button_visibility()
        
        # Edit Event Details button is now available for both create and edit modes

    def apply_filter(self):
        """Apply the filter to the players list"""
        if not self.filter_text:
            self.filtered_players = self.players.copy()
        else:
            filter_lower = self.filter_text.lower()
            self.filtered_players = []
            
            for player in self.players:
                # Handle both dict and tuple formats
                if isinstance(player, dict):
                    fid = str(player['fid'])
                    nickname = player['nickname']
                else:
                    fid = str(player[0])
                    nickname = player[1] if len(player) > 1 else ""
                
                # Check if filter matches FID or nickname (partial, case-insensitive)
                if filter_lower in fid.lower() or filter_lower in nickname.lower():
                    self.filtered_players.append(player)
        
        # Update max page based on filtered results
        self.max_page = (len(self.filtered_players) - 1) // 25 if self.filtered_players else 0
        
        # Ensure current page is valid
        if self.page > self.max_page:
            self.page = self.max_page
    
    def update_clear_button_visibility(self):
        """Enable/disable the Clear button based on filter status"""
        clear_button = next((item for item in self.children if hasattr(item, 'label') and item.label == "❌ Clear"), None)
        if clear_button:
            clear_button.disabled = not bool(self.filter_text)

    def update_select_menu(self):
        # Remove existing select menu
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        start_idx = self.page * 25
        end_idx = min(start_idx + 25, len(self.filtered_players))
        current_players = self.filtered_players[start_idx:end_idx]

        # Create options with status emojis
        options = []
        for player in current_players:
            if isinstance(player, dict):
                fid = player['fid']
                nickname = player['nickname']
                furnace_lv = player.get('furnace_lv', 0)
                # Check if we have an updated status in selected_players
                if fid in self.selected_players:
                    status = self.selected_players[fid]['attendance_type']
                else:
                    status = player.get('status', 'not_recorded')
                status_emoji = self.cog._get_status_emoji(status)
                
                label = f"{status_emoji} {nickname[:40]}"
                description = f"FID: {fid} | FC: {FC_LEVEL_MAPPING.get(furnace_lv, str(furnace_lv))}"
            else:
                # Handle tuple format with 3 or 5 elements
                if len(player) == 3:
                    fid, nickname, furnace_lv = player
                    if fid in self.selected_players:
                        status = self.selected_players[fid]['attendance_type']
                    else:
                        status = 'not_recorded'
                    status_emoji = self.cog._get_status_emoji(status)
                elif len(player) >= 5:
                    fid, nickname, furnace_lv, status, points = player[:5]
                    if fid in self.selected_players:
                        status = self.selected_players[fid]['attendance_type']
                    status_emoji = self.cog._get_status_emoji(status)
                else:
                    # Fallback
                    fid = player[0]
                    nickname = player[1] if len(player) > 1 else "Unknown"
                    furnace_lv = player[2] if len(player) > 2 else 0
                    if fid in self.selected_players:
                        status = self.selected_players[fid]['attendance_type']
                    else:
                        status = 'not_recorded'
                    status_emoji = self.cog._get_status_emoji(status)
                
                label = f"{status_emoji} {nickname[:40]}"
                description = f"FID: {fid} | FC: {FC_LEVEL_MAPPING.get(furnace_lv, str(furnace_lv))}"
                
            options.append(discord.SelectOption(
                label=label,
                value=str(fid),
                description=description[:100],
                emoji="👤"
            ))
        
        # Update placeholder to show filter status
        if self.filter_text:
            if not options:
                # No results found - create a dummy option
                placeholder = f"❌ No results for '{self.filter_text}'"
                options = [discord.SelectOption(
                    label="No players found",
                    value="none",
                    description="Clear the filter to see all players",
                    emoji="❌"
                )]
            else:
                placeholder = f"👥 Filtered: '{self.filter_text}' - {len(self.filtered_players)} results (Page {self.page + 1}/{self.max_page + 1})"
        else:
            placeholder = f"👥 Select a player to mark attendance... (Page {self.page + 1}/{self.max_page + 1})"
        
        select = discord.ui.Select(
            placeholder=placeholder,
            options=options
        )
        
        async def select_callback(interaction: discord.Interaction):
            try:
                self.current_select = select
                
                # Check if this is the "no results" option
                if select.values[0] == "none":
                    await interaction.response.send_message(
                        "No players found with the current filter. Use the Clear button to remove the filter.",
                        ephemeral=True
                    )
                    return
                
                selected_fid = int(select.values[0])
                
                # Find the selected player with proper error handling
                selected_player = None
                
                # Check if we have players and determine format
                if self.players:
                    if isinstance(self.players[0], dict):
                        selected_player = next((p for p in self.players if p['fid'] == selected_fid), None)
                    else:
                        selected_player = next((p for p in self.players if p[0] == selected_fid), None)
                
                if selected_player:
                    await self.show_player_attendance_options(interaction, selected_player)
                else:
                    # Handle the case where player is not found
                    error_embed = discord.Embed(
                        title="❌ Error",
                        description="Player not found. Please try again.",
                        color=discord.Color.red()
                    )
                    await interaction.response.edit_message(embed=error_embed, view=self)
            except Exception as e:
                error_embed = discord.Embed(
                    title="❌ Error",
                    description="An error occurred while selecting the player. Please try again.",
                    color=discord.Color.red()
                )
                try:
                    await interaction.response.edit_message(embed=error_embed, view=self)
                except:
                    # If response already sent, try followup
                    await interaction.followup.send(embed=error_embed, ephemeral=True)
        
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
        # Handle both dict and tuple formats
        if isinstance(player, dict):
            fid = player['fid']
            nickname = player['nickname']
            furnace_lv = player.get('furnace_lv', 0)
        else:
            # Handle tuple format - can be 3 or 5 elements
            if len(player) >= 5:
                fid, nickname, furnace_lv, status, points = player[:5]
            else:
                fid, nickname, furnace_lv = player[:3]
        
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
    
    @discord.ui.button(label="🔍 Filter", style=discord.ButtonStyle.secondary, row=1)
    async def filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = PlayerFilterModal(self)
        await interaction.response.send_modal(modal)
    
    @discord.ui.button(label="❌ Clear", style=discord.ButtonStyle.danger, row=1, disabled=True)
    async def clear_filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.filter_text = ""
        self.page = 0
        self.apply_filter()
        self.update_select_menu()
        self.update_clear_button_visibility()
        await self.update_main_embed(interaction)
    
    @discord.ui.button(label="⚙️ Edit Event", style=discord.ButtonStyle.secondary, row=1)
    async def edit_event_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Show view to edit event type and date
        view = EditEventDetailsView(self.session_id, self.session_name, self.event_type, self.event_date, self, is_edit=self.is_edit)
        embed = discord.Embed(
            title="⚙️ Edit Event",
            description=(
                f"**Session:** {self.session_name}\n"
                f"**Current Event Type:** {self.event_type}\n"
                f"**Current Date:** {self.event_date.strftime('%Y-%m-%d %H:%M UTC') if isinstance(self.event_date, datetime) else self.event_date or 'Not set'}\n\n"
                "Select a new event type from the dropdown and/or edit the date."
            ),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="📊 View Summary", style=discord.ButtonStyle.primary, row=2)
    async def view_summary_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_players:
            # Show error in the same message
            error_embed = discord.Embed(
                title="❌ No Data",
                description="No attendance has been marked yet.",
                color=discord.Color.orange()
            )
            back_view = discord.ui.View(timeout=7200)
            back_button = discord.ui.Button(
                label="⬅️ Close",
                style=discord.ButtonStyle.secondary
            )
            back_button.callback = lambda i: self.update_main_embed(i)
            back_view.add_item(back_button)
            
            await interaction.response.edit_message(embed=error_embed, view=back_view)
            return
        
        await self.show_summary(interaction)

    @discord.ui.button(label="✅ Finish Attendance", style=discord.ButtonStyle.success, row=2)
    async def finish_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            if not self.selected_players:
                error_embed = discord.Embed(
                    title="❌ No Data",
                    description="No attendance has been marked yet.",
                    color=discord.Color.orange()
                )
                back_view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(
                    label="⬅️ Close",
                    style=discord.ButtonStyle.secondary
                )
                back_button.callback = lambda i: self.update_main_embed(i)
                back_view.add_item(back_button)
                
                await interaction.response.edit_message(embed=error_embed, view=back_view)
                return
            
            await interaction.response.defer()
            await self.cog.process_attendance_results(
                interaction, 
                self.selected_players, 
                self.alliance_name, 
                self.session_name, 
                use_defer=True,
                session_id=self.session_id,
                is_edit=self.is_edit,
                event_type=self.event_type,
                event_date=self.event_date,
                alliance_id=self.alliance_id
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            await interaction.edit_original_response(
                content=f"❌ An error occurred while processing attendance: {str(e)}",
                embed=None,
                view=None
            )

    @discord.ui.button(label="⬅️ Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_to_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_attendance_menu(interaction)

    async def update_main_embed(self, interaction: discord.Interaction):
        marked_count = sum(1 for p in self.selected_players.values() 
                          if p['attendance_type'] in ['present', 'absent'])
        total_count = len(self.players)
        
        # Build description with filter info
        description_parts = [
            f"**Session:** {self.session_name}",
            f"**Progress:** {marked_count}/{total_count} players marked",
            f"**Current Page:** {self.page + 1}/{self.max_page + 1}"
        ]
        
        if self.filter_text:
            description_parts.append(f"**Filter Active:** '{self.filter_text}' ({len(self.filtered_players)} results)")
        
        description_parts.extend([
            "",
            "Select a player from the dropdown to mark their attendance.",
            "Use the buttons below to navigate, view summary, or finish."
        ])
        
        embed = discord.Embed(
            title=f"📋 Marking Attendance - {self.alliance_name}",
            description="\n".join(description_parts),
            color=discord.Color.blue()
        )
        
        if total_count > 0:
            present = sum(1 for p in self.selected_players.values() if p['attendance_type'] == 'present')
            absent = sum(1 for p in self.selected_players.values() if p['attendance_type'] == 'absent')
            not_recorded = total_count - present - absent
            
            embed.add_field(
                name="📊 Current Stats",
                value=f"Present: {present}\nAbsent: {absent}\nNot Recorded: {not_recorded}",
                inline=True
            )
        
        await interaction.response.edit_message(embed=embed, view=self, attachments=[])

    async def show_summary(self, interaction: discord.Interaction):
        """Show attendance summary using unified report function"""
        try:
            report_cog = self.cog.bot.get_cog("AttendanceReport")
            if report_cog:
                await report_cog.show_attendance_report(
                    interaction=interaction,
                    alliance_id=self.alliance_id,
                    session_name=self.session_name,
                    session_id=self.session_id,
                    is_preview=True,
                    selected_players=self.selected_players,
                    marking_view=self
                )
            else:
                await interaction.response.send_message(
                    "❌ Attendance Report module not loaded.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"ERROR in show_summary: {e}")
            import traceback
            traceback.print_exc()
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"❌ An error occurred while generating the summary: {str(e)}",
                    ephemeral=True
                )


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
            
            self.parent_view.add_player_attendance(self.fid, self.nickname, self.attendance_type, points, self.last_attendance)
            
            # Update the select menu to reflect the new status
            self.parent_view.update_select_menu()
            
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
        # Only count present and absent as "marked", not "not_recorded"
        marked_count = sum(1 for p in self.parent_view.selected_players.values() 
                          if p['attendance_type'] in ['present', 'absent'])
        total_count = len(self.parent_view.players)
        
        # Create status display
        status_display = {
            "present": "Present",
            "absent": "Absent",
            "not_recorded": "Not Recorded"
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
        
        total_count = len(self.parent_view.players)
        if total_count > 0:
            present = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'present')
            absent = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'absent')
            not_recorded = total_count - present - absent
            
            embed.add_field(
                name="📊 Current Stats",
                value=f"Present: {present}\nAbsent: {absent}\nNot Recorded: {not_recorded}",
                inline=True
            )
        
        # Defer first, then edit
        await interaction.response.defer()
        await interaction.edit_original_response(embed=embed, view=self.parent_view)

class PlayerAttendanceView(discord.ui.View):
    def __init__(self, player, parent_view):
        super().__init__(timeout=7200)
        self.player = player
        self.parent_view = parent_view
        self.event_type = parent_view.event_type if hasattr(parent_view, 'event_type') else "Other"
        
        # Handle both dict and tuple formats
        if isinstance(player, dict):
            self.fid = player['fid']
            self.nickname = player['nickname']
            self.furnace_lv = player.get('furnace_lv', 0)
        else:
            # Handle tuple format - can be 3 or 5 elements
            if len(player) >= 5:
                self.fid, self.nickname, self.furnace_lv, status, points = player[:5]
            else:
                self.fid, self.nickname, self.furnace_lv = player[:3]

    async def fetch_last_attendance(self, fid):
        def query():
            with sqlite3.connect('db/attendance.sqlite') as conn:
                cursor = conn.cursor()
                # Check which schema we have
                cursor.execute("PRAGMA table_info(attendance_records)")
                columns = {col[1] for col in cursor.fetchall()}
                
                if 'player_id' in columns:
                    # New schema - filter by event type and exclude current session
                    cursor.execute(
                        "SELECT status, event_date FROM attendance_records "
                        "WHERE player_id = ? AND event_type = ? "
                        "AND event_date < ? AND session_id != ? "
                        "ORDER BY event_date DESC LIMIT 1",
                        (str(fid), self.event_type, self.parent_view.event_date, self.parent_view.session_id)
                    )
                else:
                    # Old schema
                    cursor.execute(
                        "SELECT attendance_status, marked_date FROM attendance_records "
                        "WHERE fid = ? "
                        "ORDER BY marked_date DESC LIMIT 1",
                        (fid,)
                    )
                result = cursor.fetchone()
                if result:
                    status, date_str = result
                    # Format the date
                    try:
                        if 'T' in date_str:
                            date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                            formatted_date = date_obj.strftime("%m/%d")
                        else:
                            formatted_date = date_str[:10]
                    except:
                        formatted_date = date_str[:10] if len(date_str) >= 10 else date_str
                    
                    status_display = status.replace('_', ' ').title() if status else status
                    return f"{status_display} ({formatted_date})"
                else:
                    return "N/A"
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

    @discord.ui.button(label="Not Recorded", style=discord.ButtonStyle.secondary, custom_id="not_recorded")
    async def not_recorded_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._mark_attendance(interaction, "not_recorded")

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
        """Mark attendance directly with deferred interaction for absent/not_recorded"""
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

                pass
            
            # Add to parent view's selected players (or remove if not_recorded)
            if attendance_type == 'not_recorded':
                # Remove from selected players if marking as not_recorded
                if self.fid in self.parent_view.selected_players:
                    del self.parent_view.selected_players[self.fid]
            else:
                # Add to selected players for present/absent
                self.parent_view.add_player_attendance(self.fid, self.nickname, attendance_type, points, last_attendance)
            
            # Update the select menu to reflect the new status
            self.parent_view.update_select_menu()
            
            # Update the main embed with confirmation message
            marked_count = sum(1 for p in self.parent_view.selected_players.values() 
                              if p['attendance_type'] in ['present', 'absent'])
            total_count = len(self.parent_view.players)
            
            # Create status display
            status_display = {
                "present": "Present",
                "absent": "Absent", 
                "not_recorded": "Not Recorded"
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
            
            total_count = len(self.parent_view.players)
            if total_count > 0:
                present = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'present')
                absent = sum(1 for p in self.parent_view.selected_players.values() if p['attendance_type'] == 'absent')
                not_recorded = total_count - present - absent
                
                embed.add_field(
                    name="📊 Current Stats",
                    value=f"Present: {present}\nAbsent: {absent}\nNot Recorded: {not_recorded}",
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

class Attendance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.setup_database()

    def _get_status_emoji(self, status):
        """Helper to get status emoji"""
        return {"present": "✅", "absent": "❌", "not_recorded": "⚪"}.get(status, "❓")

    def _format_last_attendance(self, last_attendance):
        """Helper to format last attendance with emojis"""
        if last_attendance == "N/A" or "(" not in last_attendance:
            return last_attendance
        
        replacements = [
            ("present", "✅"), ("Present", "✅"),
            ("absent", "❌"), ("Absent", "❌"),
            ("not_recorded", "⚪"), ("Not Recorded", "⚪"), ("not recorded", "⚪")
        ]
        
        for old, new in replacements:
            last_attendance = last_attendance.replace(old, new)
        return last_attendance

    def _create_error_embed(self, title, description, color=discord.Color.red()):
        """Helper to create error embeds"""
        return discord.Embed(title=title, description=description, color=color)

    def _create_back_view(self, callback):
        """Helper to create back button view"""
        view = discord.ui.View(timeout=7200)
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
            raise

    def setup_database(self):
        """Set up simplified attendance database with single table"""
        try:
            # Create attendance database if it doesn't exist
            if not os.path.exists("db/attendance.sqlite"):
                os.makedirs("db", exist_ok=True)
                sqlite3.connect("db/attendance.sqlite").close()
            
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                
                # Create unified attendance records table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attendance_records (
                        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        session_name TEXT NOT NULL,
                        event_type TEXT NOT NULL DEFAULT 'Other',
                        event_date TIMESTAMP,
                        player_id TEXT NOT NULL,
                        player_name TEXT NOT NULL,
                        alliance_id TEXT NOT NULL,
                        alliance_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        points INTEGER DEFAULT 0,
                        marked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        marked_by TEXT,
                        marked_by_username TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(session_id, player_id)
                    )
                """)
                
                # Create indices for performance
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_attendance_session ON attendance_records(session_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_attendance_alliance ON attendance_records(alliance_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_attendance_created ON attendance_records(created_at)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_attendance_player ON attendance_records(player_id)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_attendance_event_type ON attendance_records(event_type)")

                # Create user preferences table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS user_preferences (
                        user_id INTEGER PRIMARY KEY,
                        report_type TEXT DEFAULT 'text'
                    )
                """)
                
                attendance_db.commit()
                
        except Exception as e:
            pass

    async def show_attendance_menu(self, interaction: discord.Interaction):
        """Show the main attendance menu"""
        # Check if used in a server context
        if interaction.guild is None:
            await interaction.response.send_message(
                "❌ This command can only be used in a server, not in DMs.",
                ephemeral=True
            )
            return
            
        embed = discord.Embed(
            title="📋 Attendance System",
            description=(
                "Please select an operation:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "📋 **Mark Attendance**\n"
                "└ Create or modify attendance records\n\n"
                "👀 **View Attendance**\n"
                "└ View attendance records and export reports\n\n"
                "⚙️ **Settings**\n"
                "└ Configure attendance preferences\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        
        view = AttendanceView(self, interaction.user.id, interaction.guild.id)
        await view.initialize_permissions_and_alliances()
        
        # Handle both regular and deferred interactions
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view, attachments=[])
        else:
            await interaction.response.edit_message(embed=embed, view=view, attachments=[])

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
                    cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY alliance_id")
                    alliances = cursor.fetchall()
                    return alliances, [], True
            
            # Non-global admin - get only alliances they've been assigned to
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("""
                    SELECT alliances_id 
                    FROM adminserver 
                    WHERE admin = ?
                """, (user_id,))
                admin_alliance_ids = cursor.fetchall()
                
            if admin_alliance_ids:
                # Validate that all alliance IDs are integers to prevent SQL injection
                validated_ids = []
                for aid_tuple in admin_alliance_ids:
                    if isinstance(aid_tuple[0], int):
                        validated_ids.append(aid_tuple[0])
                    else:
                        pass
                
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
                    alliances = cursor.fetchall()
                    return alliances, alliances, False
            
            return [], [], False
                
        except Exception as e:
            return [], [], False

    async def show_alliance_selection_for_marking(self, interaction: discord.Interaction):
        """Show alliance selection specifically for marking attendance"""
        try:
            # Check if used in a server context
            if interaction.guild is None:
                error_embed = self._create_error_embed(
                    "❌ Error",
                    "This command can only be used in a server, not in DMs."
                )
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
                return
                
            # Get admin permissions
            admin_result = await self._check_admin_permissions(interaction.user.id)
            if not admin_result:
                error_embed = self._create_error_embed(
                    "❌ Access Denied", 
                    "You do not have permission to use this command."
                )
                back_view = self._create_back_view(lambda i: self.show_attendance_menu(i))
                await interaction.response.edit_message(embed=error_embed, view=back_view)
                return
            
            is_initial = admin_result[0]
            
            # Get alliances based on permissions
            if is_initial == 1:
                # Global admin - get all alliances
                with sqlite3.connect('db/alliance.sqlite') as db:
                    cursor = db.cursor()
                    cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY alliance_id")
                    alliances = cursor.fetchall()
            else:
                # Non-global admin - get alliances from adminserver
                with sqlite3.connect('db/settings.sqlite') as settings_db:
                    cursor = settings_db.cursor()
                    cursor.execute("SELECT alliances_id FROM adminserver WHERE admin = ?", (interaction.user.id,))
                    special_permissions_raw = cursor.fetchall()
                    allowed_alliances = set(row[0] for row in special_permissions_raw)
                
                if allowed_alliances:
                    with sqlite3.connect('db/alliance.sqlite') as db:
                        cursor = db.cursor()
                        placeholders = ','.join('?' * len(allowed_alliances))
                        cursor.execute(f"SELECT alliance_id, name FROM alliance_list WHERE alliance_id IN ({placeholders}) ORDER BY alliance_id", 
                                     list(allowed_alliances))
                        alliances = cursor.fetchall()
                else:
                    alliances = []
            
            if not alliances:
                error_embed = self._create_error_embed(
                    "❌ No Alliances Found",
                    "No alliances found for your permissions."
                )
                back_view = self._create_back_view(lambda i: self.show_attendance_menu(i))
                await interaction.response.edit_message(embed=error_embed, view=back_view)
                return
            
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

            # Get alliance member counts
            alliance_ids = [a[0] for a in alliances]
            alliances_with_counts = []
            
            if alliance_ids:
                with sqlite3.connect('db/users.sqlite') as db:
                    cursor = db.cursor()
                    placeholders = ','.join('?' * len(alliance_ids))
                    cursor.execute(f"""
                        SELECT alliance, COUNT(*) 
                        FROM users 
                        WHERE alliance IN ({placeholders}) 
                        GROUP BY alliance
                    """, [str(aid) for aid in alliance_ids])
                    counts = dict(cursor.fetchall())
                
                alliances_with_counts = [
                    (aid, name, counts.get(str(aid), 0))
                    for aid, name in alliances
                ]
            
            view = AllianceSelectView(alliances_with_counts, self, is_marking=True)
            await interaction.response.edit_message(embed=select_embed, view=view)
            
        except Exception as e:
            error_embed = self._create_error_embed(
                "❌ Error", 
                "An error occurred while showing alliance selection."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    async def show_session_selection_for_marking(self, interaction: discord.Interaction, alliance_id: int):
        """Show available sessions for marking/editing attendance"""
        try:
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Query database for sessions of this alliance from single table
            sessions = []
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT 
                        session_id,
                        session_name,
                        event_type,
                        MIN(event_date) as session_date,
                        COUNT(DISTINCT player_id) as player_count,
                        SUM(CASE WHEN status != 'not_recorded' THEN 1 ELSE 0 END) as marked_count
                    FROM attendance_records
                    WHERE alliance_id = ?
                    GROUP BY session_id
                    ORDER BY session_date DESC
                """, (str(alliance_id),))
                raw_sessions = cursor.fetchall()
                
                # Convert tuples to dictionaries for SessionSelectView
                sessions = [
                    {
                        'session_id': row[0],
                        'name': row[1],
                        'event_type': row[2],
                        'date': row[3].split('T')[0] if row[3] else "Unknown",
                        'player_count': row[4],
                        'marked_count': row[5]
                    }
                    for row in raw_sessions
                ]

            # Create session selection view with new session option
            if sessions:
                description = (
                    "Please select an existing session or create a new one:\n\n"
                    f"**Alliance:** {alliance_name}\n"
                    f"**Available Sessions:** {len(sessions)}\n\n"
                    "Sessions are sorted by date (newest first)."
                )
            else:
                description = (
                    f"**Alliance:** {alliance_name}\n"
                    f"**Available Sessions:** No sessions found\n\n"
                    "Click the **New Session** button below to create your first attendance session for this alliance."
                )
            
            embed = discord.Embed(
                title=f"📋 Mark Attendance - {alliance_name}",
                description=description,
                color=discord.Color.blue()
            )

            view = SessionSelectView(sessions, alliance_id, self, is_viewing=False)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            error_embed = self._create_error_embed(
                "❌ Error",
                "An error occurred while loading sessions."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    async def show_attendance_marking(self, interaction: discord.Interaction, alliance_id: int, alliance_name: str, session_name: str, session_id: int = None, is_edit: bool = False, event_type: str = "Other", event_date: datetime = None):
        """Show attendance marking interface with status display"""
        try:
            # Get all alliance members
            players = []
            attendance_records = {}
            
            with sqlite3.connect('db/users.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv 
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY furnace_lv DESC, nickname
                """, (alliance_id,))
                alliance_members = cursor.fetchall()
            
            # If editing existing session, get attendance records
            if is_edit and session_id:
                with sqlite3.connect('db/attendance.sqlite') as db:
                    cursor = db.cursor()
                    cursor.execute("""
                        SELECT player_id, status, points, event_type, event_date
                        FROM attendance_records
                        WHERE session_id = ?
                    """, (session_id,))
                    
                    for record in cursor.fetchall():
                        attendance_records[int(record[0])] = {
                            'status': record[1],
                            'points': record[2]
                        }
                        # Get event type and date from first record
                        if not event_type or event_type == "Other":
                            event_type = record[3]
                        if not event_date and record[4]:
                            try:
                                event_date = datetime.fromisoformat(record[4])
                            except:
                                pass
            
            # Combine member data with attendance status
            for fid, nickname, furnace_lv in alliance_members:
                status = 'not_recorded'
                points = 0
                
                if fid in attendance_records:
                    status = attendance_records[fid]['status']
                    points = attendance_records[fid]['points']
                
                players.append((fid, nickname, furnace_lv, status, points))
            
            if not players:
                error_embed = discord.Embed(
                    title="❌ No Members Found",
                    description="This alliance has no members.",
                    color=discord.Color.red()
                )
                back_view = self._create_back_view(lambda i: self.show_session_selection_for_marking(i, alliance_id))
                if interaction.response.is_done():
                    await interaction.edit_original_response(embed=error_embed, view=back_view)
                else:
                    await interaction.response.edit_message(embed=error_embed, view=back_view)
                return
            
            # Calculate counts
            present_count = sum(1 for p in players if p[3] == 'present')
            absent_count = sum(1 for p in players if p[3] == 'absent')
            not_recorded_count = sum(1 for p in players if p[3] == 'not_recorded')
            
            event_icon = EVENT_TYPE_ICONS.get(event_type, "📋")
            embed = discord.Embed(
                title=f"📋 Mark Attendance - {alliance_name}",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Event Type:** {event_icon} {event_type}\n"
                    f"**Mode:** {'Edit Existing' if is_edit else 'New Session'}\n"
                    f"**Total Members:** {len(players)}\n"
                    f"**Status:** ✅ Present: {present_count} | ❌ Absent: {absent_count} | ❓ Not Recorded: {not_recorded_count}\n\n"
                    "Select players to mark their attendance:"
                ),
                color=discord.Color.blue()
            )
            
            view = PlayerSelectView(players, alliance_name, session_name, self, alliance_id, session_id, is_edit, event_type=event_type, event_date=event_date)
            
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.response.edit_message(embed=embed, view=view)
            
        except Exception as e:
            error_embed = self._create_error_embed(
                "❌ Error",
                "An error occurred while loading the attendance interface."
            )
            await interaction.response.edit_message(embed=error_embed, view=None)

    async def process_attendance_results(self, interaction: discord.Interaction, selected_players: dict, alliance_name: str, session_name: str, use_defer: bool = True, session_id: int = None, is_edit: bool = False, event_type: str = "Other", event_date: datetime = None, alliance_id: int = None):
        """Process and display final attendance results"""
        try:
            # Count attendance types
            present_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'present')
            absent_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'absent')
            not_recorded_count = sum(1 for p in selected_players.values() if p['attendance_type'] == 'not_recorded')

            # Create new session ID if not editing
            if not is_edit:
                # Create new session
                session_id = str(uuid.uuid4())

            # Save attendance records
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                
                # First, if creating new session, insert all players as not_recorded
                if not is_edit:
                    # Get all alliance members
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        users_cursor = users_db.cursor()
                        users_cursor.execute("""
                            SELECT fid, nickname, furnace_lv 
                            FROM users 
                            WHERE alliance = ? 
                            ORDER BY nickname
                        """, (alliance_id,))
                        all_members = users_cursor.fetchall()
                    
                    # Insert all members as not_recorded initially
                    for member in all_members:
                        member_fid, member_nickname, member_furnace_lv = member
                        cursor.execute("""
                            INSERT INTO attendance_records 
                            (player_id, player_name, session_id, session_name, alliance_id, alliance_name,
                             status, points, event_type, event_date, 
                             marked_at, marked_by, marked_by_username)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            str(member_fid), member_nickname, session_id, session_name, 
                            str(alliance_id), alliance_name,
                            'not_recorded', 0, 
                            event_type, 
                            event_date.isoformat() if event_date else datetime.utcnow().isoformat(),
                            datetime.utcnow().isoformat(), 
                            str(interaction.user.id), interaction.user.name
                        ))
                
                # Now update with actual attendance data
                for fid, player_data in selected_players.items():
                    if player_data['attendance_type'] != 'not_recorded':
                        # First check if the player exists in attendance_records
                        cursor.execute("""
                            SELECT COUNT(*) FROM attendance_records
                            WHERE player_id = ? AND session_id = ?
                        """, (str(fid), session_id))
                        
                        exists = cursor.fetchone()[0] > 0
                        
                        if exists:
                            # Update the record with actual attendance
                            cursor.execute("""
                                UPDATE attendance_records 
                                SET status = ?, points = ?, marked_at = ?
                                WHERE player_id = ? AND session_id = ?
                            """, (
                                player_data['attendance_type'], 
                                player_data['points'],
                                datetime.utcnow().isoformat(),
                                str(fid), 
                                session_id
                            ))
                        else:
                            # Player doesn't exist (newly added to alliance), insert them
                            cursor.execute("""
                                INSERT INTO attendance_records 
                                (player_id, player_name, session_id, session_name, alliance_id, alliance_name,
                                 status, points, event_type, event_date, 
                                 marked_at, marked_by, marked_by_username)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                str(fid), player_data['nickname'], session_id, session_name, 
                                str(alliance_id), alliance_name,
                                player_data['attendance_type'], player_data['points'], 
                                event_type, 
                                event_date.isoformat() if event_date else datetime.utcnow().isoformat(),
                                datetime.utcnow().isoformat(), 
                                str(interaction.user.id), interaction.user.name
                            ))
                
                db.commit()
            
            # Show completion report based on user preference
            if hasattr(interaction, 'guild') and interaction.guild:
                pass

            # Calculate total players
            if not is_edit:
                total_players = len(all_members)
            else:
                # For edit mode, get total count from the database
                with sqlite3.connect('db/users.sqlite') as users_db:
                    users_cursor = users_db.cursor()
                    users_cursor.execute("""
                        SELECT COUNT(*) FROM users WHERE alliance = ?
                    """, (alliance_id,))
                    total_players = users_cursor.fetchone()[0]
            
            actual_not_recorded = total_players - present_count - absent_count
            
            # Format event date
            event_date_str = "Not set"
            if event_date:
                try:
                    if isinstance(event_date, str):
                        event_date_obj = datetime.fromisoformat(event_date.replace('Z', '+00:00'))
                    else:
                        event_date_obj = event_date
                    event_date_str = event_date_obj.strftime('%Y-%m-%d %H:%M')
                except:
                    event_date_str = str(event_date)
            
            # Show simple success message
            success_embed = discord.Embed(
                title="✅ Attendance Saved Successfully",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Alliance:** {alliance_name}\n"
                    f"**Event Type:** {event_type}\n"
                    f"**Event Date:** {event_date_str}\n\n"
                    f"**Summary:**\n"
                    f"✅ Present: {present_count}\n"
                    f"❌ Absent: {absent_count}\n"
                    f"⚪ Not Recorded: {actual_not_recorded}\n"
                    f"**Total Players:** {total_players}"
                ),
                color=discord.Color.green()
            )
            success_embed.set_footer(text=f"Marked by {interaction.user.name}")
            
            # Create a simple back button
            back_view = self._create_back_view(lambda i: self.show_attendance_menu(i))
            
            # Update the original message
            if use_defer:
                await interaction.edit_original_response(embed=success_embed, view=back_view)
            else:
                await interaction.response.edit_message(embed=success_embed, view=back_view)

        except Exception as e:
            print(f"ERROR in process_attendance_results: {e}")
            import traceback
            traceback.print_exc()
            error_embed = discord.Embed(
                title="❌ Error",
                description=f"An error occurred while generating the attendance report: {str(e)}",
                color=discord.Color.red()
            )
            
            if use_defer:
                await interaction.edit_original_response(embed=error_embed, view=None)
            else:
                await interaction.response.edit_message(embed=error_embed, view=None)

class SessionSelectView(discord.ui.View):
    """Unified session select view for both marking and viewing"""
    def __init__(self, sessions, alliance_id, cog, is_viewing=False):
        super().__init__(timeout=7200)
        self.sessions = sessions
        self.alliance_id = alliance_id
        self.cog = cog
        self.is_viewing = is_viewing
 
        # Add dropdown for session selection only if there are sessions
        if sessions:
            options = []
            for session in sessions[:25]:  # Discord limit
                event_icon = EVENT_TYPE_ICONS.get(session.get('event_type', 'Other'), '📋')
                options.append(discord.SelectOption(
                    label=f"{session['name'][:90]} [{session.get('event_type', 'Other')}]",
                    value=str(session['session_id']),
                    description=f"{session.get('date', 'Unknown date')} - {session.get('marked_count', 0)}/{session.get('player_count', 0)} marked",
                    emoji=event_icon
                ))
            
            select = discord.ui.Select(
                placeholder="📋 Select a session...",
                options=options
            )
            select.callback = lambda interaction: self.on_select(interaction)
            self.add_item(select)
        
        # New Session button (only for marking mode)
        if not self.is_viewing:
            new_session_button = discord.ui.Button(
                label="New Session",
                style=discord.ButtonStyle.primary,
                emoji="➕",
                row=1
            )
            new_session_button.callback = self.new_session_callback
            self.add_item(new_session_button)
        
        # Back button (always shown)
        back_button = discord.ui.Button(
            label="⬅️ Back",
            style=discord.ButtonStyle.secondary,
            row=1
        )
        back_button.callback = self.back_button_callback
        self.add_item(back_button)
    
    async def new_session_callback(self, interaction: discord.Interaction):
        """Create a new session"""
        await interaction.response.send_modal(SessionNameModal(self.cog, self.alliance_id))

    async def back_button_callback(self, interaction: discord.Interaction):
        """Go back to appropriate menu"""
        if self.is_viewing:
            # For viewing mode, go back to attendance menu
            attendance_cog = self.cog.bot.get_cog("Attendance")
            if attendance_cog:
                await attendance_cog.show_attendance_menu(interaction)
        else:
            # For marking mode, go back to alliance selection
            await self.cog.show_alliance_selection_for_marking(interaction)
 
    async def on_select(self, interaction: discord.Interaction):
        """Handle session selection"""
        try:
            await interaction.response.defer()
            
            session_id = interaction.data['values'][0]
            # Find the selected session
            selected_session = None
            for session in self.sessions:
                if session['session_id'] == session_id:
                    selected_session = session
                    break
                    
            if selected_session:
                if self.is_viewing:
                    # For viewing mode, show the report
                    report_cog = self.cog.bot.get_cog("AttendanceReport")
                    if report_cog:
                        await report_cog.show_attendance_report(
                            interaction,
                            self.alliance_id,
                            selected_session['name'],
                            session_id=session_id
                        )
                else:
                    # For marking mode, show attendance marking
                    await self.cog.show_attendance_marking(
                        interaction, 
                        self.alliance_id, 
                        await self.cog._get_alliance_name(self.alliance_id),
                        selected_session['name'], 
                        session_id=session_id,
                        is_edit=True
                    )
            else:
                await interaction.edit_original_response(
                    content="❌ Session not found.",
                    embed=None,
                    view=None
                )
        except Exception as e:
            await interaction.edit_original_response(
                content="❌ An error occurred while loading the session.",
                embed=None,
                view=None
            )

class SessionNameModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id):
        super().__init__(title="Create New Session")
        self.cog = cog
        self.alliance_id = alliance_id
        
        self.session_name = discord.ui.TextInput(
            label="Session Name",
            placeholder="Enter session name (e.g., 'Bear Tuesday', 'Canyon Sunday')",
            min_length=1,
            max_length=100,
            required=True
        )
        self.add_item(self.session_name)
    
    async def on_submit(self, interaction: discord.Interaction):
        session_name = self.session_name.value.strip()
        alliance_name = await self.cog._get_alliance_name(self.alliance_id)
        
        await self.cog.show_attendance_marking(
            interaction,
            self.alliance_id,
            alliance_name,
            session_name,
            is_edit=False
        )
async def setup(bot):
    try:
        cog = Attendance(bot)
        await bot.add_cog(cog)
    except Exception as e:
        print(f"❌ Failed to load Attendance cog: {e}")