import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from datetime import datetime
import asyncio
import re
import os
import plotly.graph_objects as go
import pandas as pd
import uuid

# FC Level mapping for furnace levels
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

def parse_points(points_str):
    """Parse points string that may contain K/M suffixes"""
    try:
        points_str = points_str.strip().upper()
        
        # Remove any commas
        points_str = points_str.replace(',', '')
        
        # Check for M suffix (millions)
        if points_str.endswith('M'):
            number = float(points_str[:-1])
            return int(number * 1_000_000)
        
        # Check for K suffix (thousands)
        elif points_str.endswith('K'):
            number = float(points_str[:-1])
            return int(number * 1_000)
        
        # Regular number
        else:
            return int(float(points_str))
            
    except (ValueError, TypeError):
        raise ValueError("Invalid points format")

class AttendanceView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(
        label="Mark Attendance",
        emoji="📋",
        style=discord.ButtonStyle.primary,
        custom_id="mark_attendance"
    )
    async def mark_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user is admin
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
                admin_result = cursor.fetchone()
                
                if not admin_result:
                    await interaction.response.send_message(
                        "❌ You do not have permission to use this command.", 
                        ephemeral=True
                    )
                    return
                    
                is_initial = admin_result[0]

            # Get available alliances
            alliances, special_alliances, is_global = await self.cog.get_admin_alliances(
                interaction.user.id, 
                interaction.guild_id
            )
            
            if not alliances:
                await interaction.response.send_message(
                    "❌ No alliances found for your permissions.", 
                    ephemeral=True
                )
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
            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            view = AllianceSelectView(alliances_with_counts, self.cog, is_marking=True)
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"Error in mark_attendance_button: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while processing your request.", 
                ephemeral=True
            )

    @discord.ui.button(
        label="View Attendance",
        emoji="👀",
        style=discord.ButtonStyle.secondary,
        custom_id="view_attendance"
    )
    async def view_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Check if user is admin
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
                admin_result = cursor.fetchone()
                
                if not admin_result:
                    await interaction.response.send_message(
                        "❌ You do not have permission to use this command.", 
                        ephemeral=True
                    )
                    return

            # Get available alliances
            alliances, special_alliances, is_global = await self.cog.get_admin_alliances(
                interaction.user.id, 
                interaction.guild_id
            )
            
            if not alliances:
                await interaction.response.send_message(
                    "❌ No alliances found for your permissions.", 
                    ephemeral=True
                )
                return

            # Create alliance selection for viewing
            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            view = AllianceSelectView(alliances_with_counts, self.cog, is_marking=False)
            
            select_embed = discord.Embed(
                title="👀 View Attendance - Alliance Selection",
                description="Please select an alliance to view attendance records:",
                color=discord.Color.green()
            )
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"Error in view_attendance_button: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while processing your request.", 
                ephemeral=True
            )

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
            await interaction.response.send_message(
                "❌ Session name cannot be empty.",
                ephemeral=True
            )
            return
            
        await self.cog.show_attendance_marking(
            interaction, 
            self.alliance_id,
            session_name
        )

class AllianceSelectView(discord.ui.View):
    def __init__(self, alliances_with_counts, cog, page=0, is_marking=False):
        super().__init__(timeout=180)
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

        if hasattr(self, 'prev_button'):
            self.prev_button.disabled = self.page == 0
        if hasattr(self, 'next_button'):
            self.next_button.disabled = self.page == self.max_page

    @discord.ui.button(label="◀️", style=discord.ButtonStyle.secondary)
    async def prev_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(label="▶️", style=discord.ButtonStyle.secondary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self.update_select_menu()
        await interaction.response.edit_message(view=self)

class PlayerSelectView(discord.ui.View):
    def __init__(self, players, alliance_name, session_name, cog, page=0):
        super().__init__(timeout=300)
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
        if hasattr(self, 'prev_button'):
            self.prev_button.disabled = self.page == 0
        if hasattr(self, 'next_button'):
            self.next_button.disabled = self.page == self.max_page

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
            await interaction.response.send_message(
                "❌ No attendance has been marked yet.",
                ephemeral=True
            )
            return
        
        await self.show_summary(interaction)

    @discord.ui.button(label="✅ Finish Attendance", style=discord.ButtonStyle.success, row=1)
    async def finish_attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_players:
            await interaction.response.send_message(
                "❌ No attendance has been marked yet.",
                ephemeral=True
            )
            return
        
        await self.cog.process_attendance_results(interaction, self.selected_players, self.alliance_name, self.session_name)

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
        summary_lines = ["```"]
        summary_lines.append("PLAYER | STATUS | POINTS")
        summary_lines.append("-" * 40)
        
        for fid, data in self.selected_players.items():
            status_display = {
                "present": "Present",
                "absent": "Absent", 
                "not_signed": "Not Signed"
            }.get(data['attendance_type'], data['attendance_type'])
            
            line = f"{data['nickname'][:15]:<15} | {status_display:<10} | {data['points']:,}"
            summary_lines.append(line)
        
        summary_lines.append("```")
        
        embed = discord.Embed(
            title=f"📊 Attendance Summary - {self.alliance_name}",
            description=f"**Session:** {self.session_name}\n\n" + "\n".join(summary_lines),
            color=discord.Color.green()
        )
        
        # Add back button
        back_view = discord.ui.View(timeout=180)
        back_button = discord.ui.Button(
            label="⬅️ Back to Selection",
            style=discord.ButtonStyle.secondary
        )
        
        async def back_callback(back_interaction: discord.Interaction):
            await self.update_main_embed(back_interaction)
        
        back_button.callback = back_callback
        back_view.add_item(back_button)
        
        await interaction.response.edit_message(embed=embed, view=back_view)

    def add_player_attendance(self, fid, nickname, attendance_type, points, last_event_attendance):
        self.selected_players[fid] = {
            'nickname': nickname,
            'attendance_type': attendance_type,
            'points': points,
            'last_event_attendance': last_event_attendance
        }

class PlayerAttendanceView(discord.ui.View):
    def __init__(self, player, parent_view):
        super().__init__(timeout=300)
        self.player = player
        self.parent_view = parent_view
        self.fid, self.nickname, self.furnace_lv = player

    @discord.ui.button(
        label="Present",
        style=discord.ButtonStyle.success,
        custom_id="present"
    )
    async def present_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AttendanceModal(self.fid, self.nickname, "present", self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Absent", 
        style=discord.ButtonStyle.danger,
        custom_id="absent"
    )
    async def absent_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AttendanceModal(self.fid, self.nickname, "absent", self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Not Signed",
        style=discord.ButtonStyle.secondary,
        custom_id="not_signed"
    )
    async def not_signed_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = AttendanceModal(self.fid, self.nickname, "not_signed", self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="⬅️ Back to List",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_list"
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.parent_view.update_main_embed(interaction)

class AttendanceModal(discord.ui.Modal):
    def __init__(self, fid, nickname, attendance_type, parent_view):
        super().__init__(title=f"Attendance Details - {nickname}")
        self.fid = fid
        self.nickname = nickname
        self.attendance_type = attendance_type  # "present", "absent", or "not_signed"
        self.parent_view = parent_view
        
        self.points_input = discord.ui.TextInput(
            label="Points",
            placeholder="Enter points (e.g., 100, 4.3K, 2.5M)",
            required=True,
            max_length=15
        )
        self.add_item(self.points_input)
        
        self.last_event_input = discord.ui.TextInput(
            label="Last Event Attendance",
            placeholder="Enter last event attendance (e.g., Present/Absent)",
            required=True,
            max_length=50
        )
        self.add_item(self.last_event_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse points using the new function
            try:
                points = parse_points(self.points_input.value.strip())
            except ValueError as e:
                await interaction.response.send_message(
                    "❌ Invalid points format. Use numbers, K (thousands), or M (millions). Example: 100, 4.3K, 2.5M",
                    ephemeral=True
                )
                return
                
            last_event_attendance = self.last_event_input.value.strip()
            
            # Store attendance data in dedicated attendance database
            try:
                with sqlite3.connect('db/attendance.sqlite', timeout=10.0) as attendance_db:
                    cursor = attendance_db.cursor()
                    
                    # Get user alliance info
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        user_cursor = users_db.cursor()
                        user_cursor.execute("SELECT alliance FROM users WHERE fid = ?", (self.fid,))
                        user_result = user_cursor.fetchone()
                        
                        if not user_result:
                            raise ValueError(f"User with FID {self.fid} not found in database")
                        
                        alliance_id = user_result[0]
                        
                        # Get alliance name
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            alliance_cursor = alliance_db.cursor()
                            alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                            alliance_result = alliance_cursor.fetchone()
                            alliance_name = alliance_result[0] if alliance_result else "Unknown Alliance"
                    
                    # Insert attendance record with session name
                    cursor.execute("""
                        INSERT INTO attendance_records 
                        (fid, nickname, alliance_id, alliance_name, attendance_status, points, 
                         last_event_attendance, marked_date, marked_by, marked_by_username, session_name)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (self.fid, self.nickname, alliance_id, alliance_name, self.attendance_type, 
                          points, last_event_attendance, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
                          interaction.user.id, interaction.user.name, self.parent_view.session_name))
                    
                    attendance_db.commit()
                    print(f"✓ Attendance saved for {self.nickname} (FID: {self.fid}) in dedicated database")
                
                # Add player attendance to parent view
                self.parent_view.add_player_attendance(self.fid, self.nickname, self.attendance_type, points, last_event_attendance)
                
                # Show success message with back button
                success_embed = discord.Embed(
                    title="✅ Attendance Marked Successfully",
                    description=(
                        f"**Player:** {self.nickname}\n"
                        f"**Status:** {self.attendance_type.replace('_', ' ').title()}\n"
                        f"**Points:** {points:,}\n"
                        f"**Last Event:** {last_event_attendance}\n"
                        f"**Session:** {self.parent_view.session_name}\n\n"
                        "Click the button below to return to player selection."
                    ),
                    color=discord.Color.green()
                )
                
                # Create a view with a back button
                back_view = discord.ui.View(timeout=180)
                back_button = discord.ui.Button(
                    label="⬅️ Back to Player Selection",
                    style=discord.ButtonStyle.primary
                )
                
                async def back_callback(back_interaction: discord.Interaction):
                    await self.parent_view.update_main_embed(back_interaction)
                
                back_button.callback = back_callback
                back_view.add_item(back_button)
                
                await interaction.response.edit_message(embed=success_embed, view=back_view)
                
            except ValueError as val_error:
                print(f"Validation error in attendance modal: {val_error}")
                await interaction.response.send_message(
                    f"❌ {str(val_error)}",
                    ephemeral=True
                )
            except sqlite3.Error as db_error:
                print(f"Database error in attendance modal: {db_error}")
                error_msg = "❌ Database error occurred while saving attendance."
                if "locked" in str(db_error).lower():
                    error_msg += " Database is busy, please try again in a moment."
                elif "no such table" in str(db_error).lower():
                    error_msg += " Database table not found, please contact an administrator."
                else:
                    error_msg += " Please try again."
                
                await interaction.response.send_message(error_msg, ephemeral=True)
            except Exception as save_error:
                print(f"Save error in attendance modal: {save_error}")
                await interaction.response.send_message(
                    f"❌ An error occurred while saving attendance: {str(save_error)[:100]}",
                    ephemeral=True
                )
                
        except Exception as e:
            print(f"General error in attendance modal: {e}")
            import traceback
            traceback.print_exc()
            
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred while processing attendance.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "❌ An error occurred while processing attendance.",
                    ephemeral=True
                )

class SessionSelectView(discord.ui.View):
    def __init__(self, sessions, alliance_id, cog):
        super().__init__(timeout=180)
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
        
    async def on_select(self, interaction: discord.Interaction):
        session_name = interaction.data['values'][0]
        await self.cog.show_attendance_report(interaction, self.alliance_id, session_name)

class Attendance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.setup_database()

    def setup_database(self):
        """Set up dedicated attendance database"""
        try:
            # Create attendance database if it doesn't exist
            if not os.path.exists("db/attendance.sqlite"):
                open("db/attendance.sqlite", 'a').close()
                print("✓ Created new attendance database")
            
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
                
                attendance_db.commit()
                print("✓ Attendance database setup completed")
                
        except Exception as e:
            print(f"Error setting up attendance database: {e}")

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
                with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                    cursor = alliance_db.cursor()
                    placeholders = ','.join('?' * len(special_alliance_ids))
                    cursor.execute(f"""
                        SELECT DISTINCT alliance_id, name
                        FROM alliance_list
                        WHERE alliance_id IN ({placeholders})
                        ORDER BY name
                    """, [aid[0] for aid in special_alliance_ids])
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

    async def process_attendance_results(self, interaction: discord.Interaction, selected_players: dict, alliance_name: str, session_name: str):
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
            
            # Generate attendance report
            report_lines = ["```"]
            report_lines.append("PLAYER         | ATTENDANCE   | LAST ATTENDANCE | POINTS")
            report_lines.append("-" * 60)
            
            for fid, data in selected_players.items():
                if data['attendance_type'] == "present":
                    attendance_status = "Present"
                elif data['attendance_type'] == "absent":
                    attendance_status = "Absent"
                else:  # not_signed
                    attendance_status = "Not Signed"
                    
                line = f"{data['nickname'][:12]:<14} | {attendance_status:<12} | {data['last_event_attendance'][:15]:<15} | {data['points']:,}"
                report_lines.append(line)
            
            report_lines.append("```")
            
            # Create final embed
            embed = discord.Embed(
                title=f"✅ Attendance Report - {alliance_name}",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"**Session ID:** {session_id if session_id else 'N/A'}\n"
                    f"**Total Players:** {len(selected_players)}\n"
                    f"**Present:** {present_count}\n"
                    f"**Absent:** {absent_count}\n"
                    f"**Not Signed:** {not_signed_count}\n\n"
                    "**Attendance Details:**\n"
                    "\n".join(report_lines)
                ),
                color=discord.Color.green()
            )
            
            embed.set_footer(text=f"Marked by {interaction.user.name} | Saved to database")
            
            await interaction.response.edit_message(embed=embed, view=None)

        except Exception as e:
            print(f"Error processing attendance results: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while generating the attendance report.",
                ephemeral=True
            )

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
                await interaction.response.edit_message(
                    content=f"❌ No attendance sessions found for {alliance_name}.",
                    embed=None,
                    view=None
                )
                return
        
            # Create session selection view
            view = SessionSelectView(sessions, alliance_id, self)
            
            embed = discord.Embed(
                title=f"📋 Attendance Sessions - {alliance_name}",
                description="Please select a session to view attendance records:",
                color=discord.Color.blue()
            )
            
            await interaction.response.edit_message(embed=embed, view=view)
    
        except Exception as e:
            print(f"Error showing session selection: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading sessions.",
                ephemeral=True
            )

    async def show_attendance_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show attendance records for a specific session as a Plotly table image"""
        try:
            # Get alliance name
            alliance_name = "Unknown Alliance"
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = cursor.fetchone()
                if alliance_result:
                    alliance_name = alliance_result[0]

            # Get attendance records
            records = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                cursor.execute("""
                    SELECT nickname, attendance_status, last_event_attendance, points, marked_date, marked_by_username
                    FROM attendance_records
                    WHERE alliance_id = ? AND session_name = ?
                    ORDER BY marked_date DESC
                """, (alliance_id, session_name))
                records = cursor.fetchall()

            if not records:
                await interaction.response.edit_message(
                    content=f"❌ No attendance records found for session '{session_name}' in {alliance_name}.",
                    embed=None,
                    view=None
                )
                return

            # Generate Plotly table
            try:
                # Prepare data
                players = []
                statuses = []
                points = []
                last_events = []
                marked_dates = []
                marked_bys = []
                
                for row in records:
                    players.append(row[0] or "Unknown")
                    statuses.append(row[1].replace('_', ' ').title())
                    last_events.append(row[2] or "N/A")
                    points.append(f"{row[3]:,}" if row[3] else "0")
                    marked_dates.append(row[4].split()[0] if row[4] else "N/A")
                    marked_bys.append(row[5] or "Unknown")
                
                # Create Plotly table
                fig = go.Figure(data=[go.Table(
                    header=dict(
                        values=['<b>Player</b>', '<b>Status</b>', '<b>Last Event</b>', '<b>Points</b>', '<b>Date</b>', '<b>Marked By</b>'],
                        fill_color='#1f77b4',  # Discord blue
                        font=dict(color='white', size=14),
                        align='left'
                    ),
                    cells=dict(
                        values=[players, statuses, last_events, points, marked_dates, marked_bys],
                        fill_color='#f0f8ff',  # Light blue
                        align='left',
                        font=dict(size=12)
                    )
                )])
                
                # Update layout
                fig.update_layout(
                    title=f'Attendance Report - {alliance_name} | Session: {session_name}',
                    title_font_size=20,
                    margin=dict(l=20, r=20, t=80, b=20),
                    height=600 + len(records) * 30  # Dynamic height based on rows
                )
                
                # Generate unique filename
                filename = f"attendance_{uuid.uuid4().hex}.png"
                fig.write_image(filename, scale=2)
                
                # Create Discord file
                file = discord.File(filename, filename="attendance_report.png")
                
                # Create embed
                embed = discord.Embed(
                    title=f"📊 Attendance Report - {alliance_name}",
                    description=f"**Session:** {session_name}\n**Total Players:** {len(records)}",
                    color=discord.Color.blue()
                )
                embed.set_image(url="attachment://attendance_report.png")
                
                # Add session ID if available
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
                
                if session_id:
                    embed.set_footer(text=f"Session ID: {session_id}")
                
                # Send response with image
                await interaction.response.edit_message(
                    content=None,
                    embed=embed,
                    view=None,
                    attachments=[file]
                )
                
                # Clean up temporary file
                os.remove(filename)
                
            except ImportError:
                # Fallback to text if Plotly not installed
                await self.fallback_text_report(interaction, records, alliance_name, session_name)
            except Exception as e:
                print(f"Plotly error: {e}")
                await self.fallback_text_report(interaction, records, alliance_name, session_name)

        except Exception as e:
            print(f"Error showing attendance report: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while generating attendance report.",
                ephemeral=True
            )

    async def fallback_text_report(self, interaction, records, alliance_name, session_name):
        """Fallback to text-based report if Plotly fails"""
        report_lines = ["```"]
        report_lines.append("PLAYER         | STATUS      | LAST EVENT  | POINTS      | DATE       | BY")
        report_lines.append("-" * 70)
        
        for row in records:
            nickname = row[0] or "Unknown"
            attendance_status = row[1] or "unknown"
            last_event = row[2] or "N/A"
            points = row[3] or 0
            marked_date = row[4] or "N/A"
            marked_by_username = row[5] or "Unknown"
            
            display_status = attendance_status.replace('_', ' ').title()[:10]
            last_event_display = last_event[:10]
            points_str = f"{points:,}"
            date_str = marked_date.split()[0] if marked_date else "N/A"
            by_str = marked_by_username[:10]
            
            line = f"{nickname[:12]:<14} | {display_status:<11} | {last_event_display:<11} | {points_str:<11} | {date_str} | {by_str}"
            report_lines.append(line)
        
        report_lines.append("```")

        embed = discord.Embed(
            title=f"📊 Attendance Report - {alliance_name}",
            description=(
                f"**Session:** {session_name}\n"
                f"**Total Players:** {len(records)}\n\n"
                "\n".join(report_lines)
            ),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=None)

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
        
        view = AttendanceView(self)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

async def setup(bot):
    try:
        cog = Attendance(bot)
        await bot.add_cog(cog)
        print("✓ Attendance cog loaded successfully")
    except Exception as e:
        print(f"❌ Failed to load Attendance cog: {e}")