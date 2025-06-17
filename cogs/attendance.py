
import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from datetime import datetime
import asyncio

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

            view = AllianceSelectView(alliances_with_counts, self.cog)
            
            async def select_callback(select_interaction: discord.Interaction):
                alliance_id = int(view.current_select.values[0])
                await self.cog.show_attendance_marking(select_interaction, alliance_id)

            view.callback = select_callback
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

            view = AllianceSelectView(alliances_with_counts, self.cog)
            
            async def select_callback(select_interaction: discord.Interaction):
                alliance_id = int(view.current_select.values[0])
                await self.cog.show_attendance_report(select_interaction, alliance_id)

            view.callback = select_callback
            
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

class AllianceSelectView(discord.ui.View):
    def __init__(self, alliances_with_counts, cog, page=0):
        super().__init__(timeout=180)
        self.alliances = alliances_with_counts
        self.cog = cog
        self.page = page
        self.max_page = (len(alliances_with_counts) - 1) // 25 if alliances_with_counts else 0
        self.current_select = None
        self.callback = None
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
            if self.callback:
                await self.callback(interaction)
        
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
    def __init__(self, players, alliance_name, cog):
        super().__init__(timeout=300)
        self.players = players
        self.alliance_name = alliance_name
        self.cog = cog
        self.selected_players = {}
        self.current_player_index = 0
        self.show_current_player()

    def show_current_player(self):
        self.clear_items()
        
        if self.current_player_index >= len(self.players):
            # All players processed, show summary
            self.add_item(discord.ui.Button(
                label="Finish Attendance",
                style=discord.ButtonStyle.success,
                custom_id="finish_attendance"
            ))
            return

        player = self.players[self.current_player_index]
        fid, nickname, furnace_lv = player

        # Add attendance buttons
        present_button = discord.ui.Button(
            label="Present",
            style=discord.ButtonStyle.success,
            custom_id=f"present_{fid}"
        )
        absent_button = discord.ui.Button(
            label="Absent", 
            style=discord.ButtonStyle.danger,
            custom_id=f"absent_{fid}"
        )
        
        async def button_callback(interaction: discord.Interaction):
            button_id = interaction.data["custom_id"]
            is_present = button_id.startswith("present_")
            
            if button_id == "finish_attendance":
                await self.cog.process_attendance_results(interaction, self.selected_players, self.alliance_name)
                return
            
            # Show points input modal
            modal = AttendanceModal(fid, nickname, is_present, self)
            await interaction.response.send_modal(modal)

        present_button.callback = button_callback
        absent_button.callback = button_callback
        
        self.add_item(present_button)
        self.add_item(absent_button)

    def next_player(self, fid, nickname, is_present, points, last_event_attendance):
        self.selected_players[fid] = {
            'nickname': nickname,
            'present': is_present,
            'points': points,
            'last_event_attendance': last_event_attendance
        }
        self.current_player_index += 1
        self.show_current_player()

class AttendanceModal(discord.ui.Modal):
    def __init__(self, fid, nickname, is_present, parent_view):
        super().__init__(title=f"Attendance Details - {nickname}")
        self.fid = fid
        self.nickname = nickname
        self.is_present = is_present
        self.parent_view = parent_view
        
        self.points_input = discord.ui.TextInput(
            label="Points",
            placeholder="Enter points (e.g., 100)",
            required=True,
            max_length=10
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
            points = int(self.points_input.value.strip())
            last_event_attendance = self.last_event_input.value.strip()
            
            # Store attendance data
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                
                # Check if attendance table exists, create if not
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attendance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fid INTEGER,
                        present INTEGER,
                        points INTEGER,
                        last_event_attendance TEXT,
                        marked_date TEXT,
                        marked_by INTEGER,
                        FOREIGN KEY (fid) REFERENCES users(fid)
                    )
                """)
                
                # Insert attendance record
                cursor.execute("""
                    INSERT INTO attendance (fid, present, points, last_event_attendance, marked_date, marked_by)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (self.fid, 1 if self.is_present else 0, points, last_event_attendance, 
                      datetime.now().strftime('%Y-%m-%d %H:%M:%S'), interaction.user.id))
                
                users_db.commit()
            
            # Move to next player
            self.parent_view.next_player(self.fid, self.nickname, self.is_present, points, last_event_attendance)
            
            # Update the message with next player
            if self.parent_view.current_player_index < len(self.parent_view.players):
                current_player = self.parent_view.players[self.parent_view.current_player_index]
                embed = discord.Embed(
                    title=f"📋 Marking Attendance - {self.parent_view.alliance_name}",
                    description=(
                        f"**Current Player:** {current_player[1]} (FID: {current_player[0]})\n"
                        f"**Progress:** {self.parent_view.current_player_index + 1}/{len(self.parent_view.players)}\n\n"
                        "Please mark attendance for this player:"
                    ),
                    color=discord.Color.blue()
                )
            else:
                embed = discord.Embed(
                    title="✅ All Players Processed",
                    description="Click 'Finish Attendance' to generate the final report.",
                    color=discord.Color.green()
                )
            
            await interaction.response.edit_message(embed=embed, view=self.parent_view)
            
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid points value. Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in attendance modal: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while saving attendance.",
                ephemeral=True
            )

class Attendance(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.setup_database()

    def setup_database(self):
        """Set up attendance table in users database"""
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS attendance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fid INTEGER,
                        present INTEGER,
                        points INTEGER,
                        last_event_attendance TEXT,
                        marked_date TEXT,
                        marked_by INTEGER,
                        FOREIGN KEY (fid) REFERENCES users(fid)
                    )
                """)
                users_db.commit()
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

    async def show_attendance_marking(self, interaction: discord.Interaction, alliance_id: int):
        """Show the attendance marking interface for selected alliance"""
        try:
            # Get alliance name
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = cursor.fetchone()
                
                if not alliance_result:
                    await interaction.response.send_message(
                        "❌ Alliance not found.",
                        ephemeral=True
                    )
                    return
                    
                alliance_name = alliance_result[0]

            # Get alliance members
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv 
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY nickname
                """, (alliance_id,))
                players = cursor.fetchall()

            if not players:
                await interaction.response.send_message(
                    f"❌ No players found in alliance {alliance_name}.",
                    ephemeral=True
                )
                return

            # Start attendance marking process
            first_player = players[0]
            embed = discord.Embed(
                title=f"📋 Marking Attendance - {alliance_name}",
                description=(
                    f"**Current Player:** {first_player[1]} (FID: {first_player[0]})\n"
                    f"**Progress:** 1/{len(players)}\n\n"
                    "Please mark attendance for this player:"
                ),
                color=discord.Color.blue()
            )

            view = PlayerSelectView(players, alliance_name, self)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"Error showing attendance marking: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading attendance marking.",
                ephemeral=True
            )

    async def process_attendance_results(self, interaction: discord.Interaction, selected_players: dict, alliance_name: str):
        """Process and display final attendance results"""
        try:
            # Generate attendance report
            report_lines = ["```"]
            report_lines.append("PLAYER | ATTENDANCE | LAST ATTENDANCE | POINTS")
            report_lines.append("-" * 60)
            
            for fid, data in selected_players.items():
                attendance_status = "Present" if data['present'] else "Absent"
                line = f"{data['nickname'][:15]:<15} | {attendance_status:<10} | {data['last_event_attendance'][:15]:<15} | {data['points']}"
                report_lines.append(line)
            
            report_lines.append("```")
            
            # Create final embed
            embed = discord.Embed(
                title=f"✅ Attendance Report - {alliance_name}",
                description=(
                    f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"**Total Players:** {len(selected_players)}\n"
                    f"**Present:** {sum(1 for p in selected_players.values() if p['present'])}\n"
                    f"**Absent:** {sum(1 for p in selected_players.values() if not p['present'])}\n\n"
                    "**Attendance Details:**\n"
                    "\n".join(report_lines)
                ),
                color=discord.Color.green()
            )
            
            embed.set_footer(text=f"Marked by {interaction.user.name}")
            
            await interaction.response.edit_message(embed=embed, view=None)

        except Exception as e:
            print(f"Error processing attendance results: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while generating the attendance report.",
                ephemeral=True
            )

    async def show_attendance_report(self, interaction: discord.Interaction, alliance_id: int):
        """Show attendance records for an alliance"""
        try:
            # Get alliance name
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_result = cursor.fetchone()
                
                if not alliance_result:
                    await interaction.response.send_message(
                        "❌ Alliance not found.",
                        ephemeral=True
                    )
                    return
                    
                alliance_name = alliance_result[0]

            # Get recent attendance records
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT u.nickname, a.present, a.last_event_attendance, a.points, a.marked_date
                    FROM attendance a
                    JOIN users u ON a.fid = u.fid
                    WHERE u.alliance = ?
                    ORDER BY a.marked_date DESC
                    LIMIT 50
                """, (alliance_id,))
                records = cursor.fetchall()

            if not records:
                await interaction.response.send_message(
                    f"❌ No attendance records found for alliance {alliance_name}.",
                    ephemeral=True
                )
                return

            # Generate report
            report_lines = ["```"]
            report_lines.append("PLAYER | ATTENDANCE | LAST ATTENDANCE | POINTS | DATE")
            report_lines.append("-" * 80)
            
            for nickname, present, last_event, points, marked_date in records[:20]:
                attendance_status = "Present" if present else "Absent"
                date_str = marked_date.split()[0] if marked_date else "N/A"
                line = f"{nickname[:12]:<12} | {attendance_status:<10} | {last_event[:10]:<10} | {points:<6} | {date_str}"
                report_lines.append(line)
            
            if len(records) > 20:
                report_lines.append(f"... and {len(records) - 20} more records")
            
            report_lines.append("```")

            embed = discord.Embed(
                title=f"📊 Attendance History - {alliance_name}",
                description=(
                    f"**Recent Attendance Records**\n"
                    f"**Total Records:** {len(records)}\n\n"
                    "\n".join(report_lines)
                ),
                color=discord.Color.blue()
            )

            await interaction.response.edit_message(embed=embed, view=None)

        except Exception as e:
            print(f"Error showing attendance report: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading attendance records.",
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
        
        view = AttendanceView(self)
        await interaction.response.edit_message(embed=embed, view=view)

async def setup(bot):
    await bot.add_cog(Attendance(bot))
