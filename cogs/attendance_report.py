import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import re
import csv
import io
from io import BytesIO
import os
from .attendance import SessionSelectView

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
    print("Matplotlib not available - using text attendance reports only")

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

EVENT_TYPE_ICONS = {
    "Foundry": "🏭",
    "Canyon Clash": "⚔️",
    "Crazy Joe": "🤪",
    "Bear Trap": "🐻",
    "Castle Battle": "🏰",
    "Frostdragon Tyrant": "🐉",
    "Other": "📋"
}

class ExportFormatSelectView(discord.ui.View):
    def __init__(self, cog, records, session_info):
        super().__init__(timeout=300)
        self.cog = cog
        self.records = records
        self.session_info = session_info
        
    @discord.ui.select(
        placeholder="Select export format...",
        options=[
            discord.SelectOption(label="CSV", value="csv", description="Comma-separated values", emoji="📄"),
            discord.SelectOption(label="TSV", value="tsv", description="Tab-separated values", emoji="📋"),
            discord.SelectOption(label="HTML", value="html", description="Web page format", emoji="🌐")
        ]
    )
    async def format_select(self, interaction: discord.Interaction, select: discord.ui.Select):
        await self.cog.process_export(interaction, select.values[0], self.records, self.session_info)

class AttendanceReport(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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
    
    def _format_date_for_table(self, date_str: str) -> str:
        """Format date string for table display"""
        try:
            if 'T' in date_str:
                # Parse ISO format and convert to desired format
                date_obj = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return date_obj.strftime("%Y-%m-%d %H:%M UTC")
            else: # Already formatted or partial date
                return date_str
        except:
            # Fallback to original if parsing fails
            return date_str.split()[0] if date_str else "N/A"

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

    async def generate_csv_export(self, records, session_info):
        """Generate CSV export file"""
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write metadata
        writer.writerow(['Session Name:', session_info['session_name']])
        writer.writerow(['Alliance:', session_info['alliance_name']])
        writer.writerow(['Event Type:', session_info.get('event_type', 'Other')])
        if session_info.get('event_date'):
            writer.writerow(['Event Date:', session_info['event_date'].split('T')[0] if isinstance(session_info['event_date'], str) else session_info['event_date']])
        writer.writerow(['Export Date:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Total Players:', session_info['total_players']])
        writer.writerow(['Present:', session_info['present_count'], 'Absent:', session_info['absent_count']])
        writer.writerow([])
        
        # Write headers
        writer.writerow(['FID', 'Nickname', 'Status', 'Points', 'Last Event Attendance', 'Marked By'])
        
        # Write data
        for record in records:
            writer.writerow([
                record[0],  # FID
                record[1],  # Nickname
                record[2].replace('_', ' ').title(),  # Status
                record[3] if record[3] else 0,  # Points
                record[4] if record[4] else 'N/A',  # Last Event
                record[6]   # Marked By
            ])
        
        output.seek(0)
        filename = f"attendance_{session_info['alliance_name'].replace(' ', '_')}_{session_info['session_name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return discord.File(io.BytesIO(output.getvalue().encode('utf-8')), filename=filename)

    async def generate_tsv_export(self, records, session_info):
        """Generate TSV export file"""
        output = io.StringIO()
        writer = csv.writer(output, delimiter='\t')
        
        # Write metadata
        writer.writerow(['Session Name:', session_info['session_name']])
        writer.writerow(['Alliance:', session_info['alliance_name']])
        writer.writerow(['Event Type:', session_info.get('event_type', 'Other')])
        if session_info.get('event_date'):
            writer.writerow(['Event Date:', session_info['event_date'].split('T')[0] if isinstance(session_info['event_date'], str) else session_info['event_date']])
        writer.writerow(['Export Date:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Total Players:', session_info['total_players']])
        writer.writerow(['Present:', session_info['present_count'], 'Absent:', session_info['absent_count']])
        writer.writerow([])  # Empty row
        
        # Write headers
        writer.writerow(['FID', 'Nickname', 'Status', 'Points', 'Last Event Attendance', 'Marked By'])
        
        # Write data
        for record in records:
            writer.writerow([
                record[0],  # FID
                record[1],  # Nickname
                record[2].replace('_', ' ').title(),  # Status
                record[3] if record[3] else 0,  # Points
                record[4] if record[4] else 'N/A',  # Last Event
                record[6]   # Marked By
            ])
        
        output.seek(0)
        filename = f"attendance_{session_info['alliance_name'].replace(' ', '_')}_{session_info['session_name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.tsv"
        return discord.File(io.BytesIO(output.getvalue().encode('utf-8')), filename=filename)

    async def generate_html_export(self, records, session_info):
        """Generate HTML export file"""
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Attendance Report - {session_info['alliance_name']} - {session_info['session_name']}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background-color: #4CAF50;
            color: white;
            padding: 20px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        .stats {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            background-color: white;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            font-weight: bold;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .present {{ color: #4CAF50; font-weight: bold; }}
        .absent {{ color: #f44336; font-weight: bold; }}
        .not-recorded {{ color: #9e9e9e; font-weight: bold; }}
        .footer {{
            text-align: center;
            margin-top: 20px;
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Attendance Report</h1>
        <h2>{session_info['alliance_name']} - {session_info['session_name']}</h2>
    </div>
    
    <div class="stats">
        <h3>Summary</h3>
        <p><strong>Event Type:</strong> {session_info.get('event_type', 'Other')}</p>
        {'<p><strong>Event Date:</strong> ' + (session_info['event_date'].split('T')[0] if isinstance(session_info.get('event_date'), str) else str(session_info.get('event_date', ''))) + '</p>' if session_info.get('event_date') else ''}
        <p><strong>Export Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Total Players:</strong> {session_info['total_players']}</p>
        <p>
            <span class="present">Present: {session_info['present_count']}</span> | 
            <span class="absent">Absent: {session_info['absent_count']}</span>
        </p>
    </div>
    
    <table>
        <thead>
            <tr>
                <th>FID</th>
                <th>Nickname</th>
                <th>Status</th>
                <th>Points</th>
                <th>Last Event Attendance</th>
                <th>Marked By</th>
            </tr>
        </thead>
        <tbody>
"""
        
        # Add data rows
        for record in records:
            status = record[2]
            status_class = status.replace('_', '-')
            status_display = status.replace('_', ' ').title()
            
            html_content += f"""            <tr>
                <td>{record[0]}</td>
                <td>{record[1]}</td>
                <td class="{status_class}">{status_display}</td>
                <td>{record[3] if record[3] else 0:,}</td>
                <td>{record[4] if record[4] else 'N/A'}</td>
                <td>{record[6]}</td>
            </tr>
"""
        
        html_content += """        </tbody>
    </table>
    
    <div class="footer">
        <p>Generated by Whiteout Discord Bot</p>
    </div>
</body>
</html>"""
        
        filename = f"attendance_{session_info['alliance_name'].replace(' ', '_')}_{session_info['session_name'].replace(' ', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        return discord.File(io.BytesIO(html_content.encode('utf-8')), filename=filename)

    async def process_export(self, interaction: discord.Interaction, format_type: str, records, session_info):
        """Process export request and send file via DM"""
        try:
            # Defer the response as file generation might take a moment
            await interaction.response.defer(ephemeral=True)
            
            # Generate the appropriate file
            if format_type == "csv":
                file = await self.generate_csv_export(records, session_info)
                format_name = "CSV"
            elif format_type == "tsv":
                file = await self.generate_tsv_export(records, session_info)
                format_name = "TSV"
            elif format_type == "html":
                file = await self.generate_html_export(records, session_info)
                format_name = "HTML"
            else:
                await interaction.followup.send(
                    "❌ Invalid export format selected.",
                    ephemeral=True
                )
                return
            
            # Try to DM the file
            try:
                await interaction.user.send(
                    f"📊 **Attendance Report Export**\n"
                    f"**Format:** {format_name}\n"
                    f"**Alliance:** {session_info['alliance_name']}\n"
                    f"**Session:** {session_info['session_name']}\n"
                    f"**Event Type:** {session_info.get('event_type', 'Other')}\n"
                    f"**Total Records:** {session_info['total_players']}",
                    file=file
                )
                await interaction.followup.send(
                    "✅ Attendance report sent to your DMs!",
                    ephemeral=True
                )
            except discord.Forbidden:
                await interaction.followup.send(
                    "❌ Could not send DM. Please enable DMs from server members and try again.",
                    ephemeral=True
                )
            except discord.HTTPException as e:
                if "Maximum message size" in str(e):
                    await interaction.followup.send(
                        "❌ Report too large to send via Discord (8MB limit). Please try exporting fewer records.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"❌ An error occurred while sending the report: {str(e)}",
                        ephemeral=True
                    )
                    
        except Exception as e:
            print(f"Error in process_export: {e}")
            await interaction.followup.send(
                "❌ An error occurred while generating the export.",
                ephemeral=True
            )

    async def show_attendance_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str, 
                                   is_preview=False, selected_players=None, session_id=None, marking_view=None):
        """
        Show attendance records with user's preferred format
        - is_preview=True: 3-column format for marking attendance preview
        - is_preview=False: 6-column format for full report viewing
        - selected_players: Used only for preview mode from marking flow
        """
        try:            
            # Get user's report preference
            report_type = await self.get_user_report_preference(interaction.user.id)
            
            # If matplotlib is not available, force text mode
            if report_type == "matplotlib" and not MATPLOTLIB_AVAILABLE:
                report_type = "text"
                
            if report_type == "matplotlib":
                await self.show_matplotlib_report(interaction, alliance_id, session_name, is_preview, selected_players, session_id, marking_view)
            else:
                await self.show_text_report(interaction, alliance_id, session_name, is_preview, selected_players, session_id, marking_view)
                
        except Exception as e:
            print(f"Error showing attendance report: {e}")
            import traceback
            traceback.print_exc()
            await interaction.edit_original_response(
                content="❌ An error occurred while generating attendance report.",
                embed=None,
                view=None
            )

    async def show_matplotlib_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str,
                                   is_preview=False, selected_players=None, session_id=None, marking_view=None):
        """Show attendance records as a Matplotlib table image"""
        try:
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Handle preview mode vs full report mode
            if is_preview and selected_players:
                # Preview mode - use selected_players data
                filtered_players = {
                    fid: data for fid, data in selected_players.items()
                    if data['attendance_type'] != 'not_recorded'
                }
                
                # Get event info from marking view if available
                event_type = marking_view.event_type if marking_view and hasattr(marking_view, 'event_type') else None
                event_date = marking_view.event_date if marking_view and hasattr(marking_view, 'event_date') else None
                
                # Convert to records format for consistency
                records = []
                for fid, data in sorted(filtered_players.items(), key=lambda x: x[1]['points'], reverse=True):
                    records.append((
                        fid,
                        data['nickname'],
                        data['attendance_type'],
                        data['points'],
                        data.get('last_event_attendance', 'N/A'),
                        None,  # No date in preview
                        None   # No marked_by in preview
                    ))
                
                if not records:
                    await interaction.response.edit_message(
                        content=f"❌ No attendance has been marked yet.",
                        embed=None,
                        view=None
                    )
                    return
                    
                present_count = sum(1 for r in records if r[2] == 'present')
                absent_count = sum(1 for r in records if r[2] == 'absent')
                
            else:
                # Full report mode - fetch from database
                records = []
                event_type = None
                event_date = None
                with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                    cursor = attendance_db.cursor()
                    if session_id:
                        # Use session_id if provided (more specific)
                        cursor.execute("""
                            SELECT player_id, player_name, status, points, event_type, event_date, marked_by_username
                            FROM attendance_records
                            WHERE session_id = ? AND status != 'not_recorded'
                            ORDER BY points DESC, marked_at DESC
                        """, (session_id,))
                    else:
                        # Fallback to session_name (less specific, may include multiple sessions)
                        cursor.execute("""
                            SELECT player_id, player_name, status, points, event_type, event_date, marked_by_username
                            FROM attendance_records
                            WHERE alliance_id = ? AND session_name = ? AND status != 'not_recorded'
                            ORDER BY points DESC, marked_at DESC
                        """, (str(alliance_id), session_name))
                    db_records = cursor.fetchall()
                    
                    # Get session_id if not provided (needed for last event lookup)
                    if not session_id and db_records:
                        cursor.execute("""
                            SELECT DISTINCT session_id FROM attendance_records
                            WHERE alliance_id = ? AND session_name = ?
                            LIMIT 1
                        """, (str(alliance_id), session_name))
                        result = cursor.fetchone()
                        if result:
                            session_id = result[0]
                    
                    # Convert to expected format and get event info
                    for record in db_records:
                        if event_type is None:
                            event_type = record[4]
                        if event_date is None:
                            event_date = record[5]
                        
                        # Fetch last event attendance for this player
                        last_event_attendance = await self.fetch_last_event_attendance(
                            record[0], event_type, event_date, session_id
                        ) if event_type and event_date and session_id else "N/A"
                        
                        # Format: (fid, nickname, status, points, last_event_attendance, marked_date, marked_by)
                        records.append((
                            record[0],  # player_id
                            record[1],  # player_name
                            record[2],  # status
                            record[3],  # points
                            last_event_attendance,
                            event_date, # use event_date
                            record[6]   # marked_by_username
                        ))

                if not records:
                    await interaction.response.edit_message(
                        content=f"❌ No attendance records found for session '{session_name}' in {alliance_name}.",
                        embed=None,
                        view=None
                    )
                    return

                # Count attendance types
                present_count = sum(1 for r in records if r[2] == 'present')
                absent_count = sum(1 for r in records if r[2] == 'absent')

            not_recorded_count = 0  # We're not showing not_recorded in reports

            # Generate Matplotlib table image - different headers for preview vs full
            if is_preview:
                headers = ["Player", "Status", "Points"]
                table_color = '#28a745'  # Green for preview
            else:
                headers = ["Player", "Status", "Last Event", "Points", "Marked By"]
                table_color = '#1f77b4'  # Blue for full report
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
                if is_preview:
                    # Preview mode - 3 columns
                    status_display = {
                        "present": "Present",
                        "absent": "Absent"
                    }.get(row[2], row[2])
                    
                    table_data.append([
                        wrap_text(fix_arabic(row[1] or "Unknown")),
                        wrap_text(fix_arabic(status_display)),
                        wrap_text(f"{row[3]:,}" if row[3] else "0")
                    ])
                else:
                    # Full report - 5 columns (Date column removed)
                    table_data.append([
                        wrap_text(fix_arabic(row[1] or "Unknown")),
                        wrap_text(fix_arabic(row[2].replace('_', ' ').title())),
                        wrap_text(fix_arabic(row[4] if row[4] else "N/A"), width=40),
                        wrap_text(f"{row[3]:,}" if row[3] else "0"),
                        wrap_text(fix_arabic(row[6] or "Unknown"))
                    ])

            # Calculate figure height based on number of rows
            row_height = 0.6 if not is_preview else 0.5
            header_height = 2
            fig_height = min(header_height + len(table_data) * row_height, 25 if not is_preview else 20)
            
            # Adjust figure width for preview mode
            fig_width = 10 if is_preview else 13
            
            fig, ax = plt.subplots(figsize=(fig_width, fig_height))
            ax.axis('off')
            
            # Format title with event type and date
            title_text = f'Attendance Report - {alliance_name} | {session_name}'
            if event_type:
                title_text += f' [{event_type}]'
            if event_date:
                # Extract just the date part if it's a datetime string
                date_str = event_date.split('T')[0] if 'T' in str(event_date) else str(event_date).split()[0]
                title_text += f' | Date: {date_str}'
            
            ax.text(0.5, 0.98, title_text, 
                   transform=ax.transAxes, fontsize=16 if not is_preview else 14, color=table_color, 
                   ha='center', va='top', weight='bold')
            
            # Create table with adjusted position to avoid title overlap
            table = ax.table(
                cellText=table_data,
                colLabels=headers,
                cellLoc='left',
                loc='upper center',
                bbox=[0, -0.05, 1, 0.90],  # Move down and reduce height to avoid title
                colColours=[table_color]*len(headers)
            )
            table.auto_set_font_size(False)
            table.set_fontsize(12)
            table.scale(1, 1.5)
            
            # Set larger width for columns - only for full report
            if not is_preview:
                nrows = len(table_data) + 1
                for row in range(nrows):
                    cell = table[(row, 2)]
                    cell.set_width(0.35)
                    cell = table[(row, 4)]
                    cell.set_width(0.25)

            img_buffer = BytesIO()
            plt.savefig(img_buffer, format='png', bbox_inches='tight')
            plt.close(fig)
            img_buffer.seek(0)

            file = discord.File(img_buffer, filename="attendance_report.png")

            # Format embed title with event type
            embed_title = f"📊 Attendance Report - {alliance_name}"
            description_text = f"**Session:** {session_name}"
            if event_type:
                description_text += f" [{event_type}]"
            description_text += f"\n**Total Marked:** {len(records)} players"
            
            embed = discord.Embed(
                title=embed_title,
                description=description_text,
                color=discord.Color.green() if is_preview else discord.Color.blue()
            )
            embed.set_image(url="attachment://attendance_report.png")

            # Create view based on mode
            if is_preview:
                # Preview mode - create a simple back button that clears attachments
                view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(
                    label="⬅️ Back",
                    style=discord.ButtonStyle.secondary
                )
                
                async def preview_back_callback(back_interaction: discord.Interaction):
                    # Check if we have a stored marking view to return to
                    if marking_view:
                        await marking_view.update_main_embed(back_interaction)
                    else:
                        # Fallback - just remove attachment
                        await back_interaction.response.edit_message(attachments=[])
                
                back_button.callback = preview_back_callback
                view.add_item(back_button)
            else:
                # Full report mode - back and export buttons
                view = discord.ui.View(timeout=7200)
                
                # Back button
                back_button = discord.ui.Button(
                    label="⬅️ Back",
                    style=discord.ButtonStyle.secondary
                )
                
                async def back_callback(back_interaction: discord.Interaction):
                    await self.show_session_selection(back_interaction, alliance_id)
                
                back_button.callback = back_callback
                view.add_item(back_button)
                
                # Export button - only for full reports
                export_button = discord.ui.Button(
                    label="Export",
                    emoji="📥",
                    style=discord.ButtonStyle.primary
                )
                
                async def export_callback(export_interaction: discord.Interaction):
                    session_info = {
                        'session_name': session_name,
                        'alliance_name': alliance_name,
                        'event_type': event_type or 'Other',
                        'event_date': event_date,
                        'total_players': len(records),
                        'present_count': present_count,
                        'absent_count': absent_count,
                        'not_recorded_count': not_recorded_count
                    }
                    export_view = ExportFormatSelectView(self, records, session_info)
                    await export_interaction.response.send_message(
                        "Select export format:",
                        view=export_view,
                        ephemeral=True
                    )
                
                export_button.callback = export_callback
                view.add_item(export_button)

            # Handle both regular and deferred interactions
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view, attachments=[file])
            else:
                await interaction.response.edit_message(embed=embed, view=view, attachments=[file])

        except Exception as e:
            print(f"Matplotlib error: {e}")
            # Fallback to text report
            await self.show_text_report(interaction, alliance_id, session_name, is_preview, selected_players, session_id, marking_view)

    async def fetch_last_event_attendance(self, player_id: str, event_type: str, event_date: str, session_id: str):
        """Fetch the last attendance for a player of the same event type before the current event date"""
        try:
            with sqlite3.connect('db/attendance.sqlite') as db:
                cursor = db.cursor()
                # Get the last attendance of the same event type before the current event date
                cursor.execute("""
                    SELECT status, event_date 
                    FROM attendance_records 
                    WHERE player_id = ? 
                    AND event_type = ? 
                    AND event_date < ?
                    AND session_id != ?
                    ORDER BY event_date DESC 
                    LIMIT 1
                """, (player_id, event_type, event_date, session_id))
                
                result = cursor.fetchone()
                if result:
                    status, last_date = result
                    # Format the date
                    try:
                        last_date_obj = datetime.fromisoformat(last_date.replace('Z', '+00:00'))
                        date_str = last_date_obj.strftime("%m/%d")
                    except:
                        date_str = last_date.split('T')[0] if 'T' in last_date else last_date
                    
                    status_display = status.replace('_', ' ').title()
                    return f"{status_display} ({date_str})"
                else:
                    # No record found - check if there are ANY previous events of this type
                    cursor.execute("""
                        SELECT COUNT(DISTINCT session_id) 
                        FROM attendance_records 
                        WHERE event_type = ? 
                        AND event_date < ?
                        AND session_id != ?
                    """, (event_type, event_date, session_id))
                    
                    event_count = cursor.fetchone()
                    if event_count and event_count[0] > 0:
                        # There were previous events of this type, but player wasn't in them
                        # This could mean they're new to the alliance
                        return "New Player"
                    else:
                        # This is the first event of this type
                        return "First Event"
        except Exception as e:
            print(f"Error fetching last attendance: {e}")
            return "N/A"

    async def show_text_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str,
                             is_preview=False, selected_players=None, session_id=None, marking_view=None):
        """Show attendance records for a specific session with emoji-based formatting"""
        try:
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Handle preview mode vs full report mode
            if is_preview and selected_players:
                # Preview mode - use selected_players data
                filtered_players = {
                    fid: data for fid, data in selected_players.items()
                    if data['attendance_type'] != 'not_recorded'
                }
                
                # Get event info from marking view if available
                event_type = marking_view.event_type if marking_view and hasattr(marking_view, 'event_type') else None
                event_date = marking_view.event_date if marking_view and hasattr(marking_view, 'event_date') else None
                
                # Convert to records format for consistency
                records = []
                for fid, data in sorted(filtered_players.items(), key=lambda x: x[1]['points'], reverse=True):
                    records.append((
                        fid,
                        data['nickname'],
                        data['attendance_type'],
                        data['points'],
                        data.get('last_event_attendance', 'N/A'),
                        event_date,  # use event_date from marking view
                        None   # No marked_by in preview
                    ))
                
                if not records:
                    await interaction.response.edit_message(
                        content=f"❌ No attendance has been marked yet.",
                        embed=None,
                        view=None
                    )
                    return
            else:
                # Full report mode - fetch from database
                records = []
                event_type = None
                event_date = None
                with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                    cursor = attendance_db.cursor()
                    if session_id:
                        # Use session_id if provided (more specific)
                        cursor.execute("""
                            SELECT player_id, player_name, status, points, event_type, event_date, marked_by_username
                            FROM attendance_records
                            WHERE session_id = ? AND status != 'not_recorded'
                            ORDER BY points DESC, marked_at DESC
                        """, (session_id,))
                    else:
                        # Fallback to session_name (less specific, may include multiple sessions)
                        cursor.execute("""
                            SELECT player_id, player_name, status, points, event_type, event_date, marked_by_username
                            FROM attendance_records
                            WHERE alliance_id = ? AND session_name = ? AND status != 'not_recorded'
                            ORDER BY points DESC, marked_at DESC
                        """, (str(alliance_id), session_name))
                    db_records = cursor.fetchall()
                    
                    # Get session_id if not provided (needed for last event lookup)
                    if not session_id and db_records:
                        cursor.execute("""
                            SELECT DISTINCT session_id FROM attendance_records
                            WHERE alliance_id = ? AND session_name = ?
                            LIMIT 1
                        """, (str(alliance_id), session_name))
                        result = cursor.fetchone()
                        if result:
                            session_id = result[0]
                    
                    # Convert to expected format and get event info
                    for record in db_records:
                        if event_type is None:
                            event_type = record[4]
                        if event_date is None:
                            event_date = record[5]
                        
                        # Fetch last event attendance for this player
                        last_event_attendance = await self.fetch_last_event_attendance(
                            record[0], event_type, event_date, session_id
                        ) if event_type and event_date and session_id else "N/A"
                        
                        # Format: (fid, nickname, status, points, last_event_attendance, marked_date, marked_by)
                        records.append((
                            record[0],  # player_id
                            record[1],  # player_name
                            record[2],  # status
                            record[3],  # points
                            last_event_attendance,
                            event_date, # use event_date
                            record[6]   # marked_by_username
                        ))

            if not records:
                await interaction.edit_original_response(
                    content=f"❌ No attendance records found for session '{session_name}' in {alliance_name}.",
                    embed=None,
                    view=None
                )
                return

            # Count attendance types
            present_count = sum(1 for r in records if r[2] == 'present')
            absent_count = sum(1 for r in records if r[2] == 'absent')
            not_recorded_count = 0  # We're not showing not_recorded in reports anymore

            # Get session ID from attendance records if not provided
            if not session_id:
                try:
                    with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                        cursor = attendance_db.cursor()
                        cursor.execute("""
                            SELECT DISTINCT session_id FROM attendance_records
                            WHERE session_name = ? AND alliance_id = ?
                            LIMIT 1
                        """, (session_name, str(alliance_id)))
                        result = cursor.fetchone()
                        if result:
                            session_id = result[0]
                except:
                    pass

            # Build the report sections
            report_sections = []
            
            # Summary section
            report_sections.append("📊 **SUMMARY**")
            session_line = f"**Session:** {session_name}"
            if event_type:
                session_line += f" [{event_type}]"
            report_sections.append(session_line)
            report_sections.append(f"**Alliance:** {alliance_name}")
            report_sections.append(f"**Date:** {records[0][5].split()[0] if records else 'N/A'}")
            report_sections.append(f"**Total Marked:** {len(records)} players")
            report_sections.append(f"**Present:** {present_count} | **Absent:** {absent_count}")
            if session_id:
                report_sections.append(f"**Session ID:** {session_id}")
            report_sections.append("")
            
            # Player details section
            report_sections.append("👥 **PLAYER DETAILS**")
            report_sections.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
            
            # Sort: Present (by points desc) → Absent
            def sort_key(record):
                attendance_type = record[2]
                points = record[3] or 0
                
                type_priority = {
                    "present": 1,
                    "absent": 2
                }.get(attendance_type, 3)
                
                return (type_priority, -points)
            
            sorted_records = sorted(records, key=sort_key)
            
            for record in sorted_records:
                fid = record[0]
                nickname = record[1] or "Unknown"
                attendance_status = record[2]
                points = record[3] or 0
                last_event_attendance = record[4] or "N/A"
                
                # Get status emoji
                status_emoji = self._get_status_emoji(attendance_status)
                
                # Convert last attendance status to relevant emoji
                last_event_display = self._format_last_attendance(last_event_attendance)
                
                points_display = f"{points:,}" if points > 0 else "0"
                
                player_line = f"{status_emoji} **{nickname}** (FID: {fid})"
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
            
            # Create view with back and export buttons
            view = discord.ui.View(timeout=7200)
            
            # Back button
            back_button = discord.ui.Button(
                label="⬅️ Back to Sessions",
                style=discord.ButtonStyle.secondary
            )
            
            async def back_callback(back_interaction: discord.Interaction):
                await self.show_session_selection(back_interaction, alliance_id)
            
            back_button.callback = back_callback
            view.add_item(back_button)
            
            # Export button
            export_button = discord.ui.Button(
                label="Export",
                emoji="📥",
                style=discord.ButtonStyle.primary
            )
            
            async def export_callback(export_interaction: discord.Interaction):
                session_info = {
                    'session_name': session_name,
                    'alliance_name': alliance_name,
                    'event_type': event_type or 'Other',
                    'event_date': event_date,
                    'total_players': len(records),
                    'present_count': present_count,
                    'absent_count': absent_count,
                    'not_recorded_count': not_recorded_count
                }
                export_view = ExportFormatSelectView(self, records, session_info)
                await export_interaction.response.send_message(
                    "Select export format:",
                    view=export_view,
                    ephemeral=True
                )
            
            export_button.callback = export_callback
            view.add_item(export_button)
            
            # Handle both regular and deferred interactions
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"Error showing text attendance report: {e}")
            # Try to respond appropriately based on interaction state
            error_content = "❌ An error occurred while generating attendance report."
            if interaction.response.is_done():
                await interaction.edit_original_response(content=error_content, embed=None, view=None)
            else:
                await interaction.response.edit_message(content=error_content, embed=None, view=None)

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
        
            # Get session details from single attendance_records table
            sessions = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
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
                
                for row in cursor.fetchall():
                    sessions.append({
                        'session_id': row[0],
                        'name': row[1],
                        'event_type': row[2],
                        'date': row[3].split('T')[0] if row[3] else "Unknown",
                        'player_count': row[4],
                        'marked_count': row[5]
                    })

            if not sessions:
                # Create embed for no sessions found
                embed = discord.Embed(
                    title=f"📋 Attendance Sessions - {alliance_name}",
                    description=f"❌ **No attendance sessions found for {alliance_name}.**\n\nTo create attendance records, use the 'Mark Attendance' option from the main menu.",
                    color=discord.Color.orange()
                )
                
                # Add back button
                back_view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(
                    label="⬅️ Back to Alliance Selection",
                    style=discord.ButtonStyle.secondary
                )
                
                async def back_callback(back_interaction: discord.Interaction):
                    attendance_cog = self.bot.get_cog("Attendance")
                    if attendance_cog:
                        await attendance_cog.show_attendance_menu(back_interaction)
                
                back_button.callback = back_callback
                back_view.add_item(back_button)
                
                if interaction.response.is_done():
                    await interaction.edit_original_response(
                        content=None,
                        embed=embed,
                        view=back_view,
                        attachments=[]
                    )
                else:
                    await interaction.response.edit_message(
                        content=None,
                        embed=embed,
                        view=back_view,
                        attachments=[]
                    )
                return
        
            # Create session selection view
            view = SessionSelectView(sessions, alliance_id, self, is_viewing=True)
            
            embed = discord.Embed(
                title=f"📋 Attendance Sessions - {alliance_name}",
                description="Please select a session to view attendance records:",
                color=discord.Color.blue()
            )
            
            if interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view, attachments=[])
            else:
                await interaction.response.edit_message(embed=embed, view=view, attachments=[])
    
        except Exception as e:
            print(f"Error showing session selection: {e}")
            if interaction.response.is_done():
                await interaction.edit_original_response(
                    content="❌ An error occurred while loading sessions.",
                    embed=None,
                    view=None
                )
            else:
                await interaction.response.send_message(
                    "❌ An error occurred while loading sessions.",
                    ephemeral=True
                )

async def setup(bot):
    try:
        cog = AttendanceReport(bot)
        await bot.add_cog(cog)
    except Exception as e:
        print(f"❌ Failed to load AttendanceReport cog: {e}")