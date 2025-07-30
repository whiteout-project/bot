import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import os
import re
import csv
import io
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

class ExportFormatSelectView(discord.ui.View):
    def __init__(self, cog, records, session_info):
        super().__init__(timeout=60)
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
        attendance_cog = self.cog.bot.get_cog("Attendance")
        if attendance_cog:
            await attendance_cog.show_attendance_menu(interaction)
 
    async def on_select(self, interaction: discord.Interaction):
        await interaction.response.defer()
        session_name = interaction.data['values'][0]
        await self.cog.show_attendance_report(interaction, self.alliance_id, session_name)

class AttendanceReport(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

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
        writer.writerow(['Export Date:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Total Players:', session_info['total_players']])
        writer.writerow(['Present:', session_info['present_count'], 'Absent:', session_info['absent_count'], 'Not Signed:', session_info['not_signed_count']])
        writer.writerow([])  # Empty row
        
        # Write headers
        writer.writerow(['FID', 'Nickname', 'Status', 'Points', 'Last Event Attendance', 'Marked Date', 'Marked By'])
        
        # Write data
        for record in records:
            writer.writerow([
                record[0],  # FID
                record[1],  # Nickname
                record[2].replace('_', ' ').title(),  # Status
                record[3] if record[3] else 0,  # Points
                record[4] if record[4] else 'N/A',  # Last Event
                record[5],  # Marked Date
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
        writer.writerow(['Export Date:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        writer.writerow(['Total Players:', session_info['total_players']])
        writer.writerow(['Present:', session_info['present_count'], 'Absent:', session_info['absent_count'], 'Not Signed:', session_info['not_signed_count']])
        writer.writerow([])  # Empty row
        
        # Write headers
        writer.writerow(['FID', 'Nickname', 'Status', 'Points', 'Last Event Attendance', 'Marked Date', 'Marked By'])
        
        # Write data
        for record in records:
            writer.writerow([
                record[0],  # FID
                record[1],  # Nickname
                record[2].replace('_', ' ').title(),  # Status
                record[3] if record[3] else 0,  # Points
                record[4] if record[4] else 'N/A',  # Last Event
                record[5],  # Marked Date
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
        .not-signed {{ color: #9e9e9e; font-weight: bold; }}
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
        <p><strong>Export Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p><strong>Total Players:</strong> {session_info['total_players']}</p>
        <p>
            <span class="present">Present: {session_info['present_count']}</span> | 
            <span class="absent">Absent: {session_info['absent_count']}</span> | 
            <span class="not-signed">Not Signed: {session_info['not_signed_count']}</span>
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
                <th>Marked Date</th>
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
                <td>{record[5]}</td>
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
            await interaction.edit_original_response(
                content="❌ An error occurred while generating attendance report.",
                embed=None,
                view=None
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
                    SELECT fid, nickname, attendance_status, points, last_event_attendance, marked_date, marked_by_username
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
            present_count = sum(1 for r in records if r[2] == 'present')
            absent_count = sum(1 for r in records if r[2] == 'absent')
            not_signed_count = sum(1 for r in records if r[2] == 'not_signed')

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
                    wrap_text(fix_arabic(row[1] or "Unknown")),  # Nickname
                    wrap_text(fix_arabic(row[2].replace('_', ' ').title())),
                    wrap_text(fix_arabic(row[4] if row[4] else "N/A"), width=40),
                    wrap_text(f"{row[3]:,}" if row[3] else "0"),
                    wrap_text(fix_arabic(row[5].split()[0] if row[5] else "N/A")),
                    wrap_text(fix_arabic(row[6] or "Unknown"))
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

            # Create view with back and export buttons
            view = discord.ui.View(timeout=1800)
            
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
                    'total_players': len(records),
                    'present_count': present_count,
                    'absent_count': absent_count,
                    'not_signed_count': not_signed_count
                }
                export_view = ExportFormatSelectView(self, records, session_info)
                await export_interaction.response.send_message(
                    "Select export format:",
                    view=export_view,
                    ephemeral=True
                )
            
            export_button.callback = export_callback
            view.add_item(export_button)

            await interaction.edit_original_response(embed=embed, view=view, attachments=[file])

        except Exception as e:
            print(f"Matplotlib error: {e}")
            # Fallback to text report
            await self.show_text_report(interaction, alliance_id, session_name)

    async def show_text_report(self, interaction: discord.Interaction, alliance_id: int, session_name: str):
        """Show attendance records for a specific session with emoji-based formatting"""
        try:
            # Get alliance name
            alliance_name = await self._get_alliance_name(alliance_id)

            # Get attendance records - sorted by points descending
            records = []
            with sqlite3.connect('db/attendance.sqlite') as attendance_db:
                cursor = attendance_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, attendance_status, points, last_event_attendance, marked_date, marked_by_username
                    FROM attendance_records
                    WHERE alliance_id = ? AND session_name = ?
                    ORDER BY points DESC, marked_date DESC
                """, (alliance_id, session_name))
                records = cursor.fetchall()

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
            not_signed_count = sum(1 for r in records if r[2] == 'not_signed')

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
            report_sections.append(f"**Date:** {records[0][5].split()[0] if records else 'N/A'}")
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
                attendance_type = record[2]
                points = record[3] or 0
                
                type_priority = {
                    "present": 1,
                    "absent": 2, 
                    "not_signed": 3
                }.get(attendance_type, 4)
                
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
            view = discord.ui.View(timeout=1800)
            
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
                    'total_players': len(records),
                    'present_count': present_count,
                    'absent_count': absent_count,
                    'not_signed_count': not_signed_count
                }
                export_view = ExportFormatSelectView(self, records, session_info)
                await export_interaction.response.send_message(
                    "Select export format:",
                    view=export_view,
                    ephemeral=True
                )
            
            export_button.callback = export_callback
            view.add_item(export_button)
            
            await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            print(f"Error showing text attendance report: {e}")
            await interaction.edit_original_response(
                content="❌ An error occurred while generating attendance report.",
                embed=None,
                view=None
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
            view = SessionSelectView(sessions, alliance_id, self)
            
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