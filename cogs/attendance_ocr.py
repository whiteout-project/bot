"""
Attendance OCR helper. Extracts attendance data from screenshot images.
"""
import discord
from discord.ext import commands
import io
import re
import sqlite3
import logging
import uuid
from datetime import datetime
from difflib import SequenceMatcher
import numpy as np

logger = logging.getLogger('notification')

try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    Image = None
    PIL_AVAILABLE = False

# RapidOCR setup
OCR_AVAILABLE = False
rapid_ocr = None

if PIL_AVAILABLE:
    try:
        from rapidocr import RapidOCR
        rapid_ocr = RapidOCR()
        OCR_AVAILABLE = True
        logger.info("RapidOCR initialized for attendance OCR")
        print("[INFO] RapidOCR initialized for attendance OCR")
    except ImportError:
        logger.warning("rapidocr not installed. Attendance OCR will be disabled.")
        print("[WARNING] rapidocr not installed. Attendance OCR disabled.")
    except Exception as e:
        logger.error(f"Failed to initialize RapidOCR for attendance: {e}")
        print(f"[ERROR] Failed to initialize RapidOCR for attendance: {e}")

# Import from attendance.py
try:
    from cogs.attendance import EVENT_TYPES, EVENT_TYPE_ICONS, LEGION_EVENT_TYPES
except ImportError:
    EVENT_TYPES = ["Foundry", "Canyon Clash", "Crazy Joe", "Bear Trap", "Castle Battle", "Frostdragon Tyrant", "Other"]
    EVENT_TYPE_ICONS = {
        "Foundry": "🏭", "Canyon Clash": "⚔️", "Crazy Joe": "🤪",
        "Bear Trap": "🐻", "Castle Battle": "🏰", "Frostdragon Tyrant": "🐉", "Other": "📋"
    }
    LEGION_EVENT_TYPES = ["Foundry", "Canyon Clash"]

# Import theme
try:
    from cogs.pimp_my_bot import get_theme
except ImportError:
    get_theme = None


def get_current_theme():
    """Get current theme or return a default object."""
    if get_theme:
        return get_theme()

    class DefaultTheme:
        emColor1 = discord.Color.blue()
        emColor2 = discord.Color.red()
        emColor3 = discord.Color.green()
        verifiedIcon = "✅"
        deniedIcon = "❌"
        warningIcon = "⚠️"
        homeIcon = "🏠"
        settingsIcon = "⚙️"
        searchIcon = "🔍"
        plusIcon = "➕"
        pinIcon = "📌"
    return DefaultTheme()


def clean_damage_number(raw: str) -> int:
    """Extract integer from damage string, removing commas and non-digits."""
    if not raw:
        return 0
    clean = re.sub(r"[^\d]", "", raw)
    return int(clean) if clean else 0


def fuzzy_match_score(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings (0.0 to 1.0)."""
    # Normalize strings for comparison
    s1_clean = re.sub(r'[^\w\s]', '', s1.lower()).strip()
    s2_clean = re.sub(r'[^\w\s]', '', s2.lower()).strip()
    return SequenceMatcher(None, s1_clean, s2_clean).ratio()


def parse_ocr_damage_ranking(ocr_text: str) -> list[dict]:
    """
    Parse OCR text to extract player names and damage points.
    Returns list of {'name': str, 'damage': int, 'rank': int or None}
    """
    results = []

    # Pattern to match player entries with damage points
    # Format: <name> followed by "Damage Points:" and number
    # Also handle Chinese: 伤害点数
    damage_pattern = re.compile(
        r'(.+?)\s*(?:Damage Points|伤害点数)[:\s]*([\d,\.]+)',
        re.IGNORECASE | re.MULTILINE
    )

    for match in damage_pattern.finditer(ocr_text):
        name = match.group(1).strip()
        damage_str = match.group(2)
        damage = clean_damage_number(damage_str)

        # Clean up name - remove rank numbers at start
        name = re.sub(r'^\d+[\.\s]*', '', name).strip()

        # Skip if name is empty or too short
        if len(name) < 2:
            continue

        results.append({
            'name': name,
            'damage': damage,
            'rank': len(results) + 1
        })

    return results


class AttendanceOCR(commands.Cog):
    """OCR-based attendance tracking from game screenshots."""

    def __init__(self, bot):
        self.bot = bot

        # Database connections
        self.alliance_conn = sqlite3.connect("db/alliance.sqlite", timeout=30.0, check_same_thread=False)
        self.alliance_conn.execute("PRAGMA journal_mode=WAL")
        self.alliance_conn.execute("PRAGMA synchronous=NORMAL")
        self.alliance_cursor = self.alliance_conn.cursor()

        self.users_conn = sqlite3.connect("db/users.sqlite", timeout=30.0, check_same_thread=False)
        self.users_conn.execute("PRAGMA journal_mode=WAL")
        self.users_conn.execute("PRAGMA synchronous=NORMAL")
        self.users_cursor = self.users_conn.cursor()

        self.attendance_conn = sqlite3.connect("db/attendance.sqlite", timeout=30.0, check_same_thread=False)
        self.attendance_conn.execute("PRAGMA journal_mode=WAL")
        self.attendance_conn.execute("PRAGMA synchronous=NORMAL")
        self.attendance_cursor = self.attendance_conn.cursor()

        # Add attendance_ocr_channel column if missing
        self.alliance_cursor.execute("PRAGMA table_info(alliancesettings)")
        columns = [col[1] for col in self.alliance_cursor.fetchall()]

        if 'attendance_ocr_channel' not in columns:
            self.alliance_cursor.execute("""
                ALTER TABLE alliancesettings
                ADD COLUMN attendance_ocr_channel INTEGER
            """)
            self.alliance_conn.commit()
            logger.info("Added attendance_ocr_channel column to alliancesettings")
            print("[INFO] Added attendance_ocr_channel column to alliancesettings")

    def cog_unload(self):
        if hasattr(self, 'alliance_conn'):
            self.alliance_conn.close()
        if hasattr(self, 'users_conn'):
            self.users_conn.close()
        if hasattr(self, 'attendance_conn'):
            self.attendance_conn.close()

    def get_registered_alliances(self, guild_id: int) -> list[tuple[int, str]]:
        """Get all alliances registered to this guild."""
        self.alliance_cursor.execute("""
            SELECT a.alliance_id, COALESCE(l.name, 'Alliance ' || a.alliance_id)
            FROM alliancesettings a
            LEFT JOIN alliance_list l ON a.alliance_id = l.alliance_id
            WHERE a.guild_id = ?
        """, (guild_id,))
        return self.alliance_cursor.fetchall()

    @discord.app_commands.command(name="attendance_ocr_channel", description="Set or view the attendance OCR channel for an alliance")
    @discord.app_commands.describe(
        action="Set or remove the attendance OCR channel",
        channel="The channel to use for attendance OCR (leave empty to view current)"
    )
    @discord.app_commands.choices(action=[
        discord.app_commands.Choice(name="Set Channel", value="set"),
        discord.app_commands.Choice(name="Remove Channel", value="remove"),
        discord.app_commands.Choice(name="View Current", value="view")
    ])
    async def attendance_ocr_channel_command(
        self,
        interaction: discord.Interaction,
        action: discord.app_commands.Choice[str],
        channel: discord.TextChannel = None
    ):
        """Configure the attendance OCR channel for an alliance."""
        theme = get_current_theme()

        # Get alliances for this guild
        alliances = self.get_registered_alliances(interaction.guild_id)

        if not alliances:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} No Alliances Found",
                    description="No alliances are registered to this server. Please register an alliance first.",
                    color=theme.emColor2
                ),
                ephemeral=True
            )
            return

        if action.value == "view":
            # Show current OCR channels
            lines = []
            for alliance_id, alliance_name in alliances:
                self.alliance_cursor.execute(
                    "SELECT attendance_ocr_channel FROM alliancesettings WHERE alliance_id = ?",
                    (alliance_id,)
                )
                result = self.alliance_cursor.fetchone()
                channel_id = result[0] if result and result[0] else None

                if channel_id:
                    ch = interaction.guild.get_channel(channel_id)
                    channel_text = ch.mention if ch else f"Unknown ({channel_id})"
                else:
                    channel_text = "*Not set*"

                lines.append(f"**{alliance_name}**: {channel_text}")

            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{theme.settingsIcon} Attendance OCR Channels",
                    description="\n".join(lines) if lines else "No alliances configured.",
                    color=theme.emColor1
                ),
                ephemeral=True
            )
            return

        if action.value == "set":
            if not channel:
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.deniedIcon} Channel Required",
                        description="Please specify a channel to set as the attendance OCR channel.",
                        color=theme.emColor2
                    ),
                    ephemeral=True
                )
                return

            # If only one alliance, set it directly
            if len(alliances) == 1:
                alliance_id, alliance_name = alliances[0]
                self.alliance_cursor.execute(
                    "UPDATE alliancesettings SET attendance_ocr_channel = ? WHERE alliance_id = ?",
                    (channel.id, alliance_id)
                )
                self.alliance_conn.commit()

                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.verifiedIcon} Channel Set",
                        description=f"Attendance OCR channel for **{alliance_name}** set to {channel.mention}.\n\nPost screenshots with damage rankings to this channel to automatically track attendance.",
                        color=theme.emColor3
                    ),
                    ephemeral=True
                )
            else:
                # Multiple alliances - show selector
                view = AllianceSelectView(self, alliances, channel, "set", interaction.user.id)
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.settingsIcon} Select Alliance",
                        description=f"Select which alliance to set {channel.mention} as the attendance OCR channel:",
                        color=theme.emColor1
                    ),
                    view=view,
                    ephemeral=True
                )
            return

        if action.value == "remove":
            if len(alliances) == 1:
                alliance_id, alliance_name = alliances[0]
                self.alliance_cursor.execute(
                    "UPDATE alliancesettings SET attendance_ocr_channel = NULL WHERE alliance_id = ?",
                    (alliance_id,)
                )
                self.alliance_conn.commit()

                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.verifiedIcon} Channel Removed",
                        description=f"Attendance OCR channel for **{alliance_name}** has been removed.",
                        color=theme.emColor3
                    ),
                    ephemeral=True
                )
            else:
                # Multiple alliances - show selector
                view = AllianceSelectView(self, alliances, None, "remove", interaction.user.id)
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.settingsIcon} Select Alliance",
                        description="Select which alliance to remove the attendance OCR channel from:",
                        color=theme.emColor1
                    ),
                    view=view,
                    ephemeral=True
                )

    def get_alliance_for_channel(self, channel_id: int) -> tuple[int, str] | None:
        """Get alliance_id and name for a configured OCR channel."""
        self.alliance_cursor.execute("""
            SELECT a.alliance_id, l.name
            FROM alliancesettings a
            LEFT JOIN alliance_list l ON a.alliance_id = l.alliance_id
            WHERE a.attendance_ocr_channel = ?
        """, (channel_id,))
        result = self.alliance_cursor.fetchone()
        if result:
            return (result[0], result[1] or f"Alliance {result[0]}")
        return None

    def get_alliance_members(self, alliance_id: int) -> list[dict]:
        """Get all members of an alliance for matching."""
        self.users_cursor.execute("""
            SELECT fid, nickname, furnace_lv
            FROM users
            WHERE alliance = ?
        """, (str(alliance_id),))

        return [
            {'fid': row[0], 'nickname': row[1] or '', 'furnace_lv': row[2] or 0}
            for row in self.users_cursor.fetchall()
        ]

    def match_players_to_members(self, ocr_players: list[dict], alliance_members: list[dict]) -> list[dict]:
        """
        Match OCR'd player names to alliance members using fuzzy matching.
        Returns list with match info added to each player.
        """
        results = []

        for player in ocr_players:
            ocr_name = player['name']
            best_match = None
            best_score = 0.0

            for member in alliance_members:
                member_name = member['nickname']
                if not member_name:
                    continue

                score = fuzzy_match_score(ocr_name, member_name)

                if score > best_score:
                    best_score = score
                    best_match = member

            # Categorize match confidence
            if best_score >= 0.90:
                match_type = 'high'
            elif best_score >= 0.70:
                match_type = 'low'
            else:
                match_type = 'none'
                best_match = None

            results.append({
                'ocr_name': ocr_name,
                'damage': player['damage'],
                'rank': player['rank'],
                'matched_member': best_match,
                'match_score': best_score,
                'match_type': match_type
            })

        return results

    def check_duplicate_session(self, alliance_id: int, event_type: str, event_date: str) -> dict | None:
        """Check if a session exists for same alliance/event/date."""
        self.attendance_cursor.execute("""
            SELECT DISTINCT session_id, session_name, event_type, event_date
            FROM attendance_records
            WHERE alliance_id = ? AND event_type = ? AND date(event_date) = date(?)
            LIMIT 1
        """, (str(alliance_id), event_type, event_date))

        result = self.attendance_cursor.fetchone()
        if result:
            return {
                'session_id': result[0],
                'session_name': result[1],
                'event_type': result[2],
                'event_date': result[3]
            }
        return None

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return

        if not message.attachments:
            return

        # Check if channel is configured for attendance OCR
        alliance_info = self.get_alliance_for_channel(message.channel.id)
        if not alliance_info:
            return

        alliance_id, alliance_name = alliance_info

        # Process image attachments
        for attachment in message.attachments[:1]:  # Only first image
            if not any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg']):
                continue

            await self.process_attendance_image(message, attachment, alliance_id, alliance_name)

    async def process_attendance_image(self, message, attachment, alliance_id: int, alliance_name: str):
        """Process an image attachment for attendance OCR."""
        theme = get_current_theme()

        if not OCR_AVAILABLE:
            await message.channel.send(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} OCR Not Available",
                    description="RapidOCR is not installed. Please install it with `pip install rapidocr`.",
                    color=theme.emColor2
                )
            )
            return

        try:
            # Read and process image
            image_data = await attachment.read()
            image = Image.open(io.BytesIO(image_data))
            image_array = np.array(image.convert('RGB'))

            # Run OCR
            result = rapid_ocr(image_array)

            # Extract text from OCR result
            extracted_text = ""
            if result:
                if hasattr(result, 'txts') and result.txts:
                    extracted_text = "\n".join(result.txts)
                elif hasattr(result, '__iter__'):
                    texts = []
                    for item in result:
                        if isinstance(item, (list, tuple)) and len(item) >= 2:
                            texts.append(str(item[1]))
                    extracted_text = "\n".join(texts)

            if not extracted_text.strip():
                await message.channel.send(
                    embed=discord.Embed(
                        title=f"{theme.warningIcon} No Text Found",
                        description="Could not extract any text from the image. Please ensure the screenshot is clear and contains the damage ranking.",
                        color=theme.emColor2
                    )
                )
                return

            # Parse damage ranking
            ocr_players = parse_ocr_damage_ranking(extracted_text)

            if not ocr_players:
                await message.channel.send(
                    embed=discord.Embed(
                        title=f"{theme.warningIcon} No Players Found",
                        description="Could not find any player damage rankings in the image. Make sure the screenshot shows the damage ranking list.",
                        color=theme.emColor2
                    )
                )
                return

            # Get alliance members for matching
            alliance_members = self.get_alliance_members(alliance_id)

            # Match players
            matched_players = self.match_players_to_members(ocr_players, alliance_members)

            # Count matches
            high_matches = sum(1 for p in matched_players if p['match_type'] == 'high')
            low_matches = sum(1 for p in matched_players if p['match_type'] == 'low')
            no_matches = sum(1 for p in matched_players if p['match_type'] == 'none')

            # Create confirmation view
            view = AttendanceOCRConfirmView(
                cog=self,
                message=message,
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                matched_players=matched_players,
                alliance_members=alliance_members,
                author_id=message.author.id
            )

            # Build initial embed
            embed = discord.Embed(
                title=f"{theme.searchIcon} Attendance OCR Results",
                description=(
                    f"**Alliance:** {alliance_name}\n"
                    f"**Date:** {datetime.utcnow().strftime('%Y-%m-%d')}\n"
                    f"**Players Found:** {len(matched_players)}\n\n"
                    f"{theme.verifiedIcon} **High Match:** {high_matches}\n"
                    f"{theme.warningIcon} **Low Match:** {low_matches}\n"
                    f"{theme.deniedIcon} **Unmatched:** {no_matches}"
                ),
                color=theme.emColor1
            )
            embed.set_footer(text="Select event type and review matches before submitting")

            await message.channel.send(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Error processing attendance image: {e}")
            print(f"[ERROR] Error processing attendance image: {e}")
            await message.channel.send(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Processing Error",
                    description=f"An error occurred while processing the image: {e}",
                    color=theme.emColor2
                )
            )


class AttendanceOCRConfirmView(discord.ui.View):
    """View for confirming OCR attendance results."""

    def __init__(self, cog, message, alliance_id, alliance_name, matched_players, alliance_members, author_id):
        super().__init__(timeout=3600)  # 1 hour timeout
        self.cog = cog
        self.original_message = message
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.matched_players = matched_players
        self.alliance_members = alliance_members
        self.author_id = author_id

        self.selected_event_type = "Bear Trap"  # Default for damage rankings
        self.selected_event_subtype = None
        self.event_date = datetime.utcnow()

        # Add event type select
        self.add_item(self._create_event_select())

    def _create_event_select(self):
        """Create event type dropdown."""
        theme = get_current_theme()
        options = []

        for event_type in EVENT_TYPES:
            emoji = EVENT_TYPE_ICONS.get(event_type, "📋")
            is_default = event_type == self.selected_event_type
            options.append(discord.SelectOption(
                label=event_type,
                value=event_type,
                emoji=emoji,
                default=is_default
            ))

        select = discord.ui.Select(
            placeholder=f"{theme.pinIcon} Select Event Type...",
            options=options,
            row=0
        )
        select.callback = self._on_event_select
        return select

    async def _on_event_select(self, interaction: discord.Interaction):
        """Handle event type selection."""
        self.selected_event_type = interaction.data['values'][0]

        # Check for duplicates
        duplicate = self.cog.check_duplicate_session(
            self.alliance_id,
            self.selected_event_type,
            self.event_date.strftime('%Y-%m-%d')
        )

        theme = get_current_theme()

        if duplicate:
            # Show duplicate warning
            embed = discord.Embed(
                title=f"{theme.warningIcon} Possible Duplicate",
                description=(
                    f"An attendance session already exists for this event:\n\n"
                    f"**Session:** {duplicate['session_name']}\n"
                    f"**Event:** {duplicate['event_type']}\n"
                    f"**Date:** {duplicate['event_date'][:10]}\n\n"
                    f"Choose how to proceed:"
                ),
                color=theme.emColor2
            )

            # Replace view with duplicate options
            self.clear_items()
            self.add_item(DuplicateMergeButton(self, duplicate['session_id']))
            self.add_item(DuplicateNewButton(self))
            self.add_item(CancelButton())

            await interaction.response.edit_message(embed=embed, view=self)
        else:
            # Update embed with selected event type
            high_matches = sum(1 for p in self.matched_players if p['match_type'] == 'high')
            low_matches = sum(1 for p in self.matched_players if p['match_type'] == 'low')
            no_matches = sum(1 for p in self.matched_players if p['match_type'] == 'none')

            embed = discord.Embed(
                title=f"{theme.searchIcon} Attendance OCR Results",
                description=(
                    f"**Alliance:** {self.alliance_name}\n"
                    f"**Event:** {EVENT_TYPE_ICONS.get(self.selected_event_type, '📋')} {self.selected_event_type}\n"
                    f"**Date:** {self.event_date.strftime('%Y-%m-%d')}\n"
                    f"**Players Found:** {len(self.matched_players)}\n\n"
                    f"{theme.verifiedIcon} **High Match:** {high_matches}\n"
                    f"{theme.warningIcon} **Low Match:** {low_matches}\n"
                    f"{theme.deniedIcon} **Unmatched:** {no_matches}"
                ),
                color=theme.emColor1
            )
            embed.set_footer(text="Review matches or submit directly")

            await interaction.response.edit_message(embed=embed, view=self)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "You cannot interact with this attendance session.",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Review Matches", style=discord.ButtonStyle.primary, row=1)
    async def review_matches(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show match review view."""
        view = MatchReviewView(self)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Submit", style=discord.ButtonStyle.success, row=1)
    async def submit_attendance(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Submit the attendance records."""
        await self._create_attendance_records(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cancel the attendance submission."""
        theme = get_current_theme()
        embed = discord.Embed(
            title=f"{theme.deniedIcon} Cancelled",
            description="Attendance submission cancelled.",
            color=theme.emColor2
        )
        await interaction.response.edit_message(embed=embed, view=None)

    async def _create_attendance_records(self, interaction: discord.Interaction, merge_session_id: str = None):
        """Create attendance records in the database."""
        theme = get_current_theme()

        await interaction.response.defer()

        try:
            # Generate or use existing session ID
            if merge_session_id:
                session_id = merge_session_id
                # Get existing session name
                self.cog.attendance_cursor.execute(
                    "SELECT session_name FROM attendance_records WHERE session_id = ? LIMIT 1",
                    (session_id,)
                )
                result = self.cog.attendance_cursor.fetchone()
                session_name = result[0] if result else f"{self.selected_event_type} - {self.event_date.strftime('%Y-%m-%d')}"
            else:
                session_id = str(uuid.uuid4())
                session_name = f"{self.selected_event_type} - {self.event_date.strftime('%Y-%m-%d')}"

            records_created = 0
            records_updated = 0
            unmatched_created = 0

            for player in self.matched_players:
                if player['matched_member']:
                    # Matched player
                    member = player['matched_member']
                    player_id = str(member['fid'])
                    player_name = member['nickname']
                else:
                    # Unmatched player - use OCR name with special ID
                    player_id = f"ocr_unmatched_{uuid.uuid4().hex[:8]}"
                    player_name = player['ocr_name']
                    unmatched_created += 1

                # Check if record exists (for merge)
                if merge_session_id:
                    self.cog.attendance_cursor.execute("""
                        SELECT record_id FROM attendance_records
                        WHERE session_id = ? AND player_id = ?
                    """, (session_id, player_id))
                    existing = self.cog.attendance_cursor.fetchone()

                    if existing:
                        # Update existing record
                        self.cog.attendance_cursor.execute("""
                            UPDATE attendance_records
                            SET status = 'present', points = ?, marked_at = ?
                            WHERE record_id = ?
                        """, (player['damage'], datetime.utcnow().isoformat(), existing[0]))
                        records_updated += 1
                        continue

                # Insert new record
                self.cog.attendance_cursor.execute("""
                    INSERT INTO attendance_records
                    (session_id, session_name, event_type, event_subtype, event_date,
                     player_id, player_name, alliance_id, alliance_name,
                     status, points, marked_at, marked_by, marked_by_username)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    session_id,
                    session_name,
                    self.selected_event_type,
                    self.selected_event_subtype,
                    self.event_date.isoformat(),
                    player_id,
                    player_name,
                    str(self.alliance_id),
                    self.alliance_name,
                    'present',
                    player['damage'],
                    datetime.utcnow().isoformat(),
                    str(interaction.user.id),
                    interaction.user.display_name
                ))
                records_created += 1

            self.cog.attendance_conn.commit()

            # Build success message
            summary_parts = []
            if records_created:
                summary_parts.append(f"**{records_created}** records created")
            if records_updated:
                summary_parts.append(f"**{records_updated}** records updated")
            if unmatched_created:
                summary_parts.append(f"**{unmatched_created}** unmatched players")

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Attendance Saved",
                description=(
                    f"**Session:** {session_name}\n"
                    f"**Event:** {EVENT_TYPE_ICONS.get(self.selected_event_type, '📋')} {self.selected_event_type}\n\n"
                    f"{' | '.join(summary_parts)}"
                ),
                color=theme.emColor3
            )

            if unmatched_created:
                embed.set_footer(text=f"⚠️ {unmatched_created} players couldn't be matched to alliance members")

            await interaction.followup.edit_message(interaction.message.id, embed=embed, view=None)

        except Exception as e:
            logger.error(f"Error creating attendance records: {e}")
            print(f"[ERROR] Error creating attendance records: {e}")
            await interaction.followup.send(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Error",
                    description=f"Failed to save attendance: {e}",
                    color=theme.emColor2
                ),
                ephemeral=True
            )


class MatchReviewView(discord.ui.View):
    """View for reviewing and correcting player matches."""

    def __init__(self, parent_view: AttendanceOCRConfirmView, page: int = 0):
        super().__init__(timeout=3600)
        self.parent_view = parent_view
        self.page = page
        self.items_per_page = 10

        total_pages = (len(parent_view.matched_players) - 1) // self.items_per_page + 1

        # Add navigation if needed
        if total_pages > 1:
            if self.page > 0:
                self.add_item(PrevPageButton())
            if self.page < total_pages - 1:
                self.add_item(NextPageButton())

    def build_embed(self) -> discord.Embed:
        """Build the match review embed."""
        theme = get_current_theme()

        start_idx = self.page * self.items_per_page
        end_idx = min(start_idx + self.items_per_page, len(self.parent_view.matched_players))
        page_players = self.parent_view.matched_players[start_idx:end_idx]

        lines = []
        for player in page_players:
            ocr_name = player['ocr_name']
            damage = player['damage']
            match_type = player['match_type']

            if match_type == 'high':
                icon = theme.verifiedIcon
                member = player['matched_member']
                match_info = f"→ **{member['nickname']}** ({player['match_score']:.0%})"
            elif match_type == 'low':
                icon = theme.warningIcon
                member = player['matched_member']
                match_info = f"→ **{member['nickname']}** ({player['match_score']:.0%}) ⚠️"
            else:
                icon = theme.deniedIcon
                match_info = "→ *No match found*"

            lines.append(f"{icon} `{ocr_name}` {match_info}\n   Damage: {damage:,}")

        total_pages = (len(self.parent_view.matched_players) - 1) // self.items_per_page + 1

        embed = discord.Embed(
            title=f"{theme.searchIcon} Match Review (Page {self.page + 1}/{total_pages})",
            description="\n\n".join(lines) if lines else "No players to display.",
            color=theme.emColor1
        )

        # Summary in footer
        high = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'high')
        low = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'low')
        none = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'none')
        embed.set_footer(text=f"✅ High: {high} | ⚠️ Low: {low} | ❌ None: {none}")

        return embed

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.parent_view.author_id:
            await interaction.response.send_message(
                "You cannot interact with this view.",
                ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Go back to main confirmation view."""
        theme = get_current_theme()

        high_matches = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'high')
        low_matches = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'low')
        no_matches = sum(1 for p in self.parent_view.matched_players if p['match_type'] == 'none')

        embed = discord.Embed(
            title=f"{theme.searchIcon} Attendance OCR Results",
            description=(
                f"**Alliance:** {self.parent_view.alliance_name}\n"
                f"**Event:** {EVENT_TYPE_ICONS.get(self.parent_view.selected_event_type, '📋')} {self.parent_view.selected_event_type}\n"
                f"**Date:** {self.parent_view.event_date.strftime('%Y-%m-%d')}\n"
                f"**Players Found:** {len(self.parent_view.matched_players)}\n\n"
                f"{theme.verifiedIcon} **High Match:** {high_matches}\n"
                f"{theme.warningIcon} **Low Match:** {low_matches}\n"
                f"{theme.deniedIcon} **Unmatched:** {no_matches}"
            ),
            color=theme.emColor1
        )

        await interaction.response.edit_message(embed=embed, view=self.parent_view)

    @discord.ui.button(label="Submit", style=discord.ButtonStyle.success, row=2)
    async def submit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Submit from review view."""
        await self.parent_view._create_attendance_records(interaction)


class PrevPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="◀ Previous", style=discord.ButtonStyle.secondary, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: MatchReviewView = self.view
        new_view = MatchReviewView(view.parent_view, view.page - 1)
        embed = new_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=new_view)


class NextPageButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Next ▶", style=discord.ButtonStyle.secondary, row=1)

    async def callback(self, interaction: discord.Interaction):
        view: MatchReviewView = self.view
        new_view = MatchReviewView(view.parent_view, view.page + 1)
        embed = new_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=new_view)


class DuplicateMergeButton(discord.ui.Button):
    def __init__(self, parent_view: AttendanceOCRConfirmView, session_id: str):
        super().__init__(label="Merge into Existing", style=discord.ButtonStyle.primary)
        self.parent_view = parent_view
        self.session_id = session_id

    async def callback(self, interaction: discord.Interaction):
        await self.parent_view._create_attendance_records(interaction, merge_session_id=self.session_id)


class DuplicateNewButton(discord.ui.Button):
    def __init__(self, parent_view: AttendanceOCRConfirmView):
        super().__init__(label="Create New Session", style=discord.ButtonStyle.secondary)
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        await self.parent_view._create_attendance_records(interaction)


class CancelButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Cancel", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        theme = get_current_theme()
        embed = discord.Embed(
            title=f"{theme.deniedIcon} Cancelled",
            description="Attendance submission cancelled.",
            color=theme.emColor2
        )
        await interaction.response.edit_message(embed=embed, view=None)


class AllianceSelectView(discord.ui.View):
    """View for selecting an alliance when configuring OCR channel."""

    def __init__(self, cog, alliances: list[tuple[int, str]], channel: discord.TextChannel, action: str, author_id: int):
        super().__init__(timeout=120)
        self.cog = cog
        self.channel = channel
        self.action = action
        self.author_id = author_id

        # Create alliance select
        options = [
            discord.SelectOption(label=name, value=str(alliance_id))
            for alliance_id, name in alliances
        ]

        select = discord.ui.Select(
            placeholder="Select an alliance...",
            options=options
        )
        select.callback = self.on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                "You cannot interact with this.",
                ephemeral=True
            )
            return False
        return True

    async def on_select(self, interaction: discord.Interaction):
        theme = get_current_theme()
        alliance_id = int(interaction.data['values'][0])

        # Get alliance name
        self.cog.alliance_cursor.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?",
            (alliance_id,)
        )
        result = self.cog.alliance_cursor.fetchone()
        alliance_name = result[0] if result else f"Alliance {alliance_id}"

        if self.action == "set":
            self.cog.alliance_cursor.execute(
                "UPDATE alliancesettings SET attendance_ocr_channel = ? WHERE alliance_id = ?",
                (self.channel.id, alliance_id)
            )
            self.cog.alliance_conn.commit()

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Channel Set",
                description=f"Attendance OCR channel for **{alliance_name}** set to {self.channel.mention}.\n\nPost screenshots with damage rankings to this channel to automatically track attendance.",
                color=theme.emColor3
            )
        else:  # remove
            self.cog.alliance_cursor.execute(
                "UPDATE alliancesettings SET attendance_ocr_channel = NULL WHERE alliance_id = ?",
                (alliance_id,)
            )
            self.cog.alliance_conn.commit()

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Channel Removed",
                description=f"Attendance OCR channel for **{alliance_name}** has been removed.",
                color=theme.emColor3
            )

        await interaction.response.edit_message(embed=embed, view=None)


async def setup(bot):
    await bot.add_cog(AttendanceOCR(bot))
