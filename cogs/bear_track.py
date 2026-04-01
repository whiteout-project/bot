"""
Bear damage tracking. Records, views, and charts bear hunt damage per alliance.
"""
import discord
from discord.ext import commands
from discord import app_commands
import io
import re
import os
import sqlite3
import logging
from datetime import datetime, date, timedelta, timezone
from cogs.attendance import MATPLOTLIB_AVAILABLE
from .pimp_my_bot import theme, safe_edit_message, check_interaction_user
from .permission_handler import PermissionManager
import numpy as np

logger = logging.getLogger('bot')

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
        logger.info("RapidOCR initialized successfully")
        print("[INFO] RapidOCR initialized for bear track OCR")
    except ImportError:
        logger.warning("rapidocr not installed. OCR will be disabled.")
        print("[WARNING] rapidocr not installed. Bear track OCR disabled.")
    except Exception as e:
        logger.error(f"Failed to initialize RapidOCR: {e}")
        print(f"[ERROR] Failed to initialize RapidOCR: {e}")

os.makedirs("db", exist_ok=True)


def init_bear_database():
    """Initialize bear_hunts table with proper settings."""
    db_path = "db/bear_data.sqlite"
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bear_hunts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alliance_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            hunting_trap INTEGER NOT NULL,
            rallies INTEGER,
            total_damage INTEGER,
            UNIQUE (alliance_id, date, hunting_trap)
        )
    """)
    conn.commit()
    conn.close()


init_bear_database()


def bear_damage(raw) -> int:
    """Clean bear score from different language formats."""
    if not raw:
        return 0
    clean = re.sub(r"[^\d]", "", str(raw))
    return int(clean) if clean else 0


def format_damage_for_embed(value) -> str:
    """Formats integer with commas for embed display."""
    try:
        cleaned = re.sub(r"[^\d]", "", str(value))
        if not cleaned:
            return "0"
        return f"{int(cleaned):,}"
    except Exception:
        return "0"


def validate_bear_submission(date_str, hunting_trap, rallies, total_damage):
    """Validate bear submission fields and return list of errors."""
    errors = []

    try:
        datetime.strptime(str(date_str), "%Y-%m-%d")
    except Exception:
        errors.append("Date must be in YYYY-MM-DD format.")

    try:
        hunting_trap = int(hunting_trap)
        if hunting_trap not in (1, 2):
            errors.append("Hunting trap must be 1 or 2.")
    except Exception:
        errors.append("Hunting trap must be a number (1 or 2).")

    try:
        rallies = int(rallies)
        if rallies <= 0:
            errors.append("Rallies must be a number greater than 0.")
    except Exception:
        errors.append("Rallies must be a whole number.")

    try:
        total_damage = int(total_damage)
        if total_damage <= 0:
            errors.append("Total damage must be a number greater than 0.")
    except Exception:
        errors.append("Total damage must be a whole number.")

    return errors


# ---------------------------------------------------------------------------
# bear_data_embed — chart generation
# ---------------------------------------------------------------------------

def bear_data_embed(
    *,
    alliance_id: int,
    alliance_name: str,
    hunting_trap: int,
    dates: list[datetime],
    rallies_list: list[int],
    total_damages: list[int],
    title_suffix: str | None = None,
    damage_range_days: int | None = None
):
    first_date = min(dates)
    last_date = max(dates)

    last_rallies = rallies_list[-1]
    last_damage = total_damages[-1]

    avg_rallies = int(sum(rallies_list) / len(rallies_list))
    avg_damage = int(sum(total_damages) / len(total_damages))

    rallies_diff = last_rallies - avg_rallies
    damage_diff = last_damage - avg_damage

    title = f"{alliance_name} Trap {hunting_trap}"
    if title_suffix:
        title += f" - {title_suffix}"

    embed = discord.Embed(title=title, color=theme.emColor1)

    embed.add_field(
        name="Date Range",
        value=f"{first_date:%Y-%m-%d} → {last_date:%Y-%m-%d}",
        inline=False
    )
    embed.add_field(name="Average Rallies", value=str(avg_rallies), inline=False)
    embed.add_field(name="Average Total Damage", value=f"{avg_damage:,}", inline=False)
    embed.add_field(name="Last Bear Rallies", value=str(last_rallies), inline=True)
    embed.add_field(name="Last Bear Damage", value=f"{last_damage:,}", inline=True)
    embed.add_field(name="Difference in Rallies", value=f"{rallies_diff:+d}", inline=True)
    embed.add_field(name="Difference in Damage", value=f"{damage_diff:+,}", inline=True)

    if damage_range_days and damage_range_days > 0:
        embed.set_footer(text=f"Showing last {damage_range_days} days of damage")
    else:
        embed.set_footer(text="Showing all historical damage records")

    image_file = None

    if MATPLOTLIB_AVAILABLE:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        from matplotlib.ticker import MaxNLocator

        try:
            plt.style.use("fivethirtyeight")
            plt.figure(figsize=(10, 7), facecolor="#1a1a2d")

            plt.plot(dates, total_damages, marker='o', linewidth=3)

            ax = plt.gca()
            ax.set_facecolor("#1a1a2d")
            for spine in ax.spines.values():
                spine.set_visible(False)

            plt.title(
                f"{alliance_name} Total Damage Over Time - Trap {hunting_trap}",
                color="#99c2ff",
                fontfamily="sans-serif",
                fontweight="bold",
                fontsize=16,
                loc="left",
                pad=30
            )

            plt.ylabel("Total Damage", color="white", fontsize=12, fontweight="bold", labelpad=15)
            plt.yticks(color="white")

            ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
            ax.xaxis.set_major_locator(MaxNLocator(nbins=15))
            plt.xticks(rotation=45, color="white")

            def damage_formatter(x, pos):
                try:
                    x = float(x)
                    if x >= 1_000_000_000_000:
                        val = x / 1_000_000_000_000
                        return f"{int(val)}T" if val.is_integer() else f"{val:.1f}T"
                    elif x >= 1_000_000_000:
                        val = x / 1_000_000_000
                        return f"{int(val)}B" if val.is_integer() else f"{val:.1f}B"
                    elif x >= 1_000_000:
                        val = x / 1_000_000
                        return f"{int(val)}M" if val.is_integer() else f"{val:.1f}M"
                    else:
                        return f"{int(x)}"
                except Exception:
                    return str(x)

            ax.yaxis.set_major_formatter(plt.matplotlib.ticker.FuncFormatter(damage_formatter))

            plt.tight_layout()

            buffer = io.BytesIO()
            plt.savefig(buffer, format="png", dpi=200, transparent=True)
            plt.close()

            buffer.seek(0)
            image_file = discord.File(buffer, filename="plot.png")
            embed.set_image(url="attachment://plot.png")

        except Exception as e:
            logger.error(f"Failed to generate chart: {e}")
            print(f"[ERROR] Failed to generate chart: {e}")

    return embed, image_file


# ---------------------------------------------------------------------------
# BearTrack cog
# ---------------------------------------------------------------------------

class BearTrack(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Persistent DB connections with WAL mode
        self.alliance_conn = sqlite3.connect("db/alliance.sqlite", timeout=30.0, check_same_thread=False)
        self.alliance_conn.execute("PRAGMA journal_mode=WAL")
        self.alliance_conn.execute("PRAGMA synchronous=NORMAL")
        self.alliance_conn.commit()
        self.alliance_cursor = self.alliance_conn.cursor()

        self.bear_conn = sqlite3.connect("db/bear_data.sqlite", timeout=30.0, check_same_thread=False)
        self.bear_conn.execute("PRAGMA journal_mode=WAL")
        self.bear_conn.execute("PRAGMA synchronous=NORMAL")
        self.bear_conn.commit()
        self.bear_cursor = self.bear_conn.cursor()

        # Ensure required columns exist on alliancesettings
        self.alliance_cursor.execute("PRAGMA table_info(alliancesettings)")
        columns = [col[1] for col in self.alliance_cursor.fetchall()]

        new_columns = {
            "bear_score_channel": "INTEGER",
            "bear_keywords": "TEXT",
            "bear_damage_range": "INTEGER DEFAULT 0",
            "bear_admin_only_view": "INTEGER DEFAULT 0",
            "bear_admin_only_add": "INTEGER DEFAULT 0",
        }
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                self.alliance_cursor.execute(
                    f"ALTER TABLE alliancesettings ADD COLUMN {col_name} {col_type}"
                )

        self.alliance_conn.commit()

        # DataSubmit helper with shared connections
        self.data_submit = DataSubmit(self.alliance_conn, self.bear_conn)

    def cog_unload(self):
        if hasattr(self, 'alliance_conn'):
            self.alliance_conn.close()
        if hasattr(self, 'bear_conn'):
            self.bear_conn.close()

    # -------------------------------------------------------------------
    # Settings helpers (column-based, not JSON)
    # -------------------------------------------------------------------

    def get_bear_settings(self, alliance_id: int) -> dict:
        """Return bear settings dict from individual columns."""
        self.alliance_cursor.execute(
            "SELECT bear_score_channel, bear_keywords, bear_damage_range, "
            "bear_admin_only_view, bear_admin_only_add "
            "FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,)
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return {
                "channel_id": None,
                "keywords": [],
                "damage_range": 0,
                "admin_only_view": 0,
                "admin_only_add": 0,
            }
        return {
            "channel_id": row[0],
            "keywords": [kw.strip() for kw in row[1].split(",") if kw.strip()] if row[1] else [],
            "damage_range": row[2] or 0,
            "admin_only_view": row[3] or 0,
            "admin_only_add": row[4] or 0,
        }

    def update_bear_setting(self, alliance_id: int, column: str, value):
        """Update a single bear setting column."""
        allowed = {"bear_score_channel", "bear_keywords", "bear_damage_range",
                    "bear_admin_only_view", "bear_admin_only_add"}
        if column not in allowed:
            return
        self.alliance_cursor.execute(
            f"UPDATE alliancesettings SET {column} = ? WHERE alliance_id = ?",
            (value, alliance_id)
        )
        self.alliance_conn.commit()

    async def get_keywords_for_channel(self, channel_id: int) -> list:
        """Return keywords list for the alliance that has this bear channel."""
        self.alliance_cursor.execute(
            "SELECT bear_keywords FROM alliancesettings WHERE bear_score_channel = ?",
            (channel_id,)
        )
        result = self.alliance_cursor.fetchone()
        if result and result[0]:
            return [kw.strip() for kw in result[0].split(",") if kw.strip()]
        return []

    # -------------------------------------------------------------------
    # Permission check
    # -------------------------------------------------------------------

    async def check_bear_permission(self, interaction: discord.Interaction, alliance_id: int, action: str) -> bool:
        """
        Check if user has permission for an action on bear data.
        Actions: "view", "add", "manage"
        """
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)

        if action == "manage":
            if is_global:
                return True
            if is_admin:
                alliance_ids, _ = PermissionManager.get_admin_alliance_ids(
                    interaction.user.id, interaction.guild_id if interaction.guild else 0
                )
                if alliance_id in alliance_ids:
                    return True
            await interaction.response.send_message(
                f"{theme.deniedIcon} You don't have permission to manage settings for this alliance.",
                ephemeral=True
            )
            return False

        settings = self.get_bear_settings(alliance_id)
        key = "admin_only_add" if action == "add" else "admin_only_view"
        only_admin = settings.get(key, 0)

        if not only_admin:
            return True

        if is_global:
            return True
        if is_admin:
            alliance_ids, _ = PermissionManager.get_admin_alliance_ids(
                interaction.user.id, interaction.guild_id if interaction.guild else 0
            )
            if alliance_id in alliance_ids:
                return True

        await interaction.response.send_message(
            f"{theme.deniedIcon} You don't have permission to {action} bear damage for this alliance.",
            ephemeral=True
        )
        return False

    # -------------------------------------------------------------------
    # Autocomplete helpers
    # -------------------------------------------------------------------

    async def alliance_autocomplete(self, interaction: discord.Interaction, current: str):
        self.alliance_cursor.execute(
            "SELECT alliance_id, name FROM alliance_list WHERE name LIKE ? ORDER BY name LIMIT 20",
            (f"%{current}%",)
        )
        rows = self.alliance_cursor.fetchall()
        return [
            discord.app_commands.Choice(name=row[1], value=str(row[0]))
            for row in rows
        ]

    async def hunting_trap_autocomplete(self, interaction: discord.Interaction, current: str):
        return [
            discord.app_commands.Choice(name="1", value=1),
            discord.app_commands.Choice(name="2", value=2),
        ]

    # -------------------------------------------------------------------
    # on_message — OCR processing
    # -------------------------------------------------------------------

    @commands.Cog.listener()
    async def on_message(self, message):
        if message.author.bot:
            return
        if not message.content.strip() and not message.attachments:
            return

        # Check if channel is a bear_score_channel and get keywords in one query
        self.alliance_cursor.execute(
            "SELECT bear_keywords FROM alliancesettings WHERE bear_score_channel = ?",
            (message.channel.id,)
        )
        row = self.alliance_cursor.fetchone()
        if not row:
            return

        keywords = [kw.strip() for kw in row[0].split(",") if kw.strip()] if row[0] else []
        if keywords and not any(kw.lower() in message.content.lower() for kw in keywords):
            return

        await self.process_bear_hunt_data(message)

    async def process_bear_hunt_data(self, message):
        """Process bear hunt image data using RapidOCR."""
        if not message.attachments:
            return

        for attachment in message.attachments[:1]:
            if not any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg']):
                continue

            try:
                image_data = await attachment.read()
                image = Image.open(io.BytesIO(image_data))

                # RapidOCR
                if OCR_AVAILABLE:
                    try:
                        image_array = np.array(image.convert('RGB'))
                        result = rapid_ocr(image_array)

                        if result:
                            if hasattr(result, 'txts') and result.txts:
                                extracted_text = " ".join(result.txts)
                            elif hasattr(result, '__iter__'):
                                texts = []
                                for item in result:
                                    if isinstance(item, (list, tuple)) and len(item) >= 2:
                                        texts.append(str(item[1]))
                                extracted_text = " ".join(texts) if texts else str(result)
                            else:
                                extracted_text = str(result)
                            ocr_success = bool(extracted_text.strip())
                        else:
                            extracted_text = ""
                            ocr_success = False
                    except Exception as e:
                        logger.warning(f"OCR failed: {e}")
                        print(f"[WARNING] OCR failed: {e}")
                        extracted_text = ""
                        ocr_success = False
                else:
                    extracted_text = ""
                    ocr_success = False

                # Multi-language variants
                trap_variants = ["Hunting Trap", "Piege de Chasse", "Jagdfalle"]
                rallies_variants = ["Rallies", "Ralliements", "Rallys"]
                damage_variants = ["Alliance Damage", "Degats Totaux de I'Alliance", "Gesamt Allianzschaden"]

                trap_pattern = r"(?:{})\s*\n*\s*(\d+)".format("|".join(map(re.escape, trap_variants)))
                rallies_pattern = r"(?:{})[:\s]*(\d+)".format("|".join(map(re.escape, rallies_variants)))
                damage_pattern = r"(?:{})[:\s]*([\d,\.\s]+)".format("|".join(map(re.escape, damage_variants)))

                today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                hunting_trap_value = rallies_value = ""
                damage_int = 0

                if ocr_success:
                    clean_text = re.sub(r"\s+", " ", extracted_text)

                    hunting_trap_match = re.search(trap_pattern, clean_text)
                    rallies_match = re.search(rallies_pattern, extracted_text)
                    total_damage_match = re.search(damage_pattern, extracted_text)

                    hunting_trap_value = hunting_trap_match.group(1) if hunting_trap_match else ""
                    rallies_value = rallies_match.group(1) if rallies_match else ""
                    raw_damage = total_damage_match.group(1) if total_damage_match else ""
                    damage_int = bear_damage(raw_damage)
                    total_damage_display = format_damage_for_embed(damage_int)

                    embed_title = f"{theme.chartIcon} Extracted Data"
                else:
                    total_damage_display = ""
                    embed_title = f"{theme.warnIcon} OCR could not read the image. Please fill in the values manually."

                embed = discord.Embed(title=embed_title, color=theme.emColor1)
                embed.add_field(name="Date", value=today_date, inline=False)
                embed.add_field(name="Hunting Trap", value=hunting_trap_value or "-", inline=True)
                embed.add_field(name="Rallies", value=rallies_value or "-", inline=True)
                embed.add_field(name="Total Alliance Damage", value=total_damage_display or "-", inline=False)

                view = ExtractedDataView(
                    data_submit=self.data_submit,
                    date=today_date,
                    hunting_trap=hunting_trap_value,
                    rallies=rallies_value,
                    damage_int=damage_int,
                    original_user_id=message.author.id
                )

                await message.channel.send(embed=embed, view=view)

            except Exception as e:
                logger.error(f"Exception while processing image: {e}")
                print(f"[ERROR] Exception while processing image: {e}")
                await message.channel.send(f"{theme.deniedIcon} Error processing image: {e}")

    # -------------------------------------------------------------------
    # Slash commands
    # -------------------------------------------------------------------

    @app_commands.command(name="bear_damage_add", description="Manually add bear hunt damage data")
    @app_commands.autocomplete(alliance=alliance_autocomplete, hunting_trap=hunting_trap_autocomplete)
    @app_commands.describe(
        alliance="Alliance name",
        hunting_trap="Hunting trap number",
        rallies="Number of rallies",
        total_damage="Total alliance damage",
        date="UTC date (YYYY-MM-DD). Defaults to today."
    )
    async def bear_damage_add(self, interaction: discord.Interaction, alliance: str, hunting_trap: int,
                              rallies: int, total_damage: int, date: str | None = None):
        try:
            alliance_id = int(alliance)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid alliance selected.", ephemeral=True
            )
            return

        allowed = await self.check_bear_permission(interaction, alliance_id, "add")
        if not allowed:
            return

        if not date:
            date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        self.alliance_cursor.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?",
            (alliance_id,)
        )
        row = self.alliance_cursor.fetchone()
        alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

        errors = validate_bear_submission(date, hunting_trap, rallies, total_damage)
        if errors:
            msg = f"{theme.deniedIcon} Submission failed:\n" + "\n".join(f"- {e}" for e in errors)
            await interaction.response.send_message(msg, ephemeral=True)
            return

        await self.data_submit.process_submission(
            interaction,
            date=date,
            hunting_trap=hunting_trap,
            rallies=rallies,
            total_damage=total_damage,
            alliance_id=alliance_id,
            alliance_name=alliance_name
        )

    @app_commands.command(name="bear_damage_view", description="View bear damage for an alliance")
    @app_commands.autocomplete(alliance=alliance_autocomplete, hunting_trap=hunting_trap_autocomplete)
    @app_commands.describe(
        alliance="Select an alliance",
        hunting_trap="Hunting trap number",
        from_date="Start date (YYYY-MM-DD)",
        to_date="End date (YYYY-MM-DD)"
    )
    async def bear_damage_view(self, interaction: discord.Interaction, alliance: str, hunting_trap: int,
                               from_date: str | None = None, to_date: str | None = None):
        try:
            alliance_id = int(alliance)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid alliance selected.", ephemeral=True
            )
            return

        allowed = await self.check_bear_permission(interaction, alliance_id, "view")
        if not allowed:
            return

        await interaction.response.defer()

        view = BearDamageView(
            data_submit=self.data_submit,
            cog=self,
            original_user_id=interaction.user.id,
            alliance_id=alliance_id,
            hunting_trap=hunting_trap,
            from_date=datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None,
            to_date=datetime.strptime(to_date, "%Y-%m-%d").date() if to_date else None
        )

        embed, file = await self.data_submit.process_view(
            alliance_id=alliance_id,
            hunting_trap=hunting_trap,
            from_date=from_date,
            to_date=to_date,
        )

        if not embed:
            await interaction.followup.send(
                f"{theme.deniedIcon} No data found for the selected parameters.", ephemeral=True
            )
            return

        await interaction.followup.send(embed=embed, file=file if file else None, view=view)

    # -------------------------------------------------------------------
    # Main menu entry point
    # -------------------------------------------------------------------

    async def show_bear_track_menu(self, interaction: discord.Interaction):
        """Display the bear damage tracking main menu."""
        try:
            view = BearMenuView(cog=self, original_user_id=interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.chartIcon} Bear Damage Tracking",
                description=(
                    f"Track your alliance's bear damage over time and view trends.\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.chartIcon} **View Bear Damage**\n"
                    f"  Select an alliance and date range to see a damage graph\n\n"
                    f"{theme.editListIcon} **Edit Bear Damage**\n"
                    f"  Edit or delete saved damage records for your alliances\n\n"
                    f"{theme.settingsIcon} **Settings**\n"
                    f"  Configure bear channel, keywords, damage range, and permissions\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_bear_track_menu: {e}")
            print(f"[ERROR] Error in show_bear_track_menu: {e}")
            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Failed to load Bear Tracking menu.", ephemeral=True
                    )
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class ExtractedDataView(discord.ui.View):
    """OCR result confirmation view — submit, edit, or cancel extracted data."""

    def __init__(self, data_submit, date, hunting_trap, rallies, damage_int, original_user_id):
        super().__init__(timeout=7200)
        self.data_submit = data_submit
        self.date = date
        self.hunting_trap = hunting_trap
        self.rallies = rallies
        self.total_damage = damage_int
        self.original_user_id = original_user_id

    @discord.ui.button(label="Submit", style=discord.ButtonStyle.green)
    async def submit_data(self, interaction: discord.Interaction, button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        total_damage_int = bear_damage(self.total_damage) if isinstance(self.total_damage, str) else self.total_damage

        try:
            dt = datetime.strptime(self.date, "%Y-%m-%d")
            normalized_date = dt.strftime("%Y-%m-%d")
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid date format. Must be YYYY-MM-DD.", ephemeral=True
            )
            return

        self.date = normalized_date

        errors = validate_bear_submission(self.date, self.hunting_trap, self.rallies, total_damage_int)
        if errors:
            msg = f"{theme.deniedIcon} Submission failed:\n" + "\n".join(f"- {e}" for e in errors)
            await interaction.response.send_message(msg, ephemeral=True)
            return

        try:
            await self.data_submit.process_submission(
                interaction,
                self.date,
                self.hunting_trap,
                self.rallies,
                total_damage_int
            )
        except Exception as e:
            logger.error(f"Error in submit_data: {e}")
            print(f"[ERROR] Error in submit_data: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error during submission: {e}", ephemeral=True
            )

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary)
    async def edit_data(self, interaction: discord.Interaction, button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(EditModal(self))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel_data(self, interaction: discord.Interaction, button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        embed = discord.Embed(
            description=f"{theme.deniedIcon} Data entry canceled.",
            color=theme.emColor2
        )
        await interaction.response.edit_message(content=None, embed=embed, view=None)


class EditModal(discord.ui.Modal):
    """Modal for editing OCR-extracted bear data before submission."""

    def __init__(self, view: ExtractedDataView):
        super().__init__(title="Edit Bear Data")
        self.parent_view = view

        self.date_input = discord.ui.TextInput(label="Date", default=view.date)
        self.hunting_trap_input = discord.ui.TextInput(label="Hunting Trap", default=str(view.hunting_trap))
        self.rallies_input = discord.ui.TextInput(label="Rallies", default=str(view.rallies))
        self.total_damage_input = discord.ui.TextInput(
            label="Total Damage", default=format_damage_for_embed(view.total_damage)
        )

        self.add_item(self.date_input)
        self.add_item(self.hunting_trap_input)
        self.add_item(self.rallies_input)
        self.add_item(self.total_damage_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            dt = datetime.strptime(self.date_input.value, "%Y-%m-%d")
            date_normalized = dt.strftime("%Y-%m-%d")
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid date format. Must be YYYY-MM-DD.", ephemeral=True
            )
            return

        try:
            hunting_trap = int(self.hunting_trap_input.value)
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hunting Trap must be a number (1 or 2).", ephemeral=True
            )
            return

        try:
            rallies = int(self.rallies_input.value)
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Rallies must be a whole number.", ephemeral=True
            )
            return

        self.parent_view.date = date_normalized
        self.parent_view.hunting_trap = hunting_trap
        self.parent_view.rallies = rallies
        self.parent_view.total_damage = bear_damage(self.total_damage_input.value)

        embed = discord.Embed(title=f"{theme.editListIcon} Edited Data", color=theme.emColor1)
        embed.add_field(name="Date", value=self.parent_view.date, inline=False)
        embed.add_field(name="Hunting Trap", value=self.parent_view.hunting_trap, inline=True)
        embed.add_field(name="Rallies", value=self.parent_view.rallies, inline=True)
        embed.add_field(
            name="Total Alliance Damage",
            value=format_damage_for_embed(self.parent_view.total_damage),
            inline=False
        )

        await interaction.response.edit_message(embed=embed, view=self.parent_view)


# ---------------------------------------------------------------------------
# BearMenuView — main navigation
# ---------------------------------------------------------------------------

class BearMenuView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id

    @discord.ui.button(label="View Bear Damage", style=discord.ButtonStyle.primary, emoji=theme.chartIcon, row=1)
    async def view_bear_damage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        view = BearDamageView(
            data_submit=self.cog.data_submit,
            cog=self.cog,
            original_user_id=self.original_user_id,
        )

        embed = discord.Embed(
            title=f"{theme.chartIcon} Bear Damage Viewer",
            description=(
                f"Select an alliance, trap, and date range to view damage.\n"
                f"{theme.upperDivider}\n"
                f"Use the dropdown to pick an alliance, then choose a trap and date range.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Edit Bear Damage", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=1)
    async def edit_bear_damage(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You need admin permissions to edit bear damage records.",
                ephemeral=True
            )
            return

        view = BearDamageEditView(
            cog=self.cog,
            original_user_id=self.original_user_id,
        )

        embed = discord.Embed(
            title=f"{theme.editListIcon} Edit Bear Damage",
            description=(
                f"Select an alliance to view and manage its damage records.\n"
                f"{theme.upperDivider}\n"
                f"Pick an alliance from the dropdown, then select a record to edit or delete.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Settings", style=discord.ButtonStyle.primary, emoji=theme.settingsIcon, row=2)
    async def settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You need admin permissions to access bear settings.",
                ephemeral=True
            )
            return

        view = BearSettingsView(cog=self.cog, original_user_id=self.original_user_id)
        embed = view._build_settings_embed()
        await safe_edit_message(interaction, embed=embed, view=view, content=None)

    @discord.ui.button(label="Main Menu", style=discord.ButtonStyle.secondary, emoji=theme.homeIcon, row=2)
    async def main_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        main_menu_cog = self.cog.bot.get_cog("MainMenu")
        if main_menu_cog:
            await main_menu_cog.show_main_menu(interaction)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Main menu not available.", ephemeral=True
            )


# ---------------------------------------------------------------------------
# AllianceSelect — reusable alliance dropdown
# ---------------------------------------------------------------------------

class AllianceSelect(discord.ui.Select):
    def __init__(self, parent_view, options: list[discord.SelectOption], action: str):
        self.parent_view = parent_view
        self.action = action
        for opt in options:
            opt.default = (parent_view.alliance_id is not None and int(opt.value) == parent_view.alliance_id)

        super().__init__(
            placeholder="Select an alliance",
            min_values=1,
            max_values=1,
            options=options if options else [discord.SelectOption(label="No alliances", value="__none__")]
        )

    async def callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.parent_view.original_user_id):
            return
        try:
            new_alliance_id = int(self.values[0])

            allowed = await self.parent_view.cog.check_bear_permission(
                interaction, new_alliance_id, self.action
            )
            if not allowed:
                return

            self.parent_view.alliance_id = new_alliance_id

            for opt in self.options:
                opt.default = (int(opt.value) == self.parent_view.alliance_id)

            if hasattr(self.parent_view, "on_alliance_selected"):
                await self.parent_view.on_alliance_selected(interaction)
            elif hasattr(self.parent_view, "try_redraw"):
                await self.parent_view.try_redraw(interaction)

        except Exception as e:
            logger.error(f"Error in AllianceSelect callback: {e}")
            print(f"[ERROR] Error in AllianceSelect callback: {e}")
            try:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Error processing alliance selection.", ephemeral=True
                )
            except Exception:
                pass


def build_alliance_options(alliance_conn) -> list[discord.SelectOption]:
    """Build alliance select options from DB."""
    cursor = alliance_conn.cursor()
    cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name ASC LIMIT 25")
    rows = cursor.fetchall()
    return [discord.SelectOption(label=name, value=str(aid)) for aid, name in rows]


# ---------------------------------------------------------------------------
# BearDamageView — view damage graph with filters
# ---------------------------------------------------------------------------

class BearDamageView(discord.ui.View):
    def __init__(self, data_submit, *, cog, original_user_id,
                 alliance_id: int | None = None, hunting_trap: int | None = None,
                 from_date: date | None = None, to_date: date | None = None):
        super().__init__(timeout=7200)
        self.data_submit = data_submit
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id = alliance_id
        self.hunting_trap = hunting_trap
        self.from_date = from_date
        self.to_date = to_date

        options = build_alliance_options(cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="view"))

    def is_ready(self) -> bool:
        return all([self.alliance_id, self.hunting_trap, self.from_date, self.to_date])

    def missing_inputs(self) -> list[str]:
        missing = []
        if not self.alliance_id:
            missing.append("alliance")
        if not self.hunting_trap:
            missing.append("trap")
        if not self.from_date or not self.to_date:
            missing.append("date range")
        return missing

    async def try_redraw(self, interaction: discord.Interaction):
        if not self.is_ready():
            missing = ", ".join(self.missing_inputs())
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select: **{missing}** to draw the graph.",
                ephemeral=True
            )
            return

        embed, file = await self.data_submit.process_view(
            alliance_id=self.alliance_id,
            hunting_trap=self.hunting_trap,
            from_date=self.from_date.strftime("%Y-%m-%d"),
            to_date=self.to_date.strftime("%Y-%m-%d"),
        )

        if not embed:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No data found for the selected parameters.",
                ephemeral=True
            )
            return

        await interaction.response.edit_message(
            embed=embed, attachments=[file] if file else [], view=self
        )

    @discord.ui.button(label="Date Range", style=discord.ButtonStyle.primary, emoji=theme.calendarIcon, row=2)
    async def date_range(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await interaction.response.send_modal(DateRangeModal(self))

    @discord.ui.button(label="Trap 1", style=discord.ButtonStyle.secondary, emoji=theme.bearTrapIcon, row=2)
    async def trap_1_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self._select_trap(interaction, 1)

    @discord.ui.button(label="Trap 2", style=discord.ButtonStyle.secondary, emoji=theme.bearTrapIcon, row=2)
    async def trap_2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self._select_trap(interaction, 2)

    async def _select_trap(self, interaction: discord.Interaction, trap_number: int):
        if self.hunting_trap == trap_number:
            await interaction.response.send_message(
                f"{theme.warnIcon} Already showing Trap {trap_number}.",
                ephemeral=True
            )
            return
        self.hunting_trap = trap_number
        await self.try_redraw(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=2)
    async def back_to_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


# ---------------------------------------------------------------------------
# BearDamageEditView — edit/delete records
# ---------------------------------------------------------------------------

class BearDamageEditView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id

        self.alliance_id: int | None = None
        self.selected_record_id: int | None = None
        self.date: str | None = None
        self.hunting_trap: int | None = None
        self.rallies: int | None = None
        self.total_damage: int | None = None

        options = build_alliance_options(cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="manage"))

        self.date_trap_select = discord.ui.Select(
            placeholder="Select a record",
            min_values=1,
            max_values=1,
            options=[discord.SelectOption(label="Select an alliance first", value="__placeholder__")],
            disabled=True
        )
        self.date_trap_select.callback = self.date_trap_selected
        self.add_item(self.date_trap_select)

    def build_record_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{theme.editListIcon} Bear Damage Record",
            color=theme.emColor1
        )
        embed.add_field(name="Date", value=self.date or "-", inline=False)
        embed.add_field(name="Hunting Trap", value=self.hunting_trap or "-", inline=True)
        embed.add_field(name="Rallies", value=self.rallies or "-", inline=True)
        embed.add_field(
            name="Total Alliance Damage",
            value=format_damage_for_embed(self.total_damage) if self.total_damage else "-",
            inline=False
        )
        return embed

    async def date_trap_selected(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        selected_value = self.date_trap_select.values[0]
        if selected_value in ("__placeholder__", "__none__"):
            return

        self.selected_record_id = int(selected_value)

        try:
            row = self.cog.bear_cursor.execute(
                "SELECT date, hunting_trap, rallies, total_damage FROM bear_hunts WHERE id = ?",
                (self.selected_record_id,)
            ).fetchone()
        except Exception as e:
            logger.error(f"Failed to fetch record: {e}")
            print(f"[ERROR] Failed to fetch record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to fetch record.", ephemeral=True
            )
            return

        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Record not found.", ephemeral=True
            )
            return

        self.date, self.hunting_trap, self.rallies, self.total_damage = row

        # Update defaults on the select
        self._refresh_record_select_defaults()

        await interaction.response.edit_message(content=None, view=self, embed=self.build_record_embed())

    def _refresh_record_select_defaults(self):
        for opt in self.date_trap_select.options:
            if opt.value in ("__placeholder__", "__none__"):
                continue
            opt.default = (int(opt.value) == self.selected_record_id) if self.selected_record_id else False

    async def on_alliance_selected(self, interaction: discord.Interaction):
        """Called by AllianceSelect when an alliance is picked."""
        try:
            rows = self.cog.bear_cursor.execute(
                "SELECT id, hunting_trap, date FROM bear_hunts WHERE alliance_id = ? ORDER BY date DESC LIMIT 25",
                (self.alliance_id,)
            ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch records: {e}")
            print(f"[ERROR] Failed to fetch records: {e}")
            rows = []

        if not rows:
            self.date_trap_select.options = [discord.SelectOption(label="No records found", value="__none__")]
            self.date_trap_select.disabled = True
        else:
            self.date_trap_select.options = [
                discord.SelectOption(
                    label=f"{dt} - Trap {trap}",
                    value=str(row_id),
                    default=(self.selected_record_id == row_id) if self.selected_record_id else False
                )
                for row_id, trap, dt in rows
            ]
            self.date_trap_select.disabled = False

        self.selected_record_id = None
        self.date = self.hunting_trap = self.rallies = self.total_damage = None

        await interaction.response.edit_message(content=None, view=self, embed=self.build_record_embed())

    @discord.ui.button(label="Filter Records", style=discord.ButtonStyle.primary, emoji=theme.searchIcon, row=2)
    async def filter_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select an alliance first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        await interaction.response.send_modal(RecordFilterModal(self))

    @discord.ui.button(label="Edit", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=2)
    async def edit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id or not self.selected_record_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Select an alliance and record first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        await interaction.response.send_modal(RecordEditModal(self))

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.danger, emoji=theme.trashIcon, row=2)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        if not self.alliance_id or not self.selected_record_id:
            await interaction.response.send_message(
                f"{theme.warnIcon} Select an alliance and record first.", ephemeral=True
            )
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        # Delete the record
        try:
            self.cog.bear_cursor.execute(
                "DELETE FROM bear_hunts WHERE id = ?", (self.selected_record_id,)
            )
            self.cog.bear_conn.commit()
        except Exception as e:
            logger.error(f"Failed to delete record: {e}")
            print(f"[ERROR] Failed to delete record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to delete record.", ephemeral=True
            )
            return

        self.selected_record_id = None
        self.date = self.hunting_trap = self.rallies = self.total_damage = None

        # Refresh record list
        await self.on_alliance_selected(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=2)
    async def back_to_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


class RecordFilterModal(discord.ui.Modal):
    """Filter damage records by trap, month, year."""

    def __init__(self, parent_view: BearDamageEditView):
        super().__init__(title="Filter Damage Records")
        self.parent_view = parent_view

        self.trap_input = discord.ui.TextInput(
            label="Trap Number (1 or 2)", required=False,
            placeholder="Leave empty for no trap filter"
        )
        self.month_input = discord.ui.TextInput(
            label="Month (1-12)", required=False,
            placeholder="Leave empty for no month filter"
        )
        self.year_input = discord.ui.TextInput(
            label="Year (YYYY)", required=False,
            placeholder="Leave empty for no year filter"
        )
        self.add_item(self.trap_input)
        self.add_item(self.month_input)
        self.add_item(self.year_input)

    async def on_submit(self, interaction: discord.Interaction):
        trap_val = self.trap_input.value.strip()
        month_val = self.month_input.value.strip()
        year_val = self.year_input.value.strip()

        filters = []
        params = [self.parent_view.alliance_id]

        if trap_val:
            filters.append("hunting_trap = ?")
            params.append(trap_val)
        if month_val:
            filters.append("strftime('%m', date) = ?")
            params.append(month_val.zfill(2))
        if year_val:
            filters.append("strftime('%Y', date) = ?")
            params.append(year_val)

        where_extra = (" AND " + " AND ".join(filters)) if filters else ""

        try:
            rows = self.parent_view.cog.bear_cursor.execute(
                f"SELECT id, hunting_trap, date FROM bear_hunts "
                f"WHERE alliance_id = ?{where_extra} ORDER BY date DESC LIMIT 25",
                params
            ).fetchall()
        except Exception as e:
            logger.error(f"Failed to fetch filtered records: {e}")
            print(f"[ERROR] Failed to fetch filtered records: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to fetch filtered records.", ephemeral=True
            )
            return

        if not rows:
            self.parent_view.date_trap_select.options = [
                discord.SelectOption(label="No records found", value="__none__")
            ]
            self.parent_view.date_trap_select.disabled = True
        else:
            self.parent_view.date_trap_select.options = [
                discord.SelectOption(label=f"{dt} - Trap {trap}", value=str(row_id))
                for row_id, trap, dt in rows
            ]
            self.parent_view.date_trap_select.disabled = False

        self.parent_view.selected_record_id = None
        self.parent_view.date = self.parent_view.hunting_trap = None
        self.parent_view.rallies = self.parent_view.total_damage = None

        await interaction.response.edit_message(
            view=self.parent_view, embed=self.parent_view.build_record_embed()
        )


class RecordEditModal(discord.ui.Modal):
    """Modal for editing an existing bear damage record in the database."""

    def __init__(self, parent_view: BearDamageEditView):
        super().__init__(title="Edit Bear Record")
        self.parent_view = parent_view

        self.date_input = discord.ui.TextInput(
            label="Date", default=parent_view.date or ""
        )
        self.hunting_trap_input = discord.ui.TextInput(
            label="Hunting Trap", default=str(parent_view.hunting_trap or "")
        )
        self.rallies_input = discord.ui.TextInput(
            label="Rallies", default=str(parent_view.rallies or "")
        )
        self.total_damage_input = discord.ui.TextInput(
            label="Total Damage",
            default=format_damage_for_embed(parent_view.total_damage) if parent_view.total_damage else ""
        )
        self.add_item(self.date_input)
        self.add_item(self.hunting_trap_input)
        self.add_item(self.rallies_input)
        self.add_item(self.total_damage_input)

    async def on_submit(self, interaction: discord.Interaction):
        # Validate
        try:
            dt = datetime.strptime(self.date_input.value, "%Y-%m-%d")
            new_date = dt.strftime("%Y-%m-%d")
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Date must be in YYYY-MM-DD format.", ephemeral=True
            )
            return

        try:
            new_trap = int(self.hunting_trap_input.value)
            if new_trap not in (1, 2):
                raise ValueError
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hunting Trap must be 1 or 2.", ephemeral=True
            )
            return

        try:
            new_rallies = int(self.rallies_input.value)
            if new_rallies <= 0:
                raise ValueError
        except Exception:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Rallies must be a whole number greater than 0.", ephemeral=True
            )
            return

        new_damage = bear_damage(self.total_damage_input.value)
        if new_damage <= 0:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Total Damage must be greater than 0.", ephemeral=True
            )
            return

        # Save to DB
        try:
            self.parent_view.cog.bear_cursor.execute(
                "UPDATE bear_hunts SET date = ?, hunting_trap = ?, rallies = ?, total_damage = ? WHERE id = ?",
                (new_date, new_trap, new_rallies, new_damage, self.parent_view.selected_record_id)
            )
            self.parent_view.cog.bear_conn.commit()
        except Exception as e:
            logger.error(f"Failed to update record: {e}")
            print(f"[ERROR] Failed to update record: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save record.", ephemeral=True
            )
            return

        self.parent_view.date = new_date
        self.parent_view.hunting_trap = new_trap
        self.parent_view.rallies = new_rallies
        self.parent_view.total_damage = new_damage

        embed = self.parent_view.build_record_embed()
        embed.description = f"{theme.verifiedIcon} Record updated successfully."

        await interaction.response.edit_message(embed=embed, view=self.parent_view)


# ---------------------------------------------------------------------------
# BearSettingsView — settings management
# ---------------------------------------------------------------------------

class BearSettingsView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.alliance_id: int | None = None
        self._build_components()

    def _build_components(self):
        self.clear_items()
        options = build_alliance_options(self.cog.alliance_conn)
        self.add_item(AllianceSelect(self, options, action="manage"))

        has_alliance = self.alliance_id is not None

        channel_btn = discord.ui.Button(label="Change Bear Channel", style=discord.ButtonStyle.primary, emoji=theme.announceIcon, row=2, disabled=not has_alliance)
        channel_btn.callback = self._change_channel_callback
        self.add_item(channel_btn)

        keywords_btn = discord.ui.Button(label="Manage Keywords", style=discord.ButtonStyle.primary, emoji=theme.editListIcon, row=2, disabled=not has_alliance)
        keywords_btn.callback = self._manage_keywords_callback
        self.add_item(keywords_btn)

        range_btn = discord.ui.Button(label="Set Damage Range", style=discord.ButtonStyle.primary, emoji=theme.chartIcon, row=2, disabled=not has_alliance)
        range_btn.callback = self._set_range_callback
        self.add_item(range_btn)

        add_perm_btn = discord.ui.Button(label="Toggle Add Permission", style=discord.ButtonStyle.secondary, emoji=theme.lockIcon, row=3, disabled=not has_alliance)
        add_perm_btn.callback = self._toggle_add_callback
        self.add_item(add_perm_btn)

        view_perm_btn = discord.ui.Button(label="Toggle View Permission", style=discord.ButtonStyle.secondary, emoji=theme.eyeIcon, row=3, disabled=not has_alliance)
        view_perm_btn.callback = self._toggle_view_callback
        self.add_item(view_perm_btn)

        back_btn = discord.ui.Button(label="Back", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=3)
        back_btn.callback = self._back_callback
        self.add_item(back_btn)

    def _build_settings_embed(self) -> discord.Embed:
        description = (
            f"Configure bear damage tracking for your alliances.\n"
            f"{theme.upperDivider}\n"
            f"{theme.announceIcon} **Change Bear Channel** - Where the bot looks for bear screenshots\n"
            f"{theme.editListIcon} **Manage Keywords** - Words that must be in a message to trigger OCR\n"
            f"{theme.chartIcon} **Set Damage Range** - How many days of data to show (0 = all)\n"
            f"{theme.lockIcon} **Toggle Permissions** - Who can add or view damage data\n"
            f"{theme.lowerDivider}"
        )

        if self.alliance_id:
            settings = self.cog.get_bear_settings(self.alliance_id)
            channel_id = settings["channel_id"]
            keywords = ", ".join(settings["keywords"]) if settings["keywords"] else "None"
            damage_range = settings["damage_range"]
            view_text = "Admins only" if settings["admin_only_view"] else "Everyone"
            add_text = "Admins only" if settings["admin_only_add"] else "Everyone"
            channel_display = f"<#{channel_id}>" if channel_id else "Not set"

            description += (
                f"\n**Current Settings**\n"
                f"{theme.upperDivider}\n"
                f"**Bear Channel:** {channel_display}\n"
                f"**Keywords:** {keywords}\n"
                f"**Damage History Range:** {damage_range} day(s) {'(all history)' if damage_range == 0 else ''}\n"
                f"**Add Permission:** {add_text}\n"
                f"**View Permission:** {view_text}\n"
                f"{theme.lowerDivider}"
            )

        embed = discord.Embed(
            title=f"{theme.settingsIcon} Bear Settings",
            description=description,
            color=theme.emColor1
        )
        return embed

    async def on_alliance_selected(self, interaction: discord.Interaction):
        """Called by AllianceSelect when an alliance is picked."""
        if not self.alliance_id:
            return
        self._build_components()
        embed = self._build_settings_embed()
        await interaction.response.edit_message(content=None, view=self, embed=embed)

    async def _change_channel_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        view = BearChannelSelectView(
            cog=self.cog,
            alliance_id=self.alliance_id,
            parent_settings_view=self,
            parent_message=interaction.message
        )
        await interaction.response.send_message(
            "Select the bear score channel for this alliance:",
            view=view,
            ephemeral=True
        )

    async def _manage_keywords_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        settings = self.cog.get_bear_settings(self.alliance_id)
        current_keywords = ", ".join(settings["keywords"])

        await interaction.response.send_modal(
            KeywordsModal(current_keywords, self.cog, self.alliance_id, self)
        )

    async def _set_range_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return

        settings = self.cog.get_bear_settings(self.alliance_id)
        current_range = settings["damage_range"]

        await interaction.response.send_modal(
            DamageRangeModal(self.cog, self.alliance_id, current_range, self)
        )

    async def _toggle_add_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        await self._toggle_permission(interaction, "add")

    async def _toggle_view_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        allowed = await self.cog.check_bear_permission(interaction, self.alliance_id, "manage")
        if not allowed:
            return
        await self._toggle_permission(interaction, "view")

    async def _toggle_permission(self, interaction: discord.Interaction, mode: str):
        settings = self.cog.get_bear_settings(self.alliance_id)
        key = f"admin_only_{mode}"
        current = settings.get(key, 0)
        new_value = 0 if current else 1

        column = f"bear_admin_only_{mode}"
        self.cog.update_bear_setting(self.alliance_id, column, new_value)

        embed = self._build_settings_embed()
        embed.description += f"\n{theme.verifiedIcon} {mode.capitalize()} permission updated."
        await safe_edit_message(interaction, embed=embed, view=self, content=None)

    async def _back_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_bear_track_menu(interaction)


# ---------------------------------------------------------------------------
# Supporting views and modals
# ---------------------------------------------------------------------------

class DateRangeModal(discord.ui.Modal, title="Select Date Range"):
    from_date = discord.ui.TextInput(
        label="From Date (YYYY-MM-DD)", required=False, placeholder="2026-01-01"
    )
    to_date = discord.ui.TextInput(
        label="To Date (YYYY-MM-DD)", required=False, placeholder="2026-01-31"
    )

    def __init__(self, parent_view: BearDamageView):
        super().__init__()
        self.parent_view = parent_view
        if self.parent_view.from_date:
            self.from_date.default = self.parent_view.from_date.strftime("%Y-%m-%d")
        if self.parent_view.to_date:
            self.to_date.default = self.parent_view.to_date.strftime("%Y-%m-%d")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            if self.from_date.value:
                self.parent_view.from_date = datetime.strptime(self.from_date.value, "%Y-%m-%d").date()
            if self.to_date.value:
                self.parent_view.to_date = datetime.strptime(self.to_date.value, "%Y-%m-%d").date()
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid date format. Use YYYY-MM-DD.", ephemeral=True
            )
            return

        await self.parent_view.try_redraw(interaction)


class BearChannelSelectView(discord.ui.View):
    def __init__(self, cog, alliance_id: int, parent_settings_view: BearSettingsView, parent_message: discord.Message = None):
        super().__init__(timeout=180)
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view
        self.parent_message = parent_message
        self.add_item(BearChannelSelect(self))


class BearChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, parent_view: BearChannelSelectView):
        super().__init__(
            placeholder="Select a channel...",
            min_values=1,
            max_values=1,
            channel_types=[discord.ChannelType.text, discord.ChannelType.news]
        )
        self.parent_view = parent_view

    async def callback(self, interaction: discord.Interaction):
        try:
            selected_channel = self.values[0]
            channel_id = selected_channel.id

            self.parent_view.cog.update_bear_setting(
                self.parent_view.alliance_id,
                "bear_score_channel",
                channel_id
            )

            await interaction.response.edit_message(
                content=f"{theme.verifiedIcon} Bear score channel set to {selected_channel.mention}",
                view=None
            )

            # Refresh parent settings embed on the original message
            try:
                parent_msg = self.parent_view.parent_message
                if parent_msg:
                    settings_view = self.parent_view.parent_settings_view
                    embed = settings_view._build_settings_embed()
                    await parent_msg.edit(embed=embed, view=settings_view)
            except Exception as e:
                logger.warning(f"Could not refresh parent settings embed: {e}")

        except Exception as e:
            logger.error(f"BearChannelSelect callback error: {e}")
            print(f"[ERROR] BearChannelSelect callback error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save channel.", ephemeral=True
            )


class KeywordsModal(discord.ui.Modal):
    def __init__(self, current_keywords: str, cog, alliance_id: int,
                 parent_settings_view: BearSettingsView):
        super().__init__(title="Manage Bear Keywords")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view

        self.keywords_input = discord.ui.TextInput(
            label="Keywords (comma-separated)",
            style=discord.TextStyle.paragraph,
            default=current_keywords or "",
            placeholder="e.g. bear, damage, trap, rally",
            required=False,
            max_length=400
        )
        self.add_item(self.keywords_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            keywords = self.keywords_input.value.strip()
            keyword_csv = ", ".join([kw.strip() for kw in keywords.split(",") if kw.strip()]) if keywords else None

            self.cog.update_bear_setting(self.alliance_id, "bear_keywords", keyword_csv)

            embed = self.parent_settings_view._build_settings_embed()
            embed.description += f"\n{theme.verifiedIcon} Keywords updated."
            await safe_edit_message(interaction, embed=embed, view=self.parent_settings_view, content=None)

        except Exception as e:
            logger.error(f"KeywordsModal error: {e}")
            print(f"[ERROR] KeywordsModal error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to update keywords.", ephemeral=True
            )


class DamageRangeModal(discord.ui.Modal):
    def __init__(self, cog, alliance_id: int, current_range: int,
                 parent_settings_view: BearSettingsView):
        super().__init__(title="Set Damage History Range")
        self.cog = cog
        self.alliance_id = alliance_id
        self.parent_settings_view = parent_settings_view

        self.range_input = discord.ui.TextInput(
            label="Number of days (0 = full history)",
            placeholder="Enter number of days",
            default=str(current_range),
            required=True,
            max_length=5
        )
        self.add_item(self.range_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days = int(self.range_input.value.strip())
            if days < 0:
                raise ValueError
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a non-negative whole number.", ephemeral=True
            )
            return

        try:
            self.cog.update_bear_setting(self.alliance_id, "bear_damage_range", days)
        except Exception as e:
            logger.error(f"Failed to update damage range: {e}")
            print(f"[ERROR] Failed to update damage range: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save damage range.", ephemeral=True
            )
            return

        embed = self.parent_settings_view._build_settings_embed()
        embed.description += f"\n{theme.verifiedIcon} Damage range set to {days} days."
        await safe_edit_message(interaction, embed=embed, view=self.parent_settings_view, content=None)


# ---------------------------------------------------------------------------
# DataSubmit — handles data insertion and view generation
# ---------------------------------------------------------------------------

class DataSubmit:
    def __init__(self, alliance_conn, bear_conn):
        self.alliance_conn = alliance_conn
        self.alliance_cursor = alliance_conn.cursor()
        self.bear_conn = bear_conn
        self.bear_cursor = bear_conn.cursor()

    async def process_submission(self, interaction, date, hunting_trap, rallies, total_damage,
                                 *, alliance_id: int | None = None, alliance_name: str | None = None):
        if not interaction.response.is_done():
            await interaction.response.defer()

        # Resolve alliance from channel if not provided
        if alliance_id is None:
            self.alliance_cursor.execute(
                "SELECT alliance_id FROM alliancesettings WHERE bear_score_channel = ?",
                (interaction.channel.id,)
            )
            row = self.alliance_cursor.fetchone()
            if not row:
                await interaction.followup.send(
                    f"{theme.deniedIcon} This channel is not configured as a bear score channel.",
                    ephemeral=True
                )
                return
            alliance_id = int(row[0])

        # Resolve alliance name
        if alliance_name is None:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,)
            )
            row = self.alliance_cursor.fetchone()
            alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

        if isinstance(date, datetime):
            date = date.strftime("%Y-%m-%d")

        total_damage = int(total_damage)

        # Insert
        try:
            self.bear_cursor.execute(
                "INSERT INTO bear_hunts (alliance_id, date, hunting_trap, rallies, total_damage) "
                "VALUES (?, ?, ?, ?, ?)",
                (alliance_id, date, hunting_trap, rallies, total_damage)
            )
            self.bear_conn.commit()
        except sqlite3.IntegrityError:
            await interaction.followup.send(
                f"{theme.warnIcon} This alliance already submitted this trap for that date.",
                ephemeral=True
            )
            return

        # Get damage range setting
        self.alliance_cursor.execute(
            "SELECT bear_damage_range FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,)
        )
        range_row = self.alliance_cursor.fetchone()
        damage_range_days = range_row[0] if range_row and range_row[0] else 0

        # Fetch all data for this trap
        self.bear_cursor.execute(
            "SELECT date, rallies, total_damage FROM bear_hunts "
            "WHERE alliance_id = ? AND hunting_trap = ? ORDER BY date ASC",
            (alliance_id, hunting_trap)
        )
        rows = self.bear_cursor.fetchall()

        # Apply damage range filter
        if damage_range_days > 0:
            today = datetime.now(timezone.utc).date()
            range_start = today - timedelta(days=damage_range_days)
            filtered_rows = [
                r for r in rows
                if datetime.strptime(r[0], "%Y-%m-%d").date() >= range_start
            ]
            if not filtered_rows:
                filtered_rows = rows
        else:
            filtered_rows = rows

        dates = [datetime.strptime(r[0], "%Y-%m-%d") for r in filtered_rows]
        rallies_list = [int(r[1]) if r[1] else 0 for r in filtered_rows]
        total_damages = [int(r[2]) if r[2] else 0 for r in filtered_rows]

        try:
            embed, image_file = bear_data_embed(
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                hunting_trap=hunting_trap,
                dates=dates,
                rallies_list=rallies_list,
                total_damages=total_damages,
                title_suffix="Latest Submission",
                damage_range_days=damage_range_days
            )
        except Exception as e:
            logger.error(f"Failed to generate embed: {e}")
            print(f"[ERROR] Failed to generate embed: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error generating graph.", ephemeral=True
            )
            return

        try:
            await interaction.edit_original_response(embed=embed, attachments=[image_file] if image_file else [], view=None)
        except discord.NotFound:
            # Fallback: original response unavailable (e.g. OCR flow via channel.send)
            try:
                await interaction.followup.send(embed=embed, file=image_file if image_file else None)
            except Exception as e:
                logger.error(f"Failed to send submission result: {e}")
                print(f"[ERROR] Failed to send submission result: {e}")
        except Exception as e:
            logger.error(f"Failed to edit original message: {e}")
            print(f"[ERROR] Failed to edit original message: {e}")

    async def process_view(self, *, alliance_id: int, hunting_trap: int,
                           from_date: str | None = None, to_date: str | None = None,
                           alliance_name: str | None = None):
        """Generate a view embed and chart for bear damage data."""
        if alliance_name is None:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,)
            )
            row = self.alliance_cursor.fetchone()
            alliance_name = row[0] if row else f"Alliance ID: {alliance_id}"

        self.bear_cursor.execute(
            "SELECT date, rallies, total_damage FROM bear_hunts "
            "WHERE alliance_id = ? AND hunting_trap = ? ORDER BY date ASC",
            (alliance_id, hunting_trap)
        )
        rows = self.bear_cursor.fetchall()

        if not rows:
            return None, None

        if not from_date:
            from_date = rows[0][0]
        if not to_date:
            to_date = rows[-1][0]

        try:
            from_dt = datetime.strptime(from_date, "%Y-%m-%d").date()
            to_dt = datetime.strptime(to_date, "%Y-%m-%d").date()
        except ValueError:
            return None, None

        if from_dt > to_dt:
            return None, None

        filtered_rows = [
            r for r in rows
            if from_dt <= datetime.strptime(r[0], "%Y-%m-%d").date() <= to_dt
        ]
        if not filtered_rows:
            return None, None

        dates = [datetime.strptime(r[0], "%Y-%m-%d") for r in filtered_rows]
        rallies_list = [int(r[1]) if r[1] else 0 for r in filtered_rows]
        total_damages = [int(r[2]) if r[2] else 0 for r in filtered_rows]

        try:
            embed, file = bear_data_embed(
                alliance_id=alliance_id,
                alliance_name=alliance_name,
                hunting_trap=hunting_trap,
                dates=dates,
                rallies_list=rallies_list,
                total_damages=total_damages,
                title_suffix="View Damage",
                damage_range_days=None
            )
        except Exception as e:
            logger.error(f"bear_data_embed failed: {e}")
            print(f"[ERROR] bear_data_embed failed: {e}")
            return None, None

        return embed, file


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

async def setup(bot):
    await bot.add_cog(BearTrack(bot))
