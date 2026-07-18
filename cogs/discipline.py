"""
cogs/discipline.py — Alliance Discipline Log

Provides a full infraction tracking system for alliance admins. Infractions
are tied to a player FID, an event, an infraction type, and a punishment.
Each record carries an optional expiry date and can be soft-deleted for a
permanent audit trail.

Access
------
- Via the Settings menu → Discipline Log button
- Via the /disc slash command (admin only)

Permissions
-----------
- Alliance Admin and above: Log, view, and edit infractions
- Global Admin and above:   Also delete records and configure settings

Slash commands
--------------
- /disc                     Open the main discipline menu
- /disc-setup <channel>     Set the leadership channel for infraction posts
- /disc-expiry <days>       Set the default expiry period (days)

Features
--------
Log Infraction
  Step-by-step wizard: FID → Event → Infraction type → Punishment type →
  Notes & expiry → Confirm. On confirm, posts a summary embed to the
  configured leadership channel and offers to DM the member directly.

View History
  Look up a player by FID. Filter by time range (30/60/90 days or all time)
  and toggle expired records on/off. Each record shows event, infraction,
  punishment, expiry status, who logged it, and any notes.

Edit Expiry
  Select any active record by FID and update its expiry date. Accepts
  "never", a number of days from today, or a YYYY-MM-DD date.

Delete Record  (Global Admin only)
  Soft-deletes a record — marks it deleted and records who deleted it and
  when. The row is retained in the database for audit purposes.

Settings  (Global Admin only)
  Configure the Discord channel that receives infraction posts and the
  default expiry period used when logging without specifying a date.

Database
--------
db/discipline.sqlite
  infractions — one row per infraction record
    id, fid, event, infraction, punishment, notes,
    logged_by_id, logged_by_name, logged_at,
    expiry_date (NULL = permanent), is_deleted, deleted_by_id, deleted_at

db/settings.sqlite
  discipline_settings — key/value store
    channel_id          Discord channel for leadership posts
    default_expiry_days Default expiry period in days (default: 30)
"""

import discord
from discord.ext import commands
import sqlite3
import hashlib
import time
import ssl
import aiohttp
from datetime import datetime, timedelta
from .pimp_my_bot import theme
from .permission_handler import PermissionManager
from .browser_headers import get_headers
from .bot_level_mapping import LEVEL_MAPPING
from .event_registry import DISCIPLINE_EVENTS as EVENTS

DB_PATH = 'db/discipline.sqlite'
SETTINGS_DB = 'db/settings.sqlite'
USERS_DB = 'db/users.sqlite'

INFRACTIONS = ["No Show", "Incorrect Heroes", "Incorrect Troop Ratio", "Other"]
PUNISHMENTS = ["Warning", "Kicked", "Banned", "Zeroed", "Other"]


# ── DB helpers ──────────────────────────────────────────────────────────────

def _init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS infractions (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                fid           TEXT    NOT NULL,
                event         TEXT    NOT NULL,
                infraction    TEXT    NOT NULL,
                punishment    TEXT    NOT NULL,
                notes         TEXT    DEFAULT '',
                logged_by_id  TEXT    NOT NULL,
                logged_by_name TEXT   NOT NULL,
                logged_at     TEXT    NOT NULL,
                expiry_date   TEXT,
                is_deleted    INTEGER DEFAULT 0,
                deleted_by_id TEXT,
                deleted_at    TEXT
            )
        """)
        db.commit()
    with sqlite3.connect(SETTINGS_DB) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS discipline_settings (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        db.execute("INSERT OR IGNORE INTO discipline_settings VALUES ('channel_id', NULL)")
        db.execute("INSERT OR IGNORE INTO discipline_settings VALUES ('default_expiry_days', '30')")
        db.commit()


def _get_setting(key):
    with sqlite3.connect(SETTINGS_DB) as db:
        row = db.execute(
            "SELECT value FROM discipline_settings WHERE key = ?", (key,)
        ).fetchone()
        return row[0] if row else None


def _get_member(fid):
    with sqlite3.connect(USERS_DB) as db:
        return db.execute(
            "SELECT fid, nickname, furnace_lv, kid FROM users WHERE fid = ?", (fid,)
        ).fetchone()


def _parse_expiry(raw, default_days):
    raw = (raw or "").strip().lower()
    if raw in ("never", ""):
        return None
    if raw == "default":
        return (datetime.utcnow() + timedelta(days=default_days)).strftime("%Y-%m-%d")
    try:
        days = int(raw)
        return (datetime.utcnow() + timedelta(days=days)).strftime("%Y-%m-%d")
    except ValueError:
        try:
            datetime.strptime(raw, "%Y-%m-%d")
            return raw
        except ValueError:
            return (datetime.utcnow() + timedelta(days=default_days)).strftime("%Y-%m-%d")


def _level_name(lv):
    return LEVEL_MAPPING.get(lv, f"Level {lv}") if lv else "Unknown"


def _member_card_embed(fid, nickname, furnace_lv, kid, color=None):
    return discord.Embed(
        title=f"{theme.userIcon} {nickname}",
        description=(
            f"{theme.upperDivider}\n"
            f"**{theme.fidIcon} ID:** `{fid}`\n"
            f"**{theme.levelIcon} Furnace Level:** `{_level_name(furnace_lv)}`\n"
            f"**{theme.globeIcon} State:** `{kid}`\n"
            f"{theme.lowerDivider}\n"
        ),
        color=color or theme.emColor1,
    )


def _is_expired(expiry_date):
    if not expiry_date:
        return False
    try:
        return datetime.strptime(expiry_date, "%Y-%m-%d") < datetime.utcnow()
    except ValueError:
        return False


async def _fetch_avatar(fid):
    try:
        secret = "tB87#kPtkxqOS2"
        current_time = int(time.time() * 1000)
        form = f"fid={fid}&time={current_time}"
        sign = hashlib.md5((form + secret).encode("utf-8")).hexdigest()
        form = f"sign={sign}&{form}"
        url = "https://wos-giftcode-api.centurygame.com/api/player"
        headers = get_headers("https://wos-giftcode-api.centurygame.com")
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.post(url, headers=headers, data=form, ssl=ssl_context) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("data", {}).get("avatar_image")
    except Exception:
        pass
    return None


# ── Modals ───────────────────────────────────────────────────────────────────

class FIDModal(discord.ui.Modal, title="Enter Player ID"):
    fid = discord.ui.TextInput(
        label="Player FID",
        placeholder="Enter the player's game ID...",
        max_length=20,
    )

    def __init__(self, next_step):
        super().__init__()
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        await self._next(interaction, self.fid.value.strip())


class OtherTextModal(discord.ui.Modal):
    value = discord.ui.TextInput(
        label="Enter custom value",
        placeholder="Describe the infraction / punishment...",
        max_length=100,
    )

    def __init__(self, title, next_step):
        super().__init__(title=title)
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        await self._next(interaction, self.value.value.strip())


class NotesExpiryModal(discord.ui.Modal, title="Notes & Expiry"):
    notes = discord.ui.TextInput(
        label="Notes",
        placeholder="Any additional details...",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500,
    )
    expiry = discord.ui.TextInput(
        label="Expiry",
        placeholder="default / never / days (e.g. 60) / date (YYYY-MM-DD)",
        required=False,
        max_length=20,
    )

    def __init__(self, next_step):
        super().__init__()
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        await self._next(interaction, self.notes.value.strip(), self.expiry.value.strip())


class EditExpiryModal(discord.ui.Modal, title="Edit Expiry Date"):
    expiry = discord.ui.TextInput(
        label="New Expiry",
        placeholder="never / days (e.g. 60) / date (YYYY-MM-DD)",
        max_length=20,
    )

    def __init__(self, record_id, next_step):
        super().__init__()
        self.record_id = record_id
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        await self._next(interaction, self.record_id, self.expiry.value.strip())


# ── Log flow views ────────────────────────────────────────────────────────────

class EventSelectView(discord.ui.View):
    def __init__(self, state: dict, author_id: int):
        super().__init__(timeout=300)
        self.state = state
        self.author_id = author_id

        options = [discord.SelectOption(label=e, value=e) for e in EVENTS]
        select = discord.ui.Select(
            placeholder=f"{theme.calendarIcon} Select event...",
            options=options,
        )
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        self.state["event"] = interaction.data["values"][0]
        await _show_infraction_select(interaction, self.state, self.author_id)


async def _show_infraction_select(interaction, state, author_id):
    member = state["member"]
    embed = _member_card_embed(*member)
    embed.add_field(
        name=f"{theme.calendarIcon} Event",
        value=f"`{state['event']}`",
        inline=False,
    )
    embed.set_footer(text="Step 2 of 4 — Select infraction")

    view = InfractionSelectView(state, author_id)
    await interaction.response.edit_message(embed=embed, view=view)


class InfractionSelectView(discord.ui.View):
    def __init__(self, state: dict, author_id: int):
        super().__init__(timeout=300)
        self.state = state
        self.author_id = author_id

        options = [discord.SelectOption(label=i, value=i) for i in INFRACTIONS]
        select = discord.ui.Select(
            placeholder=f"{theme.editListIcon} Select infraction...",
            options=options,
        )
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        if value == "Other":
            async def after_other(i, text):
                self.state["infraction"] = text
                await _show_punishment_select(i, self.state, self.author_id)
            await interaction.response.send_modal(OtherTextModal("Custom Infraction", after_other))
        else:
            self.state["infraction"] = value
            await _show_punishment_select(interaction, self.state, self.author_id)


async def _show_punishment_select(interaction, state, author_id):
    member = state["member"]
    embed = _member_card_embed(*member)
    embed.add_field(name=f"{theme.calendarIcon} Event", value=f"`{state['event']}`", inline=True)
    embed.add_field(name=f"{theme.editListIcon} Infraction", value=f"`{state['infraction']}`", inline=True)
    embed.set_footer(text="Step 3 of 4 — Select punishment")

    view = PunishmentSelectView(state, author_id)
    if interaction.response.is_done():
        await interaction.edit_original_response(embed=embed, view=view)
    else:
        await interaction.response.edit_message(embed=embed, view=view)


class PunishmentSelectView(discord.ui.View):
    def __init__(self, state: dict, author_id: int):
        super().__init__(timeout=300)
        self.state = state
        self.author_id = author_id

        options = [discord.SelectOption(label=p, value=p) for p in PUNISHMENTS]
        select = discord.ui.Select(
            placeholder=f"{theme.shieldIcon} Select punishment...",
            options=options,
        )
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        value = interaction.data["values"][0]
        if value == "Other":
            async def after_other(i, text):
                self.state["punishment"] = text
                await _show_notes_expiry(i, self.state, self.author_id)
            await interaction.response.send_modal(OtherTextModal("Custom Punishment", after_other))
        else:
            self.state["punishment"] = value
            await _show_notes_expiry(interaction, self.state, self.author_id)


async def _show_notes_expiry(interaction, state, author_id):
    default_days = int(_get_setting("default_expiry_days") or 30)

    async def after_modal(i, notes, expiry_raw):
        state["notes"] = notes
        state["expiry_date"] = _parse_expiry(expiry_raw or "default", default_days)
        await _show_confirm(i, state, author_id)

    await interaction.response.send_modal(NotesExpiryModal(after_modal))


async def _show_confirm(interaction, state, author_id):
    member = state["member"]
    expiry_display = state["expiry_date"] if state["expiry_date"] else "Never"

    embed = discord.Embed(
        title=f"{theme.warnIcon} Confirm Infraction",
        description=(
            f"{theme.upperDivider}\n"
            f"**{theme.userIcon} Member:** `{member[1]}` (ID: `{member[0]}`)\n"
            f"**{theme.calendarIcon} Event:** `{state['event']}`\n"
            f"**{theme.editListIcon} Infraction:** `{state['infraction']}`\n"
            f"**{theme.shieldIcon} Punishment:** `{state['punishment']}`\n"
            f"**{theme.timeIcon} Expires:** `{expiry_display}`\n"
        ),
        color=theme.emColor4,
    )
    if state.get("notes"):
        embed.add_field(name=f"{theme.documentIcon} Notes", value=state["notes"], inline=False)
    embed.description += theme.lowerDivider
    embed.set_footer(text="Confirm to log this infraction")

    view = ConfirmLogView(state, author_id)
    if interaction.response.is_done():
        await interaction.edit_original_response(embed=embed, view=view)
    else:
        await interaction.response.edit_message(embed=embed, view=view)


class ConfirmLogView(discord.ui.View):
    def __init__(self, state: dict, author_id: int):
        super().__init__(timeout=300)
        self.state = state
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Confirm", emoji="✅", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        state = self.state
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        member = state["member"]

        with sqlite3.connect(DB_PATH) as db:
            cursor = db.execute(
                """
                INSERT INTO infractions
                    (fid, event, infraction, punishment, notes,
                     logged_by_id, logged_by_name, logged_at, expiry_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    member[0], state["event"], state["infraction"], state["punishment"],
                    state.get("notes", ""), str(interaction.user.id),
                    interaction.user.display_name, now, state.get("expiry_date"),
                ),
            )
            record_id = cursor.lastrowid
            db.commit()

        expiry_display = state["expiry_date"] if state["expiry_date"] else "Never"

        # Post to leadership channel
        channel_id = _get_setting("channel_id")
        if channel_id:
            try:
                channel = interaction.client.get_channel(int(channel_id))
                if channel:
                    avatar_url = await _fetch_avatar(member[0])
                    log_embed = discord.Embed(
                        title=f"{theme.shieldIcon} New Infraction Logged",
                        description=(
                            f"{theme.upperDivider}\n"
                            f"**{theme.userIcon} Member:** `{member[1]}` (ID: `{member[0]}`)\n"
                            f"**{theme.calendarIcon} Event:** `{state['event']}`\n"
                            f"**{theme.editListIcon} Infraction:** `{state['infraction']}`\n"
                            f"**{theme.shieldIcon} Punishment:** `{state['punishment']}`\n"
                            f"**{theme.timeIcon} Expires:** `{expiry_display}`\n"
                            f"**{theme.userIcon} Logged By:** {interaction.user.mention}\n"
                            f"**{theme.calendarIcon} Date:** `{now}`\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor2,
                    )
                    if state.get("notes"):
                        log_embed.add_field(
                            name=f"{theme.documentIcon} Notes", value=state["notes"], inline=False
                        )
                    if avatar_url:
                        log_embed.set_thumbnail(url=avatar_url)
                    await channel.send(embed=log_embed)
            except Exception as e:
                print(f"[discipline] channel post error: {e}")

        success_embed = discord.Embed(
            title=f"{theme.verifiedIcon} Infraction Logged",
            description=(
                f"Record ID `#{record_id}` saved.\n\n"
                f"Do you want to notify this member on Discord?"
            ),
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=success_embed, view=NotifyView(state, record_id, self.author_id))

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title=f"{theme.deniedIcon} Cancelled",
            description="Infraction was not logged.",
            color=theme.emColor4,
        )
        await interaction.response.edit_message(embed=embed, view=None)


class NotifyView(discord.ui.View):
    def __init__(self, state: dict, record_id: int, author_id: int):
        super().__init__(timeout=300)
        self.state = state
        self.record_id = record_id
        self.author_id = author_id

        user_select = discord.ui.UserSelect(placeholder="Select Discord member to notify...")
        user_select.callback = self._on_user_select
        self.add_item(user_select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_user_select(self, interaction: discord.Interaction):
        member = interaction.data["resolved"]["users"]
        user_id = list(member.keys())[0]
        user_data = member[user_id]
        username = user_data.get("username", "Unknown")

        state = self.state
        expiry_display = state["expiry_date"] if state["expiry_date"] else "Never"

        dm_embed = discord.Embed(
            title=f"{theme.shieldIcon} Infraction Notice",
            description=(
                f"You have received an infraction in **{interaction.guild.name if interaction.guild else 'the alliance'}**.\n\n"
                f"{theme.upperDivider}\n"
                f"**{theme.calendarIcon} Event:** `{state['event']}`\n"
                f"**{theme.editListIcon} Infraction:** `{state['infraction']}`\n"
                f"**{theme.shieldIcon} Punishment:** `{state['punishment']}`\n"
                f"**{theme.timeIcon} Expires:** `{expiry_display}`\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor2,
        )
        if state.get("notes"):
            dm_embed.add_field(name=f"{theme.documentIcon} Notes", value=state["notes"], inline=False)

        try:
            discord_user = await interaction.client.fetch_user(int(user_id))
            await discord_user.send(embed=dm_embed)
            msg = f"{theme.verifiedIcon} Notified **{username}** via DM."
        except discord.Forbidden:
            msg = f"{theme.warnIcon} Could not DM **{username}** (DMs disabled)."
        except Exception as e:
            msg = f"{theme.deniedIcon} DM failed: {e}"

        done_embed = discord.Embed(
            title=f"{theme.verifiedIcon} Done",
            description=msg,
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=done_embed, view=None)

    @discord.ui.button(label="Skip Notification", emoji="⏭️", style=discord.ButtonStyle.secondary)
    async def skip(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Done",
            description=f"Infraction `#{self.record_id}` logged. No notification sent.",
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ── View History flow ─────────────────────────────────────────────────────────

class ViewHistoryOptionsView(discord.ui.View):
    def __init__(self, fid: str, member, author_id: int):
        super().__init__(timeout=300)
        self.fid = fid
        self.member = member
        self.author_id = author_id
        self.show_expired = False

        time_opts = [
            discord.SelectOption(label="Last 30 Days", value="30"),
            discord.SelectOption(label="Last 60 Days", value="60"),
            discord.SelectOption(label="Last 90 Days", value="90"),
            discord.SelectOption(label="All Time", value="all"),
        ]
        time_select = discord.ui.Select(placeholder=f"{theme.timeIcon} Select time range...", options=time_opts)
        time_select.callback = self._on_time_select
        self.add_item(time_select)
        self.time_select = time_select

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_time_select(self, interaction: discord.Interaction):
        self.selected_range = interaction.data["values"][0]
        await self._render(interaction)

    @discord.ui.button(label="Show Expired", emoji="👁️", style=discord.ButtonStyle.secondary, row=1)
    async def toggle_expired(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.show_expired = not self.show_expired
        button.label = "Hide Expired" if self.show_expired else "Show Expired"
        button.style = discord.ButtonStyle.primary if self.show_expired else discord.ButtonStyle.secondary
        if hasattr(self, "selected_range"):
            await self._render(interaction)
        else:
            await interaction.response.edit_message(view=self)

    async def _render(self, interaction: discord.Interaction):
        fid = self.fid
        member = self.member
        time_range = getattr(self, "selected_range", "all")

        params = [fid]
        where = "fid = ? AND is_deleted = 0"
        if time_range != "all":
            cutoff = (datetime.utcnow() - timedelta(days=int(time_range))).strftime("%Y-%m-%d")
            where += " AND logged_at >= ?"
            params.append(cutoff)
        if not self.show_expired:
            where += " AND (expiry_date IS NULL OR expiry_date >= ?)"
            params.append(datetime.utcnow().strftime("%Y-%m-%d"))

        with sqlite3.connect(DB_PATH) as db:
            rows = db.execute(
                f"SELECT id, event, infraction, punishment, notes, logged_by_name, logged_at, expiry_date "
                f"FROM infractions WHERE {where} ORDER BY logged_at DESC",
                params,
            ).fetchall()

        embed = _member_card_embed(*member)
        embed.title += f"  —  Infraction History"

        if not rows:
            embed.add_field(
                name=f"{theme.blankListIcon} No Records",
                value="No infractions found for the selected filters.",
                inline=False,
            )
        else:
            for row in rows:
                rid, event, infraction, punishment, notes, logged_by, logged_at, expiry_date = row
                expired = _is_expired(expiry_date)
                expiry_str = expiry_date if expiry_date else "Never"
                status = " `[Expired]`" if expired else ""
                field_name = f"{theme.shieldIcon} #{rid}{status}  •  {logged_at[:10]}"
                field_val = (
                    f"**Event:** `{event}`\n"
                    f"**Infraction:** `{infraction}`\n"
                    f"**Punishment:** `{punishment}`\n"
                    f"**Expires:** `{expiry_str}`\n"
                    f"**Logged by:** `{logged_by}`"
                )
                if notes:
                    field_val += f"\n**Notes:** {notes}"
                embed.add_field(name=field_name, value=field_val, inline=False)

        range_label = f"Last {time_range} days" if time_range != "all" else "All time"
        expired_label = " (including expired)" if self.show_expired else ""
        embed.set_footer(text=f"{range_label}{expired_label}  •  {len(rows)} record(s)")

        await interaction.response.edit_message(embed=embed, view=self)


# ── Edit Expiry flow ──────────────────────────────────────────────────────────

class EditExpirySelectView(discord.ui.View):
    def __init__(self, rows, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id

        options = []
        for row in rows[:25]:
            rid, event, infraction, logged_at, expiry_date = row
            label = f"#{rid} — {event} — {infraction}"[:100]
            desc = f"Logged: {logged_at[:10]}  Expires: {expiry_date or 'Never'}"[:100]
            options.append(discord.SelectOption(label=label, value=str(rid), description=desc))

        select = discord.ui.Select(placeholder=f"{theme.editListIcon} Select infraction to edit...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        record_id = int(interaction.data["values"][0])

        async def after_modal(i, rid, raw):
            default_days = int(_get_setting("default_expiry_days") or 30)
            new_expiry = _parse_expiry(raw, default_days) if raw.strip().lower() != "never" else None

            with sqlite3.connect(DB_PATH) as db:
                db.execute(
                    "UPDATE infractions SET expiry_date = ? WHERE id = ?", (new_expiry, rid)
                )
                db.commit()

            expiry_display = new_expiry if new_expiry else "Never"
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Expiry Updated",
                description=f"Record `#{rid}` expiry set to `{expiry_display}`.",
                color=theme.emColor3,
            )
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=None)
            else:
                await i.response.edit_message(embed=embed, view=None)

        await interaction.response.send_modal(EditExpiryModal(record_id, after_modal))


# ── Delete flow ───────────────────────────────────────────────────────────────

class DeleteSelectView(discord.ui.View):
    def __init__(self, rows, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id

        options = []
        for row in rows[:25]:
            rid, event, infraction, logged_at = row
            label = f"#{rid} — {event} — {infraction}"[:100]
            desc = f"Logged: {logged_at[:10]}"[:100]
            options.append(discord.SelectOption(label=label, value=str(rid), description=desc))

        select = discord.ui.Select(placeholder=f"{theme.trashIcon} Select record to delete...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        record_id = int(interaction.data["values"][0])

        embed = discord.Embed(
            title=f"{theme.warnIcon} Confirm Delete",
            description=(
                f"Soft-delete record `#{record_id}`?\n\n"
                f"The record will be hidden from all views but **remains in the database**."
            ),
            color=theme.emColor2,
        )
        await interaction.response.edit_message(
            embed=embed, view=ConfirmDeleteView(record_id, self.author_id, interaction.user)
        )


class ConfirmDeleteView(discord.ui.View):
    def __init__(self, record_id: int, author_id: int, deleter):
        super().__init__(timeout=120)
        self.record_id = record_id
        self.author_id = author_id
        self.deleter = deleter

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Delete", emoji="🗑️", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with sqlite3.connect(DB_PATH) as db:
            db.execute(
                "UPDATE infractions SET is_deleted = 1, deleted_by_id = ?, deleted_at = ? WHERE id = ?",
                (str(self.deleter.id), now, self.record_id),
            )
            db.commit()
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Record Deleted",
            description=f"Record `#{self.record_id}` has been soft-deleted.",
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title=f"{theme.deniedIcon} Cancelled",
            description="No records were deleted.",
            color=theme.emColor4,
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ── Settings flow ────────────────────────────────────────────────────────────

class DiscSettingsView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id

        channel_select = discord.ui.ChannelSelect(
            placeholder=f"{theme.bellIcon} Select discipline log channel...",
            channel_types=[discord.ChannelType.text],
        )
        channel_select.callback = self._on_channel_select
        self.add_item(channel_select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    async def _on_channel_select(self, interaction: discord.Interaction):
        channel_id = interaction.data["values"][0]
        with sqlite3.connect(SETTINGS_DB) as db:
            db.execute(
                "INSERT OR REPLACE INTO discipline_settings VALUES ('channel_id', ?)",
                (str(channel_id),),
            )
            db.commit()

        channel = interaction.guild.get_channel(int(channel_id)) if interaction.guild else None
        channel_mention = channel.mention if channel else f"<#{channel_id}>"

        current_expiry = _get_setting("default_expiry_days") or "30"
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Discipline Settings",
            description=(
                f"{theme.upperDivider}\n"
                f"**{theme.bellIcon} Log Channel:** {channel_mention}\n"
                f"**{theme.timeIcon} Default Expiry:** `{current_expiry}` days\n"
                f"{theme.lowerDivider}\n\n"
                f"{theme.verifiedIcon} Channel saved! Use the button below to update the default expiry."
            ),
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=embed, view=ExpiryUpdateView(self.author_id))

    @staticmethod
    def settings_embed():
        channel_id = _get_setting("channel_id")
        expiry = _get_setting("default_expiry_days") or "30"
        channel_str = f"<#{channel_id}>" if channel_id else "`Not set`"
        return discord.Embed(
            title=f"{theme.settingsIcon} Discipline Settings",
            description=(
                f"{theme.upperDivider}\n"
                f"**{theme.bellIcon} Log Channel:** {channel_str}\n"
                f"**{theme.timeIcon} Default Expiry:** `{expiry}` days\n"
                f"{theme.lowerDivider}\n\n"
                f"Select a channel from the dropdown to update it."
            ),
            color=theme.emColor1,
        )


class ExpiryUpdateModal(discord.ui.Modal, title="Set Default Expiry"):
    days = discord.ui.TextInput(
        label="Default expiry (days)",
        placeholder="e.g. 30",
        max_length=4,
    )

    def __init__(self, author_id: int):
        super().__init__()
        self.author_id = author_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            days_val = int(self.days.value.strip())
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Enter a whole number of days.", ephemeral=True
            )
            return

        with sqlite3.connect(SETTINGS_DB) as db:
            db.execute(
                "INSERT OR REPLACE INTO discipline_settings VALUES ('default_expiry_days', ?)",
                (str(days_val),),
            )
            db.commit()

        channel_id = _get_setting("channel_id")
        channel_str = f"<#{channel_id}>" if channel_id else "`Not set`"
        embed = discord.Embed(
            title=f"{theme.settingsIcon} Discipline Settings",
            description=(
                f"{theme.upperDivider}\n"
                f"**{theme.bellIcon} Log Channel:** {channel_str}\n"
                f"**{theme.timeIcon} Default Expiry:** `{days_val}` days\n"
                f"{theme.lowerDivider}\n\n"
                f"{theme.verifiedIcon} Default expiry updated to `{days_val}` days."
            ),
            color=theme.emColor3,
        )
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=None)
        else:
            await interaction.response.edit_message(embed=embed, view=None)


class ExpiryUpdateView(discord.ui.View):
    def __init__(self, author_id: int):
        super().__init__(timeout=300)
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Set Default Expiry", emoji="📅", style=discord.ButtonStyle.primary)
    async def set_expiry(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ExpiryUpdateModal(self.author_id))


# ── Main menu view ────────────────────────────────────────────────────────────

class DisciplineMenuView(discord.ui.View):
    def __init__(self, cog, is_global: bool, author_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.is_global = is_global
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.",
                ephemeral=True,
            )
            return False
        return True

    @discord.ui.button(label="Log Infraction", emoji="📝", style=discord.ButtonStyle.danger, row=0)
    async def log_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def after_fid(i, fid):
            member = _get_member(fid)
            if not member:
                err = discord.Embed(
                    title=f"{theme.deniedIcon} Member Not Found",
                    description=f"No member with ID `{fid}` found in the alliance list.",
                    color=theme.emColor2,
                )
                if i.response.is_done():
                    await i.edit_original_response(embed=err, view=None)
                else:
                    await i.response.send_message(embed=err, ephemeral=True)
                return

            state = {"member": member, "event": None, "infraction": None, "punishment": None}
            embed = _member_card_embed(*member)
            embed.set_footer(text="Step 1 of 4 — Select event")
            view = EventSelectView(state, i.user.id)
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=view)
            else:
                await i.response.edit_message(embed=embed, view=view)

        await interaction.response.send_modal(FIDModal(after_fid))

    @discord.ui.button(label="View History", emoji="👁️", style=discord.ButtonStyle.primary, row=0)
    async def view_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def after_fid(i, fid):
            member = _get_member(fid)
            if not member:
                err = discord.Embed(
                    title=f"{theme.deniedIcon} Member Not Found",
                    description=f"No member with ID `{fid}` found in the alliance list.",
                    color=theme.emColor2,
                )
                if i.response.is_done():
                    await i.edit_original_response(embed=err, view=None)
                else:
                    await i.response.send_message(embed=err, ephemeral=True)
                return

            embed = _member_card_embed(*member)
            embed.set_footer(text="Select time range to view history")
            view = ViewHistoryOptionsView(fid, member, i.user.id)
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=view)
            else:
                await i.response.edit_message(embed=embed, view=view)

        await interaction.response.send_modal(FIDModal(after_fid))

    @discord.ui.button(label="Edit Expiry", emoji="📅", style=discord.ButtonStyle.secondary, row=1)
    async def edit_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def after_fid(i, fid):
            with sqlite3.connect(DB_PATH) as db:
                rows = db.execute(
                    "SELECT id, event, infraction, logged_at, expiry_date "
                    "FROM infractions WHERE fid = ? AND is_deleted = 0 ORDER BY logged_at DESC",
                    (fid,),
                ).fetchall()

            if not rows:
                err = discord.Embed(
                    title=f"{theme.deniedIcon} No Records",
                    description=f"No active infractions found for ID `{fid}`.",
                    color=theme.emColor2,
                )
                if i.response.is_done():
                    await i.edit_original_response(embed=err, view=None)
                else:
                    await i.response.send_message(embed=err, ephemeral=True)
                return

            embed = discord.Embed(
                title=f"{theme.calendarIcon} Edit Expiry",
                description=f"Select an infraction for ID `{fid}` to update its expiry date.",
                color=theme.emColor1,
            )
            view = EditExpirySelectView(rows, i.user.id)
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=view)
            else:
                await i.response.edit_message(embed=embed, view=view)

        await interaction.response.send_modal(FIDModal(after_fid))

    @discord.ui.button(label="Delete Record", emoji="🗑️", style=discord.ButtonStyle.danger, row=1)
    async def delete_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global Admins can delete records.",
                ephemeral=True,
            )
            return

        async def after_fid(i, fid):
            with sqlite3.connect(DB_PATH) as db:
                rows = db.execute(
                    "SELECT id, event, infraction, logged_at "
                    "FROM infractions WHERE fid = ? AND is_deleted = 0 ORDER BY logged_at DESC",
                    (fid,),
                ).fetchall()

            if not rows:
                err = discord.Embed(
                    title=f"{theme.deniedIcon} No Records",
                    description=f"No active infractions found for ID `{fid}`.",
                    color=theme.emColor2,
                )
                if i.response.is_done():
                    await i.edit_original_response(embed=err, view=None)
                else:
                    await i.response.send_message(embed=err, ephemeral=True)
                return

            embed = discord.Embed(
                title=f"{theme.trashIcon} Delete Record",
                description=f"Select an infraction for ID `{fid}` to soft-delete.",
                color=theme.emColor2,
            )
            view = DeleteSelectView(rows, i.user.id)
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=view)
            else:
                await i.response.edit_message(embed=embed, view=view)

        await interaction.response.send_modal(FIDModal(after_fid))

    @discord.ui.button(label="Settings", emoji="⚙️", style=discord.ButtonStyle.secondary, row=2)
    async def settings_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global Admins can change discipline settings.",
                ephemeral=True,
            )
            return
        embed = DiscSettingsView.settings_embed()
        await interaction.response.edit_message(embed=embed, view=DiscSettingsView(self.author_id))


# ── Cog ───────────────────────────────────────────────────────────────────────

class DisciplineCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        _init_db()

    async def show_disc_menu(self, interaction: discord.Interaction):
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You do not have permission to use this command.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title=f"{theme.shieldIcon} Discipline Log",
            description=(
                f"Select an operation:\n\n"
                f"{theme.upperDivider}\n"
                f"📝 **Log Infraction** — Record a new write-up\n"
                f"👁️ **View History** — View a member's infraction history\n"
                f"📅 **Edit Expiry** — Change when an infraction expires\n"
                + (f"🗑️ **Delete Record** — Soft-delete a record *(Global Admin)*\n" if is_global else "")
                + (f"⚙️ **Settings** — Set log channel & default expiry *(Global Admin)*\n" if is_global else "")
                + theme.lowerDivider
            ),
            color=theme.emColor1,
        )
        embed.set_footer(text="Only you can see this menu")
        await interaction.response.send_message(
            embed=embed,
            view=DisciplineMenuView(self, is_global, interaction.user.id),
            ephemeral=True,
        )

    @discord.app_commands.command(name="disc", description="Discipline log management")
    async def disc(self, interaction: discord.Interaction):
        await self.show_disc_menu(interaction)

    @discord.app_commands.command(name="disc-setup", description="Set the discipline log channel")
    async def disc_setup(self, interaction: discord.Interaction, channel: discord.TextChannel):
        _, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global Admins can configure the discipline channel.",
                ephemeral=True,
            )
            return
        with sqlite3.connect(SETTINGS_DB) as db:
            db.execute(
                "INSERT OR REPLACE INTO discipline_settings VALUES ('channel_id', ?)",
                (str(channel.id),),
            )
            db.commit()
        await interaction.response.send_message(
            f"{theme.verifiedIcon} Discipline log channel set to {channel.mention}.",
            ephemeral=True,
        )

    @discord.app_commands.command(name="disc-expiry", description="Set the default infraction expiry (days)")
    async def disc_expiry(self, interaction: discord.Interaction, days: int):
        _, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global Admins can configure the default expiry.",
                ephemeral=True,
            )
            return
        with sqlite3.connect(SETTINGS_DB) as db:
            db.execute(
                "INSERT OR REPLACE INTO discipline_settings VALUES ('default_expiry_days', ?)",
                (str(days),),
            )
            db.commit()
        await interaction.response.send_message(
            f"{theme.verifiedIcon} Default expiry set to `{days}` days.",
            ephemeral=True,
        )


async def setup(bot):
    await bot.add_cog(DisciplineCog(bot))
