"""
cogs/transfer.py — State Transfer List Manager

Maintains a prioritised list of players nominated for an outbound state
transfer. Admins create a named transfer event, add players by FID (bulk
or one-at-a-time), assign priority tiers, and export a ranked CSV for
distribution. Each event tracks an optional power threshold to flag players
who require a special pass.

Access
------
- Via the Settings menu → State Transfer button
- Via the /transfer slash command (admin only)

Permissions
-----------
- Alliance Admin and above: Full access to all features

Slash commands
--------------
- /transfer    Open the transfer event manager

Features
--------
Create Event
  Enter a name, transfer date, and optional power threshold. If an event is
  already active, the admin can continue it or close it and start a new one.

Add Players
  Enter one or more FIDs (one per line or comma-separated). Select a priority
  tier for the whole batch (High / Med / Low). Each FID is looked up via the
  game API to fetch the player's nickname; any FIDs that can't be resolved
  are reported as failures.

View List
  Displays all players in the active event sorted by priority then addition
  order. Players whose power exceeds the threshold are flagged with ⚠️.

Change Priority
  Pick any player from a dropdown and reassign their priority tier.

Remove Player
  Pick any player from a dropdown and remove them from the list (hard delete).

Export CSV
  Enter the number of available passes. Generates a ranked CSV file with a
  cutoff row inserted at position N+1 so recipients can immediately see who
  makes the cut. Players above the power threshold are flagged in a
  "Special Pass Required" column. The file is sent to the requesting admin
  via Discord DM.

Close Event
  Archives the current event (sets status to 'closed'). All data is retained
  in the database for reference.

Database
--------
db/transfer.sqlite
  transfer_events — one row per transfer event
    id, name, transfer_date, power_threshold,
    status (active | closed), created_at, created_by_id

  transfer_players — one row per nominated player
    id, event_id, fid, name, power, priority (high | med | low),
    added_by_id, added_at
"""

import csv
import discord
import hashlib
import io
import sqlite3
import ssl
import time
import aiohttp
from datetime import datetime
from discord.ext import commands
from .browser_headers import get_headers
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

DB_PATH = 'db/transfer.sqlite'

PRIORITY_ORDER = {'high': 0, 'med': 1, 'low': 2}
PRIORITY_LABELS = {'high': 'High', 'med': 'Med', 'low': 'Low'}


# ── DB ────────────────────────────────────────────────────────────────────────

def _init_db():
    with sqlite3.connect(DB_PATH) as db:
        db.execute("""
            CREATE TABLE IF NOT EXISTS transfer_events (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                name            TEXT    NOT NULL,
                transfer_date   TEXT    NOT NULL,
                power_threshold TEXT,
                status          TEXT    NOT NULL DEFAULT 'active',
                created_at      TEXT    NOT NULL,
                created_by_id   TEXT    NOT NULL
            )
        """)
        db.execute("""
            CREATE TABLE IF NOT EXISTS transfer_players (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id    INTEGER NOT NULL,
                fid         TEXT    NOT NULL,
                name        TEXT    NOT NULL,
                power       TEXT,
                priority    TEXT    NOT NULL DEFAULT 'med',
                added_by_id TEXT    NOT NULL,
                added_at    TEXT    NOT NULL,
                FOREIGN KEY (event_id) REFERENCES transfer_events(id)
            )
        """)
        db.commit()


def _get_active_event():
    with sqlite3.connect(DB_PATH) as db:
        return db.execute(
            "SELECT id, name, transfer_date, power_threshold FROM transfer_events WHERE status = 'active' ORDER BY id DESC LIMIT 1"
        ).fetchone()


def _event_label(event):
    return f"{event[1]} — {event[2]}"


# ── WOS API ───────────────────────────────────────────────────────────────────

async def _fetch_player(fid):
    try:
        secret = "tB87#kPtkxqOS2"
        current_time = int(time.time() * 1000)
        form = f"fid={fid}&time={current_time}"
        sign = hashlib.md5((form + secret).encode("utf-8")).hexdigest()
        form = f"sign={sign}&{form}"
        url = "https://wos-giftcode-api.centurygame.com/api/player"
        headers = get_headers("https://wos-giftcode-api.centurygame.com")
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.post(url, headers=headers, data=form, ssl=ssl_ctx) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("data"):
                        return data["data"].get("nickname"), data["data"].get("avatar_image")
    except Exception:
        pass
    return None, None


# ── Modals ────────────────────────────────────────────────────────────────────

class NewEventModal(discord.ui.Modal, title="Create Transfer Event"):
    event_name = discord.ui.TextInput(
        label="Event Name",
        placeholder="e.g. Move to State 5",
        max_length=100,
    )
    transfer_date = discord.ui.TextInput(
        label="Transfer Date",
        placeholder="YYYY-MM-DD",
        max_length=20,
    )
    power_threshold = discord.ui.TextInput(
        label="Power Threshold (optional)",
        placeholder="e.g. 100000000  — players above this need a special pass",
        required=False,
        max_length=20,
    )

    def __init__(self, next_step):
        super().__init__()
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        await self._next(
            interaction,
            self.event_name.value.strip(),
            self.transfer_date.value.strip(),
            self.power_threshold.value.strip() or None,
        )


class AddPlayersModal(discord.ui.Modal, title="Add Players to Transfer List"):
    fids = discord.ui.TextInput(
        label="Player FIDs",
        placeholder="One per line or comma-separated",
        style=discord.TextStyle.paragraph,
        max_length=2000,
    )

    def __init__(self, next_step):
        super().__init__()
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.fids.value.strip()
        if "\n" in raw:
            fid_list = [f.strip() for f in raw.split("\n") if f.strip()]
        else:
            fid_list = [f.strip() for f in raw.split(",") if f.strip()]
        fid_list = [f for f in fid_list if f.isdigit()]
        await self._next(interaction, fid_list)


class ExportModal(discord.ui.Modal, title="Export Transfer List"):
    passes = discord.ui.TextInput(
        label="Number of Available Passes",
        placeholder="e.g. 30",
        max_length=4,
    )

    def __init__(self, next_step):
        super().__init__()
        self._next = next_step

    async def on_submit(self, interaction: discord.Interaction):
        try:
            count = int(self.passes.value.strip())
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Enter a whole number.", ephemeral=True
            )
            return
        await self._next(interaction, count)


# ── Priority select ───────────────────────────────────────────────────────────

class PrioritySelectView(discord.ui.View):
    def __init__(self, fid_list, author_id):
        super().__init__(timeout=120)
        self.fid_list = fid_list
        self.author_id = author_id

        options = [
            discord.SelectOption(label="High", value="high", emoji="🔴"),
            discord.SelectOption(label="Med",  value="med",  emoji="🟡"),
            discord.SelectOption(label="Low",  value="low",  emoji="🟢"),
        ]
        select = discord.ui.Select(placeholder="Select priority for this batch...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        priority = interaction.data["values"][0]
        event = _get_active_event()
        if not event:
            await interaction.response.edit_message(
                embed=discord.Embed(title=f"{theme.deniedIcon} No active event.", color=theme.emColor4),
                view=None,
            )
            return

        now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        added, failed = [], []

        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.timeIcon} Fetching player data...",
                description=f"Looking up {len(self.fid_list)} FID(s)...",
                color=theme.emColor1,
            ),
            view=None,
        )

        for fid in self.fid_list:
            nickname, _ = await _fetch_player(fid)
            if nickname:
                with sqlite3.connect(DB_PATH) as db:
                    db.execute(
                        "INSERT INTO transfer_players (event_id, fid, name, power, priority, added_by_id, added_at) VALUES (?, ?, ?, NULL, ?, ?, ?)",
                        (event[0], fid, nickname, priority, str(interaction.user.id), now),
                    )
                    db.commit()
                added.append(f"`{nickname}` ({fid})")
            else:
                failed.append(fid)

        desc = f"{theme.upperDivider}\n"
        if added:
            desc += f"**{theme.verifiedIcon} Added ({len(added)}):**\n" + "\n".join(added[:20])
            if len(added) > 20:
                desc += f"\n...and {len(added) - 20} more"
            desc += "\n\n"
        if failed:
            desc += f"**{theme.deniedIcon} Not found ({len(failed)}):**\n" + ", ".join(failed[:20])
            desc += "\n\n"
        desc += f"{theme.lowerDivider}"

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Players Added — Priority: {PRIORITY_LABELS[priority]}",
            description=desc,
            color=theme.emColor3 if added else theme.emColor4,
        )
        await interaction.edit_original_response(embed=embed, view=None)


# ── Remove player select ──────────────────────────────────────────────────────

class RemovePlayerView(discord.ui.View):
    def __init__(self, rows, author_id):
        super().__init__(timeout=120)
        self.author_id = author_id

        options = [
            discord.SelectOption(
                label=f"{row[2]} ({row[1]})"[:100],
                value=str(row[0]),
                description=f"Priority: {PRIORITY_LABELS.get(row[3], row[3])}  Power: {row[4] or 'N/A'}",
            )
            for row in rows[:25]
        ]
        select = discord.ui.Select(placeholder=f"{theme.trashIcon} Select player to remove...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        pid = int(interaction.data["values"][0])
        with sqlite3.connect(DB_PATH) as db:
            row = db.execute("SELECT fid, name FROM transfer_players WHERE id = ?", (pid,)).fetchone()
            db.execute("DELETE FROM transfer_players WHERE id = ?", (pid,))
            db.commit()
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Player Removed",
            description=f"`{row[1]}` ({row[0]}) removed from the transfer list.",
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ── Change priority select ────────────────────────────────────────────────────

class ChangePriorityPlayerView(discord.ui.View):
    def __init__(self, rows, author_id):
        super().__init__(timeout=120)
        self.author_id = author_id
        self.rows = rows

        options = [
            discord.SelectOption(
                label=f"{row[2]} ({row[1]})"[:100],
                value=str(row[0]),
                description=f"Current: {PRIORITY_LABELS.get(row[3], row[3])}",
            )
            for row in rows[:25]
        ]
        select = discord.ui.Select(placeholder="Select player to update...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        pid = int(interaction.data["values"][0])
        row = next((r for r in self.rows if r[0] == pid), None)
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.editListIcon} Select New Priority",
                description=f"Changing priority for **{row[2]}** ({row[1]})",
                color=theme.emColor1,
            ),
            view=ChangePriorityView(pid, row[2], interaction.user.id),
        )


class ChangePriorityView(discord.ui.View):
    def __init__(self, pid, name, author_id):
        super().__init__(timeout=120)
        self.pid = pid
        self.name = name
        self.author_id = author_id

        options = [
            discord.SelectOption(label="High", value="high", emoji="🔴"),
            discord.SelectOption(label="Med",  value="med",  emoji="🟡"),
            discord.SelectOption(label="Low",  value="low",  emoji="🟢"),
        ]
        select = discord.ui.Select(placeholder="Select new priority...", options=options)
        select.callback = self._on_select
        self.add_item(select)

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        priority = interaction.data["values"][0]
        with sqlite3.connect(DB_PATH) as db:
            db.execute("UPDATE transfer_players SET priority = ? WHERE id = ?", (priority, self.pid))
            db.commit()
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Priority Updated",
            description=f"**{self.name}** priority set to **{PRIORITY_LABELS[priority]}**.",
            color=theme.emColor3,
        )
        await interaction.response.edit_message(embed=embed, view=None)


# ── Confirm replace event ─────────────────────────────────────────────────────

class ConfirmReplaceView(discord.ui.View):
    def __init__(self, author_id):
        super().__init__(timeout=120)
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Start New", emoji="🆕", style=discord.ButtonStyle.danger)
    async def start_new(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def after_modal(i, name, date, threshold):
            with sqlite3.connect(DB_PATH) as db:
                db.execute("UPDATE transfer_events SET status = 'closed' WHERE status = 'active'")
                db.commit()
            await _create_event(i, name, date, threshold)

        await interaction.response.send_modal(NewEventModal(after_modal))

    @discord.ui.button(label="Continue Existing", emoji="📋", style=discord.ButtonStyle.success)
    async def continue_existing(self, interaction: discord.Interaction, button: discord.ui.Button):
        event = _get_active_event()
        cog = interaction.client.get_cog("TransferCog")
        if cog:
            await cog.show_main_menu(interaction, event)
        else:
            await interaction.response.edit_message(
                embed=discord.Embed(title=f"{theme.deniedIcon} Transfer module not found.", color=theme.emColor4),
                view=None,
            )

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.deniedIcon} Cancelled",
                description="No changes made.",
                color=theme.emColor4,
            ),
            view=None,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

async def _create_event(interaction, name, date, threshold, use_send=False):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with sqlite3.connect(DB_PATH) as db:
        db.execute(
            "INSERT INTO transfer_events (name, transfer_date, power_threshold, status, created_at, created_by_id) VALUES (?, ?, ?, 'active', ?, ?)",
            (name, date, threshold, now, str(interaction.user.id)),
        )
        db.commit()
    event = _get_active_event()
    cog = interaction.client.get_cog("TransferCog")
    if cog:
        await cog.show_main_menu(interaction, event, use_send=use_send)


# ── Main menu ─────────────────────────────────────────────────────────────────

class TransferMenuView(discord.ui.View):
    def __init__(self, cog, event, author_id):
        super().__init__(timeout=300)
        self.cog = cog
        self.event = event
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Add Players", emoji="➕", style=discord.ButtonStyle.success, row=0)
    async def add_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def after_fids(i, fid_list):
            if not fid_list:
                await i.response.edit_message(
                    embed=discord.Embed(
                        title=f"{theme.deniedIcon} No valid FIDs entered.",
                        color=theme.emColor4,
                    ),
                    view=None,
                )
                return
            embed = discord.Embed(
                title=f"{theme.userIcon} Select Priority",
                description=f"Adding **{len(fid_list)}** player(s). Select a priority for this batch.",
                color=theme.emColor1,
            )
            if i.response.is_done():
                await i.edit_original_response(embed=embed, view=PrioritySelectView(fid_list, i.user.id))
            else:
                await i.response.edit_message(embed=embed, view=PrioritySelectView(fid_list, i.user.id))

        await interaction.response.send_modal(AddPlayersModal(after_fids))

    @discord.ui.button(label="View List", emoji="👁️", style=discord.ButtonStyle.primary, row=0)
    async def view_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        event = self.event
        with sqlite3.connect(DB_PATH) as db:
            rows = db.execute(
                "SELECT id, fid, name, priority, power FROM transfer_players WHERE event_id = ? ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END, added_at",
                (event[0],),
            ).fetchall()

        threshold = event[3]
        embed = discord.Embed(
            title=f"{theme.listIcon} Transfer List — {_event_label(event)}",
            color=theme.emColor1,
        )

        if not rows:
            embed.description = f"{theme.blankListIcon} No players on the list yet."
        else:
            lines = []
            for idx, (pid, fid, name, priority, power) in enumerate(rows, 1):
                flag = ""
                if power and threshold:
                    try:
                        if float(power) > float(threshold):
                            flag = " ⚠️"
                    except ValueError:
                        pass
                pri_emoji = {"high": "🔴", "med": "🟡", "low": "🟢"}.get(priority, "⚪")
                power_str = f" • Power: `{power}`" if power else ""
                lines.append(f"`{idx}.` {pri_emoji} **{name}**{flag} — `{fid}`{power_str}")
            embed.description = "\n".join(lines[:40])
            if len(rows) > 40:
                embed.description += f"\n...and {len(rows) - 40} more"
            embed.set_footer(text=f"{len(rows)} player(s) total" + (f"  •  ⚠️ = needs special pass (power > {threshold})" if threshold else ""))

        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=self)
        else:
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Change Priority", emoji="📅", style=discord.ButtonStyle.secondary, row=1)
    async def priority_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        event = self.event
        with sqlite3.connect(DB_PATH) as db:
            rows = db.execute(
                "SELECT id, fid, name, priority, power FROM transfer_players WHERE event_id = ? ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END, added_at",
                (event[0],),
            ).fetchall()

        if not rows:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No players on the list.", ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"{theme.editListIcon} Change Priority",
            description="Select a player to update their priority.",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=ChangePriorityPlayerView(rows, interaction.user.id))

    @discord.ui.button(label="Remove Player", emoji="🗑️", style=discord.ButtonStyle.danger, row=1)
    async def remove_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        event = self.event
        with sqlite3.connect(DB_PATH) as db:
            rows = db.execute(
                "SELECT id, fid, name, priority, power FROM transfer_players WHERE event_id = ? ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END, added_at",
                (event[0],),
            ).fetchall()

        if not rows:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No players on the list.", ephemeral=True
            )
            return

        embed = discord.Embed(
            title=f"{theme.trashIcon} Remove Player",
            description="Select a player to remove from the transfer list.",
            color=theme.emColor2,
        )
        await interaction.response.edit_message(embed=embed, view=RemovePlayerView(rows, interaction.user.id))

    @discord.ui.button(label="Export CSV", emoji="📤", style=discord.ButtonStyle.primary, row=2)
    async def export_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        event = self.event

        async def after_modal(i, pass_count):
            with sqlite3.connect(DB_PATH) as db:
                rows = db.execute(
                    "SELECT fid, name, priority, power FROM transfer_players WHERE event_id = ? ORDER BY CASE priority WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END, added_at",
                    (event[0],),
                ).fetchall()

            if not rows:
                await i.response.send_message(
                    f"{theme.deniedIcon} No players on the list to export.", ephemeral=True
                )
                return

            threshold = event[3]
            output = io.StringIO()
            writer = csv.writer(output)

            writer.writerow(["Transfer Event:", _event_label(event)])
            writer.writerow(["Available Passes:", pass_count])
            if threshold:
                writer.writerow(["Power Threshold:", threshold])
            writer.writerow(["Export Date:", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")])
            writer.writerow(["Total Players:", len(rows)])
            writer.writerow([])
            writer.writerow(["#", "FID", "Name", "Priority", "Power", "Special Pass Required"])

            for idx, (fid, name, priority, power) in enumerate(rows, 1):
                if idx == pass_count + 1:
                    writer.writerow([])
                    writer.writerow(["--- PASS CUTOFF ---", f"Passes available: {pass_count}", "", "", "", ""])
                    writer.writerow([])

                special_pass = ""
                if power and threshold:
                    try:
                        special_pass = "YES" if float(power) > float(threshold) else ""
                    except ValueError:
                        pass

                writer.writerow([idx, fid, name, PRIORITY_LABELS.get(priority, priority), power or "", special_pass])

            output.seek(0)
            safe_name = _event_label(event).replace(" ", "_").replace("—", "-")
            filename = f"transfer_{safe_name}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            file = discord.File(io.BytesIO(output.getvalue().encode("utf-8")), filename=filename)

            try:
                await i.user.send(
                    embed=discord.Embed(
                        title=f"{theme.verifiedIcon} Transfer List Exported",
                        description=(
                            f"**{_event_label(event)}**\n\n"
                            f"{theme.userIcon} **Players:** {len(rows)}\n"
                            f"{theme.shieldIcon} **Passes:** {pass_count}\n"
                            f"Players ranked High → Med → Low with cutoff at position #{pass_count}."
                        ),
                        color=theme.emColor3,
                    ),
                    file=file,
                )
                if i.response.is_done():
                    await i.edit_original_response(
                        embed=discord.Embed(
                            title=f"{theme.verifiedIcon} Export Sent",
                            description="Check your DMs for the CSV file.",
                            color=theme.emColor3,
                        ),
                        view=None,
                    )
                else:
                    await i.response.edit_message(
                        embed=discord.Embed(
                            title=f"{theme.verifiedIcon} Export Sent",
                            description="Check your DMs for the CSV file.",
                            color=theme.emColor3,
                        ),
                        view=None,
                    )
            except discord.Forbidden:
                await i.response.send_message(
                    f"{theme.deniedIcon} Could not DM you — please enable DMs from server members.",
                    ephemeral=True,
                )

        await interaction.response.send_modal(ExportModal(after_modal))

    @discord.ui.button(label="Close Event", emoji="🔒", style=discord.ButtonStyle.secondary, row=2)
    async def close_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title=f"{theme.warnIcon} Close Event?",
            description=(
                f"Close **{_event_label(self.event)}**?\n\n"
                "The event and its list will be archived but not deleted."
            ),
            color=theme.emColor4,
        )
        await interaction.response.edit_message(embed=embed, view=ConfirmCloseView(self.event, interaction.user.id))


class ConfirmCloseView(discord.ui.View):
    def __init__(self, event, author_id):
        super().__init__(timeout=120)
        self.event = event
        self.author_id = author_id

    async def interaction_check(self, interaction: discord.Interaction):
        if interaction.user.id != self.author_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the original user can interact with this.", ephemeral=True
            )
            return False
        return True

    @discord.ui.button(label="Close Event", emoji="🔒", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        with sqlite3.connect(DB_PATH) as db:
            db.execute("UPDATE transfer_events SET status = 'closed' WHERE id = ?", (self.event[0],))
            db.commit()
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.verifiedIcon} Event Closed",
                description=f"**{_event_label(self.event)}** has been archived.",
                color=theme.emColor3,
            ),
            view=None,
        )

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = interaction.client.get_cog("TransferCog")
        if cog:
            await cog.show_main_menu(interaction, self.event)


# ── Cog ───────────────────────────────────────────────────────────────────────

class TransferCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        _init_db()

    async def show_main_menu(self, interaction: discord.Interaction, event, use_send=False):
        embed = discord.Embed(
            title=f"{theme.listIcon} State Transfer",
            description=(
                f"**Active Event:** {_event_label(event)}\n"
                + (f"**Power Threshold:** `{event[3]}`\n" if event[3] else "")
                + f"\n{theme.upperDivider}\n"
                f"➕ **Add Players** — Add one or more FIDs to the list\n"
                f"👁️ **View List** — See current transfer list\n"
                f"📅 **Change Priority** — Update a player's priority\n"
                f"🗑️ **Remove Player** — Remove someone from the list\n"
                f"📤 **Export CSV** — Generate ranked CSV sent via DM\n"
                f"🔒 **Close Event** — Archive this event\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )
        embed.set_footer(text="Only you can see this menu")
        view = TransferMenuView(self, event, interaction.user.id)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=embed, view=view)
        elif use_send:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

    async def show_transfer_menu(self, interaction: discord.Interaction):
        await self._transfer_entry(interaction)

    @discord.app_commands.command(name="transfer", description="State transfer list management")
    async def transfer(self, interaction: discord.Interaction):
        await self._transfer_entry(interaction)

    async def _transfer_entry(self, interaction: discord.Interaction):
        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} You do not have permission to use this command.",
                ephemeral=True,
            )
            return

        event = _get_active_event()

        if event:
            embed = discord.Embed(
                title=f"{theme.listIcon} State Transfer",
                description=(
                    f"An active transfer event already exists:\n\n"
                    f"{theme.upperDivider}\n"
                    f"**{_event_label(event)}**\n"
                    + (f"Power Threshold: `{event[3]}`\n" if event[3] else "")
                    + f"{theme.lowerDivider}\n\n"
                    f"Do you want to continue with this event or start a new one?"
                ),
                color=theme.emColor1,
            )
            await interaction.response.send_message(embed=embed, view=ConfirmReplaceView(interaction.user.id), ephemeral=True)
        else:
            async def after_modal(i, name, date, threshold):
                await _create_event(i, name, date, threshold, use_send=True)

            await interaction.response.send_modal(NewEventModal(after_modal))


async def setup(bot):
    await bot.add_cog(TransferCog(bot))
