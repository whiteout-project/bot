"""Admin UI + DB helpers for per-channel OCR config: alliance owner, accepted event types, info-message toggles."""
from __future__ import annotations
import sqlite3
from typing import Optional

import discord

from .pimp_my_bot import theme
from .permission_handler import PermissionManager
from .attendance_ocr_parsers import EVENT_TYPES

_SETTINGS_DB = "db/settings.sqlite"


# ── channel-config DB helpers ─────────────────────────────────────────────

def get_channel_settings(channel_id: int) -> Optional[dict]:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT channel_id, alliance_id, post_info_message, pin_info_message, "
            "info_message_id, auto_delete_screenshots "
            "FROM ocr_channel_settings WHERE channel_id = ?",
            (channel_id,),
        ).fetchone()
    if row is None:
        return None
    return {
        "channel_id": row[0],
        "alliance_id": row[1],
        "post_info_message": bool(row[2]),
        "pin_info_message": bool(row[3]),
        "info_message_id": row[4],
        "auto_delete_screenshots": bool(row[5]) if row[5] is not None else True,
    }


def set_auto_delete_screenshots(channel_id: int, value: bool) -> None:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "UPDATE ocr_channel_settings SET auto_delete_screenshots = ? WHERE channel_id = ?",
            (int(value), channel_id),
        )
        conn.commit()


def upsert_channel_settings(channel_id: int, alliance_id: int,
                            post_info_message: bool = False,
                            pin_info_message: bool = False) -> None:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "INSERT INTO ocr_channel_settings "
            "(channel_id, alliance_id, post_info_message, pin_info_message) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET "
            "alliance_id=excluded.alliance_id, "
            "post_info_message=excluded.post_info_message, "
            "pin_info_message=excluded.pin_info_message",
            (channel_id, alliance_id, int(post_info_message), int(pin_info_message)),
        )
        conn.commit()


def set_info_message_id(channel_id: int, message_id: Optional[int]) -> None:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "UPDATE ocr_channel_settings SET info_message_id = ? WHERE channel_id = ?",
            (message_id, channel_id),
        )
        conn.commit()


def delete_channel_settings(channel_id: int) -> None:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute("DELETE FROM ocr_channel_settings WHERE channel_id = ?", (channel_id,))
        conn.execute("DELETE FROM ocr_channel_event_keywords WHERE channel_id = ?", (channel_id,))
        conn.commit()


def get_channel_keywords(channel_id: int) -> dict[str, list[str]]:
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT event_type, keyword FROM ocr_channel_event_keywords "
            "WHERE channel_id = ? ORDER BY event_type, keyword",
            (channel_id,),
        ).fetchall()
    out: dict[str, list[str]] = {}
    for event_type, kw in rows:
        out.setdefault(event_type, []).append(kw)
    return out


def set_event_keywords(channel_id: int, event_type: str, keywords: list[str]) -> None:
    keywords = [k.strip() for k in keywords if k.strip()]
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "DELETE FROM ocr_channel_event_keywords WHERE channel_id = ? AND event_type = ?",
            (channel_id, event_type),
        )
        conn.executemany(
            "INSERT INTO ocr_channel_event_keywords (channel_id, event_type, keyword) "
            "VALUES (?, ?, ?)",
            [(channel_id, event_type, kw) for kw in keywords],
        )
        conn.commit()


def get_enabled_events(channel_id: int) -> list[str]:
    """Return event_types enabled on this channel, in EVENT_TYPES order."""
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        rows = conn.execute(
            "SELECT event_type FROM ocr_channel_enabled_events WHERE channel_id = ?",
            (channel_id,),
        ).fetchall()
    enabled = {r[0] for r in rows}
    return [et for et in EVENT_TYPES if et in enabled]


def enable_event_for_channel(channel_id: int, event_type: str) -> None:
    """Mark event_type as enabled on this channel. Idempotent. Does not add keywords."""
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO ocr_channel_enabled_events "
            "(channel_id, event_type) VALUES (?, ?)",
            (channel_id, event_type),
        )
        conn.commit()


def disable_event_for_channel(channel_id: int, event_type: str) -> None:
    """Disable event_type on this channel and clear any keywords stored for it."""
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        conn.execute(
            "DELETE FROM ocr_channel_enabled_events WHERE channel_id = ? AND event_type = ?",
            (channel_id, event_type),
        )
        conn.execute(
            "DELETE FROM ocr_channel_event_keywords WHERE channel_id = ? AND event_type = ?",
            (channel_id, event_type),
        )
        conn.commit()


def remove_event_from_channel(channel_id: int, event_type: str) -> None:
    """Back-compat shim — equivalent to disable_event_for_channel."""
    disable_event_for_channel(channel_id, event_type)


def get_ocr_upload_admin_only(alliance_id: int) -> bool:
    """Read the per-alliance "admins only can upload" gate."""
    with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
        row = conn.execute(
            "SELECT ocr_upload_admin_only FROM alliancesettings WHERE alliance_id = ?",
            (alliance_id,),
        ).fetchone()
    return bool(row[0]) if row and row[0] is not None else False


def set_ocr_upload_admin_only(alliance_id: int, value: bool) -> None:
    """Write the per-alliance "admins only" gate. INSERTs the row if missing."""
    with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
        conn.execute(
            "INSERT INTO alliancesettings (alliance_id, ocr_upload_admin_only) "
            "VALUES (?, ?) "
            "ON CONFLICT(alliance_id) DO UPDATE SET ocr_upload_admin_only = excluded.ocr_upload_admin_only",
            (alliance_id, int(bool(value))),
        )
        conn.commit()


def find_conflicting_channel_owner(channel_id: int, requesting_alliance_id: int) -> Optional[tuple[str, int, str]]:
    """Return (feature, alliance_id, alliance_name) of the alliance that
    already claims this channel for OCR (Bear or Screenshot Upload), or
    None if it's free. Re-picking by the same alliance is not a conflict.
    """
    with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
        row = conn.execute(
            "SELECT a.alliance_id, l.name FROM alliancesettings a "
            "LEFT JOIN alliance_list l ON l.alliance_id = a.alliance_id "
            "WHERE a.bear_score_channel = ? AND a.alliance_id != ? LIMIT 1",
            (channel_id, requesting_alliance_id),
        ).fetchone()
        if row:
            return ("Bear Tracking", int(row[0]), row[1] or f"#{row[0]}")
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        row = conn.execute(
            "SELECT alliance_id FROM ocr_channel_settings "
            "WHERE channel_id = ? AND alliance_id != ? LIMIT 1",
            (channel_id, requesting_alliance_id),
        ).fetchone()
    if row:
        return ("Screenshot Upload", int(row[0]), _alliance_name(int(row[0])))
    return None


def format_channel_conflict(conflict: tuple[str, int, str], channel_mention: str) -> str:
    feature, _aid, alliance_name = conflict
    return (
        f"{channel_mention} is already used as a **{feature}** channel by "
        f"alliance **{alliance_name}**. Pick a different channel, or remove "
        f"it from {alliance_name} first."
    )


# User-facing "where to find this screenshot in-game" descriptions, keyed by
# the event_type used in EVENT_TYPES. Foundry / Canyon entries cover BOTH
# the pre-event registration mail AND the post-event result mail — the bot
# auto-detects which kind a screenshot is and merges them into one session.
_EVENT_LOCATIONS: dict[str, str] = {
    "foundry_battle":    "Mail → Alliance → [Foundry Battle] Registration Ends *and* Results",
    "canyon_clash":      "Mail → Alliance → [Canyon Clash] Solo Participation *and* Legion X Battle Result",
    "power_rankings":    "Alliance → Power Rankings",
    "alliance_showdown": "Mail → Alliance → Alliance Showdown Ranking",
}


def render_info_message(channel_id: int) -> str:
    enabled = get_enabled_events(channel_id)
    if not enabled:
        return (
            f"{theme.importIcon} **Screenshot Upload — no event types configured yet.**\n"
            "An admin needs to set up which event screenshots this channel accepts."
        )

    settings = get_channel_settings(channel_id) or {}
    admin_only = get_ocr_upload_admin_only(settings.get("alliance_id", 0))

    lines = [
        f"{theme.importIcon} **Upload event screenshots here**",
        "",
        f"{theme.listIcon} **Supported screenshots**",
        f"{theme.upperDivider}",
    ]
    for et in enabled:
        cfg = EVENT_TYPES.get(et)
        label = cfg.label if cfg else et
        location = _EVENT_LOCATIONS.get(et, "")
        if location:
            lines.append(f"• **{label}**  ·  {location}")
        else:
            lines.append(f"• **{label}**")
    lines.append(f"{theme.lowerDivider}")
    lines.append("")
    lines.append(f"{theme.warnIcon} **Before you upload**")
    lines.append(f"{theme.upperDivider}")
    lines.append(
        "• One event at a time — upload all screenshots for a single event together. "
        "For Foundry/Canyon, include BOTH the registration mail AND the result mail "
        "in the same drop so the bot can match registered combatants to their scores."
    )
    lines.append("• Don't mix screenshots from different events in the same upload.")
    lines.append(
        "• Include the header (with timestamp) of each screenshot — "
        "this is how the bot identifies the event and tells registration apart from results."
    )
    lines.append("• OCR isn't perfect; review and edit the parsed data before submitting.")
    lines.append("• Save your current event before starting the next upload.")
    lines.append(
        f"• Set your in-game interface language to **English** before "
        f"taking screenshots — other languages aren't supported yet."
    )
    lines.append(f"{theme.lowerDivider}")
    lines.append("")
    lines.append(
        f"{theme.lockIcon} **Upload permission:** "
        f"{'Admins only' if admin_only else 'Anyone in this channel'}"
    )
    if settings.get("auto_delete_screenshots", True):
        lines.append(
            f"{theme.trashIcon} **Auto-delete:** your screenshot messages are "
            "removed after the bot finishes reading them, keeping this channel clean."
        )
    return "\n".join(lines)


# ── admin UI helpers ──────────────────────────────────────────────────────

def _admin_alliance_ids(user_id: int, guild_id: int) -> list[int]:
    ids, is_global = PermissionManager.get_admin_alliance_ids(user_id, guild_id)
    if is_global:
        with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
            rows = conn.execute("SELECT alliance_id FROM alliance_list").fetchall()
        return [int(r[0]) for r in rows]
    return [int(a) for a in ids]


def _alliance_name(alliance_id: int) -> str:
    with sqlite3.connect("db/alliance.sqlite", timeout=30.0) as conn:
        row = conn.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?",
            (alliance_id,),
        ).fetchone()
    return row[0] if row else f"#{alliance_id}"


def _all_configured_channels(allowed_alliance_ids: list[int]) -> list[dict]:
    if not allowed_alliance_ids:
        return []
    placeholders = ",".join("?" * len(allowed_alliance_ids))
    with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
        rows = conn.execute(
            f"SELECT channel_id, alliance_id, post_info_message, pin_info_message "
            f"FROM ocr_channel_settings WHERE alliance_id IN ({placeholders}) "
            f"ORDER BY alliance_id, channel_id",
            allowed_alliance_ids,
        ).fetchall()
    return [
        {"channel_id": r[0], "alliance_id": r[1],
         "post_info_message": bool(r[2]), "pin_info_message": bool(r[3])}
        for r in rows
    ]


def _summarize_events(channel_id: int) -> str:
    """Format the event list for a configured channel. If every supported
    event type is enabled, collapse to 'All Supported Events'.
    """
    enabled = get_enabled_events(channel_id)
    if not enabled:
        return "(no events)"
    if len(enabled) == len(EVENT_TYPES):
        return "All Events"
    return ", ".join(EVENT_TYPES[et].label for et in enabled)


def build_overview_embed(user_id: int, guild_id: int) -> discord.Embed:
    alliance_ids = _admin_alliance_ids(user_id, guild_id)
    channels = _all_configured_channels(alliance_ids)

    intro = (
        "Each configured channel becomes an upload zone for event "
        "screenshots. The bot OCRs each upload, routes it to the right "
        "event parser, and lets the uploader review the extracted data "
        "before recording it."
    )

    lines = [
        intro,
        "",
        "**Configured Channels**",
        f"{theme.upperDivider}",
    ]
    if not channels:
        lines.append(f"{theme.warnIcon} No channels for screenshot upload configured yet.")
    else:
        for i, ch in enumerate(channels):
            lines.append(f"{theme.importIcon} **<#{ch['channel_id']}>**")
            lines.append(f"└ Alliance: `{_alliance_name(ch['alliance_id'])}`")
            lines.append(f"└ Accepting: {_summarize_events(ch['channel_id'])}")
            if i < len(channels) - 1:
                lines.append("")
    lines.append(f"{theme.lowerDivider}")
    lines.append("")
    if channels:
        lines.append(
            "Pick a channel from the dropdown to edit its event types and "
            "settings, or use **Add Channel** to register another."
        )
    else:
        lines.append("Use **Add Channel** to register your first channel.")

    return discord.Embed(
        title=f"{theme.importIcon} Screenshot Upload",
        description="\n".join(lines),
        color=theme.emColor1,
    )


# ── admin views ───────────────────────────────────────────────────────────

class OCRChannelListView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self._build_components()

    def _channel_label(self, channel_id: int) -> str:
        """Look up the actual channel name via the bot; fall back to the ID if missing."""
        ch = self.cog.bot.get_channel(channel_id)
        return f"#{ch.name}" if ch is not None else f"#{channel_id}"

    def _build_components(self):
        self.clear_items()
        alliance_ids = _admin_alliance_ids(self.user_id, self.guild_id)
        channels = _all_configured_channels(alliance_ids)

        if channels:
            options = [
                discord.SelectOption(
                    label=self._channel_label(ch["channel_id"])[:100],
                    value=str(ch["channel_id"]),
                    description=_alliance_name(ch["alliance_id"])[:100],
                )
                for ch in channels[:25]
            ]
            select = discord.ui.Select(
                placeholder="Edit a configured channel…",
                options=options, row=0,
            )
            select.callback = self._edit_existing
            self.add_item(select)

        add_btn = discord.ui.Button(
            label="Add Channel", emoji=theme.addIcon,
            style=discord.ButtonStyle.primary, row=1,
        )
        add_btn.callback = self._add_new
        self.add_item(add_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._back
        self.add_item(back_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this menu can use it.",
                ephemeral=True,
            )
            return False
        return True

    async def _add_new(self, interaction: discord.Interaction):
        view = _ChannelPickerView(self.cog, self.user_id, self.guild_id, parent=self)
        embed = discord.Embed(
            title=f"{theme.importIcon} Add Screenshot Upload Channel",
            description="Pick the channel where event screenshots will be uploaded.",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def _edit_existing(self, interaction: discord.Interaction):
        channel_id = int(interaction.data["values"][0])
        settings = get_channel_settings(channel_id)
        if settings is None:
            await interaction.response.send_message(
                f"{theme.warnIcon} That channel is no longer configured.",
                ephemeral=True,
            )
            return
        view = OCRChannelEditView(
            self.cog, self.user_id, self.guild_id,
            channel_id=channel_id, alliance_id=settings["alliance_id"], parent=self,
        )
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    async def _back(self, interaction: discord.Interaction):
        attendance_cog = self.cog.bot.get_cog("Attendance")
        if attendance_cog:
            await attendance_cog.show_attendance_menu(interaction)


class _ChannelPickerView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int, parent: OCRChannelListView):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.parent = parent

        select = discord.ui.ChannelSelect(
            channel_types=[discord.ChannelType.text],
            placeholder="Pick a channel…",
            min_values=1, max_values=1, row=0,
        )
        select.callback = self._picked
        self.add_item(select)

        cancel = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        cancel.callback = self._cancel
        self.add_item(cancel)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    async def _picked(self, interaction: discord.Interaction):
        channel_id = int(interaction.data["values"][0])
        view = _AlliancePickerView(
            self.cog, self.user_id, self.guild_id,
            channel_id=channel_id, parent=self.parent,
        )
        embed = discord.Embed(
            title=f"{theme.importIcon} Add Screenshot Upload Channel",
            description=f"Which alliance owns <#{channel_id}>?",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=view)

    async def _cancel(self, interaction: discord.Interaction):
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=build_overview_embed(self.user_id, self.guild_id),
            view=self.parent,
        )


class _AlliancePickerView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int, channel_id: int,
                 parent: OCRChannelListView):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.parent = parent

        alliance_ids = _admin_alliance_ids(user_id, guild_id)
        options = [
            discord.SelectOption(label=_alliance_name(aid)[:100], value=str(aid))
            for aid in alliance_ids[:25]
        ]
        select = discord.ui.Select(
            placeholder="Pick an alliance…",
            options=options or [discord.SelectOption(label="(no alliances)", value="0")],
            row=0, disabled=not options,
        )
        select.callback = self._picked
        self.add_item(select)

        cancel = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        cancel.callback = self._cancel
        self.add_item(cancel)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return interaction.user.id == self.user_id

    async def _picked(self, interaction: discord.Interaction):
        alliance_id = int(interaction.data["values"][0])

        # Reservation check: refuse if another alliance already owns this
        # channel for Bear Tracking or Screenshot Upload.
        conflict = find_conflicting_channel_owner(self.channel_id, alliance_id)
        if conflict is not None:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Channel already in use",
                description=format_channel_conflict(conflict, f"<#{self.channel_id}>"),
                color=theme.emColor4,
            )
            self.parent._build_components()
            await interaction.response.edit_message(embed=embed, view=self.parent)
            return

        upsert_channel_settings(self.channel_id, alliance_id,
                                post_info_message=False, pin_info_message=False)
        # Enable every supported event by default — admin can disable individual ones
        # afterwards. No keywords installed: the fingerprint regex classifies on its own.
        for et in EVENT_TYPES:
            enable_event_for_channel(self.channel_id, et)
        # Post + pin the info message immediately so the toggle state matches reality.
        await self.cog.refresh_info_message(self.channel_id)

        view = OCRChannelEditView(
            self.cog, self.user_id, self.guild_id,
            channel_id=self.channel_id, alliance_id=alliance_id, parent=self.parent,
        )
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    async def _cancel(self, interaction: discord.Interaction):
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=build_overview_embed(self.user_id, self.guild_id),
            view=self.parent,
        )


class OCRChannelEditView(discord.ui.View):
    def __init__(self, cog, user_id: int, guild_id: int, channel_id: int,
                 alliance_id: int, parent: OCRChannelListView):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.alliance_id = alliance_id
        self.parent = parent
        self._build_components()

    def _enabled_events(self) -> set[str]:
        return set(get_enabled_events(self.channel_id))

    def build_embed(self) -> discord.Embed:
        enabled = self._enabled_events()
        settings = get_channel_settings(self.channel_id) or {}
        admin_only = get_ocr_upload_admin_only(self.alliance_id)
        post_on = bool(settings.get("post_info_message"))
        pin_on = bool(settings.get("pin_info_message"))

        # Info message status: "On (Pinned)" / "On" / "Off"
        if post_on and pin_on:
            info_state = "On (Pinned)"
        elif post_on:
            info_state = "On"
        else:
            info_state = "Off"

        uploaders_state = (
            "Only admins can upload" if admin_only
            else "Anyone with channel access can upload"
        )

        lines = [
            f"**Channel:** <#{self.channel_id}>  ·  "
            f"**Alliance:** `{_alliance_name(self.alliance_id)}`",
            "",
            f"**Configure this channel**",
            f"{theme.upperDivider}",
            f"{theme.listIcon} **Accepted event types**",
        ]
        enabled_in_order = [et for et in EVENT_TYPES if et in enabled]
        if enabled_in_order:
            for et in enabled_in_order:
                lines.append(f"  • {EVENT_TYPES[et].label}")
        else:
            lines.append("  *(none — channel won't process any screenshots)*")
        lines.append("└ Use the dropdown below to add or remove event types")
        lines.append("")
        lines.append(f"{theme.documentIcon} **Info message:** {info_state}")
        lines.append("└ Pinned helper message that explains how to upload screenshots")
        lines.append("")
        lines.append(f"{theme.lockIcon} **Uploaders:** {uploaders_state}")
        lines.append("└ Per-alliance setting — applies to every Screenshot Upload "
                     f"channel for `{_alliance_name(self.alliance_id)}`")
        lines.append("")
        auto_delete_on = bool(settings.get("auto_delete_screenshots", True))
        lines.append(
            f"{theme.trashIcon} **Auto-delete screenshots:** "
            f"{'On' if auto_delete_on else 'Off'}"
        )
        lines.append("└ Removes the uploader's screenshot messages after OCR "
                     "finishes, keeping the channel clean")
        lines.append("")
        lines.append(f"{theme.editListIcon} **Edit Keywords**")
        lines.append("└ Optional keyword filter per event type — usually not needed; "
                     "fingerprint detection alone is reliable")
        lines.append("")
        lines.append(f"{theme.trashIcon} **Remove Channel**")
        lines.append("└ Stop processing screenshots here and remove the info message")
        lines.append(f"{theme.lowerDivider}")
        return discord.Embed(
            title=f"{theme.importIcon} Edit Screenshot Upload Channel",
            description="\n".join(lines),
            color=theme.emColor1,
        )

    def _build_components(self):
        self.clear_items()
        enabled = self._enabled_events()
        options = [
            discord.SelectOption(label=cfg.label[:100], value=et, default=et in enabled)
            for et, cfg in EVENT_TYPES.items()
        ]
        select = discord.ui.Select(
            placeholder="Toggle event types…",
            options=options,
            min_values=0, max_values=len(options), row=0,
        )
        select.callback = self._toggle_events
        self.add_item(select)

        settings = get_channel_settings(self.channel_id) or {}
        post_on = bool(settings.get("post_info_message"))
        pin_on = bool(settings.get("pin_info_message"))

        post_btn = discord.ui.Button(
            label=f"Info message: {'On' if post_on else 'Off'}",
            emoji=theme.documentIcon,
            style=discord.ButtonStyle.success if post_on else discord.ButtonStyle.secondary,
            row=1,
        )
        post_btn.callback = self._toggle_post
        self.add_item(post_btn)

        pin_btn = discord.ui.Button(
            label=f"Pin info: {'On' if pin_on else 'Off'}",
            emoji=theme.pinIcon,
            style=discord.ButtonStyle.success if pin_on else discord.ButtonStyle.secondary,
            row=1,
            disabled=not post_on,
        )
        pin_btn.callback = self._toggle_pin
        self.add_item(pin_btn)

        admin_only = get_ocr_upload_admin_only(self.alliance_id)
        uploaders_btn = discord.ui.Button(
            label=f"Uploaders: {'Admins only' if admin_only else 'Anyone'}",
            emoji=theme.lockIcon,
            style=discord.ButtonStyle.success if admin_only else discord.ButtonStyle.secondary,
            row=1,
        )
        uploaders_btn.callback = self._toggle_uploaders
        self.add_item(uploaders_btn)

        auto_delete_on = bool(settings.get("auto_delete_screenshots", True))
        auto_delete_btn = discord.ui.Button(
            label=f"Auto-delete: {'On' if auto_delete_on else 'Off'}",
            emoji=theme.trashIcon,
            style=discord.ButtonStyle.success if auto_delete_on else discord.ButtonStyle.secondary,
            row=1,
        )
        auto_delete_btn.callback = self._toggle_auto_delete
        self.add_item(auto_delete_btn)

        keywords_btn = discord.ui.Button(
            label="Edit Keywords", emoji=theme.editListIcon,
            style=discord.ButtonStyle.primary, row=2,
        )
        keywords_btn.callback = self._open_keywords
        self.add_item(keywords_btn)

        remove_btn = discord.ui.Button(
            label="Remove Channel", emoji=theme.trashIcon,
            style=discord.ButtonStyle.danger, row=2,
        )
        remove_btn.callback = self._remove
        self.add_item(remove_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        back_btn.callback = self._back
        self.add_item(back_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this menu can use it.",
                ephemeral=True,
            )
            return False
        return True

    async def _toggle_events(self, interaction: discord.Interaction):
        selected = set(interaction.data["values"])
        current = self._enabled_events()
        # Enabling an event no longer auto-installs keywords — fingerprint
        # detection runs without them. Admins can add keywords later via
        # the Detection Keywords editor if they want a narrowing prefilter.
        for et in selected - current:
            enable_event_for_channel(self.channel_id, et)
        for et in current - selected:
            disable_event_for_channel(self.channel_id, et)
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _toggle_post(self, interaction: discord.Interaction):
        settings = get_channel_settings(self.channel_id) or {}
        upsert_channel_settings(
            self.channel_id, self.alliance_id,
            post_info_message=not bool(settings.get("post_info_message")),
            pin_info_message=bool(settings.get("pin_info_message")),
        )
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _toggle_pin(self, interaction: discord.Interaction):
        settings = get_channel_settings(self.channel_id) or {}
        upsert_channel_settings(
            self.channel_id, self.alliance_id,
            post_info_message=bool(settings.get("post_info_message")),
            pin_info_message=not bool(settings.get("pin_info_message")),
        )
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _toggle_uploaders(self, interaction: discord.Interaction):
        # Per-alliance setting — flipping it here affects every Screenshot
        # Upload channel owned by the same alliance.
        current = get_ocr_upload_admin_only(self.alliance_id)
        set_ocr_upload_admin_only(self.alliance_id, not current)
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _toggle_auto_delete(self, interaction: discord.Interaction):
        settings = get_channel_settings(self.channel_id) or {}
        current = bool(settings.get("auto_delete_screenshots", True))
        set_auto_delete_screenshots(self.channel_id, not current)
        self._build_components()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _open_keywords(self, interaction: discord.Interaction):
        view = _KeywordsView(
            self.cog, self.user_id, self.guild_id,
            channel_id=self.channel_id, alliance_id=self.alliance_id,
            parent=self,
        )
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    async def _remove(self, interaction: discord.Interaction):
        # Best-effort: also delete the channel's info message so removing the
        # config doesn't leave an orphaned pinned post behind.
        settings = get_channel_settings(self.channel_id)
        if settings and settings.get("info_message_id"):
            channel = self.cog.bot.get_channel(self.channel_id)
            if channel is not None:
                try:
                    msg = await channel.fetch_message(settings["info_message_id"])
                    await msg.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
        delete_channel_settings(self.channel_id)
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=build_overview_embed(self.user_id, self.guild_id),
            view=self.parent,
        )

    async def _back(self, interaction: discord.Interaction):
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=build_overview_embed(self.user_id, self.guild_id),
            view=self.parent,
        )


class _KeywordsView(discord.ui.View):
    """Per-channel keyword editor. Select an event to edit its classifier keywords."""

    def __init__(self, cog, user_id: int, guild_id: int, channel_id: int,
                 alliance_id: int, parent: "OCRChannelEditView"):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.alliance_id = alliance_id
        self.parent = parent
        self._build_components()

    def build_embed(self) -> discord.Embed:
        keywords = get_channel_keywords(self.channel_id)
        lines = [
            f"{theme.upperDivider}",
            f"**Channel:** <#{self.channel_id}>",
            "",
            "Keywords decide which event a screenshot belongs to. The bot",
            "OCR's the first uploaded image and matches these against the",
            "result, case-insensitively.",
            "",
            "**Current keywords:**",
        ]
        for et in EVENT_TYPES:
            cfg = EVENT_TYPES[et]
            if et in keywords:
                kw_text = ", ".join(keywords[et])
            else:
                kw_text = "_(disabled)_"
            lines.append(f"  • **{cfg.label}** — {kw_text}")
        lines.append(f"{theme.lowerDivider}")
        return discord.Embed(
            title=f"{theme.editListIcon} Detection Keywords",
            description="\n".join(lines),
            color=theme.emColor1,
        )

    def _build_components(self):
        self.clear_items()
        keywords = get_channel_keywords(self.channel_id)
        options = [
            discord.SelectOption(
                label=EVENT_TYPES[et].label[:100],
                value=et,
                description=", ".join(keywords.get(et, EVENT_TYPES[et].default_keywords))[:100],
            )
            for et in EVENT_TYPES
        ]
        select = discord.ui.Select(
            placeholder="Pick an event to edit its keywords…",
            options=options, row=0,
        )
        select.callback = self._open_modal
        self.add_item(select)

        reset_btn = discord.ui.Button(
            label="Clear All Keywords", emoji=theme.trashIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        reset_btn.callback = self._reset_all
        self.add_item(reset_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._back
        self.add_item(back_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only the user who opened this menu can use it.",
                ephemeral=True,
            )
            return False
        return True

    async def _open_modal(self, interaction: discord.Interaction):
        et = interaction.data["values"][0]
        current = get_channel_keywords(self.channel_id).get(
            et, list(EVENT_TYPES[et].default_keywords)
        )
        await interaction.response.send_modal(
            _KeywordsModal(self, event_type=et, current_keywords=current)
        )

    async def _reset_all(self, interaction: discord.Interaction):
        # Clear every keyword for this channel. Events stay enabled and fall
        # back to fingerprint-only detection.
        with sqlite3.connect(_SETTINGS_DB, timeout=30.0) as conn:
            conn.execute(
                "DELETE FROM ocr_channel_event_keywords WHERE channel_id = ?",
                (self.channel_id,),
            )
            conn.commit()
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _back(self, interaction: discord.Interaction):
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=self.parent.build_embed(), view=self.parent,
        )

    async def reload(self, interaction: discord.Interaction):
        """Re-render after a modal save."""
        self._build_components()
        await self.cog.refresh_info_message(self.channel_id)
        if interaction.response.is_done():
            await interaction.edit_original_response(embed=self.build_embed(), view=self)
        else:
            await interaction.response.edit_message(embed=self.build_embed(), view=self)


class _KeywordsModal(discord.ui.Modal):
    """Edit the classifier keywords for one event type. One keyword per line."""

    def __init__(self, parent: _KeywordsView, *, event_type: str,
                 current_keywords: list[str]):
        cfg = EVENT_TYPES[event_type]
        super().__init__(title=f"Keywords — {cfg.label}"[:45])
        self.parent = parent
        self.event_type = event_type

        self.input = discord.ui.TextInput(
            label="Keywords (one per line)",
            style=discord.TextStyle.paragraph,
            default="\n".join(current_keywords),
            placeholder="Foundry Battle\nFoundry",
            required=False,
            max_length=500,
        )
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        keywords = [k.strip() for k in self.input.value.splitlines() if k.strip()]
        # Empty input clears the keyword filter but keeps the event enabled —
        # the fingerprint regex takes over as the sole classifier.
        set_event_keywords(self.parent.channel_id, self.event_type, keywords)
        await self.parent.reload(interaction)
