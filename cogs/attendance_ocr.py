"""Attendance OCR cog: routes screenshot uploads in configured channels to event-specific parsers."""
from __future__ import annotations
import logging
from typing import Optional

import discord
from discord.ext import commands

from .pimp_my_bot import theme
from .permission_handler import PermissionManager
from .attendance_ocr_parsers import (
    build_session,
    classify_event,
    OcrUploadSession,
)
from .attendance_ocr_setup import (
    get_channel_settings,
    get_channel_keywords,
    get_enabled_events,
    get_ocr_upload_admin_only,
    set_info_message_id,
    render_info_message,
    build_overview_embed,
    OCRChannelListView,
)

logger = logging.getLogger("alliance")

_IMAGE_EXTS = (".png", ".jpg", ".jpeg", ".webp")


class AttendanceOCR(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.active_sessions: dict[tuple[int, int], OcrUploadSession] = {}

    # Content fingerprints for the info message the current code produces.
    # Only these exact strings count as "ours" — historical wordings aren't
    # included to avoid false-positive deletions of unrelated bot pins.
    _INFO_MSG_FINGERPRINTS = (
        "Upload event screenshots here",                  # populated state heading
        "Screenshot Upload — no event types configured",  # empty-state heading
    )

    def _looks_like_info_message(self, msg: discord.Message) -> bool:
        """True if this is a bot-authored info message from any historical version."""
        if msg.author.id != self.bot.user.id:
            return False
        content = msg.content or ""
        return any(fp in content for fp in self._INFO_MSG_FINGERPRINTS)

    async def _find_bot_info_messages(self, channel: discord.TextChannel) -> list[discord.Message]:
        """Scan pinned messages for bot-authored info messages (current or stale).
        Pinned-only is bounded to <=50 messages per channel and is where every
        version of the info message has lived."""
        try:
            pins = await channel.pins()
        except (discord.Forbidden, discord.HTTPException):
            return []
        return [m for m in pins if self._looks_like_info_message(m)]

    async def refresh_info_message(self, channel_id: int) -> None:
        settings = get_channel_settings(channel_id)
        if settings is None:
            return
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            return

        # Self-heal: find any bot-authored info messages from any historical
        # version sitting pinned in the channel. Includes the one tracked by
        # info_message_id if present, plus any older untracked ones.
        ours = await self._find_bot_info_messages(channel)
        tracked_id = settings.get("info_message_id")
        if tracked_id and not any(m.id == tracked_id for m in ours):
            # The stored ID isn't pinned (or isn't ours anymore) — try fetching
            # it directly in case it exists but is unpinned.
            try:
                tracked_msg = await channel.fetch_message(tracked_id)
                if self._looks_like_info_message(tracked_msg):
                    ours.append(tracked_msg)
            except (discord.NotFound, discord.Forbidden):
                pass

        # If info message is toggled OFF, remove every one we found and clear
        # the stored ID. Includes pre-tracking-era leftovers.
        if not settings["post_info_message"]:
            for m in ours:
                try:
                    await m.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass
            if tracked_id:
                set_info_message_id(channel_id, None)
            return

        content = render_info_message(channel_id)

        # Keep exactly one — prefer the tracked one, else the most recent.
        keep: discord.Message | None = None
        if tracked_id:
            for m in ours:
                if m.id == tracked_id:
                    keep = m
                    break
        if keep is None and ours:
            keep = max(ours, key=lambda m: m.created_at)

        # Delete duplicates (older bot info messages left around)
        for m in ours:
            if keep is None or m.id != keep.id:
                try:
                    await m.delete()
                except (discord.NotFound, discord.Forbidden):
                    pass

        if keep is None:
            # Nothing existing — post fresh.
            try:
                keep = await channel.send(content)
            except discord.Forbidden:
                logger.warning(f"AttendanceOCR: cannot post info message in channel {channel_id}")
                return
            set_info_message_id(channel_id, keep.id)
        else:
            try:
                await keep.edit(content=content)
            except discord.Forbidden:
                return
            if keep.id != tracked_id:
                set_info_message_id(channel_id, keep.id)

        if settings["pin_info_message"]:
            try:
                if not keep.pinned:
                    await keep.pin(reason="Screenshot Upload info message")
            except discord.Forbidden:
                pass
        else:
            try:
                if keep.pinned:
                    await keep.unpin(reason="Screenshot Upload pin toggled off")
            except discord.Forbidden:
                pass

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None or not message.attachments:
            return

        settings = get_channel_settings(message.channel.id)
        if settings is None:
            return

        images = [a for a in message.attachments if a.filename.lower().endswith(_IMAGE_EXTS)]
        if not images:
            return

        # Per-alliance permission gate. When restricted, only bot admins
        # can post screenshots for processing; others get a self-deleting notice.
        if get_ocr_upload_admin_only(settings["alliance_id"]):
            is_admin, _ = PermissionManager.is_admin(message.author.id)
            if not is_admin:
                await message.channel.send(
                    f"{theme.deniedIcon} Only admins can upload screenshots here.",
                    delete_after=20,
                )
                return

        key = (message.channel.id, message.author.id)
        session = self.active_sessions.get(key)
        if session is not None and not session.finalized and not session.cancelled:
            await session.add_attachments(images)
            await self._maybe_delete_source(message, settings)
            return

        classification, ocr_text = await self._classify_first_image(
            message.channel.id, images[0]
        )
        event_type = classification[0] if classification else None
        if event_type is None:
            # Log the OCR text snippet — invaluable when an admin reports
            # "the bot can't identify my screenshot" and we need to see
            # what RapidOCR actually produced.
            preview = (ocr_text or "<empty>")[:300].replace("\n", " ")
            logger.info(
                f"AttendanceOCR: classification failed in channel {message.channel.id}. "
                f"OCR preview: {preview!r}"
            )
            enabled = get_enabled_events(message.channel.id)
            from .attendance_ocr_parsers import EVENT_TYPES
            enabled_labels = ", ".join(
                EVENT_TYPES[et].label for et in enabled if et in EVENT_TYPES
            ) or "(none)"
            await message.channel.send(
                f"{theme.warnIcon} Couldn't identify the event in that screenshot.\n"
                f"This channel accepts: **{enabled_labels}**\n"
                f"If your screenshot is for one of these, the bot's text "
                f"recognition may have misread it — try a clearer screenshot, "
                f"or ask an admin to add the missing event type if your "
                f"screenshot is for something else.",
                delete_after=30,
            )
            return

        new_session = build_session(
            event_type,
            cog=self, channel=message.channel,
            uploader=message.author, alliance_id=settings["alliance_id"],
        )
        if new_session is None:
            await message.channel.send(
                f"{theme.warnIcon} Parser for `{event_type}` not implemented yet.",
                delete_after=20,
            )
            return

        self.active_sessions[key] = new_session
        await new_session.start(images)
        await self._maybe_delete_source(message, settings)

    async def _maybe_delete_source(self, message: discord.Message, settings: dict) -> None:
        """Delete the user's upload message after its attachments have been
        processed, when the channel has auto_delete_screenshots enabled."""
        if not settings.get("auto_delete_screenshots", True):
            return
        try:
            await message.delete()
        except (discord.Forbidden, discord.NotFound, discord.HTTPException):
            # Forbidden = bot lacks Manage Messages; nothing actionable.
            pass

    async def _classify_first_image(
        self, channel_id: int, attachment: discord.Attachment
    ) -> tuple[Optional[tuple[str, str]], str]:
        """Fingerprint-first event classification.

        For each event_type enabled on this channel, the configured keywords
        (if any) act as an optional prefilter. The per-kind fingerprint regex
        decides — it's specific enough to tell registration apart from
        results within the same event family without keyword help.

        Returns `((event_type, kind) or None, ocr_text)` so the caller can
        log the OCR'd text when classification fails.
        """
        try:
            from . import bear_track
            data = await attachment.read()
            text = await bear_track.ocr_bytes(data, lang=bear_track.DEFAULT_OCR_LANG)
        except Exception:
            logger.exception("AttendanceOCR: classify-step OCR failed")
            return None, ""

        classification = classify_event(
            text,
            enabled_events=get_enabled_events(channel_id),
            keywords_by_event=get_channel_keywords(channel_id),
        )
        return classification, text

    def end_session(self, channel_id: int, uploader_id: int) -> None:
        self.active_sessions.pop((channel_id, uploader_id), None)

    async def show_channel_setup_menu(self, interaction: discord.Interaction) -> None:
        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Access Denied",
                    description="You do not have permission to configure Screenshot Upload channels.",
                    color=theme.emColor4,
                ),
                view=None,
            )
            return
        embed = build_overview_embed(interaction.user.id, interaction.guild.id)
        view = OCRChannelListView(self, interaction.user.id, interaction.guild.id)
        await interaction.response.edit_message(embed=embed, view=view)


async def setup(bot):
    await bot.add_cog(AttendanceOCR(bot))
