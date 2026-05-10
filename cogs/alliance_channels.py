"""Per-alliance channel setup. Consolidates ID Channel, Activity Log, and Sync
Log into a single view so admins configure all three in one place."""

import discord
from discord.ext import commands
import sqlite3
import logging
from datetime import datetime
from .pimp_my_bot import theme, safe_edit_message

logger = logging.getLogger('alliance')


class AllianceChannels(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def show_channel_setup_for(self, interaction: discord.Interaction, alliance_id: int):
        """Open Channel Setup for a specific alliance (called from the Hub)."""
        with sqlite3.connect('db/alliance.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
            )
            row = cur.fetchone()
        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance not found.", ephemeral=True
            )
            return

        view = ChannelSetupView(alliance_id, row[0], self)
        await safe_edit_message(
            interaction, embed=view.build_embed(interaction.guild),
            view=view, content=None,
        )


class ChannelSetupView(discord.ui.View):
    """Three-channel control surface. Each row: Set + Clear. Bottom: Back."""

    KINDS = {
        "id":   ("ID Channel",   "fidIcon",      "_set_id_channel",  "_clear_id_channel"),
        "log":  ("Activity Log", "documentIcon", "_set_activity_log", "_clear_activity_log"),
        "sync": ("Sync Log",     "refreshIcon",  "_set_sync_log",     "_clear_sync_log"),
    }

    def __init__(self, alliance_id: int, alliance_name: str, cog):
        super().__init__(timeout=7200)
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.cog = cog
        self._build_components()

    # ── DB readers ─────────────────────────────────────────────────────

    def _get_id_channel(self):
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "SELECT channel_id FROM id_channels WHERE alliance_id = ? LIMIT 1",
                (self.alliance_id,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _get_activity_log_channel(self):
        with sqlite3.connect('db/settings.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "SELECT channel_id FROM alliance_logs WHERE alliance_id = ?",
                (self.alliance_id,),
            )
            row = cur.fetchone()
        return row[0] if row else None

    def _get_sync_log_channel(self):
        with sqlite3.connect('db/alliance.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "SELECT channel_id FROM alliancesettings WHERE alliance_id = ?",
                (self.alliance_id,),
            )
            row = cur.fetchone()
        return row[0] if row and row[0] else None

    # ── DB writers ─────────────────────────────────────────────────────

    def _set_id_channel(self, guild_id: int, channel_id: int, user_id: int):
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "SELECT alliance_id FROM id_channels "
                "WHERE guild_id = ? AND channel_id = ? AND alliance_id != ?",
                (guild_id, channel_id, self.alliance_id),
            )
            conflict = cur.fetchone()
            if conflict:
                other_alliance_id = conflict[0]
                with sqlite3.connect('db/alliance.sqlite') as a_db:
                    a_cur = a_db.cursor()
                    a_cur.execute(
                        "SELECT name FROM alliance_list WHERE alliance_id = ?",
                        (other_alliance_id,),
                    )
                    row = a_cur.fetchone()
                other_name = row[0] if row else f"alliance {other_alliance_id}"
                raise ValueError(
                    f"This channel is already the ID channel for `{other_name}`. "
                    f"Remove it there first or pick a different channel."
                )

            cur.execute(
                "DELETE FROM id_channels WHERE guild_id = ? AND alliance_id = ?",
                (guild_id, self.alliance_id),
            )
            cur.execute(
                "INSERT INTO id_channels "
                "(guild_id, alliance_id, channel_id, created_at, created_by) "
                "VALUES (?, ?, ?, ?, ?)",
                (guild_id, self.alliance_id, channel_id,
                 datetime.now().isoformat(), user_id),
            )
            db.commit()

    def _clear_id_channel(self, guild_id: int):
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "DELETE FROM id_channels WHERE guild_id = ? AND alliance_id = ?",
                (guild_id, self.alliance_id),
            )
            db.commit()

    def _set_activity_log(self, channel_id: int, **_):
        with sqlite3.connect('db/settings.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "INSERT INTO alliance_logs (alliance_id, channel_id) VALUES (?, ?) "
                "ON CONFLICT(alliance_id) DO UPDATE SET channel_id = excluded.channel_id",
                (self.alliance_id, channel_id),
            )
            db.commit()

    def _clear_activity_log(self, **_):
        with sqlite3.connect('db/settings.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "DELETE FROM alliance_logs WHERE alliance_id = ?", (self.alliance_id,)
            )
            db.commit()

    def _set_sync_log(self, channel_id: int, **_):
        with sqlite3.connect('db/alliance.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "INSERT INTO alliancesettings (alliance_id, channel_id) VALUES (?, ?) "
                "ON CONFLICT(alliance_id) DO UPDATE SET channel_id = excluded.channel_id",
                (self.alliance_id, channel_id),
            )
            db.commit()

    def _clear_sync_log(self, **_):
        with sqlite3.connect('db/alliance.sqlite') as db:
            cur = db.cursor()
            cur.execute(
                "UPDATE alliancesettings SET channel_id = NULL WHERE alliance_id = ?",
                (self.alliance_id,),
            )
            db.commit()

    # ── Embed / build ─────────────────────────────────────────────────

    def build_embed(self, guild) -> discord.Embed:
        def fmt(channel_id):
            return f"<#{channel_id}>" if channel_id else "_(none)_"

        return discord.Embed(
            title=f"{theme.allianceIcon} {self.alliance_name} — Channel Setup",
            description=(
                f"Configure the three per-alliance channels in one place.\n\n"
                f"**Channels**\n"
                f"{theme.upperDivider}\n"
                f"{theme.fidIcon} **ID Channel**\n"
                f"└ {fmt(self._get_id_channel())}\n"
                f"└ Self-registration channel where players post their in-game ID\n\n"
                f"{theme.documentIcon} **Activity Log**\n"
                f"└ {fmt(self._get_activity_log_channel())}\n"
                f"└ Posts member additions, removals, and history events\n\n"
                f"{theme.refreshIcon} **Sync Log**\n"
                f"└ {fmt(self._get_sync_log_channel())}\n"
                f"└ Posts sync status messages from the periodic API sync\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )

    def _build_components(self):
        self.clear_items()
        rows = [
            ("id",   theme.fidIcon,      0),
            ("log",  theme.documentIcon, 1),
            ("sync", theme.refreshIcon,  2),
        ]
        for kind, icon, row in rows:
            label = self.KINDS[kind][0]
            set_btn = discord.ui.Button(
                label=f"Set {label}", emoji=icon,
                style=discord.ButtonStyle.primary, row=row,
            )
            set_btn.callback = self._make_set_callback(kind)
            self.add_item(set_btn)

            clear_btn = discord.ui.Button(
                label="Clear", emoji=theme.deniedIcon,
                style=discord.ButtonStyle.secondary, row=row,
            )
            clear_btn.callback = self._make_clear_callback(kind)
            self.add_item(clear_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=3,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    # ── Button callbacks ──────────────────────────────────────────────

    def _make_set_callback(self, kind: str):
        async def cb(interaction: discord.Interaction):
            channels = interaction.guild.text_channels if interaction.guild else []
            if not channels:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No text channels available.", ephemeral=True
                )
                return
            picker = _ChannelPickerView(channels, kind, self)
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.announceIcon} Pick a channel — "
                          f"{self.KINDS[kind][0]}",
                    description=(
                        f"Select the channel for **{self.alliance_name}**, "
                        f"or click **Cancel** to keep the current setting."
                    ),
                    color=theme.emColor1,
                ),
                view=picker,
            )
        return cb

    def _make_clear_callback(self, kind: str):
        async def cb(interaction: discord.Interaction):
            if kind == "id":
                self._clear_id_channel(interaction.guild_id)
            elif kind == "log":
                self._clear_activity_log()
            else:
                self._clear_sync_log()
            self._build_components()
            await interaction.response.edit_message(
                embed=self.build_embed(interaction.guild), view=self,
            )
        return cb

    async def _on_back(self, interaction: discord.Interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_alliance_hub(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Main Menu module not found.", ephemeral=True
            )

    def apply_pick(self, interaction: discord.Interaction, kind: str, channel_id: int):
        """Called by _ChannelPickerView when the user picks a channel."""
        if kind == "id":
            self._set_id_channel(interaction.guild_id, channel_id, interaction.user.id)
        elif kind == "log":
            self._set_activity_log(channel_id)
        else:
            self._set_sync_log(channel_id)


class _ChannelPickerView(discord.ui.View):
    """In-place paginated channel picker for ChannelSetupView. Re-renders the
    parent view on selection or cancellation."""

    PAGE_SIZE = 25

    def __init__(self, channels, kind: str, parent: ChannelSetupView):
        super().__init__(timeout=7200)
        self.channels = list(channels)
        self.kind = kind
        self.parent = parent
        self.page = 0
        self.total_pages = max(
            1, (len(self.channels) + self.PAGE_SIZE - 1) // self.PAGE_SIZE
        )
        self._build()

    def _build(self):
        self.clear_items()
        start = self.page * self.PAGE_SIZE
        page_channels = self.channels[start:start + self.PAGE_SIZE]

        options = []
        for c in page_channels:
            label = f"#{c.name}"[:100]
            options.append(discord.SelectOption(
                label=label, value=str(c.id),
                description=f"ID: {c.id}",
                emoji=theme.announceIcon,
            ))
        select = discord.ui.Select(
            placeholder=f"Select channel ({self.page + 1}/{self.total_pages})",
            options=options, row=0,
        )
        select.callback = self._on_pick
        self.add_item(select)

        if self.total_pages > 1:
            prev_btn = discord.ui.Button(
                emoji=theme.prevIcon, style=discord.ButtonStyle.secondary,
                disabled=self.page == 0, row=1,
            )
            prev_btn.callback = self._on_prev
            self.add_item(prev_btn)

            next_btn = discord.ui.Button(
                emoji=theme.nextIcon, style=discord.ButtonStyle.secondary,
                disabled=self.page >= self.total_pages - 1, row=1,
            )
            next_btn.callback = self._on_next
            self.add_item(next_btn)

        cancel_btn = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _on_pick(self, interaction: discord.Interaction):
        select = next(c for c in self.children if isinstance(c, discord.ui.Select))
        channel_id = int(select.values[0])
        try:
            self.parent.apply_pick(interaction, self.kind, channel_id)
        except Exception as e:
            logger.error(f"Channel pick error ({self.kind}): {e}")
            print(f"Channel pick error ({self.kind}): {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to save channel: {e}", ephemeral=True
            )
            return
        self.parent._build_components()
        await interaction.response.edit_message(
            embed=self.parent.build_embed(interaction.guild),
            view=self.parent,
        )

    async def _on_prev(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build()
        await interaction.response.edit_message(view=self)

    async def _on_next(self, interaction: discord.Interaction):
        self.page = min(self.total_pages - 1, self.page + 1)
        self._build()
        await interaction.response.edit_message(view=self)

    async def _on_cancel(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            embed=self.parent.build_embed(interaction.guild),
            view=self.parent,
        )


async def setup(bot):
    await bot.add_cog(AllianceChannels(bot))
