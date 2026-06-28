"""
Alliance history cog. Tracks and displays nickname and furnace level changes.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
import re
from .alliance_member_operations import AllianceSelectView
from .permission_handler import PermissionManager
from .pimp_my_bot import theme
from .bot_level_mapping import LEVEL_MAPPING
from . import power_changes

logger = logging.getLogger('alliance')


def _fmt_power(n) -> str:
    if n is None:
        return "N/A"
    n = int(n)
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.1f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    return f"{n:,}"


async def _open_post_to_channel_picker(interaction: discord.Interaction, embed: discord.Embed):
    """Show an ephemeral channel picker. On selection, post `embed` to that
    channel publicly with a 'Requested by ...' footer."""
    if interaction.guild is None:
        await interaction.response.send_message(
            f"{theme.deniedIcon} This action requires a server context.",
            ephemeral=True,
        )
        return
    channels = list(interaction.guild.text_channels)
    if not channels:
        await interaction.response.send_message(
            f"{theme.deniedIcon} No text channels available.", ephemeral=True
        )
        return

    requester = interaction.user
    view = _PostToChannelPickerView(channels, embed, requester)
    await interaction.response.send_message(
        embed=discord.Embed(
            title=f"{theme.announceIcon} Post History to Channel",
            description=(
                f"Pick a channel to post the history embed to. "
                f"It'll be visible to everyone in that channel and tagged with "
                f"_Requested by {requester.mention}_."
            ),
            color=theme.emColor1,
        ),
        view=view,
        ephemeral=True,
    )


class _SingleHistoryResultView(discord.ui.View):
    """Wraps a single-member history embed with Back + Post to Channel."""

    def __init__(self, cog, alliance_id, history_type: str, embed: discord.Embed):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.history_type = history_type  # "furnace", "nickname", "power", or "combat_power"
        self.embed = embed

    @discord.ui.button(label="Back", emoji=f"{theme.backIcon}",
                       style=discord.ButtonStyle.secondary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.alliance_id is None:
            await interaction.response.send_message(
                f"{theme.deniedIcon} No history context to return to.", ephemeral=True
            )
            return
        if self.history_type == "furnace":
            await self.cog.show_member_list_furnace(interaction, self.alliance_id)
        elif self.history_type == "nickname":
            await self.cog.show_member_list_nickname(interaction, self.alliance_id)
        elif self.history_type == "power":
            await self.cog.show_member_list_power(interaction, self.alliance_id)
        elif self.history_type == "combat_power":
            await self.cog.show_member_list_combat_power(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Unknown history type.", ephemeral=True
            )

    @discord.ui.button(label="Post to Channel", emoji=f"{theme.announceIcon}",
                       style=discord.ButtonStyle.primary, row=0)
    async def post_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _open_post_to_channel_picker(interaction, self.embed)


class _PostToChannelPickerView(discord.ui.View):
    """Paginated channel picker for the History 'Post to Channel' button."""

    PAGE_SIZE = 25

    def __init__(self, channels, embed_to_post, requester):
        super().__init__(timeout=7200)
        self.channels = list(channels)
        self.embed_to_post = embed_to_post
        self.requester = requester
        self.page = 0
        self.total_pages = max(
            1, (len(self.channels) + self.PAGE_SIZE - 1) // self.PAGE_SIZE
        )
        self._build()

    def _build(self):
        self.clear_items()
        start = self.page * self.PAGE_SIZE
        page_channels = self.channels[start:start + self.PAGE_SIZE]
        options = [
            discord.SelectOption(
                label=f"#{c.name}"[:100],
                value=str(c.id),
                description=f"ID: {c.id}",
                emoji=theme.announceIcon,
            )
            for c in page_channels
        ]
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
        channel = interaction.guild.get_channel(channel_id) if interaction.guild else None
        if channel is None:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Channel not found",
                    description="The selected channel is no longer accessible.",
                    color=theme.emColor2,
                ),
                view=None,
            )
            return
        try:
            embed_copy = self.embed_to_post.copy()
            embed_copy.set_footer(text=f"Requested by {self.requester.display_name}")
            await channel.send(embed=embed_copy)
        except discord.Forbidden:
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Cannot post in {channel.mention}",
                    description="The bot doesn't have permission to send messages there.",
                    color=theme.emColor2,
                ),
                view=None,
            )
            return
        except Exception as e:
            logger.error(f"Post-to-channel failed: {e}")
            print(f"Post-to-channel failed: {e}")
            await interaction.response.edit_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Failed to post",
                    description=f"Error: {e}",
                    color=theme.emColor2,
                ),
                view=None,
            )
            return
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.verifiedIcon} Posted",
                description=f"History embed sent to {channel.mention}.",
                color=theme.emColor3,
            ),
            view=None,
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
            embed=discord.Embed(
                title=f"{theme.deniedIcon} Cancelled",
                description="Nothing was posted.",
                color=theme.emColor4,
            ),
            view=None,
        )


class AllianceHistory(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.level_mapping = LEVEL_MAPPING

    async def show_furnace_history(self, interaction: discord.Interaction, fid: int):
        try:
            with sqlite3.connect('db/changes.sqlite', timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT old_furnace_lv, new_furnace_lv, change_date
                    FROM furnace_changes
                    WHERE fid = ?
                    ORDER BY change_date DESC
                """, (fid,))
                changes = cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    "No furnace changes found for this player.",
                    ephemeral=True
                )
                return

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute(
                    "SELECT nickname, furnace_lv, alliance FROM users WHERE fid = ?",
                    (fid,),
                )
                user_info = cursor.fetchone()
                nickname = user_info[0] if user_info else "Unknown"
                current_level = user_info[1] if user_info else 0
                alliance_id = user_info[2] if user_info else None

            embed = discord.Embed(
                title=f"{theme.levelIcon} Furnace Level History",
                description=(
                    f"**Player:** `{nickname}`\n"
                    f"**ID:** `{fid}`\n"
                    f"**Current Level:** `{self.level_mapping.get(current_level, str(current_level))}`\n"
                    f"{theme.upperDivider}\n"
                ),
                color=theme.emColor1
            )

            for old_level, new_level, change_date in changes:
                old_level_str = self.level_mapping.get(int(old_level), str(old_level))
                new_level_str = self.level_mapping.get(int(new_level), str(new_level))
                embed.add_field(
                    name=f"Level Change at {change_date}",
                    value=f"{theme.stoveOldIcon} `{old_level_str}` ➜ {theme.stoveIcon} `{new_level_str}`",
                    inline=False
                )

            await interaction.followup.send(
                embed=embed,
                view=_SingleHistoryResultView(self, alliance_id, "furnace", embed),
                ephemeral=True,
            )

        except Exception as e:
            logger.error(f"Error in show_furnace_history: {e}")
            print(f"Error in show_furnace_history: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while displaying the furnace history.",
                ephemeral=True
            )

    async def show_nickname_history(self, interaction: discord.Interaction, fid: int):
        try:
            with sqlite3.connect('db/changes.sqlite', timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT old_nickname, new_nickname, change_date
                    FROM nickname_changes
                    WHERE fid = ?
                    ORDER BY change_date DESC
                """, (fid,))
                changes = cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    "No nickname changes found for this player.",
                    ephemeral=True
                )
                return

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute(
                    "SELECT nickname, furnace_lv, alliance FROM users WHERE fid = ?",
                    (fid,),
                )
                user_info = cursor.fetchone()
                nickname = user_info[0] if user_info else "Unknown"
                current_level = user_info[1] if user_info else 0
                alliance_id = user_info[2] if user_info else None

            embed = discord.Embed(
                title=f"{theme.editListIcon} Nickname History",
                description=(
                    f"**Player:** `{nickname}`\n"
                    f"**ID:** `{fid}`\n"
                    f"**Current Level:** `{self.level_mapping.get(current_level, str(current_level))}`\n"
                    f"{theme.upperDivider}\n"
                ),
                color=theme.emColor1
            )

            for old_name, new_name, change_date in changes:
                embed.add_field(
                    name=f"Nickname Change at {change_date}",
                    value=f"{theme.avatarOldIcon} `{old_name}` ➜ {theme.avatarIcon} `{new_name}`",
                    inline=False
                )

            await interaction.followup.send(
                embed=embed,
                view=_SingleHistoryResultView(self, alliance_id, "nickname", embed),
                ephemeral=True,
            )

        except Exception as e:
            logger.error(f"Error in show_nickname_history: {e}")
            print(f"Error in show_nickname_history: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while displaying the nickname history.",
                ephemeral=True
            )

    async def show_member_list_furnace(self, interaction: discord.Interaction, alliance_id: int):
        """Show the member list for furnace-history selection (skip alliance picker)."""
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute(
                    "SELECT fid, nickname, furnace_lv FROM users "
                    "WHERE alliance = ? ORDER BY furnace_lv DESC, nickname",
                    (alliance_id,),
                )
                members = cursor.fetchall()

            if not members:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No members found in this alliance.",
                    ephemeral=True,
                )
                return

            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id = ?",
                    (alliance_id,),
                )
                alliance_name = cursor.fetchone()[0]

            view = MemberListView(self, members, alliance_name, alliance_id=alliance_id)
            embed = discord.Embed(
                title=f"{theme.levelIcon} {alliance_name} - Member List",
                description=(
                    f"Select a member to view furnace history:\n"
                    f"{theme.upperDivider}\n"
                    f"Total Members: {len(members)}\n"
                    f"Current Page: 1/{view.total_pages}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Error in show_member_list_furnace: {e}")
            print(f"Error in show_member_list_furnace: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing the member list.",
                    ephemeral=True,
                )

    async def show_history_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: pick history type for a known alliance (skip alliance picker)."""
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id = ?",
                    (alliance_id,),
                )
                row = cursor.fetchone()
            alliance_name = row[0] if row else f"Alliance {alliance_id}"

            embed = discord.Embed(
                title=f"{theme.listIcon} {alliance_name} — Member History",
                description=(
                    f"Pick which history to view:\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.stoveIcon} **Furnace Changes** — track FC level changes over time\n"
                    f"{theme.editListIcon} **Nickname Changes** — see when members renamed\n"
                    f"{theme.upIcon} **Power Changes** - track power increase/decrease over time\n"
                    f"{theme.chartIcon} **Combat Power Changes** - track combat power over time\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            view = HistoryTypeView(self, alliance_id)
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Error in show_history_for: {e}")
            print(f"Error in show_history_for: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while opening member history.",
                    ephemeral=True,
                )

    async def show_member_list_nickname(self, interaction: discord.Interaction, alliance_id: int):
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_name = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY furnace_lv DESC, nickname
                """, (alliance_id,))
                members = cursor.fetchall()

            if not members:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No members found in this alliance.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"{theme.editListIcon} {alliance_name} - Member List",
                description=(
                    f"Select a member to view nickname history:\n"
                    f"{theme.upperDivider}\n"
                    f"Total Members: {len(members)}\n"
                    f"Current Page: 1/{(len(members) + 24) // 25}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = MemberListViewNickname(self, members, alliance_name, alliance_id=alliance_id)

            await interaction.response.edit_message(
                embed=embed,
                view=view
            )

        except Exception as e:
            logger.error(f"Error in show_member_list_nickname: {e}")
            print(f"Error in show_member_list_nickname: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while displaying the member list.",
                ephemeral=True
            )

    async def show_power_history(self, interaction: discord.Interaction, fid: int,
                                metric: str = "power"):
        label = "Combat Power" if metric == "combat_power" else "Power"
        col = "combat_power" if metric == "combat_power" else "power"
        try:
            changes = power_changes.history(fid, metric)
            if not changes:
                await interaction.followup.send(
                    f"No {label.lower()} changes found for this player.",
                    ephemeral=True,
                )
                return

            with sqlite3.connect('db/users.sqlite') as users_db:
                info = users_db.execute(
                    f"SELECT nickname, {col}, alliance FROM users WHERE fid = ?",
                    (fid,),
                ).fetchone()
            nickname = info[0] if info else "Unknown"
            current = info[1] if info else None
            alliance_id = info[2] if info else None

            embed = discord.Embed(
                title=f"{theme.chartIcon} {label} History",
                description=(
                    f"**Player:** `{nickname}`\n"
                    f"**ID:** `{fid}`\n"
                    f"**Current {label}:** `{_fmt_power(current)}`\n"
                    f"{theme.upperDivider}\n"
                ),
                color=theme.emColor1,
            )
            for ch in changes:
                badge = power_changes.format_delta(ch["pct"])
                embed.add_field(
                    name=f"Change at {ch['change_date'][:10]}",
                    value=f"`{_fmt_power(ch['old'])}` {theme.forwardIcon} "
                          f"`{_fmt_power(ch['new'])}`  {badge}",
                    inline=False,
                )
            await interaction.followup.send(
                embed=embed,
                view=_SingleHistoryResultView(self, alliance_id, metric, embed),
                ephemeral=True,
            )
        except Exception as e:
            logger.error(f"Error in show_power_history ({metric}): {e}")
            print(f"Error in show_power_history ({metric}): {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while displaying the {label.lower()} history.",
                ephemeral=True,
            )

    async def show_member_list_power(self, interaction: discord.Interaction, alliance_id: int):
        await self._show_member_list_power(interaction, alliance_id, "power")

    async def show_member_list_combat_power(self, interaction: discord.Interaction, alliance_id: int):
        await self._show_member_list_power(interaction, alliance_id, "combat_power")

    async def _show_member_list_power(self, interaction: discord.Interaction,
                                      alliance_id: int, metric: str):
        label = "Combat Power" if metric == "combat_power" else "Power"
        col = "combat_power" if metric == "combat_power" else "power"
        icon = theme.chartIcon
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                members = users_db.execute(
                    f"SELECT fid, nickname, {col} FROM users "
                    f"WHERE alliance = ? ORDER BY {col} DESC, nickname",
                    (alliance_id,),
                ).fetchall()

            if not members:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No members found in this alliance.",
                    ephemeral=True,
                )
                return

            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                row = alliance_db.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id = ?",
                    (alliance_id,),
                ).fetchone()
            alliance_name = row[0] if row else f"Alliance {alliance_id}"

            view = MemberListViewPower(self, members, alliance_name, alliance_id, metric)
            embed = discord.Embed(
                title=f"{icon} {alliance_name} - Member List",
                description=(
                    f"Select a member to view {label.lower()} history:\n"
                    f"{theme.upperDivider}\n"
                    f"Total Members: {len(members)}\n"
                    f"Current Page: 1/{view.total_pages}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Error in _show_member_list_power ({metric}): {e}")
            print(f"Error in _show_member_list_power ({metric}): {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing the member list.",
                    ephemeral=True,
                )

    async def show_recent_changes(self, interaction: discord.Interaction, alliance_name: str, match: re.Match):
        time_multipliers = {"h": 1, "d": 24, "mo": 24 * 30}
        time_dict = {"h": "hour(s)", "d": "day(s)", "mo": "month(s)"}
        hours = int(match.groups()[0]) * time_multipliers[match.groups()[1]]
        human_readable_time = f"{match.groups()[0]} {time_dict[match.groups()[1]]}"
        
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                alliance_id = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname 
                    FROM users 
                    WHERE alliance = ?
                """, (alliance_id,))
                members = {fid: name for fid, name in cursor.fetchall()}

            with sqlite3.connect('db/changes.sqlite', timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT fid, old_furnace_lv, new_furnace_lv, change_date
                    FROM furnace_changes
                    WHERE fid IN ({})
                    AND change_date >= datetime('now', '-{} hours')
                    ORDER BY change_date DESC
                """.format(','.join('?' * len(members)), hours), tuple(members.keys()))
                changes = cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    f"No level changes found in the last {human_readable_time} for {alliance_name}.",
                    ephemeral=True
                )
                return

            chunks = [changes[i:i + 25] for i in range(0, len(changes), 25)]

            view = RecentChangesView(
                chunks, members, self.level_mapping, alliance_name, human_readable_time,
                cog=self, alliance_id=alliance_id,
            )
            await interaction.followup.send(
                embed=view.get_embed(), view=view, ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in show_recent_changes: {e}")
            print(f"Error in show_recent_changes: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing recent changes.",
                ephemeral=True
            )

    async def show_recent_nickname_changes(self, interaction: discord.Interaction, alliance_name: str, match: re.Match):
        time_multipliers = {"h": 1, "d": 24, "mo": 24 * 30}
        time_dict = {"h": "hour(s)", "d": "day(s)", "mo": "month(s)"}
        hours = int(match.groups()[0]) * time_multipliers[match.groups()[1]]
        human_readable_time = f"{match.groups()[0]} {time_dict[match.groups()[1]]}"
        
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                alliance_id = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname 
                    FROM users 
                    WHERE alliance = ?
                """, (alliance_id,))
                members = {fid: name for fid, name in cursor.fetchall()}

            with sqlite3.connect('db/changes.sqlite', timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT fid, old_nickname, new_nickname, change_date
                    FROM nickname_changes
                    WHERE fid IN ({})
                    AND change_date >= datetime('now', '-{} hours')
                    ORDER BY change_date DESC
                """.format(','.join('?' * len(members)), hours), tuple(members.keys()))
                changes = cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    f"No nickname changes found in the last {human_readable_time} for {alliance_name}.",
                    ephemeral=True
                )
                return

            chunks = [changes[i:i + 25] for i in range(0, len(changes), 25)]

            view = RecentNicknameChangesView(
                chunks, members, alliance_name, human_readable_time,
                cog=self, alliance_id=alliance_id,
            )
            await interaction.followup.send(
                embed=view.get_embed(), view=view, ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in show_recent_nickname_changes: {e}")
            print(f"Error in show_recent_nickname_changes: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing recent changes.",
                ephemeral=True
            )

class HistoryTypeView(discord.ui.View):
    """Hub-context history-type picker — alliance is already known."""

    def __init__(self, cog, alliance_id: int):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id

    @discord.ui.button(label="Furnace Changes", emoji=f"{theme.stoveIcon}",
                       style=discord.ButtonStyle.primary, row=0)
    async def furnace(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_member_list_furnace(interaction, self.alliance_id)

    @discord.ui.button(label="Nickname Changes", emoji=f"{theme.editListIcon}",
                       style=discord.ButtonStyle.primary, row=0)
    async def nickname(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_member_list_nickname(interaction, self.alliance_id)

    @discord.ui.button(label="Power Changes", emoji=f"{theme.upIcon}",
                       style=discord.ButtonStyle.primary, row=1)
    async def power(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_member_list_power(interaction, self.alliance_id)

    @discord.ui.button(label="Combat Power Changes", emoji=f"{theme.chartIcon}",
                       style=discord.ButtonStyle.primary, row=1)
    async def combat_power(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_member_list_combat_power(interaction, self.alliance_id)

    @discord.ui.button(label="Back to Hub", emoji=f"{theme.backIcon}",
                       style=discord.ButtonStyle.secondary, row=2)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu and hasattr(main_menu, "show_alliance_hub"):
            await main_menu.show_alliance_hub(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hub not available.", ephemeral=True
            )


class MemberListView(discord.ui.View):
    def __init__(self, cog, members, alliance_name, alliance_id=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.members = members
        self.alliance_name = alliance_name
        self.alliance_id = alliance_id
        self.current_page = 0
        self.total_pages = (len(members) + 24) // 25
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        start_idx = self.current_page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"Select a member (Page {self.current_page + 1}/{self.total_pages})",
            options=[
                discord.SelectOption(
                    label=f"{name}",
                    value=str(fid),
                    description=f"ID: {fid} | Level: {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}"
                ) for fid, name, furnace_lv in current_members
            ],
            row=0
        )

        async def member_callback(interaction):
            try:
                fid = int(select.values[0])
                await interaction.response.defer()
                await self.cog.show_furnace_history(interaction, fid)
            except Exception as e:
                logger.error(f"Error in member_callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while showing furnace history.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while showing furnace history.",
                        ephemeral=True
                    )

        select.callback = member_callback
        self.add_item(select)

        last_day_button = discord.ui.Button(
            label="Last 24 Hours",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_day",
            row=1
        )
        last_day_button.callback = self.last_day_callback
        self.add_item(last_day_button)

        last_week_button = discord.ui.Button(
            label="Last 7 Days",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_week",
            row=1
        )
        last_week_button.callback = self.last_week_callback
        self.add_item(last_week_button)

        custom_time_button = discord.ui.Button(
            label="Custom",
            emoji=f"{theme.settingsIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="custom_time",
            row=1
        )
        custom_time_button.callback = self.custom_time_callback
        self.add_item(custom_time_button)

        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="Previous",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="previous",
                disabled=self.current_page == 0,
                row=2
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="Next",
                emoji=f"{theme.forwardIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next",
                disabled=self.current_page == self.total_pages - 1,
                row=2
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

        search_button = discord.ui.Button(
            label="Search by ID",
            emoji=f"{theme.searchIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="search_fid",
            row=2
        )
        search_button.callback = self.search_callback
        self.add_item(search_button)

        if self.alliance_id is not None:
            back_button = discord.ui.Button(
                label="Back",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                row=3,
            )
            back_button.callback = self._back_callback
            self.add_item(back_button)

    async def _back_callback(self, interaction: discord.Interaction):
        await self.cog.show_history_for(interaction, self.alliance_id)

    async def last_week_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, re.match(r"^(\d+)(h|d|mo)$", "7d"))
        except Exception as e:
            logger.error(f"Error in last_week_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def last_day_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, re.match(r"^(\d+)(h|d|mo)$", "24h"))
        except Exception as e:
            logger.error(f"Error in last_day_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def custom_time_callback(self, interaction: discord.Interaction):
        try:
            modal = CustomTimeModal(self.cog, self.alliance_name)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logger.error(f"Error in custom_time_callback: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing the time input.",
                ephemeral=True
            )

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = max(0, self.current_page - 1)
        await self.update_page(interaction)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await self.update_page(interaction)

    async def search_callback(self, interaction: discord.Interaction):
        modal = FurnaceHistoryIDSearchModal(self.cog)
        await interaction.response.send_modal(modal)

    async def update_page(self, interaction: discord.Interaction):
        self.update_view()
        
        embed = discord.Embed(
            title=f"{theme.levelIcon} {self.alliance_name} - Member List",
            description=(
                f"Select a member to view furnace history:\n"
                f"{theme.upperDivider}\n"
                f"Total Members: {len(self.members)}\n"
                f"Current Page: {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await interaction.response.edit_message(embed=embed, view=self)

class FurnaceHistoryIDSearchModal(discord.ui.Modal, title="Search by ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        self.fid = discord.ui.TextInput(
            label="ID",
            placeholder="Enter ID number...",
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            fid = int(self.fid.value)
            await interaction.response.defer()
            await self.cog.show_furnace_history(interaction, fid)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid ID format. Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Error in FurnaceHistoryIDSearchModal on_submit: {e}")
            print(f"Error in FurnaceHistoryIDSearchModal on_submit: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )

class MemberListViewNickname(discord.ui.View):
    def __init__(self, cog, members, alliance_name, alliance_id=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.members = members
        self.alliance_name = alliance_name
        self.alliance_id = alliance_id
        self.current_page = 0
        self.total_pages = (len(members) + 24) // 25
        self.update_view()

    def update_view(self):
        self.clear_items()

        start_idx = self.current_page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"Select a member (Page {self.current_page + 1}/{self.total_pages})",
            options=[
                discord.SelectOption(
                    label=f"{name}",
                    value=str(fid),
                    description=f"ID: {fid} | Level: {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}"
                ) for fid, name, furnace_lv in current_members
            ],
            row=0
        )

        async def member_callback(interaction):
            try:
                fid = int(select.values[0])
                await interaction.response.defer()
                await self.cog.show_nickname_history(interaction, fid)
            except Exception as e:
                logger.error(f"Error in member_callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while showing nickname history.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while showing nickname history.",
                        ephemeral=True
                    )

        select.callback = member_callback
        self.add_item(select)

        last_day_button = discord.ui.Button(
            label="Last 24 Hours",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_day_nick",
            row=1
        )
        last_day_button.callback = self.last_day_callback
        self.add_item(last_day_button)

        last_week_button = discord.ui.Button(
            label="Last 7 Days",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_week_nick",
            row=1
        )
        last_week_button.callback = self.last_week_callback
        self.add_item(last_week_button)

        custom_time_button = discord.ui.Button(
            label="Custom",
            emoji=f"{theme.settingsIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="custom_time_nick",
            row=1
        )
        custom_time_button.callback = self.custom_time_callback
        self.add_item(custom_time_button)

        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="Previous",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="previous_nick",
                disabled=self.current_page == 0,
                row=2
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="Next",
                emoji=f"{theme.forwardIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next_nick",
                disabled=self.current_page == self.total_pages - 1,
                row=2
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

        search_button = discord.ui.Button(
            label="Search by ID",
            emoji=f"{theme.searchIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="search_fid_nick",
            row=2
        )
        search_button.callback = self.search_callback
        self.add_item(search_button)

        if self.alliance_id is not None:
            back_button = discord.ui.Button(
                label="Back",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                row=3,
            )
            back_button.callback = self._back_callback
            self.add_item(back_button)

    async def _back_callback(self, interaction: discord.Interaction):
        await self.cog.show_history_for(interaction, self.alliance_id)

    async def last_week_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, re.match(r"^(\d+)(h|d|mo)$", "7d"))
        except Exception as e:
            logger.error(f"Error in last_week_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def last_day_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, re.match(r"^(\d+)(h|d|mo)$", "24h"))
        except Exception as e:
            logger.error(f"Error in last_day_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def custom_time_callback(self, interaction: discord.Interaction):
        try:
            modal = CustomTimeModalNickname(self.cog, self.alliance_name)
            await interaction.response.send_modal(modal)
        except Exception as e:
            logger.error(f"Error in custom_time_callback: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing the time input.",
                ephemeral=True
            )

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = max(0, self.current_page - 1)
        await self.update_page(interaction)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await self.update_page(interaction)

    async def search_callback(self, interaction: discord.Interaction):
        modal = NicknameHistoryIDSearchModal(self.cog)
        await interaction.response.send_modal(modal)

    async def update_page(self, interaction: discord.Interaction):
        self.update_view()
        
        embed = discord.Embed(
            title=f"{theme.editListIcon} {self.alliance_name} - Member List",
            description=(
                f"Select a member to view nickname history:\n"
                f"{theme.upperDivider}\n"
                f"Total Members: {len(self.members)}\n"
                f"Current Page: {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await interaction.response.edit_message(embed=embed, view=self)

class NicknameHistoryIDSearchModal(discord.ui.Modal, title="Search by ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        self.fid = discord.ui.TextInput(
            label="ID",
            placeholder="Enter ID number...",
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            fid = int(self.fid.value)
            await interaction.response.defer()
            await self.cog.show_nickname_history(interaction, fid)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid ID format. Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Error in NicknameHistoryIDSearchModal on_submit: {e}")
            print(f"Error in NicknameHistoryIDSearchModal on_submit: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )

class MemberListViewPower(discord.ui.View):
    def __init__(self, cog, members, alliance_name, alliance_id, metric):
        super().__init__(timeout=7200)
        self.cog = cog
        self.members = members
        self.alliance_name = alliance_name
        self.alliance_id = alliance_id
        self.metric = metric
        self.current_page = 0
        self.total_pages = (len(members) + 24) // 25
        self.update_view()

    def update_view(self):
        self.clear_items()

        label = "Combat Power" if self.metric == "combat_power" else "Power"
        start_idx = self.current_page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"Select a member (Page {self.current_page + 1}/{self.total_pages})",
            options=[
                discord.SelectOption(
                    label=f"{name}",
                    value=str(fid),
                    description=f"ID: {fid} | {label}: {_fmt_power(val)}"
                ) for fid, name, val in current_members
            ],
            row=0,
        )

        metric = self.metric

        async def member_callback(interaction):
            try:
                fid = int(select.values[0])
                await interaction.response.defer()
                await self.cog.show_power_history(interaction, fid, metric)
            except Exception as e:
                logger.error(f"Error in MemberListViewPower member_callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while showing {label.lower()} history.",
                        ephemeral=True,
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while showing {label.lower()} history.",
                        ephemeral=True,
                    )

        select.callback = member_callback
        self.add_item(select)

        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="Previous",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="previous_power",
                disabled=self.current_page == 0,
                row=1,
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="Next",
                emoji=f"{theme.forwardIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next_power",
                disabled=self.current_page == self.total_pages - 1,
                row=1,
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

        if self.alliance_id is not None:
            back_button = discord.ui.Button(
                label="Back",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                row=2,
            )
            back_button.callback = self._back_callback
            self.add_item(back_button)

    async def _back_callback(self, interaction: discord.Interaction):
        await self.cog.show_history_for(interaction, self.alliance_id)

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = max(0, self.current_page - 1)
        await self.update_page(interaction)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await self.update_page(interaction)

    async def update_page(self, interaction: discord.Interaction):
        self.update_view()
        label = "Combat Power" if self.metric == "combat_power" else "Power"
        icon = theme.chartIcon
        embed = discord.Embed(
            title=f"{icon} {self.alliance_name} - Member List",
            description=(
                f"Select a member to view {label.lower()} history:\n"
                f"{theme.upperDivider}\n"
                f"Total Members: {len(self.members)}\n"
                f"Current Page: {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=embed, view=self)


class CustomTimeModal(discord.ui.Modal, title="Custom Time Range"):
    def __init__(self, cog, alliance_name):
        super().__init__()
        self.cog = cog
        self.alliance_name = alliance_name
        self.time_frame = discord.ui.TextInput(
            label="Time Frame",
            placeholder="eg. 24h, 3d, 2mo",
            required=True,
            min_length=2
        )
        self.add_item(self.time_frame)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            time_frame = self.time_frame.value.strip().lower()
            time_pattern = r"^(\d+)(h|d|mo)$"
            
            match = re.match(time_pattern, time_frame)
            
            if match and int(match.groups()[0]) < 1:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a number 1 or greater.",
                    ephemeral=True
                )
                return
            
            if not match:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid format. Please enter a valid time frame (e.g. 24h, 3d, 2mo).",
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, match)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Error in CustomTimeModal on_submit: {e}")
            print(f"Error in CustomTimeModal on_submit: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing your request.",
                ephemeral=True
            )

class RecentChangesView(discord.ui.View):
    def __init__(self, chunks, members, level_mapping, alliance_name, time,
                 cog=None, alliance_id=None):
        super().__init__(timeout=7200)
        self.chunks = chunks
        self.members = members
        self.level_mapping = level_mapping
        self.alliance_name = alliance_name
        self.time = time
        self.cog = cog
        self.alliance_id = alliance_id
        self.current_page = 0
        self.total_pages = len(chunks)

        # Hide Post button when caller didn't provide context
        if alliance_id is None or cog is None:
            for child in list(self.children):
                if isinstance(child, discord.ui.Button) and getattr(child, "custom_id", None) == "history_post":
                    self.remove_item(child)

        self.update_buttons()

    def get_embed(self):
        embed = discord.Embed(
            title=f"{theme.levelIcon} Recent Level Changes - {self.alliance_name}",
            description=(
                f"Showing changes in the last {self.time}\n"
                f"{theme.upperDivider}\n"
                f"Total Changes: {sum(len(chunk) for chunk in self.chunks)}\n"
                f"Page {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1
        )

        for fid, old_value, new_value, timestamp in self.chunks[self.current_page]:
            old_level = self.level_mapping.get(int(old_value), str(old_value))
            new_level = self.level_mapping.get(int(new_value), str(new_value))
            embed.add_field(
                name=f"{self.members[fid]} (ID: {fid})",
                value=f"{theme.stoveOldIcon} `{old_level}` ➜ {theme.stoveIcon} `{new_level}`\n{theme.timeIcon} {timestamp}",
                inline=False
            )

        if self.total_pages > 1:
            embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")

        return embed

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == self.total_pages - 1

    @discord.ui.button(label="Previous", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary, custom_id="previous")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Next", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Post to Channel", emoji=f"{theme.announceIcon}",
                       style=discord.ButtonStyle.primary, custom_id="history_post", row=1)
    async def post_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _open_post_to_channel_picker(interaction, self.get_embed())


class RecentNicknameChangesView(discord.ui.View):
    def __init__(self, chunks, members, alliance_name, time,
                 cog=None, alliance_id=None):
        super().__init__(timeout=7200)
        self.chunks = chunks
        self.members = members
        self.alliance_name = alliance_name
        self.time = time
        self.cog = cog
        self.alliance_id = alliance_id
        self.current_page = 0
        self.total_pages = len(chunks)

        if alliance_id is None or cog is None:
            for child in list(self.children):
                if isinstance(child, discord.ui.Button) and getattr(child, "custom_id", None) == "history_post_nick":
                    self.remove_item(child)

        self.update_buttons()

    def get_embed(self):
        embed = discord.Embed(
            title=f"{theme.editListIcon} Recent Nickname Changes - {self.alliance_name}",
            description=(
                f"Showing changes in the last {self.time}\n"
                f"{theme.upperDivider}\n"
                f"Total Changes: {sum(len(chunk) for chunk in self.chunks)}\n"
                f"Page {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1
        )

        for fid, old_name, new_name, timestamp in self.chunks[self.current_page]:
            embed.add_field(
                name=f"{self.members[fid]} (ID: {fid})",
                value=f"{theme.avatarOldIcon} `{old_name}` ➜ {theme.avatarIcon} `{new_name}`\n{theme.timeIcon} {timestamp}",
                inline=False
            )

        if self.total_pages > 1:
            embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")

        return embed

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == self.total_pages - 1

    @discord.ui.button(label="Previous", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary, custom_id="previous_nick_recent")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Next", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary, custom_id="next_nick_recent")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Post to Channel", emoji=f"{theme.announceIcon}",
                       style=discord.ButtonStyle.primary,
                       custom_id="history_post_nick", row=1)
    async def post_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _open_post_to_channel_picker(interaction, self.get_embed())


class CustomTimeModalNickname(discord.ui.Modal, title="Custom Time Range"):
    def __init__(self, cog, alliance_name):
        super().__init__()
        self.cog = cog
        self.alliance_name = alliance_name
        self.time_frame = discord.ui.TextInput(
            label="Time Frame",
            placeholder="eg. 24h, 3d, 2mo",
            required=True,
            min_length=2
        )
        self.add_item(self.time_frame)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            time_frame = self.time_frame.value.strip().lower()
            time_pattern = r"^(\d+)(h|d|mo)$"
            
            match = re.match(time_pattern, time_frame)
            
            if match and int(match.groups()[0]) < 1:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a number 1 or greater.",
                    ephemeral=True
                )
                return
            
            if not match:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid format. Please enter a valid time frame (e.g. 24h, 3d, 2mo).",
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, match)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Error in CustomTimeModalNickname on_submit: {e}")
            print(f"Error in CustomTimeModalNickname on_submit: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing your request.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(AllianceHistory(bot)) 