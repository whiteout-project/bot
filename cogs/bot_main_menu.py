"""
Centralized menu system that handles all main menu logic and routing.
"""

import discord
from discord.ext import commands
import logging
import sqlite3
from .permission_handler import (
    PermissionManager, TIER_OWNER, TIER_GLOBAL, TIER_SERVER, TIER_ALLIANCE, TIER_NONE,
)
from .pimp_my_bot import theme, safe_edit_message, check_interaction_user

logger = logging.getLogger('bot')


def _tier_icon(tier: str) -> str:
    return {
        TIER_OWNER: theme.crownIcon,
        TIER_GLOBAL: theme.medalIcon,
        TIER_SERVER: theme.shieldIcon,
        TIER_ALLIANCE: theme.pinIcon,
    }.get(tier, theme.userIcon)


def _tier_label(tier: str) -> str:
    return {
        TIER_OWNER: "Bot Owner",
        TIER_GLOBAL: "Global Admin",
        TIER_SERVER: "Server Admin",
        TIER_ALLIANCE: "Alliance Admin",
    }.get(tier, "Unknown")


def _tier_blurb(tier: str, alliance_count: int = 0) -> str:
    """Plain-language scope description shown in admin context views."""
    if tier == TIER_OWNER:
        return "Full access everywhere · recovery anchor · cannot be removed except by Transfer Owner."
    if tier == TIER_GLOBAL:
        return "Full access to every alliance on every server, plus admin management."
    if tier == TIER_SERVER:
        return "Manages **ALL** alliances on the Discord server they operate from."
    if tier == TIER_ALLIANCE:
        return f"Limited to **{alliance_count}** specific alliance(s). Adding more or clearing all changes their tier."
    return "No permissions."


_TIER_OPTION_DESC = {
    TIER_GLOBAL: "Full access to every alliance",
    TIER_SERVER: "All alliances on the Discord server they operate from",
    TIER_ALLIANCE: "Limited to specific alliances picked below",
}


def _build_tier_select(staged_tier: str, *, disabled: bool = False,
                      placeholder: str = "Tier", row: int = 0) -> discord.ui.Select:
    """Settable-tier dropdown (Global / Server / Alliance). Owner is never
    selectable here — that's transferred via TransferOwnerView."""
    options = [
        discord.SelectOption(
            label=_tier_label(t), value=t,
            description=_TIER_OPTION_DESC[t],
            emoji=_tier_icon(t),
            default=(staged_tier == t),
        )
        for t in (TIER_GLOBAL, TIER_SERVER, TIER_ALLIANCE)
    ]
    return discord.ui.Select(
        placeholder=placeholder, options=options,
        min_values=1, max_values=1, row=row, disabled=disabled,
    )


def _build_alliance_select(all_alliances, staged_ids,
                          *, max_options: int = 25,
                          placeholder: str = "Pick alliances (any number)…",
                          row: int = 1) -> discord.ui.Select:
    """Multi-select for alliance assignments. Capped at `max_options` because
    Discord doesn't allow more options per select."""
    options = [
        discord.SelectOption(
            label=name[:100], value=str(aid),
            default=(aid in staged_ids),
        )
        for aid, name in all_alliances[:max_options]
    ]
    return discord.ui.Select(
        placeholder=placeholder, options=options,
        min_values=0, max_values=len(options) if options else 1, row=row,
    )


class MainMenu(commands.Cog):
    """Centralized main menu cog for bot navigation."""

    def __init__(self, bot):
        self.bot = bot

    def build_main_menu_embed(self) -> discord.Embed:
        """Single source of truth for the top-level Settings Menu embed."""
        return discord.Embed(
            title=f"{theme.settingsIcon} Settings Menu",
            description=(
                f"Welcome to the bot settings. Select a category to get started:\n\n"
                f"**Menu Categories**\n"
                f"{theme.upperDivider}\n"
                f"{theme.allianceIcon} **Alliances**\n"
                f"└ Manage alliances, members, and registration\n\n"
                f"{theme.giftIcon} **Gift Codes**\n"
                f"└ Manage gift codes and rewards\n\n"
                f"{theme.bellIcon} **Notifications**\n"
                f"└ Event notification system for Bear, KE, and more\n\n"
                f"{theme.listIcon} **Attendance**\n"
                f"└ Track and export event attendance\n\n"
                f"{theme.chartIcon} **Bear Tracking**\n"
                f"└ Track bear hunt damage and view statistics\n\n"
                f"{theme.ministerIcon} **Minister Scheduling**\n"
                f"└ Manage state minister appointments\n\n"
                f"{theme.paletteIcon} **Themes**\n"
                f"└ Customize bot icons and colors\n\n"
                f"{theme.lockIcon} **Permissions**\n"
                f"└ Manage bot administrators (Global Admin only)\n\n"
                f"{theme.robotIcon} **Maintenance**\n"
                f"└ Updates, backups, and support\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )

    async def show_main_menu(self, interaction: discord.Interaction):
        """Display the main settings menu - entry point for all navigation."""
        try:
            embed = self.build_main_menu_embed()
            view = MainMenuView(self)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_main_menu: {e}")
            print(f"Error in show_main_menu: {e}")

    async def show_alliance_management(self, interaction: discord.Interaction):
        """Entry to Alliances — pick an alliance to manage, or run a bulk
        action across alliances (gated by tier)."""
        try:
            tier = PermissionManager.get_tier(interaction.user.id)
            if tier == TIER_NONE:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You don't have admin permissions.",
                    ephemeral=True,
                )
                return

            alliances, _ = PermissionManager.get_admin_alliances(
                interaction.user.id, interaction.guild_id
            )

            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM users WHERE alliance = ?",
                        (alliance_id,),
                    )
                    count = cursor.fetchone()[0] or 0
                alliances_with_counts.append((alliance_id, name, count))

            tier_label = {
                TIER_OWNER: "Bot Owner",
                TIER_GLOBAL: "Global Admin",
                TIER_SERVER: "Server Admin",
                TIER_ALLIANCE: "Alliance Admin",
            }.get(tier, "Admin")

            # Quick stats for the overview section
            visible_ids = [aid for aid, _, _ in alliances_with_counts]
            total_members = sum(count for _, _, count in alliances_with_counts)
            active_syncs = 0
            if visible_ids:
                placeholders = ",".join("?" * len(visible_ids))
                with sqlite3.connect('db/alliance.sqlite') as adb:
                    acur = adb.cursor()
                    acur.execute(
                        f"SELECT COUNT(*) FROM alliancesettings "
                        f"WHERE alliance_id IN ({placeholders}) AND interval > 0",
                        visible_ids,
                    )
                    active_syncs = acur.fetchone()[0] or 0
            servers_visible = (
                len(self.bot.guilds) if tier in (TIER_OWNER, TIER_GLOBAL) else 1
            )

            overview_lines = [
                f"{_tier_icon(tier)} **Access:** `{tier_label}`",
                f"{theme.allianceIcon} **Alliances:** `{len(alliances_with_counts)}`",
                f"{theme.membersIcon} **Total Members:** `{total_members}`",
                f"{theme.refreshIcon} **Active Syncs:** "
                f"`{active_syncs}/{len(alliances_with_counts)}`",
            ]
            if tier in (TIER_OWNER, TIER_GLOBAL):
                overview_lines.insert(
                    1, f"{theme.globeIcon} **Servers:** `{servers_visible}`"
                )

            description = (
                f"**Overview**\n"
                f"{theme.upperDivider}\n"
                + "\n".join(overview_lines) + "\n"
                f"{theme.lowerDivider}\n\n"
                f"Pick an alliance from the dropdown to manage members, history, "
                f"ID channel, activity log, and sync — or use a bulk action below. "
                f"Buttons greyed out are above your permission level.\n\n"
                f"**Bulk Actions**\n"
                f"{theme.upperDivider}\n"
                f"{theme.addIcon} **Add Alliance**\n"
                f"└ Register a new alliance\n\n"
                f"{theme.transferIcon} **Transfer Members**\n"
                f"└ Move members between alliances\n\n"
                f"{theme.exportIcon} **Export Members**\n"
                f"└ Export one alliance or all of them to CSV/TSV\n\n"
                f"{theme.refreshIcon} **Sync All**\n"
                f"└ Refresh all alliance data from the game API\n\n"
                f"{theme.editListIcon} **Self-Registration**\n"
                f"└ Manage the global Self-Registration system\n"
                f"{theme.lowerDivider}"
            )
            if not alliances_with_counts:
                description += (
                    f"\n\n{theme.deniedIcon} No alliances are visible to you yet. "
                    f"Use **Add Alliance** to create one."
                )

            embed = discord.Embed(
                title=f"{theme.allianceIcon} Alliances",
                description=description,
                color=theme.emColor1,
            )

            view = AllianceManagementEntryView(self, alliances_with_counts, tier)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_alliance_management: {e}")
            print(f"Error in show_alliance_management: {e}")

    async def show_alliance_hub(self, interaction: discord.Interaction, alliance_id: int):
        """Per-alliance hub — all per-alliance actions for one alliance."""
        try:
            with sqlite3.connect('db/alliance.sqlite') as db:
                cursor = db.cursor()
                cursor.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id = ?",
                    (alliance_id,),
                )
                row = cursor.fetchone()
            if not row:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance {alliance_id} not found.",
                    ephemeral=True,
                )
                return
            alliance_name = row[0]

            with sqlite3.connect('db/users.sqlite') as db:
                cursor = db.cursor()
                cursor.execute(
                    "SELECT COUNT(*), AVG(furnace_lv), MAX(furnace_lv) "
                    "FROM users WHERE alliance = ?",
                    (alliance_id,),
                )
                count, avg_fl, max_fl = cursor.fetchone()
            count = count or 0

            if count > 0:
                avg_label = self._fc_label(int(avg_fl) if avg_fl else 0)
                max_label = self._fc_label(int(max_fl) if max_fl else 0)
                stats_line = (
                    f"`{count}` member{'s' if count != 1 else ''} · "
                    f"Highest `{max_label}` · Avg `{avg_label}`"
                )
            else:
                stats_line = "_No members yet — use **Add Members** to get started_"

            tier = PermissionManager.get_tier(interaction.user.id)
            accessible, _ = PermissionManager.get_admin_alliances(
                interaction.user.id, interaction.guild_id
            )
            alliances_with_counts = []
            for aid, name in accessible:
                with sqlite3.connect('db/users.sqlite') as udb:
                    ucur = udb.cursor()
                    ucur.execute(
                        "SELECT COUNT(*) FROM users WHERE alliance = ?", (aid,)
                    )
                    a_count = ucur.fetchone()[0] or 0
                alliances_with_counts.append((aid, name, a_count))

            embed = discord.Embed(
                title=f"{theme.allianceIcon} {alliance_name}",
                description=(
                    f"**Overview**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.fidIcon} **ID:** `{alliance_id}`\n"
                    f"{theme.membersIcon} {stats_line}\n"
                    f"{theme.lowerDivider}\n\n"
                    f"Pick an action below, or use the dropdown to switch "
                    f"to a different alliance.\n\n"
                    f"**Actions**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.membersIcon} **Manage Members**\n"
                    f"└ View, add, transfer, export and remove members\n\n"
                    f"{theme.announceIcon} **Channel Setup**\n"
                    f"└ Configure alliance channels: ID, Sync, Log\n\n"
                    f"{theme.refreshIcon} **Sync Settings**\n"
                    f"└ Sync interval, start time and other options\n\n"
                    f"{theme.editListIcon} **Edit Name**\n"
                    f"└ Rename this alliance\n\n"
                    f"{theme.listIcon} **History**\n"
                    f"└ Furnace level and nickname change history per member\n\n"
                    f"{theme.trashIcon} **Delete Alliance**\n"
                    f"└ Permanently remove this alliance and all related data\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )

            view = AllianceHubView(
                self, alliance_id, alliance_name, tier, alliances_with_counts,
            )
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_alliance_hub: {e}")
            print(f"Error in show_alliance_hub: {e}")


    def _fc_label(self, fl: int) -> str:
        """Render a furnace_lv int as e.g. 'FC 8 - 2'. Falls back to the int."""
        cog = self.bot.get_cog("AllianceMemberOperations")
        if cog and hasattr(cog, 'level_mapping'):
            return cog.level_mapping.get(fl, str(fl))
        return str(fl)

    async def show_self_registration(self, interaction: discord.Interaction):
        """Unified Self-Registration menu — global toggle + ID channel scan settings."""
        try:
            view = SelfRegistrationView(self)
            await view.show(interaction)
        except Exception as e:
            logger.error(f"Error in show_self_registration: {e}")
            print(f"Error in show_self_registration: {e}")

    async def show_permissions(self, interaction: discord.Interaction):
        """Display the Permissions sub-menu (admin management).

        Entry point for the rebuilt admin manager: a paginated list of
        admins with tier badges, plus the claim-owner banner when an
        unowned bot needs a Bot Owner picked.
        """
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)
            if not is_global:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only global administrators can access permissions management.",
                    ephemeral=True,
                )
                return
            view = AdminManagerView(self, interaction.user.id)
            await view.refresh_data(self.bot)
            embed = view.build_embed()
            await safe_edit_message(interaction, embed=embed, view=view, content=None)
        except Exception as e:
            logger.error(f"Error in show_permissions: {e}")
            print(f"Error in show_permissions: {e}")

    async def show_maintenance(self, interaction: discord.Interaction):
        """Display the Maintenance sub-menu."""
        try:
            _, is_global = PermissionManager.is_admin(interaction.user.id)

            embed = discord.Embed(
                title=f"{theme.robotIcon} Maintenance",
                description=(
                    f"Bot maintenance and support options:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.refreshIcon} **Check for Updates**\n"
                    f"└ Check for and install bot updates (Global Admin only)\n\n"
                    f"{theme.archiveIcon} **Backup System**\n"
                    f"└ Create / view / clean local + DM backups (Global Admin only)\n\n"
                    f"{theme.heartIcon} **Bot Health**\n"
                    f"└ API status, DB health, system info, restart, cleanup tools (Global Admin only)\n\n"
                    f"{theme.supportIcon} **Request Support**\n"
                    f"└ Open a support DM with logs attached\n\n"
                    f"{theme.infoIcon} **About Project**\n"
                    f"└ View project info, links, and credits\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = MaintenanceView(self, is_global)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_maintenance: {e}")
            print(f"Error in show_maintenance: {e}")


# ============================================================================
# Main Menu View
# ============================================================================

class MainMenuView(discord.ui.View):
    """Main menu with 8 category buttons."""

    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Alliances",
        emoji=theme.allianceIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_management",
        row=0
    )
    async def alliance_management_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_alliance_management(interaction)

    @discord.ui.button(
        label="Gift Codes",
        emoji=theme.giftIcon,
        style=discord.ButtonStyle.primary,
        custom_id="gift_codes",
        row=0
    )
    async def gift_codes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            gift_cog = self.cog.bot.get_cog("GiftOperations")
            if gift_cog:
                await gift_cog.show_gift_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Gift Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Gift Codes menu: {e}")
            print(f"Error loading Gift Codes menu: {e}")

    @discord.ui.button(
        label="Notifications",
        emoji=theme.bellIcon,
        style=discord.ButtonStyle.primary,
        custom_id="notifications",
        row=0
    )
    async def notifications_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            notification_cog = self.cog.bot.get_cog("NotificationSystem")
            if notification_cog:
                await notification_cog.show_notification_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Notification System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Notifications menu: {e}")
            print(f"Error loading Notifications menu: {e}")

    @discord.ui.button(
        label="Attendance",
        emoji=theme.listIcon,
        style=discord.ButtonStyle.primary,
        custom_id="attendance_tracking",
        row=1
    )
    async def attendance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            attendance_cog = self.cog.bot.get_cog("Attendance")
            if attendance_cog:
                await attendance_cog.show_attendance_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Attendance System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Attendance menu: {e}")
            print(f"Error loading Attendance menu: {e}")

    @discord.ui.button(
        label="Bear Tracking",
        emoji=theme.chartIcon,
        style=discord.ButtonStyle.primary,
        custom_id="bear_tracking",
        row=1
    )
    async def bear_tracking_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            bear_cog = self.cog.bot.get_cog("BearTrack")
            if bear_cog:
                await bear_cog.show_bear_track_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Bear Tracking module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Bear Tracking menu: {e}")
            print(f"Error loading Bear Tracking menu: {e}")

    @discord.ui.button(
        label="Minister Scheduling",
        emoji=theme.ministerIcon,
        style=discord.ButtonStyle.primary,
        custom_id="minister_scheduling",
        row=1
    )
    async def minister_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            minister_cog = self.cog.bot.get_cog("MinisterMenu")
            if minister_cog:
                await minister_cog.show_minister_channel_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Minister Scheduling module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Minister menu: {e}")
            print(f"Error loading Minister menu: {e}")

    @discord.ui.button(
        label="Themes",
        emoji=theme.paletteIcon,
        style=discord.ButtonStyle.primary,
        custom_id="themes",
        row=2
    )
    async def themes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            theme_cog = self.cog.bot.get_cog("Theme")
            if theme_cog:
                await theme_cog.show_theme_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Theme module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Theme menu: {e}")
            print(f"Error loading Theme menu: {e}")

    @discord.ui.button(
        label="Permissions",
        emoji=theme.lockIcon,
        style=discord.ButtonStyle.primary,
        custom_id="permissions_top",
        row=2
    )
    async def permissions_top_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_permissions(interaction)

    @discord.ui.button(
        label="Maintenance",
        emoji=theme.robotIcon,
        style=discord.ButtonStyle.primary,
        custom_id="maintenance",
        row=2
    )
    async def maintenance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_maintenance(interaction)


# ============================================================================
# Alliances View
# ============================================================================

async def _route_to_cog(interaction: discord.Interaction, bot, cog_name: str,
                        method_name: str, *args, fallback_method: str | None = None,
                        missing_label: str | None = None):
    """Try cog.method(*args); fall back to cog.fallback_method(interaction) if
    the primary method doesn't exist yet (used during the staged hub rollout
    so unconverted sub-cogs still work)."""
    cog = bot.get_cog(cog_name)
    if cog is None:
        await interaction.response.send_message(
            f"{theme.deniedIcon} {missing_label or cog_name} module not found.",
            ephemeral=True,
        )
        return
    method = getattr(cog, method_name, None)
    if callable(method):
        await method(interaction, *args)
        return
    if fallback_method:
        fb = getattr(cog, fallback_method, None)
        if callable(fb):
            await fb(interaction)
            return
    await interaction.response.send_message(
        f"{theme.deniedIcon} {missing_label or cog_name} entry point not found.",
        ephemeral=True,
    )


class AllianceManagementEntryView(discord.ui.View):
    """Entry view: pick an alliance to manage, or run a bulk action across
    alliances. Bulk action buttons gate on tier (Owner/Global/Server can use
    them all; Alliance tier can only use Transfer if they cover 2+ alliances)."""

    def __init__(self, cog, alliances_with_counts, tier: str):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliances = alliances_with_counts  # [(alliance_id, name, count), ...]
        self.tier = tier
        self.page = 0
        self.max_page = max(0, (len(alliances_with_counts) - 1) // 25)
        self._build_select()
        self._apply_permission_gates()

    def _apply_permission_gates(self):
        is_server_or_above = self.tier in (TIER_OWNER, TIER_GLOBAL, TIER_SERVER)
        is_global_or_above = self.tier in (TIER_OWNER, TIER_GLOBAL)
        can_transfer = is_server_or_above or len(self.alliances) >= 2

        gates = {
            "alliance_entry_add": is_server_or_above,
            "alliance_entry_transfer": can_transfer,
            "alliance_entry_export": is_server_or_above,
            "alliance_entry_sync_all": is_server_or_above,
            "alliance_entry_registration": is_global_or_above,
        }
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                allowed = gates.get(getattr(child, "custom_id", None))
                if allowed is False:
                    child.disabled = True

    def _build_select(self):
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        if not self.alliances:
            return

        start = self.page * 25
        end = min(start + 25, len(self.alliances))
        page_items = self.alliances[start:end]

        options = [
            discord.SelectOption(
                label=name[:50],
                value=str(aid),
                description=f"ID: {aid} · {count} member{'s' if count != 1 else ''}"[:100],
                emoji=theme.allianceIcon,
            )
            for aid, name, count in page_items
        ]
        placeholder = f"{theme.allianceIcon} Pick an alliance to manage…"
        if self.max_page > 0:
            placeholder += f" (Page {self.page + 1}/{self.max_page + 1})"
        select = discord.ui.Select(placeholder=placeholder, options=options, row=0)
        select.callback = self._on_select
        self.add_item(select)

        for item in self.children:
            if isinstance(item, discord.ui.Button) and getattr(item, 'custom_id', None) == "alliance_entry_prev":
                item.disabled = self.page == 0
            elif isinstance(item, discord.ui.Button) and getattr(item, 'custom_id', None) == "alliance_entry_next":
                item.disabled = self.page >= self.max_page

    async def _on_select(self, interaction: discord.Interaction):
        select = next(c for c in self.children if isinstance(c, discord.ui.Select))
        alliance_id = int(select.values[0])
        await self.cog.show_alliance_hub(interaction, alliance_id)

    @discord.ui.button(
        emoji=theme.prevIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="alliance_entry_prev",
        row=1,
    )
    async def prev_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = max(0, self.page - 1)
        self._build_select()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(
        emoji=theme.nextIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="alliance_entry_next",
        row=1,
    )
    async def next_page(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.page = min(self.max_page, self.page + 1)
        self._build_select()
        await interaction.response.edit_message(view=self)

    @discord.ui.button(
        label="Add Alliance",
        emoji=theme.addIcon,
        style=discord.ButtonStyle.success,
        custom_id="alliance_entry_add",
        row=2,
    )
    async def add_alliance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "Alliance",
            "show_add_alliance_for",
            fallback_method="show_alliance_operations",
            missing_label="Alliance",
        )

    @discord.ui.button(
        label="Self-Registration",
        emoji=theme.userIcon,
        style=discord.ButtonStyle.success,
        custom_id="alliance_entry_registration",
        row=2,
    )
    async def registration(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_self_registration(interaction)

    @discord.ui.button(
        label="Transfer Members",
        emoji=theme.transferIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_entry_transfer",
        row=2,
    )
    async def transfer_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "AllianceMemberOperations",
            "show_transfer_members",
            fallback_method="handle_member_operations",
            missing_label="Member Management",
        )

    @discord.ui.button(
        label="Export Members",
        emoji=theme.exportIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_entry_export",
        row=3,
    )
    async def export_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "AllianceMemberOperations",
            "show_export_members",
            fallback_method="handle_member_operations",
            missing_label="Member Management",
        )

    @discord.ui.button(
        label="Sync All",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.primary,
        custom_id="alliance_entry_sync_all",
        row=3,
    )
    async def sync_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "Alliance",
            "sync_all_alliances",
            fallback_method="show_alliance_operations",
            missing_label="Alliance",
        )

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="alliance_entry_main_menu",
        row=3,
    )
    async def main_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


class AllianceHubView(discord.ui.View):
    """Per-alliance hub. Lean layout:
      row 0: alliance switch dropdown
      row 1: Manage Members | History
      row 2: Channel Setup | Sync Settings | Edit Name
      row 3: Back | Delete Alliance
    """

    def __init__(self, cog, alliance_id: int, alliance_name: str,
                 tier: str, alliances_with_counts: list):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.tier = tier
        self.alliances = alliances_with_counts  # [(aid, name, count), ...]
        self._build_select()

    def _build_select(self):
        # Drop any existing select first (for rebuilds)
        for item in self.children[:]:
            if isinstance(item, discord.ui.Select):
                self.remove_item(item)

        # Show only when there's something to switch TO
        switchable = [a for a in self.alliances if a[0] != self.alliance_id]
        if not switchable:
            return

        # Discord caps at 25 options; if there are more, show the first 25
        # alphabetically (acceptable simplification — large guilds are rare).
        options = [
            discord.SelectOption(
                label=name[:50],
                value=str(aid),
                description=f"ID: {aid} · {count} member{'s' if count != 1 else ''}"[:100],
                emoji=theme.allianceIcon,
            )
            for aid, name, count in sorted(switchable, key=lambda a: a[1].lower())[:25]
        ]
        select = discord.ui.Select(
            placeholder=f"{theme.refreshIcon} Switch alliance…",
            options=options, row=0,
        )
        select.callback = self._on_switch
        self.add_item(select)

    async def _on_switch(self, interaction: discord.Interaction):
        select = next(c for c in self.children if isinstance(c, discord.ui.Select))
        await self.cog.show_alliance_hub(interaction, int(select.values[0]))

    # ── Primary actions (row 1, green) ──

    @discord.ui.button(label="Manage Members", emoji=theme.membersIcon,
                       style=discord.ButtonStyle.success, row=1)
    async def manage_members(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "AllianceMemberOperations",
            "show_manage_members_for", self.alliance_id,
            fallback_method="handle_member_operations",
            missing_label="Member Management",
        )

    @discord.ui.button(label="Channel Setup", emoji=theme.announceIcon,
                       style=discord.ButtonStyle.success, row=1)
    async def channel_setup(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "AllianceChannels",
            "show_channel_setup_for", self.alliance_id,
            missing_label="Channel Setup",
        )

    # ── Secondary actions (row 2) ──

    @discord.ui.button(label="Sync Settings", emoji=theme.refreshIcon,
                       style=discord.ButtonStyle.primary, row=2)
    async def sync_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "BotOperations",
            "show_control_settings_for", self.alliance_id,
            fallback_method="show_control_settings_menu",
            missing_label="Bot Operations",
        )

    @discord.ui.button(label="Edit Name", emoji=theme.editListIcon,
                       style=discord.ButtonStyle.primary, row=2)
    async def edit_name(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "Alliance",
            "show_edit_name_for", self.alliance_id,
            fallback_method="show_alliance_operations",
            missing_label="Alliance",
        )

    @discord.ui.button(label="History", emoji=theme.listIcon,
                       style=discord.ButtonStyle.secondary, row=2)
    async def history(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "AllianceHistory",
            "show_history_for", self.alliance_id,
            fallback_method="show_alliance_history_menu",
            missing_label="Alliance History",
        )

    # ── Nav (row 3) ──

    @discord.ui.button(label="Back", emoji=theme.backIcon,
                       style=discord.ButtonStyle.secondary, row=3)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_alliance_management(interaction)

    @discord.ui.button(label="Delete Alliance", emoji=theme.trashIcon,
                       style=discord.ButtonStyle.danger, row=3)
    async def delete_alliance(self, interaction: discord.Interaction, button: discord.ui.Button):
        await _route_to_cog(
            interaction, self.cog.bot, "Alliance",
            "show_delete_alliance_for", self.alliance_id,
            fallback_method="show_alliance_operations",
            missing_label="Alliance",
        )


# ============================================================================
# Self-Registration View
# ============================================================================

class SelfRegistrationView(discord.ui.View):
    """Unified Self-Registration menu. Surfaces global state + lets the admin
    toggle the `/register` slash command and tweak how ID channels behave
    (startup scan, scan limit, auto-delete timer)."""

    DEFAULT_SETTINGS = {
        "scan_enabled": 1,
        "scan_limit": 50,
        "delete_after": 10,
        "respond_to_invalid": 0,
    }

    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog

    def _get_settings(self, guild_id):
        id_cog = self.cog.bot.get_cog("AllianceIDChannel")
        if id_cog and guild_id is not None:
            try:
                return id_cog.get_guild_settings(guild_id)
            except Exception:
                pass
        return dict(self.DEFAULT_SETTINGS)

    def _is_register_enabled(self) -> bool:
        register_cog = self.cog.bot.get_cog("AllianceRegistration")
        return bool(register_cog and register_cog.is_registration_enabled())

    def build_embed(self, interaction: discord.Interaction) -> discord.Embed:
        register_enabled = self._is_register_enabled()
        settings = self._get_settings(interaction.guild_id)
        scan_enabled = bool(settings.get("scan_enabled", 1))
        scan_limit = settings.get("scan_limit", 50)
        delete_after = settings.get("delete_after")
        respond_invalid = bool(settings.get("respond_to_invalid", 0))
        delete_text = (
            "Permanent (no auto-delete)" if delete_after is None
            else f"{delete_after} seconds"
        )

        register_status = (
            f"{theme.verifiedIcon} **Self-Registration:** `enabled`"
            if register_enabled
            else f"{theme.deniedIcon} **Self-Registration:** `disabled`"
        )
        scan_status = (
            f"{theme.verifiedIcon} **Startup Scan:** `enabled`"
            if scan_enabled
            else f"{theme.deniedIcon} **Startup Scan:** `disabled`"
        )
        respond_status = (
            f"{theme.verifiedIcon} **Reply to Invalid Posts:** `enabled`"
            if respond_invalid
            else f"{theme.deniedIcon} **Reply to Invalid Posts:** `disabled`"
        )

        return discord.Embed(
            title=f"{theme.userIcon} Self-Registration",
            description=(
                f"Configure how players join their alliance through the bot.\n\n"
                f"**Slash Command**\n"
                f"{theme.upperDivider}\n"
                f"{register_status}\n"
                f"└ Players run `/register <id> <alliance>` to add themselves\n"
                f"└ Doesn't require the bot to read messages in any channel\n"
                f"{theme.lowerDivider}\n\n"
                f"**ID Channel Behavior** _(applies to all configured ID channels)_\n"
                f"{theme.upperDivider}\n"
                f"{scan_status}\n"
                f"└ Catch IDs posted while the bot was offline\n\n"
                f"{respond_status}\n"
                f"└ React + reply when someone posts non-numeric content\n"
                f"└ Disable this to silently ignore chatter in misconfigured channels\n\n"
                f"{theme.listIcon} **Scan Limit:** `{scan_limit}` messages per channel\n"
                f"└ Max messages checked on each scan\n\n"
                f"{theme.editListIcon} **Auto-Delete Replies:** `{delete_text}`\n"
                f"└ How long bot replies (errors, warnings) stay visible\n"
                f"{theme.lowerDivider}\n\n"
                f"_Per-alliance ID channels are configured in_ "
                f"**Alliances → _alliance_ → Channel Setup**."
            ),
            color=theme.emColor1,
        )

    def _add_toggle(self, label_prefix: str, emoji, enabled: bool, callback, row: int):
        btn = discord.ui.Button(
            label=f"{label_prefix}: {'On' if enabled else 'Off'}",
            emoji=emoji,
            style=discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary,
            row=row,
        )
        btn.callback = callback
        self.add_item(btn)

    def _build_components(self, interaction: discord.Interaction):
        self.clear_items()
        register_enabled = self._is_register_enabled()
        settings = self._get_settings(interaction.guild_id)
        scan_enabled = bool(settings.get("scan_enabled", 1))
        respond_invalid = bool(settings.get("respond_to_invalid", 0))

        self._add_toggle("Self-Registration", theme.userIcon, register_enabled,
                         self._toggle_register, row=0)
        self._add_toggle("Startup Scan", theme.refreshIcon, scan_enabled,
                         self._toggle_scan, row=0)
        self._add_toggle("Reply to Invalid", theme.deniedIcon, respond_invalid,
                         self._toggle_respond, row=0)

        scan_limit_btn = discord.ui.Button(
            label="Edit Scan Limit", emoji=theme.listIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        scan_limit_btn.callback = self._edit_scan_limit
        self.add_item(scan_limit_btn)

        delete_btn = discord.ui.Button(
            label="Edit Auto-Delete", emoji=theme.editListIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        delete_btn.callback = self._edit_delete_after
        self.add_item(delete_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        back_btn.callback = self._back
        self.add_item(back_btn)

    async def show(self, interaction: discord.Interaction):
        self._build_components(interaction)
        await safe_edit_message(
            interaction, embed=self.build_embed(interaction), view=self, content=None
        )

    async def _toggle_register(self, interaction: discord.Interaction):
        register_cog = self.cog.bot.get_cog("AllianceRegistration")
        if not register_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Registration module not found.",
                ephemeral=True,
            )
            return
        register_cog.set_registration_enabled(not register_cog.is_registration_enabled())
        await self.show(interaction)

    async def _toggle_scan(self, interaction: discord.Interaction):
        id_cog = self.cog.bot.get_cog("AllianceIDChannel")
        if not id_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID Channel module not found.",
                ephemeral=True,
            )
            return
        settings = self._get_settings(interaction.guild_id)
        new_value = 0 if settings.get("scan_enabled", 1) else 1
        id_cog.ensure_guild_settings(interaction.guild_id)
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cursor = db.cursor()
            cursor.execute(
                "UPDATE id_channel_settings SET scan_enabled = ? WHERE guild_id = ?",
                (new_value, interaction.guild_id),
            )
            db.commit()
        await self.show(interaction)

    async def _toggle_respond(self, interaction: discord.Interaction):
        id_cog = self.cog.bot.get_cog("AllianceIDChannel")
        if not id_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID Channel module not found.",
                ephemeral=True,
            )
            return
        settings = self._get_settings(interaction.guild_id)
        new_value = 0 if settings.get("respond_to_invalid", 0) else 1
        id_cog.ensure_guild_settings(interaction.guild_id)
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cursor = db.cursor()
            cursor.execute(
                "UPDATE id_channel_settings SET respond_to_invalid = ? WHERE guild_id = ?",
                (new_value, interaction.guild_id),
            )
            db.commit()
        await self.show(interaction)

    async def _edit_scan_limit(self, interaction: discord.Interaction):
        from .alliance_id_channel import ScanLimitModal
        id_cog = self.cog.bot.get_cog("AllianceIDChannel")
        if not id_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID Channel module not found.", ephemeral=True
            )
            return
        settings = self._get_settings(interaction.guild_id)
        await interaction.response.send_modal(
            ScanLimitModal(id_cog, settings.get("scan_limit", 50), refresh=self.show)
        )

    async def _edit_delete_after(self, interaction: discord.Interaction):
        from .alliance_id_channel import DeleteAfterModal
        id_cog = self.cog.bot.get_cog("AllianceIDChannel")
        if not id_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} ID Channel module not found.", ephemeral=True
            )
            return
        settings = self._get_settings(interaction.guild_id)
        delete_after = settings.get("delete_after")
        current = delete_after if delete_after is not None else 0
        await interaction.response.send_modal(
            DeleteAfterModal(id_cog, current, refresh=self.show)
        )

    async def _back(self, interaction: discord.Interaction):
        await self.cog.show_alliance_management(interaction)


# ============================================================================
# Permissions View
# ============================================================================

class AdminManagerView(discord.ui.View):
    """Paginated admin list with tier badges, claim-owner banner, and the
    Add-Admin entry point. Selecting an admin opens AdminContextView."""

    PAGE_SIZE = 25  # Discord SelectOption max per dropdown

    def __init__(self, cog, viewer_id: int):
        super().__init__(timeout=7200)
        self.cog = cog
        self.viewer_id = viewer_id
        self.page = 0
        # Populated by refresh_data():
        self.admins: list = []
        self.owner_id = None
        self._names: dict = {}

    async def refresh_data(self, bot):
        """Reload admin list + display names. Call before build_embed/_build."""
        self.admins = PermissionManager.list_admins()
        self.owner_id = PermissionManager.get_owner_id()
        self._names = {}
        for a in self.admins:
            self._names[a['id']] = await _resolve_user_name(bot, a['id'])
        self._build()

    def _total_pages(self) -> int:
        return max(1, -(-len(self.admins) // self.PAGE_SIZE))

    def build_embed(self) -> discord.Embed:
        owner_admin = next((a for a in self.admins if a['is_owner']), None)
        owner_name = self._names.get(owner_admin['id']) if owner_admin else None

        if owner_admin is not None:
            owner_line = f"{theme.crownIcon} `{owner_name}` (`{owner_admin['id']}`)"
        else:
            owner_line = (
                f"{theme.warnIcon} **Not yet claimed.** The first Global admin to "
                f"click **Claim Bot Owner** below becomes the permanent owner."
            )

        # Tier breakdown
        counts = {TIER_OWNER: 0, TIER_GLOBAL: 0, TIER_SERVER: 0, TIER_ALLIANCE: 0}
        for a in self.admins:
            counts[a['tier']] = counts.get(a['tier'], 0) + 1

        embed = discord.Embed(
            title=f"{theme.lockIcon} Permissions",
            description=(
                f"Permissions system allows you to configure access levels for admins. "
                f"Higher tiers can do everything lower tiers can. Use the dropdowns "
                f"below to add a new admin or open an existing one to change their tier, "
                f"scope, or remove them.\n\n"
                f"**Tiers**\n"
                f"{theme.upperDivider}\n"
                f"{theme.crownIcon} **Bot Owner** — recovery anchor; only one; "
                f"changed via Transfer Owner\n"
                f"{theme.medalIcon} **Global Admin** — admin management + every "
                f"alliance on all servers\n"
                f"{theme.shieldIcon} **Server Admin** — manages all alliances "
                f"on their Discord server\n"
                f"{theme.pinIcon} **Alliance Admin** — limited to the specific "
                f"alliance(s) they're assigned to\n"
                f"{theme.lowerDivider}\n\n"
                f"**Current state**\n"
                f"{theme.upperDivider}\n"
                f"**Bot Owner:** {owner_line}\n"
                f"**Total admins:** {len(self.admins)} "
                f"· {counts[TIER_GLOBAL]} Global · {counts[TIER_SERVER]} Server · "
                f"{counts[TIER_ALLIANCE]} Alliance\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )
        if self.admins:
            total_pages = self._total_pages()
            start = self.page * self.PAGE_SIZE
            end = min(start + self.PAGE_SIZE, len(self.admins))
            lines = []
            for a in self.admins[start:end]:
                name = self._names.get(a['id']) or f"User {a['id']}"
                icon = _tier_icon(a['tier'])
                tier_text = _tier_label(a['tier'])
                if a['tier'] == TIER_ALLIANCE:
                    tier_text += f" ({a['alliance_count']})"
                lines.append(f"{icon} **{name}** — {tier_text}")
            header = (
                f"Admins {start + 1}–{end} of {len(self.admins)}"
                if total_pages > 1 else f"Admins ({len(self.admins)})"
            )
            embed.add_field(name=header, value="\n".join(lines)[:1024], inline=False)
        else:
            embed.add_field(
                name="Admins (0)",
                value="*No admins yet. Use the user picker below to add the first one.*",
                inline=False,
            )
        return embed

    def _build(self):
        self.clear_items()

        # Row 0: Claim-Owner banner button (only when no owner exists).
        # Anyone with Global tier can click it; first click wins.
        if self.owner_id is None:
            claim_btn = discord.ui.Button(
                label="Claim Bot Owner",
                emoji=theme.crownIcon,
                style=discord.ButtonStyle.success,
                row=0,
            )
            claim_btn.callback = self._on_claim_owner
            self.add_item(claim_btn)

        # Row 1: Add admin via UserSelect (single user).
        add_select = discord.ui.UserSelect(
            placeholder="+ Add admin (pick a user)…",
            min_values=0, max_values=1, row=1,
        )
        add_select.callback = self._on_add_user_picked
        self.add_item(add_select)

        # Row 2: open-admin Select for the current page.
        if self.admins:
            start = self.page * self.PAGE_SIZE
            end = min(start + self.PAGE_SIZE, len(self.admins))
            options = []
            for a in self.admins[start:end]:
                name = self._names.get(a['id']) or f"User {a['id']}"
                tier = a['tier']
                desc = {
                    TIER_OWNER: "Bot Owner — recovery anchor",
                    TIER_GLOBAL: "Global — full access",
                    TIER_SERVER: "Server — all alliances on the server",
                    TIER_ALLIANCE: f"Alliance — {a['alliance_count']} alliance(s)",
                }.get(tier, tier)
                options.append(discord.SelectOption(
                    label=f"{name}"[:100],
                    value=str(a['id']),
                    description=desc[:100],
                    emoji=_tier_icon(tier),
                ))
            admin_select = discord.ui.Select(
                placeholder="Open an admin…",
                options=options, min_values=1, max_values=1, row=2,
            )
            admin_select.callback = self._on_admin_picked
            self.add_item(admin_select)

        # Row 3: pagination (only when needed).
        total_pages = self._total_pages()
        if total_pages > 1:
            prev_btn = discord.ui.Button(
                label="◀ Prev", style=discord.ButtonStyle.secondary,
                row=3, disabled=(self.page == 0),
            )
            prev_btn.callback = self._on_prev
            page_lbl = discord.ui.Button(
                label=f"Page {self.page + 1}/{total_pages}",
                style=discord.ButtonStyle.secondary,
                row=3, disabled=True,
            )
            next_btn = discord.ui.Button(
                label="Next ▶", style=discord.ButtonStyle.secondary,
                row=3, disabled=(self.page >= total_pages - 1),
            )
            next_btn.callback = self._on_next
            self.add_item(prev_btn)
            self.add_item(page_lbl)
            self.add_item(next_btn)

        # Row 4: Back.
        back_btn = discord.ui.Button(
            label="Back", style=discord.ButtonStyle.secondary,
            emoji=theme.backIcon, row=4,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    # ───── callbacks ─────

    async def _on_claim_owner(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        _, is_global = PermissionManager.is_admin(self.viewer_id)
        if not is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global admins can claim ownership.",
                ephemeral=True,
            )
            return
        PermissionManager.claim_owner(self.viewer_id)
        await self.refresh_data(self.cog.bot)
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_add_user_picked(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        users = interaction.data.get('values') or []
        if not users:
            await interaction.response.defer()
            return
        target_id = int(users[0])
        if any(a['id'] == target_id for a in self.admins):
            await interaction.response.send_message(
                f"{theme.warnIcon} <@{target_id}> is already an admin. Pick them from the list to edit.",
                ephemeral=True,
            )
            return
        view = AddAdminView(self.cog, self.viewer_id, target_id, parent_view=self)
        await view.refresh_data(self.cog.bot)
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    async def _on_admin_picked(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        target_id = int(interaction.data['values'][0])
        view = AdminContextView(self.cog, self.viewer_id, target_id, parent_view=self)
        await view.refresh_data(self.cog.bot)
        await interaction.response.edit_message(embed=view.build_embed(), view=view)

    async def _on_prev(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        if self.page > 0:
            self.page -= 1
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_next(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        if self.page < self._total_pages() - 1:
            self.page += 1
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_back(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await self.cog.show_main_menu(interaction)


async def _resolve_user_name(bot, user_id: int) -> str:
    user = bot.get_user(user_id)
    if user is None:
        try:
            user = await bot.fetch_user(user_id)
        except Exception:
            user = None
    return user.display_name if user else f"User {user_id}"


async def _return_to_parent(interaction, parent_view, bot):
    """Refresh the parent admin-list view and show it on the interaction's
    original message. Shared by every "go back" / "save and exit" callback.
    """
    await parent_view.refresh_data(bot)
    if interaction.response.is_done():
        await interaction.edit_original_response(
            embed=parent_view.build_embed(), view=parent_view,
        )
    else:
        await interaction.response.edit_message(
            embed=parent_view.build_embed(), view=parent_view,
        )


class AdminContextView(discord.ui.View):
    """Per-admin actions: change tier, assign/unassign alliances, remove
    admin. Owner is tier-locked; only the owner sees Transfer Owner."""

    MAX_ALLIANCE_OPTIONS = 25  # Discord SelectOption limit

    def __init__(self, cog, viewer_id: int, target_id: int, parent_view: AdminManagerView):
        super().__init__(timeout=7200)
        self.cog = cog
        self.viewer_id = viewer_id
        self.target_id = target_id
        self.parent_view = parent_view
        # Populated by refresh_data():
        self.target_name = None
        self.target_tier = TIER_NONE
        self.is_target_owner = False
        self.target_alliance_ids: list = []
        self.all_alliances: list = []          # [(id, name), ...]
        self.staged_tier = TIER_GLOBAL          # what the admin will be saved as
        self.staged_alliance_ids: list = []     # diffed against current on Save

    async def refresh_data(self, bot):
        self.target_name = await _resolve_user_name(bot, self.target_id)
        self.target_tier = PermissionManager.get_tier(self.target_id)
        self.is_target_owner = PermissionManager.is_owner(self.target_id)
        self.all_alliances = PermissionManager.list_alliances()
        self.target_alliance_ids = PermissionManager.get_admin_alliance_assignments(self.target_id)
        self.staged_tier = self.target_tier if self.target_tier != TIER_OWNER else TIER_GLOBAL
        self.staged_alliance_ids = list(self.target_alliance_ids)
        self._build()

    def build_embed(self) -> discord.Embed:
        tier = self.target_tier
        icon = _tier_icon(tier)
        embed = discord.Embed(
            title=f"{icon} {self.target_name}",
            description=(
                f"{theme.upperDivider}\n"
                f"**Tier:** {_tier_label(tier)}\n"
                f"**ID:** `{self.target_id}`\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1,
        )
        embed.add_field(
            name="Scope",
            value=_tier_blurb(tier, alliance_count=len(self.target_alliance_ids)),
            inline=False,
        )
        if tier == TIER_ALLIANCE and self.target_alliance_ids:
            id_to_name = {aid: name for aid, name in self.all_alliances}
            assigned = [
                f"`{id_to_name.get(aid, f'#{aid}')}`"
                for aid in self.target_alliance_ids
            ]
            embed.add_field(
                name=f"Assigned Alliances ({len(assigned)})",
                value=", ".join(assigned)[:1024],
                inline=False,
            )
        if self.is_target_owner:
            embed.set_footer(
                text="Bot Owner — tier locked. Use Transfer Owner to hand off to another Global."
            )
        elif tier == TIER_SERVER and self.staged_tier == TIER_ALLIANCE:
            embed.set_footer(
                text="⚠ Selecting alliances below will narrow access from 'all alliances on the server' to just those."
            )
        return embed

    def _build(self):
        self.clear_items()
        tier_select = _build_tier_select(
            self.staged_tier,
            disabled=self.is_target_owner,
            placeholder=("Owner — tier locked" if self.is_target_owner else "Tier"),
        )
        tier_select.callback = self._on_tier_change
        self.add_item(tier_select)

        if self.staged_tier == TIER_ALLIANCE and not self.is_target_owner and self.all_alliances:
            alliance_select = _build_alliance_select(
                self.all_alliances, self.staged_alliance_ids,
                max_options=self.MAX_ALLIANCE_OPTIONS,
            )
            alliance_select.callback = self._on_alliances_change
            self.add_item(alliance_select)

        # Row 2: Save / Remove / Back
        save_btn = discord.ui.Button(
            label="Save", emoji=theme.saveIcon,
            style=discord.ButtonStyle.success, row=2,
            disabled=self.is_target_owner,
        )
        save_btn.callback = self._on_save
        self.add_item(save_btn)

        remove_btn = discord.ui.Button(
            label="Remove Admin", emoji=theme.trashIcon,
            style=discord.ButtonStyle.danger, row=2,
            disabled=self.is_target_owner,
        )
        remove_btn.callback = self._on_remove
        self.add_item(remove_btn)

        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

        # Row 3: Transfer Owner — only the actual current owner can hand off
        # ownership, and only when looking at themselves.
        if (self.is_target_owner and self.viewer_id == self.target_id):
            transfer_btn = discord.ui.Button(
                label="Transfer Owner", emoji=theme.crownIcon,
                style=discord.ButtonStyle.primary, row=3,
            )
            transfer_btn.callback = self._on_transfer_owner
            self.add_item(transfer_btn)

    # ───── callbacks ─────

    async def _on_tier_change(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        self.staged_tier = interaction.data['values'][0]
        # Demoting to Server clears staged alliances (Server has none by definition).
        # Promoting to Alliance keeps current selections to allow editing.
        if self.staged_tier == TIER_SERVER:
            self.staged_alliance_ids = []
        elif self.staged_tier == TIER_GLOBAL:
            self.staged_alliance_ids = []
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_alliances_change(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        self.staged_alliance_ids = [int(v) for v in (interaction.data.get('values') or [])]
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_save(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        if self.is_target_owner:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Owner tier cannot be changed via Save. Use Transfer Owner.",
                ephemeral=True,
            )
            return
        if self.staged_tier == TIER_ALLIANCE and not self.staged_alliance_ids:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance tier needs at least one alliance picked. "
                f"Pick alliances or change tier to Server (manages all alliances).",
                ephemeral=True,
            )
            return
        try:
            PermissionManager.set_tier(
                self.target_id, self.staged_tier,
                alliance_ids=self.staged_alliance_ids if self.staged_tier == TIER_ALLIANCE else None,
            )
        except ValueError as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {e}", ephemeral=True,
            )
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)

    async def _on_remove(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        if self.is_target_owner:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Owner cannot be removed. Use Transfer Owner first.",
                ephemeral=True,
            )
            return
        if self.target_id == self.viewer_id:
            # Allowed — admin removing themselves — but block if it'd leave the bot with no Globals.
            if (self.target_tier in (TIER_OWNER, TIER_GLOBAL)
                    and PermissionManager.count_globals() <= 1):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You're the last Global admin; promote someone "
                    f"else to Global before removing yourself.",
                    ephemeral=True,
                )
                return
        confirm_view = _ConfirmActionView(
            self.viewer_id,
            on_confirm=lambda i: self._do_remove(i),
            on_cancel=lambda i: self._refresh_self(i),
        )
        embed = discord.Embed(
            title=f"{theme.warnIcon} Confirm Remove",
            description=(
                f"Remove **{self.target_name}** (`{self.target_id}`) — currently "
                f"**{_tier_label(self.target_tier)}**?\nThis cannot be undone."
            ),
            color=theme.emColor2,
        )
        await interaction.response.edit_message(embed=embed, view=confirm_view)

    async def _do_remove(self, interaction):
        try:
            PermissionManager.remove_admin(self.target_id)
        except ValueError as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {e}", ephemeral=True,
            )
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)

    async def _refresh_self(self, interaction):
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_back(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)

    async def _on_transfer_owner(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        view = TransferOwnerView(self.cog, self.viewer_id, parent_view=self.parent_view)
        await view.refresh_data(self.cog.bot)
        await interaction.response.edit_message(embed=view.build_embed(), view=view)


class _ConfirmActionView(discord.ui.View):
    """Generic Yes/No confirmation. Yes/No callbacks receive the interaction."""
    def __init__(self, viewer_id, *, on_confirm, on_cancel):
        super().__init__(timeout=120)
        self.viewer_id = viewer_id
        self._on_confirm = on_confirm
        self._on_cancel = on_cancel

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.danger, emoji=theme.verifiedIcon, row=0)
    async def confirm(self, interaction, _btn):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await self._on_confirm(interaction)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji=theme.backIcon, row=0)
    async def cancel(self, interaction, _btn):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await self._on_cancel(interaction)


# ============================================================================
# Add Admin View
# ============================================================================

class AddAdminView(discord.ui.View):
    """Pick tier + (conditional) alliances for a freshly-selected Discord
    user, then Confirm to insert them as an admin in one shot."""

    MAX_ALLIANCE_OPTIONS = 25

    def __init__(self, cog, viewer_id: int, target_id: int, parent_view: AdminManagerView):
        super().__init__(timeout=7200)
        self.cog = cog
        self.viewer_id = viewer_id
        self.target_id = target_id
        self.parent_view = parent_view
        self.target_name = None
        self.all_alliances: list = []
        self.staged_tier = TIER_SERVER  # safest default — server-wide, no specific assignments
        self.staged_alliance_ids: list = []

    async def refresh_data(self, bot):
        self.target_name = await _resolve_user_name(bot, self.target_id)
        self.all_alliances = PermissionManager.list_alliances()
        if PermissionManager.get_owner_id() is None and not PermissionManager.list_admins():
            self.staged_tier = TIER_GLOBAL
        self._build()

    def build_embed(self) -> discord.Embed:
        embed = discord.Embed(
            title=f"{theme.addIcon} Add Admin: {self.target_name}",
            description=(
                f"{theme.upperDivider}\n"
                f"**User:** <@{self.target_id}> (`{self.target_id}`)\n"
                f"**Tier:** {_tier_label(self.staged_tier)} {_tier_icon(self.staged_tier)}\n"
                f"{theme.lowerDivider}\n"
                f"{_tier_blurb(self.staged_tier, alliance_count=len(self.staged_alliance_ids))}"
            ),
            color=theme.emColor1,
        )
        if PermissionManager.get_owner_id() is None and not PermissionManager.list_admins():
            embed.set_footer(text=f"No admins yet — {self.target_name} will become the Bot Owner automatically.")
        return embed

    def _build(self):
        self.clear_items()
        tier_select = _build_tier_select(self.staged_tier)
        tier_select.callback = self._on_tier_change
        self.add_item(tier_select)

        if self.staged_tier == TIER_ALLIANCE and self.all_alliances:
            alliance_select = _build_alliance_select(
                self.all_alliances, self.staged_alliance_ids,
                max_options=self.MAX_ALLIANCE_OPTIONS,
                placeholder="Pick alliances (one or more)…",
            )
            alliance_select.callback = self._on_alliances_change
            self.add_item(alliance_select)

        confirm_btn = discord.ui.Button(
            label="Add Admin", emoji=theme.verifiedIcon,
            style=discord.ButtonStyle.success, row=2,
        )
        confirm_btn.callback = self._on_confirm
        self.add_item(confirm_btn)

        cancel_btn = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        cancel_btn.callback = self._on_cancel
        self.add_item(cancel_btn)

    async def _on_tier_change(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        self.staged_tier = interaction.data['values'][0]
        if self.staged_tier != TIER_ALLIANCE:
            self.staged_alliance_ids = []
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_alliances_change(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        self.staged_alliance_ids = [int(v) for v in (interaction.data.get('values') or [])]
        self._build()
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_confirm(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        if self.staged_tier == TIER_ALLIANCE and not self.staged_alliance_ids:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Pick at least one alliance, or switch tier to Server (manages all).",
                ephemeral=True,
            )
            return
        try:
            PermissionManager.add_admin(
                self.target_id, tier=self.staged_tier,
                alliance_ids=self.staged_alliance_ids if self.staged_tier == TIER_ALLIANCE else None,
            )
        except ValueError as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {e}", ephemeral=True,
            )
            return
        # If they auto-became owner (brand-new install path), the parent
        # list view will show them with the crown — no extra messaging needed.
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)

    async def _on_cancel(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)


# ============================================================================
# Transfer Owner View
# ============================================================================

class TransferOwnerView(discord.ui.View):
    """Current owner picks another Global to receive the owner badge.
    Recipient must already be Global tier."""

    def __init__(self, cog, viewer_id: int, parent_view: AdminManagerView):
        super().__init__(timeout=600)
        self.cog = cog
        self.viewer_id = viewer_id
        self.parent_view = parent_view
        self.candidates: list = []  # [(id, name)]

    async def refresh_data(self, bot):
        admins = PermissionManager.list_admins()
        self.candidates = []
        for a in admins:
            if a['tier'] == TIER_GLOBAL:  # only non-owner Globals are valid recipients
                name = await _resolve_user_name(bot, a['id'])
                self.candidates.append((a['id'], name))
        self._build()

    def build_embed(self) -> discord.Embed:
        if not self.candidates:
            description = (
                f"{theme.warnIcon} **No eligible recipients.**\n\n"
                f"Transfer Owner moves the bot ownership to another Global admin. "
                f"There aren't any other Global admins yet — promote someone to "
                f"Global first (Permissions → pick admin → tier select → Global)."
            )
        else:
            description = (
                f"{theme.upperDivider}\n"
                f"Pick a Global admin to receive the Bot Owner badge. "
                f"This is **immediate and atomic** — you become a regular Global, "
                f"the recipient becomes the Owner.\n"
                f"{theme.lowerDivider}"
            )
        return discord.Embed(
            title=f"{theme.crownIcon} Transfer Bot Owner",
            description=description,
            color=theme.emColor2,
        )

    def _build(self):
        self.clear_items()
        if self.candidates:
            options = [
                discord.SelectOption(
                    label=name[:100], value=str(uid),
                    description=f"User ID {uid}",
                    emoji=_tier_icon(TIER_GLOBAL),
                )
                for uid, name in self.candidates[:25]
            ]
            select = discord.ui.Select(
                placeholder="Pick the new Bot Owner…",
                options=options, min_values=1, max_values=1, row=0,
            )
            select.callback = self._on_pick
            self.add_item(select)
        back_btn = discord.ui.Button(
            label="Back", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=1,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_pick(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        target_id = int(interaction.data['values'][0])
        target_name = next((n for uid, n in self.candidates if uid == target_id), str(target_id))
        confirm_view = _ConfirmActionView(
            self.viewer_id,
            on_confirm=lambda i: self._do_transfer(i, target_id),
            on_cancel=lambda i: self._refresh_self(i),
        )
        embed = discord.Embed(
            title=f"{theme.warnIcon} Confirm Transfer",
            description=(
                f"Transfer Bot Owner to **{target_name}** (`{target_id}`)?\n\n"
                f"You will become a regular Global Admin. They become the recovery anchor."
            ),
            color=theme.emColor2,
        )
        await interaction.response.edit_message(embed=embed, view=confirm_view)

    async def _do_transfer(self, interaction, target_id):
        try:
            PermissionManager.transfer_owner(self.viewer_id, target_id)
        except ValueError as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {e}", ephemeral=True,
            )
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)

    async def _refresh_self(self, interaction):
        await interaction.response.edit_message(embed=self.build_embed(), view=self)

    async def _on_back(self, interaction):
        if not await check_interaction_user(interaction, self.viewer_id):
            return
        await _return_to_parent(interaction, self.parent_view, self.cog.bot)


# ============================================================================
# Maintenance View
# ============================================================================

class MaintenanceView(discord.ui.View):
    """Maintenance sub-menu."""

    def __init__(self, cog, is_global: bool = False):
        super().__init__(timeout=None)
        self.cog = cog
        self.is_global = is_global

        # Disable Global Admin only buttons for non-global admins
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                if child.label in ["Check for Updates", "Backup System", "Bot Health"]:
                    child.disabled = not is_global

    @discord.ui.button(
        label="Check for Updates",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.primary,
        custom_id="check_updates",
        row=0
    )
    async def check_updates_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Handled by bot_operations.py on_interaction listener
        pass

    @discord.ui.button(
        label="Backup System",
        emoji=theme.archiveIcon,
        style=discord.ButtonStyle.primary,
        custom_id="backup_system",
        row=0
    )
    async def backup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            backup_cog = self.cog.bot.get_cog("BackupOperations")
            if backup_cog:
                await backup_cog.show_backup_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Backup System module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Backup System: {e}")
            print(f"Error loading Backup System: {e}")

    @discord.ui.button(
        label="Bot Health",
        emoji=theme.heartIcon,
        style=discord.ButtonStyle.primary,
        custom_id="bot_health",
        row=0
    )
    async def bot_health_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            health_cog = self.cog.bot.get_cog("BotHealth")
            if health_cog:
                await health_cog.show_health_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Bot Health module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Bot Health: {e}")
            print(f"Error loading Bot Health: {e}")

    @discord.ui.button(
        label="Request Support",
        emoji=theme.supportIcon,
        style=discord.ButtonStyle.primary,
        custom_id="request_support",
        row=1
    )
    async def support_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            support_cog = self.cog.bot.get_cog("SupportOperations")
            if support_cog:
                # Skip the intermediate Support Operations submenu — show the
                # support info directly. Gather Logs lives under Bot Health now.
                await support_cog.show_support_info(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Support Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading Support menu: {e}")
            print(f"Error loading Support menu: {e}")

    @discord.ui.button(
        label="About Project",
        emoji=theme.infoIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="about_project",
        row=1
    )
    async def about_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            support_cog = self.cog.bot.get_cog("SupportOperations")
            if support_cog:
                await support_cog.show_about_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Support Operations module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error loading About menu: {e}")
            print(f"Error loading About menu: {e}")

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu_from_maintenance",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


async def setup(bot):
    await bot.add_cog(MainMenu(bot))
