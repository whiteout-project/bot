"""Gift code settings — OCR configuration, test FID management, redemption priority, and auto-redemption setup."""

import discord
import asyncio
import sqlite3
import time
import base64
import logging
import requests
import json
import traceback

from .pimp_my_bot import theme, safe_edit_message, check_interaction_user, notify_view_expired
from .alliance_member_operations import AllianceSelectView
from .permission_handler import PermissionManager

logger = logging.getLogger('gift')


# ---------------------------------------------------------------------------
# Standalone functions (converted from cog methods: self -> cog)
# ---------------------------------------------------------------------------

async def update_test_fid(cog, new_fid, kid=None):
    """Set the test ID and its state (needed for redemption/validation)."""
    try:
        cog.settings_cursor.execute(
            "INSERT INTO test_fid_settings (test_fid, kid) VALUES (?, ?)", (new_fid, kid))
        cog.settings_conn.commit()
        cog.logger.info(f"Test ID updated to {new_fid} (state {kid})")
        return True

    except sqlite3.Error as db_err:
        cog.logger.exception(f"Database error updating test ID: {db_err}")
        return False
    except Exception as e:
        cog.logger.exception(f"Unexpected error updating test ID: {e}")
        return False


def get_test_fid(cog):
    """
    Get the current test ID from the database.

    Returns:
        str: The current test ID, or the default "45379845" if not found
    """
    try:
        cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
        result = cog.settings_cursor.fetchone()
        return result[0] if result else "45379845"
    except Exception as e:
        cog.logger.exception(f"Error getting test ID: {e}")
        return "45379845"


async def get_validation_fid(cog):
    """Get the best available ID for gift code validation.

    Hierarchy:
    1. Configured test ID (if valid)
    2. Random alliance member ID (if no test ID)
    3. default test ID (45379845) as fallback

    Returns:
        tuple: (fid, source) where source is 'test_fid', 'alliance_member', or 'default'
    """
    try:
        # First try: Use configured test ID if it's valid
        test_fid = get_test_fid(cog)

        # Check if test ID is actually configured (not default)
        cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
        result = cog.settings_cursor.fetchone()

        if result and result[0] != "45379845":
            # A configured test ID is used as-is; a bad/kid-less one just yields NO_STATE
            # during validation, which rotates to member FIDs.
            cog.logger.info(f"Using configured test ID for validation: {test_fid}")
            return test_fid, 'test_fid'

        # Second try: Use a random alliance member
        with sqlite3.connect('db/users.sqlite') as users_conn:
            users_cursor = users_conn.cursor()
            users_cursor.execute("""
                SELECT fid, nickname FROM users
                WHERE alliance IS NOT NULL AND alliance != ''
                ORDER BY RANDOM()
                LIMIT 1
            """)
            member = users_cursor.fetchone()

            if member:
                fid, nickname = member
                cog.logger.info(f"Using alliance member ID for validation: {fid} ({nickname})")
                return fid, 'alliance_member'

        # Third try: Fall back to default ID
        cog.logger.info("No alliance members found, using default ID for validation: 45379845")
        return "45379845", 'default'

    except Exception as e:
        cog.logger.exception(f"Error in get_validation_fid: {e}")
        return "45379845", 'default'


def build_solver_status_field(cog):
    """(name, value) embed field: validation-FID mode + redemption stats since startup."""
    test_fid = get_test_fid(cog)
    fid_line = f"`{test_fid}` (fixed)" if test_fid and test_fid != "45379845" else "Rotating alliance members"

    stats = cog.processing_stats
    submissions = stats.get("redemption_submissions", 0)
    s_ok = stats["server_validation_success"]
    s_fail = stats["server_validation_failure"]
    pass_rate = (s_ok / (s_ok + s_fail) * 100) if (s_ok + s_fail) > 0 else 0

    value = (
        f"{theme.fidIcon} **Validation ID:** {fid_line}\n"
        f"{theme.chartIcon} **Redemption Submissions:** `{submissions}`\n"
        f"{theme.verifiedIcon} **Success:** `{s_ok}` \u00b7 {theme.deniedIcon} **Failure:** `{s_fail}`\n"
        f"{theme.chartIcon} **Server Pass Rate:** `{pass_rate:.1f}%`"
    )
    return f"{theme.searchIcon} Redemption (since startup)", value


def clear_test_fid(cog):
    """Remove any configured test FID so validation rotates through random alliance members."""
    try:
        cog.settings_cursor.execute("DELETE FROM test_fid_settings")
        cog.settings_conn.commit()
        return True
    except Exception as e:
        cog.logger.exception(f"Error clearing test ID: {e}")
        return False


async def show_redemption_priority(cog, interaction: discord.Interaction):
    """Show the redemption priority management interface (global admin only)."""
    try:
        # Check global admin permission
        cog.settings_cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
        admin_info = cog.settings_cursor.fetchone()

        if not admin_info or admin_info[0] != 1:
            error_msg = f"{theme.deniedIcon} Only global administrators can manage redemption priority."
            if interaction.response.is_done():
                await interaction.followup.send(error_msg, ephemeral=True)
            else:
                await interaction.response.send_message(error_msg, ephemeral=True)
            return

        # Get all alliances with their priority info
        cog.alliance_cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY alliance_id")
        all_alliances = cog.alliance_cursor.fetchall()

        if not all_alliances:
            error_msg = "No alliances found."
            if interaction.response.is_done():
                await interaction.followup.send(error_msg, ephemeral=True)
            else:
                await interaction.response.send_message(error_msg, ephemeral=True)
            return

        # Get priority info for alliances
        alliance_ids = [a[0] for a in all_alliances]
        placeholders = ','.join('?' * len(alliance_ids))
        cog.cursor.execute(f"""
            SELECT alliance_id, priority FROM giftcodecontrol
            WHERE alliance_id IN ({placeholders})
        """, alliance_ids)
        priority_data = {row[0]: row[1] for row in cog.cursor.fetchall()}

        # Build alliance list with priorities
        alliances_with_priority = []
        for alliance_id, name in all_alliances:
            priority = priority_data.get(alliance_id, 0)
            alliances_with_priority.append((alliance_id, name, priority))

        # Sort by priority, then by alliance_id
        alliances_with_priority.sort(key=lambda x: (x[2], x[0]))

        # Create embed
        embed = discord.Embed(
            title=f"{theme.chartIcon} Redemption Priority",
            description="Configure the order in which alliances receive gift codes.\nSelect an alliance and use the buttons to change its position.",
            color=theme.emColor1
        )

        # Build priority list
        priority_list = []
        for idx, (alliance_id, name, priority) in enumerate(alliances_with_priority, 1):
            priority_list.append(f"`{idx}.` **{name}**")

        embed.add_field(
            name="Current Priority Order",
            value="\n".join(priority_list) if priority_list else "No alliances configured",
            inline=False
        )

        view = RedemptionPriorityView(cog, alliances_with_priority)

        if interaction.response.is_done():
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    except Exception as e:
        cog.logger.exception(f"Error in show_redemption_priority: {e}")
        error_msg = f"An error occurred: {str(e)}"
        if interaction.response.is_done():
            await interaction.followup.send(error_msg, ephemeral=True)
        else:
            await interaction.response.send_message(error_msg, ephemeral=True)


async def setup_giftcode_auto(cog, interaction: discord.Interaction):
    admin_info = await cog.get_admin_info(interaction.user.id)
    if not admin_info:
        await interaction.response.send_message(
            f"{theme.deniedIcon} You are not authorized to perform this action.",
            ephemeral=True
        )
        return

    available_alliances = await cog.get_available_alliances(interaction)
    if not available_alliances:
        await interaction.response.send_message(
            embed=discord.Embed(
                title=f"{theme.deniedIcon} No Available Alliances",
                description="You don't have access to any alliances.",
                color=theme.emColor2
            ),
            ephemeral=True
        )
        return

    cog.cursor.execute("SELECT alliance_id, status FROM giftcodecontrol")
    current_status = dict(cog.cursor.fetchall())

    alliances_with_counts = []
    for alliance_id, name in available_alliances:
        with sqlite3.connect('db/users.sqlite') as users_db:
            cursor = users_db.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
            member_count = cursor.fetchone()[0]
            alliances_with_counts.append((alliance_id, name, member_count))

    auto_gift_embed = discord.Embed(
        title=f"{theme.settingsIcon} Gift Code Settings",
        description=(
            f"Select an alliance to configure automatic redemption:\n\n"
            f"**Alliance List**\n"
            f"{theme.upperDivider}\n"
            f"Select an alliance from the list below:\n"
        ),
        color=theme.emColor1
    )

    view = AllianceSelectView(alliances_with_counts, cog, context="giftcode")

    view.current_select.options.insert(0, discord.SelectOption(
        label="ENABLE ALL ALLIANCES",
        value="enable_all",
        description="Enable automatic redemption for all alliances",
        emoji=f"{theme.verifiedIcon}"
    ))

    view.current_select.options.insert(1, discord.SelectOption(
        label="DISABLE ALL ALLIANCES",
        value="disable_all",
        description="Disable automatic redemption for all alliances",
        emoji=f"{theme.deniedIcon}"
    ))

    async def alliance_callback(select_interaction: discord.Interaction, alliance_id=None):
        try:
            if alliance_id is not None:
                selected_value = str(alliance_id)
            else:
                selected_value = view.current_select.values[0]

            if selected_value in ["enable_all", "disable_all"]:
                status = 1 if selected_value == "enable_all" else 0

                for alliance_id, _, _ in alliances_with_counts:
                    if status == 1:
                        # When enabling, assign next available priority
                        cog.cursor.execute("SELECT COALESCE(MAX(priority), 0) + 1 FROM giftcodecontrol")
                        next_priority = cog.cursor.fetchone()[0]
                        cog.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status, priority)
                            VALUES (?, ?, ?)
                            ON CONFLICT(alliance_id)
                            DO UPDATE SET status = excluded.status,
                                priority = CASE WHEN giftcodecontrol.priority = 0 THEN excluded.priority ELSE giftcodecontrol.priority END
                            """,
                            (alliance_id, status, next_priority)
                        )
                    else:
                        # When disabling, keep existing priority
                        cog.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status)
                            VALUES (?, ?)
                            ON CONFLICT(alliance_id)
                            DO UPDATE SET status = excluded.status
                            """,
                            (alliance_id, status)
                        )
                cog.conn.commit()

                status_text = "enabled" if status == 1 else "disabled"
                success_embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Automatic Redemption Updated",
                    description=(
                        f"**Configuration Details**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.globeIcon} **Scope:** All Alliances\n"
                        f"{theme.chartIcon} **Status:** Automatic redemption {status_text}\n"
                        f"{theme.userIcon} **Updated by:** {select_interaction.user.mention}\n"
                        f"{theme.lowerDivider}\n"
                    ),
                    color=theme.emColor3
                )

                await select_interaction.response.edit_message(
                    embed=success_embed,
                    view=None
                )
                return

            alliance_id = int(selected_value)
            alliance_name = next((name for aid, name in available_alliances if aid == alliance_id), "Unknown")

            current_setting = "enabled" if current_status.get(alliance_id, 0) == 1 else "disabled"

            confirm_embed = discord.Embed(
                title=f"{theme.settingsIcon} Automatic Redemption Configuration",
                description=(
                    f"**Alliance Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                    f"{theme.chartIcon} **Current Status:** Automatic redemption is {current_setting}\n"
                    f"{theme.lowerDivider}\n\n"
                    f"Do you want to enable or disable automatic redemption for this alliance?"
                ),
                color=discord.Color.yellow()
            )

            confirm_view = discord.ui.View()

            async def button_callback(button_interaction: discord.Interaction):
                try:
                    status = 1 if button_interaction.data['custom_id'] == "confirm" else 0

                    if status == 1:
                        # When enabling, assign next available priority
                        cog.cursor.execute("SELECT COALESCE(MAX(priority), 0) + 1 FROM giftcodecontrol")
                        next_priority = cog.cursor.fetchone()[0]
                        cog.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status, priority)
                            VALUES (?, ?, ?)
                            ON CONFLICT(alliance_id)
                            DO UPDATE SET status = excluded.status,
                                priority = CASE WHEN giftcodecontrol.priority = 0 THEN excluded.priority ELSE giftcodecontrol.priority END
                            """,
                            (alliance_id, status, next_priority)
                        )
                    else:
                        # When disabling, keep existing priority
                        cog.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status)
                            VALUES (?, ?)
                            ON CONFLICT(alliance_id)
                            DO UPDATE SET status = excluded.status
                            """,
                            (alliance_id, status)
                        )
                    cog.conn.commit()

                    status_text = "enabled" if status == 1 else "disabled"
                    success_embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Automatic Redemption Updated",
                        description=(
                            f"**Configuration Details**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                            f"{theme.chartIcon} **Status:** Automatic redemption {status_text}\n"
                            f"{theme.userIcon} **Updated by:** {button_interaction.user.mention}\n"
                            f"{theme.lowerDivider}\n"
                        ),
                        color=theme.emColor3
                    )

                    await button_interaction.response.edit_message(
                        embed=success_embed,
                        view=None
                    )

                except Exception as e:
                    cog.logger.exception(f"Button callback error: {str(e)}")
                    if not button_interaction.response.is_done():
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} An error occurred while updating the settings.",
                            ephemeral=True
                        )
                    else:
                        await button_interaction.followup.send(
                            f"{theme.deniedIcon} An error occurred while updating the settings.",
                            ephemeral=True
                        )

            confirm_button = discord.ui.Button(
                label="Enable",
                emoji=f"{theme.verifiedIcon}",
                style=discord.ButtonStyle.success,
                custom_id="confirm"
            )
            confirm_button.callback = button_callback

            deny_button = discord.ui.Button(
                label="Disable",
                emoji=f"{theme.deniedIcon}",
                style=discord.ButtonStyle.danger,
                custom_id="deny"
            )
            deny_button.callback = button_callback

            confirm_view.add_item(confirm_button)
            confirm_view.add_item(deny_button)

            if not select_interaction.response.is_done():
                await select_interaction.response.edit_message(
                    embed=confirm_embed,
                    view=confirm_view
                )
            else:
                await select_interaction.message.edit(
                    embed=confirm_embed,
                    view=confirm_view
                )

        except Exception as e:
            cog.logger.exception(f"Error in alliance selection: {e}")
            if not select_interaction.response.is_done():
                await select_interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while processing your selection.",
                    ephemeral=True
                )
            else:
                await select_interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while processing your selection.",
                    ephemeral=True
                )

    view.callback = alliance_callback

    await interaction.response.send_message(
        embed=auto_gift_embed,
        view=view,
        ephemeral=True
    )


# ---------------------------------------------------------------------------
# View and Modal classes (kept as-is, they use self.cog internally)
# ---------------------------------------------------------------------------

class TestIDModal(discord.ui.Modal, title="Set Test ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

        try:
            self.cog.settings_cursor.execute("SELECT test_fid, kid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.cog.settings_cursor.fetchone()
            current_fid = result[0] if result and result[0] != "45379845" else ""
            current_kid = str(result[1]) if result and result[0] != "45379845" and result[1] is not None else ""
        except Exception:
            current_fid = current_kid = ""

        self.test_fid = discord.ui.TextInput(
            label="Player ID (blank = rotate members)",
            placeholder="Leave blank to validate with random alliance members",
            default=current_fid,
            required=False,
            max_length=20
        )
        self.add_item(self.test_fid)
        self.test_kid = discord.ui.TextInput(
            label="State # (required when an ID is set)",
            placeholder="The state this player ID is in",
            default=current_kid,
            required=False,
            max_length=10
        )
        self.add_item(self.test_kid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            raw = self.test_fid.value.strip()
            kid_raw = self.test_kid.value.strip()
            if not raw:
                self.cog.clear_test_fid()
                self.cog.logger.info(f"Test FID cleared by {interaction.user.id}; validation will rotate alliance members.")
            elif not raw.isdigit():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid ID. Enter a numeric player ID, or leave it blank to rotate alliance members.",
                    ephemeral=True)
                return
            elif not kid_raw.isdigit():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} A test ID needs its State number so it can redeem. Enter the state, or clear the ID to rotate members.",
                    ephemeral=True)
                return
            else:
                await self.cog.update_test_fid(raw, int(kid_raw))
                self.cog.logger.info(f"Test FID set to {raw} (state {kid_raw}) by {interaction.user.id}.")

            # Refresh the settings menu in place so its solver status shows the new mode.
            await self.cog.show_settings_menu(interaction)
        except Exception as e:
            self.cog.logger.exception(f"Error setting test ID: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(f"{theme.deniedIcon} An error occurred: {str(e)}", ephemeral=True)


class RedemptionPriorityView(discord.ui.View):
    def __init__(self, cog, alliances_with_priority):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliances = alliances_with_priority  # List of (alliance_id, name, priority)
        self.selected_alliance_id = None

        # Alliance select menu
        options = [
            discord.SelectOption(
                label=f"{idx}. {name}",
                value=str(alliance_id),
                description=f"Priority position {idx}"
            )
            for idx, (alliance_id, name, _) in enumerate(self.alliances, 1)
        ]

        if options:
            self.alliance_select = discord.ui.Select(
                placeholder="Select an alliance to move",
                options=options[:25],  # Discord limit
                row=0
            )
            self.alliance_select.callback = self.alliance_select_callback
            self.add_item(self.alliance_select)

    async def alliance_select_callback(self, interaction: discord.Interaction):
        self.selected_alliance_id = int(self.alliance_select.values[0])

        # Update embed to show selected alliance with marker
        embed = discord.Embed(
            title=f"{theme.chartIcon} Redemption Priority",
            description="Configure the order in which alliances receive gift codes.\nSelect an alliance and use the buttons to change its position.",
            color=theme.emColor1
        )

        priority_list = []
        for idx, (alliance_id, name, _) in enumerate(self.alliances, 1):
            marker = " ◀" if alliance_id == self.selected_alliance_id else ""
            priority_list.append(f"`{idx}.` **{name}**{marker}")

        embed.add_field(
            name="Current Priority Order",
            value="\n".join(priority_list) if priority_list else "No alliances configured",
            inline=False
        )

        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Move Up", style=discord.ButtonStyle.primary, emoji=f"{theme.upIcon}", row=1)
    async def move_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alliance_id:
            await interaction.response.send_message("Please select an alliance first.", ephemeral=True)
            return

        # Find current position
        current_idx = next((i for i, (aid, _, _) in enumerate(self.alliances) if aid == self.selected_alliance_id), None)
        if current_idx is None or current_idx == 0:
            await interaction.response.send_message("Alliance is already at the top.", ephemeral=True)
            return

        # Swap with the alliance above
        await self._swap_priorities(current_idx, current_idx - 1)
        await self._refresh_view(interaction)

    @discord.ui.button(label="Move Down", style=discord.ButtonStyle.primary, emoji=f"{theme.downIcon}", row=1)
    async def move_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alliance_id:
            await interaction.response.send_message("Please select an alliance first.", ephemeral=True)
            return

        # Find current position
        current_idx = next((i for i, (aid, _, _) in enumerate(self.alliances) if aid == self.selected_alliance_id), None)
        if current_idx is None or current_idx >= len(self.alliances) - 1:
            await interaction.response.send_message("Alliance is already at the bottom.", ephemeral=True)
            return

        # Swap with the alliance below
        await self._swap_priorities(current_idx, current_idx + 1)
        await self._refresh_view(interaction)

    @discord.ui.button(label="Done", style=discord.ButtonStyle.secondary, emoji=f"{theme.verifiedIcon}", row=1)
    async def done_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.chartIcon} Priority Updated",
                description="Redemption priority order has been saved.",
                color=theme.emColor3
            ),
            view=None
        )

    async def _swap_priorities(self, idx1, idx2):
        """Swap the priorities of two alliances in the list and database."""
        alliance1_id, name1, priority1 = self.alliances[idx1]
        alliance2_id, name2, priority2 = self.alliances[idx2]

        # Assign new sequential priorities based on position
        new_priority1 = idx2 + 1
        new_priority2 = idx1 + 1

        # Update database
        self.cog.cursor.execute("""
            INSERT INTO giftcodecontrol (alliance_id, status, priority)
            VALUES (?, 0, ?)
            ON CONFLICT(alliance_id) DO UPDATE SET priority = excluded.priority
        """, (alliance1_id, new_priority1))

        self.cog.cursor.execute("""
            INSERT INTO giftcodecontrol (alliance_id, status, priority)
            VALUES (?, 0, ?)
            ON CONFLICT(alliance_id) DO UPDATE SET priority = excluded.priority
        """, (alliance2_id, new_priority2))

        self.cog.conn.commit()

        # Swap in local list
        self.alliances[idx1] = (alliance1_id, name1, new_priority1)
        self.alliances[idx2] = (alliance2_id, name2, new_priority2)
        self.alliances[idx1], self.alliances[idx2] = self.alliances[idx2], self.alliances[idx1]

    async def _refresh_view(self, interaction: discord.Interaction):
        """Refresh the embed and view after a priority change."""
        # Rebuild embed
        embed = discord.Embed(
            title=f"{theme.chartIcon} Redemption Priority",
            description="Configure the order in which alliances receive gift codes.\nSelect an alliance and use the buttons to change its position.",
            color=theme.emColor1
        )

        priority_list = []
        for idx, (alliance_id, name, _) in enumerate(self.alliances, 1):
            marker = " ◀" if alliance_id == self.selected_alliance_id else ""
            priority_list.append(f"`{idx}.` **{name}**{marker}")

        embed.add_field(
            name="Current Priority Order",
            value="\n".join(priority_list) if priority_list else "No alliances configured",
            inline=False
        )

        # Rebuild select options
        options = [
            discord.SelectOption(
                label=f"{idx}. {name}",
                value=str(alliance_id),
                description=f"Priority position {idx}"
            )
            for idx, (alliance_id, name, _) in enumerate(self.alliances, 1)
        ]

        if options:
            self.alliance_select.options = options[:25]

        await interaction.response.edit_message(embed=embed, view=self)


class ClearCacheConfirmView(discord.ui.View):
    def __init__(self, parent_cog):
        super().__init__(timeout=60)
        self.parent_cog = parent_cog

    @discord.ui.button(label="Confirm Clear", style=discord.ButtonStyle.danger, emoji=f"{theme.verifiedIcon}")
    async def confirm_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        try: # Clear the user_giftcodes table
            self.parent_cog.cursor.execute("DELETE FROM user_giftcodes")
            deleted_count = self.parent_cog.cursor.rowcount
            self.parent_cog.conn.commit()

            success_embed = discord.Embed(
                title=f"{theme.verifiedIcon} Redemption Cache Cleared",
                description=f"Successfully deleted {deleted_count:,} redemption records.\n\nUsers can now attempt to redeem gift codes again.",
                color=theme.emColor3
            )

            self.parent_cog.logger.info(f"Redemption cache cleared by user {interaction.user.id}: {deleted_count} records deleted")

            await interaction.response.edit_message(embed=success_embed, view=None)

        except Exception as e:
            self.parent_cog.logger.exception(f"Error clearing redemption cache: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} Error",
                description=f"Failed to clear redemption cache: {str(e)}",
                color=theme.emColor2
            )
            await safe_edit_message(interaction, embed=error_embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji=f"{theme.deniedIcon}")
    async def cancel_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        cancel_embed = discord.Embed(
            title=f"{theme.deniedIcon} Operation Cancelled",
            description="Redemption cache was not cleared.",
            color=theme.emColor1
        )
        await interaction.response.edit_message(embed=cancel_embed, view=None)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True



# ---------------------------------------------------------------------------
# Redemption Summary — per-alliance opt-in channel summary after redemptions
# ---------------------------------------------------------------------------

async def show_redemption_summary(cog, interaction: discord.Interaction):
    """Per-alliance config for the post-redemption summary embed."""
    alliances, _ = PermissionManager.get_admin_alliances(
        interaction.user.id, interaction.guild_id or 0
    )
    if not alliances:
        msg = f"{theme.deniedIcon} You have no alliances to configure."
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)
        return
    view = RedemptionSummaryView(cog, interaction.user.id, alliances)
    await safe_edit_message(interaction, embed=view.build_embed(), view=view, content=None)
    view.message = await interaction.original_response()


class RedemptionSummaryView(discord.ui.View):
    """Pick an alliance, then toggle whether/what its redemption summary posts."""

    def __init__(self, cog, user_id: int, alliances):
        super().__init__(timeout=7200)
        self.cog = cog
        self.user_id = user_id
        self.alliances = alliances            # [(alliance_id, name), ...]
        self.selected_id = None
        self.message = None
        self._build_components()

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        return await check_interaction_user(interaction, self.user_id)

    def _settings(self):
        from .gift_redemption import get_summary_settings
        return get_summary_settings(self.cog, self.selected_id) if self.selected_id else None

    def _build_components(self):
        self.clear_items()
        options = [
            discord.SelectOption(
                label=name[:100], value=str(aid),
                default=str(aid) == str(self.selected_id),
            )
            for aid, name in self.alliances[:25]
        ]
        select = discord.ui.Select(placeholder="Select an alliance", options=options, row=0)
        select.callback = self._on_alliance
        self.add_item(select)

        s = self._settings()
        if s is not None:
            self._add_toggle("Summary", theme.redeemIcon, bool(s["enabled"]), self._toggle_enabled, row=1)
            if s["enabled"]:
                self._add_toggle("Successful", theme.verifiedIcon, bool(s["success"]), self._toggle_success, row=2)
                self._add_toggle("Already Redeemed", theme.giftIcon, bool(s["already"]), self._toggle_already, row=2)
                self._add_toggle("Failed", theme.deniedIcon, bool(s["failed"]), self._toggle_failed, row=2)

        back = discord.ui.Button(label="Back", emoji=f"{theme.backIcon}", style=discord.ButtonStyle.secondary, row=3)
        back.callback = self._back
        self.add_item(back)

    def _add_toggle(self, label, emoji, enabled, callback, row):
        btn = discord.ui.Button(
            label=f"{label}: {'On' if enabled else 'Off'}",
            emoji=f"{emoji}",
            style=discord.ButtonStyle.success if enabled else discord.ButtonStyle.secondary,
            row=row,
        )
        btn.callback = callback
        self.add_item(btn)

    def build_embed(self) -> discord.Embed:
        desc = (
            "Choose an alliance, then turn its post-redemption summary on or off "
            "and pick which results it lists.\n\n"
            "When on, the bot posts one embed in the gift channel after each code "
            "finishes for that alliance, listing the buckets you enable.\n"
        )
        s = self._settings()
        if s is not None:
            name = next((n for a, n in self.alliances if str(a) == str(self.selected_id)), self.selected_id)
            if not s["enabled"]:
                state = f"{theme.deniedIcon} Off"
            else:
                picked = [lbl for lbl, on in (
                    ("Successful", s["success"]), ("Already Redeemed", s["already"]), ("Failed", s["failed"])
                ) if on]
                state = f"{theme.verifiedIcon} On — " + (", ".join(picked) if picked else "no buckets selected yet")
            desc += f"\n{theme.upperDivider}\n**{name}:** {state}\n{theme.lowerDivider}"
        return discord.Embed(
            title=f"{theme.redeemIcon} Redemption Summary",
            description=desc,
            color=theme.emColor1,
        )

    async def _refresh(self, interaction: discord.Interaction):
        self._build_components()
        await safe_edit_message(interaction, embed=self.build_embed(), view=self, content=None)

    async def _on_alliance(self, interaction: discord.Interaction):
        self.selected_id = int(interaction.data["values"][0])
        await self._refresh(interaction)

    async def _set(self, interaction, **kwargs):
        from .gift_redemption import set_summary_settings
        set_summary_settings(self.cog, self.selected_id, **kwargs)
        await self._refresh(interaction)

    async def _toggle_enabled(self, interaction):
        s = self._settings()
        turning_on = not s["enabled"]
        kwargs = {"enabled": turning_on}
        # Sensible default on first enable: show Failed (the actionable bucket).
        if turning_on and not (s["success"] or s["already"] or s["failed"]):
            kwargs["failed"] = True
        await self._set(interaction, **kwargs)

    async def _toggle_success(self, interaction):
        await self._set(interaction, success=not self._settings()["success"])

    async def _toggle_already(self, interaction):
        await self._set(interaction, already=not self._settings()["already"])

    async def _toggle_failed(self, interaction):
        await self._set(interaction, failed=not self._settings()["failed"])

    async def _back(self, interaction: discord.Interaction):
        await self.cog.show_settings_menu(interaction)

    async def on_timeout(self):
        await notify_view_expired(self, "redemption summary settings")



# --- Member state management ---

async def show_state_management(cog, interaction: discord.Interaction):
    view = StateManagementView(cog, interaction.user.id)
    await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)


class StateManagementView(discord.ui.View):
    def __init__(self, cog, user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = user_id
        self.last_result = None

    async def build_embed(self):
        from . import gift_state_resolver as gsr
        missing = await asyncio.to_thread(gsr.fids_missing_state)
        survey = await asyncio.to_thread(gsr.survey_alliance_bindings)
        multistate = [r for r in survey if r["multistate"]]
        unbound = [r for r in survey if r["current_kid"] is None and not r["multistate"]]
        bindable = [r for r in unbound if r["proposed_kid"] is not None]

        lines = [
            "Redemption now needs each member's state on file. "
            "Fix wrong or missing states here.\n",
            f"{theme.upperDivider}",
            f"{theme.membersIcon} **Members missing a state:** `{len(missing)}`",
            f"{theme.allianceIcon} **Unbound alliances:** `{len(unbound)}` "
            f"({len(bindable)} can be auto-bound)",
            f"{theme.allianceIcon} **Multistate alliances:** `{len(multistate)}` (never auto-bound)\n",
            f"{theme.chartIcon} **Auto-bind Alliances**",
            "└ Set each alliance's state from its members' majority",
            f"{theme.membersIcon} **Assign State to Missing**",
            "└ Give members with no state their alliance's state (instant, no game calls)",
            f"{theme.searchIcon} **Resolve Unknown (API)**",
            f"└ Probe the game for the rest, one at a time (about {max(1, len(missing)) * 2}s total; no rewards sent)",
            f"\n{theme.infoIcon} Can't auto-bind an alliance (too few known members)? "
            f"Set its state on the alliance itself (Alliance Management -> Set State), then Assign State to Missing.",
        ]
        if self.last_result:
            lines.append(f"\n{theme.verifiedIcon} {self.last_result}")
        lines.append(f"{theme.lowerDivider}")

        return discord.Embed(
            title=f"{theme.fidIcon} Member States",
            description="\n".join(lines),
            color=theme.emColor1,
        )

    @discord.ui.button(label="Auto-bind Alliances", style=discord.ButtonStyle.primary,
                       emoji=f"{theme.chartIcon}", row=0)
    async def bind_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        result = await asyncio.to_thread(self.cog.bind_alliance_states)
        b, m = len(result["bound"]), len(result["multistate"])
        self.last_result = (
            f"Bound {b} alliance(s) to a state; flagged {m} as multistate."
            if (b or m) else "No alliances had a clear majority to bind."
        )
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    @discord.ui.button(label="Assign State to Missing", style=discord.ButtonStyle.primary,
                       emoji=f"{theme.membersIcon}", row=0)
    async def assign_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        n = await asyncio.to_thread(self.cog.assign_alliance_state_to_missing)
        self.last_result = f"Assigned an alliance state to {n} member(s) that had none."
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    @discord.ui.button(label="Resolve Unknown (API)", style=discord.ButtonStyle.secondary,
                       emoji=f"{theme.searchIcon}", row=1)
    async def resolve_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        from . import gift_state_resolver as gsr
        missing = await asyncio.to_thread(gsr.fids_missing_state)
        if not missing:
            self.last_result = "No members are missing a state."
            await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)
            return
        # Probing the game is slow (per-FID paced) - defer so the interaction doesn't expire.
        await interaction.response.defer()
        found = await self.cog.resolve_remaining_missing_states()
        self.last_result = f"Resolved {len(found)} of {len(missing)} unknown state(s) via the game."
        await interaction.edit_original_response(embed=await self.build_embed(), view=self)

    @discord.ui.button(label="Multistate Alliances", style=discord.ButtonStyle.secondary,
                       emoji=f"{theme.allianceIcon}", row=1)
    async def multistate_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        view = MultistateView(self.cog, self.original_user_id)
        await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary,
                       emoji=f"{theme.backIcon}", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_alliance_management(interaction)

    async def on_timeout(self):
        await notify_view_expired(self, "member states menu")


class MultistateView(discord.ui.View):
    """Toggle which alliances are 'multistate' (members from many states - never auto-bound)."""
    def __init__(self, cog, user_id, page=0):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = user_id
        self.page = page
        self.rows = []

    async def _load(self):
        from . import gift_state_resolver as gsr
        self.rows = await asyncio.to_thread(gsr.survey_alliance_bindings)
        self.rows.sort(key=lambda r: (not r["multistate"], str(r["name"]).lower()))

    async def build_embed(self):
        await self._load()
        self.clear_items()
        pages = max(1, (len(self.rows) + 24) // 25)
        self.page = max(0, min(self.page, pages - 1))
        page_rows = self.rows[self.page * 25:(self.page + 1) * 25]

        if page_rows:
            options = []
            for r in page_rows:
                on = r["multistate"]
                status = "Multistate" if on else (f"State {r['current_kid']}" if r["current_kid"] else "Unbound")
                options.append(discord.SelectOption(
                    label=str(r["name"])[:100], value=str(r["alliance_id"]),
                    description=f"{status} - tap to turn multistate {'off' if on else 'on'}"[:100],
                    emoji=theme.verifiedIcon if on else None,
                ))
            select = discord.ui.Select(placeholder="Toggle an alliance's multistate flag", options=options, row=0)
            select.callback = self._on_select
            self.add_item(select)

        if pages > 1:
            self._add_nav(pages)
        back = discord.ui.Button(label="Back", emoji=f"{theme.backIcon}", style=discord.ButtonStyle.secondary, row=2)
        back.callback = self._on_back
        self.add_item(back)

        lines = [
            "Mark alliances whose members span many states (public/mixed bots). "
            "Multistate alliances are never auto-bound to one state and their members "
            "keep their own resolved states.\n",
            f"{theme.upperDivider}",
            f"{theme.allianceIcon} Currently multistate: "
            f"`{sum(1 for r in self.rows if r['multistate'])}`",
            f"{theme.lowerDivider}",
        ]
        return discord.Embed(title=f"{theme.allianceIcon} Multistate Alliances",
                             description="\n".join(lines), color=theme.emColor1)

    def _add_nav(self, pages):
        prev = discord.ui.Button(label="Prev", style=discord.ButtonStyle.secondary, row=1, disabled=self.page == 0)
        nxt = discord.ui.Button(label="Next", style=discord.ButtonStyle.secondary, row=1, disabled=self.page >= pages - 1)
        prev.callback = self._on_prev
        nxt.callback = self._on_next
        self.add_item(prev)
        self.add_item(nxt)

    async def _on_select(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        from . import gift_state_resolver as gsr
        alliance_id = int(interaction.data["values"][0])
        currently = await asyncio.to_thread(gsr.is_multistate, alliance_id)
        await asyncio.to_thread(gsr.set_multistate, alliance_id, not currently)
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    async def _on_prev(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        self.page -= 1
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    async def _on_next(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        self.page += 1
        await safe_edit_message(interaction, embed=await self.build_embed(), view=self, content=None)

    async def _on_back(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        view = StateManagementView(self.cog, self.original_user_id)
        await safe_edit_message(interaction, embed=await view.build_embed(), view=view, content=None)

    async def on_timeout(self):
        await notify_view_expired(self, "multistate alliances menu")
