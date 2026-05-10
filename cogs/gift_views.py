"""Gift code UI views, modals, and CRUD operations."""

import discord
import sqlite3
import asyncio
import logging
from datetime import datetime

from .pimp_my_bot import theme, safe_edit_message, check_interaction_user
from .alliance_member_operations import AllianceSelectView
from .permission_handler import PermissionManager

logger = logging.getLogger('gift')


# ---------------------------------------------------------------------------
# Standalone helper / CRUD functions  (cog = GiftOperations instance)
# ---------------------------------------------------------------------------

async def handle_success(cog, message, giftcode):
    test_fid = cog.get_test_fid()
    status = await cog.claim_giftcode_rewards_wos(test_fid, giftcode)

    if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
        cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
        if not cog.cursor.fetchone():
            cog.cursor.execute("INSERT INTO gift_codes (giftcode, date) VALUES (?, ?)", (giftcode, datetime.now()))
            cog.conn.commit()

            try:
                asyncio.create_task(cog.api.add_giftcode(giftcode))
            except Exception:
                pass

            await message.add_reaction(f"{theme.verifiedIcon}")
            await message.reply("Gift code successfully added.", mention_author=False)
    elif status == "TIME_ERROR":
        await message.add_reaction(f"{theme.deniedIcon}")
        await message.reply("Gift code expired.", mention_author=False)
    elif status == "CDK_NOT_FOUND":
        await message.add_reaction(f"{theme.deniedIcon}")
        await message.reply("The gift code is incorrect.", mention_author=False)
    elif status == "USAGE_LIMIT":
        await message.add_reaction(f"{theme.deniedIcon}")
        await message.reply("Usage limit has been reached for this code.", mention_author=False)


handle_already_received = handle_success


async def get_admin_info(cog, user_id):
    """Get admin info - delegates to centralized PermissionManager"""
    is_admin, is_global = PermissionManager.is_admin(user_id)
    if not is_admin:
        return None
    return (user_id, 1 if is_global else 0)


async def get_alliance_names(cog, user_id, is_global=False):
    if is_global:
        cog.alliance_cursor.execute("SELECT name FROM alliance_list")
        return [row[0] for row in cog.alliance_cursor.fetchall()]
    else:
        cog.settings_cursor.execute("""
            SELECT alliances_id FROM adminserver WHERE admin = ?
        """, (user_id,))
        alliance_ids = [row[0] for row in cog.settings_cursor.fetchall()]

        if alliance_ids:
            placeholders = ','.join('?' * len(alliance_ids))
            cog.alliance_cursor.execute(f"""
                SELECT name FROM alliance_list
                WHERE alliance_id IN ({placeholders})
            """, alliance_ids)
            return [row[0] for row in cog.alliance_cursor.fetchall()]
        return []


async def get_available_alliances(cog, interaction: discord.Interaction):
    """Get available alliances - delegates to centralized PermissionManager"""
    user_id = interaction.user.id
    guild_id = interaction.guild_id if interaction.guild else None

    alliances, _ = PermissionManager.get_admin_alliances(user_id, guild_id or 0)
    return alliances


async def create_gift_code(cog, interaction: discord.Interaction):
    cog.settings_cursor.execute("SELECT 1 FROM admin WHERE id = ?", (interaction.user.id,))
    if not cog.settings_cursor.fetchone():
        await interaction.response.send_message(
            f"{theme.deniedIcon} You are not authorized to create gift codes.",
            ephemeral=True
        )
        return

    modal = CreateGiftCodeModal(cog)
    try:
        await interaction.response.send_modal(modal)
    except Exception as e:
        cog.logger.exception(f"Error showing modal: {e}")
        if not interaction.response.is_done():
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while showing the gift code creation form.",
                ephemeral=True
            )


async def list_gift_codes(cog, interaction: discord.Interaction):
    cog.cursor.execute("""
        SELECT
            gc.giftcode,
            gc.date,
            COUNT(DISTINCT ugc.fid) as used_count
        FROM gift_codes gc
        LEFT JOIN user_giftcodes ugc ON gc.giftcode = ugc.giftcode
        WHERE gc.validation_status = 'validated'
        GROUP BY gc.giftcode
        ORDER BY gc.date DESC
    """)

    codes = cog.cursor.fetchall()

    if not codes:
        await interaction.response.send_message(
            "No active gift codes found in the database.",
            ephemeral=True
        )
        return

    embed = discord.Embed(
        title=f"{theme.giftIcon} Active Gift Codes",
        description="Currently active and valid gift codes.",
        color=theme.emColor1
    )

    for code, date, used_count in codes:
        embed.add_field(
            name=f"Code: {code}",
            value=f"Created: {date}\nUsed by: {used_count} users",
            inline=False
        )

    await interaction.response.send_message(embed=embed, ephemeral=True)


async def delete_gift_code(cog, interaction: discord.Interaction):
    try:
        settings_conn = sqlite3.connect('db/settings.sqlite')
        settings_cursor = settings_conn.cursor()

        settings_cursor.execute("""
            SELECT 1 FROM admin
            WHERE id = ? AND is_initial = 1
        """, (interaction.user.id,))

        is_admin = settings_cursor.fetchone()
        settings_cursor.close()
        settings_conn.close()

        if not is_admin:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} Unauthorized Access",
                    description="This action requires Global Admin privileges.",
                    color=theme.emColor2
                ),
                ephemeral=True
            )
            return

        cog.cursor.execute("""
            SELECT
                gc.giftcode,
                gc.date,
                gc.validation_status,
                COUNT(DISTINCT ugc.fid) as used_count
            FROM gift_codes gc
            LEFT JOIN user_giftcodes ugc ON gc.giftcode = ugc.giftcode
            GROUP BY gc.giftcode, gc.date, gc.validation_status
            ORDER BY gc.date ASC
        """)

        codes = cog.cursor.fetchall()

        if not codes:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} No Gift Codes",
                    description="There are no gift codes in the database to delete.",
                    color=theme.emColor2
                ),
                ephemeral=True
            )
            return

        # Discord limits Select menus to 25 options
        total_codes = len(codes)
        codes_to_show = codes[:25] if total_codes > 25 else codes

        select_options = []
        for code, date, validation_status, used_count in codes_to_show:
            if validation_status == 'validated':
                status_display = f"{theme.verifiedIcon} Valid"
            elif validation_status == 'invalid':
                status_display = f"{theme.deniedIcon} Invalid"
            elif validation_status == 'pending':
                status_display = f"{theme.warnIcon} Pending"
            else:
                status_display = f"{theme.infoIcon} Unknown"

            select_options.append(
                discord.SelectOption(
                    label=f"Code: {code}",
                    description=f"{status_display} | Created: {date} | Used: {used_count}",
                    value=code
                )
            )

        # Handling for 0 codes to avoid errors
        if not select_options:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title=f"{theme.deniedIcon} No Gift Codes Available",
                    description="No gift codes found in the database to delete.",
                    color=theme.emColor2
                ),
                ephemeral=True
            )
            return

        select = discord.ui.Select(
            placeholder="Select a gift code to delete",
            options=select_options
        )

        async def select_callback(select_interaction):
            selected_code = select_interaction.data["values"][0]

            confirm = discord.ui.Button(
                style=discord.ButtonStyle.danger,
                label="Confirm Delete",
                custom_id="confirm"
            )
            cancel = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                label="Cancel",
                custom_id="cancel"
            )

            async def button_callback(button_interaction):
                try:
                    if button_interaction.data.get('custom_id') == "confirm":
                        try:
                            cog.cursor.execute("DELETE FROM gift_codes WHERE giftcode = ?", (selected_code,))
                            cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (selected_code,))
                            cog.conn.commit()

                            success_embed = discord.Embed(
                                title=f"{theme.verifiedIcon} Gift Code Deleted",
                                description=(
                                    f"**Deletion Details**\n"
                                    f"{theme.upperDivider}\n"
                                    f"{theme.giftIcon} **Gift Code:** `{selected_code}`\n"
                                    f"{theme.userIcon} **Deleted by:** {button_interaction.user.mention}\n"
                                    f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                                    f"{theme.lowerDivider}\n"
                                ),
                                color=theme.emColor3
                            )

                            await button_interaction.response.edit_message(
                                embed=success_embed,
                                view=None
                            )

                        except Exception as e:
                            await button_interaction.response.send_message(
                                f"{theme.deniedIcon} An error occurred while deleting the gift code.",
                                ephemeral=True
                            )

                    else:
                        cancel_embed = discord.Embed(
                            title=f"{theme.deniedIcon} Deletion Cancelled",
                            description="The gift code deletion was cancelled.",
                            color=theme.emColor2
                        )
                        await button_interaction.response.edit_message(
                            embed=cancel_embed,
                            view=None
                        )

                except Exception as e:
                    cog.logger.exception(f"Button callback error: {str(e)}")
                    try:
                        await button_interaction.response.send_message(
                            f"{theme.deniedIcon} An error occurred while processing the request.",
                            ephemeral=True
                        )
                    except Exception:
                        await button_interaction.followup.send(
                            f"{theme.deniedIcon} An error occurred while processing the request.",
                            ephemeral=True
                        )

            confirm.callback = button_callback
            cancel.callback = button_callback

            confirm_view = discord.ui.View()
            confirm_view.add_item(confirm)
            confirm_view.add_item(cancel)

            confirmation_embed = discord.Embed(
                title=f"{theme.warnIcon} Confirm Deletion",
                description=(
                    f"**Gift Code Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Selected Code:** `{selected_code}`\n"
                    f"{theme.warnIcon} **Warning:** This action cannot be undone!\n"
                    f"{theme.lowerDivider}\n"
                ),
                color=theme.emColor4
            )

            await select_interaction.response.edit_message(
                embed=confirmation_embed,
                view=confirm_view
            )

        select.callback = select_callback
        view = discord.ui.View()
        view.add_item(select)

        # Build description with truncation notice if needed
        description_text = (
            f"**Instructions**\n"
            f"{theme.upperDivider}\n"
            f"{theme.num1Icon} Select a gift code from the menu below\n"
            f"{theme.num2Icon} Confirm your selection\n"
            f"{theme.num3Icon} The code will be permanently deleted\n"
            f"{theme.lowerDivider}\n"
        )

        if total_codes > 25:
            description_text += (
                f"\n{theme.warnIcon} **Note:** Showing 25 of {total_codes} codes.\n"
                f"Oldest codes are shown first.\n"
                f"To delete newer codes, you'll need to delete the older ones first."
            )

        initial_embed = discord.Embed(
            title=f"{theme.trashIcon} Delete Gift Code",
            description=description_text,
            color=theme.emColor1
        )

        await interaction.response.send_message(
            embed=initial_embed,
            view=view,
            ephemeral=True
        )

    except Exception as e:
        cog.logger.exception(f"Delete gift code error: {str(e)}")
        await interaction.response.send_message(
            f"{theme.deniedIcon} An error occurred while processing the request.",
            ephemeral=True
        )


async def show_settings_menu(cog, interaction: discord.Interaction):
    """Show unified settings menu with all configuration options."""
    admin_info = await get_admin_info(cog, interaction.user.id)
    if not admin_info:
        await interaction.response.send_message(
            f"{theme.deniedIcon} You are not authorized to perform this action.",
            ephemeral=True
        )
        return

    is_global = admin_info[1] == 1

    settings_embed = discord.Embed(
        title=f"{theme.settingsIcon} Gift Code Settings",
        description=(
            f"{theme.upperDivider}\n"
            f"{theme.announceIcon} **Channel Management**\n"
            f"└ Set up and manage the channel(s) where the bot scans for new codes\n\n"
            f"{theme.giftIcon} **Automatic Redemption**\n"
            f"└ Enable/disable auto-redemption of new valid gift codes\n\n"
            f"{theme.chartIcon} **Redemption Priority**\n"
            f"└ Change the order in which alliances auto-redeem new gift codes\n\n"
            f"{theme.searchIcon} **Channel History Scan**\n"
            f"└ Scan for gift codes in existing messages in a gift channel\n\n"
            f"{theme.settingsIcon} **CAPTCHA Settings**\n"
            f"└ Configure CAPTCHA-solver related settings and image saving\n"
            f"{theme.lowerDivider}"
        ),
        color=theme.emColor1
    )

    settings_view = SettingsMenuView(cog, interaction.user.id, is_global)

    await interaction.response.edit_message(
        embed=settings_embed,
        view=settings_view
    )


# ---------------------------------------------------------------------------
# Modals
# ---------------------------------------------------------------------------

class CreateGiftCodeModal(discord.ui.Modal):
    def __init__(self, cog):
        super().__init__(title="Create Gift Code")
        self.cog = cog

        self.giftcode = discord.ui.TextInput(
            label="Gift Code",
            placeholder="Enter the gift code",
            required=True,
            min_length=4,
            max_length=20
        )
        self.add_item(self.giftcode)

    async def on_submit(self, interaction: discord.Interaction):
        logger = self.cog.logger
        await interaction.response.defer(ephemeral=True)

        code = self.cog.clean_gift_code(self.giftcode.value)
        logger.info(f"[CreateGiftCodeModal] Code entered: {code}")
        final_embed = discord.Embed(title=f"{theme.giftIcon} Gift Code Creation Result")

        # Check if code already exists
        self.cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
        if self.cog.cursor.fetchone():
            logger.info(f"[CreateGiftCodeModal] Code {code} already exists in DB.")
            final_embed.title = f"{theme.infoIcon} Gift Code Exists"
            final_embed.description = (
                f"**Gift Code Details**\n{theme.upperDivider}\n"
                f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                f"{theme.verifiedIcon} **Status:** Code already exists in database.\n"
                f"{theme.lowerDivider}\n"
            )
            final_embed.color = theme.emColor1
        else: # Validate the code immediately
            logger.info(f"[CreateGiftCodeModal] Validating code {code} before adding to DB.")

            validation_embed = discord.Embed(
                title=f"{theme.refreshIcon} Validating Gift Code...",
                description=f"Checking if `{code}` is valid...",
                color=theme.emColor1
            )
            await interaction.edit_original_response(embed=validation_embed)

            is_valid, validation_msg = await self.cog.validate_gift_code_immediately(code, "button")

            if is_valid: # Valid code - send to API and add to DB
                logger.info(f"[CreateGiftCodeModal] Code '{code}' validated successfully.")

                if hasattr(self.cog, 'api') and self.cog.api:
                    asyncio.create_task(self.cog.api.add_giftcode(code))

                await self.cog._process_auto_use(code)

                self.cog.cursor.execute("SELECT COUNT(*) FROM giftcodecontrol WHERE status = 1")
                auto_count = self.cog.cursor.fetchone()[0]

                final_embed.title = f"{theme.verifiedIcon} Gift Code Validated"
                final_embed.description = (
                    f"**Gift Code Details**\n{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                    f"{theme.verifiedIcon} **Status:** {validation_msg}\n"
                    f"{theme.editListIcon} **Action:** Added to database and sent to API\n"
                    f"{theme.refreshIcon} **Auto-redemption:** {'Queued for ' + str(auto_count) + ' alliance(s)' if auto_count else 'Disabled'}\n"
                    f"{theme.lowerDivider}\n"
                )
                final_embed.color = theme.emColor3

            elif is_valid is False: # Invalid code - do not add
                logger.warning(f"[CreateGiftCodeModal] Code '{code}' is invalid: {validation_msg}")

                final_embed.title = f"{theme.deniedIcon} Invalid Gift Code"
                final_embed.description = (
                    f"**Gift Code Details**\n{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                    f"{theme.deniedIcon} **Status:** {validation_msg}\n"
                    f"{theme.editListIcon} **Action:** Code not added to database\n"
                    f"{theme.lowerDivider}\n"
                )
                final_embed.color = theme.emColor2

            else: # Validation inconclusive - add as pending
                logger.warning(f"[CreateGiftCodeModal] Code '{code}' validation inconclusive: {validation_msg}")

                try:
                    date = datetime.now().strftime("%Y-%m-%d")
                    self.cog.cursor.execute(
                        "INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                        (code, date, "pending")
                    )
                    self.cog.conn.commit()

                    final_embed.title = f"{theme.warnIcon} Gift Code Added (Pending)"
                    final_embed.description = (
                        f"**Gift Code Details**\n{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                        f"{theme.warnIcon} **Status:** {validation_msg}\n"
                        f"{theme.editListIcon} **Action:** Added for later validation\n"
                        f"{theme.lowerDivider}\n"
                    )
                    final_embed.color = theme.emColor4

                except sqlite3.Error as db_err:
                    logger.exception(f"[CreateGiftCodeModal] DB Error inserting code '{code}': {db_err}")
                    final_embed.title = f"{theme.deniedIcon} Database Error"
                    final_embed.description = f"Failed to save gift code `{code}` to the database. Please check logs."
                    final_embed.color = theme.emColor2

        try:
            await interaction.edit_original_response(embed=final_embed)
            logger.info(f"[CreateGiftCodeModal] Final result embed sent for code {code}.")
        except Exception as final_edit_err:
            logger.exception(f"[CreateGiftCodeModal] Failed to edit interaction with final result for {code}: {final_edit_err}")


class DeleteGiftCodeModal(discord.ui.Modal, title="Delete Gift Code"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    giftcode = discord.ui.TextInput(
        label="Gift Code",
        placeholder="Enter the gift code to delete",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        code = self.giftcode.value

        self.cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
        if not self.cog.cursor.fetchone():
            await interaction.response.send_message(
                f"{theme.deniedIcon} Gift code not found!",
                ephemeral=True
            )
            return

        self.cog.cursor.execute("DELETE FROM gift_codes WHERE giftcode = ?", (code,))
        self.cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (code,))
        self.cog.conn.commit()

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Gift Code Deleted",
            description=f"Gift code `{code}` has been deleted successfully.",
            color=theme.emColor3
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------

class GiftView(discord.ui.View):
    def __init__(self, cog, original_user_id):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id

    @discord.ui.button(
        label="Add Gift Code",
        style=discord.ButtonStyle.green,
        custom_id="create_gift",
        emoji=f"{theme.giftIcon}",
        row=0
    )
    async def create_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.create_gift_code(interaction)

    @discord.ui.button(
        label="List Gift Codes",
        style=discord.ButtonStyle.blurple,
        custom_id="list_gift",
        emoji=f"{theme.listIcon}",
        row=0
    )
    async def list_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.list_gift_codes(interaction)

    @discord.ui.button(
        label="Redeem Gift Code",
        emoji=f"{theme.targetIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="use_gift_alliance",
        row=0
    )
    async def use_gift_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        try:
            admin_info = await self.cog.get_admin_info(interaction.user.id)
            if not admin_info:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You are not authorized to perform this action.",
                    ephemeral=True
                )
                return

            available_alliances = await self.cog.get_available_alliances(interaction)
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

            alliances_with_counts = []
            for alliance_id, name in available_alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            alliance_embed = discord.Embed(
                title=f"{theme.targetIcon} Redeem Gift Code",
                description=(
                    f"Select an alliance to use gift code:\n\n"
                    f"**Alliance List**\n"
                    f"{theme.upperDivider}\n"
                    f"Select an alliance from the list below:\n"
                ),
                color=theme.emColor1
            )

            view = AllianceSelectView(alliances_with_counts, self.cog, context="giftcode")

            view.current_select.options.insert(0, discord.SelectOption(
                label="ALL ALLIANCES",
                value="all",
                description=f"Apply to all {len(alliances_with_counts)} alliances",
                emoji=theme.globeIcon
            ))

            async def alliance_callback(select_interaction: discord.Interaction, alliance_id=None):
                try:
                    # If alliance_id is provided (from ID search modal), use it directly
                    if alliance_id is not None:
                        selected_value = str(alliance_id)
                    else:
                        selected_value = view.current_select.values[0]

                    if selected_value == "all":
                        # Get alliances ordered by priority
                        alliance_ids = [aid for aid, _, _ in alliances_with_counts]
                        placeholders = ','.join('?' * len(alliance_ids))
                        self.cog.cursor.execute(f"""
                            SELECT alliance_id FROM giftcodecontrol
                            WHERE alliance_id IN ({placeholders})
                            ORDER BY priority ASC, alliance_id ASC
                        """, alliance_ids)
                        prioritized = [row[0] for row in self.cog.cursor.fetchall()]
                        # Add any alliances not in giftcodecontrol at the end, ordered by ID
                        remaining = sorted([aid for aid in alliance_ids if aid not in prioritized])
                        all_alliances = prioritized + remaining
                    else:
                        alliance_id = int(selected_value)
                        all_alliances = [alliance_id]

                    self.cog.cursor.execute("""
                        SELECT giftcode, date FROM gift_codes
                        WHERE validation_status != 'invalid'
                        ORDER BY date DESC
                    """)
                    gift_codes = self.cog.cursor.fetchall()

                    if not gift_codes:
                        await select_interaction.response.edit_message(
                            content="No active gift codes available.",
                            embed=None,
                            view=None
                        )
                        return

                    giftcode_embed = discord.Embed(
                        title=f"{theme.giftIcon} Select Gift Code",
                        description=(
                            f"Select a gift code to use:\n\n"
                            f"**Gift Code List**\n"
                            f"{theme.upperDivider}\n"
                            f"Select a gift code from the list below:\n"
                        ),
                        color=theme.emColor1
                    )

                    select_giftcode = discord.ui.Select(
                        placeholder="Select a gift code",
                        options=[
                            discord.SelectOption(
                                label=f"Code: {code}",
                                value=code,
                                description=f"Created: {date}",
                                emoji=theme.giftIcon
                            ) for code, date in gift_codes
                        ]
                    )

                    # Add ALL CODES option at the beginning
                    select_giftcode.options.insert(0, discord.SelectOption(
                        label="ALL CODES",
                        value="all_codes",
                        description=f"Redeem all {len(gift_codes)} active codes",
                        emoji=theme.packageIcon
                    ))

                    async def giftcode_callback(giftcode_interaction: discord.Interaction):
                        try:
                            selected_code_value = giftcode_interaction.data["values"][0]

                            # Handle ALL CODES selection
                            if selected_code_value == "all_codes":
                                selected_codes = [code for code, date in gift_codes]
                                code_display = f"ALL ({len(selected_codes)} codes)"
                            else:
                                selected_codes = [selected_code_value]
                                code_display = f"`{selected_code_value}`"

                            alliance_display = 'ALL' if selected_value == 'all' else next((name for aid, name, _ in alliances_with_counts if aid == alliance_id), 'Unknown')
                            total_redemptions = len(selected_codes) * len(all_alliances)

                            confirm_embed = discord.Embed(
                                title=f"{theme.warnIcon} Confirm Gift Code Usage",
                                description=(
                                    f"Are you sure you want to use {'these gift codes' if len(selected_codes) > 1 else 'this gift code'}?\n\n"
                                    f"**Details**\n"
                                    f"{theme.upperDivider}\n"
                                    f"{theme.giftIcon} **Gift Code{'s' if len(selected_codes) > 1 else ''}:** {code_display}\n"
                                    f"{theme.allianceIcon} **Alliances:** {alliance_display} ({len(all_alliances)})\n"
                                    f"{theme.chartIcon} **Total redemptions:** {total_redemptions}\n"
                                    f"{theme.lowerDivider}\n"
                                ),
                                color=theme.emColor4
                            )

                            confirm_view = discord.ui.View()

                            async def confirm_callback(button_interaction: discord.Interaction):
                                try:
                                    # Defer first so followup.send works for batch progress
                                    await button_interaction.response.defer()

                                    await self.cog.add_manual_redemption_to_queue(
                                        selected_codes, all_alliances, button_interaction
                                    )

                                    queue_status = await self.cog.get_queue_status()

                                    alliance_names = []
                                    for aid in all_alliances[:3]:  # Show first 3 alliance names
                                        name = next((n for a_id, n, _ in alliances_with_counts if a_id == aid), 'Unknown')
                                        alliance_names.append(name)

                                    alliance_list = ", ".join(alliance_names)
                                    if len(all_alliances) > 3:
                                        alliance_list += f" and {len(all_alliances) - 3} more"

                                    queue_summary = []
                                    your_position = None

                                    for code, items in queue_status['queue_by_code'].items():
                                        alliance_count = len([i for i in items if i.get('alliance_id')])

                                        if code in selected_codes and your_position is None:
                                            your_position = min(i['position'] for i in items)

                                        queue_summary.append(f"• `{code}` - {alliance_count} alliance{'s' if alliance_count != 1 else ''}")

                                    queue_info = "\n".join(queue_summary) if queue_summary else "Queue is empty"

                                    queue_embed = discord.Embed(
                                        title=f"{theme.verifiedIcon} Redemptions Queued Successfully",
                                        description=(
                                            f"Gift code redemptions added to the queue.\n\n"
                                            f"**Your Redemption**\n"
                                            f"{theme.upperDivider}\n"
                                            f"{theme.giftIcon} **Gift Code{'s' if len(selected_codes) > 1 else ''}:** {code_display}\n"
                                            f"{theme.allianceIcon} **Alliances:** {alliance_list}\n"
                                            f"{theme.chartIcon} **Total redemptions:** {len(selected_codes) * len(all_alliances)}\n"
                                            f"{theme.lowerDivider}\n\n"
                                            f"**Full Queue Details**\n"
                                            f"{queue_info}\n\n"
                                            f"{theme.chartIcon} **Total items in queue:** {queue_status['queue_length']}\n"
                                            f"{theme.pinIcon} **Your position:** #{your_position if your_position else 'Processing'}\n\n"
                                            f"{theme.infoIcon} You'll receive notifications as each alliance is processed."
                                        ),
                                        color=theme.emColor3
                                    )
                                    queue_embed.set_footer(text="Gift codes are processed sequentially to prevent issues.")

                                    await button_interaction.edit_original_response(
                                        embed=queue_embed,
                                        view=None
                                    )

                                except Exception as e:
                                    self.cog.logger.exception(f"Error queueing gift code redemptions: {e}")
                                    await button_interaction.followup.send(
                                        f"{theme.deniedIcon} An error occurred while queueing the gift code redemptions.",
                                        ephemeral=True
                                    )

                            async def cancel_callback(button_interaction: discord.Interaction):
                                cancel_embed = discord.Embed(
                                    title=f"{theme.deniedIcon} Operation Cancelled",
                                    description="The gift code usage has been cancelled.",
                                    color=theme.emColor2
                                )
                                await button_interaction.response.edit_message(
                                    embed=cancel_embed,
                                    view=None
                                )

                            confirm_button = discord.ui.Button(
                                label="Confirm",
                                style=discord.ButtonStyle.success,
                                emoji=f"{theme.verifiedIcon}"
                            )
                            cancel_button = discord.ui.Button(
                                label="Cancel",
                                style=discord.ButtonStyle.danger,
                                emoji=f"{theme.deniedIcon}"
                            )

                            confirm_button.callback = confirm_callback
                            cancel_button.callback = cancel_callback

                            confirm_view.add_item(confirm_button)
                            confirm_view.add_item(cancel_button)

                            await giftcode_interaction.response.edit_message(
                                embed=confirm_embed,
                                view=confirm_view
                            )
                        except Exception as e:
                            self.cog.logger.exception(f"Gift code callback error: {e}")
                            await giftcode_interaction.response.send_message(
                                f"{theme.deniedIcon} An error occurred while processing the gift code.",
                                ephemeral=True
                            )

                    select_giftcode.callback = giftcode_callback
                    giftcode_view = discord.ui.View()
                    giftcode_view.add_item(select_giftcode)

                    await select_interaction.response.edit_message(
                        embed=giftcode_embed,
                        view=giftcode_view
                    )
                except Exception as e:
                    self.cog.logger.exception(f"Alliance callback error: {e}")
                    await select_interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while processing the alliance selection.",
                        ephemeral=True
                    )

            view.current_select.callback = alliance_callback
            await interaction.response.send_message(
                embed=alliance_embed,
                view=view,
                ephemeral=True
            )
        except Exception as e:
            self.cog.logger.exception(f"Use gift alliance button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing the request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Settings",
        style=discord.ButtonStyle.secondary,
        custom_id="gift_code_settings",
        emoji=f"{theme.settingsIcon}",
        row=1
    )
    async def gift_code_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_settings_menu(interaction)

    @discord.ui.button(
        label="Delete Gift Code",
        emoji=f"{theme.trashIcon}",
        style=discord.ButtonStyle.danger,
        custom_id="delete_gift",
        row=1
    )
    async def delete_gift_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        try:
            await self.cog.delete_gift_code(interaction)
        except Exception as e:
            self.cog.logger.exception(f"Delete gift button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing delete request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Main Menu",
        emoji=f"{theme.homeIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu_from_gifts",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        try:
            main_menu_cog = self.cog.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_main_menu(interaction)
        except Exception as e:
            logger.error(f"Error returning to main menu: {e}")
            print(f"Error returning to main menu: {e}")


class SettingsMenuView(discord.ui.View):
    def __init__(self, cog, original_user_id, is_global: bool = False):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.is_global = is_global

        # Disable global-admin-only buttons for non-global admins
        if not is_global:
            for child in self.children:
                if isinstance(child, discord.ui.Button) and child.label in [
                    "Redemption Priority", "CAPTCHA Settings"
                ]:
                    child.disabled = True

    @discord.ui.button(
        label="Channel Management",
        style=discord.ButtonStyle.green,
        custom_id="channel_management",
        emoji=f"{theme.announceIcon}",
        row=0
    )
    async def channel_management_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.manage_channel_settings(interaction)

    @discord.ui.button(
        label="Automatic Redemption",
        style=discord.ButtonStyle.primary,
        custom_id="auto_gift_settings",
        emoji=f"{theme.giftIcon}",
        row=0
    )
    async def auto_gift_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.setup_giftcode_auto(interaction)

    @discord.ui.button(
        label="Redemption Priority",
        style=discord.ButtonStyle.primary,
        custom_id="redemption_priority",
        emoji=f"{theme.chartIcon}",
        row=0
    )
    async def redemption_priority_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_redemption_priority(interaction)

    @discord.ui.button(
        label="Channel History Scan",
        style=discord.ButtonStyle.secondary,
        custom_id="channel_history_scan",
        emoji=f"{theme.searchIcon}",
        row=1
    )
    async def channel_history_scan_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.channel_history_scan(interaction)

    @discord.ui.button(
        label="CAPTCHA Settings",
        style=discord.ButtonStyle.secondary,
        custom_id="captcha_settings",
        emoji=f"{theme.settingsIcon}",
        row=1
    )
    async def captcha_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_ocr_settings(interaction)

    @discord.ui.button(
        label="Back",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_main",
        emoji=f"{theme.backIcon}",
        row=2
    )
    async def back_to_main_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return
        await self.cog.show_gift_menu(interaction)
