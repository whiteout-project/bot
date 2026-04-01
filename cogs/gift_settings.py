"""Gift code settings — OCR configuration, test FID management, redemption priority, and auto-redemption setup."""

import discord
import sqlite3
import asyncio
import time
import base64
import re
import os
import logging
import requests
import json
import traceback

from .pimp_my_bot import theme, safe_edit_message
from .gift_captchasolver import GiftCaptchaSolver
from .alliance_member_operations import AllianceSelectView

logger = logging.getLogger('gift')


# ---------------------------------------------------------------------------
# Standalone functions (converted from cog methods: self -> cog)
# ---------------------------------------------------------------------------

async def verify_test_fid(cog, fid):
    """
    Verify that a ID is valid by attempting to login to the account.

    Args:
        fid (str): The ID to verify

    Returns:
        tuple: (is_valid, message) where is_valid is a boolean and message is a string
    """
    session = None
    try:
        cog.logger.info(f"Verifying test ID: {fid}")

        session, response_stove_info = await cog.get_stove_info_wos(fid)

        try:
            player_info_json = response_stove_info.json()
        except json.JSONDecodeError:
            cog.logger.error(f"Invalid JSON response when verifying ID {fid}")
            return False, "Invalid response from server"

        login_successful = player_info_json.get("msg") == "success"

        if login_successful:
            try:
                nickname = player_info_json.get("data", {}).get("nickname", "Unknown")
                furnace_lv = player_info_json.get("data", {}).get("stove_lv", "Unknown")
                cog.logger.info(f"Test ID {fid} is valid. Nickname: {nickname}, Level: {furnace_lv}")
                return True, "Valid account"
            except Exception as e:
                cog.logger.exception(f"Error parsing player info for ID {fid}: {e}")
                return True, "Valid account (but error getting details)"
        else:
            error_msg = player_info_json.get("msg", "Unknown error")
            cog.logger.info(f"Test ID {fid} is invalid. Error: {error_msg}")
            return False, f"Login failed: {error_msg}"

    except requests.exceptions.ConnectionError:
        cog.logger.warning(f"Connection error verifying test ID {fid}. Check bot connectivity to the WOS Gift Code API.")
        return False, "Connection error: WOS API unavailable"
    except requests.exceptions.Timeout:
        cog.logger.warning(f"Timeout verifying test ID {fid}. Check bot connectivity to the WOS Gift Code API.")
        return False, "Connection error: Request timed out"
    except requests.exceptions.RequestException as e:
        cog.logger.warning(f"Request error verifying test ID {fid}: {type(e).__name__}")
        return False, f"Connection error: {type(e).__name__}"
    except Exception as e:
        cog.logger.exception(f"Error verifying test ID {fid}: {e}")
        return False, f"Verification error: {str(e)}"
    finally:
        if session:
            session.close()


async def update_test_fid(cog, new_fid):
    """
    Update the test ID in the database.

    Args:
        new_fid (str): The new test ID

    Returns:
        bool: True if update was successful, False otherwise
    """
    try:
        cog.logger.info(f"Updating test ID to: {new_fid}")

        cog.settings_cursor.execute("""
            INSERT INTO test_fid_settings (test_fid) VALUES (?)
        """, (new_fid,))
        cog.settings_conn.commit()

        cog.logger.info(f"Test ID updated successfully to {new_fid}")
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
        str: The current test ID, or the default "244886619" if not found
    """
    try:
        cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
        result = cog.settings_cursor.fetchone()
        return result[0] if result else "244886619"
    except Exception as e:
        cog.logger.exception(f"Error getting test ID: {e}")
        return "244886619"


async def get_validation_fid(cog):
    """Get the best available ID for gift code validation.

    Hierarchy:
    1. Configured test ID (if valid)
    2. Random alliance member ID (if no test ID)
    3. Relo default ID (244886619) as fallback

    Returns:
        tuple: (fid, source) where source is 'test_fid', 'alliance_member', or 'default'
    """
    try:
        # First try: Use configured test ID if it's valid
        test_fid = get_test_fid(cog)

        # Check if test ID is actually configured (not default)
        cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
        result = cog.settings_cursor.fetchone()

        if result and result[0] != "244886619":
            # Test ID is configured, verify it's valid
            is_valid, _ = await verify_test_fid(cog, test_fid)
            if is_valid:
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
        cog.logger.info("No alliance members found, using default ID for validation: 244886619")
        return "244886619", 'default'

    except Exception as e:
        cog.logger.exception(f"Error in get_validation_fid: {e}")
        return "244886619", 'default'


async def show_ocr_settings(cog, interaction: discord.Interaction):
        """Show OCR settings menu."""
        try:
            cog.settings_cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
            admin_info = cog.settings_cursor.fetchone()

            if not admin_info or admin_info[0] != 1:
                error_msg = f"{theme.deniedIcon} Only global administrators can access OCR settings."
                if interaction.response.is_done():
                    await interaction.followup.send(error_msg, ephemeral=True)
                else:
                    await interaction.response.send_message(error_msg, ephemeral=True)
                return

            cog.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings = cog.settings_cursor.fetchone()

            if not ocr_settings:
                cog.logger.warning("No OCR settings found in DB, inserting defaults.")
                cog.settings_cursor.execute("INSERT INTO ocr_settings (enabled, save_images) VALUES (1, 0)")
                cog.settings_conn.commit()
                ocr_settings = (1, 0)

            enabled, save_images_setting = ocr_settings
            current_test_fid = get_test_fid(cog)

            onnx_available = False
            solver_status_msg = "N/A"
            if cog.captcha_solver:
                if cog.captcha_solver.is_initialized:
                    onnx_available = True
                    solver_status_msg = "Initialized & Ready"
                elif hasattr(cog.captcha_solver, 'is_initialized'):
                    onnx_available = True
                    solver_status_msg = "Initialization Failed (Check Logs)"
                else:
                    solver_status_msg = "Error (Instance missing flags)"
            else:
                try:
                    # Suppress ONNX C++ GPU warning (writes to fd 2, not sys.stderr)
                    import sys, os as _os
                    _fd, _null = sys.stderr.fileno(), _os.open(_os.devnull, _os.O_WRONLY)
                    _bak = _os.dup(_fd); _os.dup2(_null, _fd); _os.close(_null)
                    import onnxruntime
                    _os.dup2(_bak, _fd); _os.close(_bak)
                    onnx_available = True
                    solver_status_msg = "Disabled or Init Failed"
                except ImportError:
                    onnx_available = False
                    solver_status_msg = "onnxruntime library missing"

            save_options_text = {
                0: f"{theme.deniedIcon} None", 1: f"{theme.warnIcon} Failed Only", 2: f"{theme.verifiedIcon} Success Only", 3: f"{theme.saveIcon} All"
            }
            save_images_display = save_options_text.get(save_images_setting, f"Unknown ({save_images_setting})")

            embed = discord.Embed(
                title=f"{theme.searchIcon} CAPTCHA Solver Settings (ONNX)",
                description=(
                    f"Configure the automatic CAPTCHA solver for gift code redemption.\n\n"
                    f"**Current Settings**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.robotIcon} **OCR Enabled:** {f'{theme.verifiedIcon} Yes' if enabled == 1 else f'{theme.deniedIcon} No'}\n"
                    f"{theme.saveIcon} **Save CAPTCHA Images:** {save_images_display}\n"
                    f"{theme.fidIcon} **Test ID:** `{current_test_fid}`\n"
                    f"{theme.giftIcon} **ONNX Runtime:** {f'{theme.verifiedIcon} Found' if onnx_available else f'{theme.deniedIcon} Missing'}\n"
                    f"{theme.settingsIcon} **Solver Status:** `{solver_status_msg}`\n"
                    f"{theme.lowerDivider}\n"
                ),
                color=theme.emColor1
            )

            if not onnx_available:
                embed.add_field(
                    name=f"{theme.warnIcon} Missing Library",
                    value=(
                        "ONNX Runtime and required libraries are needed for CAPTCHA solving.\n"
                        "The model files must be in the bot/models/ directory.\n"
                        "Try installing dependencies:\n"
                        "```pip install onnxruntime pillow numpy\n"
                    ), inline=False
                )

            stats_lines = []
            stats_lines.append("**Captcha Solver (Raw Format):**")
            ocr_calls = cog.processing_stats['ocr_solver_calls']
            ocr_valid = cog.processing_stats['ocr_valid_format']
            ocr_format_rate = (ocr_valid / ocr_calls * 100) if ocr_calls > 0 else 0
            stats_lines.append(f"• Solver Calls: `{ocr_calls}`")
            stats_lines.append(f"• Valid Format Returns: `{ocr_valid}` ({ocr_format_rate:.1f}%)")

            stats_lines.append("\n**Redemption Process (Server Side):**")
            submissions = cog.processing_stats['captcha_submissions']
            server_success = cog.processing_stats['server_validation_success']
            server_fail = cog.processing_stats['server_validation_failure']
            total_server_val = server_success + server_fail
            server_pass_rate = (server_success / total_server_val * 100) if total_server_val > 0 else 0
            stats_lines.append(f"• Captcha Submissions: `{submissions}`")
            stats_lines.append(f"• Server Validation Success: `{server_success}`")
            stats_lines.append(f"• Server Validation Failure: `{server_fail}`")
            stats_lines.append(f"• Server Pass Rate: `{server_pass_rate:.1f}%`")

            total_fids = cog.processing_stats['total_fids_processed']
            total_time = cog.processing_stats['total_processing_time']
            avg_time = (total_time / total_fids if total_fids > 0 else 0)
            stats_lines.append(f"• Avg. ID Processing Time: `{avg_time:.2f}s` (over `{total_fids}` IDs)")

            embed.add_field(
                name=f"{theme.chartIcon} Processing Statistics (Since Bot Start)",
                value="\n".join(stats_lines),
                inline=False
            )

            embed.add_field(
                name=f"{theme.warnIcon} Important Note",
                value="Saving images (especially 'All') can consume significant disk space over time.",
                inline=False
            )

            view = OCRSettingsView(cog, ocr_settings, onnx_available)

            if interaction.response.is_done():
                try:
                    await interaction.edit_original_response(embed=embed, view=view)
                except discord.NotFound:
                    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                except Exception as e_edit:
                    cog.logger.exception(f"Error editing original response in show_ocr_settings: {e_edit}")
                    await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            else:
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except sqlite3.Error as db_err:
            cog.logger.exception(f"Database error in show_ocr_settings: {db_err}")
            error_message = f"{theme.deniedIcon} A database error occurred while loading OCR settings."
            if interaction.response.is_done(): await interaction.followup.send(error_message, ephemeral=True)
            else: await interaction.response.send_message(error_message, ephemeral=True)
        except Exception as e:
            cog.logger.exception(f"Error showing OCR settings: {e}")
            traceback.print_exc()
            error_message = f"{theme.deniedIcon} An unexpected error occurred while loading OCR settings."
            if interaction.response.is_done():
                await interaction.followup.send(error_message, ephemeral=True)
            else:
                await interaction.response.send_message(error_message, ephemeral=True)


async def update_ocr_settings(cog, interaction, enabled=None, save_images=None):
    """Update OCR settings in the database and reinitialize the solver if needed."""
    try:
        cog.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
        current_settings = cog.settings_cursor.fetchone()
        if not current_settings:
            current_settings = (1, 0)

        current_enabled, current_save_images = current_settings

        target_enabled = enabled if enabled is not None else current_enabled
        target_save_images = save_images if save_images is not None else current_save_images

        cog.settings_cursor.execute("""
            UPDATE ocr_settings SET enabled = ?, save_images = ?
            WHERE id = (SELECT MAX(id) FROM ocr_settings)
            """, (target_enabled, target_save_images))
        if cog.settings_cursor.rowcount == 0:
            cog.settings_cursor.execute("""
                INSERT INTO ocr_settings (enabled, save_images) VALUES (?, ?)
                """, (target_enabled, target_save_images))
        cog.settings_conn.commit()
        cog.logger.info(f"GiftOps: Updated OCR settings in DB -> Enabled={target_enabled}, SaveImages={target_save_images}")

        message_suffix = "Settings updated."
        reinitialize_solver = False

        if enabled is not None and enabled != current_enabled:
            reinitialize_solver = True
            message_suffix = f"Solver has been {'enabled' if target_enabled == 1 else 'disabled'}."

        if save_images is not None and cog.captcha_solver and cog.captcha_solver.is_initialized:
            cog.captcha_solver.save_images_mode = target_save_images
            cog.logger.info(f"GiftOps: Updated live captcha_solver.save_images_mode to {target_save_images}")
            if not reinitialize_solver:
                message_suffix = "Image saving preference updated."

        if reinitialize_solver:
            cog.captcha_solver = None
            if target_enabled == 1:
                cog.logger.info("GiftOps: OCR is being enabled/reinitialized...")
                try:
                    cog.captcha_solver = GiftCaptchaSolver(save_images=target_save_images)
                    if cog.captcha_solver.is_initialized:
                        cog.logger.info("GiftOps: ONNX solver reinitialized successfully.")
                        message_suffix += " Solver reinitialized."
                    else:
                        cog.logger.error("GiftOps: ONNX solver FAILED to reinitialize.")
                        message_suffix += " Solver reinitialization failed."
                        cog.captcha_solver = None
                        return False, f"CAPTCHA solver settings updated. {message_suffix}"
                except ImportError as imp_err:
                    cog.logger.exception(f"GiftOps: ERROR - Reinitialization failed: Missing library {imp_err}")
                    message_suffix += f" Solver initialization failed (Missing Library: {imp_err})."
                    cog.captcha_solver = None
                    return False, f"CAPTCHA solver settings updated. {message_suffix}"
                except Exception as e:
                    cog.logger.exception(f"GiftOps: ERROR - Reinitialization failed: {e}")
                    message_suffix += f" Solver initialization failed ({e})."
                    cog.captcha_solver = None
                    return False, f"CAPTCHA solver settings updated. {message_suffix}"
            else:
                cog.logger.info("GiftOps: OCR disabled, solver instance removed/kept None.")

        return True, f"CAPTCHA solver settings: {message_suffix}"

    except sqlite3.Error as db_err:
        cog.logger.exception(f"Database error updating OCR settings: {db_err}")
        return False, f"Database error updating OCR settings: {db_err}"
    except Exception as e:
        cog.logger.exception(f"Unexpected error updating OCR settings: {e}")
        return False, f"Unexpected error updating OCR settings: {e}"


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

class TestIDModal(discord.ui.Modal, title="Change Test ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

        try:
            self.cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.cog.settings_cursor.fetchone()
            current_fid = result[0] if result else "244886619"
        except Exception:
            current_fid = "244886619"

        self.test_fid = discord.ui.TextInput(
            label="Enter New Player ID",
            placeholder="Example: 244886619",
            default=current_fid,
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.test_fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer the response since we'll make an API call to validate
            await interaction.response.defer(ephemeral=True)

            new_fid = self.test_fid.value.strip()

            if not new_fid.isdigit():
                await interaction.followup.send(f"{theme.deniedIcon} Invalid ID format. Please enter a numeric ID.", ephemeral=True)
                return

            is_valid, message = await self.cog.verify_test_fid(new_fid)

            if is_valid:
                success = await self.cog.update_test_fid(new_fid)

                if success:
                    embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Test ID Updated",
                        description=(
                            f"**Test ID Configuration**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.fidIcon} **ID:** `{new_fid}`\n"
                            f"{theme.verifiedIcon} **Status:** Validated\n"
                            f"{theme.editListIcon} **Action:** Updated in database\n"
                            f"{theme.lowerDivider}\n"
                        ),
                        color=theme.emColor3
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)

                    await self.cog.show_ocr_settings(interaction)
                else:
                    await interaction.followup.send(f"{theme.deniedIcon} Failed to update test ID in database. Check logs for details.", ephemeral=True)
            else:
                embed = discord.Embed(
                    title=f"{theme.deniedIcon} Invalid Test ID",
                    description=(
                        f"**Test ID Validation**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.fidIcon} **ID:** `{new_fid}`\n"
                        f"{theme.deniedIcon} **Status:** Invalid ID\n"
                        f"{theme.editListIcon} **Reason:** {message}\n"
                        f"{theme.lowerDivider}\n"
                    ),
                    color=theme.emColor2
                )
                await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            self.cog.logger.exception(f"Error updating test ID: {e}")
            await interaction.followup.send(f"{theme.deniedIcon} An error occurred: {str(e)}", ephemeral=True)


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


class OCRSettingsView(discord.ui.View):
    def __init__(self, cog, ocr_settings, onnx_available):
        super().__init__(timeout=7200)
        self.cog = cog
        self.enabled = ocr_settings[0]
        self.save_images_setting = ocr_settings[1]
        self.onnx_available = onnx_available
        self.disable_controls = not onnx_available

        # Row 0: Enable/Disable Button, Test Button
        self.enable_ocr_button_item = discord.ui.Button(
            emoji=f"{theme.verifiedIcon}" if self.enabled == 1 else "🚫",
            custom_id="enable_ocr", row=0,
            label="Disable CAPTCHA Solver" if self.enabled == 1 else "Enable CAPTCHA Solver",
            style=discord.ButtonStyle.danger if self.enabled == 1 else discord.ButtonStyle.success,
            disabled=self.disable_controls
        )
        self.enable_ocr_button_item.callback = self.enable_ocr_button
        self.add_item(self.enable_ocr_button_item)

        self.test_ocr_button_item = discord.ui.Button(
            label="Test CAPTCHA Solver", style=discord.ButtonStyle.secondary, emoji=f"{theme.testIcon}",
            custom_id="test_ocr", row=0,
            disabled=self.disable_controls
        )
        self.test_ocr_button_item.callback = self.test_ocr_button
        self.add_item(self.test_ocr_button_item)

        # Add the Change Test ID Button
        self.change_test_fid_button_item = discord.ui.Button(
            label="Change Test ID", style=discord.ButtonStyle.primary, emoji=f"{theme.refreshIcon}",
            custom_id="change_test_fid", row=0,
            disabled=self.disable_controls
        )
        self.change_test_fid_button_item.callback = self.change_test_fid_button
        self.add_item(self.change_test_fid_button_item)

        # Add the Clear Redemption Cache Button
        self.clear_cache_button_item = discord.ui.Button(
            label="Clear Redemption Cache", style=discord.ButtonStyle.danger, emoji=f"{theme.trashIcon}",
            custom_id="clear_redemption_cache", row=1,
            disabled=self.disable_controls
        )
        self.clear_cache_button_item.callback = self.clear_redemption_cache_button
        self.add_item(self.clear_cache_button_item)

        # Row 2: Image Save Select Menu
        self.image_save_select_item = discord.ui.Select(
            placeholder="Select Captcha Image Saving Option",
            min_values=1, max_values=1, row=2, custom_id="image_save_select",
            options=[
                discord.SelectOption(label="Don't Save Any Images", value="0", description="Fastest, no disk usage"),
                discord.SelectOption(label="Save Only Failed Captchas", value="1", description="For debugging server rejects"),
                discord.SelectOption(label="Save Only Successful Captchas", value="2", description="To see what worked"),
                discord.SelectOption(label="Save All Captchas (High Disk Usage!)", value="3", description="Comprehensive debugging")
            ],
            disabled=self.disable_controls
        )
        for option in self.image_save_select_item.options:
            option.default = (str(self.save_images_setting) == option.value)
        self.image_save_select_item.callback = self.image_save_select_callback
        self.add_item(self.image_save_select_item)

    async def change_test_fid_button(self, interaction: discord.Interaction):
        """Handle the change test ID button click."""
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        await interaction.response.send_modal(TestIDModal(self.cog))

    async def enable_ocr_button(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        new_enabled = 1 if self.enabled == 0 else 0
        success, message = await self.cog.update_ocr_settings(interaction, enabled=new_enabled)
        await self.cog.show_ocr_settings(interaction)

    async def test_ocr_button(self, interaction: discord.Interaction):
        logger = self.cog.logger
        user_id = interaction.user.id
        current_time = time.time()

        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        if not self.cog.captcha_solver or not self.cog.captcha_solver.is_initialized:
            await interaction.response.send_message(f"{theme.deniedIcon} CAPTCHA solver is not initialized. Ensure OCR is enabled.", ephemeral=True)
            return

        last_test_time = self.cog.test_captcha_cooldowns.get(user_id, 0)
        if current_time - last_test_time < self.cog.test_captcha_delay:
            remaining_time = int(self.cog.test_captcha_delay - (current_time - last_test_time))
            await interaction.response.send_message(f"{theme.deniedIcon} Please wait {remaining_time} more seconds before testing again.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        logger.info(f"[Test Button] User {user_id} triggered test.")
        self.cog.test_captcha_cooldowns[user_id] = current_time

        captcha_image_base64 = None
        image_bytes = None
        error = None
        captcha_code = None
        success = False
        method = "N/A"
        confidence = 0.0
        solve_duration = 0.0
        test_fid = self.cog.get_test_fid()
        session = None

        try:
            logger.info(f"[Test Button] First logging in with test ID {test_fid}...")
            session, response_stove_info = await self.cog.get_stove_info_wos(test_fid)

            try:
                player_info_json = response_stove_info.json()
                if player_info_json.get("msg") != "success":
                    logger.error(f"[Test Button] Login failed for test ID {test_fid}: {player_info_json.get('msg')}")
                    await interaction.followup.send(f"{theme.deniedIcon} Login failed with test ID {test_fid}. Please check if the ID is valid.", ephemeral=True)
                    return
                logger.info(f"[Test Button] Successfully logged in with test ID {test_fid}")
            except Exception as json_err:
                logger.error(f"[Test Button] Error parsing login response: {json_err}")
                await interaction.followup.send(f"{theme.deniedIcon} Error processing login response.", ephemeral=True)
                return

            logger.info(f"[Test Button] Fetching captcha for test ID {test_fid} using established session...")
            captcha_image_base64, error = await self.cog.fetch_captcha(test_fid, session)
            logger.info(f"[Test Button] Captcha fetch result: Error='{error}', HasImage={captcha_image_base64 is not None}")

            if error:
                await interaction.followup.send(f"{theme.deniedIcon} Error fetching test captcha from the API: `{error}`", ephemeral=True)
                return

            if captcha_image_base64:
                try:
                    if captcha_image_base64.startswith("data:image"):
                        img_b64_data = captcha_image_base64.split(",", 1)[1]
                    else:
                        img_b64_data = captcha_image_base64
                    image_bytes = base64.b64decode(img_b64_data)
                    logger.info("[Test Button] Successfully decoded base64 image.")
                except Exception as decode_err:
                    logger.error(f"[Test Button] Failed to decode base64 image: {decode_err}")
                    await interaction.followup.send(f"{theme.deniedIcon} Failed to decode captcha image data.", ephemeral=True)
                    return
            else:
                logger.error("[Test Button] Captcha fetch returned no image data.")
                await interaction.followup.send(f"{theme.deniedIcon} Failed to retrieve captcha image data from API.", ephemeral=True)
                return

            if image_bytes:
                logger.info("[Test Button] Solving fetched captcha...")
                start_solve_time = time.time()
                captcha_code, success, method, confidence, _ = await self.cog.captcha_solver.solve_captcha(
                    image_bytes, fid=f"test-{user_id}", attempt=0
                )
                solve_duration = time.time() - start_solve_time
                log_confidence_str = f'{confidence:.2f}' if isinstance(confidence, float) else 'N/A'
                logger.info(f"[Test Button] Solve result: Success={success}, Code='{captcha_code}', Method='{method}', Conf={log_confidence_str}. Duration: {solve_duration:.2f}s")
            else:
                 logger.error("[Test Button] Logic error: image_bytes is None before solving.")
                 await interaction.followup.send(f"{theme.deniedIcon} Internal error before solving captcha.", ephemeral=True)
                 return

            confidence_str = f'{confidence:.2f}' if isinstance(confidence, float) else 'N/A'
            embed = discord.Embed(
                title=f"{theme.searchIcon} CAPTCHA Solver Test Results (ONNX)",
                description=(
                    f"**Test Summary**\n{theme.upperDivider}\n"
                    f"{theme.robotIcon} **OCR Success:** {f'{theme.verifiedIcon} Yes' if success else f'{theme.deniedIcon} No'}\n"
                    f"{theme.searchIcon} **Recognized Code:** `{captcha_code if success and captcha_code else 'N/A'}`\n"
                    f"{theme.chartIcon} **Confidence:** `{confidence_str}`\n"
                    f"{theme.timeIcon} **Solve Time:** `{solve_duration:.2f}s`\n"
                    f"{theme.lowerDivider}\n"
                ), color=theme.emColor3 if success else discord.Color.red()
            )

            save_path_str = None
            save_error_str = None
            try:
                self.cog.settings_cursor.execute("SELECT save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
                save_setting_row = self.cog.settings_cursor.fetchone()
                current_save_mode = save_setting_row[0] if save_setting_row else 0

                should_save_img = False
                save_tag = "UNKNOWN"
                if success and current_save_mode in [2, 3]:
                    should_save_img = True
                    save_tag = captcha_code if captcha_code else "SUCCESS_NOCDE"
                elif not success and current_save_mode in [1, 3]:
                    should_save_img = True
                    save_tag = "FAILED"

                if should_save_img and image_bytes:
                    logger.info(f"[Test Button] Attempting to save image based on mode {current_save_mode}. Status success={success}, tag='{save_tag}'")
                    captcha_dir = self.cog.captcha_solver.captcha_dir
                    safe_tag = re.sub(r'[\\/*?:"<>|]', '_', save_tag)
                    timestamp = int(time.time())

                    if success:
                         base_filename = f"{safe_tag}.png"
                    else:
                         base_filename = f"FAIL_{safe_tag}_{timestamp}.png"

                    test_path = os.path.join(captcha_dir, base_filename)

                    counter = 1
                    orig_path = test_path
                    while os.path.exists(test_path) and counter <= 100:
                        name, ext = os.path.splitext(orig_path)
                        test_path = f"{name}_{counter}{ext}"
                        counter += 1

                    if counter > 100:
                        save_error_str = f"Could not find unique filename for {base_filename} after 100 tries."
                        logger.warning(f"[Test Button] {save_error_str}")
                    else:
                        os.makedirs(captcha_dir, exist_ok=True)
                        with open(test_path, "wb") as f:
                            f.write(image_bytes)
                        save_path_str = os.path.basename(test_path)
                        logger.info(f"[Test Button] Saved test captcha image to {test_path}")

            except Exception as img_save_err:
                logger.exception(f"[Test Button] Error saving test image: {img_save_err}")
                save_error_str = f"Error during saving: {img_save_err}"

            if save_path_str:
                embed.add_field(name="📸 Captcha Image Saved", value=f"`{save_path_str}` in `{os.path.relpath(self.cog.captcha_solver.captcha_dir)}`", inline=False)
            elif save_error_str:
                embed.add_field(name=f"{theme.warnIcon} Image Save Error", value=save_error_str, inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"[Test Button] Test completed for user {user_id}.")

        except requests.exceptions.ConnectionError:
            logger.warning(f"[Test Button] Connection error for user {user_id}. WOS API may be unavailable.")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: Unable to reach WOS API. Please check your internet connection.", ephemeral=True)
            except Exception:
                pass
        except requests.exceptions.Timeout:
            logger.warning(f"[Test Button] Timeout for user {user_id}. WOS API may be slow.")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: Request timed out. WOS API may be overloaded or unavailable.", ephemeral=True)
            except Exception:
                pass
        except requests.exceptions.RequestException as e:
            logger.warning(f"[Test Button] Request error for user {user_id}: {type(e).__name__}")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: {type(e).__name__}. Please try again later.", ephemeral=True)
            except Exception:
                pass
        except Exception as e:
            logger.exception(f"[Test Button] UNEXPECTED Error during test for user {user_id}: {e}")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} An unexpected error occurred during the test: `{e}`. Please check the bot logs.", ephemeral=True)
            except Exception as followup_err:
                logger.error(f"[Test Button] Failed to send final error followup to user {user_id}: {followup_err}")
        finally:
            if session:
                session.close()

    async def clear_redemption_cache_button(self, interaction: discord.Interaction):
        """Handle the clear redemption cache button click."""
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        # Create confirmation embed
        embed = discord.Embed(
            title=f"{theme.warnIcon} Clear Redemption Cache",
            description=(
                "This will **permanently delete** all gift code redemption records from the database.\n\n"
                "**What this does:**\n"
                "• Removes all entries from the `user_giftcodes` table\n"
                "• Allows users to attempt redeeming gift codes again\n"
                "• Useful for development testing and image collection\n\n"
                "**Warning:** This action cannot be undone!"
            ),
            color=discord.Color.orange()
        )

        # Get current count for display
        try:
            self.cog.cursor.execute("SELECT COUNT(*) FROM user_giftcodes")
            current_count = self.cog.cursor.fetchone()[0]
            embed.add_field(
                name=f"{theme.chartIcon} Current Records",
                value=f"{current_count:,} redemption records will be deleted",
                inline=False
            )
        except Exception as e:
            self.cog.logger.error(f"Error getting user_giftcodes count: {e}")
            embed.add_field(
                name=f"{theme.chartIcon} Current Records",
                value="Unable to count records",
                inline=False
            )

        # Create confirmation view
        confirm_view = ClearCacheConfirmView(self.cog)
        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

    async def image_save_select_callback(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            selected_value = int(interaction.data["values"][0])

            success, message = await self.cog.update_ocr_settings(
                interaction=interaction,
                save_images=selected_value
            )

            if success:
                self.save_images_setting = selected_value
                for option in self.image_save_select_item.options:
                    option.default = (str(self.save_images_setting) == option.value)
            else:
                await interaction.followup.send(f"{theme.deniedIcon} {message}", ephemeral=True)

        except ValueError:
            await interaction.followup.send(f"{theme.deniedIcon} Invalid selection value for image saving.", ephemeral=True)
        except Exception as e:
            self.cog.logger.exception("Error processing image save selection in OCRSettingsView.")
            await interaction.followup.send(f"{theme.deniedIcon} An error occurred while updating image saving settings.", ephemeral=True)
