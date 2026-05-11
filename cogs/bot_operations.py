"""
Core bot operations. Admin management, alliance control messages, and bot settings.
"""
import discord
from discord.ext import commands
import os
import sys
import sqlite3
import asyncio
import requests
import logging
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

logger = logging.getLogger('bot')


class _UpdateAndRestartView(discord.ui.View):
    """Single button on the Check-for-Updates ephemeral. Triggers a restart
    that filters --no-update from the relaunch args so the startup updater
    can install the pending release."""

    def __init__(self, bot):
        super().__init__(timeout=7200)
        self.bot = bot

    @discord.ui.button(
        label="Update & Restart",
        emoji=f"{theme.refreshIcon}",
        style=discord.ButtonStyle.danger,
    )
    async def update_and_restart(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_admin or not is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only Global Admins can update the bot.",
                ephemeral=True,
            )
            return
        health_cog = self.bot.get_cog("BotHealth")
        if not health_cog:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Bot Health module not found — cannot restart.",
                ephemeral=True,
            )
            return
        await interaction.response.defer()
        await health_cog.perform_restart(interaction, allow_update=True)


class BotOperations(commands.Cog):
    def __init__(self, bot, conn):
        self.bot = bot
        self.conn = conn
        self.settings_db = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.settings_cursor = self.settings_db.cursor()
        self.alliance_db = sqlite3.connect('db/alliance.sqlite', timeout=30.0, check_same_thread=False)
        self.c_alliance = self.alliance_db.cursor()
        self.setup_database()

    def get_current_version(self):
        """Get current version from version file"""
        try:
            if os.path.exists("version"):
                with open("version", "r") as f:
                    return f.read().strip()
            return "v0.0.0"
        except Exception:
            return "v0.0.0"
        
    def setup_database(self):
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS admin (
                    id INTEGER PRIMARY KEY,
                    is_initial INTEGER DEFAULT 0
                )
            """)

            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS adminserver (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin INTEGER NOT NULL,
                    alliances_id INTEGER NOT NULL,
                    FOREIGN KEY (admin) REFERENCES admin(id),
                    UNIQUE(admin, alliances_id)
                )
            """)

            # Migration: add is_owner column for the explicit Bot Owner anchor.
            # is_owner=1 means THE bot owner (recovery anchor, can't be removed
            # except via explicit Transfer Owner). is_initial=1 means Global
            # tier (multiple allowed). is_owner always implies is_initial.
            self.settings_cursor.execute("PRAGMA table_info(admin)")
            admin_cols = [row[1] for row in self.settings_cursor.fetchall()]
            if 'is_owner' not in admin_cols:
                self.settings_cursor.execute(
                    "ALTER TABLE admin ADD COLUMN is_owner INTEGER DEFAULT 0"
                )

            self.settings_db.commit()
            self._backfill_owner_if_needed()

        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            print(f"Error setting up database: {e}")

    def _backfill_owner_if_needed(self):
        """One-shot owner backfill on startup.

        - If any admin already has is_owner=1: no-op (idempotent).
        - If exactly one admin has is_initial=1: promote that admin to owner.
        - Otherwise (0 or >=2 globals, no owner yet): leave unset, log a
          console banner so the actual host knows to claim ownership the
          first time they open Settings → Permissions.
        """
        self.settings_cursor.execute("SELECT 1 FROM admin WHERE is_owner = 1 LIMIT 1")
        if self.settings_cursor.fetchone():
            return

        self.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
        globals_ = [row[0] for row in self.settings_cursor.fetchall()]

        if len(globals_) == 1:
            self.settings_cursor.execute(
                "UPDATE admin SET is_owner = 1 WHERE id = ?", (globals_[0],)
            )
            self.settings_db.commit()
            logger.info(f"[OWNER-CLAIM] Auto-promoted single Global admin {globals_[0]} to Bot Owner.")
            print(f"[OWNER-CLAIM] Auto-promoted single Global admin {globals_[0]} to Bot Owner.")
        elif len(globals_) >= 2:
            msg = (
                f"[OWNER-CLAIM] {len(globals_)} Global admins detected, no Bot Owner is set. "
                f"The first Global admin to open Settings -> Permissions and click 'Claim Bot Owner' "
                f"becomes the permanent owner."
            )
            logger.warning(msg)
            print(msg)
        # len(globals_) == 0: brand-new install. The first admin created via
        # the new Add Admin flow gets is_initial=1, is_owner=1 atomically.

    def cog_unload(self):
        """Close database connections when cog is unloaded."""
        try:
            self.settings_db.close()
            self.alliance_db.close()
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if not interaction.type == discord.InteractionType.component:
            return

        custom_id = interaction.data.get("custom_id", "")
        
        if custom_id == "bot_operations":
            return
        
        if custom_id == "alliance_control_messages":
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.warnIcon} The bot-wide toggle was replaced by a per-alliance "
                    f"\"Show Sync Messages\" setting under Sync Settings.",
                    ephemeral=True,
                )
            return

        if custom_id in (
            "assign_alliance", "add_admin", "remove_admin",
            "view_admin_permissions", "view_administrators",
        ):
            # The old Permissions UI was rebuilt as a single admin-list flow
            # (see show_permissions in bot_main_menu.py). These custom_ids
            # only fire from stale persisted messages — point users at the
            # new flow instead of erroring.
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.warnIcon} The Permissions menu was redesigned. "
                    f"Please reopen Settings → Permissions to use the new flow.",
                    ephemeral=True,
                )
            return

        elif custom_id in ["main_menu", "bot_status", "bot_settings"]:
            try:
                if custom_id == "main_menu":
                    try:
                        main_menu_cog = self.bot.get_cog("MainMenu")
                        if main_menu_cog:
                            await main_menu_cog.show_main_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon} An error occurred while returning to main menu.",
                                ephemeral=True
                            )
                    except Exception as e:
                        print(f"[ERROR] Main Menu error in bot operations: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while returning to main menu.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while returning to main menu.",
                                ephemeral=True
                            )

            except Exception as e:
                if not interaction.response.is_done():
                    print(f"Error processing {custom_id}: {e}")
                    await interaction.response.send_message(
                        "An error occurred while processing your request.",
                        ephemeral=True
                    )
        elif custom_id == "check_updates":
            try:
                is_admin, is_global = PermissionManager.is_admin(interaction.user.id)

                if not is_admin or not is_global:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Only global administrators can use this command.",
                        ephemeral=True
                    )
                    return

                current_version, new_version, update_notes, updates_needed = await self.check_for_updates()

                if not current_version or not new_version:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Failed to check for updates. Please try again later.", 
                        ephemeral=True
                    )
                    return

                main_embed = discord.Embed(
                    title=f"{theme.refreshIcon} Bot Update Status",
                    color=theme.emColor1 if not updates_needed else discord.Color.yellow()
                )

                main_embed.add_field(
                    name="Current Version",
                    value=f"`{current_version}`",
                    inline=True
                )

                main_embed.add_field(
                    name="Latest Version",
                    value=f"`{new_version}`",
                    inline=True
                )

                if updates_needed:
                    main_embed.add_field(
                        name="Status",
                        value=f"{theme.refreshIcon} **Update Available**",
                        inline=True
                    )

                    if update_notes:
                        notes_text = "\n".join([f"• {note.lstrip('- *•').strip()}" for note in update_notes[:10]])
                        if len(update_notes) > 10:
                            notes_text += f"\n• ... and more!"
                        
                        main_embed.add_field(
                            name="Release Notes",
                            value=notes_text[:1024],  # Discord field limit
                            inline=False
                        )

                    # Windows hosts don't auto-restart — flag it so the admin
                    # knows manual intervention will be needed after the update.
                    update_help = (
                        f"Click **Update & Restart** below to install the "
                        f"update now. The bot will reconnect automatically."
                    )
                    if sys.platform == 'win32' and not os.path.exists('/.dockerenv'):
                        update_help = (
                            f"Click **Update & Restart** below to install the "
                            f"update now.\n\n{theme.warnIcon} **Windows host "
                            f"detected.** The bot will stop after downloading "
                            f"the update — someone with host access needs to "
                            f"start it again with `python main.py` for the "
                            f"update to finish installing."
                        )
                    main_embed.add_field(
                        name="How to Update",
                        value=update_help,
                        inline=False
                    )
                else:
                    main_embed.add_field(
                        name="Status",
                        value=f"{theme.verifiedIcon} **Up to Date**",
                        inline=True
                    )
                    main_embed.description = "Your bot is running the latest version!"

                view = _UpdateAndRestartView(self.bot) if updates_needed else None
                await interaction.response.send_message(
                    embed=main_embed,
                    view=view,
                    ephemeral=True,
                )

            except Exception as e:
                print(f"Check updates error: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while checking for updates.",
                        ephemeral=True
                    )

    async def confirm_permission_removal(self, admin_id: int, alliance_id: int, confirm_interaction: discord.Interaction):
        try:
            self.settings_cursor.execute("""
                DELETE FROM adminserver 
                WHERE admin = ? AND alliances_id = ?
            """, (admin_id, alliance_id))
            self.settings_db.commit()
            return True
        except Exception as e:
            return False

    async def check_for_updates(self):
        """Check for updates using GitHub releases API"""
        try:
            latest_release_url = "https://api.github.com/repos/whiteout-project/bot/releases/latest"
            
            response = await asyncio.to_thread(requests.get, latest_release_url, timeout=10)
            if response.status_code != 200:
                return None, None, [], False

            latest_release_data = response.json()
            latest_tag = latest_release_data.get("tag_name", "")
            current_version = self.get_current_version()
            
            if not latest_tag:
                return current_version, None, [], False

            updates_needed = current_version != latest_tag
            
            # Parse release notes
            update_notes = []
            release_body = latest_release_data.get("body", "")
            if release_body:
                for line in release_body.split('\n'):
                    line = line.strip()
                    if line and (line.startswith('-') or line.startswith('*') or line.startswith('•')):
                        update_notes.append(line)

            return current_version, latest_tag, update_notes, updates_needed

        except Exception as e:
            print(f"Error checking for updates: {e}")
            return None, None, [], False
    
    async def show_control_settings_menu(self, interaction: discord.Interaction):
        """Show the per-alliance Sync Settings menu (with alliance picker)."""
        try:
            if interaction.guild is None:
                await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
                return

            alliances, _ = PermissionManager.get_admin_alliances(interaction.user.id, interaction.guild.id)

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliances found.",
                    ephemeral=True
                )
                return

            view = SyncSettingsView(self.c_alliance, self.alliance_db, alliances, interaction)
            await view.update_view(interaction)

        except Exception as e:
            print(f"Error in show_sync_settings_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing sync settings.",
                    ephemeral=True
                )

    async def show_control_settings_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: open Sync Settings for a known alliance (no picker)."""
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
                    f"{theme.deniedIcon} Alliance not found.", ephemeral=True
                )
                return
            alliance_name = row[0]

            view = SyncSettingsView(
                self.c_alliance, self.alliance_db,
                [(alliance_id, alliance_name)], interaction,
                locked_alliance_id=alliance_id,
                return_to_hub=True,
            )
            view.selected_alliance = alliance_id
            await view.update_view(interaction)

        except Exception as e:
            print(f"Error in show_control_settings_for: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing sync settings.",
                    ephemeral=True
                )


class SyncSettingsView(discord.ui.View):
    MIN_INTERVAL_MINUTES = 30
    MAX_INTERVAL_MINUTES = 1440  # 24h

    def __init__(self, alliance_cursor, alliance_db, alliances, initial_interaction,
                 *, locked_alliance_id: int | None = None, return_to_hub: bool = False):
        super().__init__(timeout=7200)
        self.alliance_cursor = alliance_cursor
        self.alliance_db = alliance_db
        self.alliances = alliances
        self.locked_alliance_id = locked_alliance_id
        self.return_to_hub = return_to_hub
        self.selected_alliance = locked_alliance_id
        self.interval = 0
        self.start_time = None
        self.auto_remove = False
        self.notify_on_transfer = False
        self.keep_control_log = False
        self.show_sync_message = True
        self.setup_components()

    def setup_components(self):
        self.clear_items()

        if self.locked_alliance_id is None:
            self.alliance_select = discord.ui.Select(
                placeholder="Select an alliance..." if not self.selected_alliance else f"Selected: {next((name for aid, name in self.alliances if aid == self.selected_alliance), 'Unknown')[:50]}",
                options=[
                    discord.SelectOption(
                        label=f"{name[:50]}",
                        value=str(alliance_id),
                        description=f"Alliance ID: {alliance_id}",
                        default=(alliance_id == self.selected_alliance) if self.selected_alliance else False
                    ) for alliance_id, name in self.alliances[:25]
                ],
                row=0
            )
            self.alliance_select.callback = self.alliance_selected
            self.add_item(self.alliance_select)

        if self.selected_alliance:
            self.edit_interval_button = discord.ui.Button(
                label="Edit Interval",
                style=discord.ButtonStyle.primary,
                emoji=f"{theme.refreshIcon}",
                row=1,
            )
            self.edit_interval_button.callback = self.open_interval_modal
            self.add_item(self.edit_interval_button)

            self.edit_start_button = discord.ui.Button(
                label="Edit Start Time",
                style=discord.ButtonStyle.primary,
                emoji=f"{theme.pinIcon}",
                row=1,
            )
            self.edit_start_button.callback = self.open_start_time_modal
            self.add_item(self.edit_start_button)

            self.show_msg_button = discord.ui.Button(
                label=f"Sync Messages: {'On' if self.show_sync_message else 'Off'}",
                style=discord.ButtonStyle.success if self.show_sync_message else discord.ButtonStyle.secondary,
                emoji=f"{theme.messageIcon}",
                row=2,
            )
            self.show_msg_button.callback = self.toggle_show_sync_message
            self.add_item(self.show_msg_button)

            self.keep_log_button = discord.ui.Button(
                label=f"Keep Sync Log: {'On' if self.keep_control_log else 'Off'}",
                style=discord.ButtonStyle.success if self.keep_control_log else discord.ButtonStyle.secondary,
                emoji=f"{theme.listIcon}",
                row=2,
                disabled=not self.show_sync_message,
            )
            self.keep_log_button.callback = self.toggle_keep_control_log
            self.add_item(self.keep_log_button)

            self.auto_remove_button = discord.ui.Button(
                label=f"Auto-Removal: {'On' if self.auto_remove else 'Off'}",
                style=discord.ButtonStyle.success if self.auto_remove else discord.ButtonStyle.secondary,
                emoji=f"{theme.refreshIcon}",
                row=3,
            )
            self.auto_remove_button.callback = self.toggle_auto_removal
            self.add_item(self.auto_remove_button)

            self.notify_button = discord.ui.Button(
                label=f"Notifications: {'On' if self.notify_on_transfer else 'Off'}",
                style=discord.ButtonStyle.success if self.notify_on_transfer else discord.ButtonStyle.secondary,
                emoji=f"{theme.bellIcon}",
                row=3,
                disabled=not self.auto_remove,
            )
            self.notify_button.callback = self.toggle_notifications
            self.add_item(self.notify_button)

        self.back_button = discord.ui.Button(
            label="Back to Hub" if self.return_to_hub else "Back",
            style=discord.ButtonStyle.secondary,
            emoji=f"{theme.backIcon}",
            row=4,
        )
        self.back_button.callback = self.back_to_bot_operations
        self.add_item(self.back_button)

    async def update_view(self, interaction: discord.Interaction):
        if self.selected_alliance:
            alliance_name = next((name for aid, name in self.alliances if aid == self.selected_alliance), "Unknown")
            self.alliance_cursor.execute("""
                SELECT auto_remove_on_transfer, notify_on_transfer, keep_control_log,
                       show_sync_message, interval, start_time
                FROM alliancesettings
                WHERE alliance_id = ?
            """, (self.selected_alliance,))
            result = self.alliance_cursor.fetchone()
            self.auto_remove = bool(result[0]) if result and result[0] is not None else False
            self.notify_on_transfer = bool(result[1]) if result and len(result) > 1 and result[1] is not None else False
            self.keep_control_log = bool(result[2]) if result and len(result) > 2 and result[2] is not None else False
            self.show_sync_message = bool(result[3]) if result and len(result) > 3 and result[3] is not None else True
            self.interval = int(result[4]) if result and len(result) > 4 and result[4] is not None else 0
            self.start_time = result[5] if result and len(result) > 5 and result[5] else None

            show_emoji = theme.verifiedIcon if self.show_sync_message else theme.deniedIcon
            log_emoji = theme.verifiedIcon if self.keep_control_log else theme.trashIcon
            status_emoji = theme.verifiedIcon if self.auto_remove else theme.deniedIcon
            notify_emoji = theme.bellIcon if self.notify_on_transfer else theme.muteIcon

            log_line = (
                "Keep the message after sync finishes"
                if self.keep_control_log
                else "Delete the message after sync finishes"
            )
            log_suffix = "" if self.show_sync_message else " _(turn on Show progress first)_"
            notify_suffix = "" if self.auto_remove else " _(turn on Auto-Removal first)_"

            interval_display = (
                f"`{self.interval} min`" if self.interval > 0 else "`disabled`"
            )
            start_time_display = (
                f"`{self.start_time} UTC`" if self.start_time
                else "_(starts on bot startup)_"
            )

            embed = discord.Embed(
                title=f"{theme.settingsIcon} Sync Settings · {alliance_name}",
                description=(
                    f"{theme.upperDivider}\n"
                    f"**Schedule**\n"
                    f"{theme.refreshIcon} Sync Interval: {interval_display}\n"
                    f"{theme.pinIcon} Start Time: {start_time_display}\n\n"
                    f"**Sync Channel Messages**\n"
                    f"{show_emoji} Show progress message during sync\n"
                    f"{log_emoji} {log_line}{log_suffix}\n\n"
                    f"**State Transfer**\n"
                    f"{status_emoji} Auto-remove members who transfer states\n"
                    f"{notify_emoji} Notify admin when an auto-removal happens{notify_suffix}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )
        else:
            embed = discord.Embed(
                title=f"{theme.settingsIcon} Sync Settings",
                description=(
                    "Pick an alliance from the dropdown to configure:\n"
                    "• Whether the bot posts a sync progress message\n"
                    "• Whether that message is kept or deleted after the sync\n"
                    "• Auto-removal of members who transfer states\n"
                    "• Admin notifications for those removals"
                ),
                color=theme.emColor1
            )
        
        # Update components based on current state
        self.setup_components()
        
        # Edit the message
        if interaction.response.is_done():
            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                embed=embed,
                view=self
            )
        else:
            await interaction.response.edit_message(embed=embed, view=self)
    
    async def alliance_selected(self, interaction: discord.Interaction):
        """Handle alliance selection from dropdown"""
        try:
            self.selected_alliance = int(self.alliance_select.values[0])
            await self.update_view(interaction)
        except Exception as e:
            print(f"Error in alliance_selected: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while selecting the alliance.",
                ephemeral=True
            )
    
    async def toggle_auto_removal(self, interaction: discord.Interaction):
        """Toggle auto-removal setting"""
        try:
            # Toggle the value in database
            new_value = not self.auto_remove
            self.alliance_cursor.execute("""
                UPDATE alliancesettings 
                SET auto_remove_on_transfer = ?
                WHERE alliance_id = ?
            """, (1 if new_value else 0, self.selected_alliance))
            
            # If disabling auto-removal, also disable notifications
            if not new_value:
                self.alliance_cursor.execute("""
                    UPDATE alliancesettings 
                    SET notify_on_transfer = 0
                    WHERE alliance_id = ?
                """, (self.selected_alliance,))
            
            self.alliance_db.commit()
            
            # Update the view
            await self.update_view(interaction)
            
        except Exception as e:
            print(f"Error toggling auto-removal: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while updating the setting.",
                ephemeral=True
            )
    
    async def toggle_notifications(self, interaction: discord.Interaction):
        """Toggle notification setting"""
        try:
            # Toggle the value in database
            new_value = not self.notify_on_transfer
            self.alliance_cursor.execute("""
                UPDATE alliancesettings 
                SET notify_on_transfer = ?
                WHERE alliance_id = ?
            """, (1 if new_value else 0, self.selected_alliance))
            self.alliance_db.commit()
            
            # Update the view
            await self.update_view(interaction)
            
        except Exception as e:
            print(f"Error toggling notifications: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while updating the setting.",
                ephemeral=True
            )
    
    async def toggle_keep_control_log(self, interaction: discord.Interaction):
        """Toggle keep control log setting"""
        try:
            new_value = not self.keep_control_log
            self.alliance_cursor.execute("""
                UPDATE alliancesettings
                SET keep_control_log = ?
                WHERE alliance_id = ?
            """, (1 if new_value else 0, self.selected_alliance))
            self.alliance_db.commit()

            await self.update_view(interaction)

        except Exception as e:
            print(f"Error toggling keep control log: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while updating the setting.",
                ephemeral=True
            )

    async def toggle_show_sync_message(self, interaction: discord.Interaction):
        try:
            new_value = not self.show_sync_message
            self.alliance_cursor.execute(
                "UPDATE alliancesettings SET show_sync_message = ? WHERE alliance_id = ?",
                (1 if new_value else 0, self.selected_alliance),
            )
            self.alliance_db.commit()
            await self.update_view(interaction)
        except Exception as e:
            print(f"Error toggling show_sync_message: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while updating the setting.",
                ephemeral=True,
            )

    async def open_interval_modal(self, interaction: discord.Interaction):
        await interaction.response.send_modal(SyncIntervalModal(self, self.interval))

    async def open_start_time_modal(self, interaction: discord.Interaction):
        await interaction.response.send_modal(StartTimeModal(self, self.start_time))

    async def back_to_bot_operations(self, interaction: discord.Interaction):
        """Return to the Alliance Hub when locked, otherwise the Alliances menu."""
        try:
            main_menu = interaction.client.get_cog("MainMenu")
            if not main_menu:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Unable to return to the Alliances menu.",
                    ephemeral=True,
                )
                return
            if self.return_to_hub and self.locked_alliance_id is not None:
                await main_menu.show_alliance_hub(interaction, self.locked_alliance_id)
            else:
                await main_menu.show_alliance_management(interaction)
        except Exception as e:
            logger.error(f"Error in back_to_bot_operations: {e}")
            print(f"Error in back_to_bot_operations: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred.",
                    ephemeral=True
                )

class SyncIntervalModal(discord.ui.Modal):
    """Numeric sync interval input. Validates min/max/0-as-disabled."""

    def __init__(self, parent_view: SyncSettingsView, current_interval: int):
        super().__init__(title="Edit Sync Interval")
        self.parent_view = parent_view
        self.input = discord.ui.TextInput(
            label=f"Interval ({SyncSettingsView.MIN_INTERVAL_MINUTES}-"
                  f"{SyncSettingsView.MAX_INTERVAL_MINUTES} min, or 0 to disable)",
            placeholder=f"e.g. 60",
            default=str(current_interval),
            required=True,
            max_length=4,
        )
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.input.value.strip()
        try:
            value = int(raw)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Interval must be a whole number.", ephemeral=True
            )
            return
        if value < 0:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Interval cannot be negative.", ephemeral=True
            )
            return
        if value != 0 and (
            value < SyncSettingsView.MIN_INTERVAL_MINUTES
            or value > SyncSettingsView.MAX_INTERVAL_MINUTES
        ):
            await interaction.response.send_message(
                f"{theme.deniedIcon} Interval must be **0** (disabled) or between "
                f"**{SyncSettingsView.MIN_INTERVAL_MINUTES}** and "
                f"**{SyncSettingsView.MAX_INTERVAL_MINUTES}** minutes. "
                f"This minimum prevents the queue from piling up if a sync takes "
                f"longer than the interval.",
                ephemeral=True,
            )
            return
        self.parent_view.alliance_cursor.execute(
            "UPDATE alliancesettings SET interval = ? WHERE alliance_id = ?",
            (value, self.parent_view.selected_alliance),
        )
        self.parent_view.alliance_db.commit()
        await self.parent_view.update_view(interaction)


class StartTimeModal(discord.ui.Modal):
    """HH:MM (UTC) start time input for the sync schedule. Empty clears it."""

    def __init__(self, parent_view: SyncSettingsView, current_start_time):
        super().__init__(title="Edit Sync Start Time (UTC)")
        self.parent_view = parent_view
        self.input = discord.ui.TextInput(
            label="Start Time (HH:MM UTC)",
            placeholder="14:00 — leave empty to clear",
            default=current_start_time or "",
            required=False,
            max_length=5,
        )
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        raw = self.input.value.strip()
        if not raw:
            new_value = None
        else:
            import re
            if not re.match(r'^([01]?\d|2[0-3]):([0-5]\d)$', raw):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid time format. Use HH:MM (e.g. `14:00`).",
                    ephemeral=True,
                )
                return
            h, m = raw.split(':')
            new_value = f"{int(h):02d}:{int(m):02d}"
        self.parent_view.alliance_cursor.execute(
            "UPDATE alliancesettings SET start_time = ? WHERE alliance_id = ?",
            (new_value, self.parent_view.selected_alliance),
        )
        self.parent_view.alliance_db.commit()
        await self.parent_view.update_view(interaction)


async def setup(bot):
    await bot.add_cog(BotOperations(bot, sqlite3.connect('db/settings.sqlite')))