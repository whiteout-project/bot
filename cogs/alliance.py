"""
Alliance management cog. Handles alliance CRUD, settings, and member listing.
"""
import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import asyncio
import logging
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message
from .process_queue import ALLIANCE_CONTROL

logger = logging.getLogger('alliance')


def _alliance_sync_in_flight(process_queue, alliance_id: int) -> bool:
    """True if any sync work for this alliance is queued or running.

    Checks both 'alliance_sync' (scheduled periodic syncs) and 'alliance_control'
    (manual syncs via UI). Used to skip duplicate enqueues so the queue can't
    pile up when interval < sync duration or admins click Sync All repeatedly.
    """
    return (
        process_queue.has_queued_or_active("alliance_sync", alliance_id=alliance_id)
        or process_queue.has_queued_or_active("alliance_control", alliance_id=alliance_id)
    )


class Alliance(commands.Cog):
    def __init__(self, bot, conn):
        self.bot = bot
        self.conn = conn
        self.c = self.conn.cursor()
        
        self.conn_users = sqlite3.connect('db/users.sqlite', timeout=30.0, check_same_thread=False)
        self.c_users = self.conn_users.cursor()

        self.conn_settings = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.c_settings = self.conn_settings.cursor()

        self.conn_giftcode = sqlite3.connect('db/giftcode.sqlite', timeout=30.0, check_same_thread=False)
        self.c_giftcode = self.conn_giftcode.cursor()

        self._create_table()
        self._check_and_add_column()

    def _create_table(self):
        self.c.execute("""
            CREATE TABLE IF NOT EXISTS alliance_list (
                alliance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                discord_server_id INTEGER
            )
        """)
        self.conn.commit()

    def _check_and_add_column(self):
        self.c.execute("PRAGMA table_info(alliance_list)")
        columns = [info[1] for info in self.c.fetchall()]
        if "discord_server_id" not in columns:
            self.c.execute("ALTER TABLE alliance_list ADD COLUMN discord_server_id INTEGER")
            self.conn.commit()

    def cog_unload(self):
        """Close database connections when cog is unloaded."""
        for conn_name in ['conn_users', 'conn_settings', 'conn_giftcode']:
            if hasattr(self, conn_name):
                try:
                    getattr(self, conn_name).close()
                except Exception:
                    pass

    async def view_alliances(self, interaction: discord.Interaction):
        
        if interaction.guild is None:
            await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server, not in DMs.", ephemeral=True)
            return

        user_id = interaction.user.id
        guild_id = interaction.guild.id

        # Use centralized permission check
        is_admin, is_global = PermissionManager.is_admin(user_id)
        if not is_admin:
            await interaction.response.send_message("You do not have permission to view alliances.", ephemeral=True)
            return

        try:
            if is_global:
                # Global admin - show all alliances
                query = """
                    SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                    FROM alliance_list a
                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                    ORDER BY a.alliance_id ASC
                """
                self.c.execute(query)
            else:
                # Get alliance IDs using centralized permission manager
                alliance_ids, _ = PermissionManager.get_admin_alliance_ids(user_id, guild_id)

                if not alliance_ids:
                    embed = discord.Embed(
                        title="Existing Alliances",
                        description="No alliances found for your permissions.",
                        color=theme.emColor1
                    )
                    await interaction.response.send_message(embed=embed, ephemeral=True)
                    return

                placeholders = ','.join('?' * len(alliance_ids))
                query = f"""
                    SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                    FROM alliance_list a
                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                    WHERE a.alliance_id IN ({placeholders})
                    ORDER BY a.alliance_id ASC
                """
                self.c.execute(query, alliance_ids)

            alliances = self.c.fetchall()

            alliance_list = ""
            for alliance_id, name, interval in alliances:
                
                self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                member_count = self.c_users.fetchone()[0]
                
                interval_text = f"{interval} minutes" if interval > 0 else "No automatic sync"
                alliance_list += f"{theme.allianceIcon} **{alliance_id}: {name}**\n{theme.userIcon} Members: {member_count}\n{theme.timeIcon} Sync Interval: {interval_text}\n\n"

            if not alliance_list:
                alliance_list = "No alliances found."

            embed = discord.Embed(
                title="Existing Alliances",
                description=alliance_list,
                color=theme.emColor1
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(
                "An error occurred while fetching alliances.", 
                ephemeral=True
            )

    @app_commands.command(name="settings", description="Open settings menu.")
    async def settings(self, interaction: discord.Interaction):
        try:
            if interaction.guild is not None: # Check bot permissions only if in a guild
                perm_check = interaction.guild.get_member(interaction.client.user.id)
                if not perm_check.guild_permissions.administrator:
                    await interaction.response.send_message(
                        f"Beeb boop {theme.robotIcon} I need **Administrator** permissions to function. "
                        "Go to server settings --> Roles --> find my role --> scroll down and turn on Administrator", 
                        ephemeral=True
                    )
                    return
                
            self.c_settings.execute("SELECT COUNT(*) FROM admin")
            admin_count = self.c_settings.fetchone()[0]

            user_id = interaction.user.id

            if admin_count == 0:
                self.c_settings.execute("""
                    INSERT INTO admin (id, is_initial) 
                    VALUES (?, 1)
                """, (user_id,))
                self.conn_settings.commit()

                first_use_embed = discord.Embed(
                    title=f"{theme.newIcon} First Time Setup",
                    description=(
                        "This command has been used for the first time and no administrators were found.\n\n"
                        f"**{interaction.user.name}** has been added as the Global Administrator.\n\n"
                        "You can now access all administrative functions."
                    ),
                    color=theme.emColor3
                )
                await interaction.response.send_message(embed=first_use_embed, ephemeral=True)
                
                await asyncio.sleep(3)
                
            self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
            admin = self.c_settings.fetchone()

            if admin is None:
                await interaction.response.send_message(
                    "You do not have permission to access this menu.",
                    ephemeral=True
                )
                return

            # Delegate to MainMenu cog for the actual menu display
            main_menu_cog = self.bot.get_cog("MainMenu")
            if main_menu_cog:
                if admin_count == 0:
                    # First time setup - need to send initial response then show menu
                    await main_menu_cog.show_main_menu(interaction)
                else:
                    # Normal flow - send menu as initial response
                    await self._send_initial_main_menu(interaction, main_menu_cog)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Main Menu module not found.",
                    ephemeral=True
                )

        except Exception as e:
            if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                logger.error(f"Settings command error: {e}")
                print(f"Settings command error: {e}")
            error_message = "An error occurred while processing your request."
            if not interaction.response.is_done():
                await interaction.response.send_message(error_message, ephemeral=True)
            else:
                await interaction.followup.send(error_message, ephemeral=True)

    async def _send_initial_main_menu(self, interaction: discord.Interaction, main_menu_cog):
        """Send the main menu as the initial response (for /settings command)."""
        from .bot_main_menu import MainMenuView

        embed = main_menu_cog.build_main_menu_embed()
        view = MainMenuView(main_menu_cog)
        await interaction.response.send_message(embed=embed, view=view)

    async def show_alliance_operations(self, interaction: discord.Interaction):
        """Display the Alliance Operations menu (Add/Edit/Delete/View alliances)."""
        try:
            embed = discord.Embed(
                title=f"{theme.allianceIcon} Alliance Operations",
                description=(
                    f"Please select an operation:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.addIcon} **Add Alliance**\n"
                    f"└ Create a new alliance\n\n"
                    f"{theme.editListIcon} **Edit Alliance**\n"
                    f"└ Modify existing alliance settings\n\n"
                    f"{theme.trashIcon} **Delete Alliance**\n"
                    f"└ Remove an existing alliance\n\n"
                    f"{theme.eyesIcon} **View Alliances**\n"
                    f"└ List all available alliances\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Add Alliance",
                emoji=theme.addIcon,
                style=discord.ButtonStyle.success,
                custom_id="add_alliance"
            ))
            view.add_item(discord.ui.Button(
                label="Edit Alliance",
                emoji=theme.editListIcon,
                style=discord.ButtonStyle.primary,
                custom_id="edit_alliance"
            ))
            view.add_item(discord.ui.Button(
                label="Delete Alliance",
                emoji=theme.trashIcon,
                style=discord.ButtonStyle.danger,
                custom_id="delete_alliance"
            ))
            view.add_item(discord.ui.Button(
                label="View Alliances",
                emoji=theme.eyesIcon,
                style=discord.ButtonStyle.primary,
                custom_id="view_alliances"
            ))
            view.add_item(discord.ui.Button(
                label="Check Alliance",
                emoji=theme.searchIcon,
                style=discord.ButtonStyle.primary,
                custom_id="check_alliance"
            ))
            view.add_item(discord.ui.Button(
                label="Back",
                emoji=theme.backIcon,
                style=discord.ButtonStyle.secondary,
                custom_id="back_to_alliance_management"
            ))

            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_alliance_operations: {e}")
            print(f"Error in show_alliance_operations: {e}")

    async def show_add_alliance_for(self, interaction: discord.Interaction):
        """Direct entry to Add Alliance flow (no operations sub-menu)."""
        await self.add_alliance(interaction)

    async def sync_all_alliances(self, interaction: discord.Interaction):
        """Enqueue an alliance_control sync for every alliance the admin can access."""
        try:
            allowed_alliance_ids, is_global = PermissionManager.get_admin_alliance_ids(
                interaction.user.id, interaction.guild_id
            )

            if is_global:
                self.c.execute(
                    "SELECT alliance_id, name FROM alliance_list ORDER BY name"
                )
            elif allowed_alliance_ids:
                placeholders = ",".join("?" * len(allowed_alliance_ids))
                self.c.execute(
                    f"SELECT alliance_id, name FROM alliance_list "
                    f"WHERE alliance_id IN ({placeholders}) ORDER BY name",
                    allowed_alliance_ids,
                )
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You don't have permission to sync any alliances.",
                    ephemeral=True,
                )
                return

            alliances = self.c.fetchall()

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliances found to sync.",
                    ephemeral=True,
                )
                return

            process_queue = self.bot.get_cog("ProcessQueue")
            if not process_queue:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Process Queue module not found.",
                    ephemeral=True,
                )
                return

            if not self.bot.get_cog("AllianceSync"):
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance Sync module not found.",
                    ephemeral=True,
                )
                return

            eligible = []
            skipped_pending = []
            for alliance_id, name in alliances:
                if _alliance_sync_in_flight(process_queue, alliance_id):
                    skipped_pending.append((alliance_id, name))
                else:
                    eligible.append((alliance_id, name))

            if not eligible:
                await interaction.response.send_message(
                    f"{theme.warnIcon} All {len(skipped_pending)} alliance(s) already have a "
                    f"sync queued or running — nothing new to enqueue.",
                    ephemeral=True,
                )
                return

            queue_info = process_queue.get_queue_info()
            initial_queue_pos = queue_info["queue_size"] + 1

            description = (
                f"{theme.upperDivider}\n"
                f"{theme.chartIcon} **Type:** All Accessible Alliances\n"
                f"{theme.allianceIcon} **Alliances:** {len(eligible)}\n"
            )
            if skipped_pending:
                description += (
                    f"{theme.warnIcon} **Skipped (already pending):** {len(skipped_pending)}\n"
                )
            description += (
                f"{theme.pinIcon} **Status:** Queued\n"
                f"{theme.levelIcon} **Queue Position:** {initial_queue_pos}\n"
                f"{theme.lowerDivider}"
            )

            progress_embed = discord.Embed(
                title=f"{theme.hourglassIcon} Sync All Alliances",
                description=description,
                color=theme.emColor1,
            )
            await interaction.response.send_message(embed=progress_embed, ephemeral=True)
            msg = await interaction.original_response()

            queued_alliances = []
            for index, (alliance_id, name) in enumerate(eligible):
                try:
                    self.c.execute(
                        "SELECT channel_id FROM alliancesettings WHERE alliance_id = ?",
                        (alliance_id,),
                    )
                    channel_data = self.c.fetchone()
                    channel = self.bot.get_channel(channel_data[0]) if channel_data else interaction.channel
                    if not channel:
                        continue

                    process_id = process_queue.enqueue(
                        action="alliance_control",
                        priority=ALLIANCE_CONTROL,
                        alliance_id=alliance_id,
                        details={
                            "channel_id": channel.id,
                            "alliance_name": name,
                            "is_batch": True,
                            "batch_info": {
                                "current": index + 1,
                                "total": len(eligible),
                                "all_names": list(queued_alliances),
                            },
                        },
                    )
                    process_queue.attach_runtime_context(process_id, {
                        "interaction_message": msg,
                    })
                    queued_alliances.append((alliance_id, name))
                except Exception as e:
                    logger.error(f"Error queuing alliance {name}: {e}")
                    print(f"Error queuing alliance {name}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error in sync_all_alliances: {e}")
            print(f"Error in sync_all_alliances: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while queuing the sync.",
                    ephemeral=True,
                )

    async def show_edit_name_for(self, interaction: discord.Interaction, alliance_id: int):
        """Direct entry: rename a single alliance (no other settings)."""
        self.c.execute(
            "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
        )
        row = self.c.fetchone()
        if not row:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance not found.", ephemeral=True
            )
            return
        await interaction.response.send_modal(
            EditNameModal(alliance_id, row[0], self.conn)
        )

    async def show_edit_alliance_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: edit a known alliance (skip the picker)."""
        try:
            self.c.execute(
                "SELECT alliance_id, name FROM alliance_list WHERE alliance_id = ?",
                (alliance_id,),
            )
            alliance_data = self.c.fetchone()
            if not alliance_data:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance not found.", ephemeral=True
                )
                return

            self.c.execute(
                "SELECT interval, channel_id, start_time FROM alliancesettings "
                "WHERE alliance_id = ?",
                (alliance_id,),
            )
            settings_data = self.c.fetchone()

            modal = AllianceModal(
                title="Edit Alliance",
                default_name=alliance_data[1],
                default_interval=str(settings_data[0] if settings_data else 0),
                default_start_time=settings_data[2] if settings_data and settings_data[2] else "",
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            try:
                alliance_name = modal.name.value.strip()
                interval = int(modal.interval.value.strip())
                start_time_raw = modal.start_time.value.strip() if modal.start_time.value else ""

                start_time = None
                if start_time_raw:
                    import re
                    if re.match(r'^([01]?\d|2[0-3]):([0-5]\d)$', start_time_raw):
                        parts = start_time_raw.split(':')
                        start_time = f"{int(parts[0]):02d}:{int(parts[1]):02d}"
                    else:
                        await modal.interaction.response.send_message(
                            f"{theme.deniedIcon} Invalid start time format. Use HH:MM.",
                            ephemeral=True,
                        )
                        return

                channel_embed = discord.Embed(
                    title=f"{theme.retryIcon} Channel Selection",
                    description=(
                        f"**Current Channel Information**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.announceIcon} Current channel: "
                        f"{f'<#{settings_data[1]}>' if settings_data else 'Not set'}\n"
                        f"**Total Channels:** {len(interaction.guild.text_channels)}\n"
                        f"{theme.lowerDivider}"
                    ),
                    color=theme.emColor1,
                )

                async def channel_select_callback(channel_interaction: discord.Interaction):
                    try:
                        channel_id = int(channel_interaction.data["values"][0])
                        self.c.execute(
                            "UPDATE alliance_list SET name = ? WHERE alliance_id = ?",
                            (alliance_name, alliance_id),
                        )
                        if settings_data:
                            self.c.execute(
                                "UPDATE alliancesettings SET channel_id = ?, interval = ?, "
                                "start_time = ? WHERE alliance_id = ?",
                                (channel_id, interval, start_time, alliance_id),
                            )
                        else:
                            self.c.execute(
                                "INSERT INTO alliancesettings "
                                "(alliance_id, channel_id, interval, start_time) "
                                "VALUES (?, ?, ?, ?)",
                                (alliance_id, channel_id, interval, start_time),
                            )
                        self.conn.commit()

                        start_time_display = (
                            f"{start_time} UTC" if start_time
                            else "Not set (starts on bot startup)"
                        )
                        result_embed = discord.Embed(
                            title=f"{theme.verifiedIcon} Alliance Successfully Updated",
                            description=(
                                f"**🛡️ Name:** {alliance_name}\n"
                                f"**🔢 ID:** {alliance_id}\n"
                                f"**📢 Channel:** <#{channel_id}>\n"
                                f"**⏱️ Sync Interval:** {interval} minutes\n"
                                f"**🕐 Fixed Start Time:** {start_time_display}"
                            ),
                            color=theme.emColor3,
                        )
                        result_embed.timestamp = discord.utils.utcnow()
                        await channel_interaction.response.edit_message(
                            embed=result_embed, view=None
                        )
                    except Exception as e:
                        logger.error(f"Error in show_edit_alliance_for channel callback: {e}")
                        print(f"Error in show_edit_alliance_for channel callback: {e}")
                        await channel_interaction.response.edit_message(
                            embed=discord.Embed(
                                title=f"{theme.deniedIcon} Error",
                                description=f"An error occurred while updating: {e}",
                                color=theme.emColor2,
                            ),
                            view=None,
                        )

                view = PaginatedChannelView(
                    interaction.guild.text_channels, channel_select_callback
                )
                await modal.interaction.response.send_message(
                    embed=channel_embed, view=view, ephemeral=True
                )
            except ValueError:
                await modal.interaction.response.send_message(
                    f"{theme.deniedIcon} Invalid interval value.", ephemeral=True
                )
            except Exception as e:
                logger.error(f"Error in show_edit_alliance_for submit: {e}")
                print(f"Error in show_edit_alliance_for submit: {e}")
                await modal.interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred: {e}", ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error in show_edit_alliance_for: {e}")
            print(f"Error in show_edit_alliance_for: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while loading the editor.",
                    ephemeral=True,
                )

    async def show_delete_alliance_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: delete a known alliance (skip the picker)."""
        await self.alliance_delete_callback(interaction, alliance_id=alliance_id)

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get("custom_id")

            # Only handle custom_ids that belong to this cog
            handled_ids = {
                "alliance_operations", "back_to_alliance_management", "edit_alliance",
                "check_alliance", "add_alliance", "delete_alliance", "view_alliances",
                "alliance_check_select"
            }
            if custom_id not in handled_ids:
                return

            user_id = interaction.user.id
            self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
            admin = self.c_settings.fetchone()

            if admin is None:
                await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                return

            try:
                if custom_id == "alliance_operations":
                    await self.show_alliance_operations(interaction)

                elif custom_id == "back_to_alliance_management":
                    main_menu_cog = self.bot.get_cog("MainMenu")
                    if main_menu_cog:
                        await main_menu_cog.show_alliance_management(interaction)

                elif custom_id == "edit_alliance":
                    await self.edit_alliance(interaction)

                elif custom_id == "check_alliance":
                    self.c.execute("""
                        SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                        FROM alliance_list a
                        LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                        ORDER BY a.name
                    """)
                    alliances = self.c.fetchall()

                    if not alliances:
                        await interaction.response.send_message("No alliances found to check.", ephemeral=True)
                        return

                    options = [
                        discord.SelectOption(
                            label="Check All Alliances",
                            value="all",
                            description="Start control process for all alliances",
                            emoji=theme.retryIcon
                        )
                    ]
                    
                    options.extend([
                        discord.SelectOption(
                            label=f"{name[:40]}",
                            value=str(alliance_id),
                            description=f"Sync Interval: {interval} minutes"
                        ) for alliance_id, name, interval in alliances
                    ])

                    select = discord.ui.Select(
                        placeholder="Select an alliance to check",
                        options=options,
                        custom_id="alliance_check_select"
                    )

                    async def alliance_check_callback(select_interaction: discord.Interaction):
                        try:
                            selected_value = select_interaction.data["values"][0]

                            if not self.bot.get_cog('AllianceSync'):
                                await select_interaction.response.send_message("Alliance Sync module not found.", ephemeral=True)
                                return

                            # Get the ProcessQueue cog
                            process_queue = self.bot.get_cog('ProcessQueue')
                            if not process_queue:
                                await select_interaction.response.send_message(
                                    f"{theme.deniedIcon} Process Queue module not found.",
                                    ephemeral=True
                                )
                                return

                            if selected_value == "all":
                                # Get initial queue position
                                queue_info = process_queue.get_queue_info()
                                initial_queue_pos = queue_info['queue_size'] + 1
                                
                                progress_embed = discord.Embed(
                                    title=f"{theme.hourglassIcon} Alliance Control Operation",
                                    description=(
                                        f"{theme.upperDivider}\n"
                                        f"{theme.chartIcon} **Type:** All Alliances ({len(alliances)} total)\n"
                                        f"{theme.allianceIcon} **Alliances:** {len(alliances)} alliances\n"
                                        f"{theme.pinIcon} **Status:** Queued\n"
                                        f"{theme.levelIcon} **Queue Position:** {initial_queue_pos}\n"
                                        f"{theme.lowerDivider}"
                                    ),
                                    color=theme.emColor1
                                )
                                eligible = [
                                    (aid, name, interval) for aid, name, interval in alliances
                                    if not _alliance_sync_in_flight(process_queue, aid)
                                ]
                                skipped_count = len(alliances) - len(eligible)
                                if not eligible:
                                    await select_interaction.response.send_message(
                                        f"{theme.warnIcon} All {skipped_count} alliance(s) already have a "
                                        f"sync queued or running — nothing new to enqueue.",
                                        ephemeral=True,
                                    )
                                    return

                                await select_interaction.response.send_message(embed=progress_embed, ephemeral=True)
                                msg = await select_interaction.original_response()
                                message_id = msg.id

                                # Queue all alliance operations at once
                                queued_alliances = []
                                for index, (alliance_id, name, _) in enumerate(eligible):
                                    try:
                                        self.c.execute("""
                                            SELECT channel_id FROM alliancesettings WHERE alliance_id = ?
                                        """, (alliance_id,))
                                        channel_data = self.c.fetchone()
                                        channel = self.bot.get_channel(channel_data[0]) if channel_data else select_interaction.channel
                                        if not channel:
                                            continue

                                        process_id = process_queue.enqueue(
                                            action='alliance_control',
                                            priority=ALLIANCE_CONTROL,
                                            alliance_id=alliance_id,
                                            details={
                                                'channel_id': channel.id,
                                                'alliance_name': name,
                                                'is_batch': True,
                                                'batch_info': {
                                                    'current': index + 1,
                                                    'total': len(eligible),
                                                    'all_names': list(queued_alliances),
                                                },
                                            },
                                        )
                                        # Attach interaction message for live progress updates
                                        process_queue.attach_runtime_context(process_id, {
                                            'interaction_message': msg,
                                        })
                                        queued_alliances.append((alliance_id, name))

                                    except Exception as e:
                                        logger.error(f"Error queuing alliance {name}: {e}")
                                        print(f"Error queuing alliance {name}: {e}")
                                        continue

                                if skipped_count:
                                    await select_interaction.followup.send(
                                        f"{theme.warnIcon} Skipped **{skipped_count}** alliance(s) "
                                        f"that already had a sync queued or running.",
                                        ephemeral=True,
                                    )

                            else:
                                alliance_id = int(selected_value)
                                self.c.execute("""
                                    SELECT a.name, s.channel_id
                                    FROM alliance_list a
                                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                                    WHERE a.alliance_id = ?
                                """, (alliance_id,))
                                alliance_data = self.c.fetchone()

                                if not alliance_data:
                                    await select_interaction.response.send_message("Alliance not found.", ephemeral=True)
                                    return

                                alliance_name, channel_id = alliance_data

                                if _alliance_sync_in_flight(process_queue, alliance_id):
                                    await select_interaction.response.send_message(
                                        f"{theme.warnIcon} **{alliance_name}** already has a sync queued "
                                        f"or running — skipped to avoid duplicate work.",
                                        ephemeral=True,
                                    )
                                    return

                                channel = self.bot.get_channel(channel_id) if channel_id else select_interaction.channel

                                # Get queue info for position
                                queue_info = process_queue.get_queue_info()
                                queue_position = queue_info['queue_size'] + 1

                                status_embed = discord.Embed(
                                    title=f"{theme.hourglassIcon} Alliance Control Operation",
                                    description=(
                                        f"{theme.upperDivider}\n"
                                        f"{theme.chartIcon} **Type:** Single Alliance\n"
                                        f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                                        f"{theme.pinIcon} **Status:** Queued\n"
                                        f"{theme.levelIcon} **Queue Position:** {queue_position}\n"
                                        f"{theme.lowerDivider}"
                                    ),
                                    color=theme.emColor1
                                )
                                await select_interaction.response.send_message(embed=status_embed, ephemeral=True)
                                msg = await select_interaction.original_response()

                                if not channel:
                                    return

                                process_id = process_queue.enqueue(
                                    action='alliance_control',
                                    priority=ALLIANCE_CONTROL,
                                    alliance_id=alliance_id,
                                    details={
                                        'channel_id': channel.id,
                                        'alliance_name': alliance_name,
                                        'is_batch': False,
                                    },
                                )
                                process_queue.attach_runtime_context(process_id, {
                                    'interaction_message': msg,
                                })

                        except Exception as e:
                            logger.error(f"Alliance check error: {e}")
                            print(f"Alliance check error: {e}")
                            await select_interaction.response.send_message(
                                "An error occurred during the control process.",
                                ephemeral=True
                            )

                    select.callback = alliance_check_callback
                    view = discord.ui.View()
                    view.add_item(select)

                    embed = discord.Embed(
                        title=f"{theme.searchIcon} Alliance Control",
                        description=(
                            f"Please select an alliance to check:\n\n"
                            f"**Information**\n"
                            f"{theme.upperDivider}\n"
                            f"• Select 'Check All Alliances' to process all alliances\n"
                            f"• Control process may take a few minutes\n"
                            f"• Results will be shared in the designated channel\n"
                            f"• Other controls will be queued during the process\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor1
                    )
                    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

                elif custom_id == "add_alliance":
                    await self.add_alliance(interaction)

                elif custom_id == "delete_alliance":
                    await self.delete_alliance(interaction)

                elif custom_id == "view_alliances":
                    await self.view_alliances(interaction)

                elif custom_id == "main_menu":
                    # Delegate to MainMenu cog
                    main_menu_cog = self.bot.get_cog("MainMenu")
                    if main_menu_cog:
                        await main_menu_cog.show_main_menu(interaction)

            except Exception as e:
                if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                    logger.error(f"Error processing interaction with custom_id '{custom_id}': {e}")
                    print(f"Error processing interaction with custom_id '{custom_id}': {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your request. Please try again.",
                        ephemeral=True
                    )

    async def add_alliance(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message(
                "Please perform this action in a Discord channel.", ephemeral=True
            )
            return
        await interaction.response.send_modal(AddAllianceModal(self))

    async def edit_alliance(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
            return

        user_id = interaction.user.id
        guild_id = interaction.guild.id

        # Get alliances this admin can access
        admin_alliances, is_global = PermissionManager.get_admin_alliances(user_id, guild_id)

        if not admin_alliances:
            no_alliance_embed = discord.Embed(
                title=f"{theme.deniedIcon}No Alliances Found",
                description="You don't have access to any alliances.",
                color=theme.emColor2
            )
            return await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)

        # Fetch full alliance details for the ones admin can access
        alliance_ids = [a[0] for a in admin_alliances]
        placeholders = ','.join('?' * len(alliance_ids))
        self.c.execute(f"""
            SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval, COALESCE(s.channel_id, 0) as channel_id
            FROM alliance_list a
            LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
            WHERE a.alliance_id IN ({placeholders})
            ORDER BY a.alliance_id ASC
        """, alliance_ids)
        alliances = self.c.fetchall()

        if not alliances:
            no_alliance_embed = discord.Embed(
                title=f"{theme.deniedIcon}No Alliances Found",
                description=(
                    "There are no alliances registered in the database.\n"
                    "Please create an alliance first using the `/alliance create` command."
                ),
                color=theme.emColor2
            )
            no_alliance_embed.set_footer(text="Use /alliance create to add a new alliance")
            return await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)

        alliance_options = [
            discord.SelectOption(
                label=f"{name} (ID: {alliance_id})",
                value=f"{alliance_id}",
                description=f"Interval: {interval} minutes"
            ) for alliance_id, name, interval, _ in alliances
        ]
        
        items_per_page = 25
        option_pages = [alliance_options[i:i + items_per_page] for i in range(0, len(alliance_options), items_per_page)]
        total_pages = len(option_pages)

        class PaginatedAllianceView(discord.ui.View):
            def __init__(self, pages, original_callback):
                super().__init__(timeout=7200)
                self.current_page = 0
                self.pages = pages
                self.original_callback = original_callback
                self.total_pages = len(pages)
                self.update_view()

            def update_view(self):
                self.clear_items()
                
                select = discord.ui.Select(
                    placeholder=f"Select alliance ({self.current_page + 1}/{self.total_pages})",
                    options=self.pages[self.current_page]
                )
                select.callback = self.original_callback
                self.add_item(select)
                
                previous_button = discord.ui.Button(
                    label="",
                    emoji=f"{theme.prevIcon}",
                    style=discord.ButtonStyle.grey,
                    custom_id="previous",
                    disabled=(self.current_page == 0)
                )
                previous_button.callback = self.previous_callback
                self.add_item(previous_button)

                next_button = discord.ui.Button(
                    label="",
                    emoji=f"{theme.nextIcon}",
                    style=discord.ButtonStyle.grey,
                    custom_id="next",
                    disabled=(self.current_page == len(self.pages) - 1)
                )
                next_button.callback = self.next_callback
                self.add_item(next_button)

            async def previous_callback(self, interaction: discord.Interaction):
                self.current_page = (self.current_page - 1) % len(self.pages)
                self.update_view()
                
                embed = interaction.message.embeds[0]
                embed.description = (
                    f"**Instructions:**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                    f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                    f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                    f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                    f"{theme.lowerDivider}"
                )
                await interaction.response.edit_message(embed=embed, view=self)

            async def next_callback(self, interaction: discord.Interaction):
                self.current_page = (self.current_page + 1) % len(self.pages)
                self.update_view()

                embed = interaction.message.embeds[0]
                embed.description = (
                    f"**Instructions:**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                    f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                    f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                    f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                    f"{theme.lowerDivider}"
                )
                await interaction.response.edit_message(embed=embed, view=self)

        async def select_callback(select_interaction: discord.Interaction):
            try:
                alliance_id = int(select_interaction.data["values"][0])
                alliance_data = next(a for a in alliances if a[0] == alliance_id)

                self.c.execute("""
                    SELECT interval, channel_id, start_time
                    FROM alliancesettings
                    WHERE alliance_id = ?
                """, (alliance_id,))
                settings_data = self.c.fetchone()

                modal = AllianceModal(
                    title="Edit Alliance",
                    default_name=alliance_data[1],
                    default_interval=str(settings_data[0] if settings_data else 0),
                    default_start_time=settings_data[2] if settings_data and settings_data[2] else ""
                )
                await select_interaction.response.send_modal(modal)
                await modal.wait()

                try:
                    alliance_name = modal.name.value.strip()
                    interval = int(modal.interval.value.strip())
                    start_time_raw = modal.start_time.value.strip() if modal.start_time.value else ""

                    # Validate start_time format (HH:MM) if provided
                    start_time = None
                    if start_time_raw:
                        import re
                        if re.match(r'^([01]?\d|2[0-3]):([0-5]\d)$', start_time_raw):
                            # Normalize to HH:MM format
                            parts = start_time_raw.split(':')
                            start_time = f"{int(parts[0]):02d}:{int(parts[1]):02d}"
                        else:
                            error_embed = discord.Embed(
                                title="Error",
                                description="Invalid start time format. Please use HH:MM (e.g., 14:00).",
                                color=theme.emColor2
                            )
                            await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)
                            return

                    embed = discord.Embed(
                        title=f"{theme.retryIcon} Channel Selection",
                        description=(
                            f"**Current Channel Information**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.announceIcon} Current channel: {f'<#{settings_data[1]}>' if settings_data else 'Not set'}\n"
                            f"**Page:** 1/1\n"
                            f"**Total Channels:** {len(interaction.guild.text_channels)}\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor1
                    )

                    async def channel_select_callback(channel_interaction: discord.Interaction):
                        try:
                            channel_id = int(channel_interaction.data["values"][0])

                            self.c.execute("UPDATE alliance_list SET name = ? WHERE alliance_id = ?",
                                          (alliance_name, alliance_id))

                            if settings_data:
                                self.c.execute("""
                                    UPDATE alliancesettings
                                    SET channel_id = ?, interval = ?, start_time = ?
                                    WHERE alliance_id = ?
                                """, (channel_id, interval, start_time, alliance_id))
                            else:
                                self.c.execute("""
                                    INSERT INTO alliancesettings (alliance_id, channel_id, interval, start_time)
                                    VALUES (?, ?, ?, ?)
                                """, (alliance_id, channel_id, interval, start_time))

                            self.conn.commit()

                            result_embed = discord.Embed(
                                title=f"{theme.verifiedIcon} Alliance Successfully Updated",
                                description="The alliance details have been updated as follows:",
                                color=theme.emColor3
                            )

                            start_time_display = f"{start_time} UTC" if start_time else "Not set (starts on bot startup)"
                            info_section = (
                                f"**🛡️ Alliance Name**\n{alliance_name}\n\n"
                                f"**🔢 Alliance ID**\n{alliance_id}\n\n"
                                f"**📢 Channel**\n<#{channel_id}>\n\n"
                                f"**⏱️ Sync Interval**\n{interval} minutes\n\n"
                                f"**🕐 Fixed Start Time**\n{start_time_display}"
                            )
                            result_embed.add_field(name="Alliance Details", value=info_section, inline=False)

                            result_embed.set_footer(text="Alliance settings have been successfully saved")
                            result_embed.timestamp = discord.utils.utcnow()

                            await channel_interaction.response.edit_message(embed=result_embed, view=None)

                        except Exception as e:
                            error_embed = discord.Embed(
                                title=f"{theme.deniedIcon}Error",
                                description=f"An error occurred while updating the alliance: {str(e)}",
                                color=theme.emColor2
                            )
                            await channel_interaction.response.edit_message(embed=error_embed, view=None)

                    channels = modal.interaction.guild.text_channels
                    view = PaginatedChannelView(channels, channel_select_callback)
                    await modal.interaction.response.send_message(embed=embed, view=view, ephemeral=True)

                except ValueError:
                    error_embed = discord.Embed(
                        title="Error",
                        description="Invalid interval value. Please enter a number.",
                        color=theme.emColor2
                    )
                    await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)
                except Exception as e:
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Error: {str(e)}",
                        color=theme.emColor2
                    )
                    await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)

            except Exception as e:
                error_embed = discord.Embed(
                    title=f"{theme.deniedIcon}Error",
                    description=f"An error occurred: {str(e)}",
                    color=theme.emColor2
                )
                if not select_interaction.response.is_done():
                    await select_interaction.response.send_message(embed=error_embed, ephemeral=True)
                else:
                    await select_interaction.followup.send(embed=error_embed, ephemeral=True)

        view = PaginatedAllianceView(option_pages, select_callback)
        embed = discord.Embed(
            title=f"{theme.shieldIcon} Alliance Edit Menu",
            description=(
                f"**Instructions:**\n"
                f"{theme.upperDivider}\n"
                f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                f"**Current Page:** {1}/{total_pages}\n"
                f"**Total Alliances:** {len(alliances)}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )
        embed.set_footer(text="Use the dropdown menu below to select an alliance")
        embed.timestamp = discord.utils.utcnow()
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def delete_alliance(self, interaction: discord.Interaction):
        try:
            if interaction.guild is None:
                await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
                return

            user_id = interaction.user.id
            guild_id = interaction.guild.id

            # Get alliances this admin can access
            admin_alliances, is_global = PermissionManager.get_admin_alliances(user_id, guild_id)

            if not admin_alliances:
                no_alliance_embed = discord.Embed(
                    title=f"{theme.deniedIcon}No Alliances Found",
                    description="You don't have access to any alliances.",
                    color=theme.emColor2
                )
                await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)
                return

            # Use the alliances from permission manager (already has id, name)
            alliances = admin_alliances

            if not alliances:
                no_alliance_embed = discord.Embed(
                    title=f"{theme.deniedIcon}No Alliances Found",
                    description="There are no alliances to delete.",
                    color=theme.emColor2
                )
                await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)
                return

            alliance_members = {}
            for alliance_id, _ in alliances:
                self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                member_count = self.c_users.fetchone()[0]
                alliance_members[alliance_id] = member_count

            items_per_page = 25
            all_options = [
                discord.SelectOption(
                    label=f"{name[:40]} (ID: {alliance_id})",
                    value=f"{alliance_id}",
                    description=f"{theme.membersIcon} Members: {alliance_members[alliance_id]} | Click to delete",
                    emoji=theme.trashIcon
                ) for alliance_id, name in alliances
            ]
            
            option_pages = [all_options[i:i + items_per_page] for i in range(0, len(all_options), items_per_page)]
            
            embed = discord.Embed(
                title=f"{theme.trashIcon} Delete Alliance",
                description=(
                    f"**{theme.warnIcon} Warning: This action cannot be undone!**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                    f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                    f"**Current Page:** 1/{len(option_pages)}\n"
                    f"**Total Alliances:** {len(alliances)}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor2
            )
            embed.set_footer(text=f"{theme.warnIcon} Warning: Deleting an alliance will remove all its data!")
            embed.timestamp = discord.utils.utcnow()

            view = PaginatedDeleteView(option_pages, self.alliance_delete_callback)
            
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            logger.error(f"Error in delete_alliance: {e}")
            print(f"Error in delete_alliance: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon}Error",
                description="An error occurred while loading the delete menu.",
                color=theme.emColor2
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)

    async def alliance_delete_callback(self, interaction: discord.Interaction, alliance_id: int | None = None):
        try:
            if alliance_id is None:
                alliance_id = int(interaction.data["values"][0])

            self.c.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
            alliance_data = self.c.fetchone()
            
            if not alliance_data:
                await interaction.response.send_message("Alliance not found.", ephemeral=True)
                return
            
            alliance_name = alliance_data[0]

            self.c.execute("SELECT COUNT(*) FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
            settings_count = self.c.fetchone()[0]

            self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
            users_count = self.c_users.fetchone()[0]

            self.c_settings.execute("SELECT COUNT(*) FROM adminserver WHERE alliances_id = ?", (alliance_id,))
            admin_server_count = self.c_settings.fetchone()[0]

            self.c_giftcode.execute("SELECT COUNT(*) FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
            gift_channels_count = self.c_giftcode.fetchone()[0]

            self.c_giftcode.execute("SELECT COUNT(*) FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
            gift_code_control_count = self.c_giftcode.fetchone()[0]

            self.c_settings.execute("SELECT COUNT(*) FROM invalid_id_tracker WHERE alliance_id = ?", (str(alliance_id),))
            invalid_tracker_count = self.c_settings.fetchone()[0]

            self.c_settings.execute("SELECT COUNT(*) FROM alliance_logs WHERE alliance_id = ?", (alliance_id,))
            alliance_logs_count = self.c_settings.fetchone()[0]

            confirm_embed = discord.Embed(
                title=f"{theme.warnIcon} Confirm Alliance Deletion",
                description=(
                    f"Are you sure you want to delete this alliance?\n\n"
                    f"**Alliance Details:**\n"
                    f"{theme.allianceIcon} **Name:** {alliance_name}\n"
                    f"{theme.levelIcon} **ID:** {alliance_id}\n"
                    f"{theme.membersIcon} **Members:** {users_count}\n\n"
                    f"**Data to be Deleted:**\n"
                    f"{theme.settingsIcon} Alliance Settings: {settings_count}\n"
                    f"{theme.membersIcon} User Records: {users_count}\n"
                    f"{theme.allianceIcon} Admin Server Records: {admin_server_count}\n"
                    f"{theme.announceIcon} Gift Channels: {gift_channels_count}\n"
                    f"{theme.chartIcon} Gift Code Controls: {gift_code_control_count}\n"
                    f"{theme.deniedIcon} Invalid ID Tracker: {invalid_tracker_count}\n"
                    f"{theme.listIcon} Alliance Logs: {alliance_logs_count}\n\n"
                    f"**{theme.warnIcon} WARNING: This action cannot be undone!**"
                ),
                color=theme.emColor2
            )
            
            confirm_view = discord.ui.View(timeout=60)
            
            async def confirm_callback(button_interaction: discord.Interaction):
                try:
                    self.c.execute("DELETE FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_count = self.c.rowcount
                    
                    self.c.execute("DELETE FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
                    admin_settings_count = self.c.rowcount
                    
                    self.conn.commit()

                    self.c_users.execute("DELETE FROM users WHERE alliance = ?", (alliance_id,))
                    users_count_deleted = self.c_users.rowcount
                    self.conn_users.commit()

                    self.c_settings.execute("DELETE FROM adminserver WHERE alliances_id = ?", (alliance_id,))
                    admin_server_count = self.c_settings.rowcount

                    self.c_settings.execute("DELETE FROM invalid_id_tracker WHERE alliance_id = ?", (str(alliance_id),))
                    invalid_tracker_deleted = self.c_settings.rowcount

                    self.c_settings.execute("DELETE FROM alliance_logs WHERE alliance_id = ?", (alliance_id,))
                    alliance_logs_deleted = self.c_settings.rowcount

                    self.conn_settings.commit()

                    self.c_giftcode.execute("DELETE FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                    gift_channels_count = self.c_giftcode.rowcount

                    self.c_giftcode.execute("DELETE FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
                    gift_code_control_count = self.c_giftcode.rowcount
                    
                    self.conn_giftcode.commit()

                    cleanup_embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Alliance Successfully Deleted",
                        description=(
                            f"Alliance **{alliance_name}** has been deleted.\n\n"
                            "**Cleaned Up Data:**\n"
                            f"{theme.allianceIcon} Alliance Records: {alliance_count}\n"
                            f"{theme.membersIcon} Users Removed: {users_count_deleted}\n"
                            f"{theme.settingsIcon} Alliance Settings: {admin_settings_count}\n"
                            f"{theme.allianceIcon} Admin Server Records: {admin_server_count}\n"
                            f"{theme.announceIcon} Gift Channels: {gift_channels_count}\n"
                            f"{theme.chartIcon} Gift Code Controls: {gift_code_control_count}\n"
                            f"{theme.deniedIcon} Invalid ID Tracker: {invalid_tracker_deleted}\n"
                            f"{theme.listIcon} Alliance Logs: {alliance_logs_deleted}"
                        ),
                        color=theme.emColor3
                    )
                    cleanup_embed.set_footer(text="All related data has been successfully removed")
                    cleanup_embed.timestamp = discord.utils.utcnow()
                    
                    cleanup_view = discord.ui.View(timeout=60)
                    back_btn = discord.ui.Button(
                        label="Back to Alliances",
                        emoji=theme.backIcon,
                        style=discord.ButtonStyle.secondary,
                    )

                    async def _back_to_alliances(back_interaction: discord.Interaction):
                        main_menu = self.bot.get_cog("MainMenu")
                        if main_menu:
                            await main_menu.show_alliance_management(back_interaction)
                    back_btn.callback = _back_to_alliances
                    cleanup_view.add_item(back_btn)
                    await button_interaction.response.edit_message(embed=cleanup_embed, view=cleanup_view)

                except Exception as e:
                    error_embed = discord.Embed(
                        title=f"{theme.deniedIcon}Error",
                        description=f"An error occurred while deleting the alliance: {str(e)}",
                        color=theme.emColor2
                    )
                    error_view = discord.ui.View(timeout=60)
                    err_back = discord.ui.Button(
                        label="Back",
                        emoji=theme.backIcon,
                        style=discord.ButtonStyle.secondary,
                    )

                    async def _back_to_hub(back_interaction: discord.Interaction):
                        main_menu = self.bot.get_cog("MainMenu")
                        if main_menu:
                            await main_menu.show_alliance_hub(back_interaction, alliance_id)
                    err_back.callback = _back_to_hub
                    error_view.add_item(err_back)
                    await button_interaction.response.edit_message(embed=error_embed, view=error_view)

            async def cancel_callback(button_interaction: discord.Interaction):
                # Silent return to the alliance hub — user clicked Cancel,
                # no need for a separate "Cancelled" dead-end screen.
                main_menu = self.bot.get_cog("MainMenu")
                if main_menu:
                    await main_menu.show_alliance_hub(button_interaction, alliance_id)
                else:
                    await button_interaction.response.edit_message(
                        embed=discord.Embed(
                            title=f"{theme.deniedIcon} Deletion Cancelled",
                            description="Alliance deletion has been cancelled.",
                            color=theme.emColor4,
                        ),
                        view=None,
                    )

            confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.danger)
            cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.grey)
            confirm_button.callback = confirm_callback
            cancel_button.callback = cancel_callback
            confirm_view.add_item(confirm_button)
            confirm_view.add_item(cancel_button)

            await interaction.response.edit_message(embed=confirm_embed, view=confirm_view)

        except Exception as e:
            logger.error(f"Error in alliance_delete_callback: {e}")
            print(f"Error in alliance_delete_callback: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon}Error",
                description="An error occurred while processing the deletion.",
                color=theme.emColor2
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

class AllianceModal(discord.ui.Modal):
    def __init__(self, title: str, default_name: str = "", default_interval: str = "0", default_start_time: str = ""):
        super().__init__(title=title)

        self.name = discord.ui.TextInput(
            label="Alliance Name",
            placeholder="Enter alliance name",
            default=default_name,
            required=True
        )
        self.add_item(self.name)

        self.interval = discord.ui.TextInput(
            label="Sync Interval (minutes)",
            placeholder="Enter interval (0 to disable)",
            default=default_interval,
            required=True
        )
        self.add_item(self.interval)

        self.start_time = discord.ui.TextInput(
            label="Fixed Start Time (UTC, optional)",
            placeholder="HH:MM (e.g., 14:00) or leave empty",
            default=default_start_time,
            required=False,
            max_length=5
        )
        self.add_item(self.start_time)

    async def on_submit(self, interaction: discord.Interaction):
        self.interaction = interaction


class AddAllianceModal(discord.ui.Modal):
    """Single-field alliance creator. Inserts a new alliance with safe defaults
    (interval=60min, no channel, no start time). Channels and sync settings are
    configured post-creation via Channel Setup / Sync Settings."""

    DEFAULT_INTERVAL_MINUTES = 60

    def __init__(self, cog):
        super().__init__(title="Add Alliance")
        self.cog = cog
        self.name_input = discord.ui.TextInput(
            label="Alliance Name",
            placeholder="Enter the new alliance name",
            required=True,
            max_length=50,
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        alliance_name = self.name_input.value.strip()
        if not alliance_name:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance name cannot be empty.", ephemeral=True
            )
            return

        try:
            self.cog.c.execute(
                "SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,)
            )
            if self.cog.c.fetchone():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An alliance named **{alliance_name}** already exists.",
                    ephemeral=True,
                )
                return

            self.cog.c.execute(
                "INSERT INTO alliance_list (name, discord_server_id) VALUES (?, ?)",
                (alliance_name, interaction.guild.id if interaction.guild else None),
            )
            alliance_id = self.cog.c.lastrowid
            self.cog.c.execute(
                "INSERT INTO alliancesettings (alliance_id, channel_id, interval, start_time) "
                "VALUES (?, NULL, ?, NULL)",
                (alliance_id, self.DEFAULT_INTERVAL_MINUTES),
            )
            self.cog.conn.commit()

            self.cog.c_giftcode.execute(
                "INSERT INTO giftcodecontrol (alliance_id, status) VALUES (?, 1)",
                (alliance_id,),
            )
            self.cog.conn_giftcode.commit()

            # Drop the user straight onto the new alliance's hub — no
            # intermediate "created" ephemeral. The hub itself confirms the
            # alliance exists, and Channel Setup is one click away.
            main_menu = self.cog.bot.get_cog("MainMenu")
            if main_menu:
                await main_menu.show_alliance_hub(interaction, alliance_id)
            else:
                await interaction.response.send_message(
                    f"{theme.verifiedIcon} Alliance **{alliance_name}** "
                    f"(ID `{alliance_id}`) created.",
                    ephemeral=True,
                )

        except Exception as e:
            logger.error(f"Error creating alliance '{alliance_name}': {e}")
            print(f"Error creating alliance '{alliance_name}': {e}")
            try:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Failed to create alliance: {e}",
                    ephemeral=True,
                )
            except Exception:
                pass


class EditNameModal(discord.ui.Modal):
    """Single-field alliance name editor. Updates alliance_list.name on submit."""

    def __init__(self, alliance_id: int, current_name: str, conn):
        super().__init__(title="Edit Alliance Name")
        self.alliance_id = alliance_id
        self.conn = conn
        self.name_input = discord.ui.TextInput(
            label="Alliance Name",
            placeholder="Enter the new alliance name",
            default=current_name,
            required=True,
            max_length=50,
        )
        self.add_item(self.name_input)

    async def on_submit(self, interaction: discord.Interaction):
        new_name = self.name_input.value.strip()
        if not new_name:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Alliance name cannot be empty.", ephemeral=True
            )
            return
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE alliance_list SET name = ? WHERE alliance_id = ?",
                (new_name, self.alliance_id),
            )
            self.conn.commit()

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Alliance Renamed",
                description=(
                    f"{theme.allianceIcon} **Name:** {new_name}\n"
                    f"{theme.fidIcon} **ID:** {self.alliance_id}"
                ),
                color=theme.emColor3,
            )
            await interaction.response.edit_message(embed=embed, view=None)
        except Exception as e:
            logger.error(f"Error renaming alliance {self.alliance_id}: {e}")
            print(f"Error renaming alliance {self.alliance_id}: {e}")
            try:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Failed to rename alliance.", ephemeral=True
                )
            except Exception:
                pass

class PaginatedDeleteView(discord.ui.View):
    def __init__(self, pages, original_callback):
        super().__init__(timeout=7200)
        self.current_page = 0
        self.pages = pages
        self.original_callback = original_callback
        self.total_pages = len(pages)
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        select = discord.ui.Select(
            placeholder=f"Select alliance to delete ({self.current_page + 1}/{self.total_pages})",
            options=self.pages[self.current_page]
        )
        select.callback = self.original_callback
        self.add_item(select)
        
        previous_button = discord.ui.Button(
            label="",
            emoji=f"{theme.prevIcon}",
            style=discord.ButtonStyle.grey,
            custom_id="previous",
            disabled=(self.current_page == 0)
        )
        previous_button.callback = self.previous_callback
        self.add_item(previous_button)

        next_button = discord.ui.Button(
            label="",
            emoji=f"{theme.nextIcon}",
            style=discord.ButtonStyle.grey,
            custom_id="next",
            disabled=(self.current_page == len(self.pages) - 1)
        )
        next_button.callback = self.next_callback
        self.add_item(next_button)

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page - 1) % len(self.pages)
        self.update_view()
        
        embed = discord.Embed(
            title=f"{theme.trashIcon} Delete Alliance",
            description=(
                f"**{theme.warnIcon} Warning: This action cannot be undone!**\n"
                f"{theme.upperDivider}\n"
                f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor2
        )
        embed.set_footer(text=f"{theme.warnIcon} Warning: Deleting an alliance will remove all its data!")
        embed.timestamp = discord.utils.utcnow()

        await interaction.response.edit_message(embed=embed, view=self)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page + 1) % len(self.pages)
        self.update_view()

        embed = discord.Embed(
            title=f"{theme.trashIcon} Delete Alliance",
            description=(
                f"**{theme.warnIcon} Warning: This action cannot be undone!**\n"
                f"{theme.upperDivider}\n"
                f"{theme.num1Icon} Select an alliance from the dropdown menu\n"
                f"{theme.num2Icon} Use {theme.prevIcon} {theme.nextIcon} buttons to navigate between pages\n\n"
                f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor2
        )
        embed.set_footer(text=f"{theme.warnIcon} Warning: Deleting an alliance will remove all its data!")
        embed.timestamp = discord.utils.utcnow()

        await interaction.response.edit_message(embed=embed, view=self)

class PaginatedChannelView(discord.ui.View):
    def __init__(self, channels, original_callback):
        super().__init__(timeout=7200)
        self.current_page = 0
        self.channels = channels
        self.original_callback = original_callback
        self.items_per_page = 25
        self.pages = [channels[i:i + self.items_per_page] for i in range(0, len(channels), self.items_per_page)]
        self.total_pages = len(self.pages)
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        current_channels = self.pages[self.current_page]
        # Build options list without nested f-strings for Python 3.9+ compatibility
        channel_options = []
        for channel in current_channels:
            channel_label = f"#{channel.name}"[:100]
            # Determine description based on channel name length
            if len(f"#{channel.name}") > 40:
                option_description = f"Channel ID: {channel.id}"
            else:
                option_description = None

            channel_options.append(discord.SelectOption(
                label=channel_label,
                value=str(channel.id),
                description=option_description,
                emoji=theme.announceIcon
            ))
        
        select = discord.ui.Select(
            placeholder=f"Select channel ({self.current_page + 1}/{self.total_pages})",
            options=channel_options
        )
        select.callback = self.original_callback
        self.add_item(select)
        
        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="",
                emoji=f"{theme.prevIcon}",
                style=discord.ButtonStyle.grey,
                custom_id="previous",
                disabled=(self.current_page == 0)
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="",
                emoji=f"{theme.nextIcon}",
                style=discord.ButtonStyle.grey,
                custom_id="next",
                disabled=(self.current_page == len(self.pages) - 1)
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page - 1) % len(self.pages)
        self.update_view()
        
        embed = interaction.message.embeds[0]
        embed.description = (
            f"**Page:** {self.current_page + 1}/{self.total_pages}\n"
            f"**Total Channels:** {len(self.channels)}\n\n"
            "Please select a channel from the menu below."
        )
        
        await interaction.response.edit_message(embed=embed, view=self)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page + 1) % len(self.pages)
        self.update_view()
        
        embed = interaction.message.embeds[0]
        embed.description = (
            f"**Page:** {self.current_page + 1}/{self.total_pages}\n"
            f"**Total Channels:** {len(self.channels)}\n\n"
            "Please select a channel from the menu below."
        )
        
        await interaction.response.edit_message(embed=embed, view=self)

async def setup(bot):
    conn = sqlite3.connect('db/alliance.sqlite')
    await bot.add_cog(Alliance(bot, conn))