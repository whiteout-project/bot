"""
Alliance log viewer. Displays alliance control message history and member activity.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from datetime import datetime
from .alliance_member_operations import AllianceSelectView
from .alliance import PaginatedChannelView
from .pimp_my_bot import theme, safe_edit_message

logger = logging.getLogger('alliance')

class AllianceLogs(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.settings_db = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.settings_cursor = self.settings_db.cursor()

        self.alliance_db = sqlite3.connect('db/alliance.sqlite', timeout=30.0, check_same_thread=False)
        self.alliance_cursor = self.alliance_db.cursor()
        
        self.setup_database()

    def setup_database(self):
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS alliance_logs (
                    alliance_id INTEGER PRIMARY KEY,
                    channel_id INTEGER,
                    FOREIGN KEY (alliance_id) REFERENCES alliance_list (alliance_id)
                )
            """)
            
            self.settings_db.commit()
                
        except Exception as e:
            logger.error(f"Error setting up log system database: {e}")
            print(f"Error setting up log system database: {e}")

    def cog_unload(self):
        try:
            self.settings_db.close()
            self.alliance_db.close()
        except Exception:
            pass

    async def show_activity_log_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: show + manage the activity log channel for one alliance."""
        try:
            self.alliance_cursor.execute(
                "SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,)
            )
            row = self.alliance_cursor.fetchone()
            if not row:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance not found.", ephemeral=True
                )
                return
            alliance_name = row[0]

            self.settings_cursor.execute(
                "SELECT channel_id FROM alliance_logs WHERE alliance_id = ?",
                (alliance_id,),
            )
            ch_row = self.settings_cursor.fetchone()
            current_channel_id = ch_row[0] if ch_row else None
            current_channel = (
                interaction.guild.get_channel(int(current_channel_id))
                if current_channel_id else None
            )

            if current_channel:
                state_line = f"{theme.verifiedIcon} Current channel: {current_channel.mention}"
            elif current_channel_id:
                state_line = f"{theme.warnIcon} Configured channel `{current_channel_id}` is no longer accessible."
            else:
                state_line = f"{theme.deniedIcon} No activity log channel configured for this alliance."

            embed = discord.Embed(
                title=f"{theme.documentIcon} {alliance_name} — Activity Log",
                description=(
                    f"Member additions, removals, and other alliance changes are "
                    f"posted to the configured channel.\n"
                    f"{theme.upperDivider}\n"
                    f"{state_line}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            view = AllianceActivityLogView(self, alliance_id, alliance_name, bool(current_channel_id))
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_activity_log_for: {e}")
            print(f"Error in show_activity_log_for: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while loading the activity log.",
                    ephemeral=True,
                )

    async def show_log_menu(self, interaction: discord.Interaction):
        """Display the log system menu - called by MainMenu cog."""
        try:
            from .permission_handler import PermissionManager
            is_admin, _ = PermissionManager.is_admin(interaction.user.id)

            if not is_admin:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only administrators can access the log system.",
                    ephemeral=True
                )
                return

            log_embed = discord.Embed(
                title=f"{theme.documentIcon} Alliance Log System",
                description=(
                    f"Configure log channels for alliance activity:\n\n"
                    f"**Available Options**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.editListIcon} **Set Log Channel**\n"
                    f"└ Assign a log channel to an alliance\n\n"
                    f"{theme.trashIcon} **Remove Log Channel**\n"
                    f"└ Remove alliance log channel\n\n"
                    f"{theme.chartIcon} **View Log Channels**\n"
                    f"└ List all alliance log channels\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Set Log Channel",
                emoji=f"{theme.editListIcon}",
                style=discord.ButtonStyle.primary,
                custom_id="set_log_channel",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Remove Log Channel",
                emoji=f"{theme.trashIcon}",
                style=discord.ButtonStyle.danger,
                custom_id="remove_log_channel",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="View Log Channels",
                emoji=f"{theme.chartIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="view_log_channels",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Back",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="back_from_logs_to_settings",
                row=1
            ))

            await safe_edit_message(interaction, embed=log_embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_log_menu: {e}")
            print(f"Error in show_log_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while accessing the log system.",
                    ephemeral=True
                )

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if not interaction.type == discord.InteractionType.component:
            return

        custom_id = interaction.data.get("custom_id", "")

        if custom_id == "back_from_logs_to_settings":
            main_menu_cog = self.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_alliance_management(interaction)
            return

        if custom_id == "set_log_channel":
            try:
                from .permission_handler import PermissionManager

                if interaction.guild is None:
                    await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
                    return

                is_admin, _ = PermissionManager.is_admin(interaction.user.id)
                if not is_admin:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Only administrators can set log channels.",
                        ephemeral=True
                    )
                    return

                # Get alliances this admin can access
                alliances, _ = PermissionManager.get_admin_alliances(interaction.user.id, interaction.guild.id)

                if not alliances:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No alliances found.", 
                        ephemeral=True
                    )
                    return

                alliances_with_counts = []
                for alliance_id, name in alliances:
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                        member_count = cursor.fetchone()[0]
                        alliances_with_counts.append((alliance_id, name, member_count))

                alliance_embed = discord.Embed(
                    title=f"{theme.editListIcon} Set Log Channel",
                    description=(
                        f"Please select an alliance:\n\n"
                        f"**Alliance List**\n"
                        f"{theme.upperDivider}\n"
                        f"Select an alliance from the list below:\n"
                    ),
                    color=theme.emColor1
                )

                view = AllianceSelectView(alliances_with_counts, self)
                view.callback = lambda i: alliance_callback(i, view)

                async def alliance_callback(select_interaction: discord.Interaction, alliance_view):
                    try:
                        alliance_id = int(alliance_view.current_select.values[0])
                        
                        channel_embed = discord.Embed(
                            title=f"{theme.editListIcon} Set Log Channel",
                            description=(
                                f"**Instructions:**\n"
                                f"{theme.upperDivider}\n"
                                f"Please select a channel for logging\n\n"
                                f"**Page:** 1/1\n"
                                f"**Total Channels:** {len(select_interaction.guild.text_channels)}"
                            ),
                            color=theme.emColor1
                        )

                        async def channel_select_callback(channel_interaction: discord.Interaction):
                            try:
                                channel_id = int(channel_interaction.data["values"][0])
                                
                                self.settings_cursor.execute("""
                                    INSERT OR REPLACE INTO alliance_logs (alliance_id, channel_id)
                                    VALUES (?, ?)
                                """, (alliance_id, channel_id))
                                self.settings_db.commit()

                                self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                                alliance_name = self.alliance_cursor.fetchone()[0]

                                success_embed = discord.Embed(
                                    title=f"{theme.verifiedIcon} Log Channel Set",
                                    description=(
                                        f"Successfully set log channel:\n\n"
                                        f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                                        f"{theme.editListIcon} **Channel:** <#{channel_id}>\n"
                                    ),
                                    color=discord.Color.green()
                                )

                                await channel_interaction.response.edit_message(
                                    embed=success_embed,
                                    view=None
                                )

                            except Exception as e:
                                logger.error(f"Error setting log channel: {e}")
                                print(f"Error setting log channel: {e}")
                                await channel_interaction.response.send_message(
                                    f"{theme.deniedIcon} An error occurred while setting the log channel.",
                                    ephemeral=True
                                )

                        channels = select_interaction.guild.text_channels
                        channel_view = PaginatedChannelView(channels, channel_select_callback)

                        if not select_interaction.response.is_done():
                            await select_interaction.response.edit_message(
                                embed=channel_embed,
                                view=channel_view
                            )
                        else:
                            await select_interaction.message.edit(
                                embed=channel_embed,
                                view=channel_view
                            )

                    except Exception as e:
                        logger.error(f"Error in alliance selection: {e}")
                        print(f"Error in alliance selection: {e}")
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

                await interaction.response.send_message(
                    embed=alliance_embed,
                    view=view,
                    ephemeral=True
                )

            except Exception as e:
                logger.error(f"Error in set log channel: {e}")
                print(f"Error in set log channel: {e}")
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while setting up the log channel.",
                    ephemeral=True
                )

        elif custom_id == "remove_log_channel":
            try:
                from .permission_handler import PermissionManager

                if interaction.guild is None:
                    await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
                    return

                is_admin, _ = PermissionManager.is_admin(interaction.user.id)
                if not is_admin:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Only administrators can remove log channels.",
                        ephemeral=True
                    )
                    return

                # Get alliances this admin can access
                admin_alliances, _ = PermissionManager.get_admin_alliances(interaction.user.id, interaction.guild.id)
                admin_alliance_ids = [a[0] for a in admin_alliances] if admin_alliances else []

                if not admin_alliance_ids:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No alliances found.",
                        ephemeral=True
                    )
                    return

                # Get log entries only for alliances admin can access
                placeholders = ','.join('?' * len(admin_alliance_ids))
                self.settings_cursor.execute(f"""
                    SELECT al.alliance_id, al.channel_id
                    FROM alliance_logs al
                    WHERE al.alliance_id IN ({placeholders})
                """, admin_alliance_ids)
                log_entries = self.settings_cursor.fetchall()

                if not log_entries:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No alliance log channels found.",
                        ephemeral=True
                    )
                    return

                alliances_with_counts = []
                for alliance_id, channel_id in log_entries:
                    self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_result = self.alliance_cursor.fetchone()
                    alliance_name = alliance_result[0] if alliance_result else "Unknown Alliance"

                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                        member_count = cursor.fetchone()[0]
                        alliances_with_counts.append((alliance_id, alliance_name, member_count))

                if not alliances_with_counts:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No valid log channels found.", 
                        ephemeral=True
                    )
                    return

                remove_embed = discord.Embed(
                    title=f"{theme.trashIcon} Remove Log Channel",
                    description=(
                        f"Select an alliance to remove its log channel:\n\n"
                        f"**Current Log Channels**\n"
                        f"{theme.upperDivider}\n"
                        f"Select an alliance from the list below:\n"
                    ),
                    color=discord.Color.red()
                )

                view = AllianceSelectView(alliances_with_counts, self)

                async def alliance_callback(select_interaction: discord.Interaction):
                    try:
                        alliance_id = int(view.current_select.values[0])
                        
                        self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                        alliance_name = self.alliance_cursor.fetchone()[0]
                        
                        self.settings_cursor.execute("SELECT channel_id FROM alliance_logs WHERE alliance_id = ?", (alliance_id,))
                        channel_id = self.settings_cursor.fetchone()[0]
                        
                        confirm_embed = discord.Embed(
                            title=f"{theme.warnIcon} Confirm Removal",
                            description=(
                                f"Are you sure you want to remove the log channel for:\n\n"
                                f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                                f"{theme.editListIcon} **Channel:** <#{channel_id}>\n\n"
                                f"This action cannot be undone!"
                            ),
                            color=discord.Color.yellow()
                        )

                        confirm_view = discord.ui.View()
                        
                        async def confirm_callback(button_interaction: discord.Interaction):
                            try:
                                self.settings_cursor.execute("""
                                    DELETE FROM alliance_logs 
                                    WHERE alliance_id = ?
                                """, (alliance_id,))
                                self.settings_db.commit()

                                success_embed = discord.Embed(
                                    title=f"{theme.verifiedIcon} Log Channel Removed",
                                    description=(
                                        f"Successfully removed log channel for:\n\n"
                                        f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                                        f"{theme.editListIcon} **Channel:** <#{channel_id}>"
                                    ),
                                    color=discord.Color.green()
                                )

                                await button_interaction.response.edit_message(
                                    embed=success_embed,
                                    view=None
                                )

                            except Exception as e:
                                logger.error(f"Error removing log channel: {e}")
                                print(f"Error removing log channel: {e}")
                                await button_interaction.response.send_message(
                                    f"{theme.deniedIcon} An error occurred while removing the log channel.",
                                    ephemeral=True
                                )

                        async def cancel_callback(button_interaction: discord.Interaction):
                            cancel_embed = discord.Embed(
                                title=f"{theme.deniedIcon} Removal Cancelled",
                                description="The log channel removal has been cancelled.",
                                color=discord.Color.red()
                            )
                            await button_interaction.response.edit_message(
                                embed=cancel_embed,
                                view=None
                            )

                        confirm_button = discord.ui.Button(
                            label="Confirm",
                            emoji=f"{theme.verifiedIcon}",
                            style=discord.ButtonStyle.danger,
                            custom_id="confirm_remove"
                        )
                        confirm_button.callback = confirm_callback

                        cancel_button = discord.ui.Button(
                            label="Cancel",
                            emoji=f"{theme.deniedIcon}",
                            style=discord.ButtonStyle.secondary,
                            custom_id="cancel_remove"
                        )
                        cancel_button.callback = cancel_callback

                        confirm_view.add_item(confirm_button)
                        confirm_view.add_item(cancel_button)

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
                        logger.error(f"Error in alliance selection: {e}")
                        print(f"Error in alliance selection: {e}")
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
                    embed=remove_embed,
                    view=view,
                    ephemeral=True
                )

            except Exception as e:
                logger.error(f"Error in remove log channel: {e}")
                print(f"Error in remove log channel: {e}")
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while setting up the removal menu.",
                    ephemeral=True
                )

        elif custom_id == "view_log_channels":
            try:
                from .permission_handler import PermissionManager

                if interaction.guild is None:
                    await interaction.response.send_message(f"{theme.deniedIcon} This command must be used in a server.", ephemeral=True)
                    return

                is_admin, _ = PermissionManager.is_admin(interaction.user.id)
                if not is_admin:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Only administrators can view log channels.",
                        ephemeral=True
                    )
                    return

                # Get alliances this admin can access
                admin_alliances, _ = PermissionManager.get_admin_alliances(interaction.user.id, interaction.guild.id)
                admin_alliance_ids = [a[0] for a in admin_alliances] if admin_alliances else []

                if not admin_alliance_ids:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No alliances found.",
                        ephemeral=True
                    )
                    return

                # Get log entries only for alliances admin can access
                placeholders = ','.join('?' * len(admin_alliance_ids))
                self.settings_cursor.execute(f"""
                    SELECT alliance_id, channel_id
                    FROM alliance_logs
                    WHERE alliance_id IN ({placeholders})
                    ORDER BY alliance_id
                """, admin_alliance_ids)
                log_entries = self.settings_cursor.fetchall()

                if not log_entries:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} No alliance log channels found.", 
                        ephemeral=True
                    )
                    return

                list_embed = discord.Embed(
                    title=f"{theme.chartIcon} Alliance Log Channels",
                    description="Current log channel assignments:\n\n",
                    color=theme.emColor1
                )

                for alliance_id, channel_id in log_entries:
                    self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_result = self.alliance_cursor.fetchone()
                    alliance_name = alliance_result[0] if alliance_result else "Unknown Alliance"

                    channel = interaction.guild.get_channel(channel_id)
                    channel_name = channel.name if channel else "Unknown Channel"

                    list_embed.add_field(
                        name=f"{theme.allianceIcon} Alliance ID: {alliance_id}",
                        value=(
                            f"**Name:** {alliance_name}\n"
                            f"**Log Channel:** <#{channel_id}>\n"
                            f"**Channel ID:** {channel_id}\n"
                            f"**Channel Name:** #{channel_name}\n"
                            f"{theme.lowerDivider}"
                        ),
                        inline=False
                    )

                view = discord.ui.View()
                view.add_item(discord.ui.Button(
                    label="Back",
                    emoji=f"{theme.prevIcon}",
                    style=discord.ButtonStyle.secondary,
                    custom_id="log_system",
                    row=0
                ))

                await interaction.response.send_message(
                    embed=list_embed,
                    view=view,
                    ephemeral=True
                )

            except Exception as e:
                logger.error(f"Error in view log channels: {e}")
                print(f"Error in view log channels: {e}")
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while viewing log channels.",
                    ephemeral=True
                )

class AllianceActivityLogView(discord.ui.View):
    """Per-alliance activity log management — alliance is already known."""

    def __init__(self, cog, alliance_id: int, alliance_name: str, has_channel: bool):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.has_channel = has_channel

        set_btn = discord.ui.Button(
            label="Change Channel" if has_channel else "Set Channel",
            emoji=f"{theme.editListIcon}",
            style=discord.ButtonStyle.primary,
            row=0,
        )
        set_btn.callback = self._on_set
        self.add_item(set_btn)

        if has_channel:
            remove_btn = discord.ui.Button(
                label="Remove Channel",
                emoji=f"{theme.trashIcon}",
                style=discord.ButtonStyle.danger,
                row=0,
            )
            remove_btn.callback = self._on_remove
            self.add_item(remove_btn)

        back_btn = discord.ui.Button(
            label="Back to Hub",
            emoji=f"{theme.backIcon}",
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_set(self, interaction: discord.Interaction):
        cog = self.cog
        alliance_id = self.alliance_id
        alliance_name = self.alliance_name

        class _ChannelSelect(discord.ui.ChannelSelect):
            def __init__(self):
                super().__init__(
                    placeholder="Pick a channel for this alliance's activity log",
                    channel_types=[discord.ChannelType.text],
                )

            async def callback(self, channel_interaction: discord.Interaction):
                selected = self.values[0]
                try:
                    cog.settings_cursor.execute(
                        "INSERT OR REPLACE INTO alliance_logs (alliance_id, channel_id) "
                        "VALUES (?, ?)",
                        (alliance_id, selected.id),
                    )
                    cog.settings_db.commit()
                except Exception as e:
                    logger.error(f"Activity log set error: {e}")
                    print(f"Activity log set error: {e}")
                    await channel_interaction.response.send_message(
                        f"{theme.deniedIcon} Failed to set the log channel.",
                        ephemeral=True,
                    )
                    return
                await cog.show_activity_log_for(channel_interaction, alliance_id)

        select_view = discord.ui.View(timeout=300)
        select_view.add_item(_ChannelSelect())
        select_embed = discord.Embed(
            title=f"{theme.documentIcon} {alliance_name} — Pick Activity Log Channel",
            description="Pick the text channel where alliance activity will be logged.",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=select_embed, view=select_view)

    async def _on_remove(self, interaction: discord.Interaction):
        try:
            self.cog.settings_cursor.execute(
                "DELETE FROM alliance_logs WHERE alliance_id = ?",
                (self.alliance_id,),
            )
            self.cog.settings_db.commit()
        except Exception as e:
            logger.error(f"Activity log remove error: {e}")
            print(f"Activity log remove error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to remove the log channel.",
                ephemeral=True,
            )
            return
        await self.cog.show_activity_log_for(interaction, self.alliance_id)

    async def _on_back(self, interaction: discord.Interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu and hasattr(main_menu, "show_alliance_hub"):
            await main_menu.show_alliance_hub(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hub not available.", ephemeral=True
            )


async def setup(bot):
    await bot.add_cog(AllianceLogs(bot))
