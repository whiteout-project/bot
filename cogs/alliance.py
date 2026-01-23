import discord
from discord import app_commands
from discord.ext import commands
import sqlite3
import asyncio
from datetime import datetime
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

class Alliance(commands.Cog):
    def __init__(self, bot, conn):
        self.bot = bot
        self.conn = conn
        self.c = self.conn.cursor()
        
        self.conn_users = sqlite3.connect('db/users.sqlite')
        self.c_users = self.conn_users.cursor()
        
        self.conn_settings = sqlite3.connect('db/settings.sqlite')
        self.c_settings = self.conn_settings.cursor()
        
        self.conn_giftcode = sqlite3.connect('db/giftcode.sqlite')
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
                
                interval_text = f"{interval} minutes" if interval > 0 else "No automatic control"
                alliance_list += f"{theme.allianceIcon} **{alliance_id}: {name}**\n{theme.userIcon} Members: {member_count}\n{theme.timeIcon} Control Interval: {interval_text}\n\n"

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

            embed = discord.Embed(
                title=f"{theme.settingsIcon} Settings Menu",
                description=(
                    f"Please select a category:\n\n"
                    f"**Menu Categories**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance Operations**\n"
                    f"└ Manage alliances and settings\n\n"
                    f"{theme.membersIcon} **Alliance Member Operations**\n"
                    f"└ Add, remove, and view members\n\n"
                    f"{theme.robotIcon} **Bot Operations**\n"
                    f"└ Configure bot settings\n\n"
                    f"{theme.giftIcon} **Gift Code Operations**\n"
                    f"└ Manage gift codes and rewards\n\n"
                    f"{theme.listIcon} **Alliance History**\n"
                    f"└ View alliance changes and history\n\n"
                    f"{theme.supportIcon} **Support Operations**\n"
                    f"└ Access support features\n\n"
                    f"{theme.paletteIcon} **Theme Settings**\n"
                    f"└ Customize bot icons and colors\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Alliance Operations",
                emoji=theme.allianceIcon,
                style=discord.ButtonStyle.primary,
                custom_id="alliance_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Member Operations",
                emoji=theme.membersIcon,
                style=discord.ButtonStyle.primary,
                custom_id="member_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Bot Operations",
                emoji=theme.robotIcon,
                style=discord.ButtonStyle.primary,
                custom_id="bot_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Gift Operations",
                emoji=theme.giftIcon,
                style=discord.ButtonStyle.primary,
                custom_id="gift_code_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Alliance History",
                emoji=theme.listIcon,
                style=discord.ButtonStyle.primary,
                custom_id="alliance_history",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Support Operations",
                emoji=theme.supportIcon,
                style=discord.ButtonStyle.primary,
                custom_id="support_operations",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Other Features",
                emoji=theme.settingsIcon,
                style=discord.ButtonStyle.primary,
                custom_id="other_features",
                row=3
            ))
            view.add_item(discord.ui.Button(
                label="Theme Settings",
                emoji=f"{theme.paletteIcon}",
                style=discord.ButtonStyle.primary,
                custom_id="theme_settings",
                row=3
            ))

            if admin_count == 0:
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, view=view)

        except Exception as e:
            if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                print(f"Settings command error: {e}")
            error_message = "An error occurred while processing your request."
            if not interaction.response.is_done():
                await interaction.response.send_message(error_message, ephemeral=True)
            else:
                await interaction.followup.send(error_message, ephemeral=True)

    async def show_main_menu(self, interaction: discord.Interaction):
        """Display the main settings menu - can be called by other cogs"""
        try:
            embed = discord.Embed(
                title=f"{theme.settingsIcon} Settings Menu",
                description=(
                    f"Please select a category:\n\n"
                    f"**Menu Categories**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance Operations**\n"
                    f"└ Manage alliances and settings\n\n"
                    f"{theme.membersIcon} **Alliance Member Operations**\n"
                    f"└ Add, remove, and view members\n\n"
                    f"{theme.robotIcon} **Bot Operations**\n"
                    f"└ Configure bot settings\n\n"
                    f"{theme.giftIcon} **Gift Code Operations**\n"
                    f"└ Manage gift codes and rewards\n\n"
                    f"{theme.listIcon} **Alliance History**\n"
                    f"└ View alliance changes and history\n\n"
                    f"{theme.supportIcon} **Support Operations**\n"
                    f"└ Access support features\n\n"
                    f"{theme.paletteIcon} **Theme Settings**\n"
                    f"└ Customize bot icons and colors\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Alliance Operations",
                emoji=theme.allianceIcon,
                style=discord.ButtonStyle.primary,
                custom_id="alliance_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Member Operations",
                emoji=theme.membersIcon,
                style=discord.ButtonStyle.primary,
                custom_id="member_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Bot Operations",
                emoji=theme.robotIcon,
                style=discord.ButtonStyle.primary,
                custom_id="bot_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Gift Operations",
                emoji=theme.giftIcon,
                style=discord.ButtonStyle.primary,
                custom_id="gift_code_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Alliance History",
                emoji=theme.listIcon,
                style=discord.ButtonStyle.primary,
                custom_id="alliance_history",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Support Operations",
                emoji=theme.supportIcon,
                style=discord.ButtonStyle.primary,
                custom_id="support_operations",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Other Features",
                emoji=theme.settingsIcon,
                style=discord.ButtonStyle.primary,
                custom_id="other_features",
                row=3
            ))
            view.add_item(discord.ui.Button(
                label="Theme Settings",
                emoji=f"{theme.paletteIcon}",
                style=discord.ButtonStyle.primary,
                custom_id="theme_settings",
                row=3
            ))

            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                pass

        except Exception as _:
            pass

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get("custom_id")
            user_id = interaction.user.id
            self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
            admin = self.c_settings.fetchone()

            if admin is None:
                await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                return

            try:
                if custom_id == "alliance_operations":
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
                    
                    # All admins (global and server) can manage alliances
                    # Server admins are scoped to their server's alliances in the handlers
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
                        label="Main Menu", 
                        emoji=theme.homeIcon,
                        style=discord.ButtonStyle.secondary, 
                        custom_id="main_menu"
                    ))

                    await interaction.response.edit_message(embed=embed, view=view)

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
                            description=f"Control Interval: {interval} minutes"
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
                            control_cog = self.bot.get_cog('Control')
                            
                            if not control_cog:
                                await select_interaction.response.send_message("Control module not found.", ephemeral=True)
                                return
                            
                            # Ensure the centralized queue processor is running
                            await control_cog.login_handler.start_queue_processor()
                            
                            if selected_value == "all":
                                # Get initial queue position
                                queue_info = control_cog.login_handler.get_queue_info()
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
                                await select_interaction.response.send_message(embed=progress_embed, ephemeral=True)
                                msg = await select_interaction.original_response()
                                message_id = msg.id

                                # Queue all alliance operations at once
                                queued_alliances = []
                                for index, (alliance_id, name, _) in enumerate(alliances):
                                    try:
                                        self.c.execute("""
                                            SELECT channel_id FROM alliancesettings WHERE alliance_id = ?
                                        """, (alliance_id,))
                                        channel_data = self.c.fetchone()
                                        channel = self.bot.get_channel(channel_data[0]) if channel_data else select_interaction.channel
                                        
                                        # For all alliances, we'll pass the message and track which alliance
                                        await control_cog.login_handler.queue_operation({
                                            'type': 'alliance_control',
                                            'callback': lambda ch=channel, aid=alliance_id, im=msg, an=name, qa=queued_alliances, idx=index: control_cog.check_agslist(
                                                ch, aid, 
                                                interaction_message=im, 
                                                alliance_name=an,
                                                is_batch=True,
                                                batch_info={'current': idx + 1, 'total': len(alliances), 'all_names': qa}
                                            ),
                                            'description': f'Manual control check for alliance {name}',
                                            'alliance_id': alliance_id,
                                            'interaction_message': msg
                                        })
                                        queued_alliances.append((alliance_id, name))
                                    
                                    except Exception as e:
                                        print(f"Error queuing alliance {name}: {e}")
                                        continue
                                
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
                                channel = self.bot.get_channel(channel_id) if channel_id else select_interaction.channel
                                
                                # Get queue info for position
                                queue_info = control_cog.login_handler.get_queue_info()
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
                                
                                await control_cog.login_handler.queue_operation({
                                    'type': 'alliance_control',
                                    'callback': lambda ch=channel, aid=alliance_id, im=msg, an=alliance_name: control_cog.check_agslist(ch, aid, interaction_message=im, alliance_name=an),
                                    'description': f'Manual control check for alliance {alliance_name}',
                                    'alliance_id': alliance_id,
                                    'interaction_message': msg
                                })

                        except Exception as e:
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

                elif custom_id == "member_operations":
                    await self.bot.get_cog("AllianceMemberOperations").handle_member_operations(interaction)

                elif custom_id == "bot_operations":
                    try:
                        bot_ops_cog = interaction.client.get_cog("BotOperations")
                        if bot_ops_cog:
                            await bot_ops_cog.show_bot_operations_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon}Bot Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Bot operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Bot Operations.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Bot Operations.",
                                ephemeral=True
                            )

                elif custom_id == "gift_code_operations":
                    try:
                        gift_ops_cog = interaction.client.get_cog("GiftOperations")
                        if gift_ops_cog:
                            await gift_ops_cog.show_gift_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon}Gift Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        print(f"Gift operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Gift Operations.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Gift Operations.",
                                ephemeral=True
                            )

                elif custom_id == "add_alliance":
                    await self.add_alliance(interaction)

                elif custom_id == "delete_alliance":
                    await self.delete_alliance(interaction)

                elif custom_id == "view_alliances":
                    await self.view_alliances(interaction)

                elif custom_id == "main_menu":
                    await self.show_main_menu(interaction)

                elif custom_id == "support_operations":
                    try:
                        support_ops_cog = interaction.client.get_cog("SupportOperations")
                        if support_ops_cog:
                            await support_ops_cog.show_support_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon}Support Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Support operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Support Operations.", 
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Support Operations.",
                                ephemeral=True
                            )

                elif custom_id == "alliance_history":
                    try:
                        changes_cog = interaction.client.get_cog("Changes")
                        if changes_cog:
                            await changes_cog.show_alliance_history_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon}Alliance History module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        print(f"Alliance history error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Alliance History.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Alliance History.",
                                ephemeral=True
                            )

                elif custom_id == "other_features":
                    try:
                        other_features_cog = interaction.client.get_cog("OtherFeatures")
                        if other_features_cog:
                            await other_features_cog.show_other_features_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon}Other Features module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Other features error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Other Features menu.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Other Features menu.",
                                ephemeral=True
                            )

                elif custom_id == "theme_settings":
                    try:
                        theme_cog = interaction.client.get_cog("Theme")
                        if theme_cog:
                            await theme_cog.show_theme_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"{theme.deniedIcon} Theme module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Theme settings error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Theme settings.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Theme settings.",
                                ephemeral=True
                            )

            except Exception as e:
                if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                    print(f"Error processing interaction with custom_id '{custom_id}': {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your request. Please try again.",
                        ephemeral=True
                    )

    async def add_alliance(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Please perform this action in a Discord channel.", ephemeral=True)
            return

        modal = AllianceModal(title="Add Alliance")
        await interaction.response.send_modal(modal)
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
                title="Channel Selection",
                description=(
                    f"**Instructions:**\n"
                    f"{theme.upperDivider}\n"
                    f"Please select a channel for the alliance\n\n"
                    f"**Page:** 1/1\n"
                    f"**Total Channels:** {len(interaction.guild.text_channels)}"
                ),
                color=theme.emColor1
            )

            async def channel_select_callback(select_interaction: discord.Interaction):
                try:
                    self.c.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                    existing_alliance = self.c.fetchone()

                    if existing_alliance:
                        error_embed = discord.Embed(
                            title="Error",
                            description="An alliance with this name already exists.",
                            color=theme.emColor2
                        )
                        await select_interaction.response.edit_message(embed=error_embed, view=None)
                        return

                    channel_id = int(select_interaction.data["values"][0])

                    self.c.execute("INSERT INTO alliance_list (name, discord_server_id) VALUES (?, ?)",
                                 (alliance_name, interaction.guild.id))
                    alliance_id = self.c.lastrowid
                    self.c.execute("INSERT INTO alliancesettings (alliance_id, channel_id, interval, start_time) VALUES (?, ?, ?, ?)",
                                 (alliance_id, channel_id, interval, start_time))
                    self.conn.commit()

                    self.c_giftcode.execute("""
                        INSERT INTO giftcodecontrol (alliance_id, status)
                        VALUES (?, 1)
                    """, (alliance_id,))
                    self.conn_giftcode.commit()

                    result_embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Alliance Successfully Created",
                        description="The alliance has been created with the following details:",
                        color=theme.emColor3
                    )

                    start_time_display = f"{start_time} UTC" if start_time else "Not set (starts on bot startup)"
                    info_section = (
                        f"**🛡️ Alliance Name**\n{alliance_name}\n\n"
                        f"**🔢 Alliance ID**\n{alliance_id}\n\n"
                        f"**📢 Channel**\n<#{channel_id}>\n\n"
                        f"**⏱️ Control Interval**\n{interval} minutes\n\n"
                        f"**🕐 Fixed Start Time**\n{start_time_display}"
                    )
                    result_embed.add_field(name="Alliance Details", value=info_section, inline=False)

                    result_embed.set_footer(text="Alliance settings have been successfully saved")
                    result_embed.timestamp = discord.utils.utcnow()

                    await select_interaction.response.edit_message(embed=result_embed, view=None)

                except Exception as e:
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Error creating alliance: {str(e)}",
                        color=theme.emColor2
                    )
                    await select_interaction.response.edit_message(embed=error_embed, view=None)

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
                                f"**⏱️ Control Interval**\n{interval} minutes\n\n"
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
            print(f"Error in delete_alliance: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon}Error",
                description="An error occurred while loading the delete menu.",
                color=theme.emColor2
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)

    async def alliance_delete_callback(self, interaction: discord.Interaction):
        try:
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
                    
                    await button_interaction.response.edit_message(embed=cleanup_embed, view=None)
                    
                except Exception as e:
                    error_embed = discord.Embed(
                        title=f"{theme.deniedIcon}Error",
                        description=f"An error occurred while deleting the alliance: {str(e)}",
                        color=theme.emColor2
                    )
                    await button_interaction.response.edit_message(embed=error_embed, view=None)

            async def cancel_callback(button_interaction: discord.Interaction):
                cancel_embed = discord.Embed(
                    title=f"{theme.deniedIcon}Deletion Cancelled",
                    description="Alliance deletion has been cancelled.",
                    color=theme.emColor4
                )
                await button_interaction.response.edit_message(embed=cancel_embed, view=None)

            confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.danger)
            cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.grey)
            confirm_button.callback = confirm_callback
            cancel_button.callback = cancel_callback
            confirm_view.add_item(confirm_button)
            confirm_view.add_item(cancel_button)

            await interaction.response.edit_message(embed=confirm_embed, view=confirm_view)

        except Exception as e:
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
            label="Control Interval (minutes)",
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