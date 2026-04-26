"""
Startup DM and orphan detection. Sends status reports to admins and detects orphaned users.
"""
import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
import logging
from datetime import datetime
from .pimp_my_bot import theme

logger = logging.getLogger('bot')


class OrphanViewBase(discord.ui.View):
    """Base class for orphan cleanup views with shared functionality."""

    def __init__(self, orphaned_users: dict, bot, cog):
        super().__init__(timeout=3600)
        self.orphaned_users = orphaned_users
        self.bot = bot
        self.cog = cog

    async def check_already_resolved(self, interaction: discord.Interaction) -> bool:
        """Check if another admin already handled this. Returns True if resolved."""
        if self.cog.orphan_resolution["resolved"]:
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Already Resolved",
                description=(
                    f"This issue was already handled by **{self.cog.orphan_resolution['resolved_by_name']}**.\n\n"
                    f"**Action taken:** {self.cog.orphan_resolution['action_taken']}\n"
                    f"**Time:** {self.cog.orphan_resolution['timestamp']}"
                ),
                color=theme.emColor3
            )
            await interaction.response.edit_message(embed=embed, view=None)
            return True
        return False

    def mark_resolved(self, user: discord.User, action: str):
        """Mark the issue as resolved by this admin."""
        self.cog.orphan_resolution = {
            "resolved": True,
            "resolved_by": user.id,
            "resolved_by_name": user.display_name,
            "action_taken": action,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }


class OrphanCleanupView(OrphanViewBase):
    """Interactive view for admin to manage orphaned users"""

    def get_main_embed(self) -> discord.Embed:
        """Build the main notification embed."""
        total_count = sum(len(users) for users in self.orphaned_users.values())
        alliance_list = "\n".join(
            f"• ID `{aid}`: {len(users)} users"
            for aid, users in self.orphaned_users.items()
        )
        return discord.Embed(
            title=f"{theme.warnIcon} Orphaned Users Detected",
            description=(
                f"Found **{total_count}** users linked to alliances that no longer exist.\n\n"
                "These users cannot be added to new alliances until resolved.\n\n"
                f"**Orphaned by Alliance ID:**\n{alliance_list}"
            ),
            color=theme.emColor2
        )

    @discord.ui.button(label="View Details", style=discord.ButtonStyle.primary, emoji=None, row=0)
    async def view_details(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        alliance_ids = list(self.orphaned_users.keys())
        if not alliance_ids:
            return

        self.viewing_alliance = alliance_ids[0]
        users = self.orphaned_users[self.viewing_alliance]

        user_list = "\n".join(f"• `{fid}` - {nickname}" for fid, nickname in users[:15])
        if len(users) > 15:
            user_list += f"\n... and {len(users) - 15} more"

        embed = discord.Embed(
            title=f"{theme.userIcon} Orphaned Users - Alliance ID: {self.viewing_alliance}",
            description=f"**{len(users)} users:**\n{user_list}",
            color=theme.emColor1
        )

        view = OrphanDetailView(self.orphaned_users, self.bot, self.cog, self.viewing_alliance)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Delete All", style=discord.ButtonStyle.danger, emoji=None, row=0)
    async def delete_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        total_count = sum(len(users) for users in self.orphaned_users.values())
        all_fids = [fid for users in self.orphaned_users.values() for fid, _ in users]

        with sqlite3.connect('db/users.sqlite') as conn:
            cursor = conn.cursor()
            for fid in all_fids:
                cursor.execute("DELETE FROM users WHERE fid = ?", (fid,))
            conn.commit()

        self.mark_resolved(interaction.user, f"Deleted {total_count} orphaned users")

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Cleanup Complete",
            description=(
                f"Removed **{total_count}** orphaned users.\n\n"
                "They can now be re-added to alliances."
            ),
            color=theme.emColor3
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Reassign All", style=discord.ButtonStyle.success, emoji=None, row=0)
    async def reassign_all(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        with sqlite3.connect('db/alliance.sqlite') as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name")
            alliances = cursor.fetchall()

        if not alliances:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} No Alliances Available",
                description="There are no alliances to reassign users to.",
                color=theme.emColor4
            )
            await interaction.response.edit_message(embed=embed, view=None)
            return

        view = ReassignSelectView(self.orphaned_users, self.bot, self.cog, alliances)
        total_count = sum(len(users) for users in self.orphaned_users.values())

        embed = discord.Embed(
            title=f"{theme.refreshIcon} Reassign Orphaned Users",
            description=f"Select target alliance for **{total_count}** users:",
            color=theme.emColor1
        )
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Dismiss", style=discord.ButtonStyle.secondary, emoji=None, row=0)
    async def dismiss(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        self.mark_resolved(interaction.user, "Dismissed without action")

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Dismissed",
            description="Orphaned users will remain in the database.\nYou can run `/cleanup orphans` later to address this.",
            color=theme.emColor1
        )
        await interaction.response.edit_message(embed=embed, view=None)


class OrphanDetailView(OrphanViewBase):
    """View for showing details of orphaned users per alliance."""

    def __init__(self, orphaned_users: dict, bot, cog, current_alliance: str):
        super().__init__(orphaned_users, bot, cog)
        self.alliance_ids = list(orphaned_users.keys())
        self.current_index = self.alliance_ids.index(current_alliance) if current_alliance in self.alliance_ids else 0

    def get_current_embed(self) -> discord.Embed:
        alliance_id = self.alliance_ids[self.current_index]
        users = self.orphaned_users[alliance_id]

        user_list = "\n".join(f"• `{fid}` - {nickname}" for fid, nickname in users[:15])
        if len(users) > 15:
            user_list += f"\n... and {len(users) - 15} more"

        return discord.Embed(
            title=f"{theme.userIcon} Orphaned Users - Alliance ID: {alliance_id} ({self.current_index + 1}/{len(self.alliance_ids)})",
            description=f"**{len(users)} users:**\n{user_list}",
            color=theme.emColor1
        )

    @discord.ui.button(label="Delete These", style=discord.ButtonStyle.danger, row=0)
    async def delete_these(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        alliance_id = self.alliance_ids[self.current_index]
        users = self.orphaned_users[alliance_id]

        with sqlite3.connect('db/users.sqlite') as conn:
            cursor = conn.cursor()
            for fid, _ in users:
                cursor.execute("DELETE FROM users WHERE fid = ?", (fid,))
            conn.commit()

        del self.orphaned_users[alliance_id]
        self.alliance_ids = list(self.orphaned_users.keys())

        if not self.alliance_ids:
            self.mark_resolved(interaction.user, f"Deleted orphaned users from alliance {alliance_id}")
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Cleanup Complete",
                description="All orphaned users have been removed.",
                color=theme.emColor3
            )
            await interaction.response.edit_message(embed=embed, view=None)
        else:
            self.current_index = min(self.current_index, len(self.alliance_ids) - 1)
            embed = self.get_current_embed()
            await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=0)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        view = OrphanCleanupView(self.orphaned_users, self.bot, self.cog)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.primary, row=0)
    async def next_alliance(self, interaction: discord.Interaction, button: discord.ui.Button):
        if await self.check_already_resolved(interaction):
            return

        if len(self.alliance_ids) > 1:
            self.current_index = (self.current_index + 1) % len(self.alliance_ids)

        embed = self.get_current_embed()
        await interaction.response.edit_message(embed=embed, view=self)


class ReassignSelectView(OrphanViewBase):
    """View for selecting an alliance to reassign orphaned users to."""

    def __init__(self, orphaned_users: dict, bot, cog, alliances: list):
        super().__init__(orphaned_users, bot, cog)
        self.alliances = alliances

        options = [
            discord.SelectOption(label=name[:100], value=str(aid), description=f"Alliance ID: {aid}")
            for aid, name in alliances[:25]
        ]

        self.select = discord.ui.Select(
            placeholder="Select target alliance...",
            options=options
        )
        self.select.callback = self.select_callback
        self.add_item(self.select)

    async def select_callback(self, interaction: discord.Interaction):
        if await self.check_already_resolved(interaction):
            return

        target_alliance_id = int(self.select.values[0])
        target_name = next((name for aid, name in self.alliances if aid == target_alliance_id), "Unknown")

        total_count = sum(len(users) for users in self.orphaned_users.values())
        all_fids = [fid for users in self.orphaned_users.values() for fid, _ in users]

        with sqlite3.connect('db/users.sqlite') as conn:
            cursor = conn.cursor()
            for fid in all_fids:
                cursor.execute("UPDATE users SET alliance = ? WHERE fid = ?", (target_alliance_id, fid))
            conn.commit()

        self.mark_resolved(interaction.user, f"Reassigned {total_count} users to {target_name}")

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Reassignment Complete",
            description=(
                f"Moved **{total_count}** users to **{target_name}**.\n\n"
                "They are now part of the selected alliance."
            ),
            color=theme.emColor3
        )
        await interaction.response.edit_message(embed=embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = OrphanCleanupView(self.orphaned_users, self.bot, self.cog)
        embed = view.get_main_embed()
        await interaction.response.edit_message(embed=embed, view=view)

class BotStartup(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.c = self.conn.cursor()
        self.orphan_resolution = {
            "resolved": False,
            "resolved_by": None,
            "resolved_by_name": None,
            "action_taken": None,
            "timestamp": None
        }

    def cog_unload(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    def get_orphaned_users(self) -> dict:
        """Returns {alliance_id: [(fid, nickname), ...]} for orphaned users"""
        with sqlite3.connect('db/users.sqlite') as conn_users:
            with sqlite3.connect('db/alliance.sqlite') as conn_alliance:
                cursor_alliance = conn_alliance.cursor()
                cursor_alliance.execute("SELECT alliance_id FROM alliance_list")
                valid_alliances = {str(row[0]) for row in cursor_alliance.fetchall()}

                cursor_users = conn_users.cursor()
                cursor_users.execute(
                    "SELECT fid, nickname, alliance FROM users "
                    "WHERE alliance IS NOT NULL AND alliance != ''"
                )

                orphaned = {}
                for fid, nickname, alliance in cursor_users.fetchall():
                    if alliance not in valid_alliances:
                        if alliance not in orphaned:
                            orphaned[alliance] = []
                        orphaned[alliance].append((fid, nickname or "Unknown"))

                return orphaned

    async def check_and_notify_orphans(self):
        """Check for orphaned users and notify global admins if found"""
        orphaned = self.get_orphaned_users()
        if not orphaned:
            return

        total_count = sum(len(users) for users in orphaned.values())

        with sqlite3.connect('db/settings.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
            admins = cursor.fetchall()

        if not admins:
            logger.warning(f"Found {total_count} orphaned users but no global admin to notify")
            return

        view = OrphanCleanupView(orphaned, self.bot, self)
        embed = view.get_main_embed()

        for (admin_id,) in admins:
            try:
                user = await self.bot.fetch_user(admin_id)
                if user:
                    await user.send(embed=embed, view=view)
                    logger.info(f"Notified admin {user.name} about {total_count} orphaned users")
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id} about orphaned users: {e}")

    @commands.Cog.listener()
    async def on_ready(self):
        if getattr(self.bot, 'no_dm', False):
            return

        try:
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT id FROM admin WHERE is_initial = 1 LIMIT 1")
                result = cursor.fetchone()
            
            if result:
                admin_id = result[0]
                admin_user = await self.bot.fetch_user(admin_id)
                
                if admin_user:
                    # Check OCR initialization status
                    ocr_status = f"{theme.deniedIcon}"
                    ocr_details = "Not initialized"
                    try:
                        gift_operations_cog = self.bot.get_cog('GiftOperations')
                        if gift_operations_cog and hasattr(gift_operations_cog, 'captcha_solver'):
                            if gift_operations_cog.captcha_solver and gift_operations_cog.captcha_solver.is_initialized:
                                ocr_status = f"{theme.verifiedIcon}"
                                ocr_details = "Gift Code Redeemer (OCR) ready"
                            else:
                                ocr_details = "Solver not initialized"
                        else:
                            ocr_details = "GiftOperations cog not found"
                    except Exception as e:
                        ocr_details = f"Error checking OCR: {str(e)[:30]}..."

                    status_embed = discord.Embed(
                        title=f"{theme.robotIcon} Bot Successfully Activated",
                        description=(
                            f"{theme.upperDivider}\n"
                            f"**System Status**\n"
                            f"{theme.verifiedIcon} Bot is now online and operational\n"
                            f"{theme.verifiedIcon} Database connections established\n"
                            f"{theme.verifiedIcon} Command systems initialized\n"
                            f"{ocr_status} {ocr_details}\n"
                            f"{theme.middleDivider}\n"
                        ),
                        color=discord.Color.green()
                    )

                    status_embed.add_field(
                        name=f"{theme.pinIcon} Community & Support",
                        value=(
                            f"**GitHub Repository:** [Whiteout Project](https://github.com/whiteout-project/bot)\n"
                            f"**Discord Community:** [Join our Discord](https://discord.gg/apYByj6K2m)\n"
                            f"**Bug Reports:** [GitHub Issues](https://github.com/whiteout-project/bot/issues)\n"
                            f"{theme.lowerDivider}"
                        ),
                        inline=False
                    )

                    status_embed.set_footer(text="Thanks for using the bot! Maintained with ❤️ by the WOSLand Bot Team.")

                    await admin_user.send(embed=status_embed)

                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                        cursor = alliance_db.cursor()
                        cursor.execute("SELECT alliance_id, name FROM alliance_list")
                        alliances = cursor.fetchall()

                    if alliances:
                        ALLIANCES_PER_PAGE = 5
                        alliance_info = []
                        
                        for alliance_id, name in alliances:
                            info_parts = []
                            
                            with sqlite3.connect('db/users.sqlite') as users_db:
                                cursor = users_db.cursor()
                                cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                                user_count = cursor.fetchone()[0]
                                info_parts.append(f"{theme.userIcon} Members: {user_count}")
                            
                            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                                cursor = alliance_db.cursor()
                                cursor.execute("SELECT discord_server_id FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                                discord_server = cursor.fetchone()
                                if discord_server:
                                    server_id = discord_server[0]
                                    if server_id:
                                        guild = self.bot.get_guild(server_id)
                                        if guild:
                                            info_parts.append(f"{theme.globeIcon} Server Name: {guild.name}")
                                        else:
                                            info_parts.append(f"{theme.globeIcon} Server ID: {server_id}")
                            
                                cursor.execute("SELECT channel_id, interval FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
                                settings = cursor.fetchone()
                                if settings:
                                    if settings[0]:
                                        info_parts.append(f"{theme.announceIcon} Channel: <#{settings[0]}>")
                                    interval_text = f"{theme.timeIcon} Auto Check: {settings[1]} minutes" if settings[1] > 0 else f"{theme.timeIcon} No Auto Check"
                                    info_parts.append(interval_text)
                            
                            with sqlite3.connect('db/giftcode.sqlite') as gift_db:
                                cursor = gift_db.cursor()
                                cursor.execute("SELECT status FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
                                gift_status = cursor.fetchone()
                                gift_text = f"{theme.giftIcon} Gift System: Active" if gift_status and gift_status[0] == 1 else f"{theme.giftIcon} Gift System: Inactive"
                                info_parts.append(gift_text)

                                cursor.execute("SELECT channel_id FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                                gift_channel = cursor.fetchone()
                                if gift_channel and gift_channel[0]:
                                    info_parts.append(f"{theme.giftIcon} Gift Channel: <#{gift_channel[0]}>")
                            
                            alliance_info.append(
                                f"**{name}**\n" +
                                "\n".join(f"> {part}" for part in info_parts) +
                                f"\n{theme.lowerDivider}"
                            )

                        pages = [alliance_info[i:i + ALLIANCES_PER_PAGE] 
                                for i in range(0, len(alliance_info), ALLIANCES_PER_PAGE)]

                        for page_num, page in enumerate(pages, 1):
                            alliance_embed = discord.Embed(
                                title=f"{theme.chartIcon} Alliance Information (Page {page_num}/{len(pages)})",
                                color=theme.emColor1
                            )
                            alliance_embed.description = "\n".join(page)
                            await admin_user.send(embed=alliance_embed)

                    else:
                        alliance_embed = discord.Embed(
                            title=f"{theme.chartIcon} Alliance Information",
                            description="No alliances currently registered.",
                            color=theme.emColor1
                        )
                        await admin_user.send(embed=alliance_embed)

                    logger.info("Activation messages sent to admin user.")
                    self.bot.startup_dm_sent = True
                else:
                    logger.warning(f"User with Admin ID {admin_id} not found.")
            else:
                logger.warning("No record found in the admin table.")

            # Check for orphaned users and notify global admins
            await self.check_and_notify_orphans()

        except Exception as e:
            logger.error(f"An error occurred during startup: {e}")

    @app_commands.command(name="channel", description="Learn the ID of a channel.")
    @app_commands.describe(channel="The channel you want to learn the ID of")
    async def channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.send_message(
            f"The ID of the selected channel is: {channel.id}",
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(BotStartup(bot))
