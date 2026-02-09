import discord
from discord.ext import commands
import logging
import os
import sys
import platform
import zipfile
import io
from datetime import datetime, timezone, timedelta
from .pimp_my_bot import theme, safe_edit_message
from .permission_handler import PermissionManager

logger = logging.getLogger('bot')


class SupportOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.log_path = "log"
        self.version_path = "version"

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if not interaction.type == discord.InteractionType.component:
            return

        custom_id = interaction.data.get("custom_id", "")

        if custom_id == "back_to_maintenance_from_about":
            main_menu_cog = self.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_maintenance(interaction)

    async def show_support_menu(self, interaction: discord.Interaction):
        support_menu_embed = discord.Embed(
            title=f"{theme.targetIcon} Support Operations",
            description=(
                f"Get help, learn about the project, or gather diagnostic information.\n\n"
                f"**Available Operations**\n"
                f"{theme.upperDivider}\n"
                f"{theme.editListIcon} **Request Support**\n"
                f"└ Get links to help resources and community\n\n"
                f"{theme.infoIcon} **About Project**\n"
                f"└ Learn about this open source project\n\n"
                f"{theme.documentIcon} **Gather Logs**\n"
                f"└ Download recent logs for troubleshooting\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        view = SupportView(self)

        try:
            await interaction.response.edit_message(embed=support_menu_embed, view=view)
        except discord.errors.InteractionResponded:
            await interaction.message.edit(embed=support_menu_embed, view=view)

    async def show_about_menu(self, interaction: discord.Interaction):
        """Display the About Project information."""
        about_embed = discord.Embed(
            title=f"{theme.infoIcon} About Whiteout Project",
            description=(
                f"**Open Source Bot**\n"
                f"{theme.upperDivider}\n"
                f"This is an open source Discord bot for Whiteout Survival.\n"
                f"The project is community-driven and freely available for everyone.\n"
                f"**Repository:** [GitHub](https://github.com/whiteout-project/bot)\n"
                f"**Community:** [Discord](https://discord.gg/apYByj6K2m)\n\n"
                f"**Features**\n"
                f"{theme.middleDivider}\n"
                f"• Alliance member management\n"
                f"• Gift code operations\n"
                f"• Automated member tracking\n"
                f"• Event notifications\n"
                f"• ID channel verification\n"
                f"• Minister scheduling\n"
                f"• Attendance tracking\n"
                f"• and more...\n\n"
                f"**Contributing**\n"
                f"{theme.middleDivider}\n"
                f"Contributions are welcome! Please check our GitHub repository "
                f"to report issues, suggest features, or submit pull requests."
            ),
            color=discord.Color.green()
        )

        about_embed.set_footer(text=f"Made with {theme.heartIcon} by the WOSLand Bot Team.")

        view = discord.ui.View()
        view.add_item(discord.ui.Button(
            label="Back",
            emoji=f"{theme.backIcon}",
            style=discord.ButtonStyle.secondary,
            custom_id="back_to_maintenance_from_about"
        ))

        await safe_edit_message(interaction, embed=about_embed, view=view, content=None)

    async def show_support_info(self, interaction: discord.Interaction):
        support_embed = discord.Embed(
            title=f"{theme.robotIcon} Bot Support Information",
            description=(
                "If you need help with the bot or are experiencing any issues, "
                "please feel free to ask on our [Discord](https://discord.gg/apYByj6K2m)\n\n"
                "**Additional resources:**\n"
                "**GitHub Repository:** [Whiteout Project](https://github.com/whiteout-project/bot)\n"
                "**Issues & Bug Reports:** [GitHub Issues](https://github.com/whiteout-project/bot/issues)\n\n"
                "This bot is open source and maintained by the WOSLand community. "
                "You can report bugs, request features, or contribute to the project "
                "through our Discord or GitHub repository.\n\n"
                "For technical support, please make sure to provide "
                "detailed information about your problem."
            ),
            color=theme.emColor1
        )

        try:
            await interaction.response.send_message(embed=support_embed, ephemeral=True)
            try:
                await interaction.user.send(embed=support_embed)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Could not send DM because your DMs are closed!",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error sending support info: {e}")
            print(f"Error sending support info: {e}")

    async def gather_support_logs(self, interaction: discord.Interaction):
        """Gather recent logs and bot info into a zip file"""
        is_admin, _ = PermissionManager.is_admin(interaction.user.id)

        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only admins can gather support logs.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # Create in-memory zip file
            zip_buffer = io.BytesIO()
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=24)

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
                # Add bot_info.txt
                bot_info = self._generate_bot_info()
                zf.writestr('bot_info.txt', bot_info)

                # Add recent logs (last 24 hours)
                logs_added = 0
                if os.path.exists(self.log_path):
                    for filename in os.listdir(self.log_path):
                        filepath = os.path.join(self.log_path, filename)
                        if os.path.isfile(filepath) and filename != 'archive':
                            try:
                                mtime = datetime.fromtimestamp(os.path.getmtime(filepath), timezone.utc)
                                # Include files modified in last 24h
                                if mtime >= cutoff_time:
                                    zf.write(filepath, f"logs/{filename}")
                                    logs_added += 1
                            except Exception as e:
                                logger.warning(f"Could not add log file {filename}: {e}")

                # If no recent logs, add a note
                if logs_added == 0:
                    zf.writestr('logs/no_recent_logs.txt', 'No log files modified in the last 24 hours.')

            # Check size
            zip_buffer.seek(0)
            zip_size = len(zip_buffer.getvalue())
            zip_size_mb = zip_size / (1024 * 1024)

            if zip_size > 8 * 1024 * 1024:  # 8 MB Discord limit
                await interaction.followup.send(
                    f"{theme.warnIcon} Support logs are too large ({zip_size_mb:.1f} MB). "
                    f"Please contact support directly and share specific log files.",
                    ephemeral=True
                )
                return

            # Create discord file
            zip_buffer.seek(0)
            timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')
            filename = f"support-logs-{timestamp}.zip"
            file = discord.File(zip_buffer, filename=filename)

            # Try to send via DM first
            try:
                await interaction.user.send(
                    f"{theme.documentIcon} **Support Logs**\n\n"
                    f"Here are your bot's recent logs and system information.\n"
                    f"Share this file when requesting support.",
                    file=file
                )
                await interaction.followup.send(
                    f"{theme.verifiedIcon} Support logs sent to your DMs! ({zip_size_mb:.2f} MB, {logs_added} log files)",
                    ephemeral=True
                )
            except discord.Forbidden:
                # DMs closed, send in channel as ephemeral
                zip_buffer.seek(0)
                file = discord.File(zip_buffer, filename=filename)
                await interaction.followup.send(
                    f"{theme.documentIcon} **Support Logs** ({zip_size_mb:.2f} MB, {logs_added} log files)\n\n"
                    f"Could not send via DM. Download this file and share it when requesting support.",
                    file=file,
                    ephemeral=True
                )

        except Exception as e:
            logger.error(f"Error gathering support logs: {e}")
            print(f"Error gathering support logs: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Failed to gather support logs: {e}",
                ephemeral=True
            )

    def _generate_bot_info(self) -> str:
        """Generate bot information text"""
        lines = [
            "=" * 50,
            "BOT SUPPORT INFORMATION",
            "=" * 50,
            f"Generated: {datetime.now(timezone.utc).isoformat()}",
            "",
            "--- Environment ---",
            f"Python Version: {sys.version}",
            f"Platform: {platform.system()} {platform.release()}",
            f"Architecture: {platform.machine()}",
            f"Discord.py Version: {discord.__version__}",
        ]

        # Docker detection
        in_docker = os.path.exists('/.dockerenv') or os.environ.get('DOCKER', False)
        lines.append(f"Running in Docker: {'Yes' if in_docker else 'No'}")

        # Bot version
        lines.append("")
        lines.append("--- Bot Version ---")
        try:
            if os.path.isfile(self.version_path):
                with open(self.version_path, 'r') as f:
                    lines.append(f"Version: {f.read().strip()}")
            else:
                lines.append("Version file not found")
        except Exception as e:
            lines.append(f"Error reading version: {e}")

        # Health snapshot
        lines.append("")
        lines.append("--- Health Snapshot ---")
        try:
            health_cog = self.bot.get_cog("BotHealth")
            if health_cog:
                db_health = health_cog.get_database_health()
                log_health = health_cog.get_log_health()
                system_health = health_cog.get_system_health()

                lines.append(f"Uptime: {system_health['uptime']}")
                lines.append(f"Latency: {system_health['latency_ms']}ms")
                lines.append(f"Loaded Cogs: {system_health['loaded_cogs']}")
                lines.append(f"Database Size: {db_health['total_mb']:.1f} MB")
                lines.append(f"Log Folder Size: {log_health['total_mb']:.1f} MB")
                lines.append(f"Orphaned Logs: {log_health['orphaned_count']}")

                # Requirements check
                requirements = health_cog.get_requirements_health()
                lines.append("")
                lines.append("--- Dependencies ---")
                if requirements.get('error'):
                    lines.append(f"Error: {requirements['error']}")
                else:
                    lines.append(f"Status: {requirements['ok_count']}/{requirements['total']} packages OK")
                    if requirements['missing']:
                        lines.append(f"Missing packages: {', '.join(requirements['missing'])}")
                    if requirements['outdated']:
                        lines.append("Outdated packages:")
                        for pkg in requirements['outdated']:
                            lines.append(f"  - {pkg['package']}: installed {pkg['installed']}, requires {pkg['required']}")
            else:
                lines.append("Health cog not available")
        except Exception as e:
            lines.append(f"Error getting health info: {e}")

        # Loaded cogs
        lines.append("")
        lines.append("--- Loaded Cogs ---")
        for cog_name in sorted(self.bot.cogs.keys()):
            lines.append(f"  - {cog_name}")

        lines.append("")
        lines.append("=" * 50)

        return "\n".join(lines)


class SupportView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog

    @discord.ui.button(
        label="Request Support",
        emoji=f"{theme.editListIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="request_support",
        row=0
    )
    async def support_request_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_support_info(interaction)

    @discord.ui.button(
        label="About Project",
        emoji=f"{theme.infoIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="about_project",
        row=0
    )
    async def about_project_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        about_embed = discord.Embed(
            title=f"{theme.infoIcon} About Whiteout Project",
            description=(
                f"**Open Source Bot**\n"
                f"{theme.upperDivider}\n"
                f"This is an open source Discord bot for Whiteout Survival.\n"
                f"The project is community-driven and freely available for everyone.\n"
                f"**Repository:** [GitHub](https://github.com/whiteout-project/bot)\n"
                f"**Community:** [Discord](https://discord.gg/apYByj6K2m)\n\n"
                f"**Features**\n"
                f"{theme.middleDivider}\n"
                f"• Alliance member management\n"
                f"• Gift code operations\n"
                f"• Automated member tracking\n"
                f"• Bear trap notifications\n"
                f"• ID channel verification\n"
                f"• and more...\n\n"
                f"**Contributing**\n"
                f"{theme.middleDivider}\n"
                f"Contributions are welcome! Please check our GitHub repository "
                f"to report issues, suggest features, or submit pull requests."
            ),
            color=discord.Color.green()
        )

        about_embed.set_footer(text=f"Made with {theme.heartIcon} by the WOSLand Bot Team.")

        try:
            await interaction.response.send_message(embed=about_embed, ephemeral=True)
            try:
                await interaction.user.send(embed=about_embed)
            except discord.Forbidden:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Could not send DM because your DMs are closed!",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error sending project info: {e}")
            print(f"Error sending project info: {e}")

    @discord.ui.button(
        label="Gather Logs",
        emoji=f"{theme.documentIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="gather_logs",
        row=0
    )
    async def gather_logs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.gather_support_logs(interaction)

    @discord.ui.button(
        label="Back",
        emoji=f"{theme.backIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_maintenance",
        row=1
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Navigate back to Maintenance sub-menu."""
        main_menu_cog = self.cog.bot.get_cog("MainMenu")
        if main_menu_cog:
            try:
                await main_menu_cog.show_maintenance(interaction)
            except discord.errors.InteractionResponded:
                await main_menu_cog.show_maintenance(interaction)


async def setup(bot):
    await bot.add_cog(SupportOperations(bot))
