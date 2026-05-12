"""
Support ticket system. Allows users to create and manage support requests.
"""
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

    async def show_about_menu(self, interaction: discord.Interaction):
        """Display the About Project information."""
        about_embed = discord.Embed(
            title=f"{theme.infoIcon} About Whiteout Project",
            description=(
                f"**Open Source Bot**\n"
                f"{theme.upperDivider}\n"
                f"This is an open source Discord bot for Whiteout Survival.\n"
                f"The project is community-driven and freely available for everyone.\n\n"
                f"{theme.documentIcon} **Documentation:** [Wiki](https://github.com/whiteout-project/bot/wiki)\n"
                f"{theme.linkIcon} **Repository:** [GitHub](https://github.com/whiteout-project/bot)\n"
                f"{theme.chatIcon} **Community:** [Discord](https://discord.gg/apYByj6K2m)\n\n"
                f"☕ **Like the bot?** [Buy me a coffee](https://buymeacoffee.com/justncodes)\n\n"
                f"**Contributing**\n"
                f"{theme.middleDivider}\n"
                f"Contributions are welcome! Please check our GitHub repository "
                f"to report issues, suggest features, or submit pull requests.\n\n"
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

        await safe_edit_message(
            interaction, embed=support_embed,
            view=_BackToMaintenanceView(self), content=None
        )

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
                    ephemeral=True,
                )
            except discord.Forbidden:
                # DMs closed, send in channel as ephemeral
                zip_buffer.seek(0)
                file = discord.File(zip_buffer, filename=filename)
                await interaction.followup.send(
                    f"{theme.documentIcon} **Support Logs** ({zip_size_mb:.2f} MB, {logs_added} log files)\n\n"
                    f"Could not send via DM. Download this file and share it when requesting support.",
                    file=file,
                    ephemeral=True,
                )

        except Exception as e:
            logger.error(f"Error gathering support logs: {e}")
            print(f"Error gathering support logs: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Failed to gather support logs: {e}",
                ephemeral=True,
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


class _BackToMaintenanceView(discord.ui.View):
    """Request Support page nav: lets the admin gather a log bundle for support
    before heading back to Maintenance."""

    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog

    @discord.ui.button(label="Gather Logs", emoji=f"{theme.documentIcon}",
                       style=discord.ButtonStyle.primary)
    async def gather_logs(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.gather_support_logs(interaction)

    @discord.ui.button(label="Back", emoji=f"{theme.backIcon}",
                       style=discord.ButtonStyle.secondary)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu:
            await main_menu.show_maintenance(interaction)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Main Menu module not found.", ephemeral=True
            )


async def setup(bot):
    await bot.add_cog(SupportOperations(bot))
