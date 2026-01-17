import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from .pimp_my_bot import theme

class GNCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/settings.sqlite')
        self.c = self.conn.cursor()

    def cog_unload(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    @commands.Cog.listener()
    async def on_ready(self):
        try:
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT id FROM admin WHERE is_initial = 1 LIMIT 1")
                result = cursor.fetchone()
            
            if result:
                admin_id = result[0]
                admin_user = await self.bot.fetch_user(admin_id)
                
                if admin_user:
                    cursor.execute("SELECT value FROM auto LIMIT 1")
                    auto_result = cursor.fetchone()
                    auto_value = auto_result[0] if auto_result else 1
                    
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
                            f"{theme.verifiedIcon if auto_value == 1 else theme.deniedIcon} Alliance Control Messages\n"
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

                    print("Activation messages sent to admin user.")
                else:
                    print(f"User with Admin ID {admin_id} not found.")
            else:
                print("No record found in the admin table.")
        except Exception as e:
            print(f"An error occurred: {e}")

    @app_commands.command(name="channel", description="Learn the ID of a channel.")
    @app_commands.describe(channel="The channel you want to learn the ID of")
    async def channel(self, interaction: discord.Interaction, channel: discord.TextChannel):
        await interaction.response.send_message(
            f"The ID of the selected channel is: {channel.id}",
            ephemeral=True
        )

async def setup(bot):
    await bot.add_cog(GNCommands(bot))
