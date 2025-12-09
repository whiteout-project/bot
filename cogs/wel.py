import discord
from discord.ext import commands
from discord import app_commands
import sqlite3
from cogs import prettification_is_my_purpose as pimp
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
                    ocr_status = f"{pimp.deniedIcon}"
                    ocr_details = "Not initialized"
                    try:
                        gift_operations_cog = self.bot.get_cog('GiftOperations')
                        if gift_operations_cog and hasattr(gift_operations_cog, 'captcha_solver'):
                            if gift_operations_cog.captcha_solver and gift_operations_cog.captcha_solver.is_initialized:
                                ocr_status = f"{pimp.verifiedIcon}"
                                ocr_details = "Gift Code Redeemer (OCR) ready"
                            else:
                                ocr_details = "Solver not initialized"
                        else:
                            ocr_details = "GiftOperations cog not found"
                    except Exception as e:
                        ocr_details = f"Error checking OCR: {str(e)[:30]}..."
                    
                    status_embed = discord.Embed(
                        title=f"{pimp.robotIcon} Bot Successfully Activated",
                        description=(
                            f"{pimp.divider1}\n"
                            f"### {pimp.homeIcon} System Status\n"
                            f"{pimp.verifiedIcon} ***Bot is now online and operational***\n"
                            f"{pimp.verifiedIcon} ***Database connections established***\n"
                            f"{pimp.verifiedIcon} ***Command systems initialized***\n"
                            f"{pimp.verifiedIcon if auto_value == 1 else pimp.deniedIcon} ***Alliance Control Messages automated***\n"
                            f"{ocr_status} ***{ocr_details}***\n"
                            f"\n"
                            f"{pimp.divider1}\n"
                            f"### {pimp.pinIcon} Community & Support\n"
                            f"**GitHub Repository:** [Whiteout Project](https://github.com/whiteout-project/bot)\n"
                            f"**Discord Community:** [Join our Discord](https://discord.gg/apYByj6K2m)\n"
                            f"**Bug Reports:** [GitHub Issues](https://github.com/whiteout-project/bot/issues)\n"
                            f"\n"
                            f"{pimp.divider1}\n"
                       ),
                        color = pimp.emColor3
                    )

                    status_embed.set_footer(text = f"Thanks for using the bot! Maintained with ❤︎ by the WOSLand Bot Team.")

                    await admin_user.send(embed=status_embed)

                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                        cursor = alliance_db.cursor()
                        cursor.execute("SELECT alliance_id, name FROM alliance_list")
                        alliances = cursor.fetchall()

                    if alliances:
                        ALLIANCES_PER_PAGE = 5
                        alliance_info = []
                        i = 0
                        
                        for alliance_id, name in alliances:
                            info_parts = []
                            i += 1

                            with sqlite3.connect('db/users.sqlite') as users_db:
                                cursor = users_db.cursor()
                                cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                                user_count = cursor.fetchone()[0]
                                info_parts.append(f"{pimp.avatarIcon} **Members:** {user_count}")
                            
                            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                                cursor = alliance_db.cursor()
                                cursor.execute("SELECT discord_server_id FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                                discord_server = cursor.fetchone()
                                if discord_server and discord_server[0]:
                                    info_parts.append(f"{pimp.stateIcon} **Server ID:** {discord_server[0]}")
                            
                                cursor.execute("SELECT channel_id, interval FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
                                settings = cursor.fetchone()
                                if settings:
                                    if settings[0]:
                                        info_parts.append(f"{pimp.announceIcon} **Alliance Channel:** <#{settings[0]}>")
                                    interval_text = f"{pimp.alarmClockIcon} **Auto Check:** {settings[1]} minutes" if settings[1] > 0 else f"{pimp.deleteGiftCodeIcon}  No Auto Check"
                                    info_parts.append(interval_text)
                            
                            with sqlite3.connect('db/giftcode.sqlite') as gift_db:
                                cursor = gift_db.cursor()
                                cursor.execute("SELECT status FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
                                gift_status = cursor.fetchone()
                                gift_text = f"{pimp.checkGiftCodeIcon} **Gift System:** Active" if gift_status and gift_status[0] == 1 else f"{pimp.deleteGiftCodeIcon} **Gift System:** Inactive"
                                info_parts.append(gift_text)
                                
                                cursor.execute("SELECT channel_id FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                                gift_channel = cursor.fetchone()
                                if gift_channel and gift_channel[0]:
                                    info_parts.append(f"{pimp.giftIcon} **Gift Codes Channel:** <#{gift_channel[0]}>")
                            
                            if i == 1:
                                alliance_info.append(
                                    f"{pimp.divider1}\n"
                                )
                            alliance_info.append(
                                f"### {pimp.allianceIcon} {name} {pimp.allianceIcon}\n" + 
                                f"{pimp.divider2}\n" +
                                f"\n".join(f"{part}" for part in info_parts) +
                                f"\n{pimp.divider2}\n"
                            )
                            if i == len(alliances):
                                alliance_info.append(
                                    f"{pimp.divider1}"
                                )

                        pages = [alliance_info[i:i + ALLIANCES_PER_PAGE] 
                                for i in range(0, len(alliance_info), ALLIANCES_PER_PAGE)]

                        for page_num, page in enumerate(pages, 1):
                            alliance_embed = discord.Embed(
                                title = f"{pimp.allianceIcon} Alliance Information (Page {page_num}/{len(pages)})",
                                color = pimp.emColor1
                            )
                            alliance_embed.description = "\n".join(page)
                            await admin_user.send(embed=alliance_embed)

                    else:
                        alliance_embed = discord.Embed(
                            title = f"{pimp.allianceIcon} Alliance Information",
                            description = "No alliances currently registered.",
                            color = pimp.emColor1
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