import discord
from discord.ext import commands
from discord import app_commands
import sqlite3

allianceIconOld = "<:pinkCrownOld:1437374294551429297>"
allianceIcon = "<:pinkRings:1436281348670361650>"
avatarIcon = "<:pinkCrown:1436281335546118164>"   
stoveIcon = "<:pinkCarriage:1436281331515396198>"
stateIcon = "<:pinkCastle:1436281332949975040>"
listIcon = "<:pinkScroll:1436281353678360616>"
fidIcon = "<:pinkRoyalHeart:1436281349605429424>"
timeIcon = "<:pinkHourglass:1436281342533963796>"
homeIcon = "<:pinkLargeCastle:1436281344769527808>"
num1Icon = "<:pink1:1436671751303069808>"
num2Icon = "<:pink2:1436671752016236646>"
num3Icon = "<:pink3:1436671753060483122>"
pinIcon = "<:pinkRings:1436281348670361650>"
giftIcon = "<:pinkGift:1436281337005735988>"
giftsIcon = "<:pinkGiftOpen:1436281339556134922>"
heartIcon = "<:HotPinkHeart:1436291474898550864>"
alertIcon = "<:pinkGiftWarn:1437015069723459604>"
totalIcon = "<:pinkTotal:1436281354684989500>"
robotIcon = "<:pinkKnightHelmet:1437569343293493360>"
shieldIcon = "<:pinkShield:1437535908193636413>"
redeemIcon = "<:pinkWand:1436281358430376047>"
membersIcon = "<:pinkUnicorn:1436983641669374105>"
anounceIcon = "<:pinkTrumpet:1437570141490778112>"
hashtagIcon = "<:pinkGiftHashtag:1437015068268171367>"
settingsIcon = "<:pinkGiftCog:1437015067152482426>"
settings2Icon = "<:pinkSettings:1436281352612745226>"
hourglassIcon = "<:pinkHourglass:1436281342533963796>"
alarmClockIcon = "<:pinkGiftLoop:1436991292973256937>"
magnifyingIcon = "<:pinkMirror:1436281345033637929>"
checkGiftCodeIcon = "<:pinkGiftCheck:1436994529562595400>"
deleteGiftCodeIcon = "<:pinkGiftX:1436991294348988446>"
addGiftCodeIcon = "<:pinkGiftPlus:1436281340403122196>"
processingIcon = "<:pinkProcessing:1436281345956642880>"
verifiedIcon = "<:pinkVerified:1436281357017022486>"
questionIcon = "<:pinkQuestion:1436680546335068233>"
deniedIcon = "<:pinkDenied:1436281336406216776>"
deleteIcon = "<:pinkGiftMinus:1436281337794527264>"
retryIcon = "<:pinkRetrying:1436281347181252618>"
infoIcon = "<:pinkInfo:1436281343603507264>"

dividerEmojiStart1 = "<:pinkBow:1436293647590232146>", "•"
dividerEmojiPattern1 = "<:HotPinkHeart:1436291474898550864>", "•", "<:BarbiePinkHeart:1436291473917083778>", "•"
dividerEmojiEnd1 = ["<:pinkBow:1436293647590232146>"]
dividerEmojiCombined1 = []
for emoji in dividerEmojiStart1:
    dividerEmojiCombined1.append(emoji)
for emoji in dividerEmojiPattern1:
    dividerEmojiCombined1.append(emoji)
for emoji in dividerEmojiEnd1:
    dividerEmojiCombined1.append(emoji)
divider1 = ""
dividerMaxLength1 = 99
dividerLength1 = 19
if dividerLength1 > dividerMaxLength1:
    dividerLength1 = dividerMaxLength1
dividerLength2 = 47
if int(dividerLength1) >= len(dividerEmojiCombined1):
    i = 1
    while i <= dividerLength1:
        if i == 1:
            for emoji in dividerEmojiStart1:
                divider1 += emoji
                i += 1
        elif i == dividerLength1:
            for emoji in dividerEmojiEnd1:
                divider1 += emoji
                i += 1
        else :
            for emoji in dividerEmojiPattern1:
                divider1 += emoji
                i += 1
                if i > dividerLength1:
                    break
else :
    for emoji in dividerEmojiCombined1:
        divider1 += emoji

dividerEmojiStart2 = "<:pinkBow:1436293647590232146>", "•"
dividerEmojiPattern2 = ["•"]
dividerEmojiEnd2 = ["<:pinkBow:1436293647590232146>"]
dividerEmojiCombined2 = []
for emoji in dividerEmojiStart2:
    dividerEmojiCombined2.append(emoji)
for emoji in dividerEmojiPattern2:
    dividerEmojiCombined2.append(emoji)
for emoji in dividerEmojiEnd2:
    dividerEmojiCombined2.append(emoji)
divider2 = ""
dividerMaxLength2 = 99
dividerLength2 = 47
if dividerLength2 > dividerMaxLength2:
    dividerLength2 = dividerMaxLength2
if int(dividerLength2) >= len(dividerEmojiCombined2):
    i = 1
    while i <= dividerLength2:
        if i == 1:
            for emoji in dividerEmojiStart2:
                divider2 += emoji
                i += 1
        elif i == dividerLength2:
            for emoji in dividerEmojiEnd2:
                divider2 += emoji
                i += 1
        else :
            for emoji in dividerEmojiPattern2:
                divider2 += emoji
                i += 1
                if i > dividerLength2:
                    break
else :
    for emoji in dividerEmojiCombined2:
        divider2 += emoji
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
                    ocr_status = f"{deniedIcon}"
                    ocr_details = "Not initialized"
                    try:
                        gift_operations_cog = self.bot.get_cog('GiftOperations')
                        if gift_operations_cog and hasattr(gift_operations_cog, 'captcha_solver'):
                            if gift_operations_cog.captcha_solver and gift_operations_cog.captcha_solver.is_initialized:
                                ocr_status = f"{verifiedIcon}"
                                ocr_details = "Gift Code Redeemer (OCR) ready"
                            else:
                                ocr_details = "Solver not initialized"
                        else:
                            ocr_details = "GiftOperations cog not found"
                    except Exception as e:
                        ocr_details = f"Error checking OCR: {str(e)[:30]}..."
                    
                    status_embed = discord.Embed(
                        title=f"🤖 Bot Successfully Activated",
                        description=(
                            f"{divider1}\n"
                            f"### **System Status**\n"
                            f"{verifiedIcon} Bot is now online and operational\n"
                            f"{verifiedIcon} Database connections established\n"
                            f"{verifiedIcon} Command systems initialized\n"
                            f"{verifiedIcon if auto_value == 1 else deniedIcon} Alliance Control Messages\n"
                            f"{ocr_status} {ocr_details}\n"
                            f"\n"
                            f"{divider1}\n"
                            f"### {pinIcon} Community & Support\n"
                            f"**GitHub Repository:** [Whiteout Project](https://github.com/whiteout-project/bot)\n"
                            f"**Discord Community:** [Join our Discord](https://discord.gg/apYByj6K2m)\n"
                            f"**Bug Reports:** [GitHub Issues](https://github.com/whiteout-project/bot/issues)\n"
                            f"\n"
                            f"{divider1}\n"
                       ),
                        color = emColor3
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
                        
                        for alliance_id, name in alliances:
                            info_parts = []
                            
                            with sqlite3.connect('db/users.sqlite') as users_db:
                                cursor = users_db.cursor()
                                cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                                user_count = cursor.fetchone()[0]
                                info_parts.append(f"{membersIcon} Members: {user_count}")
                            
                            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                                cursor = alliance_db.cursor()
                                cursor.execute("SELECT discord_server_id FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                                discord_server = cursor.fetchone()
                                if discord_server and discord_server[0]:
                                    info_parts.append(f"{stateIcon} Server ID: {discord_server[0]}")
                            
                                cursor.execute("SELECT channel_id, interval FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
                                settings = cursor.fetchone()
                                if settings:
                                    if settings[0]:
                                        info_parts.append(f"{anounceIcon} Channel: <#{settings[0]}>")
                                    interval_text = f"{alarmClockIcon} Auto Check: {settings[1]} minutes" if settings[1] > 0 else f"{deleteGiftCodeIcon}  No Auto Check"
                                    info_parts.append(interval_text)
                            
                            with sqlite3.connect('db/giftcode.sqlite') as gift_db:
                                cursor = gift_db.cursor()
                                cursor.execute("SELECT status FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
                                gift_status = cursor.fetchone()
                                gift_text = f"{checkGiftCodeIcon} Gift System: Active" if gift_status and gift_status[0] == 1 else f"{deleteGiftCodeIcon} Gift System: Inactive"
                                info_parts.append(gift_text)
                                
                                cursor.execute("SELECT channel_id FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                                gift_channel = cursor.fetchone()
                                if gift_channel and gift_channel[0]:
                                    info_parts.append(f"{giftsIcon} Gift Channel: <#{gift_channel[0]}>")
                            
                            alliance_info.append(
                                f"**{name}**\n" + 
                                f"{divider1}\n\n" +
                                f"\n".join(f"> {part}" for part in info_parts) +
                                f"\n\n{divider1}"
                            )

                        pages = [alliance_info[i:i + ALLIANCES_PER_PAGE] 
                                for i in range(0, len(alliance_info), ALLIANCES_PER_PAGE)]

                        for page_num, page in enumerate(pages, 1):
                            alliance_embed = discord.Embed(
                                title = f"{allianceIcon} Alliance Information (Page {page_num}/{len(pages)})",
                                color = emColor1
                            )
                            alliance_embed.description = "\n".join(page)
                            await admin_user.send(embed=alliance_embed)

                    else:
                        alliance_embed = discord.Embed(
                            title = f"{allianceIcon} Alliance Information",
                            description = "No alliances currently registered.",
                            color = emColor1
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