import discord
from discord.ext import commands
import aiohttp
import hashlib
import ssl
import time
import asyncio
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
robotIcon = "<:pinkKnightHelmet:1437674905989681162>"
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
processingIcon = "<:pinkScollopProcessing:1437690329691197590>"
verifiedIcon = "<:pinkScollopVerified:1437690336305483807>"
questionIcon = "<:pinkScollopQuestion:1437690329959628955>"
transferIcon = "<:pinkScollopTransfer:1437690334409785416>"
multiplyIcon = "<:pinkScollopMultiply:1437690328541958185>"
deniedIcon = "<:pinkScollopDenied:1437690326063120446>"
deleteIcon = "<:pinkScollopMinus:1437690327975723028>"
retryIcon = "<:pinkScollopRetrying:1437690331545206875>"
totalIcon = "<:pinkScollopTotal:1437690333801484308>"
infoIcon = "<:pinkScollopInfo:1437690327128477776>"
addIcon = "<:pinkScollopAdd:1437690325694156800>"

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

emColorString1 = "#FFBDE4" #Baby Pink
emColor1 = int(emColorString1.lstrip('#'), 16) #to replace .blue()
emColorString2 = "#FF0080" #Hot Pink
emColor2 = int(emColorString2.lstrip('#'), 16) #to replace .red()
emColorString3 = "#FF69B4" #Barbie Pink
emColor3 = int(emColorString3.lstrip('#'), 16) #to replace .green()
emColorString4 = "#FF8FCC" #Pinkie Pink
emColor4 = int(emColorString4.lstrip('#'), 16) #to replace .orange() and .yellow() and .gold()

furnnaceLevelImageDefaultURL = "https://cdn-icons-png.freepik.com/512/12388/12388244.png"
furnaceLevelImageHostURL = "https://cdn-icons-png.freepik.com/"
furnaceLevelImageURLs = ["512/12388/12388244.png", 
                         "512/9932/9932942.png", "512/9933/9933046.png", "512/9933/9933166.png", "512/9933/9933287.png", "512/9933/9933409.png", 
                         "512/9933/9933534.png", "512/9933/9933651.png", "512/9933/9933772.png", "512/9933/9933880.png", "512/9932/9932953.png",
                         "512/9932/9932949.png", "512/9932/9932960.png", "512/9932/9932970.png", "512/9932/9932981.png", "512/9932/9932991.png", 
                         "512/9933/9933002.png", "512/9933/9933013.png", "512/9933/9933024.png", "512/9933/9933035.png", "512/9933/9933058.png",
                         "512/9933/9933069.png", "512/9933/9933079.png", "512/9933/9933091.png", "512/9933/9933100.png", "512/9933/9933110.png", 
                         "512/9933/9933121.png", "512/9933/9933133.png", "512/9933/9933144.png", "512/9933/9933156.png", "512/9933/9933177.png"]
class WCommand(commands.Cog):

    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/changes.sqlite')
        self.c = self.conn.cursor()
        self.SECRET = "tB87#kPtkxqOS2"
        
        self.level_mapping = {
            31: "30-1", 32: "30-2", 33: "30-3", 34: "30-4",
            35: "FC 1", 36: "FC 1 - 1", 37: "FC 1 - 2", 38: "FC 1 - 3", 39: "FC 1 - 4",
            40: "FC 2", 41: "FC 2 - 1", 42: "FC 2 - 2", 43: "FC 2 - 3", 44: "FC 2 - 4",
            45: "FC 3", 46: "FC 3 - 1", 47: "FC 3 - 2", 48: "FC 3 - 3", 49: "FC 3 - 4",
            50: "FC 4", 51: "FC 4 - 1", 52: "FC 4 - 2", 53: "FC 4 - 3", 54: "FC 4 - 4",
            55: "FC 5", 56: "FC 5 - 1", 57: "FC 5 - 2", 58: "FC 5 - 3", 59: "FC 5 - 4",
            60: "FC 6", 61: "FC 6 - 1", 62: "FC 6 - 2", 63: "FC 6 - 3", 64: "FC 6 - 4",
            65: "FC 7", 66: "FC 7 - 1", 67: "FC 7 - 2", 68: "FC 7 - 3", 69: "FC 7 - 4",
            70: "FC 8", 71: "FC 8 - 1", 72: "FC 8 - 2", 73: "FC 8 - 3", 74: "FC 8 - 4",
            75: "FC 9", 76: "FC 9 - 1", 77: "FC 9 - 2", 78: "FC 9 - 3", 79: "FC 9 - 4",
            80: "FC 10", 81: "FC 10 - 1", 82: "FC 10 - 2", 83: "FC 10 - 3", 84: "FC 10 - 4"
        }
 
    def cog_unload(self):
        if hasattr(self, 'conn'):
            self.conn.close()

    @discord.app_commands.command(name='w', description='Fetches user info using fid.')
    async def w(self, interaction: discord.Interaction, fid: str):
        await self.fetch_user_info(interaction, fid)

    @w.autocomplete('fid')
    async def autocomplete_fid(self, interaction: discord.Interaction, current: str):
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT fid, nickname FROM users")
                users = cursor.fetchall()

            choices = [
                discord.app_commands.Choice(name=f"{nickname} ({fid})", value=str(fid)) 
                for fid, nickname in users
            ]

            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = choices[:25]

            return filtered_choices
        
        except Exception as e:
            print(f"Autocomplete could not be loaded: {e}")
            return []


    async def fetch_user_info(self, interaction: discord.Interaction, fid: str):
        try:
            await interaction.response.defer(thinking=True)
            
            current_time = int(time.time() * 1000)
            form = f"fid={fid}&time={current_time}"
            sign = hashlib.md5((form + self.SECRET).encode('utf-8')).hexdigest()
            form = f"sign={sign}&{form}"

            url = 'https://wos-giftcode-api.centurygame.com/api/player'
            headers = {'Content-Type': 'application/x-www-form-urlencoded'}
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

            max_retries = 3
            retry_delay = 60

            for attempt in range(max_retries):
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, data=form, ssl=ssl_context) as response:
                        if response.status == 200:
                            data = await response.json()
                            nickname = data['data']['nickname']
                            fid_value = data['data']['fid']
                            stove_level = data['data']['stove_lv']
                            kid = data['data']['kid']
                            avatar_image = data['data']['avatar_image']
                            stove_lv_content = data['data'].get('stove_lv_content')

                            if stove_level > 30:
                                stove_level_name = self.level_mapping.get(stove_level, f"Level {stove_level}")
                            else:
                                stove_level_name = f"Level {stove_level}"

                            user_info = None
                            alliance_info = None
                            
                            with sqlite3.connect('db/users.sqlite') as users_db:
                                cursor = users_db.cursor()
                                cursor.execute("SELECT *, alliance FROM users WHERE fid=?", (fid_value,))
                                user_info = cursor.fetchone()
                                
                                if user_info and user_info[-1]:
                                    with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                                        cursor = alliance_db.cursor()
                                        cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (user_info[-1],))
                                        alliance_info = cursor.fetchone()
 

                            embed = discord.Embed(
                                title=f"{avatarIcon} {nickname}",
                                description=(
                                    f"{divider1}\n\n"
                                    f"**{fidIcon} FID:** `{fid_value}`\n"
                                    f"**{stoveIcon} Furnace:** `{stove_level_name}`\n"
                                    f"**{stateIcon} State:** `{kid}`\n\n"
                                    f"{divider1}\n\n"
                                ),
                                color=emColor3,
                            )

                            if alliance_info:
                                embed.description += f"**{allianceIcon} Alliance:** `{alliance_info[0]}`\n\n{divider1}\n\n"

                            if avatar_image:
                                embed.set_image(url = avatar_image)

                            embed.set_footer(text = f"[✔] Registered on the List" if user_info else f"[✘] Not on the List")
                           
                            if isinstance(stove_lv_content, str) and stove_lv_content.startswith("http"):
                                embed.set_thumbnail(url = stove_lv_content)
                            elif stove_level <=30 and furnaceLevelImageURLs[stove_level] == "":
                                embed.set_thumbnail(url = furnnaceLevelImageDefaultURL)
                            else :
                                embed.set_thumbnail(url = furnaceLevelImageHostURL + furnaceLevelImageURLs[stove_level])
                            
                            await interaction.followup.send(embed = embed)
                            return 

                        elif response.status == 429:
                            if attempt < max_retries - 1:
                                await interaction.followup.send("API limit reached, your result will be displayed automatically shortly...")
                                await asyncio.sleep(retry_delay)
            await interaction.followup.send(f"User with ID {fid} not found or an error occurred after multiple attempts.")
            
        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching user info.")


async def setup(bot):
    await bot.add_cog(WCommand(bot))
