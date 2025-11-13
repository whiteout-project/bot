import sqlite3

themeName = "Pink Pricess Theme"

with sqlite3.connect('db/pimpsettings.sqlite') as pimpsettings_db:
    cursor = pimpsettings_db.cursor()
    cursor.execute("SELECT allianceIconOld FROM pimpsettings")
    allianceIconOld = cursor.fetchone()
    if allianceIconOld is not None:
        allianceIconOld = allianceIconOld
    if allianceIconOld is None: 
        allianceIconOld = allianceIconOld

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
robotIcon = "<:pinkKnightHelmet:1437767323674083419>"
total2Icon = "<:pinkTotal:1436281354684989500>"
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
transferIcon = "<:pinkScollopTransfer:1437763382538272779>"
multiplyIcon = "<:pinkScollopMultiply:1437690328541958185>"
deniedIcon = "<:pinkScollopDenied:1437690326063120446>"
deleteIcon = "<:pinkScollopMinus:1437690327975723028>"
exportIcon = "<:pinkScollopExport:1437763381569392802>"
retryIcon = "<:pinkScollopRetrying:1437690331545206875>"
totalIcon = "<:pinkScollopTotal:1437690333801484308>"
infoIcon = "<:pinkScollopInfo:1437690327128477776>"
addIcon = "<:pinkScollopAdd:1437690325694156800>"

dividerEmojiStart1 = ["<:pinkBow:1436293647590232146>", "•"]
dividerEmojiPattern1 = ["<:HotPinkHeart:1436291474898550864>", "•", "<:BarbiePinkHeart:1436291473917083778>", "•"]
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

dividerEmojiStart2 = ["<:pinkBow:1436293647590232146>", "•"]
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