import sqlite3

themeName = "Pink Pricess Theme"
elseEmoji = "😨"

with sqlite3.connect('db/pimpsettings.sqlite') as pimpsettings_db:
    cursor = pimpsettings_db.cursor()
    cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (themeName,))
    theme = cursor.fetchone()
    allianceIconOld = theme[2] if theme else elseEmoji
    allianceIcon = theme[3] if theme else elseEmoji
    avatarIcon = theme[4] if theme else elseEmoji
    stoveIcon = theme[5] if theme else elseEmoji
    stateIcon = theme[6] if theme else elseEmoji
    listIcon = theme[7] if theme else elseEmoji
    fidIcon = theme[8] if theme else elseEmoji
    timeIcon = theme[9] if theme else elseEmoji
    homeIcon = theme[10] if theme else elseEmoji
    num1Icon = theme[11] if theme else elseEmoji
    num2Icon = theme[12] if theme else elseEmoji
    num3Icon = theme[13] if theme else elseEmoji
    newIcon = theme[14] if theme else elseEmoji
    pinIcon = theme[15] if theme else elseEmoji
    giftIcon = theme[16] if theme else elseEmoji
    giftsIcon = theme[17] if theme else elseEmoji
    alertIcon = theme[18] if theme else elseEmoji
    robotIcon = theme[19] if theme else elseEmoji
    crossIcon = theme[20] if theme else elseEmoji
    heartIcon = theme[21] if theme else elseEmoji
    total2Icon = theme[22] if theme else elseEmoji
    shieldIcon = theme[23] if theme else elseEmoji
    redeemIcon = theme[24] if theme else elseEmoji
    membersIcon = theme[25] if theme else elseEmoji
    anounceIcon = theme[26] if theme else elseEmoji
    hashtagIcon = theme[27] if theme else elseEmoji
    settingsIcon = theme[28] if theme else elseEmoji
    settings2Icon = theme[29] if theme else elseEmoji
    hourglassIcon = theme[30] if theme else elseEmoji
    alarmClockIcon = theme[31] if theme else elseEmoji
    magnifyingIcon = theme[32] if theme else elseEmoji
    checkGiftCodeIcon = theme[33] if theme else elseEmoji
    deleteGiftCodeIcon = theme[34] if theme else elseEmoji
    addGiftCodeIcon = theme[35] if theme else elseEmoji
    processingIcon = theme[36] if theme else elseEmoji
    verifiedIcon = theme[37] if theme else elseEmoji
    questionIcon = theme[38] if theme else elseEmoji
    transferIcon = theme[39] if theme else elseEmoji
    multiplyIcon = theme[40] if theme else elseEmoji
    divideIcon = theme[41] if theme else elseEmoji
    deniedIcon = theme[42] if theme else elseEmoji
    deleteIcon = theme[43] if theme else elseEmoji
    exportIcon = theme[44] if theme else elseEmoji
    importIcon = theme[45] if theme else elseEmoji
    retryIcon = theme[46] if theme else elseEmoji
    totalIcon = theme[47] if theme else elseEmoji
    infoIcon = theme[48] if theme else elseEmoji
    warnIcon = theme[49] if theme else elseEmoji
    addIcon = theme[50] if theme else elseEmoji
    dividerEmojiStart1 = theme[51].split(",") if theme else [elseEmoji]
    dividerEmojiPattern1 = theme[52].split(",") if theme else [elseEmoji]
    dividerEmojiEnd1 = theme[53].split(",") if theme else [elseEmoji]
    dividerLength1 = theme[54] if theme else 9
    dividerEmojiStart2 = theme[55].split(",") if theme else [elseEmoji]
    dividerEmojiPattern2 = theme[56].split(",") if theme else [elseEmoji]
    dividerEmojiEnd2 = theme[57].split(",") if theme else [elseEmoji]
    dividerLength2 = theme[58] if theme else 9
    emColorString1 = theme[59] if theme else "#FFFFFF"
    emColorString2 = theme[60] if theme else "#FFFFFF"
    emColorString3 = theme[61] if theme else "#FFFFFF"
    emColorString4 = theme[62] if theme else "#FFFFFF"

dividerEmojiCombined1 = []
for emoji in dividerEmojiStart1:
    dividerEmojiCombined1.append(emoji)
for emoji in dividerEmojiPattern1:
    dividerEmojiCombined1.append(emoji)
for emoji in dividerEmojiEnd1:
    dividerEmojiCombined1.append(emoji)
divider1 = ""
dividerMaxLength1 = 99
if dividerLength1 > dividerMaxLength1:
    dividerLength1 = dividerMaxLength1
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

dividerEmojiCombined2 = []
for emoji in dividerEmojiStart2:
    dividerEmojiCombined2.append(emoji)
for emoji in dividerEmojiPattern2:
    dividerEmojiCombined2.append(emoji)
for emoji in dividerEmojiEnd2:
    dividerEmojiCombined2.append(emoji)
divider2 = ""
dividerMaxLength2 = 99
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

emColor1 = int(emColorString1.lstrip('#'), 16) #to replace .blue()
emColor2 = int(emColorString2.lstrip('#'), 16) #to replace .red()
emColor3 = int(emColorString3.lstrip('#'), 16) #to replace .green()
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