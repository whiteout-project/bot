import sqlite3

elseEmoji = "👻"
furnaceLevelImageDefaultURL = "https://cdn-icons-png.freepik.com/512/12388/12388244.png"

with sqlite3.connect('db/pimpsettings.sqlite') as pimpsettings_db:
    cursor = pimpsettings_db.cursor()

    # Get the active theme (is_active = 1)
    cursor.execute("SELECT * FROM pimpsettings WHERE is_active=1 LIMIT 1")
    theme = cursor.fetchone()
    if not theme:
        cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", ("default",))
        theme = cursor.fetchone()

    allianceOldIcon = theme[3] if theme else elseEmoji
    avatarOldIcon = theme[4] if theme else elseEmoji
    stoveOldIcon = theme[5] if theme else elseEmoji
    stateOldIcon = theme[6] if theme else elseEmoji
    allianceIcon = theme[7] if theme else elseEmoji
    avatarIcon = theme[8] if theme else elseEmoji
    stoveIcon = theme[9] if theme else elseEmoji
    stateIcon = theme[10] if theme else elseEmoji
    listIcon = theme[11] if theme else elseEmoji
    fidIcon = theme[12] if theme else elseEmoji
    timeIcon = theme[13] if theme else elseEmoji
    homeIcon = theme[14] if theme else elseEmoji
    num1Icon = theme[15] if theme else elseEmoji
    num2Icon = theme[16] if theme else elseEmoji
    num3Icon = theme[17] if theme else elseEmoji
    num4Icon = theme[18] if theme else elseEmoji
    num5Icon = theme[19] if theme else elseEmoji
    num10Icon = theme[20] if theme else elseEmoji
    newIcon = theme[21] if theme else elseEmoji
    pinIcon = theme[22] if theme else elseEmoji
    saveIcon = theme[23] if theme else elseEmoji
    robotIcon = theme[24] if theme else elseEmoji
    crossIcon = theme[25] if theme else elseEmoji
    heartIcon = theme[26] if theme else elseEmoji
    shieldIcon = theme[27] if theme else elseEmoji
    targetIcon = theme[28] if theme else elseEmoji
    redeemIcon = theme[29] if theme else elseEmoji
    membersIcon = theme[30] if theme else elseEmoji
    averageIcon = theme[31] if theme else elseEmoji
    messageIcon = theme[32] if theme else elseEmoji
    supportIcon = theme[33] if theme else elseEmoji
    foundryIcon = theme[34] if theme else elseEmoji
    announceIcon = theme[35] if theme else elseEmoji
    ministerIcon = theme[36] if theme else elseEmoji
    researchIcon = theme[37] if theme else elseEmoji
    trainingIcon = theme[38] if theme else elseEmoji
    crazyJoeIcon = theme[39] if theme else elseEmoji
    bearTrapIcon = theme[40] if theme else elseEmoji
    calendarIcon = theme[41] if theme else elseEmoji
    editListIcon = theme[42] if theme else elseEmoji
    settingsIcon = theme[43] if theme else elseEmoji
    hourglassIcon = theme[44] if theme else elseEmoji
    messageNoIcon = theme[45] if theme else elseEmoji
    blankListIcon = theme[46] if theme else elseEmoji
    alarmClockIcon = theme[47] if theme else elseEmoji
    magnifyingIcon = theme[48] if theme else elseEmoji
    frostdragonIcon = theme[49] if theme else elseEmoji
    canyonClashIcon = theme[50] if theme else elseEmoji
    constructionIcon = theme[51] if theme else elseEmoji
    castleBattleIcon = theme[52] if theme else elseEmoji
    giftIcon = theme[53] if theme else elseEmoji
    giftsIcon = theme[54] if theme else elseEmoji
    giftAddIcon = theme[55] if theme else elseEmoji
    giftAlarmIcon = theme[56] if theme else elseEmoji
    gifAlertIcon = theme[57] if theme else elseEmoji
    giftCheckIcon = theme[58] if theme else elseEmoji
    giftTotalIcon = theme[59] if theme else elseEmoji
    giftDeleteIcon = theme[60] if theme else elseEmoji
    giftHashtagIcon = theme[61] if theme else elseEmoji
    giftSettingsIcon = theme[62] if theme else elseEmoji
    processingIcon = theme[63] if theme else elseEmoji
    verifiedIcon = theme[64] if theme else elseEmoji
    questionIcon = theme[65] if theme else elseEmoji
    transferIcon = theme[66] if theme else elseEmoji
    multiplyIcon = theme[67] if theme else elseEmoji
    divideIcon = theme[68] if theme else elseEmoji
    deniedIcon = theme[69] if theme else elseEmoji
    deleteIcon = theme[70] if theme else elseEmoji
    exportIcon = theme[71] if theme else elseEmoji
    importIcon = theme[72] if theme else elseEmoji
    retryIcon = theme[73] if theme else elseEmoji
    totalIcon = theme[74] if theme else elseEmoji
    infoIcon = theme[75] if theme else elseEmoji
    warnIcon = theme[76] if theme else elseEmoji
    addIcon = theme[77] if theme else elseEmoji
    shutdownZzzIcon = theme[78] if theme else elseEmoji
    shutdownDoorIcon = theme[79] if theme else elseEmoji
    shutdownHandIcon = theme[80] if theme else elseEmoji
    shutdownMoonIcon = theme[81] if theme else elseEmoji
    shutdownPlugIcon = theme[82] if theme else elseEmoji
    shutdownStopIcon = theme[83] if theme else elseEmoji
    shutdownClapperIcon = theme[84] if theme else elseEmoji
    shutdownSparkleIcon = theme[85] if theme else elseEmoji
    dividerEmojiStart1 = theme[86].split(",") if theme else [elseEmoji]
    dividerEmojiPattern1 = theme[87].split(",") if theme else [elseEmoji]
    dividerEmojiEnd1 = theme[88].split(",") if theme else [elseEmoji]
    dividerLength1 = theme[89] if theme else 12
    dividerEmojiStart2 = theme[90].split(",") if theme else [elseEmoji]
    dividerEmojiPattern2 = theme[91].split(",") if theme else [elseEmoji]
    dividerEmojiEnd2 = theme[92].split(",") if theme else [elseEmoji]
    dividerLength2 = theme[93] if theme else 12
    emColorString1 = theme[94] if theme else "#FFFFFF"
    emColorString2 = theme[95] if theme else "#FFFFFF"
    emColorString3 = theme[96] if theme else "#FFFFFF"
    emColorString4 = theme[97] if theme else "#FFFFFF"
    headerColor1 = theme[98] if theme else "#FFFFFF"
    headerColor2 = theme[99] if theme else "#FFFFFF"
    furnaceLevel0Icon = theme[100] if theme else furnaceLevelImageDefaultURL
    furnaceLevel1Icon = theme[101] if theme else furnaceLevelImageDefaultURL
    furnaceLevel2Icon = theme[102] if theme else furnaceLevelImageDefaultURL
    furnaceLevel3Icon = theme[103] if theme else furnaceLevelImageDefaultURL
    furnaceLevel4Icon = theme[104] if theme else furnaceLevelImageDefaultURL
    furnaceLevel5Icon = theme[105] if theme else furnaceLevelImageDefaultURL
    furnaceLevel6Icon = theme[106] if theme else furnaceLevelImageDefaultURL
    furnaceLevel7Icon = theme[107] if theme else furnaceLevelImageDefaultURL
    furnaceLevel8Icon = theme[108] if theme else furnaceLevelImageDefaultURL
    furnaceLevel9Icon = theme[109] if theme else furnaceLevelImageDefaultURL
    furnaceLevel10Icon = theme[110] if theme else furnaceLevelImageDefaultURL
    furnaceLevel11Icon = theme[111] if theme else furnaceLevelImageDefaultURL
    furnaceLevel12Icon = theme[112] if theme else furnaceLevelImageDefaultURL
    furnaceLevel13Icon = theme[113] if theme else furnaceLevelImageDefaultURL
    furnaceLevel14Icon = theme[114] if theme else furnaceLevelImageDefaultURL
    furnaceLevel15Icon = theme[115] if theme else furnaceLevelImageDefaultURL
    furnaceLevel16Icon = theme[116] if theme else furnaceLevelImageDefaultURL
    furnaceLevel17Icon = theme[117] if theme else furnaceLevelImageDefaultURL
    furnaceLevel18Icon = theme[118] if theme else furnaceLevelImageDefaultURL
    furnaceLevel19Icon = theme[119] if theme else furnaceLevelImageDefaultURL
    furnaceLevel20Icon = theme[120] if theme else furnaceLevelImageDefaultURL
    furnaceLevel21Icon = theme[121] if theme else furnaceLevelImageDefaultURL
    furnaceLevel22Icon = theme[122] if theme else furnaceLevelImageDefaultURL
    furnaceLevel23Icon = theme[123] if theme else furnaceLevelImageDefaultURL
    furnaceLevel24Icon = theme[124] if theme else furnaceLevelImageDefaultURL
    furnaceLevel25Icon = theme[125] if theme else furnaceLevelImageDefaultURL
    furnaceLevel26Icon = theme[126] if theme else furnaceLevelImageDefaultURL
    furnaceLevel27Icon = theme[127] if theme else furnaceLevelImageDefaultURL
    furnaceLevel28Icon = theme[128] if theme else furnaceLevelImageDefaultURL
    furnaceLevel29Icon = theme[129] if theme else furnaceLevelImageDefaultURL
    furnaceLevel30Icon = theme[130] if theme else furnaceLevelImageDefaultURL

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
if dividerLength1 >= len(dividerEmojiCombined1):
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
                if i == dividerLength1:
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
                if i == dividerLength2:
                    break
else :
    for emoji in dividerEmojiCombined2:
        divider2 += emoji

emColor1 = int(emColorString1.lstrip('#'), 16) #to replace .blue()
emColor2 = int(emColorString2.lstrip('#'), 16) #to replace .red()
emColor3 = int(emColorString3.lstrip('#'), 16) #to replace .green()
emColor4 = int(emColorString4.lstrip('#'), 16) #to replace .orange() and .yellow() and .gold()

furnaceLevelImageURLs = [furnaceLevel0Icon, 
                         furnaceLevel1Icon, furnaceLevel2Icon, furnaceLevel3Icon, furnaceLevel4Icon, furnaceLevel5Icon,
                         furnaceLevel6Icon, furnaceLevel7Icon, furnaceLevel8Icon, furnaceLevel9Icon, furnaceLevel10Icon,
                         furnaceLevel11Icon, furnaceLevel12Icon, furnaceLevel13Icon, furnaceLevel14Icon, furnaceLevel15Icon,
                         furnaceLevel16Icon, furnaceLevel17Icon, furnaceLevel18Icon, furnaceLevel19Icon, furnaceLevel20Icon,
                         furnaceLevel21Icon, furnaceLevel22Icon, furnaceLevel23Icon, furnaceLevel24Icon, furnaceLevel25Icon,
                         furnaceLevel26Icon, furnaceLevel27Icon, furnaceLevel28Icon, furnaceLevel29Icon, furnaceLevel30Icon]