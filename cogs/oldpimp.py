import discord
from discord.ext import commands
import sqlite3
from cogs import prettification_is_my_purpose as pimp

class PIMP(commands.Cog):

    def __init__(self, bot):
        self.bot = bot

    @discord.app_commands.command(name='pimp', description='Shows Theme Icons (Must Select Theme Name).')
    async def pimp(self, interaction: discord.Interaction, themename: str):
        await self.fetch_theme_info(interaction, themename)

    @pimp.autocomplete('themename')
    async def autocomplete_themename(self, interaction: discord.Interaction, current: str):
        try:
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT themename FROM pimpsettings")
                themes = [row[0] for row in cursor.fetchall()]

            choices = [
                discord.app_commands.Choice(name=theme, value=theme) 
                for theme in themes
            ]

            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = choices[:25]

            return filtered_choices
        
        except Exception as e:
            print(f"Autocomplete could not be loaded: {e}")
            return []


    async def fetch_theme_info(self, interaction: discord.Interaction, themename: str):
        try:
            await interaction.response.defer(thinking=True)
            
            elseEmoji = "👻"
            
            with sqlite3.connect('db/pimpsettings.sqlite') as pimpSettings_db:
                cursor = pimpSettings_db.cursor()
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (themename,))
                theme = cursor.fetchone()
                
                if not theme:
                    await interaction.followup.send(f"Theme '{themename}' not found.")
                    return
                    
                allianceOldIcon = theme[2] if theme else elseEmoji
                avatarOldIcon = theme[3] if theme else elseEmoji
                stoveOldIcon = theme[4] if theme else elseEmoji
                stateOldIcon = theme[5] if theme else elseEmoji
                allianceIcon = theme[6] if theme else elseEmoji
                avatarIcon = theme[7] if theme else elseEmoji
                stoveIcon = theme[8] if theme else elseEmoji
                stateIcon = theme[9] if theme else elseEmoji
                listIcon = theme[10]if theme else elseEmoji
                fidIcon = theme[11] if theme else elseEmoji
                timeIcon = theme[12] if theme else elseEmoji
                homeIcon = theme[13] if theme else elseEmoji
                num1Icon = theme[14] if theme else elseEmoji
                num2Icon = theme[15] if theme else elseEmoji
                num3Icon = theme[16] if theme else elseEmoji
                newIcon = theme[17] if theme else elseEmoji
                pinIcon = theme[18] if theme else elseEmoji
                giftIcon = theme[19] if theme else elseEmoji
                giftsIcon = theme[20] if theme else elseEmoji
                alertIcon = theme[21] if theme else elseEmoji
                robotIcon = theme[22] if theme else elseEmoji
                crossIcon = theme[23] if theme else elseEmoji
                heartIcon = theme[24] if theme else elseEmoji
                total2Icon = theme[25] if theme else elseEmoji
                shieldIcon = theme[26] if theme else elseEmoji
                targetIcon = theme[27] if theme else elseEmoji
                redeemIcon = theme[28] if theme else elseEmoji
                membersIcon = theme[29] if theme else elseEmoji
                announceIcon = theme[30] if theme else elseEmoji
                averageIcon = theme[31] if theme else elseEmoji
                hashtagIcon = theme[32] if theme else elseEmoji
                messageIcon = theme[33] if theme else elseEmoji
                supportIcon = theme[34] if theme else elseEmoji
                settingsIcon = theme[35] if theme else elseEmoji
                settings2Icon = theme[36] if theme else elseEmoji
                hourglassIcon = theme[37] if theme else elseEmoji
                messageNoIcon = theme[38] if theme else elseEmoji
                alarmClockIcon = theme[39] if theme else elseEmoji
                magnifyingIcon = theme[40] if theme else elseEmoji
                checkGiftCodeIcon = theme[41] if theme else elseEmoji
                deleteGiftCodeIcon = theme[42] if theme else elseEmoji
                addGiftCodeIcon = theme[43] if theme else elseEmoji
                processingIcon = theme[44] if theme else elseEmoji
                verifiedIcon = theme[45] if theme else elseEmoji
                questionIcon = theme[46] if theme else elseEmoji
                transferIcon = theme[47] if theme else elseEmoji
                multiplyIcon = theme[48] if theme else elseEmoji
                divideIcon = theme[49] if theme else elseEmoji
                deniedIcon = theme[50] if theme else elseEmoji
                deleteIcon = theme[51] if theme else elseEmoji
                exportIcon = theme[52] if theme else elseEmoji
                importIcon = theme[53] if theme else elseEmoji
                retryIcon = theme[54] if theme else elseEmoji
                totalIcon = theme[55] if theme else elseEmoji
                infoIcon = theme[56] if theme else elseEmoji
                warnIcon = theme[57] if theme else elseEmoji
                addIcon = theme[58] if theme else elseEmoji
                dividerEmojiStart1 = theme[59].split(",") if theme else [elseEmoji]
                dividerEmojiPattern1 = theme[60].split(",") if theme else [elseEmoji]
                dividerEmojiEnd1 = theme[61].split(",") if theme else [elseEmoji]
                dividerLength1 = theme[62] if theme else 9
                dividerEmojiStart2 = theme[63].split(",") if theme else [elseEmoji]
                dividerEmojiPattern2 = theme[64].split(",") if theme else [elseEmoji]
                dividerEmojiEnd2 = theme[65].split(",") if theme else [elseEmoji]
                dividerLength2 = theme[66] if theme else 9
                emColorString1 = theme[67] if theme else "#FFFFFF"
                emColorString2 = theme[68] if theme else "#FFFFFF"
                emColorString3 = theme[69] if theme else "#FFFFFF"
                emColorString4 = theme[70] if theme else "#FFFFFF"

            embed = discord.Embed(
                title=f"{pimp.pinIcon} {themename}",
                description=(
                    f"### {pimp.pinIcon} Theme Settings\n"
                    f"{pimp.divider1}\n\n"
                    f"{pimp.divider2}\n"
                    f"**Variable Name:** allianceOldIcon\n"
                    f"**Emoji:** {allianceOldIcon}\n"
                    f"**Emoji URL:** \\{allianceOldIcon}\n"
                    f"{pimp.divider2}\n"
                    f"avatarOldIcon = {avatarOldIcon} = \\{avatarOldIcon}\n"
                    f"stoveOldIcon = {stoveOldIcon} = \\{stoveOldIcon}\n"
                    f"stateOldIcon = {stateOldIcon} = \\{stateOldIcon}\n"
                    f"allianceIcon = {allianceIcon} = \\{allianceIcon}\n"
                    f"avatarIcon = {avatarIcon} = \\{avatarIcon}\n"
                    f"stoveIcon = {stoveIcon} = \\{stoveIcon}\n"
                    f"stateIcon = {stateIcon} = \\{stateIcon}\n"
                    f"listIcon = {listIcon} = \\{listIcon}\n"
                    f"fidIcon = {fidIcon} = \\{fidIcon}\n"
                    f"timeIcon = {timeIcon} = \\{timeIcon}\n"
                    f"homeIcon = {homeIcon} = \\{homeIcon}\n"
                    f"num1Icon = {num1Icon} = \\{num1Icon}\n"
                    f"num2Icon = {num2Icon} = \\{num2Icon}\n"
                    f"num3Icon = {num3Icon} = \\{num3Icon}\n"
                    f"newIcon = {newIcon} = \\{newIcon}\n"
                    f"pinIcon = {pinIcon} = \\{pinIcon}\n"
                    f"giftIcon = {giftIcon} = \\{giftIcon}\n"
                    f"giftsIcon = {giftsIcon} = \\{giftsIcon}\n"
                    f"alertIcon = {alertIcon} = \\{alertIcon}\n"
                    f"robotIcon = {robotIcon} = \\{robotIcon}\n"
                    f"crossIcon = {crossIcon} = \\{crossIcon}\n"
                    f"heartIcon = {heartIcon} = \\{heartIcon}\n"
                    f"total2Icon = {total2Icon} = \\{total2Icon}\n"
                    f"shieldIcon = {shieldIcon} = \\{shieldIcon}\n"
                    f"targetIcon = {targetIcon} = \\{targetIcon}\n"
                    f"redeemIcon = {redeemIcon} = \\{redeemIcon}\n"
                    f"membersIcon = {membersIcon} = \\{membersIcon}\n"
                    f"announceIcon = {announceIcon} = \\{announceIcon}\n"
                    f"averageIcon = {averageIcon} = \\{averageIcon}\n"
                    f"hashtagIcon = {hashtagIcon} = \\{hashtagIcon}\n"
                    f"messageIcon = {messageIcon} = \\{messageIcon}\n"
                    f"supportIcon = {supportIcon} = \\{supportIcon}\n"
                    f"settingsIcon = {settingsIcon} = \\{settingsIcon}\n"
                    f"settings2Icon = {settings2Icon} = \\{settings2Icon}\n"
                    f"hourglassIcon = {hourglassIcon} = \\{hourglassIcon}\n"
                    f"messageNoIcon = {messageNoIcon} = \\{messageNoIcon}\n"
                    f"alarmClockIcon = {alarmClockIcon} = \\{alarmClockIcon}\n"
                    f"magnifyingIcon = {magnifyingIcon} = \\{magnifyingIcon}\n"
                    f"checkGiftCodeIcon = {checkGiftCodeIcon} = \\{checkGiftCodeIcon}\n"
                    f"deleteGiftCodeIcon = {deleteGiftCodeIcon} = \\{deleteGiftCodeIcon}\n"
                    f"addGiftCodeIcon = {addGiftCodeIcon} = \\{addGiftCodeIcon}\n"
                    f"processingIcon = {processingIcon} = \\{processingIcon}\n"
                    f"verifiedIcon = {verifiedIcon} = \\{verifiedIcon}\n"
                    f"questionIcon = {questionIcon} = \\{questionIcon}\n"
                    f"transferIcon = {transferIcon} = \\{transferIcon}\n"
                    f"multiplyIcon = {multiplyIcon} = \\{multiplyIcon}\n"
                    f"divideIcon = {divideIcon} = \\{divideIcon}\n"
                    f"deniedIcon = {deniedIcon} = \\{deniedIcon}\n"
                    f"deleteIcon = {deleteIcon} = \\{deleteIcon}\n"
                    f"exportIcon = {exportIcon} = \\{exportIcon}\n"
                    f"importIcon = {importIcon} = \\{importIcon}\n"
                    f"retryIcon = {retryIcon} = \\{retryIcon}\n"
                    f"totalIcon = {totalIcon} = \\{totalIcon}\n"
                    f"infoIcon = {infoIcon} = \\{infoIcon}\n"
                    f"warnIcon = {warnIcon} = \\{warnIcon}\n"
                    f"addIcon = {addIcon} = \\{addIcon}\n"
                    f"dividerEmojiStart1 = {dividerEmojiStart1} = \\{dividerEmojiStart1}\n"
                    f"dividerEmojiPattern1 = {dividerEmojiPattern1} = \\{dividerEmojiPattern1}\n"
                    f"dividerEmojiEnd1 = {dividerEmojiEnd1} = \\{dividerEmojiEnd1}\n"
                    f"dividerLength1 = {dividerLength1}\n"
                    f"dividerEmojiStart2 = {dividerEmojiStart2} = \\{dividerEmojiStart2}\n"
                    f"dividerEmojiPattern2 = {dividerEmojiPattern2} = \\{dividerEmojiPattern2}\n"
                    f"dividerEmojiEnd2 = {dividerEmojiEnd2} = \\{dividerEmojiEnd2}\n"
                    f"dividerLength2 = {dividerLength2}\n"
                    f"emColorString1 = {emColorString1}\n"
                    f"emColorString2 = {emColorString2}\n"
                    f"emColorString3 = {emColorString3}\n"
                    f"emColorString4 = {emColorString4}\n"
                    f"{pimp.divider1}\n\n"
                ),
                color=pimp.emColor3,
            )

            # Split description if too long
            def split_text(text, max_length=2000):
                parts = []
                while text:
                    if len(text) <= max_length:
                        parts.append(text)
                        break
                    split_at = text.rfind('\n', 0, max_length)
                    if split_at == -1:
                        split_at = max_length
                    parts.append(text[:split_at])
                    text = text[split_at:].lstrip('\n')
                return parts

            if len(embed.description) > 4096:
                parts = split_text(embed.description)
                embeds = [discord.Embed(title=f"{pimp.pinIcon} {themename}" if i == 0 else f"{pimp.pinIcon} {themename} (Part {i+1})", description=part, color=pimp.emColor3) for i, part in enumerate(parts)]
            else:
                embeds = [embed]

            await interaction.followup.send(embeds=embeds)
            
        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching theme info.")

    async def show_pimp_cog_menu(self, interaction: discord.Interaction):
        """Show the PIMP cog menu (You can add buttons or other interactive elements here)."""
        try:
            embed = discord.Embed(
                title=f"{pimp.robotIcon} PIMP - Theme Icon Display",
                description=(
                    f"Use the `/pimp` command to display theme icons.\n\n"
                    f"**Available Command:**\n"
                    f"{pimp.pinIcon} `/pimp themename:<theme>` - Shows all icons for the selected theme\n\n"
                    f"**Note:** Theme names are autocompleted from available themes."
                ),
                color=pimp.emColor1
            )
            
            await interaction.response.edit_message(embed=embed, view=None)
            
        except Exception as e:
            print(f"Error in show_pimp_cog_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{pimp.deniedIcon} An error occurred while showing the PIMP menu.",
                    ephemeral=True
                )


async def setup(bot):
    await bot.add_cog(PIMP(bot))
