import discord
from discord.ext import commands
import sqlite3
from cogs import prettification_is_my_purpose as pimp

class PageModal(discord.ui.Modal):
    def __init__(self, view):
        super().__init__(title="Go to Page")
        self.view = view
        self.page_input = discord.ui.TextInput(
            label="Enter page number",
            placeholder=f"1 to {len(self.view.pages)}",
            required=True,
            min_length=1,
            max_length=3
        )
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            page_num = int(self.page_input.value) - 1  # 0-based
            if 0 <= page_num < len(self.view.pages):
                self.view.current_page = page_num
                self.view.update_buttons()
                await interaction.response.edit_message(embeds=self.view.pages[self.view.current_page], view=self.view)
            else:
                await interaction.response.send_message("Invalid page number.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

class PaginationView(discord.ui.View):
    def __init__(self, pages, current_page):
        super().__init__(timeout=300)
        self.pages = pages
        self.current_page = current_page
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()
        prev_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="prev", emoji=f"{pimp.importIcon}")
        prev_button.callback = self.prev_callback
        if self.current_page == 0:
            prev_button.disabled = True
        self.add_item(prev_button)

        page_button = discord.ui.Button(label=f"{self.current_page + 1} of {len(self.pages)}", style=discord.ButtonStyle.secondary, custom_id="pages", emoji=f"{pimp.listIcon}")
        page_button.callback = self.page_callback
        self.add_item(page_button)

        next_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="next", emoji=f"{pimp.exportIcon}")
        next_button.callback = self.next_callback
        if self.current_page == len(self.pages) - 1:
            next_button.disabled = True
        self.add_item(next_button)

    async def prev_callback(self, interaction: discord.Interaction):
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)

    async def page_callback(self, interaction: discord.Interaction):
        modal = PageModal(self)
        await interaction.response.send_modal(modal)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)

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
                anounceIcon = theme[30] if theme else elseEmoji
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
                dividerEmojiStart1 = theme[59].replace(",", "") if theme else [elseEmoji]
                dividerEmojiPattern1 = theme[60].replace(",", "") if theme else [elseEmoji]
                dividerEmojiEnd1 = theme[61].replace(",", "") if theme else [elseEmoji]
                dividerLength1 = theme[62] if theme else 9
                dividerEmojiStart2 = theme[63].replace(",", "") if theme else [elseEmoji]
                dividerEmojiPattern2 = theme[64].replace(",", "") if theme else [elseEmoji]
                dividerEmojiEnd2 = theme[65].replace(",", "") if theme else [elseEmoji]
                dividerLength2 = theme[66] if theme else 9
                emColorString1 = theme[67] if theme else "#FFFFFF"
                emColorString2 = theme[68] if theme else "#FFFFFF"
                emColorString3 = theme[69] if theme else "#FFFFFF"
                emColorString4 = theme[70] if theme else "#FFFFFF"

            lines = [
                f"allianceOldIcon = {allianceOldIcon} = \\{allianceOldIcon}",
                f"avatarOldIcon = {avatarOldIcon} = \\{avatarOldIcon}",
                f"stoveOldIcon = {stoveOldIcon} = \\{stoveOldIcon}",
                f"stateOldIcon = {stateOldIcon} = \\{stateOldIcon}",
                f"allianceIcon = {allianceIcon} = \\{allianceIcon}",
                f"avatarIcon = {avatarIcon} = \\{avatarIcon}",
                f"stoveIcon = {stoveIcon} = \\{stoveIcon}",
                f"stateIcon = {stateIcon} = \\{stateIcon}",
                f"listIcon = {listIcon} = \\{listIcon}",
                f"fidIcon = {fidIcon} = \\{fidIcon}",
                f"timeIcon = {timeIcon} = \\{timeIcon}",
                f"homeIcon = {homeIcon} = \\{homeIcon}",
                f"num1Icon = {num1Icon} = \\{num1Icon}",
                f"num2Icon = {num2Icon} = \\{num2Icon}",
                f"num3Icon = {num3Icon} = \\{num3Icon}",
                f"newIcon = {newIcon} = \\{newIcon}",
                f"pinIcon = {pinIcon} = \\{pinIcon}",
                f"giftIcon = {giftIcon} = \\{giftIcon}",
                f"giftsIcon = {giftsIcon} = \\{giftsIcon}",
                f"alertIcon = {alertIcon} = \\{alertIcon}",
                f"robotIcon = {robotIcon} = \\{robotIcon}",
                f"crossIcon = {crossIcon} = \\{crossIcon}",
                f"heartIcon = {heartIcon} = \\{heartIcon}",
                f"total2Icon = {total2Icon} = \\{total2Icon}",
                f"shieldIcon = {shieldIcon} = \\{shieldIcon}",
                f"targetIcon = {targetIcon} = \\{targetIcon}",
                f"redeemIcon = {redeemIcon} = \\{redeemIcon}",
                f"membersIcon = {membersIcon} = \\{membersIcon}",
                f"anounceIcon = {anounceIcon} = \\{anounceIcon}",
                f"averageIcon = {averageIcon} = \\{averageIcon}",
                f"hashtagIcon = {hashtagIcon} = \\{hashtagIcon}",
                f"messageIcon = {messageIcon} = \\{messageIcon}",
                f"supportIcon = {supportIcon} = \\{supportIcon}",
                f"settingsIcon = {settingsIcon} = \\{settingsIcon}",
                f"settings2Icon = {settings2Icon} = \\{settings2Icon}",
                f"hourglassIcon = {hourglassIcon} = \\{hourglassIcon}",
                f"messageNoIcon = {messageNoIcon} = \\{messageNoIcon}",
                f"alarmClockIcon = {alarmClockIcon} = \\{alarmClockIcon}",
                f"magnifyingIcon = {magnifyingIcon} = \\{magnifyingIcon}",
                f"checkGiftCodeIcon = {checkGiftCodeIcon} = \\{checkGiftCodeIcon}",
                f"deleteGiftCodeIcon = {deleteGiftCodeIcon} = \\{deleteGiftCodeIcon}",
                f"addGiftCodeIcon = {addGiftCodeIcon} = \\{addGiftCodeIcon}",
                f"processingIcon = {processingIcon} = \\{processingIcon}",
                f"verifiedIcon = {verifiedIcon} = \\{verifiedIcon}",
                f"questionIcon = {questionIcon} = \\{questionIcon}",
                f"transferIcon = {transferIcon} = \\{transferIcon}",
                f"multiplyIcon = {multiplyIcon} = \\{multiplyIcon}",
                f"divideIcon = {divideIcon} = \\{divideIcon}",
                f"deniedIcon = {deniedIcon} = \\{deniedIcon}",
                f"deleteIcon = {deleteIcon} = \\{deleteIcon}",
                f"exportIcon = {exportIcon} = \\{exportIcon}",
                f"importIcon = {importIcon} = \\{importIcon}",
                f"retryIcon = {retryIcon} = \\{retryIcon}",
                f"totalIcon = {totalIcon} = \\{totalIcon}",
                f"infoIcon = {infoIcon} = \\{infoIcon}",
                f"warnIcon = {warnIcon} = \\{warnIcon}",
                f"addIcon = {addIcon} = \\{addIcon}",
                f"dividerEmojiStart1 = {dividerEmojiStart1}",
                f"dividerEmojiPattern1 = {dividerEmojiPattern1}",
                f"dividerEmojiEnd1 = {dividerEmojiEnd1}",
                f"dividerLength1 = {dividerLength1}",
                f"dividerEmojiStart2 = {dividerEmojiStart2}",
                f"dividerEmojiPattern2 = {dividerEmojiPattern2}",
                f"dividerEmojiEnd2 = {dividerEmojiEnd2}",
                f"dividerLength2 = {dividerLength2}",
                f"emColorString1 = {emColorString1}",
                f"emColorString2 = {emColorString2}",
                f"emColorString3 = {emColorString3}",
                f"emColorString4 = {emColorString4}",
            ]

            embeds = []
            for index, line in enumerate(lines):
                parts = line.split(" = ")
                name = parts[0]
                if len(parts) == 3:
                    value = parts[1]
                    emoji_str = parts[2].strip("\\")
                    embed = discord.Embed(title=name, description=f"{parts[2]}", color=pimp.emColor3)
                    if emoji_str.startswith('<') and '>' in emoji_str:
                        emoji_id = emoji_str.split(':')[-1].strip('>')
                        embed.set_thumbnail(url=f"https://cdn.discordapp.com/emojis/{emoji_id}.png")
                else:
                    value = parts[1]
                    embed = discord.Embed(title=name, description=f"{value}", color=pimp.emColor3)
                embeds.append(embed)

            pages = [embeds[i:i+10] for i in range(0, len(embeds), 10)]
            current_page = 0

            view = PaginationView(pages, current_page)
            await interaction.followup.send(embeds=pages[current_page], view=view)
            
        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send("An error occurred while fetching theme info.")

    async def show_pimp_cog_menu(self, interaction: discord.Interaction):
        """Show the PIMP cog menu (You can add buttons or other interactive elements here)."""
        try:
            embed = discord.Embed(
                title=f"{pimp.robotIcon} PIMP - Theme Icon Display",
                description=(
                    f"Use the `/pimp` command to display theme icons.{chr(10)}{chr(10)}"
                    f"**Available Command:**{chr(10)}"
                    f"{pimp.pinIcon} `/pimp themename:<theme>` - Shows all icons for the selected theme{chr(10)}{chr(10)}"
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