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

            if themename == "default":
                lines = [
                    f"allianceOldIcon  = https://discord.com/assets/fa2c28d64be33d41.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2694.png",
                    f"avatarOldIcon = https://discord.com/assets/5b12dce3a467ed97.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f464.png",
                    f"stoveOldIcon = https://discord.com/assets/a7bd71d6389d0dfe.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f525.png",
                    f"stateOldIcon = https://discord.com/assets/864e0e4584241547.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f30e.png",
                    f"allianceIcon = https://discord.com/assets/fa2c28d64be33d41.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2694.png",
                    f"avatarIcon = https://discord.com/assets/5b12dce3a467ed97.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f464.png",
                    f"stoveIcon = https://discord.com/assets/a7bd71d6389d0dfe.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f525.png",
                    f"stateIcon = https://discord.com/assets/25e48a1e493a8668.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f30f.png",
                    f"listIcon = https://discord.com/assets/9b50a2e1be3cd515.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f4dc.png",
                    f"fidIcon = https://discord.com/assets/e205e5f16fab825d.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f194.png",
                    f"timeIcon = https://discord.com/assets/1a3e5bc356c4308b.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f570.png",
                    f"homeIcon = https://discord.com/assets/e7f086418e908e40.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f3e0.png",
                    f"num1Icon = https://discord.com/assets/83f7cb2c2f4230cd.svg = https://images.emojiterra.com/twitter/v14.0/1024px/31-20e3.png",
                    f"num2Icon = https://discord.com/assets/f36ae3caed2a0bae.svg = https://images.emojiterra.com/twitter/v14.0/1024px/32-20e3.png",
                    f"num3Icon = https://discord.com/assets/a20044fbde269579.svg = https://images.emojiterra.com/twitter/v14.0/1024px/33-20e3.png",
                    f"newIcon = https://discord.com/assets/903e44646bb0b466.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f195.png",
                    f"pinIcon = https://discord.com/assets/0da00b0fec31ced4.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f4cd.png",
                    f"giftIcon = https://discord.com/assets/949f113339307625.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f381.png",
                    f"giftsIcon = https://discord.com/assets/f1382a93639744a7.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f6cd.png",
                    f"alertIcon = https://discord.com/assets/fb6fd920c79bd504.svg = https://images.emojiterra.com/twitter/v14.0/1024px/26a0.png",
                    f"robotIcon = https://discord.com/assets/b8dafcbec499ac71.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f916.png",
                    f"crossIcon = https://discord.com/assets/fa2c28d64be33d41.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2694.png",
                    f"heartIcon = https://discord.com/assets/453654b9f13ea463.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f497.png",
                    f"total2Icon = https://discord.com/assets/51aa82833b5f08b5.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f7f0.png",
                    f"shieldIcon = https://discord.com/assets/51cc7794c9a923de.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f6e1.png",
                    f"targetIcon = https://discord.com/assets/8683903b8675f909.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f3af.png",
                    f"redeemIcon = https://discord.com/assets/c365d5d32bba07d4.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f504.png",
                    f"membersIcon = https://discord.com/assets/be8706c9515e4e6e.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f465.png",
                    f"anounceIcon = https://discord.com/assets/7401aa22d4169631.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f4e2.png",
                    f"averageIcon = https://discord.com/assets/a59b48874be63ed4.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f4c8.png",
                    f"hashtagIcon = https://discord.com/assets/81224497b397e84f.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f522.png",
                    f"messageIcon = https://discord.com/assets/6446faea65f88f9b.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f50a.png",
                    f"supportIcon = https://discord.com/assets/6e1e6f0796462dc1.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f198.png",
                    f"settingsIcon = https://discord.com/assets/7afdc0163bb3fba3.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2699.png",
                    f"settings2Icon = https://discord.com/assets/7afdc0163bb3fba3.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2699.png",
                    f"hourglassIcon = https://discord.com/assets/07910dea69f5d269.svg = https://images.emojiterra.com/twitter/v14.0/1024px/23f3.png",
                    f"messageNoIcon = https://discord.com/assets/e5278532283d7271.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f507.png",
                    f"alarmClockIcon = https://discord.com/assets/d9505466edba0bff.svg = https://images.emojiterra.com/twitter/v14.0/1024px/23f0.png",
                    f"magnifyingIcon = https://discord.com/assets/74f0a67afb481b21.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f50d.png",
                    f"checkGiftCodeIcon = https://discord.com/assets/43b7ead1fb91b731.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2705.png",
                    f"deleteGiftCodeIcon = https://discord.com/assets/7b47ccd346102b2a.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f5d1.png",
                    f"addGiftCodeIcon = https://discord.com/assets/15f799a915e5de90.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2795.png",
                    f"processingIcon = https://discord.com/assets/e541f62450f233be.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f504.png",
                    f"verifiedIcon = https://discord.com/assets/43b7ead1fb91b731.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2705.png",
                    f"questionIcon = https://discord.com/assets/881ed827548f38c6.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2753.png",
                    f"transferIcon = https://discord.com/assets/b4d10ea8c411d93f.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2194.png",
                    f"multiplyIcon = https://discord.com/assets/66932d1dab5dbc04.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2716.png",
                    f"divideIcon = https://discord.com/assets/8c01b18c49b9a764.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2797.png",
                    f"deniedIcon = https://discord.com/assets/4f584fe7b12fcf02.svg = https://images.emojiterra.com/twitter/v14.0/1024px/274c.png",
                    f"deleteIcon = https://discord.com/assets/e01b1b8006e1985f.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2796.png",
                    f"exportIcon = https://discord.com/assets/cafe86161362ab2b.svg = https://images.emojiterra.com/twitter/v14.0/1024px/27a1.png",
                    f"importIcon = https://discord.com/assets/99fca900378cc8e4.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2b05.png",
                    f"retryIcon = https://discord.com/assets/e541f62450f233be.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f504.png",
                    f"totalIcon = https://discord.com/assets/51aa82833b5f08b5.svg = https://images.emojiterra.com/twitter/v14.0/1024px/1f7f0.png",
                    f"infoIcon = https://discord.com/assets/c263a344ff649ead.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2139.png",
                    f"warnIcon = https://discord.com/assets/fb6fd920c79bd504.svg = https://images.emojiterra.com/twitter/v14.0/1024px/26a0.png",
                    f"addIcon = https://discord.com/assets/15f799a915e5de90.svg = https://images.emojiterra.com/twitter/v14.0/1024px/2795.png",
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
            else:
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
                    description = parts[2]
                    embed = discord.Embed(title=name, description=f"{description}", color=pimp.emColor3)
                    if parts[2].strip('\\').startswith('<') and '>' in parts[2]:
                        emoji_id = parts[2].strip('\\').split(':')[-1].strip('>')
                        embed.set_thumbnail(url=f"https://cdn.discordapp.com/emojis/{emoji_id}.png")
                    elif parts[2].startswith("http"):
                        embed.set_thumbnail(url=f"{parts[2]}")
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