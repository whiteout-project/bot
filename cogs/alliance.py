import discord
from discord import app_commands
from discord.ext import commands
import sqlite3  
import asyncio
from datetime import datetime

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
robotIcon = "<:pinkKnightHelmet:1437767323674083419>"
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
class Alliance(commands.Cog):
    def __init__(self, bot, conn):
        self.bot = bot
        self.conn = conn
        self.c = self.conn.cursor()
        
        self.conn_users = sqlite3.connect('db/users.sqlite')
        self.c_users = self.conn_users.cursor()
        
        self.conn_settings = sqlite3.connect('db/settings.sqlite')
        self.c_settings = self.conn_settings.cursor()
        
        self.conn_giftcode = sqlite3.connect('db/giftcode.sqlite')
        self.c_giftcode = self.conn_giftcode.cursor()

        self._create_table()
        self._check_and_add_column()

    def _create_table(self):
        self.c.execute("""
            CREATE TABLE IF NOT EXISTS alliance_list (
                alliance_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                discord_server_id INTEGER
            )
        """)
        self.conn.commit()

    def _check_and_add_column(self):
        self.c.execute("PRAGMA table_info(alliance_list)")
        columns = [info[1] for info in self.c.fetchall()]
        if "discord_server_id" not in columns:
            self.c.execute("ALTER TABLE alliance_list ADD COLUMN discord_server_id INTEGER")
            self.conn.commit()

    async def view_alliances(self, interaction: discord.Interaction):
        
        if interaction.guild is None:
            await interaction.response.send_message(f"❌ This command must be used in a server, not in DMs.", ephemeral=True)
            return

        user_id = interaction.user.id
        self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
        admin = self.c_settings.fetchone()

        if admin is None:
            await interaction.response.send_message("You do not have permission to view alliances.", ephemeral=True)
            return

        is_initial = admin[1]
        guild_id = interaction.guild.id

        try:
            if is_initial == 1:
                query = """
                    SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                    FROM alliance_list a
                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                    ORDER BY a.alliance_id ASC
                """
                self.c.execute(query)
            else:
                query = """
                    SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                    FROM alliance_list a
                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                    WHERE a.discord_server_id = ?
                    ORDER BY a.alliance_id ASC
                """
                self.c.execute(query, (guild_id,))

            alliances = self.c.fetchall()

            alliance_list = ""
            for alliance_id, name, interval in alliances:
                
                self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                member_count = self.c_users.fetchone()[0]
                
                interval_text = f"{interval} minutes" if interval > 0 else "No automatic control"
                alliance_list += f"🛡️ **{alliance_id}: {name}**\n👥 Members: {member_count}\n⏱️ Control Interval: {interval_text}\n\n"

            if not alliance_list:
                alliance_list = "No alliances found."

            embed = discord.Embed(
                title="Existing Alliances",
                description=alliance_list,
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(
                "An error occurred while fetching alliances.", 
                ephemeral=True
            )

    @app_commands.command(name="settings", description="Open settings menu.")
    async def settings(self, interaction: discord.Interaction):
        try:
            if interaction.guild is not None: # Check bot permissions only if in a guild
                perm_check = interaction.guild.get_member(interaction.client.user.id)
                if not perm_check.guild_permissions.administrator:
                    await interaction.response.send_message(
                        f"Beeb boop {robotIcon} I need **Administrator** permissions to function. "
                        f"Go to server settings --> Roles --> find my role --> scroll down and turn on Administrator", 
                        ephemeral=True
                    )
                    return
                
            self.c_settings.execute("SELECT COUNT(*) FROM admin")
            admin_count = self.c_settings.fetchone()[0]

            user_id = interaction.user.id

            if admin_count == 0:
                self.c_settings.execute("""
                    INSERT INTO admin (id, is_initial) 
                    VALUES (?, 1)
                """, (user_id,))
                self.conn_settings.commit()

                first_use_embed = discord.Embed(
                    title="🎉 First Time Setup",
                    description=(
                        "This command has been used for the first time and no administrators were found.\n\n"
                        f"**{interaction.user.name}** has been added as the Global Administrator.\n\n"
                        "You can now access all administrative functions."
                    ),
                    color=discord.Color.green()
                )
                await interaction.response.send_message(embed=first_use_embed, ephemeral=True)
                
                await asyncio.sleep(3)
                
            self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
            admin = self.c_settings.fetchone()

            if admin is None:
                await interaction.response.send_message(
                    "You do not have permission to access this menu.", 
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"⚙️ Settings Menu",
                description=(
                    f"Please select a category:\n\n"
                    f"**Menu Categories**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏰 **Alliance Operations**\n"
                    f"└ Manage alliances and settings\n\n"
                    f"👥 **Alliance Member Operations**\n"
                    f"└ Add, remove, and view members\n\n"
                    f"{robotIcon} **Bot Operations**\n"
                    f"└ Configure bot settings\n\n"
                    f"🎁 **Gift Code Operations**\n"
                    f"└ Manage gift codes and rewards\n\n"
                    f"📜 **Alliance History**\n"
                    f"└ View alliance changes and history\n\n"
                    f"🆘 **Support Operations**\n"
                    f"└ Access support features\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )
            
            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Alliance Operations",
                emoji="🏰",
                style=discord.ButtonStyle.primary,
                custom_id="alliance_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Member Operations",
                emoji="👥",
                style=discord.ButtonStyle.primary,
                custom_id="member_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Bot Operations",
                emoji=f"{robotIcon}",
                style=discord.ButtonStyle.primary,
                custom_id="bot_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Gift Operations",
                emoji="🎁",
                style=discord.ButtonStyle.primary,
                custom_id="gift_code_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Alliance History",
                emoji="📜",
                style=discord.ButtonStyle.primary,
                custom_id="alliance_history",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Support Operations",
                emoji="🆘",
                style=discord.ButtonStyle.primary,
                custom_id="support_operations",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Other Features",
                emoji="🔧",
                style=discord.ButtonStyle.primary,
                custom_id="other_features",
                row=3
            ))

            if admin_count == 0:
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.response.send_message(embed=embed, view=view)

        except Exception as e:
            if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                print(f"Settings command error: {e}")
            error_message = "An error occurred while processing your request."
            if not interaction.response.is_done():
                await interaction.response.send_message(error_message, ephemeral=True)
            else:
                await interaction.followup.send(error_message, ephemeral=True)

    async def show_main_menu(self, interaction: discord.Interaction):
        """Display the main settings menu - can be called by other cogs"""
        try:
            embed = discord.Embed(
                title="⚙️ Settings Menu",
                description=(
                    f"Please select a category:\n\n"
                    f"**Menu Categories**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🏰 **Alliance Operations**\n"
                    f"└ Manage alliances and settings\n\n"
                    f"👥 **Alliance Member Operations**\n"
                    f"└ Add, remove, and view members\n\n"
                    f"{robotIcon} **Bot Operations**\n"
                    f"└ Configure bot settings\n\n"
                    f"🎁 **Gift Code Operations**\n"
                    f"└ Manage gift codes and rewards\n\n"
                    f"📜 **Alliance History**\n"
                    f"└ View alliance changes and history\n\n"
                    f"🆘 **Support Operations**\n"
                    f"└ Access support features\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )
            
            view = discord.ui.View()
            view.add_item(discord.ui.Button(
                label="Alliance Operations",
                emoji=f"🏰",
                style=discord.ButtonStyle.primary,
                custom_id="alliance_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Member Operations",
                emoji=f"👥",
                style=discord.ButtonStyle.primary,
                custom_id="member_operations",
                row=0
            ))
            view.add_item(discord.ui.Button(
                label="Bot Operations",
                emoji=f"{robotIcon}",
                style=discord.ButtonStyle.primary,
                custom_id="bot_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Gift Operations",
                emoji=f"🎁",
                style=discord.ButtonStyle.primary,
                custom_id="gift_code_operations",
                row=1
            ))
            view.add_item(discord.ui.Button(
                label="Alliance History",
                emoji=f"📜",
                style=discord.ButtonStyle.primary,
                custom_id="alliance_history",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Support Operations",
                emoji=f"🆘",
                style=discord.ButtonStyle.primary,
                custom_id="support_operations",
                row=2
            ))
            view.add_item(discord.ui.Button(
                label="Other Features",
                emoji=f"🔧",
                style=discord.ButtonStyle.primary,
                custom_id="other_features",
                row=3
            ))

            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                pass
                
        except Exception as _:
            pass

    @commands.Cog.listener()
    async def on_interaction(self, interaction: discord.Interaction):
        if interaction.type == discord.InteractionType.component:
            custom_id = interaction.data.get("custom_id")
            user_id = interaction.user.id
            self.c_settings.execute("SELECT id, is_initial FROM admin WHERE id = ?", (user_id,))
            admin = self.c_settings.fetchone()

            if admin is None:
                await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                return

            try:
                if custom_id == "alliance_operations":
                    embed = discord.Embed(
                        title=f"🏰 Alliance Operations",
                        description=(
                            f"Please select an operation:\n\n"
                            f"**Available Operations**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"➕ **Add Alliance**\n"
                            f"└ Create a new alliance\n\n"
                            f"✏️ **Edit Alliance**\n"
                            f"└ Modify existing alliance settings\n\n"
                            f"🗑️ **Delete Alliance**\n"
                            f"└ Remove an existing alliance\n\n"
                            f"👀 **View Alliances**\n"
                            f"└ List all available alliances\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.blue()
                    )
                    
                    view = discord.ui.View()
                    view.add_item(discord.ui.Button(
                        label="Add Alliance", 
                        emoji=f"➕",
                        style=discord.ButtonStyle.success, 
                        custom_id="add_alliance", 
                        disabled=admin[1] != 1
                    ))
                    view.add_item(discord.ui.Button(
                        label="Edit Alliance", 
                        emoji=f"✏️",
                        style=discord.ButtonStyle.primary, 
                        custom_id="edit_alliance", 
                        disabled=admin[1] != 1
                    ))
                    view.add_item(discord.ui.Button(
                        label="Delete Alliance", 
                        emoji=f"🗑️",
                        style=discord.ButtonStyle.danger, 
                        custom_id="delete_alliance", 
                        disabled=admin[1] != 1
                    ))
                    view.add_item(discord.ui.Button(
                        label="View Alliances", 
                        emoji=f"👀",
                        style=discord.ButtonStyle.primary, 
                        custom_id="view_alliances"
                    ))
                    view.add_item(discord.ui.Button(
                        label="Check Alliance", 
                        emoji=f"🔍",
                        style=discord.ButtonStyle.primary, 
                        custom_id="check_alliance"
                    ))
                    view.add_item(discord.ui.Button(
                        label="Main Menu", 
                        emoji=f"🏠",
                        style=discord.ButtonStyle.secondary, 
                        custom_id="main_menu"
                    ))

                    await interaction.response.edit_message(embed=embed, view=view)

                elif custom_id == "edit_alliance":
                    if admin[1] != 1:
                        await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                        return
                    await self.edit_alliance(interaction)

                elif custom_id == "check_alliance":
                    self.c.execute("""
                        SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval
                        FROM alliance_list a
                        LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                        ORDER BY a.name
                    """)
                    alliances = self.c.fetchall()

                    if not alliances:
                        await interaction.response.send_message("No alliances found to check.", ephemeral=True)
                        return

                    options = [
                        discord.SelectOption(
                            label="Check All Alliances",
                            value="all",
                            description="Start control process for all alliances",
                            emoji=f"🔄"
                        )
                    ]
                    
                    options.extend([
                        discord.SelectOption(
                            label=f"{name[:40]}",
                            value=str(alliance_id),
                            description=f"Control Interval: {interval} minutes"
                        ) for alliance_id, name, interval in alliances
                    ])

                    select = discord.ui.Select(
                        placeholder="Select an alliance to check",
                        options=options,
                        custom_id="alliance_check_select"
                    )

                    async def alliance_check_callback(select_interaction: discord.Interaction):
                        try:
                            selected_value = select_interaction.data["values"][0]
                            control_cog = self.bot.get_cog('Control')
                            
                            if not control_cog:
                                await select_interaction.response.send_message("Control module not found.", ephemeral=True)
                                return
                            
                            # Ensure the centralized queue processor is running
                            await control_cog.login_handler.start_queue_processor()
                            
                            if selected_value == "all":
                                # Get initial queue position
                                queue_info = control_cog.login_handler.get_queue_info()
                                initial_queue_pos = queue_info['queue_size'] + 1
                                
                                progress_embed = discord.Embed(
                                    title=f"⏳ Alliance Control Operation",
                                    description=(
                                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"📊 **Type:** All Alliances ({len(alliances)} total)\n"
                                        f"🏰 **Alliances:** {len(alliances)} alliances\n"
                                        f"📍 **Status:** Queued\n"
                                        f"🔢 **Queue Position:** {initial_queue_pos}\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━"
                                    ),
                                    color=discord.Color.blue()
                                )
                                await select_interaction.response.send_message(embed=progress_embed, ephemeral=True)
                                msg = await select_interaction.original_response()
                                message_id = msg.id

                                # Queue all alliance operations at once
                                queued_alliances = []
                                for index, (alliance_id, name, _) in enumerate(alliances):
                                    try:
                                        self.c.execute("""
                                            SELECT channel_id FROM alliancesettings WHERE alliance_id = ?
                                        """, (alliance_id,))
                                        channel_data = self.c.fetchone()
                                        channel = self.bot.get_channel(channel_data[0]) if channel_data else select_interaction.channel
                                        
                                        # For all alliances, we'll pass the message and track which alliance
                                        await control_cog.login_handler.queue_operation({
                                            'type': 'alliance_control',
                                            'callback': lambda ch=channel, aid=alliance_id, im=msg, an=name, qa=queued_alliances, idx=index: control_cog.check_agslist(
                                                ch, aid, 
                                                interaction_message=im, 
                                                alliance_name=an,
                                                is_batch=True,
                                                batch_info={'current': idx + 1, 'total': len(alliances), 'all_names': qa}
                                            ),
                                            'description': f'Manual control check for alliance {name}',
                                            'alliance_id': alliance_id,
                                            'interaction_message': msg
                                        })
                                        queued_alliances.append((alliance_id, name))
                                    
                                    except Exception as e:
                                        print(f"Error queuing alliance {name}: {e}")
                                        continue
                                
                            else:
                                alliance_id = int(selected_value)
                                self.c.execute("""
                                    SELECT a.name, s.channel_id 
                                    FROM alliance_list a
                                    LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
                                    WHERE a.alliance_id = ?
                                """, (alliance_id,))
                                alliance_data = self.c.fetchone()

                                if not alliance_data:
                                    await select_interaction.response.send_message("Alliance not found.", ephemeral=True)
                                    return

                                alliance_name, channel_id = alliance_data
                                channel = self.bot.get_channel(channel_id) if channel_id else select_interaction.channel
                                
                                # Get queue info for position
                                queue_info = control_cog.login_handler.get_queue_info()
                                queue_position = queue_info['queue_size'] + 1
                                
                                status_embed = discord.Embed(
                                    title=f"⏳ Alliance Control Operation",
                                    description=(
                                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"📊 **Type:** Single Alliance\n"
                                        f"🏰 **Alliance:** {alliance_name}\n"
                                        f"📍 **Status:** Queued\n"
                                        f"🔢 **Queue Position:** {queue_position}\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━"
                                    ),
                                    color=discord.Color.blue()
                                )
                                await select_interaction.response.send_message(embed=status_embed, ephemeral=True)
                                msg = await select_interaction.original_response()
                                
                                await control_cog.login_handler.queue_operation({
                                    'type': 'alliance_control',
                                    'callback': lambda ch=channel, aid=alliance_id, im=msg, an=alliance_name: control_cog.check_agslist(ch, aid, interaction_message=im, alliance_name=an),
                                    'description': f'Manual control check for alliance {alliance_name}',
                                    'alliance_id': alliance_id,
                                    'interaction_message': msg
                                })

                        except Exception as e:
                            print(f"Alliance check error: {e}")
                            await select_interaction.response.send_message(
                                "An error occurred during the control process.", 
                                ephemeral=True
                            )

                    select.callback = alliance_check_callback
                    view = discord.ui.View()
                    view.add_item(select)

                    embed = discord.Embed(
                        title=f"🔍 Alliance Control",
                        description=(
                            f"Please select an alliance to check:\n\n"
                            f"**Information**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"• Select 'Check All Alliances' to process all alliances\n"
                            f"• Control process may take a few minutes\n"
                            f"• Results will be shared in the designated channel\n"
                            f"• Other controls will be queued during the process\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.blue()
                    )
                    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

                elif custom_id == "member_operations":
                    await self.bot.get_cog("AllianceMemberOperations").handle_member_operations(interaction)

                elif custom_id == "bot_operations":
                    try:
                        bot_ops_cog = interaction.client.get_cog("BotOperations")
                        if bot_ops_cog:
                            await bot_ops_cog.show_bot_operations_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                "❌ Bot Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Bot operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Bot Operations.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Bot Operations.",
                                ephemeral=True
                            )

                elif custom_id == "gift_code_operations":
                    try:
                        gift_ops_cog = interaction.client.get_cog("GiftOperations")
                        if gift_ops_cog:
                            await gift_ops_cog.show_gift_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"❌ Gift Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        print(f"Gift operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Gift Operations.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Gift Operations.",
                                ephemeral=True
                            )

                elif custom_id == "add_alliance":
                    if admin[1] != 1:
                        await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                        return
                    await self.add_alliance(interaction)

                elif custom_id == "delete_alliance":
                    if admin[1] != 1:
                        await interaction.response.send_message("You do not have permission to perform this action.", ephemeral=True)
                        return
                    await self.delete_alliance(interaction)

                elif custom_id == "view_alliances":
                    await self.view_alliances(interaction)

                elif custom_id == "main_menu":
                    await self.show_main_menu(interaction)

                elif custom_id == "support_operations":
                    try:
                        support_ops_cog = interaction.client.get_cog("SupportOperations")
                        if support_ops_cog:
                            await support_ops_cog.show_support_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"❌ Support Operations module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Support operations error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Support Operations.", 
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Support Operations.",
                                ephemeral=True
                            )

                elif custom_id == "alliance_history":
                    try:
                        changes_cog = interaction.client.get_cog("Changes")
                        if changes_cog:
                            await changes_cog.show_alliance_history_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"❌ Alliance History module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        print(f"Alliance history error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Alliance History.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Alliance History.",
                                ephemeral=True
                            )

                elif custom_id == "other_features":
                    try:
                        other_features_cog = interaction.client.get_cog("OtherFeatures")
                        if other_features_cog:
                            await other_features_cog.show_other_features_menu(interaction)
                        else:
                            await interaction.response.send_message(
                                f"❌ Other Features module not found.",
                                ephemeral=True
                            )
                    except Exception as e:
                        if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                            print(f"Other features error: {e}")
                        if not interaction.response.is_done():
                            await interaction.response.send_message(
                                "An error occurred while loading Other Features menu.",
                                ephemeral=True
                            )
                        else:
                            await interaction.followup.send(
                                "An error occurred while loading Other Features menu.",
                                ephemeral=True
                            )

            except Exception as e:
                if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                    print(f"Error processing interaction with custom_id '{custom_id}': {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "An error occurred while processing your request. Please try again.",
                        ephemeral=True
                    )

    async def add_alliance(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("Please perform this action in a Discord channel.", ephemeral=True)
            return

        modal = AllianceModal(title="Add Alliance")
        await interaction.response.send_modal(modal)
        await modal.wait()

        try:
            alliance_name = modal.name.value.strip()
            interval = int(modal.interval.value.strip())

            embed = discord.Embed(
                title="Channel Selection",
                description=(
                    f"**Instructions:**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Please select a channel for the alliance\n\n"
                    f"**Page:** 1/1\n"
                    f"**Total Channels:** {len(interaction.guild.text_channels)}"
                ),
                color=discord.Color.blue()
            )

            async def channel_select_callback(select_interaction: discord.Interaction):
                try:
                    self.c.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                    existing_alliance = self.c.fetchone()
                    
                    if existing_alliance:
                        error_embed = discord.Embed(
                            title="Error",
                            description="An alliance with this name already exists.",
                            color=discord.Color.red()
                        )
                        await select_interaction.response.edit_message(embed=error_embed, view=None)
                        return

                    channel_id = int(select_interaction.data["values"][0])

                    self.c.execute("INSERT INTO alliance_list (name, discord_server_id) VALUES (?, ?)", 
                                 (alliance_name, interaction.guild.id))
                    alliance_id = self.c.lastrowid
                    self.c.execute("INSERT INTO alliancesettings (alliance_id, channel_id, interval) VALUES (?, ?, ?)", 
                                 (alliance_id, channel_id, interval))
                    self.conn.commit()

                    self.c_giftcode.execute("""
                        INSERT INTO giftcodecontrol (alliance_id, status) 
                        VALUES (?, 1)
                    """, (alliance_id,))
                    self.conn_giftcode.commit()

                    result_embed = discord.Embed(
                        title=f"✅ Alliance Successfully Created",
                        description="The alliance has been created with the following details:",
                        color=discord.Color.green()
                    )
                    
                    info_section = (
                        f"**🛡️ Alliance Name**\n{alliance_name}\n\n"
                        f"**🔢 Alliance ID**\n{alliance_id}\n\n"
                        f"**📢 Channel**\n<#{channel_id}>\n\n"
                        f"**⏱️ Control Interval**\n{interval} minutes"
                    )
                    result_embed.add_field(name="Alliance Details", value=info_section, inline=False)
                    
                    result_embed.set_footer(text="Alliance settings have been successfully saved")
                    result_embed.timestamp = discord.utils.utcnow()
                    
                    await select_interaction.response.edit_message(embed=result_embed, view=None)

                except Exception as e:
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Error creating alliance: {str(e)}",
                        color=discord.Color.red()
                    )
                    await select_interaction.response.edit_message(embed=error_embed, view=None)

            channels = interaction.guild.text_channels
            view = PaginatedChannelView(channels, channel_select_callback)
            await modal.interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except ValueError:
            error_embed = discord.Embed(
                title="Error",
                description="Invalid interval value. Please enter a number.",
                color=discord.Color.red()
            )
            await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)
        except Exception as e:
            error_embed = discord.Embed(
                title="Error",
                description=f"Error: {str(e)}",
                color=discord.Color.red()
            )
            await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)

    async def edit_alliance(self, interaction: discord.Interaction):
        self.c.execute("""
            SELECT a.alliance_id, a.name, COALESCE(s.interval, 0) as interval, COALESCE(s.channel_id, 0) as channel_id 
            FROM alliance_list a 
            LEFT JOIN alliancesettings s ON a.alliance_id = s.alliance_id
            ORDER BY a.alliance_id ASC
        """)
        alliances = self.c.fetchall()
        
        if not alliances:
            no_alliance_embed = discord.Embed(
                title=f"❌ No Alliances Found",
                description=(
                    "There are no alliances registered in the database.\n"
                    "Please create an alliance first using the `/alliance create` command."
                ),
                color=discord.Color.red()
            )
            no_alliance_embed.set_footer(text="Use /alliance create to add a new alliance")
            return await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)

        alliance_options = [
            discord.SelectOption(
                label=f"{name} (ID: {alliance_id})",
                value=f"{alliance_id}",
                description=f"Interval: {interval} minutes"
            ) for alliance_id, name, interval, _ in alliances
        ]
        
        items_per_page = 25
        option_pages = [alliance_options[i:i + items_per_page] for i in range(0, len(alliance_options), items_per_page)]
        total_pages = len(option_pages)

        class PaginatedAllianceView(discord.ui.View):
            def __init__(self, pages, original_callback):
                super().__init__(timeout=7200)
                self.current_page = 0
                self.pages = pages
                self.original_callback = original_callback
                self.total_pages = len(pages)
                self.update_view()

            def update_view(self):
                self.clear_items()
                
                select = discord.ui.Select(
                    placeholder=f"Select alliance ({self.current_page + 1}/{self.total_pages})",
                    options=self.pages[self.current_page]
                )
                select.callback = self.original_callback
                self.add_item(select)
                
                previous_button = discord.ui.Button(
                    label=f"◀️",
                    style=discord.ButtonStyle.grey,
                    custom_id="previous",
                    disabled=(self.current_page == 0)
                )
                previous_button.callback = self.previous_callback
                self.add_item(previous_button)

                next_button = discord.ui.Button(
                    label=f"▶️",
                    style=discord.ButtonStyle.grey,
                    custom_id="next",
                    disabled=(self.current_page == len(self.pages) - 1)
                )
                next_button.callback = self.next_callback
                self.add_item(next_button)

            async def previous_callback(self, interaction: discord.Interaction):
                self.current_page = (self.current_page - 1) % len(self.pages)
                self.update_view()
                
                embed = interaction.message.embeds[0]
                embed.description = (
                    f"**Instructions:**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"1️⃣ Select an alliance from the dropdown menu\n"
                    f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                    f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                    f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
                await interaction.response.edit_message(embed=embed, view=self)

            async def next_callback(self, interaction: discord.Interaction):
                self.current_page = (self.current_page + 1) % len(self.pages)
                self.update_view()
                
                embed = interaction.message.embeds[0]
                embed.description = (
                    f"**Instructions:**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"1️⃣ Select an alliance from the dropdown menu\n"
                    f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                    f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                    f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                )
                await interaction.response.edit_message(embed=embed, view=self)

        async def select_callback(select_interaction: discord.Interaction):
            try:
                alliance_id = int(select_interaction.data["values"][0])
                alliance_data = next(a for a in alliances if a[0] == alliance_id)
                
                self.c.execute("""
                    SELECT interval, channel_id 
                    FROM alliancesettings 
                    WHERE alliance_id = ?
                """, (alliance_id,))
                settings_data = self.c.fetchone()
                
                modal = AllianceModal(
                    title="Edit Alliance",
                    default_name=alliance_data[1],
                    default_interval=str(settings_data[0] if settings_data else 0)
                )
                await select_interaction.response.send_modal(modal)
                await modal.wait()

                try:
                    alliance_name = modal.name.value.strip()
                    interval = int(modal.interval.value.strip())

                    embed = discord.Embed(
                        title=f"🔄 Channel Selection",
                        description=(
                            f"**Current Channel Information**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📢 Current channel: {f'<#{settings_data[1]}>' if settings_data else 'Not set'}\n"
                            f"**Page:** 1/1\n"
                            f"**Total Channels:** {len(interaction.guild.text_channels)}\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.blue()
                    )

                    async def channel_select_callback(channel_interaction: discord.Interaction):
                        try:
                            channel_id = int(channel_interaction.data["values"][0])

                            self.c.execute("UPDATE alliance_list SET name = ? WHERE alliance_id = ?", 
                                          (alliance_name, alliance_id))
                            
                            if settings_data:
                                self.c.execute("""
                                    UPDATE alliancesettings 
                                    SET channel_id = ?, interval = ? 
                                    WHERE alliance_id = ?
                                """, (channel_id, interval, alliance_id))
                            else:
                                self.c.execute("""
                                    INSERT INTO alliancesettings (alliance_id, channel_id, interval)
                                    VALUES (?, ?, ?)
                                """, (alliance_id, channel_id, interval))
                            
                            self.conn.commit()

                            result_embed = discord.Embed(
                                title=f"✅ Alliance Successfully Updated",
                                description="The alliance details have been updated as follows:",
                                color=discord.Color.green()
                            )
                            
                            info_section = (
                                f"**🛡️ Alliance Name**\n{alliance_name}\n\n"
                                f"**🔢 Alliance ID**\n{alliance_id}\n\n"
                                f"**📢 Channel**\n<#{channel_id}>\n\n"
                                f"**⏱️ Control Interval**\n{interval} minutes"
                            )
                            result_embed.add_field(name="Alliance Details", value=info_section, inline=False)
                            
                            result_embed.set_footer(text="Alliance settings have been successfully saved")
                            result_embed.timestamp = discord.utils.utcnow()
                            
                            await channel_interaction.response.edit_message(embed=result_embed, view=None)

                        except Exception as e:
                            error_embed = discord.Embed(
                                title=f"❌ Error",
                                description=f"An error occurred while updating the alliance: {str(e)}",
                                color=discord.Color.red()
                            )
                            await channel_interaction.response.edit_message(embed=error_embed, view=None)

                    channels = interaction.guild.text_channels
                    view = PaginatedChannelView(channels, channel_select_callback)
                    await modal.interaction.response.send_message(embed=embed, view=view, ephemeral=True)

                except ValueError:
                    error_embed = discord.Embed(
                        title="Error",
                        description="Invalid interval value. Please enter a number.",
                        color=discord.Color.red()
                    )
                    await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)
                except Exception as e:
                    error_embed = discord.Embed(
                        title="Error",
                        description=f"Error: {str(e)}",
                        color=discord.Color.red()
                    )
                    await modal.interaction.response.send_message(embed=error_embed, ephemeral=True)

            except Exception as e:
                error_embed = discord.Embed(
                    title=f"❌ Error",
                    description=f"An error occurred: {str(e)}",
                    color=discord.Color.red()
                )
                if not select_interaction.response.is_done():
                    await select_interaction.response.send_message(embed=error_embed, ephemeral=True)
                else:
                    await select_interaction.followup.send(embed=error_embed, ephemeral=True)

        view = PaginatedAllianceView(option_pages, select_callback)
        embed = discord.Embed(
            title=f"🛡️ Alliance Edit Menu",
            description=(
                f"**Instructions:**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"1️⃣ Select an alliance from the dropdown menu\n"
                f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                f"**Current Page:** {1}/{total_pages}\n"
                f"**Total Alliances:** {len(alliances)}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )
        embed.set_footer(text="Use the dropdown menu below to select an alliance")
        embed.timestamp = discord.utils.utcnow()
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def delete_alliance(self, interaction: discord.Interaction):
        try:
            self.c.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name")
            alliances = self.c.fetchall()
            
            if not alliances:
                no_alliance_embed = discord.Embed(
                    title=f"❌ No Alliances Found",
                    description="There are no alliances to delete.",
                    color=discord.Color.red()
                )
                await interaction.response.send_message(embed=no_alliance_embed, ephemeral=True)
                return

            alliance_members = {}
            for alliance_id, _ in alliances:
                self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                member_count = self.c_users.fetchone()[0]
                alliance_members[alliance_id] = member_count

            items_per_page = 25
            all_options = [
                discord.SelectOption(
                    label=f"{name[:40]} (ID: {alliance_id})",
                    value=f"{alliance_id}",
                    description=f"👥 Members: {alliance_members[alliance_id]} | Click to delete",
                    emoji=f"🗑️"
                ) for alliance_id, name in alliances
            ]
            
            option_pages = [all_options[i:i + items_per_page] for i in range(0, len(all_options), items_per_page)]
            
            embed = discord.Embed(
                title=f"🗑️ Delete Alliance",
                description=(
                    f"**⚠️ Warning: This action cannot be undone!**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"1️⃣ Select an alliance from the dropdown menu\n"
                    f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                    f"**Current Page:** 1/{len(option_pages)}\n"
                    f"**Total Alliances:** {len(alliances)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.red()
            )
            embed.set_footer(text=f"⚠️ Warning: Deleting an alliance will remove all its data!")
            embed.timestamp = discord.utils.utcnow()

            view = PaginatedDeleteView(option_pages, self.alliance_delete_callback)
            
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            print(f"Error in delete_alliance: {e}")
            error_embed = discord.Embed(
                title=f"❌ Error",
                description="An error occurred while loading the delete menu.",
                color=discord.Color.red()
            )
            await interaction.response.send_message(embed=error_embed, ephemeral=True)

    async def alliance_delete_callback(self, interaction: discord.Interaction):
        try:
            alliance_id = int(interaction.data["values"][0])
            
            self.c.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
            alliance_data = self.c.fetchone()
            
            if not alliance_data:
                await interaction.response.send_message("Alliance not found.", ephemeral=True)
                return
            
            alliance_name = alliance_data[0]

            self.c.execute("SELECT COUNT(*) FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
            settings_count = self.c.fetchone()[0]

            self.c_users.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
            users_count = self.c_users.fetchone()[0]

            self.c_settings.execute("SELECT COUNT(*) FROM adminserver WHERE alliances_id = ?", (alliance_id,))
            admin_server_count = self.c_settings.fetchone()[0]

            self.c_giftcode.execute("SELECT COUNT(*) FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
            gift_channels_count = self.c_giftcode.fetchone()[0]

            self.c_giftcode.execute("SELECT COUNT(*) FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
            gift_code_control_count = self.c_giftcode.fetchone()[0]

            confirm_embed = discord.Embed(
                title=f"⚠️ Confirm Alliance Deletion",
                description=(
                    f"Are you sure you want to delete this alliance?\n\n"
                    f"**Alliance Details:**\n"
                    f"🛡️ **Name:** {alliance_name}\n"
                    f"🔢 **ID:** {alliance_id}\n"
                    f"👥 **Members:** {users_count}\n\n"
                    f"**Data to be Deleted:**\n"
                    f"⚙️ Alliance Settings: {settings_count}\n"
                    f"👥 User Records: {users_count}\n"
                    f"🏰 Admin Server Records: {admin_server_count}\n"
                    f"📢 Gift Channels: {gift_channels_count}\n"
                    f"📊 Gift Code Controls: {gift_code_control_count}\n\n"
                    "**⚠️ WARNING: This action cannot be undone!**"
                ),
                color=discord.Color.red()
            )
            
            confirm_view = discord.ui.View(timeout=60)
            
            async def confirm_callback(button_interaction: discord.Interaction):
                try:
                    self.c.execute("DELETE FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_count = self.c.rowcount
                    
                    self.c.execute("DELETE FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
                    admin_settings_count = self.c.rowcount
                    
                    self.conn.commit()

                    self.c_users.execute("DELETE FROM users WHERE alliance = ?", (alliance_id,))
                    users_count_deleted = self.c_users.rowcount
                    self.conn_users.commit()

                    self.c_settings.execute("DELETE FROM adminserver WHERE alliances_id = ?", (alliance_id,))
                    admin_server_count = self.c_settings.rowcount
                    self.conn_settings.commit()

                    self.c_giftcode.execute("DELETE FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                    gift_channels_count = self.c_giftcode.rowcount

                    self.c_giftcode.execute("DELETE FROM giftcodecontrol WHERE alliance_id = ?", (alliance_id,))
                    gift_code_control_count = self.c_giftcode.rowcount
                    
                    self.conn_giftcode.commit()

                    cleanup_embed = discord.Embed(
                        title=f"✅ Alliance Successfully Deleted",
                        description=(
                            f"Alliance **{alliance_name}** has been deleted.\n\n"
                            f"**Cleaned Up Data:**\n"
                            f"🛡️ Alliance Records: {alliance_count}\n"
                            f"👥 Users Removed: {users_count_deleted}\n"
                            f"⚙️ Alliance Settings: {admin_settings_count}\n"
                            f"🏰 Admin Server Records: {admin_server_count}\n"
                            f"📢 Gift Channels: {gift_channels_count}\n"
                            f"📊 Gift Code Controls: {gift_code_control_count}"
                        ),
                        color=discord.Color.green()
                    )
                    cleanup_embed.set_footer(text="All related data has been successfully removed")
                    cleanup_embed.timestamp = discord.utils.utcnow()
                    
                    await button_interaction.response.edit_message(embed=cleanup_embed, view=None)
                    
                except Exception as e:
                    error_embed = discord.Embed(
                        title=f"❌ Error",
                        description=f"An error occurred while deleting the alliance: {str(e)}",
                        color=discord.Color.red()
                    )
                    await button_interaction.response.edit_message(embed=error_embed, view=None)

            async def cancel_callback(button_interaction: discord.Interaction):
                cancel_embed = discord.Embed(
                    title=f"❌ Deletion Cancelled",
                    description="Alliance deletion has been cancelled.",
                    color=discord.Color.grey()
                )
                await button_interaction.response.edit_message(embed=cancel_embed, view=None)

            confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.danger)
            cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.grey)
            confirm_button.callback = confirm_callback
            cancel_button.callback = cancel_callback
            confirm_view.add_item(confirm_button)
            confirm_view.add_item(cancel_button)

            await interaction.response.edit_message(embed=confirm_embed, view=confirm_view)

        except Exception as e:
            print(f"Error in alliance_delete_callback: {e}")
            error_embed = discord.Embed(
                title=f"❌ Error",
                description="An error occurred while processing the deletion.",
                color=discord.Color.red()
            )
            if not interaction.response.is_done():
                await interaction.response.send_message(embed=error_embed, ephemeral=True)
            else:
                await interaction.followup.send(embed=error_embed, ephemeral=True)

class AllianceModal(discord.ui.Modal):
    def __init__(self, title: str, default_name: str = "", default_interval: str = "0"):
        super().__init__(title=title)
        
        self.name = discord.ui.TextInput(
            label="Alliance Name",
            placeholder="Enter alliance name",
            default=default_name,
            required=True
        )
        self.add_item(self.name)
        
        self.interval = discord.ui.TextInput(
            label="Control Interval (minutes)",
            placeholder="Enter interval (0 to disable)",
            default=default_interval,
            required=True
        )
        self.add_item(self.interval)

    async def on_submit(self, interaction: discord.Interaction):
        self.interaction = interaction

class PaginatedDeleteView(discord.ui.View):
    def __init__(self, pages, original_callback):
        super().__init__(timeout=7200)
        self.current_page = 0
        self.pages = pages
        self.original_callback = original_callback
        self.total_pages = len(pages)
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        select = discord.ui.Select(
            placeholder=f"Select alliance to delete ({self.current_page + 1}/{self.total_pages})",
            options=self.pages[self.current_page]
        )
        select.callback = self.original_callback
        self.add_item(select)
        
        previous_button = discord.ui.Button(
            label=f"◀️",
            style=discord.ButtonStyle.grey,
            custom_id="previous",
            disabled=(self.current_page == 0)
        )
        previous_button.callback = self.previous_callback
        self.add_item(previous_button)

        next_button = discord.ui.Button(
            label=f"▶️",
            style=discord.ButtonStyle.grey,
            custom_id="next",
            disabled=(self.current_page == len(self.pages) - 1)
        )
        next_button.callback = self.next_callback
        self.add_item(next_button)

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page - 1) % len(self.pages)
        self.update_view()
        
        embed = discord.Embed(
            title=f"🗑️ Delete Alliance",
            description=(
                f"**⚠️ Warning: This action cannot be undone!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"1️⃣ Select an alliance from the dropdown menu\n"
                f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.red()
        )
        embed.set_footer(text=f"⚠️ Warning: Deleting an alliance will remove all its data!")
        embed.timestamp = discord.utils.utcnow()
        
        await interaction.response.edit_message(embed=embed, view=self)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page + 1) % len(self.pages)
        self.update_view()
        
        embed = discord.Embed(
            title=f"🗑️ Delete Alliance",
            description=(
                f"**⚠️ Warning: This action cannot be undone!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                f"1️⃣ Select an alliance from the dropdown menu\n"
                f"2️⃣ Use ◀️ ▶️ buttons to navigate between pages\n\n"
                f"**Current Page:** {self.current_page + 1}/{self.total_pages}\n"
                f"**Total Alliances:** {sum(len(page) for page in self.pages)}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.red()
        )
        embed.set_footer(text=f"⚠️ Warning: Deleting an alliance will remove all its data!")
        embed.timestamp = discord.utils.utcnow()
        
        await interaction.response.edit_message(embed=embed, view=self)

class PaginatedChannelView(discord.ui.View):
    def __init__(self, channels, original_callback):
        super().__init__(timeout=7200)
        self.current_page = 0
        self.channels = channels
        self.original_callback = original_callback
        self.items_per_page = 25
        self.pages = [channels[i:i + self.items_per_page] for i in range(0, len(channels), self.items_per_page)]
        self.total_pages = len(self.pages)
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        current_channels = self.pages[self.current_page]
        # Build options list without nested f-strings for Python 3.9+ compatibility
        channel_options = []
        for channel in current_channels:
            channel_label = f"#{channel.name}"[:100]
            # Determine description based on channel name length
            if len(f"#{channel.name}") > 40:
                option_description = f"Channel ID: {channel.id}"
            else:
                option_description = None

            channel_options.append(discord.SelectOption(
                label=channel_label,
                value=str(channel.id),
                description=option_description,
                emoji=f"📢"
            ))
        
        select = discord.ui.Select(
            placeholder=f"Select channel ({self.current_page + 1}/{self.total_pages})",
            options=channel_options
        )
        select.callback = self.original_callback
        self.add_item(select)
        
        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label=f"◀️",
                style=discord.ButtonStyle.grey,
                custom_id="previous",
                disabled=(self.current_page == 0)
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label=f"▶️",
                style=discord.ButtonStyle.grey,
                custom_id="next",
                disabled=(self.current_page == len(self.pages) - 1)
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page - 1) % len(self.pages)
        self.update_view()
        
        embed = interaction.message.embeds[0]
        embed.description = (
            f"**Page:** {self.current_page + 1}/{self.total_pages}\n"
            f"**Total Channels:** {len(self.channels)}\n\n"
            "Please select a channel from the menu below."
        )
        
        await interaction.response.edit_message(embed=embed, view=self)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = (self.current_page + 1) % len(self.pages)
        self.update_view()
        
        embed = interaction.message.embeds[0]
        embed.description = (
            f"**Page:** {self.current_page + 1}/{self.total_pages}\n"
            f"**Total Channels:** {len(self.channels)}\n\n"
            "Please select a channel from the menu below."
        )
        
        await interaction.response.edit_message(embed=embed, view=self)

async def setup(bot):
    conn = sqlite3.connect('db/alliance.sqlite')
    await bot.add_cog(Alliance(bot, conn))