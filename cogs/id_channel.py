import discord
from discord.ext import commands
import sqlite3
from datetime import datetime, timedelta
import os
import asyncio
import time
import hashlib
import aiohttp
import ssl
from discord.ext import tasks

SECRET = "tB87#kPtkxqOS2"

class IDChannel(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.setup_database()
        self.log_directory = 'log'
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

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

    def setup_database(self):
        if not os.path.exists('db'):
            os.makedirs('db')
            
        conn = sqlite3.connect('db/id_channel.sqlite')
        c = conn.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS id_channels
                     (guild_id INTEGER, 
                      alliance_id INTEGER,
                      channel_id INTEGER,
                      created_at TEXT,
                      created_by INTEGER,
                      UNIQUE(guild_id, channel_id))''')
        conn.commit()
        conn.close()

    async def log_action(self, action_type: str, user_id: int, guild_id: int, details: dict):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_file_path = os.path.join(self.log_directory, 'id_channel_log.txt')
        
        guild = self.bot.get_guild(guild_id)
        guild_name = guild.name if guild else "Unknown Server"
        
        user_name = "Unknown User"
        if guild:
            member = guild.get_member(user_id)
            if member:
                user_name = f"{member.name}#{member.discriminator}" if member.discriminator != '0' else member.name
        
        if user_name == "Unknown User":
            try:
                user = await self.bot.fetch_user(user_id)
                if user:
                    user_name = f"{user.name}#{user.discriminator}" if user.discriminator != '0' else user.name
            except:
                pass
        
        with open(log_file_path, 'a', encoding='utf-8') as log_file:
            log_file.write(f"\n{'='*50}\n")
            log_file.write(f"Timestamp: {timestamp}\n")
            log_file.write(f"Action: {action_type}\n")
            log_file.write(f"User: {user_name} (ID: {user_id})\n")
            log_file.write(f"Server: {guild_name} (ID: {guild_id})\n")
            log_file.write("Details:\n")
            for key, value in details.items():
                log_file.write(f"  {key}: {value}\n")
            log_file.write(f"{'='*50}\n")

    @commands.Cog.listener()
    async def on_ready(self):
        try:
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, alliance_id FROM id_channels")
                channels = cursor.fetchall()

            invalid_channels = []
            for channel_id, alliance_id in channels:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    invalid_channels.append(channel_id)
                    continue

                async for message in channel.history(limit=None, after=datetime.utcnow() - timedelta(days=1)):
                    if message.author.bot:
                        continue

                    has_bot_reaction = False
                    for reaction in message.reactions:
                        async for user in reaction.users():
                            if user == self.bot.user:
                                has_bot_reaction = True
                                break
                        if has_bot_reaction:
                            break

                    if has_bot_reaction:
                        continue

                    content = message.content.strip()
                    if not content.isdigit():
                        continue

                    fid = int(content)
                    await self.process_fid(message, fid, alliance_id)

            if invalid_channels:
                with sqlite3.connect('db/id_channel.sqlite') as db:
                    cursor = db.cursor()
                    placeholders = ','.join('?' * len(invalid_channels))
                    cursor.execute(f"""
                        DELETE FROM id_channels 
                        WHERE channel_id IN ({placeholders})
                    """, invalid_channels)
                    db.commit()

            if not self.check_channels_loop.is_running():
                self.check_channels_loop.start()

        except Exception as e:
            pass

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        try:
            if message.author.bot or not message.guild:
                return

            for reaction in message.reactions:
                async for user in reaction.users():
                    if user == self.bot.user:
                        return

            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT alliance_id FROM id_channels WHERE channel_id = ?", (message.channel.id,))
                channel_info = cursor.fetchone()
            
            if not channel_info:
                return

            alliance_id = channel_info[0]
            content = message.content.strip()

            if not content.isdigit():
                await message.add_reaction('❌')
                return

            fid = int(content)
            await self.process_fid(message, fid, alliance_id)

        except Exception as e:
            await message.add_reaction('❌')

    async def process_fid(self, message, fid, alliance_id):
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                existing_alliance = cursor.fetchone()
                
                if existing_alliance:
                    if existing_alliance[0] == alliance_id:
                        await message.add_reaction('⚠️')
                        await message.reply(f"This ID ({fid}) is already registered in this alliance!", delete_after=10)
                        return
                    else:
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            alliance_cursor = alliance_db.cursor()
                            alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (existing_alliance[0],))
                            alliance_name = alliance_cursor.fetchone()
                        
                        await message.add_reaction('⚠️')
                        await message.reply(
                            f"This ID ({fid}) is already registered in another alliance: `{alliance_name[0] if alliance_name else 'Unknown Alliance'}`",
                            delete_after=10
                        )
                        return

            max_retries = 3
            retry_delay = 60

            for attempt in range(max_retries):
                try:
                    current_time = int(time.time() * 1000)
                    form = f"fid={fid}&time={current_time}"
                    sign = hashlib.md5((form + SECRET).encode('utf-8')).hexdigest()
                    form = f"sign={sign}&{form}"
                    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE

                    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
                        async with session.post('https://wos-giftcode-api.centurygame.com/api/player', headers=headers, data=form) as response:
                            if response.status == 429:
                                if attempt < max_retries - 1:
                                    warning_embed = discord.Embed(
                                        title="⚠️ API Rate Limit Reached",
                                        description=(
                                            f"Operation is on hold due to API rate limit.\n"
                                            f"**Remaining Attempts:** `{max_retries - attempt - 1}`\n"
                                            f"**Wait Time:** `60 seconds`\n\n"
                                            f"Operation will continue automatically, please wait..."
                                        ),
                                        color=discord.Color.orange()
                                    )
                                    await message.reply(embed=warning_embed)
                                    await asyncio.sleep(retry_delay)
                                    continue
                                else:
                                    await message.add_reaction('❌')
                                    await message.reply("Operation failed due to API rate limit. Please try again later.", delete_after=10)
                                    return

                            if response.status == 200:
                                data = await response.json()

                                if data.get('data'):
                                    nickname = data['data'].get('nickname')
                                    furnace_lv = data['data'].get('stove_lv', 0)
                                    stove_lv_content = data['data'].get('stove_lv_content', None)
                                    kid = data['data'].get('kid', None)
                                    avatar_image = data['data'].get('avatar_image', None)

                                    try:
                                        with sqlite3.connect('db/users.sqlite') as users_db:
                                            cursor = users_db.cursor()
                                            cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                                            if cursor.fetchone():
                                                await message.add_reaction('⚠️')
                                                await message.reply(f"This ID ({fid}) was added by another process!", delete_after=10)
                                                return
                                                
                                            cursor.execute("""
                                                INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance)
                                                VALUES (?, ?, ?, ?, ?, ?)
                                            """, (fid, nickname, furnace_lv, kid, stove_lv_content, alliance_id))
                                            users_db.commit()
                                    except sqlite3.IntegrityError:
                                        await message.add_reaction('⚠️')
                                        await message.reply(f"This ID ({fid}) was added by another process!", delete_after=10)
                                        return

                                    await message.add_reaction('✅')

                                    if furnace_lv > 30:
                                        furnace_level_name = self.level_mapping.get(furnace_lv, f"Level {furnace_lv}")
                                    else:
                                        furnace_level_name = f"Level {furnace_lv}"

                                    success_embed = discord.Embed(
                                        title=f"✅ Member Successfully Added",
                                        description=(
                                            "━━━━━━━━━━━━━━━━━━━━━━\n"
                                            f"**👤 Name:** `{nickname}`\n"
                                            f"**🆔 ID:** `{fid}`\n"
                                            f"**🔥 Furnace Level:** `{furnace_level_name}`\n"
                                            f"**🌍 State:** `{kid}`\n"
                                            "━━━━━━━━━━━━━━━━━━━━━━"
                                        ),
                                        color=discord.Color.green()
                                    )

                                    if avatar_image:
                                        success_embed.set_image(url=avatar_image)
                                    if isinstance(stove_lv_content, str) and stove_lv_content.startswith("http"):
                                        success_embed.set_thumbnail(url=stove_lv_content)

                                    await message.reply(embed=success_embed)

                                    await self.log_action(
                                        "ADD_MEMBER",
                                        message.author.id,
                                        message.guild.id,
                                        {
                                            "fid": fid,
                                            "nickname": nickname,
                                            "alliance_id": alliance_id,
                                            "furnace_level": furnace_level_name
                                        }
                                    )
                                    return
                                else:
                                    await message.add_reaction('❌')
                                    await message.reply("No player found for this ID!", delete_after=10)
                                    return

                except Exception as e:
                    if attempt < max_retries - 1:
                        continue
                    else:
                        await message.add_reaction('❌')
                        await message.reply("An error occurred during the process!", delete_after=10)
                        return

        except Exception as e:
            await message.add_reaction('❌')
            await message.reply("An error occurred during the process!", delete_after=10)

    @tasks.loop(seconds=300)
    async def check_channels_loop(self):
        try:
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, alliance_id FROM id_channels")
                channels = cursor.fetchall()

            current_time = datetime.utcnow()
            five_minutes_ago = current_time.timestamp() - 300

            for channel_id, alliance_id in channels:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    continue

                messages_to_check = []
                async for message in channel.history(limit=50):
                    if message.created_at.timestamp() < five_minutes_ago:
                        has_bot_reaction = False
                        for reaction in message.reactions:
                            async for user in reaction.users():
                                if user == self.bot.user:
                                    has_bot_reaction = True
                                    break
                            if has_bot_reaction:
                                break
                        
                        if not has_bot_reaction:
                            messages_to_check.append(message)
                    else:
                        break

                for message in messages_to_check:
                    if message.author.bot:
                        continue

                    content = message.content.strip()
                    if not content.isdigit():
                        await message.add_reaction('❌')
                        continue

                    fid = int(content)
                    await self.process_fid(message, fid, alliance_id)

        except Exception as e:
            pass

    async def show_id_channel_menu(self, interaction: discord.Interaction):
        try:
            is_admin = False
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
                result = cursor.fetchone()
                if result:
                    is_admin = True

            if not is_admin:
                await interaction.response.send_message(
                    "❌ You don't have permission to use this feature.", 
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="🆔 ID Channel Management",
                description=(
                    "Manage your alliance ID channels here:\n\n"
                    "**Available Operations**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "➕ Create new ID channel\n"
                    "🗑️ Delete existing ID channel\n"
                    "📋 View active ID channels\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )
            
            view = IDChannelView(self)
            
            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                pass
                
        except Exception as e:
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred. Please try again.",
                    ephemeral=True
                )

class IDChannelView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="View Channels",
        emoji="📋",
        style=discord.ButtonStyle.secondary,
        custom_id="view_id_channels",
        row=1
    )
    async def view_channels_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            channels = []
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT channel_id, alliance_id, created_at, created_by
                    FROM id_channels 
                    WHERE guild_id = ?
                """, (interaction.guild_id,))
                id_channels = cursor.fetchall()

            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                alliance_cursor = alliance_db.cursor()
                for channel_id, alliance_id, created_at, created_by in id_channels:
                    alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_name = alliance_cursor.fetchone()
                    if alliance_name:
                        channels.append((channel_id, alliance_name[0], created_at, created_by))

            if not channels:
                await interaction.response.send_message(
                    "❌ No active ID channels found in this server.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="📋 Active ID Channels",
                color=discord.Color.blue()
            )

            for channel_id, alliance_name, created_at, created_by in channels:
                channel = interaction.guild.get_channel(channel_id)
                if channel:
                    creator = None
                    try:
                        creator = await interaction.guild.fetch_member(created_by)
                    except:
                        try:
                            creator = await interaction.client.fetch_user(created_by)
                        except:
                            pass

                    creator_text = creator.mention if creator else f"Unknown (ID: {created_by})"
                    
                    embed.add_field(
                        name=f"#{channel.name}",
                        value=f"**Alliance:** {alliance_name}\n"
                              f"**Created At:** {created_at}\n"
                              f"**Created By:** {creator_text}",
                        inline=False
                    )

            await interaction.response.send_message(embed=embed, ephemeral=True)

        except Exception as e:
            await interaction.response.send_message(
                "❌ An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Delete Channel",
        emoji="🗑️",
        style=discord.ButtonStyle.danger,
        custom_id="delete_id_channel",
        row=0
    )
    async def delete_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            channels = []
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, alliance_id FROM id_channels WHERE guild_id = ?", (interaction.guild_id,))
                id_channels = cursor.fetchall()

            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                alliance_cursor = alliance_db.cursor()
                for channel_id, alliance_id in id_channels:
                    alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                    alliance_name = alliance_cursor.fetchone()
                    if alliance_name:
                        channels.append((channel_id, alliance_name[0]))

            if not channels:
                await interaction.response.send_message(
                    "❌ No active ID channels found in this server.",
                    ephemeral=True
                )
                return

            options = []
            for channel_id, alliance_name in channels:
                channel = interaction.guild.get_channel(channel_id)
                if channel:
                    options.append(
                        discord.SelectOption(
                            label=f"#{channel.name}",
                            value=str(channel_id),
                            description=f"Alliance: {alliance_name}"
                        )
                    )

            class ChannelSelect(discord.ui.Select):
                def __init__(self):
                    super().__init__(
                        placeholder="Select ID channel to delete",
                        options=options,
                        custom_id="delete_channel_select"
                    )

                async def callback(self, select_interaction: discord.Interaction):
                    try:
                        channel_id = int(self.values[0])

                        with sqlite3.connect('db/id_channel.sqlite') as db:
                            cursor = db.cursor()
                            cursor.execute("DELETE FROM id_channels WHERE channel_id = ?", (channel_id,))
                            db.commit()

                        channel = select_interaction.guild.get_channel(channel_id)
                        
                        await self.view.cog.log_action(
                            "DELETE_CHANNEL",
                            select_interaction.user.id,
                            select_interaction.guild_id,
                            {
                                "channel_id": channel_id,
                                "channel_name": channel.name if channel else "Unknown"
                            }
                        )

                        success_embed = discord.Embed(
                            title="✅ ID Channel Deleted",
                            description=f"**Channel:** {channel.mention if channel else 'Deleted Channel'}\n\n"
                                      f"This channel will no longer be used as an ID channel.",
                            color=discord.Color.green()
                        )
                        
                        if not select_interaction.response.is_done():
                            await select_interaction.response.edit_message(embed=success_embed, view=None)
                        else:
                            await select_interaction.message.edit(embed=success_embed, view=None)
                            
                    except Exception as e:
                        error_embed = discord.Embed(
                            title="❌ Error",
                            description="An error occurred while deleting the channel.",
                            color=discord.Color.red()
                        )
                        if not select_interaction.response.is_done():
                            await select_interaction.response.edit_message(embed=error_embed, view=None)
                        else:
                            await select_interaction.message.edit(embed=error_embed, view=None)

            view = discord.ui.View()
            view.cog = self.cog
            view.add_item(ChannelSelect())
            
            select_embed = discord.Embed(
                title="🗑️ Delete ID Channel",
                description="Select the ID channel you want to delete:",
                color=discord.Color.red()
            )
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            await interaction.response.send_message(
                "❌ An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Create Channel",
        emoji="➕",
        style=discord.ButtonStyle.success,
        custom_id="create_id_channel",
        row=0
    )
    async def create_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT alliance_id, name FROM alliance_list")
                alliances = cursor.fetchall()

            if not alliances:
                await interaction.response.send_message(
                    "❌ No alliances found.", 
                    ephemeral=True
                )
                return

            options = [
                discord.SelectOption(
                    label=name,
                    value=str(alliance_id),
                    description=f"Alliance ID: {alliance_id}"
                ) for alliance_id, name in alliances
            ]

            class AllianceSelect(discord.ui.Select):
                def __init__(self):
                    super().__init__(
                        placeholder="Select an alliance",
                        options=options,
                        custom_id="alliance_select"
                    )

                async def callback(self, select_interaction: discord.Interaction):
                    alliance_id = int(self.values[0])
                    
                    class ChannelSelect(discord.ui.ChannelSelect):
                        def __init__(self):
                            super().__init__(
                                placeholder="Select a channel to use as ID channel",
                                channel_types=[discord.ChannelType.text]
                            )

                        async def callback(self, channel_interaction: discord.Interaction):
                            selected_channel = self.values[0]
                            
                            try:
                                with sqlite3.connect('db/id_channel.sqlite') as db:
                                    cursor = db.cursor()
                                    cursor.execute("""
                                        INSERT INTO id_channels 
                                        (guild_id, alliance_id, channel_id, created_at, created_by)
                                        VALUES (?, ?, ?, ?, ?)
                                    """, (
                                        channel_interaction.guild_id,
                                        alliance_id,
                                        selected_channel.id,
                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        channel_interaction.user.id
                                    ))
                                    db.commit()

                                await self.view.cog.log_action(
                                    "CREATE_CHANNEL",
                                    channel_interaction.user.id,
                                    channel_interaction.guild_id,
                                    {
                                        "alliance_id": alliance_id,
                                        "channel_id": selected_channel.id,
                                        "channel_name": selected_channel.name
                                    }
                                )

                                success_embed = discord.Embed(
                                    title="✅ ID Channel Created",
                                    description=f"**Channel:** {selected_channel.mention}\n"
                                              f"**Alliance:** {dict(alliances)[alliance_id]}\n\n"
                                              f"This channel will now automatically check and add IDs to the alliance.",
                                    color=discord.Color.green()
                                )
                                await channel_interaction.response.edit_message(embed=success_embed, view=None)

                            except sqlite3.IntegrityError:
                                error_embed = discord.Embed(
                                    title="❌ Error",
                                    description="This channel is already being used as an ID channel!",
                                    color=discord.Color.red()
                                )
                                await channel_interaction.response.edit_message(embed=error_embed, view=None)
                            except Exception as e:
                                error_embed = discord.Embed(
                                    title="❌ Error",
                                    description="An error occurred while creating the channel.",
                                    color=discord.Color.red()
                                )
                                await channel_interaction.response.edit_message(embed=error_embed, view=None)

                    channel_view = discord.ui.View()
                    channel_view.cog = self.view.cog
                    channel_view.add_item(ChannelSelect())
                    
                    select_embed = discord.Embed(
                        title="🔧 ID Channel Setup",
                        description="Select a channel to use as ID channel:",
                        color=discord.Color.blue()
                    )
                    await select_interaction.response.edit_message(embed=select_embed, view=channel_view)

            alliance_view = discord.ui.View()
            alliance_view.cog = self.cog
            alliance_view.add_item(AllianceSelect())
            
            initial_embed = discord.Embed(
                title="🔧 ID Channel Setup",
                description="Select an alliance for the ID channel:",
                color=discord.Color.blue()
            )
            await interaction.response.send_message(
                embed=initial_embed,
                view=alliance_view,
                ephemeral=True
            )

        except Exception as e:
            await interaction.response.send_message(
                "❌ An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Back",
        emoji="◀️",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_other_features",
        row=2
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            other_features_cog = self.cog.bot.get_cog("OtherFeatures")
            if other_features_cog:
                await other_features_cog.show_other_features_menu(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Other Features module not found.",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.response.send_message(
                "❌ An error occurred while returning to Other Features menu.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(IDChannel(bot)) 