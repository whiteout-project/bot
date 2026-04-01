"""
ID channel management. Configures channels where member IDs are displayed.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from datetime import datetime, timedelta, timezone
import os
import asyncio
import time
from discord.ext import tasks
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message
from .login_handler import LoginHandler

logger = logging.getLogger('alliance')

class AllianceIDChannel(commands.Cog):
    BACKOFF_DURATION = 300  # 5 minutes between invalid format warnings per channel

    def __init__(self, bot):
        self.bot = bot
        self.setup_database()
        self.log_directory = 'log'
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

        self.invalid_format_warnings = {}  # {channel_id: last_warning_timestamp}

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

        c.execute('''CREATE TABLE IF NOT EXISTS id_channel_settings (
                     guild_id INTEGER PRIMARY KEY,
                     scan_enabled INTEGER DEFAULT 1,
                     scan_limit INTEGER DEFAULT 50,
                     delete_after INTEGER DEFAULT 10
                 )''')

        conn.commit()
        conn.close()

    def get_guild_settings(self, guild_id):
        with sqlite3.connect('db/id_channel.sqlite') as db:
            db.row_factory = sqlite3.Row
            cursor = db.cursor()
            cursor.execute("SELECT * FROM id_channel_settings WHERE guild_id = ?", (guild_id,))
            row = cursor.fetchone()
            if row:
                return dict(row)
        return {'guild_id': guild_id, 'scan_enabled': 1, 'scan_limit': 50, 'delete_after': 10}

    def ensure_guild_settings(self, guild_id):
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("INSERT OR IGNORE INTO id_channel_settings (guild_id) VALUES (?)", (guild_id,))
            db.commit()

    async def warn_invalid_format(self, message):
        channel_id = message.channel.id
        now = time.time()
        last_warning = self.invalid_format_warnings.get(channel_id, 0)

        if now - last_warning < self.BACKOFF_DURATION:
            return

        self.invalid_format_warnings[channel_id] = now
        settings = self.get_guild_settings(message.guild.id)
        await message.add_reaction(theme.deniedIcon)
        await message.reply("Please enter a valid numeric ID.", delete_after=settings['delete_after'])

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
            except Exception:
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
                cursor.execute("SELECT channel_id, alliance_id, guild_id FROM id_channels")
                channels = cursor.fetchall()

            invalid_channels = []
            for channel_id, alliance_id, guild_id in channels:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    invalid_channels.append(channel_id)
                    continue

                settings = self.get_guild_settings(guild_id)
                if not settings['scan_enabled']:
                    continue

                async for message in channel.history(limit=settings['scan_limit'], after=datetime.now(timezone.utc) - timedelta(days=1)):
                    if message.author.bot:
                        continue

                    already_processed = any(reaction.me for reaction in message.reactions)
                    if already_processed:
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
            logger.error(f"Error in on_ready: {e}")
            print(f"Error in on_ready: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild:
            return

        try:
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT alliance_id FROM id_channels WHERE channel_id = ?", (message.channel.id,))
                channel_info = cursor.fetchone()

            if not channel_info:
                return

            alliance_id = channel_info[0]
            content = message.content.strip()

            if not content.isdigit():
                await self.warn_invalid_format(message)
                return

            fid = int(content)
            await self.process_fid(message, fid, alliance_id)

        except Exception as e:
            logger.error(f"Error in on_message: {e}")
            print(f"Error in on_message: {e}")

    async def process_fid(self, message, fid, alliance_id):
        settings = self.get_guild_settings(message.guild.id)
        delete_after = settings['delete_after']

        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                existing_alliance = cursor.fetchone()

                if existing_alliance:
                    existing_alliance_id = int(existing_alliance[0]) if existing_alliance[0] else None
                    if existing_alliance_id == alliance_id:
                        await message.add_reaction(theme.warnIcon)
                        await message.reply(f"This ID ({fid}) is already registered in this alliance!", delete_after=delete_after)
                        return
                    else:
                        with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                            alliance_cursor = alliance_db.cursor()
                            alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (existing_alliance[0],))
                            alliance_name = alliance_cursor.fetchone()

                        await message.add_reaction(theme.warnIcon)
                        await message.reply(
                            f"This ID ({fid}) is already registered in another alliance: `{alliance_name[0] if alliance_name else 'Unknown Alliance'}`",
                            delete_after=delete_after
                        )
                        return

            result = await LoginHandler().fetch_player_data(str(fid))

            if result['status'] == 'rate_limited':
                wait_time = result.get('wait_time', 60)
                warning_embed = discord.Embed(
                    title=f"{theme.warnIcon} API Rate Limit Reached",
                    description=(
                        f"Operation is on hold due to API rate limit.\n"
                        f"**Wait Time:** `{int(wait_time)} seconds`\n\n"
                        f"Operation will continue automatically, please wait..."
                    ),
                    color=discord.Color.orange()
                )
                await message.reply(embed=warning_embed)
                await asyncio.sleep(wait_time)
                result = await LoginHandler().fetch_player_data(str(fid))

            if result['status'] == 'rate_limited':
                await message.add_reaction(theme.deniedIcon)
                await message.reply("Operation failed due to API rate limit. Please try again later.", delete_after=delete_after)
                return

            if result['status'] == 'not_found':
                await message.add_reaction(theme.deniedIcon)
                await message.reply("No player found for this ID!", delete_after=delete_after)
                return

            if result['status'] == 'error':
                await message.add_reaction(theme.deniedIcon)
                error_msg = result.get('error_message', 'An error occurred during the process!')
                await message.reply(error_msg, delete_after=delete_after)
                return

            if result['status'] == 'success' and result['data']:
                nickname = result['data'].get('nickname')
                furnace_lv = result['data'].get('stove_lv', 0)
                stove_lv_content = result['data'].get('stove_lv_content', None)
                kid = result['data'].get('kid', None)
                avatar_image = result['data'].get('avatar_image', None)

                try:
                    with sqlite3.connect('db/users.sqlite') as users_db:
                        cursor = users_db.cursor()
                        cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                        if cursor.fetchone():
                            await message.add_reaction(theme.warnIcon)
                            await message.reply(f"This ID ({fid}) was added by another process!", delete_after=delete_after)
                            return

                        cursor.execute("""
                            INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (fid, nickname, furnace_lv, kid, stove_lv_content, alliance_id))
                        users_db.commit()
                except sqlite3.IntegrityError:
                    await message.add_reaction(theme.warnIcon)
                    await message.reply(f"This ID ({fid}) was added by another process!", delete_after=delete_after)
                    return

                await message.add_reaction(theme.verifiedIcon)

                if furnace_lv > 30:
                    furnace_level_name = self.level_mapping.get(furnace_lv, f"Level {furnace_lv}")
                else:
                    furnace_level_name = f"Level {furnace_lv}"

                success_embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Member Successfully Added",
                    description=(
                        f"{theme.upperDivider}\n"
                        f"**{theme.userIcon} Name:** `{nickname}`\n"
                        f"**{theme.fidIcon} ID:** `{fid}`\n"
                        f"**{theme.levelIcon} Furnace Level:** `{furnace_level_name}`\n"
                        f"**{theme.globeIcon} State:** `{kid}`\n"
                        f"{theme.lowerDivider}"
                    ),
                    color=theme.emColor3
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

        except Exception as e:
            logger.error(f"Error in process_fid: {e}")
            print(f"Error in process_fid: {e}")
            await message.add_reaction(theme.deniedIcon)
            await message.reply("An error occurred during the process!", delete_after=delete_after)

    @tasks.loop(seconds=300)
    async def check_channels_loop(self):
        try:
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, alliance_id, guild_id FROM id_channels")
                channels = cursor.fetchall()

            five_minutes_ago = datetime.now(timezone.utc) - timedelta(minutes=5)

            for channel_id, alliance_id, guild_id in channels:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    continue

                settings = self.get_guild_settings(guild_id)
                if not settings['scan_enabled']:
                    continue

                async for message in channel.history(limit=settings['scan_limit'], after=five_minutes_ago):
                    if message.author.bot:
                        continue

                    already_processed = any(reaction.me for reaction in message.reactions)
                    if already_processed:
                        continue

                    content = message.content.strip()
                    if not content.isdigit():
                        await self.warn_invalid_format(message)
                        continue

                    fid = int(content)
                    await self.process_fid(message, fid, alliance_id)

        except Exception as e:
            logger.error(f"Error in check_channels_loop: {e}")
            if '503' in str(e) or '502' in str(e) or 'connect error' in str(e).lower():
                print(f"ID Channel check skipped — Discord is temporarily unavailable. This is normal and will resolve itself.")
            else:
                print(f"Error in check_channels_loop: {e}")

    async def show_id_channel_menu(self, interaction: discord.Interaction):
        try:
            is_admin, _ = PermissionManager.is_admin(interaction.user.id)

            if not is_admin:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You don't have permission to use this feature.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"{theme.fidIcon} ID Channel Management",
                description=(
                    f"Manage your alliance ID channels here:\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.addIcon} Create new ID channel\n"
                    f"{theme.listIcon} View active ID channels\n"
                    f"{theme.trashIcon} Delete existing ID channel\n"
                    f"{theme.settingsIcon} Configure scan and auto-delete settings\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )
            
            view = IDChannelView(self)

            await safe_edit_message(interaction, embed=embed, view=view)
                
        except Exception as e:
            logger.error(f"Error in show_id_channel_menu: {e}")
            print(f"Error in show_id_channel_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred. Please try again.",
                    ephemeral=True
                )

class ScanLimitModal(discord.ui.Modal):
    def __init__(self, cog, current_value):
        super().__init__(title="Set Scan Message Limit")
        self.cog = cog
        self.limit_input = discord.ui.TextInput(
            label="Max messages to scan per channel (1-200)",
            placeholder="50",
            default=str(current_value),
            required=True,
            max_length=3
        )
        self.add_item(self.limit_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.limit_input.value.strip())
            if value < 1 or value > 200:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a value between 1 and 200.",
                    ephemeral=True
                )
                return

            self.cog.ensure_guild_settings(interaction.guild_id)
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("UPDATE id_channel_settings SET scan_limit = ? WHERE guild_id = ?",
                              (value, interaction.guild_id))
                db.commit()

            await IDChannelSettingsView(self.cog).show(interaction)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )


class DeleteAfterModal(discord.ui.Modal):
    def __init__(self, cog, current_value):
        super().__init__(title="Set Auto-Delete Timer")
        self.cog = cog
        self.timer_input = discord.ui.TextInput(
            label="Seconds before bot replies auto-delete (0-300)",
            placeholder="10",
            default=str(current_value),
            required=True,
            max_length=3
        )
        self.add_item(self.timer_input)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            value = int(self.timer_input.value.strip())
            if value < 0 or value > 300:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a value between 0 and 300 seconds.",
                    ephemeral=True
                )
                return

            # 0 means no auto-delete (None in the DB)
            db_value = None if value == 0 else value

            self.cog.ensure_guild_settings(interaction.guild_id)
            with sqlite3.connect('db/id_channel.sqlite') as db:
                cursor = db.cursor()
                cursor.execute("UPDATE id_channel_settings SET delete_after = ? WHERE guild_id = ?",
                              (db_value, interaction.guild_id))
                db.commit()

            await IDChannelSettingsView(self.cog).show(interaction)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )


class IDChannelSettingsView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog

    def build_embed(self, settings):
        scan_status = f"{theme.verifiedIcon} Enabled" if settings['scan_enabled'] else f"{theme.deniedIcon} Disabled"
        scan_limit = settings['scan_limit']
        delete_after = settings['delete_after']
        delete_text = "Permanent (no auto-delete)" if delete_after is None else f"{delete_after} seconds"

        return discord.Embed(
            title=f"{theme.settingsIcon} ID Channel Settings",
            description=(
                f"Configure how ID channels behave on this server.\n\n"
                f"{theme.upperDivider}\n"
                f"{theme.settingsIcon} **Startup Scan:** {scan_status}\n"
                f"└ Scan for missed IDs when the bot starts or every 5 minutes\n\n"
                f"{theme.listIcon} **Scan Limit:** `{scan_limit}` messages per channel\n"
                f"└ Max messages checked per scan (lower = fewer API calls)\n\n"
                f"{theme.editIcon} **Auto-Delete:** `{delete_text}`\n"
                f"└ How long bot replies (errors, warnings) stay visible\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

    async def show(self, interaction: discord.Interaction):
        settings = self.cog.get_guild_settings(interaction.guild_id)
        embed = self.build_embed(settings)
        await safe_edit_message(interaction, embed=embed, view=self)

    @discord.ui.button(label="Toggle Scan", style=discord.ButtonStyle.primary, row=0)
    async def toggle_scan(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = self.cog.get_guild_settings(interaction.guild_id)
        new_value = 0 if settings['scan_enabled'] else 1

        self.cog.ensure_guild_settings(interaction.guild_id)
        with sqlite3.connect('db/id_channel.sqlite') as db:
            cursor = db.cursor()
            cursor.execute("UPDATE id_channel_settings SET scan_enabled = ? WHERE guild_id = ?",
                          (new_value, interaction.guild_id))
            db.commit()

        settings['scan_enabled'] = new_value
        embed = self.build_embed(settings)
        await safe_edit_message(interaction, embed=embed, view=self)

    @discord.ui.button(label="Scan Limit", style=discord.ButtonStyle.secondary, row=0)
    async def set_scan_limit(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = self.cog.get_guild_settings(interaction.guild_id)
        await interaction.response.send_modal(ScanLimitModal(self.cog, settings['scan_limit']))

    @discord.ui.button(label="Auto-Delete Timer", style=discord.ButtonStyle.secondary, row=0)
    async def set_delete_after(self, interaction: discord.Interaction, button: discord.ui.Button):
        settings = self.cog.get_guild_settings(interaction.guild_id)
        current = settings['delete_after'] if settings['delete_after'] is not None else 0
        await interaction.response.send_modal(DeleteAfterModal(self.cog, current))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_id_channel_menu(interaction)


class IDChannelView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="View Channels",
        emoji=f"{theme.listIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="view_id_channels",
        row=0
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
                    f"{theme.deniedIcon} No active ID channels found in this server.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"{theme.listIcon} Active ID Channels",
                color=theme.emColor1
            )

            for channel_id, alliance_name, created_at, created_by in channels:
                channel = interaction.guild.get_channel(channel_id)
                if channel:
                    creator = None
                    try:
                        creator = await interaction.guild.fetch_member(created_by)
                    except Exception:
                        try:
                            creator = await interaction.client.fetch_user(created_by)
                        except Exception:
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
            logger.error(f"Error in view_channels_button: {e}")
            print(f"Error in view_channels_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Delete Channel",
        emoji=f"{theme.trashIcon}",
        style=discord.ButtonStyle.danger,
        custom_id="delete_id_channel",
        row=1
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
                    f"{theme.deniedIcon} No active ID channels found in this server.",
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
                            title=f"{theme.verifiedIcon} ID Channel Deleted",
                            description=f"**Channel:** {channel.mention if channel else 'Deleted Channel'}\n\n"
                                      f"This channel will no longer be used as an ID channel.",
                            color=theme.emColor3
                        )
                        
                        if not select_interaction.response.is_done():
                            await select_interaction.response.edit_message(embed=success_embed, view=None)
                        else:
                            await select_interaction.message.edit(embed=success_embed, view=None)
                            
                    except Exception as e:
                        logger.error(f"Error in delete channel select callback: {e}")
                        print(f"Error in delete channel select callback: {e}")
                        error_embed = discord.Embed(
                            title=f"{theme.deniedIcon} Error",
                            description="An error occurred while deleting the channel.",
                            color=theme.emColor2
                        )
                        if not select_interaction.response.is_done():
                            await select_interaction.response.edit_message(embed=error_embed, view=None)
                        else:
                            await select_interaction.message.edit(embed=error_embed, view=None)

            view = discord.ui.View()
            view.cog = self.cog
            view.add_item(ChannelSelect())
            
            select_embed = discord.Embed(
                title=f"{theme.trashIcon} Delete ID Channel",
                description="Select the ID channel you want to delete:",
                color=theme.emColor2
            )
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in delete_channel_button: {e}")
            print(f"Error in delete_channel_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Create Channel",
        emoji=f"{theme.addIcon}",
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
                    f"{theme.deniedIcon} No alliances found.", 
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
                                    title=f"{theme.verifiedIcon} ID Channel Created",
                                    description=f"**Channel:** {selected_channel.mention}\n"
                                              f"**Alliance:** {dict(alliances)[alliance_id]}\n\n"
                                              f"This channel will now automatically check and add IDs to the alliance.",
                                    color=theme.emColor3
                                )
                                await channel_interaction.response.edit_message(embed=success_embed, view=None)

                            except sqlite3.IntegrityError:
                                error_embed = discord.Embed(
                                    title=f"{theme.deniedIcon} Error",
                                    description="This channel is already being used as an ID channel!",
                                    color=theme.emColor2
                                )
                                await channel_interaction.response.edit_message(embed=error_embed, view=None)
                            except Exception as e:
                                logger.error(f"Error in channel select callback: {e}")
                                print(f"Error in channel select callback: {e}")
                                error_embed = discord.Embed(
                                    title=f"{theme.deniedIcon} Error",
                                    description="An error occurred while creating the channel.",
                                    color=theme.emColor2
                                )
                                await channel_interaction.response.edit_message(embed=error_embed, view=None)

                    channel_view = discord.ui.View()
                    channel_view.cog = self.view.cog
                    channel_view.add_item(ChannelSelect())
                    
                    select_embed = discord.Embed(
                        title=f"{theme.settingsIcon} ID Channel Setup",
                        description="Select a channel to use as ID channel:",
                        color=theme.emColor1
                    )
                    await select_interaction.response.edit_message(embed=select_embed, view=channel_view)

            alliance_view = discord.ui.View()
            alliance_view.cog = self.cog
            alliance_view.add_item(AllianceSelect())

            initial_embed = discord.Embed(
                title=f"{theme.settingsIcon} ID Channel Setup",
                description="Select an alliance for the ID channel:",
                color=theme.emColor1
            )
            await interaction.response.send_message(
                embed=initial_embed,
                view=alliance_view,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Error in create_channel_button: {e}")
            print(f"Error in create_channel_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred. Please try again.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Settings",
        emoji=f"{theme.settingsIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="id_channel_settings",
        row=1
    )
    async def settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await IDChannelSettingsView(self.cog).show(interaction)
        except Exception as e:
            logger.error(f"Error opening ID channel settings: {e}")
            print(f"Error opening ID channel settings: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred opening settings.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Back",
        emoji=f"{theme.backIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_self_registration",
        row=1
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Navigate back to Self-Registration sub-menu."""
        try:
            main_menu_cog = self.cog.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_self_registration(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Menu module not found.",
                    ephemeral=True
                )
        except Exception as e:
            logger.error(f"Error in back_button: {e}")
            print(f"Error in back_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while navigating back.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(AllianceIDChannel(bot)) 