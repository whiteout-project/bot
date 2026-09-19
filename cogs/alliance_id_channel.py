"""
ID channel management. Configures channels where member IDs are displayed.
"""
import discord
from discord.ext import commands
import sqlite3
import logging
from contextlib import closing
from datetime import datetime, timedelta, timezone
import os
import asyncio
import time
from discord.ext import tasks
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message
from .bot_level_mapping import LEVEL_MAPPING, format_furnace_level, parse_state
from .alliance_member_edit import new_member_name, parse_edit_line
from .alliance import check_alliance_state
from .gift_state_resolver import verify_add_state, is_multistate

logger = logging.getLogger('alliance')


def parse_id_post(content):
    """`fid` or `fid state`, or the comma form `fid, name, level, state`.
    Commas keep a numeric name from being read as a state. Returns
    (fid, name, level, state) with None for anything omitted, or None if unusable."""
    text = (content or "").strip()
    if "," in text:
        parsed = parse_edit_line(text)
        if isinstance(parsed, str):
            return None
        fid, name, level, state = parsed[:4]  # members can't self-report power
        return int(fid), name, level, state

    parts = text.split()
    if not (1 <= len(parts) <= 2) or not all(p.isdigit() for p in parts):
        return None
    state = None
    if len(parts) == 2:
        state = parse_state(parts[1])
        if state is None:
            return None
    return int(parts[0]), None, None, state


# Changing this orphans info messages already pinned by older versions.
INFO_MSG_HEADING = "Post your in-game ID here"


def id_format_examples(multistate):
    """(plain, detailed) post examples - one source so every hint stays in step."""
    if multistate:
        return "`12345678 245`", "`12345678, Name, FC 10, 245`"
    return "`12345678`", "`12345678, Name, FC 10`"


def render_info_message(alliance_id, alliance_name):
    """The standing message posted in an alliance's ID channel."""
    multistate = is_multistate(alliance_id)
    plain, detailed = id_format_examples(multistate)
    lines = [
        f"{theme.fidIcon} **{INFO_MSG_HEADING}**",
        f"Post your ID in this channel to join **{alliance_name}**. "
        f"The bot checks it and adds you.",
    ]
    if multistate:
        lines.append(
            f"{theme.globeIcon} This alliance has members in several states, so post your ID "
            f"**and** your state together: {plain}"
        )
    else:
        lines.append(f"{theme.fidIcon} Just your ID on its own: {plain}")
    lines.append(f"{theme.userIcon} To fill in your name and level too: {detailed}")
    return "\n".join(lines)


def info_settings(alliance_id):
    """(post, pin) for an alliance's ID channel info message."""
    with closing(sqlite3.connect('db/alliance.sqlite', timeout=30.0)) as conn, conn:
        row = conn.execute(
            "SELECT COALESCE(id_post_info_message, 0), COALESCE(id_pin_info_message, 1) "
            "FROM alliancesettings WHERE alliance_id = ?", (alliance_id,)).fetchone()
    return (bool(row[0]), bool(row[1])) if row else (False, True)


def set_info_settings(alliance_id, *, post=None, pin=None):
    with closing(sqlite3.connect('db/alliance.sqlite', timeout=30.0)) as conn, conn:
        conn.execute(
            "INSERT OR IGNORE INTO alliancesettings (alliance_id, channel_id, interval) "
            "VALUES (?, NULL, 0)", (alliance_id,))
        if post is not None:
            conn.execute("UPDATE alliancesettings SET id_post_info_message = ? WHERE alliance_id = ?",
                         (int(post), alliance_id))
        if pin is not None:
            conn.execute("UPDATE alliancesettings SET id_pin_info_message = ? WHERE alliance_id = ?",
                         (int(pin), alliance_id))
        conn.commit()


def channel_row_for_alliance(guild_id, alliance_id):
    """(channel_id, info_message_id) for an alliance's ID channel, or None."""
    with closing(sqlite3.connect('db/id_channel.sqlite', timeout=30.0)) as conn, conn:
        return conn.execute(
            "SELECT channel_id, info_message_id FROM id_channels "
            "WHERE guild_id = ? AND alliance_id = ?", (guild_id, alliance_id)).fetchone()


def channel_rows_for_guild(guild_id):
    """[(channel_id, alliance_id, info_message_id), ...] for every ID channel in a guild."""
    with closing(sqlite3.connect('db/id_channel.sqlite', timeout=30.0)) as conn, conn:
        return conn.execute(
            "SELECT channel_id, alliance_id, info_message_id FROM id_channels "
            "WHERE guild_id = ?", (guild_id,)).fetchall()


def all_channel_rows():
    """[(channel_id, alliance_id, info_message_id), ...] across every guild."""
    with closing(sqlite3.connect('db/id_channel.sqlite', timeout=30.0)) as conn, conn:
        return conn.execute(
            "SELECT channel_id, alliance_id, info_message_id FROM id_channels").fetchall()


def set_info_message_id(channel_id, message_id):
    with closing(sqlite3.connect('db/id_channel.sqlite', timeout=30.0)) as conn, conn:
        conn.execute("UPDATE id_channels SET info_message_id = ? WHERE channel_id = ?",
                     (message_id, channel_id))
        conn.commit()


class AllianceIDChannel(commands.Cog):
    BACKOFF_DURATION = 300  # 5 minutes between invalid format warnings per channel

    def __init__(self, bot):
        self.bot = bot
        self.setup_database()
        self.log_directory = 'log'
        if not os.path.exists(self.log_directory):
            os.makedirs(self.log_directory)

        self.invalid_format_warnings = {}  # {channel_id: last_warning_timestamp}

        self.level_mapping = LEVEL_MAPPING

    def cog_unload(self):
        if self.check_channels_loop.is_running():
            self.check_channels_loop.cancel()

    def setup_database(self):
        if not os.path.exists('db'):
            os.makedirs('db')

        with closing(sqlite3.connect('db/id_channel.sqlite')) as conn:
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
                         delete_after INTEGER DEFAULT 10,
                         respond_to_invalid INTEGER DEFAULT 0
                     )''')

            # Migrate older installs: add columns introduced after the table existed.
            c.execute("PRAGMA table_info(id_channel_settings)")
            existing_cols = {row[1] for row in c.fetchall()}
            if "delete_after" not in existing_cols:
                c.execute("ALTER TABLE id_channel_settings ADD COLUMN delete_after INTEGER DEFAULT 10")
            if "scan_limit" not in existing_cols:
                c.execute("ALTER TABLE id_channel_settings ADD COLUMN scan_limit INTEGER DEFAULT 50")
            if "scan_enabled" not in existing_cols:
                c.execute("ALTER TABLE id_channel_settings ADD COLUMN scan_enabled INTEGER DEFAULT 1")
            if "respond_to_invalid" not in existing_cols:
                c.execute("ALTER TABLE id_channel_settings ADD COLUMN respond_to_invalid INTEGER DEFAULT 0")

            conn.commit()

    def get_guild_settings(self, guild_id):
        defaults = {
            'guild_id': guild_id,
            'scan_enabled': 1,
            'scan_limit': 50,
            'delete_after': 10,
            'respond_to_invalid': 0,
        }
        with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
            db.row_factory = sqlite3.Row
            cursor = db.cursor()
            cursor.execute("SELECT * FROM id_channel_settings WHERE guild_id = ?", (guild_id,))
            row = cursor.fetchone()
        if row is None:
            return defaults
        # Merge with defaults so callers never KeyError if the schema is stale
        # (e.g. an admin's running install didn't yet pick up the ALTER TABLE
        # migration in setup_database).
        result = dict(defaults)
        result.update(dict(row))
        return result

    def ensure_guild_settings(self, guild_id):
        with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
            cursor = db.cursor()
            cursor.execute("INSERT OR IGNORE INTO id_channel_settings (guild_id) VALUES (?)", (guild_id,))
            db.commit()

    async def _safe_react_reply(self, message, emoji=None, content=None, *, delete_after=None, embed=None):
        """Best-effort reaction/reply that won't raise if the bot lacks channel perms."""
        if emoji is not None:
            try:
                await message.add_reaction(emoji)
            except discord.HTTPException:
                pass
        if content is not None or embed is not None:
            try:
                await message.reply(content, embed=embed, delete_after=delete_after)
            except discord.HTTPException:
                pass


    def _looks_like_info_message(self, msg):
        """True if this is a bot-authored ID channel info message."""
        if self.bot.user is None or msg.author.id != self.bot.user.id:
            return False
        return INFO_MSG_HEADING in (msg.content or "")

    async def _find_info_messages(self, channel, tracked_id=None):
        """Bot-authored info messages in the channel - pins are bounded to <=50."""
        try:
            pins = await channel.pins()
        except (discord.Forbidden, discord.HTTPException):
            pins = []
        found = [m for m in pins if self._looks_like_info_message(m)]
        if tracked_id and not any(m.id == tracked_id for m in found):
            # Stored message may exist but be unpinned.
            try:
                msg = await channel.fetch_message(tracked_id)
                if self._looks_like_info_message(msg):
                    found.append(msg)
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass
        return found

    async def refresh_info_message(self, channel_id, alliance_id, tracked_id=None):
        """Post, edit, pin or remove an alliance's standing info message to match its settings."""
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            return
        post, pin = await asyncio.to_thread(info_settings, alliance_id)
        found = await self._find_info_messages(channel, tracked_id)

        if not post:
            for m in found:
                try:
                    await m.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass
            if tracked_id:
                await asyncio.to_thread(set_info_message_id, channel_id, None)
            return

        alliance_name = await asyncio.to_thread(self._alliance_name, alliance_id)
        content = await asyncio.to_thread(render_info_message, alliance_id, alliance_name)

        keep = next((m for m in found if m.id == tracked_id), None)
        if keep is None and found:
            keep = max(found, key=lambda m: m.created_at)
        for m in found:
            if keep is None or m.id != keep.id:
                try:
                    await m.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                    pass

        if keep is None:
            try:
                keep = await channel.send(content)
            except (discord.Forbidden, discord.HTTPException):
                logger.warning(f"ID channel: cannot post info message in channel {channel_id}")
                return
            await asyncio.to_thread(set_info_message_id, channel_id, keep.id)
        else:
            if (keep.content or "") != content:
                try:
                    await keep.edit(content=content)
                except (discord.Forbidden, discord.HTTPException):
                    return
            if keep.id != tracked_id:
                await asyncio.to_thread(set_info_message_id, channel_id, keep.id)

        try:
            if pin and not keep.pinned:
                await keep.pin(reason="ID channel info message")
            elif not pin and keep.pinned:
                await keep.unpin(reason="ID channel info message pin turned off")
        except (discord.Forbidden, discord.HTTPException):
            pass

    async def remove_info_message(self, channel_id, tracked_id=None):
        """Delete the info message - call before dropping an id_channels row."""
        channel = self.bot.get_channel(channel_id)
        if channel is None:
            return
        for m in await self._find_info_messages(channel, tracked_id):
            try:
                await m.delete()
            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                pass

    async def refresh_alliance_info_message(self, guild_id, alliance_id):
        """Refresh one alliance's info message, if it has an ID channel here."""
        row = await asyncio.to_thread(channel_row_for_alliance, guild_id, alliance_id)
        if row:
            await self.refresh_info_message(row[0], alliance_id, row[1])

    async def reconcile_info_messages(self):
        """Bring every ID channel's info message in line with its alliance settings."""
        for channel_id, alliance_id, tracked_id in await asyncio.to_thread(all_channel_rows):
            try:
                await self.refresh_info_message(channel_id, alliance_id, tracked_id)
            except Exception as e:
                logger.error(f"ID channel info refresh failed for {channel_id}: {e}")

    def _alliance_name(self, alliance_id):
        with closing(sqlite3.connect('db/alliance.sqlite', timeout=30.0)) as conn, conn:
            row = conn.execute("SELECT name FROM alliance_list WHERE alliance_id = ?",
                               (alliance_id,)).fetchone()
        return row[0] if row else "this alliance"

    async def warn_invalid_format(self, message, alliance_id=None):
        # Default off: silently ignore non-numeric posts in ID channels unless
        # the admin has opted in. Avoids embarrassing X-emoji spam if a busy
        # general channel was accidentally configured as an ID channel.
        settings = self.get_guild_settings(message.guild.id)
        if not settings.get('respond_to_invalid', 0):
            return

        channel_id = message.channel.id
        now = time.time()
        last_warning = self.invalid_format_warnings.get(channel_id, 0)

        if now - last_warning < self.BACKOFF_DURATION:
            return

        self.invalid_format_warnings[channel_id] = now
        multistate = (alliance_id is not None
                      and await asyncio.to_thread(is_multistate, alliance_id))
        plain, detailed = id_format_examples(multistate)
        if multistate:
            hint = f"Please post your ID and state like {plain}, or {detailed} to include details."
        else:
            hint = f"Please post an ID like {plain}, or {detailed} to include details."
        await self._safe_react_reply(
            message, theme.deniedIcon, hint,
            delete_after=settings['delete_after'],
        )

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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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

                    parsed = parse_id_post(message.content)
                    if parsed is None:
                        continue

                    fid, given_name, given_level, given_state = parsed
                    await self.process_fid(message, fid, alliance_id, given_state,
                                           given_name=given_name, given_level=given_level)

            await self.reconcile_info_messages()

            if invalid_channels:
                with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute("SELECT alliance_id FROM id_channels WHERE channel_id = ?", (message.channel.id,))
                channel_info = cursor.fetchone()

            if not channel_info:
                return

            alliance_id = channel_info[0]
            parsed = parse_id_post(message.content)
            if parsed is None:
                await self.warn_invalid_format(message, alliance_id)
                return

            fid, given_name, given_level, given_state = parsed
            await self.process_fid(message, fid, alliance_id, given_state,
                                   given_name=given_name, given_level=given_level)

        except Exception as e:
            logger.error(f"Error in on_message: {e}")
            print(f"Error in on_message: {e}")

    async def process_fid(self, message, fid, alliance_id, given_state=None,
                          given_name=None, given_level=None):
        settings = self.get_guild_settings(message.guild.id)
        delete_after = settings['delete_after']

        try:
            with closing(sqlite3.connect('db/users.sqlite')) as users_db, users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                existing_alliance = cursor.fetchone()

                if existing_alliance:
                    existing_alliance_id = int(existing_alliance[0]) if existing_alliance[0] else None
                    if existing_alliance_id == alliance_id:
                        await message.add_reaction(theme.warnIcon)
                        await message.reply(f"This ID ({fid}) is already registered in this alliance!", delete_after=delete_after)
                        return

                    with closing(sqlite3.connect('db/alliance.sqlite')) as alliance_db, alliance_db:
                        alliance_cursor = alliance_db.cursor()
                        alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (existing_alliance[0],))
                        alliance_name = alliance_cursor.fetchone()

                    if alliance_name:
                        await message.add_reaction(theme.warnIcon)
                        await message.reply(
                            f"This ID ({fid}) is already registered in another alliance: `{alliance_name[0]}`",
                            delete_after=delete_after
                        )
                        return

                    # Orphan: alliance was deleted from alliance_list. Drop stale row and let registration proceed.
                    cursor.execute(
                        "DELETE FROM users WHERE fid = ? AND alliance = ?",
                        (fid, existing_alliance[0]),
                    )
                    users_db.commit()
                    logger.info(
                        f"Self-healed orphan ID {fid} from deleted alliance "
                        f"{existing_alliance[0]}; allowing registration into alliance {alliance_id}"
                    )

            # Multistate alliance with no state given: once added they can't repost to fix
            # it, so ask up front - post `<id> <state>` together in one message.
            if given_state is None and is_multistate(alliance_id):
                await message.add_reaction(theme.warnIcon)
                await message.reply(
                    f"{theme.infoIcon} This alliance has members in multiple states. "
                    f"Post your ID **and** state together, e.g. `{fid} 245`.",
                    delete_after=delete_after)
                return

            # An explicit state (the poster knows theirs) is honored; otherwise confirm
            # against the alliance's home state with one probe.
            if given_state is not None:
                kid, verified = given_state, False   # self-reported, not probed
            else:
                gift_cog = self.bot.get_cog("GiftOperations")
                if gift_cog is not None:
                    kid, verified = await verify_add_state(gift_cog, fid, alliance_id)
                else:
                    kid, verified = None, False

            state_error = check_alliance_state(alliance_id, kid)
            if state_error:
                await message.add_reaction(theme.deniedIcon)
                await message.reply(f"{theme.deniedIcon} {state_error}", delete_after=delete_after)
                return

            nickname = new_member_name(given_name, fid)
            furnace_lv = given_level or 0
            try:
                with closing(sqlite3.connect('db/users.sqlite')) as users_db, users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT alliance FROM users WHERE fid = ?", (fid,))
                    if cursor.fetchone():
                        await message.add_reaction(theme.warnIcon)
                        await message.reply(f"This ID ({fid}) was added by another process!", delete_after=delete_after)
                        return

                    cursor.execute("""
                        INSERT INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance)
                        VALUES (?, ?, ?, ?, NULL, ?)
                    """, (fid, nickname, furnace_lv, kid, alliance_id))
                    users_db.commit()
            except sqlite3.IntegrityError:
                await message.add_reaction(theme.warnIcon)
                await message.reply(f"This ID ({fid}) was added by another process!", delete_after=delete_after)
                return

            await message.add_reaction(theme.verifiedIcon)
            success_embed = discord.Embed(
                title=f"{theme.verifiedIcon} Member Successfully Added",
                description=(
                    f"{theme.upperDivider}\n"
                    f"**{theme.fidIcon} ID:** `{fid}`\n"
                    f"**{theme.userIcon} Name:** `{nickname}`\n"
                    + (f"**{theme.levelIcon} Level:** `{format_furnace_level(furnace_lv)}`\n" if furnace_lv else "")
                    + f"**{theme.globeIcon} State:** `{kid if kid is not None else 'pending resolution'}`\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor3
            )
            await message.reply(embed=success_embed)

            await self.log_action(
                "ADD_MEMBER",
                message.author.id,
                message.guild.id,
                {"fid": fid, "nickname": nickname, "alliance_id": alliance_id}
            )
            return

        except Exception as e:
            logger.error(f"Error in process_fid: {e}")
            print(f"Error in process_fid: {e}")
            await self._safe_react_reply(
                message, theme.deniedIcon, "An error occurred during the process!",
                delete_after=delete_after,
            )

    @tasks.loop(seconds=300)
    async def check_channels_loop(self):
        try:
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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

                    parsed = parse_id_post(message.content)
                    if parsed is None:
                        await self.warn_invalid_format(message, alliance_id)
                        continue

                    fid, given_name, given_level, given_state = parsed
                    await self.process_fid(message, fid, alliance_id, given_state,
                                           given_name=given_name, given_level=given_level)

        except Exception as e:
            if '503' in str(e) or '502' in str(e) or 'connect error' in str(e).lower():
                logger.info(f"ID Channel check skipped — Discord temporarily unavailable: {e}")
            else:
                logger.error(f"Error in check_channels_loop: {e}")
                print(f"Error in check_channels_loop: {e}")

    @check_channels_loop.before_loop
    async def before_check_channels_loop(self):
        await self.bot.wait_until_ready()

    async def show_id_channel_for(self, interaction: discord.Interaction, alliance_id: int):
        """Hub-context entry: show + manage the ID channel for one alliance."""
        try:
            with closing(sqlite3.connect('db/alliance.sqlite')) as alliance_db, alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute(
                    "SELECT name FROM alliance_list WHERE alliance_id = ?",
                    (alliance_id,),
                )
                row = cursor.fetchone()
            if not row:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Alliance not found.", ephemeral=True
                )
                return
            alliance_name = row[0]

            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute(
                    "SELECT channel_id FROM id_channels "
                    "WHERE guild_id = ? AND alliance_id = ?",
                    (interaction.guild_id, alliance_id),
                )
                ch_row = cursor.fetchone()
            current_channel_id = ch_row[0] if ch_row else None
            current_channel = (
                interaction.guild.get_channel(current_channel_id)
                if current_channel_id else None
            )

            if current_channel:
                state_line = f"{theme.verifiedIcon} Current channel: {current_channel.mention}"
            elif current_channel_id:
                state_line = f"{theme.warnIcon} Configured channel `{current_channel_id}` is no longer accessible."
            else:
                state_line = f"{theme.deniedIcon} No ID channel configured for this alliance."

            post_info, pin_info = await asyncio.to_thread(info_settings, alliance_id)
            multistate = await asyncio.to_thread(is_multistate, alliance_id)
            plain, detailed = id_format_examples(multistate)
            if multistate:
                format_lines = (
                    f"This alliance spans several states, so members post their ID **and** "
                    f"state together: {plain}.\n"
                    f"To fill in their details too: {detailed}.\n"
                )
            else:
                format_lines = (
                    f"Members post just their ID: {plain}.\n"
                    f"To fill in their details too: {detailed}.\n"
                )

            if not post_info:
                info_line = f"{theme.deniedIcon} Info message: off"
            elif pin_info:
                info_line = f"{theme.verifiedIcon} Info message: posted and pinned"
            else:
                info_line = f"{theme.verifiedIcon} Info message: posted (not pinned)"

            embed = discord.Embed(
                title=f"{theme.fidIcon} {alliance_name} — ID Channel",
                description=(
                    f"Players can post their in-game ID in this channel and the "
                    f"bot will verify and add them to the alliance.\n"
                    f"{format_lines}"
                    f"{theme.upperDivider}\n"
                    f"{state_line}\n"
                    f"{info_line}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1,
            )
            view = AllianceIDChannelView(self, alliance_id, alliance_name,
                                         bool(current_channel_id), post_info, pin_info)
            await safe_edit_message(interaction, embed=embed, view=view, content=None)

        except Exception as e:
            logger.error(f"Error in show_id_channel_for: {e}")
            print(f"Error in show_id_channel_for: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while loading the ID channel.",
                    ephemeral=True,
                )

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
                    f"Set up channels where players post their in-game ID to "
                    f"join an alliance.\n\n"
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.addIcon} **Create Channel**\n"
                    f"└ Pick a channel and link it to an alliance\n\n"
                    f"{theme.listIcon} **View Channels**\n"
                    f"└ List the ID channels set up on this server\n\n"
                    f"{theme.trashIcon} **Delete Channel**\n"
                    f"└ Stop using a channel for ID registration\n\n"
                    f"{theme.settingsIcon} **Settings**\n"
                    f"└ Adjust scanning and auto-delete behaviour\n"
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
    def __init__(self, cog, current_value, refresh=None):
        super().__init__(title="Set Scan Message Limit")
        self.cog = cog
        self.refresh = refresh  # async callable(interaction) — overrides default refresh
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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute("UPDATE id_channel_settings SET scan_limit = ? WHERE guild_id = ?",
                              (value, interaction.guild_id))
                db.commit()

            if self.refresh is not None:
                await self.refresh(interaction)
            else:
                await IDChannelSettingsView(self.cog).show(interaction)
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )


class DeleteAfterModal(discord.ui.Modal):
    def __init__(self, cog, current_value, refresh=None):
        super().__init__(title="Set Auto-Delete Timer")
        self.cog = cog
        self.refresh = refresh  # async callable(interaction) — overrides default refresh
        self.timer_input = discord.ui.TextInput(
            label="Auto-delete delay in seconds (0-300)",
            placeholder="e.g. 10; set 0 to keep replies forever",
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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute("UPDATE id_channel_settings SET delete_after = ? WHERE guild_id = ?",
                              (db_value, interaction.guild_id))
                db.commit()

            if self.refresh is not None:
                await self.refresh(interaction)
            else:
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
                f"{theme.editListIcon} **Auto-Delete:** `{delete_text}`\n"
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
        with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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


class AllianceIDChannelView(discord.ui.View):
    """Per-alliance ID Channel management — alliance is already known."""

    def __init__(self, cog, alliance_id: int, alliance_name: str, has_channel: bool,
                 post_info: bool = False, pin_info: bool = True):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliance_id = alliance_id
        self.alliance_name = alliance_name
        self.has_channel = has_channel
        self.post_info = post_info
        self.pin_info = pin_info

        set_btn = discord.ui.Button(
            label="Change Channel" if has_channel else "Set Channel",
            emoji=f"{theme.editListIcon}",
            style=discord.ButtonStyle.primary,
            row=0,
        )
        set_btn.callback = self._on_set
        self.add_item(set_btn)

        if has_channel:
            remove_btn = discord.ui.Button(
                label="Remove Channel",
                emoji=f"{theme.trashIcon}",
                style=discord.ButtonStyle.danger,
                row=0,
            )
            remove_btn.callback = self._on_remove
            self.add_item(remove_btn)

        if has_channel:
            info_btn = discord.ui.Button(
                label=f"Info Message: {'On' if post_info else 'Off'}",
                emoji=f"{theme.announceIcon}",
                style=(discord.ButtonStyle.success if post_info
                       else discord.ButtonStyle.secondary),
                row=1,
            )
            info_btn.callback = self._on_toggle_info
            self.add_item(info_btn)

            pin_btn = discord.ui.Button(
                label=f"Pin Message: {'On' if pin_info else 'Off'}",
                emoji=f"{theme.pinIcon}",
                style=(discord.ButtonStyle.success if pin_info
                       else discord.ButtonStyle.secondary),
                row=1,
                disabled=not post_info,
            )
            pin_btn.callback = self._on_toggle_pin
            self.add_item(pin_btn)

        back_btn = discord.ui.Button(
            label="Back to Hub",
            emoji=f"{theme.backIcon}",
            style=discord.ButtonStyle.secondary,
            row=2,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    async def _on_toggle_info(self, interaction: discord.Interaction):
        await self._apply_info_setting(interaction, post=not self.post_info)

    async def _on_toggle_pin(self, interaction: discord.Interaction):
        await self._apply_info_setting(interaction, pin=not self.pin_info)

    async def _apply_info_setting(self, interaction: discord.Interaction, **change):
        await interaction.response.defer()
        await asyncio.to_thread(set_info_settings, self.alliance_id, **change)
        await self.cog.refresh_alliance_info_message(interaction.guild_id, self.alliance_id)
        await self.cog.show_id_channel_for(interaction, self.alliance_id)

    async def _on_set(self, interaction: discord.Interaction):
        cog = self.cog
        alliance_id = self.alliance_id
        alliance_name = self.alliance_name

        class _ChannelSelect(discord.ui.ChannelSelect):
            def __init__(self):
                super().__init__(
                    placeholder="Pick the channel to use as the ID channel",
                    channel_types=[discord.ChannelType.text],
                )

            async def callback(self, channel_interaction: discord.Interaction):
                selected_channel = self.values[0]
                try:
                    with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                        cursor = db.cursor()
                        cursor.execute(
                            "DELETE FROM id_channels WHERE guild_id = ? AND alliance_id = ?",
                            (channel_interaction.guild_id, alliance_id),
                        )
                        cursor.execute(
                            "INSERT INTO id_channels "
                            "(guild_id, alliance_id, channel_id, created_at, created_by) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (
                                channel_interaction.guild_id,
                                alliance_id,
                                selected_channel.id,
                                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                channel_interaction.user.id,
                            ),
                        )
                        db.commit()
                    await cog.log_action(
                        "CREATE_CHANNEL",
                        channel_interaction.user.id,
                        channel_interaction.guild_id,
                        {
                            "alliance_id": alliance_id,
                            "channel_id": selected_channel.id,
                            "channel_name": selected_channel.name,
                        },
                    )
                except Exception as e:
                    logger.error(f"ID channel set error: {e}")
                    print(f"ID channel set error: {e}")
                    await channel_interaction.response.send_message(
                        f"{theme.deniedIcon} Failed to set the ID channel.",
                        ephemeral=True,
                    )
                    return
                await cog.refresh_alliance_info_message(channel_interaction.guild_id, alliance_id)
                await cog.show_id_channel_for(channel_interaction, alliance_id)

        select_view = discord.ui.View(timeout=300)
        select_view.add_item(_ChannelSelect())
        select_embed = discord.Embed(
            title=f"{theme.fidIcon} {alliance_name} — Pick ID Channel",
            description="Pick the text channel members will post their IDs in.",
            color=theme.emColor1,
        )
        await interaction.response.edit_message(embed=select_embed, view=select_view)

    async def _on_remove(self, interaction: discord.Interaction):
        try:
            # Drop the info message first - once the row is gone we can't find it.
            row = await asyncio.to_thread(
                channel_row_for_alliance, interaction.guild_id, self.alliance_id)
            if row:
                await self.cog.remove_info_message(row[0], row[1])
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute(
                    "DELETE FROM id_channels WHERE guild_id = ? AND alliance_id = ?",
                    (interaction.guild_id, self.alliance_id),
                )
                db.commit()
            await self.cog.log_action(
                "DELETE_CHANNEL",
                interaction.user.id,
                interaction.guild_id,
                {"alliance_id": self.alliance_id},
            )
        except Exception as e:
            logger.error(f"ID channel remove error: {e}")
            print(f"ID channel remove error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Failed to remove the ID channel.",
                ephemeral=True,
            )
            return
        await self.cog.show_id_channel_for(interaction, self.alliance_id)

    async def _on_back(self, interaction: discord.Interaction):
        main_menu = self.cog.bot.get_cog("MainMenu")
        if main_menu and hasattr(main_menu, "show_alliance_hub"):
            await main_menu.show_alliance_hub(interaction, self.alliance_id)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Hub not available.", ephemeral=True
            )


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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute("""
                    SELECT channel_id, alliance_id, created_at, created_by
                    FROM id_channels 
                    WHERE guild_id = ?
                """, (interaction.guild_id,))
                id_channels = cursor.fetchall()

            with closing(sqlite3.connect('db/alliance.sqlite')) as alliance_db, alliance_db:
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
            with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
                cursor = db.cursor()
                cursor.execute("SELECT channel_id, alliance_id FROM id_channels WHERE guild_id = ?", (interaction.guild_id,))
                id_channels = cursor.fetchall()

            with closing(sqlite3.connect('db/alliance.sqlite')) as alliance_db, alliance_db:
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

                        with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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
            with closing(sqlite3.connect('db/alliance.sqlite')) as alliance_db, alliance_db:
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
                                with closing(sqlite3.connect('db/id_channel.sqlite')) as db, db:
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