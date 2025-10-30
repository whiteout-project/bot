import discord
from discord.ext import commands
import sqlite3
from datetime import datetime, timedelta
import pytz
import os
import math
import traceback
import logging
import logging.handlers
import asyncio

class BearTrapSchedule(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Logger Setup for bear_trap.txt (shared with other bear trap cogs)
        self.logger = logging.getLogger('bear_trap')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False  # Prevent propagation to root logger
        log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        log_dir = 'log'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_path = os.path.join(log_dir, 'bear_trap.txt')

        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path, maxBytes=3 * 1024 * 1024, backupCount=1, encoding='utf-8'
        )
        file_handler.setFormatter(log_formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(file_handler)

        self.logger.info("[SCHEDULE] Cog initializing...")

        # Database connection with timeout to prevent locking
        self.db_path = 'db/beartime.sqlite'
        os.makedirs('db', exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Enable WAL mode for better concurrency
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.commit()

        # Create schedule boards table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS notification_schedule_boards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                board_type TEXT NOT NULL,
                target_channel_id INTEGER,
                max_events INTEGER DEFAULT 15,
                show_disabled INTEGER DEFAULT 0,
                auto_pin INTEGER DEFAULT 1,
                timezone TEXT DEFAULT 'UTC',
                filter_name TEXT,
                filter_time_range INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER NOT NULL,
                last_updated TIMESTAMP,
                UNIQUE(guild_id, channel_id, board_type, target_channel_id)
            )
        """)

        self.conn.commit()
        self.logger.info("[SCHEDULE] Cog initialized successfully")

    async def cog_load(self):
        """Start background tasks when cog loads"""
        self.logger.info("[SCHEDULE] Starting daily refresh task...")
        self.refresh_task = asyncio.create_task(self.daily_refresh_loop())

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        self.logger.info("[SCHEDULE] Cog unloading...")

        # Cancel refresh task
        if hasattr(self, 'refresh_task'):
            self.refresh_task.cancel()

        if hasattr(self, 'conn'):
            self.conn.close()
        self.logger.info("[SCHEDULE] Cog unloaded")

    async def daily_refresh_loop(self):
        """Background task that refreshes all boards daily at midnight in their timezone"""
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                # Get all unique timezones from boards
                self.cursor.execute("""
                    SELECT DISTINCT timezone FROM notification_schedule_boards
                """)
                timezones = [row[0] for row in self.cursor.fetchall()]

                # For each timezone, check if it's midnight (00:01)
                now_utc = datetime.now(pytz.UTC)

                for tz_str in timezones:
                    try:
                        tz = pytz.timezone(tz_str)
                        now_in_tz = now_utc.astimezone(tz)

                        # Check if it's 00:01 in this timezone (1-minute window)
                        if now_in_tz.hour == 0 and now_in_tz.minute == 1:
                            self.logger.info(f"[SCHEDULE] Daily refresh triggered for timezone: {tz_str}")

                            # Get all boards in this timezone
                            self.cursor.execute("""
                                SELECT id FROM notification_schedule_boards
                                WHERE timezone = ?
                            """, (tz_str,))
                            board_ids = [row[0] for row in self.cursor.fetchall()]

                            # Refresh each board
                            for board_id in board_ids:
                                await self.update_schedule_board(board_id)

                            self.logger.info(f"[SCHEDULE] Refreshed {len(board_ids)} board(s) for timezone {tz_str}")

                    except Exception as e:
                        self.logger.error(f"[SCHEDULE] Error refreshing timezone {tz_str}: {e}")
                        continue

                # Sleep for 60 seconds before next check
                await asyncio.sleep(60)

            except Exception as e:
                self.logger.error(f"[SCHEDULE] Error in daily refresh loop: {e}")
                await asyncio.sleep(60)  # Continue even if error occurs

    async def create_schedule_board(self, guild_id: int, channel_id: int, board_type: str,
                                    target_channel_id: int, creator_id: int, settings: dict) -> tuple:
        """
        Creates a new schedule board and posts it to Discord.
        Returns (board_id, error_message) - board_id is None if error
        """
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return (None, "Channel not found!")

            # Check bot permissions
            if not channel.permissions_for(channel.guild.me).send_messages:
                return (None, "Bot doesn't have permission to send messages in that channel!")

            # Generate initial embed
            embed = await self.generate_schedule_embed_for_new_board(
                guild_id, board_type, target_channel_id, settings
            )

            # Post message to Discord without view initially
            message = await channel.send(embed=embed)

            # Auto-pin if enabled
            if settings.get('auto_pin', True):
                try:
                    await message.pin()
                except discord.Forbidden:
                    pass  # Bot lacks pin permissions, continue anyway

            # Save to database
            self.cursor.execute("""
                INSERT INTO notification_schedule_boards
                (guild_id, channel_id, message_id, board_type, target_channel_id,
                 max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range, created_by, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                guild_id, channel_id, message.id, board_type, target_channel_id,
                settings.get('max_events', 15),
                1 if settings.get('show_disabled', False) else 0,
                1 if settings.get('auto_pin', True) else 0,
                settings.get('timezone', 'UTC'),
                settings.get('filter_name'),
                settings.get('filter_time_range'),
                creator_id,
                datetime.now(pytz.UTC).isoformat()
            ))

            self.conn.commit()
            board_id = self.cursor.lastrowid

            self.logger.info(f"[SCHEDULE] Board created - ID: {board_id}, Type: {board_type}, Guild: {guild_id}, "
                           f"Channel: {channel_id}, Creator: {creator_id}, Target: {target_channel_id}")

            return (board_id, None)

        except Exception as e:
            self.logger.error(f"[SCHEDULE] Failed to create board - Guild: {guild_id}, Error: {e}")
            print(f"[ERROR] Failed to create schedule board: {e}")
            traceback.print_exc()
            return (None, f"An error occurred: {str(e)}")

    async def delete_schedule_board(self, board_id: int) -> tuple:
        """
        Deletes a schedule board.
        Returns (success, error_message)
        """
        try:
            # Fetch board info
            self.cursor.execute("""
                SELECT guild_id, channel_id, message_id, auto_pin FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                return (False, "Board not found!")

            guild_id, channel_id, message_id, auto_pin = result

            # Try to delete Discord message
            try:
                channel = self.bot.get_channel(channel_id)
                if channel:
                    message = await channel.fetch_message(message_id)
                    if message:
                        # Unpin if it was auto-pinned
                        if auto_pin and message.pinned:
                            try:
                                await message.unpin()
                            except:
                                pass
                        await message.delete()
            except discord.NotFound:
                pass  # Message already deleted
            except Exception as e:
                print(f"[ERROR] Failed to delete Discord message: {e}")

            # Remove from database
            self.cursor.execute("DELETE FROM notification_schedule_boards WHERE id = ?", (board_id,))
            self.conn.commit()

            self.logger.info(f"[SCHEDULE] Board deleted - ID: {board_id}, Guild: {guild_id}, Channel: {channel_id}")

            return (True, None)

        except Exception as e:
            self.logger.error(f"[SCHEDULE] Failed to delete board - ID: {board_id}, Error: {e}")
            print(f"[ERROR] Failed to delete schedule board: {e}")
            traceback.print_exc()
            return (False, f"An error occurred: {str(e)}")

    async def move_schedule_board(self, board_id: int, new_channel_id: int) -> tuple:
        """
        Moves a schedule board to a different channel.
        Returns (success, error_message)
        """
        try:
            # Fetch board info
            self.cursor.execute("""
                SELECT guild_id, channel_id, message_id, board_type, target_channel_id,
                       max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range
                FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                return (False, "Board not found!")

            (guild_id, old_channel_id, old_message_id, board_type, target_channel_id,
             max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range) = result

            # Get new channel
            new_channel = self.bot.get_channel(new_channel_id)
            if not new_channel:
                return (False, "New channel not found!")

            # Check permissions
            if not new_channel.permissions_for(new_channel.guild.me).send_messages:
                return (False, "Bot doesn't have permission to send messages in the new channel!")

            # Generate embed
            settings = {
                'max_events': max_events,
                'show_disabled': bool(show_disabled),
                'auto_pin': bool(auto_pin),
                'timezone': timezone,
                'filter_name': filter_name,
                'filter_time_range': filter_time_range
            }

            embed = await self.generate_schedule_embed_for_new_board(
                guild_id, board_type, target_channel_id, settings
            )

            # Post to new channel
            new_message = await new_channel.send(embed=embed)

            # Auto-pin if enabled
            if auto_pin:
                try:
                    await new_message.pin()
                except:
                    pass

            # Delete old message
            try:
                old_channel = self.bot.get_channel(old_channel_id)
                if old_channel:
                    old_message = await old_channel.fetch_message(old_message_id)
                    if old_message:
                        if auto_pin and old_message.pinned:
                            try:
                                await old_message.unpin()
                            except:
                                pass
                        await old_message.delete()
            except:
                pass  # Old message already deleted

            # Update database
            self.cursor.execute("""
                UPDATE notification_schedule_boards
                SET channel_id = ?, message_id = ?, last_updated = ?
                WHERE id = ?
            """, (new_channel_id, new_message.id, datetime.now(pytz.UTC).isoformat(), board_id))

            self.conn.commit()

            self.logger.info(f"[SCHEDULE] Board moved - ID: {board_id}, From: {old_channel_id}, To: {new_channel_id}")

            return (True, None)

        except Exception as e:
            self.logger.error(f"[SCHEDULE] Failed to move board - ID: {board_id}, Error: {e}")
            print(f"[ERROR] Failed to move schedule board: {e}")
            traceback.print_exc()
            return (False, f"An error occurred: {str(e)}")

    async def generate_schedule_embed_for_new_board(self, guild_id: int, board_type: str,
                                                    target_channel_id: int, settings: dict) -> discord.Embed:
        """Helper to generate embed for a board that doesn't exist in DB yet"""
        return await self._generate_schedule_embed_internal(
            guild_id, board_type, target_channel_id, settings, page=0
        )

    async def generate_schedule_embed(self, board_id: int, page: int = 0) -> discord.Embed:
        """
        Generates the schedule embed for an existing board.
        """
        try:
            # Fetch board settings
            self.cursor.execute("""
                SELECT guild_id, board_type, target_channel_id, max_events,
                       show_disabled, timezone, filter_name, filter_time_range
                FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                return self._create_error_embed("Board not found!")

            (guild_id, board_type, target_channel_id, max_events,
             show_disabled, timezone, filter_name, filter_time_range) = result

            settings = {
                'max_events': max_events,
                'show_disabled': bool(show_disabled),
                'timezone': timezone,
                'filter_name': filter_name,
                'filter_time_range': filter_time_range
            }

            return await self._generate_schedule_embed_internal(
                guild_id, board_type, target_channel_id, settings, page
            )

        except Exception as e:
            print(f"[ERROR] Failed to generate schedule embed: {e}")
            traceback.print_exc()
            return self._create_error_embed(f"Error generating schedule: {str(e)}")

    async def _generate_schedule_embed_internal(self, guild_id: int, board_type: str,
                                                target_channel_id: int, settings: dict, page: int) -> discord.Embed:
        """Internal method to generate schedule embed"""
        try:
            # Query notifications based on board type
            query = """
                SELECT id, channel_id, hour, minute, timezone, description,
                       notification_type, next_notification, is_enabled
                FROM bear_notifications
                WHERE guild_id = ?
            """
            params = [guild_id]

            # Filter by channel if per-channel board
            if board_type == 'channel' and target_channel_id:
                query += " AND channel_id = ?"
                params.append(target_channel_id)

            # Filter by enabled status
            if not settings.get('show_disabled', False):
                query += " AND is_enabled = 1"

            # Filter by name if specified
            if settings.get('filter_name'):
                names = [n.strip() for n in settings['filter_name'].split(',')]
                name_conditions = " OR ".join(["description LIKE ?" for _ in names])
                query += f" AND ({name_conditions})"
                params.extend([f"%{name}%" for name in names])

            # Filter by time range if specified
            if settings.get('filter_time_range'):
                hours = settings['filter_time_range']
                query += " AND datetime(next_notification) <= datetime('now', '+' || ? || ' hours')"
                params.append(hours)

            # Exclude past events
            query += " AND next_notification IS NOT NULL AND datetime(next_notification) > datetime('now') ORDER BY next_notification ASC"

            self.cursor.execute(query, params)
            notifications = self.cursor.fetchall()

            # No notifications found
            if not notifications:
                return self._create_empty_schedule_embed(board_type, target_channel_id, settings)

            # Pagination
            max_events = settings.get('max_events', 15)
            total_notifications = len(notifications)
            total_pages = math.ceil(total_notifications / max_events) if total_notifications > 0 else 1
            page = max(0, min(page, total_pages - 1))  # Clamp page

            start_idx = page * max_events
            end_idx = min(start_idx + max_events, total_notifications)
            page_notifications = notifications[start_idx:end_idx]

            # Format notifications by urgency
            now = datetime.now(pytz.UTC)
            tz = pytz.timezone(settings.get('timezone', 'UTC'))

            sections = {
                'imminent': [],  # < 1 hour
                'soon': [],      # < 6 hours
                'upcoming': [],  # < 24 hours
                'scheduled': []  # > 24 hours
            }

            for notif in page_notifications:
                (notif_id, channel_id, hour, minute, notif_timezone, description,
                 notification_type, next_notification, is_enabled) = notif

                next_time = datetime.fromisoformat(next_notification)
                time_until = next_time - now
                hours_until = time_until.total_seconds() / 3600

                # Skip past events (defensive - shouldn't happen due to query filter)
                if hours_until < 0:
                    continue

                line = await self._format_event_line(notif, tz, board_type == 'server')

                if hours_until < 1:
                    sections['imminent'].append(line)
                elif hours_until < 6:
                    sections['soon'].append(line)
                elif hours_until < 24:
                    sections['upcoming'].append(line)
                else:
                    sections['scheduled'].append(line)

            # Build embed
            description = "📅 **Upcoming Event Schedule**\n\n"

            if sections['imminent']:
                description += "🔴 **IMMINENT** (< 1 hour)\n"
                description += "\n".join(sections['imminent']) + "\n\n"

            if sections['soon']:
                description += "🟡 **SOON** (< 6 hours)\n"
                description += "\n".join(sections['soon']) + "\n\n"

            if sections['upcoming']:
                description += "🟢 **UPCOMING** (< 24 hours)\n"
                description += "\n".join(sections['upcoming']) + "\n\n"

            if sections['scheduled']:
                description += "📋 **SCHEDULED** (> 24 hours)\n"
                description += "\n".join(sections['scheduled']) + "\n\n"

            description += "━━━━━━━━━━━━━━━━━━━━━━"

            # Determine embed color based on nearest event
            if sections['imminent']:
                color = 0xFF0000  # Red
            elif sections['soon']:
                color = 0xFF8C00  # Orange
            elif sections['upcoming']:
                color = 0x00FF00  # Green
            else:
                color = 0x0080FF  # Blue

            embed = discord.Embed(
                description=description,
                color=color
            )

            # Footer with pagination
            footer_text = f"Last updated: {now.astimezone(tz).strftime('%b %d, %I:%M %p')} {settings.get('timezone', 'UTC')}"
            if total_pages > 1:
                footer_text += f" | Page {page + 1} of {total_pages}"
            embed.set_footer(text=footer_text)

            return embed

        except Exception as e:
            print(f"[ERROR] Failed to generate schedule embed internally: {e}")
            traceback.print_exc()
            return self._create_error_embed(f"Error: {str(e)}")

    async def _format_event_line(self, notification, timezone, show_channel: bool) -> str:
        """Formats a single notification as a line in the schedule"""
        try:
            (notif_id, channel_id, hour, minute, notif_timezone, description,
             notification_type, next_notification, is_enabled) = notification

            # Parse next notification time
            next_time = datetime.fromisoformat(next_notification)
            next_time_tz = next_time.astimezone(timezone)

            # Format time
            now = datetime.now(pytz.UTC)

            # Get current date in the board's timezone
            now_in_tz = now.astimezone(timezone)
            next_date = next_time_tz.date()
            today_date = now_in_tz.date()

            # Determine date format
            if next_date == today_date:
                time_str = f"Today {next_time_tz.strftime('%H:%M')}"
            else:
                time_str = next_time_tz.strftime("%d-%m-%y %H:%M")

            # Extract notification name
            if "EMBED_MESSAGE:" in description:
                # Get embed title
                self.cursor.execute("""
                    SELECT title FROM bear_notification_embeds
                    WHERE notification_id = ?
                """, (notif_id,))
                embed_result = self.cursor.fetchone()
                name = embed_result[0] if embed_result and embed_result[0] else "Event"
            elif "PLAIN_MESSAGE:" in description:
                # Extract from plain message
                name = description.split("PLAIN_MESSAGE:")[-1].split("|")[0].strip()
                if len(name) > 30:
                    name = name[:27] + "..."
            else:
                name = description[:30] if len(description) > 30 else description

            # Build line
            line = f"• {time_str} {timezone.zone} | {name}"

            if show_channel:
                line += f" in <#{channel_id}>"

            if not is_enabled:
                line += " ⚠️ [DISABLED]"

            return line

        except Exception as e:
            print(f"[ERROR] Failed to format event line: {e}")
            return "• Error formatting event"

    def _create_empty_schedule_embed(self, board_type: str, target_channel_id: int, settings: dict) -> discord.Embed:
        """Creates an embed for when no events are scheduled"""
        description = "📅 **Upcoming Event Schedule**\n\n"

        if settings.get('filter_time_range'):
            description += f"No events in the next {settings['filter_time_range']} hours.\n\n"
        else:
            description += "No upcoming events scheduled.\n\n"

        description += "━━━━━━━━━━━━━━━━━━━━━━"

        tz = pytz.timezone(settings.get('timezone', 'UTC'))
        now = datetime.now(pytz.UTC).astimezone(tz)

        embed = discord.Embed(
            description=description,
            color=0x808080  # Gray
        )
        embed.set_footer(text=f"Last updated: {now.strftime('%b %d, %I:%M %p')} {settings.get('timezone', 'UTC')}")

        return embed

    def _create_error_embed(self, error_message: str) -> discord.Embed:
        """Creates an error embed"""
        return discord.Embed(
            title="❌ Error",
            description=error_message,
            color=0xFF0000
        )

    async def update_schedule_board(self, board_id: int) -> bool:
        """
        Updates a schedule board by regenerating and editing the Discord message.
        Returns True if successful, False otherwise.
        """
        try:
            # Fetch board info
            self.cursor.execute("""
                SELECT channel_id, message_id FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                print(f"[WARNING] Board {board_id} not found in database")
                return False

            channel_id, message_id = result

            # Get channel and message
            channel = self.bot.get_channel(channel_id)
            if not channel:
                print(f"[WARNING] Channel {channel_id} not found, removing board {board_id}")
                self.cursor.execute("DELETE FROM notification_schedule_boards WHERE id = ?", (board_id,))
                self.conn.commit()
                return False

            try:
                message = await channel.fetch_message(message_id)
            except discord.NotFound:
                print(f"[WARNING] Message {message_id} not found, removing board {board_id}")
                self.cursor.execute("DELETE FROM notification_schedule_boards WHERE id = ?", (board_id,))
                self.conn.commit()
                return False
            except Exception as e:
                print(f"[ERROR] Failed to fetch message: {e}")
                return False

            # Generate new embed
            embed = await self.generate_schedule_embed(board_id, page=0)

            # Edit message
            await message.edit(embed=embed)

            # Update last_updated timestamp
            self.cursor.execute("""
                UPDATE notification_schedule_boards
                SET last_updated = ?
                WHERE id = ?
            """, (datetime.now(pytz.UTC).isoformat(), board_id))
            self.conn.commit()

            self.logger.debug(f"[SCHEDULE] Board updated - ID: {board_id}")

            return True

        except Exception as e:
            self.logger.error(f"[SCHEDULE] Failed to update board - ID: {board_id}, Error: {e}")
            print(f"[ERROR] Failed to update schedule board {board_id}: {e}")
            traceback.print_exc()
            return False

    async def update_all_boards_for_guild(self, guild_id: int):
        """Updates all boards for a given server"""
        try:
            self.cursor.execute("""
                SELECT id FROM notification_schedule_boards
                WHERE guild_id = ?
            """, (guild_id,))
            boards = self.cursor.fetchall()

            for (board_id,) in boards:
                await self.update_schedule_board(board_id)

        except Exception as e:
            print(f"[ERROR] Failed to update all boards for guild {guild_id}: {e}")

    async def update_boards_for_notification_channel(self, guild_id: int, notification_channel_id: int):
        """Updates boards that show notifications for a specific channel"""
        try:
            # Update channel-specific boards
            self.cursor.execute("""
                SELECT id FROM notification_schedule_boards
                WHERE guild_id = ? AND board_type = 'channel' AND target_channel_id = ?
            """, (guild_id, notification_channel_id))
            channel_boards = self.cursor.fetchall()

            for (board_id,) in channel_boards:
                await self.update_schedule_board(board_id)

            # Also update server-wide boards
            self.cursor.execute("""
                SELECT id FROM notification_schedule_boards
                WHERE guild_id = ? AND board_type = 'server'
            """, (guild_id,))
            server_boards = self.cursor.fetchall()

            for (board_id,) in server_boards:
                await self.update_schedule_board(board_id)

        except Exception as e:
            print(f"[ERROR] Failed to update boards for channel {notification_channel_id}: {e}")

    async def on_notification_sent(self, guild_id: int, channel_id: int):
        """Called when a notification is sent"""
        self.logger.debug(f"[SCHEDULE] Notification sent event - Guild: {guild_id}, Channel: {channel_id}")
        await self.update_boards_for_notification_channel(guild_id, channel_id)

    async def on_notification_created(self, guild_id: int, channel_id: int):
        """Called when a notification is created"""
        self.logger.info(f"[SCHEDULE] Notification created event - Guild: {guild_id}, Channel: {channel_id}")
        await self.update_boards_for_notification_channel(guild_id, channel_id)

    async def on_notification_updated(self, guild_id: int, channel_id: int):
        """Called when a notification is updated"""
        self.logger.info(f"[SCHEDULE] Notification updated event - Guild: {guild_id}, Channel: {channel_id}")
        await self.update_boards_for_notification_channel(guild_id, channel_id)

    async def on_notification_deleted(self, guild_id: int, channel_id: int):
        """Called when a notification is deleted"""
        self.logger.info(f"[SCHEDULE] Notification deleted event - Guild: {guild_id}, Channel: {channel_id}")
        await self.update_boards_for_notification_channel(guild_id, channel_id)

    async def on_notification_toggled(self, guild_id: int, channel_id: int):
        """Called when a notification is enabled/disabled"""
        self.logger.info(f"[SCHEDULE] Notification toggled event - Guild: {guild_id}, Channel: {channel_id}")
        await self.update_boards_for_notification_channel(guild_id, channel_id)

    async def check_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user is admin (same as bear_trap.py)"""
        try:
            admin_conn = sqlite3.connect('db/settings.sqlite')
            admin_cursor = admin_conn.cursor()
            admin_cursor.execute("SELECT id FROM admin WHERE id = ?", (interaction.user.id,))
            is_admin = admin_cursor.fetchone() is not None
            admin_conn.close()

            if not is_admin:
                await interaction.response.send_message(
                    "❌ You don't have permission to use this command!",
                    ephemeral=True
                )
            return is_admin
        except Exception as e:
            print(f"[ERROR] Error checking admin: {e}")
            return False

    async def show_main_menu(self, interaction: discord.Interaction):
        """Shows the main schedule board management menu"""
        if not await self.check_admin(interaction):
            return

        try:
            # Get boards for this guild
            self.cursor.execute("""
                SELECT id, board_type, target_channel_id, channel_id
                FROM notification_schedule_boards
                WHERE guild_id = ?
                ORDER BY created_at DESC
            """, (interaction.guild.id,))
            boards = self.cursor.fetchall()

            embed = discord.Embed(
                title="📅 Schedule Board Management",
                description=(
                    "Manage automated schedule boards that display upcoming notifications.\n\n"
                    f"**Active Boards:** {len(boards)}\n\n"
                    "Use the buttons below to create or manage boards."
                ),
                color=discord.Color.blue()
            )

            view = ScheduleBoardMainView(self, interaction.guild.id, boards)
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            print(f"[ERROR] Error showing main menu: {e}")
            traceback.print_exc()
            await interaction.response.send_message(
                "❌ An error occurred while loading the menu.",
                ephemeral=True
            )

class ScheduleBoardMainView(discord.ui.View):
    """Main menu for schedule board management"""
    def __init__(self, cog, guild_id: int, boards: list):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.boards = boards

        # Disable manage button if no boards
        if not boards:
            self.manage_board_button.disabled = True

    @discord.ui.button(label="Create Board", emoji="➕", style=discord.ButtonStyle.primary, row=0)
    async def create_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = CreateBoardTypeModal(self.cog, self.guild_id)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"[ERROR] Error in create board button: {e}")
            traceback.print_exc()
            await interaction.response.send_message("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Manage Boards", emoji="⚙️", style=discord.ButtonStyle.secondary, row=0)
    async def manage_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = BoardSelectionView(self.cog, self.guild_id, self.boards)
            embed = discord.Embed(
                title="📋 Select Board to Manage",
                description=f"Choose from {len(self.boards)} board(s):",
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"[ERROR] Error in manage board button: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Refresh", emoji="🔄", style=discord.ButtonStyle.secondary, row=0)
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_main_menu(interaction)
        except Exception as e:
            print(f"[ERROR] Error in refresh button: {e}")
            traceback.print_exc()

class CreateBoardTypeModal(discord.ui.Modal):
    """Step 1: Select board type"""
    def __init__(self, cog, guild_id: int):
        super().__init__(title="Create Schedule Board - Step 1")
        self.cog = cog
        self.guild_id = guild_id

        self.board_type = discord.ui.TextInput(
            label="Board Type",
            placeholder="Enter 'server' or 'channel'",
            max_length=10,
            required=True
        )
        self.add_item(self.board_type)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            board_type = self.board_type.value.strip().lower()

            if board_type not in ["server", "channel"]:
                await interaction.response.send_message(
                    "❌ Invalid board type! Please enter 'server' or 'channel'.",
                    ephemeral=True
                )
                return

            # Proceed to channel selection
            view = CreateBoardChannelSelectView(self.cog, self.guild_id, board_type)
            embed = discord.Embed(
                title="📅 Create Schedule Board - Step 2",
                description=(
                    f"**Board Type:** {board_type.capitalize()}\n\n"
                    f"{'Select which channel to display events for:' if board_type == 'channel' else 'Select where to post the board:'}"
                ),
                color=discord.Color.blue()
            )
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

        except Exception as e:
            print(f"[ERROR] Error in create board type modal: {e}")
            traceback.print_exc()
            await interaction.response.send_message("❌ An error occurred!", ephemeral=True)

class CreateBoardChannelSelectView(discord.ui.View):
    """Step 2: Select channels (target channel + display channel)"""
    def __init__(self, cog, guild_id: int, board_type: str):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.board_type = board_type
        self.target_channel_id = None
        self.display_channel_id = None

        # Add appropriate channel select based on board type
        if board_type == "channel":
            target_select = discord.ui.ChannelSelect(
                placeholder="Select channel to track notifications for",
                channel_types=[discord.ChannelType.text],
                min_values=1,
                max_values=1,
                row=0
            )
            target_select.callback = self.target_channel_callback
            self.add_item(target_select)

        display_select = discord.ui.ChannelSelect(
            placeholder="Select where to post the board",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1,
            row=1
        )
        display_select.callback = self.display_channel_callback
        self.add_item(display_select)

    async def target_channel_callback(self, interaction: discord.Interaction):
        try:
            self.target_channel_id = int(interaction.data["values"][0])
            await interaction.response.send_message(
                f"✅ Tracking events from <#{self.target_channel_id}>",
                ephemeral=True
            )
        except Exception as e:
            print(f"[ERROR] Error in target channel select: {e}")
            traceback.print_exc()

    async def display_channel_callback(self, interaction: discord.Interaction):
        try:
            self.display_channel_id = int(interaction.data["values"][0])

            # For server boards, target_channel_id is None
            if self.board_type == "server":
                self.target_channel_id = None

            # Check if we have required selections
            if self.board_type == "channel" and not self.target_channel_id:
                await interaction.response.send_message(
                    "❌ Please select the target channel first!",
                    ephemeral=True
                )
                return

            # Proceed to settings
            await self.show_settings(interaction)

        except Exception as e:
            print(f"[ERROR] Error in display channel select: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    async def show_settings(self, interaction: discord.Interaction):
        """Move to settings configuration"""
        modal = CreateBoardSettingsModal(
            self.cog,
            self.guild_id,
            self.board_type,
            self.target_channel_id,
            self.display_channel_id,
            interaction.user.id
        )
        await interaction.response.send_modal(modal)

class CreateBoardSettingsModal(discord.ui.Modal):
    """Step 3: Configure board settings"""
    def __init__(self, cog, guild_id: int, board_type: str, target_channel_id: int,
                 display_channel_id: int, creator_id: int):
        super().__init__(title="Create Schedule Board - Step 3")
        self.cog = cog
        self.guild_id = guild_id
        self.board_type = board_type
        self.target_channel_id = target_channel_id
        self.display_channel_id = display_channel_id
        self.creator_id = creator_id

        self.max_events = discord.ui.TextInput(
            label="Max Events to Show",
            placeholder="Default: 15",
            default="15",
            max_length=3,
            required=False
        )
        self.add_item(self.max_events)

        self.timezone = discord.ui.TextInput(
            label="Timezone",
            placeholder="e.g., UTC, America/New_York, Europe/London",
            default="UTC",
            required=False
        )
        self.add_item(self.timezone)

        self.show_disabled = discord.ui.TextInput(
            label="Show Disabled Events? (yes/no)",
            placeholder="Default: no",
            default="no",
            max_length=3,
            required=False
        )
        self.add_item(self.show_disabled)

        self.auto_pin = discord.ui.TextInput(
            label="Auto-pin Board? (yes/no)",
            placeholder="Default: yes",
            default="yes",
            max_length=3,
            required=False
        )
        self.add_item(self.auto_pin)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Validate timezone
            try:
                tz = pytz.timezone(self.timezone.value.strip())
            except pytz.exceptions.UnknownTimeZoneError:
                await interaction.response.send_message(
                    "❌ Invalid timezone! Please use a valid timezone (e.g., UTC, America/New_York).",
                    ephemeral=True
                )
                return

            # Validate max events
            try:
                max_events = int(self.max_events.value.strip()) if self.max_events.value.strip() else 15
                if max_events < 1 or max_events > 50:
                    raise ValueError("Max events must be between 1 and 50")
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid max events! Please enter a number between 1 and 50.",
                    ephemeral=True
                )
                return

            # Parse yes/no values
            show_disabled = self.show_disabled.value.strip().lower() in ["yes", "y", "true", "1"]
            auto_pin = self.auto_pin.value.strip().lower() in ["yes", "y", "true", "1"]

            # Create settings dict
            settings = {
                'max_events': max_events,
                'timezone': tz.zone,
                'show_disabled': show_disabled,
                'auto_pin': auto_pin
            }

            # Defer the response while we create the board
            await interaction.response.defer(ephemeral=True)

            # Create the board
            board_id, error = await self.cog.create_schedule_board(
                self.guild_id,
                self.display_channel_id,
                self.board_type,
                self.target_channel_id,
                self.creator_id,
                settings
            )

            if error:
                await interaction.followup.send(f"❌ Failed to create board: {error}", ephemeral=True)
                return

            # Success!
            target_info = f"<#{self.target_channel_id}>" if self.board_type == "channel" else "all channels"
            await interaction.followup.send(
                f"✅ **Schedule board created!**\n\n"
                f"**Type:** {self.board_type.capitalize()}\n"
                f"**Tracking:** {target_info}\n"
                f"**Posted in:** <#{self.display_channel_id}>\n"
                f"**Board ID:** {board_id}",
                ephemeral=True
            )

        except Exception as e:
            print(f"[ERROR] Error in create board settings modal: {e}")
            traceback.print_exc()
            try:
                await interaction.followup.send("❌ An error occurred!", ephemeral=True)
            except:
                await interaction.response.send_message("❌ An error occurred!", ephemeral=True)

class BoardSelectionView(discord.ui.View):
    """View to select which board to manage"""
    def __init__(self, cog, guild_id: int, boards: list):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.boards = boards

        # Create select menu with boards
        if boards:
            options = []
            for board in boards[:25]:  # Discord limit
                board_id, board_type, target_channel_id, display_channel_id = board
                label = f"{board_type.capitalize()} Board"
                if board_type == "channel":
                    label += f" (#{target_channel_id})"
                description = f"Posted in #{display_channel_id} | ID: {board_id}"

                options.append(
                    discord.SelectOption(
                        label=label[:100],  # Discord limit
                        value=str(board_id),
                        description=description[:100],
                        emoji="📋"
                    )
                )

            select = discord.ui.Select(
                placeholder="Select a board to manage...",
                min_values=1,
                max_values=1,
                options=options,
                row=0
            )
            select.callback = self.board_select_callback
            self.add_item(select)

        # Back button
        back_btn = discord.ui.Button(label="Back", emoji="◀️", style=discord.ButtonStyle.secondary, row=1)
        back_btn.callback = self.back_callback
        self.add_item(back_btn)

    async def board_select_callback(self, interaction: discord.Interaction):
        try:
            board_id = int(interaction.data["values"][0])

            # Show board management view
            view = BoardManagementView(self.cog, self.guild_id, board_id)
            embed = await view.create_embed()
            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"[ERROR] Error in board select: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    async def back_callback(self, interaction: discord.Interaction):
        await self.cog.show_main_menu(interaction)

class BoardManagementView(discord.ui.View):
    """View to manage a specific board (edit/delete/move/preview)"""
    def __init__(self, cog, guild_id: int, board_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.board_id = board_id

    async def create_embed(self) -> discord.Embed:
        """Creates embed showing board info"""
        try:
            self.cog.cursor.execute("""
                SELECT board_type, target_channel_id, channel_id, max_events,
                       show_disabled, auto_pin, timezone, created_at
                FROM notification_schedule_boards
                WHERE id = ?
            """, (self.board_id,))
            result = self.cog.cursor.fetchone()

            if not result:
                return discord.Embed(
                    title="❌ Error",
                    description="Board not found!",
                    color=discord.Color.red()
                )

            (board_type, target_channel_id, display_channel_id, max_events,
             show_disabled, auto_pin, timezone, created_at) = result

            target_info = f"<#{target_channel_id}>" if board_type == "channel" else "All channels"

            embed = discord.Embed(
                title=f"📋 Managing Board #{self.board_id}",
                description=(
                    f"**Type:** {board_type.capitalize()}\n"
                    f"**Tracking:** {target_info}\n"
                    f"**Posted in:** <#{display_channel_id}>\n\n"
                    f"**Settings:**\n"
                    f"• Max Events: {max_events}\n"
                    f"• Timezone: {timezone}\n"
                    f"• Show Disabled: {'Yes' if show_disabled else 'No'}\n"
                    f"• Auto-pin: {'Yes' if auto_pin else 'No'}\n\n"
                    f"Created: {created_at}"
                ),
                color=discord.Color.blue()
            )

            return embed

        except Exception as e:
            print(f"[ERROR] Error creating board management embed: {e}")
            traceback.print_exc()
            return discord.Embed(
                title="❌ Error",
                description="Failed to load board info",
                color=discord.Color.red()
            )

    @discord.ui.button(label="Edit Settings", emoji="✏️", style=discord.ButtonStyle.primary, row=0)
    async def edit_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = EditBoardSettingsModal(self.cog, self.board_id)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"[ERROR] Error in edit settings: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Move Board", emoji="📤", style=discord.ButtonStyle.secondary, row=0)
    async def move_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            channel_select = discord.ui.ChannelSelect(
                placeholder="Select new channel for board",
                channel_types=[discord.ChannelType.text],
                min_values=1,
                max_values=1
            )

            async def channel_callback(select_interaction: discord.Interaction):
                await select_interaction.response.defer(ephemeral=True)
                new_channel_id = int(select_interaction.data["values"][0])

                success, error = await self.cog.move_schedule_board(self.board_id, new_channel_id)

                if error:
                    await select_interaction.followup.send(f"❌ Failed to move: {error}", ephemeral=True)
                else:
                    await select_interaction.followup.send(
                        f"✅ Board moved to <#{new_channel_id}>!",
                        ephemeral=True
                    )
                    # Refresh the view
                    embed = await self.create_embed()
                    await select_interaction.message.edit(embed=embed, view=self)

            channel_select.callback = channel_callback
            view = discord.ui.View()
            view.add_item(channel_select)

            await interaction.response.send_message(
                "Select new channel for the board:",
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"[ERROR] Error in move board: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Preview", emoji="👁️", style=discord.ButtonStyle.secondary, row=0)
    async def preview_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer(ephemeral=True)
            embed = await self.cog.generate_schedule_embed(self.board_id, page=0)
            await interaction.followup.send(
                "**Preview of schedule board:**",
                embed=embed,
                ephemeral=True
            )
        except Exception as e:
            print(f"[ERROR] Error in preview: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Delete Board", emoji="🗑️", style=discord.ButtonStyle.danger, row=1)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = ConfirmDeleteView(self.cog, self.guild_id, self.board_id)
            embed = discord.Embed(
                title="⚠️ Confirm Deletion",
                description=f"Are you sure you want to delete board #{self.board_id}?\n\nThis will remove the board message and cannot be undone.",
                color=discord.Color.red()
            )
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"[ERROR] Error in delete button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Back", emoji="◀️", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_main_menu(interaction)


class EditBoardSettingsModal(discord.ui.Modal):
    """Modal to edit board settings"""
    def __init__(self, cog, board_id: int):
        super().__init__(title="Edit Board Settings")
        self.cog = cog
        self.board_id = board_id

        # Load current settings
        cog.cursor.execute("""
            SELECT max_events, timezone, show_disabled, auto_pin
            FROM notification_schedule_boards
            WHERE id = ?
        """, (board_id,))
        result = cog.cursor.fetchone()

        if result:
            max_events, timezone, show_disabled, auto_pin = result

            self.max_events = discord.ui.TextInput(
                label="Max Events to Show",
                default=str(max_events),
                max_length=3,
                required=False
            )
            self.add_item(self.max_events)

            self.timezone = discord.ui.TextInput(
                label="Timezone",
                default=timezone,
                required=False
            )
            self.add_item(self.timezone)

            self.show_disabled = discord.ui.TextInput(
                label="Show Disabled Events? (yes/no)",
                default="yes" if show_disabled else "no",
                max_length=3,
                required=False
            )
            self.add_item(self.show_disabled)

            self.auto_pin = discord.ui.TextInput(
                label="Auto-pin Board? (yes/no)",
                default="yes" if auto_pin else "no",
                max_length=3,
                required=False
            )
            self.add_item(self.auto_pin)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Validate timezone
            try:
                tz = pytz.timezone(self.timezone.value.strip())
            except pytz.exceptions.UnknownTimeZoneError:
                await interaction.response.send_message(
                    "❌ Invalid timezone!",
                    ephemeral=True
                )
                return

            # Validate max events
            try:
                max_events = int(self.max_events.value.strip())
                if max_events < 1 or max_events > 50:
                    raise ValueError()
            except ValueError:
                await interaction.response.send_message(
                    "❌ Max events must be between 1 and 50!",
                    ephemeral=True
                )
                return

            # Parse yes/no
            show_disabled = self.show_disabled.value.strip().lower() in ["yes", "y", "true", "1"]
            auto_pin = self.auto_pin.value.strip().lower() in ["yes", "y", "true", "1"]

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET max_events = ?, timezone = ?, show_disabled = ?, auto_pin = ?
                WHERE id = ?
            """, (max_events, tz.zone, 1 if show_disabled else 0, 1 if auto_pin else 0, self.board_id))
            self.cog.conn.commit()

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

            await interaction.response.send_message(
                "✅ Settings updated and board refreshed!",
                ephemeral=True
            )

        except Exception as e:
            print(f"[ERROR] Error updating settings: {e}")
            traceback.print_exc()
            await interaction.response.send_message("❌ An error occurred!", ephemeral=True)


class ConfirmDeleteView(discord.ui.View):
    """Confirmation view for deleting a board"""
    def __init__(self, cog, guild_id: int, board_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.board_id = board_id

    @discord.ui.button(label="Yes, Delete", emoji="✅", style=discord.ButtonStyle.danger, row=0)
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer(ephemeral=True)

            success, error = await self.cog.delete_schedule_board(self.board_id)

            if error:
                await interaction.followup.send(f"❌ Failed to delete: {error}", ephemeral=True)
            else:
                await interaction.followup.send("✅ Board deleted successfully!", ephemeral=True)
                # Return to main menu
                await self.cog.show_main_menu(interaction)

        except Exception as e:
            print(f"[ERROR] Error confirming delete: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.secondary, row=0)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Return to board management
        view = BoardManagementView(self.cog, self.guild_id, self.board_id)
        embed = await view.create_embed()
        await interaction.response.edit_message(embed=embed, view=view)

async def setup(bot):
    await bot.add_cog(BearTrapSchedule(bot))