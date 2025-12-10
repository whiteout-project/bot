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
from .bear_event_types import get_event_icon

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

        # Add show_repeating_events column if it doesn't exist
        try:
            self.cursor.execute("SELECT show_repeating_events FROM notification_schedule_boards LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_schedule_boards ADD COLUMN show_repeating_events INTEGER DEFAULT 1")

        # Add use_user_timezone column if it doesn't exist
        try:
            self.cursor.execute("SELECT use_user_timezone FROM notification_schedule_boards LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_schedule_boards ADD COLUMN use_user_timezone INTEGER DEFAULT 0")

        # Add hide_daily_reset column if it doesn't exist
        try:
            self.cursor.execute("SELECT hide_daily_reset FROM notification_schedule_boards LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_schedule_boards ADD COLUMN hide_daily_reset INTEGER DEFAULT 1")

        self.conn.commit()
        self.logger.info("[SCHEDULE] Cog initialized successfully")

    async def cog_load(self):
        """Start background tasks when cog loads"""
        self.logger.info("[SCHEDULE] Starting background tasks...")
        self.refresh_task = asyncio.create_task(self.daily_refresh_loop())
        self.urgency_task = asyncio.create_task(self.urgency_update_loop())

        # Refresh all boards on startup
        self.logger.info("[SCHEDULE] Refreshing all boards on startup...")
        asyncio.create_task(self._refresh_all_boards_on_startup())

    async def cog_unload(self):
        """Cleanup when cog is unloaded"""
        self.logger.info("[SCHEDULE] Cog unloading...")

        # Cancel background tasks
        if hasattr(self, 'refresh_task'):
            self.refresh_task.cancel()
        if hasattr(self, 'urgency_task'):
            self.urgency_task.cancel()

        if hasattr(self, 'conn'):
            self.conn.close()
        self.logger.info("[SCHEDULE] Cog unloaded")

    async def _refresh_all_boards_on_startup(self):
        """Refresh all schedule boards on bot startup"""
        await self.bot.wait_until_ready()

        try:
            # Get all board IDs
            self.cursor.execute("SELECT id FROM notification_schedule_boards")
            board_ids = [row[0] for row in self.cursor.fetchall()]

            self.logger.info(f"[SCHEDULE] Found {len(board_ids)} board(s) to refresh on startup")

            # Refresh each board
            refreshed = 0
            for board_id in board_ids:
                try:
                    await self.update_schedule_board(board_id)
                    refreshed += 1
                except Exception as e:
                    self.logger.error(f"[SCHEDULE] Failed to refresh board {board_id} on startup: {e}")
                    continue

            self.logger.info(f"[SCHEDULE] Startup refresh complete: {refreshed}/{len(board_ids)} boards updated")

        except Exception as e:
            self.logger.error(f"[SCHEDULE] Error during startup refresh: {e}")

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

    async def urgency_update_loop(self):
        """Background task that updates boards when events transition to SOON or IMMINENT"""
        await self.bot.wait_until_ready()

        while not self.bot.is_closed():
            try:
                now_utc = datetime.now(pytz.UTC)

                # Get all notifications that are approaching
                svs_conn = sqlite3.connect('db/svs.sqlite')
                svs_cursor = svs_conn.cursor()

                svs_cursor.execute("""
                    SELECT id, channel_id, next_notification
                    FROM bear_notifications
                    WHERE is_enabled = 1 AND next_notification IS NOT NULL
                """)
                notifications = svs_cursor.fetchall()
                svs_conn.close()

                boards_to_update = set()

                for notif_id, channel_id, next_notif_str in notifications:
                    try:
                        # Parse next notification time
                        next_time = datetime.fromisoformat(next_notif_str.replace('Z', '+00:00'))
                        if next_time.tzinfo is None:
                            next_time = pytz.UTC.localize(next_time)

                        # Calculate time until notification
                        time_until = (next_time - now_utc).total_seconds() / 3600.0  # in hours

                        # Check if we're crossing the 6-hour or 1-hour threshold
                        crossing_threshold = False

                        if 6.0 < time_until <= 6.083:  # Just crossed 6-hour threshold
                            crossing_threshold = True
                        elif 1.0 < time_until <= 1.083:  # Just crossed 1-hour threshold
                            crossing_threshold = True

                        if crossing_threshold:
                            # Find all boards that should show this notification
                            self.cursor.execute("""
                                SELECT DISTINCT nsb.id
                                FROM notification_schedule_boards nsb
                                WHERE nsb.guild_id IN (
                                    SELECT guild_id FROM channels WHERE id = ?
                                )
                                AND (
                                    (nsb.board_type = 'server')
                                    OR (nsb.board_type = 'channel' AND nsb.target_channel_id = ?)
                                )
                            """, (channel_id, channel_id))

                            for (board_id,) in self.cursor.fetchall():
                                boards_to_update.add(board_id)

                    except Exception as e:
                        self.logger.error(f"[SCHEDULE] Error processing notification {notif_id}: {e}")
                        continue

                # Update all affected boards
                if boards_to_update:
                    self.logger.info(f"[SCHEDULE] Urgency update - refreshing {len(boards_to_update)} board(s)")
                    for board_id in boards_to_update:
                        await self.update_schedule_board(board_id)

                # Check every 5 minutes
                await asyncio.sleep(300)

            except Exception as e:
                self.logger.error(f"[SCHEDULE] Error in urgency update loop: {e}")
                await asyncio.sleep(300)  # Continue even if error occurs

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

            # Post message to Discord with placeholder
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
                 max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range, show_repeating_events, use_user_timezone, hide_daily_reset, created_by, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                guild_id, channel_id, message.id, board_type, target_channel_id,
                settings.get('max_events', 15),
                1 if settings.get('show_disabled', False) else 0,
                1 if settings.get('auto_pin', True) else 0,
                settings.get('timezone', 'UTC'),
                settings.get('filter_name'),
                settings.get('filter_time_range'),
                1 if settings.get('show_repeating_events', True) else 0,
                1 if settings.get('use_user_timezone', False) else 0,
                1 if settings.get('hide_daily_reset', True) else 0,
                creator_id,
                datetime.now(pytz.UTC).isoformat()
            ))

            self.conn.commit()
            board_id = self.cursor.lastrowid

            # Attach pagination view with the board_id
            total_pages = self._get_total_pages_from_footer(embed.footer.text if embed.footer else "")
            view = ScheduleBoardPaginationView(self, board_id, current_page=0, total_pages=total_pages)
            await message.edit(view=view)

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
                       max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range, show_repeating_events
                FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                return (False, "Board not found!")

            (guild_id, old_channel_id, old_message_id, board_type, target_channel_id,
             max_events, show_disabled, auto_pin, timezone, filter_name, filter_time_range, show_repeating_events) = result

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
                'filter_time_range': filter_time_range,
                'show_repeating_events': bool(show_repeating_events) if show_repeating_events is not None else True
            }

            embed = await self.generate_schedule_embed_for_new_board(
                guild_id, board_type, target_channel_id, settings
            )

            # Post to new channel with pagination view
            total_pages = self._get_total_pages_from_footer(embed.footer.text if embed.footer else "")
            view = ScheduleBoardPaginationView(self, board_id, current_page=0, total_pages=total_pages)
            new_message = await new_channel.send(embed=embed, view=view)

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
                       show_disabled, timezone, filter_name, filter_time_range, show_repeating_events, use_user_timezone, hide_daily_reset
                FROM notification_schedule_boards
                WHERE id = ?
            """, (board_id,))
            result = self.cursor.fetchone()

            if not result:
                return self._create_error_embed("Board not found!")

            (guild_id, board_type, target_channel_id, max_events,
             show_disabled, timezone, filter_name, filter_time_range, show_repeating_events, use_user_timezone, hide_daily_reset) = result

            settings = {
                'max_events': max_events,
                'show_disabled': bool(show_disabled),
                'timezone': timezone,
                'filter_name': filter_name,
                'filter_time_range': filter_time_range,
                'show_repeating_events': bool(show_repeating_events) if show_repeating_events is not None else True,
                'use_user_timezone': use_user_timezone if use_user_timezone is not None else 0,
                'hide_daily_reset': bool(hide_daily_reset) if hide_daily_reset is not None else True
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
                       notification_type, next_notification, is_enabled, repeat_enabled, repeat_minutes, event_type
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

            # Filter out Daily Reset events
            if settings.get('hide_daily_reset', True):
                notifications = [n for n in notifications if n[11] != 'Daily Reset']  # n[11] is event_type

            # No notifications found
            if not notifications:
                return self._create_empty_schedule_embed(board_type, target_channel_id, settings)

            # Expand repeating events if enabled
            show_repeating = settings.get('show_repeating_events', True)
            expanded_events = []
            now = datetime.now(pytz.UTC)

            # Determine time window for expanding repeating events (30 days)
            max_future_time = now + timedelta(days=30)

            for notif in notifications:
                (notif_id, channel_id, hour, minute, notif_timezone, description,
                 notification_type, next_notification, is_enabled, repeat_enabled, repeat_minutes, event_type) = notif

                next_time = datetime.fromisoformat(next_notification)

                # Add the first occurrence
                expanded_events.append((next_time, notif))

                # If repeating events are enabled and this event repeats, generate future occurrences
                if show_repeating and repeat_enabled:
                    if isinstance(repeat_minutes, int) and repeat_minutes > 0:
                        # Handle interval-based repeating (every X minutes)
                        current_time = next_time
                        while True:
                            current_time = current_time + timedelta(minutes=repeat_minutes)
                            if current_time > max_future_time:
                                break
                            # Create a modified notification tuple with updated next_notification
                            modified_notif = list(notif)
                            modified_notif[7] = current_time.isoformat()  # Update next_notification
                            expanded_events.append((current_time, tuple(modified_notif)))

                    elif repeat_minutes == -1:
                        # Handle fixed weekday repeating
                        self.cursor.execute("""
                            SELECT weekday FROM notification_days
                            WHERE notification_id = ?
                        """, (notif_id,))
                        rows = self.cursor.fetchall()
                        notification_days = set()

                        for row in rows:
                            parts = row[0].split('|')
                            notification_days.update(int(p) for p in parts)

                        # Generate occurrences for each matching weekday
                        current_date = next_time.date()
                        event_tz = pytz.timezone(notif_timezone)

                        for day_offset in range(1, 31):  # Check next 30 days
                            check_date = current_date + timedelta(days=day_offset)
                            if check_date.weekday() in notification_days:
                                occurrence_time = event_tz.localize(
                                    datetime.combine(check_date, datetime.min.time()).replace(hour=hour, minute=minute)
                                )
                                occurrence_time_utc = occurrence_time.astimezone(pytz.UTC)

                                if occurrence_time_utc > max_future_time:
                                    break

                                # Create a modified notification tuple
                                modified_notif = list(notif)
                                modified_notif[7] = occurrence_time_utc.isoformat()
                                expanded_events.append((occurrence_time_utc, tuple(modified_notif)))

            # Sort all events by time
            expanded_events.sort(key=lambda x: x[0])

            # Filter to only future events
            future_events = [(time, notif) for time, notif in expanded_events if time > now]

            if not future_events:
                return self._create_empty_schedule_embed(board_type, target_channel_id, settings)

            # Pagination (cap at 30 events per page)
            max_events = min(settings.get('max_events', 15), 30)
            total_events = len(future_events)
            total_pages = math.ceil(total_events / max_events) if total_events > 0 else 1
            page = max(0, min(page, total_pages - 1))  # Clamp page

            start_idx = page * max_events
            end_idx = min(start_idx + max_events, total_events)
            page_events = future_events[start_idx:end_idx]

            # Format notifications by urgency
            tz_string = settings.get('timezone', 'UTC')
            tz = self._get_timezone_object(tz_string)

            sections = {
                'imminent': [],   # < 1 hour
                'soon': [],       # 1-6 hours
                'upcoming': [],   # 6-24 hours
                'this_week': [],  # 1-7 days
                'next_week': [],  # 7-14 days
                'later': []       # 14-30 days
            }

            for event_time, notif in page_events:
                time_until = event_time - now
                hours_until = time_until.total_seconds() / 3600
                days_until = hours_until / 24

                # Store event with its time for later grouping by date
                event_data = (event_time, notif)

                if hours_until < 1:
                    sections['imminent'].append(event_data)
                elif hours_until < 6:
                    sections['soon'].append(event_data)
                elif hours_until < 24:
                    sections['upcoming'].append(event_data)
                elif days_until < 7:
                    sections['this_week'].append(event_data)
                elif days_until < 14:
                    sections['next_week'].append(event_data)
                else:
                    sections['later'].append(event_data)

            # Build embed
            if board_type == 'channel':
                channel_text = f"from <#{target_channel_id}> "
            else:
                channel_text = ""

            if settings.get('use_user_timezone', 0):
                tz_info = f"Showing all upcoming events {channel_text}in your local timezone."
            else:
                tz_display = self._format_timezone_display(settings.get('timezone', 'UTC'))
                tz_info = f"Showing all upcoming events {channel_text}in {tz_display}."
            description = f"📅 **Upcoming Event Schedule**\n{tz_info}\n\n"

            # Helper function to format section with day grouping
            async def format_section_with_days(events, show_channel):
                from collections import defaultdict
                days_dict = defaultdict(list)

                # Group events by date
                for event_time, notif in events:
                    event_time_tz = event_time.astimezone(tz)
                    date_key = event_time_tz.date()
                    days_dict[date_key].append((event_time, notif))

                # Build formatted output
                output_lines = []
                one_year_from_now = now.date() + timedelta(days=365)

                for date in sorted(days_dict.keys()):
                    # Date header
                    if date <= one_year_from_now:
                        # "27 November - Saturday"
                        date_str = date.strftime('%d %B - %A')
                    else:
                        # "27 November 2025 - Saturday"
                        date_str = date.strftime('%d %B %Y - %A')

                    output_lines.append(f"- **{date_str}**")

                    # Format events for this day
                    for event_time, notif in days_dict[date]:
                        line = await self._format_event_line(notif, tz, show_channel, settings.get('use_user_timezone', 0))
                        output_lines.append(f"└ {line}")

                return "\n".join(output_lines)

            if sections['imminent']:
                description += "🔴 **IMMINENT** (< 1 hour)\n"
                description += await format_section_with_days(sections['imminent'], board_type == 'server') + "\n\n"

            if sections['soon']:
                description += "🟡 **SOON** (1-6 hours)\n"
                description += await format_section_with_days(sections['soon'], board_type == 'server') + "\n\n"

            if sections['upcoming']:
                description += "🟢 **UPCOMING** (6-24 hours)\n"
                description += await format_section_with_days(sections['upcoming'], board_type == 'server') + "\n\n"

            if sections['this_week']:
                description += "📅 **2-7 DAYS**\n"
                description += await format_section_with_days(sections['this_week'], board_type == 'server') + "\n\n"

            if sections['next_week']:
                description += "📆 **1-2 WEEKS**\n"
                description += await format_section_with_days(sections['next_week'], board_type == 'server') + "\n\n"

            if sections['later']:
                description += "🗓️ **FUTURE** (14+ days)\n"
                description += await format_section_with_days(sections['later'], board_type == 'server') + "\n\n"

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
            if settings.get('use_user_timezone', 0):
                tz_indicator = "(Local Time)"
            else:
                tz_indicator = f"({self._format_timezone_display(settings.get('timezone', 'UTC'))})"

            footer_text = f"Last updated: {now.astimezone(tz).strftime('%b %d, %I:%M %p')} {tz_indicator}"
            if total_pages > 1:
                footer_text += f" | Page {page + 1} of {total_pages}"
            embed.set_footer(text=footer_text)

            return embed

        except Exception as e:
            print(f"[ERROR] Failed to generate schedule embed internally: {e}")
            traceback.print_exc()
            return self._create_error_embed(f"Error: {str(e)}")

    async def _format_event_line(self, notification, timezone_obj, show_channel: bool, use_user_timezone: int = 0) -> str:
        """Formats a single notification as a line in the schedule

        Args:
            notification: The notification tuple
            timezone_obj: Timezone object for calculations
            show_channel: Whether to show channel info
            use_user_timezone: Whether to use Discord timestamps for local timezone (1) or custom format (0)
        """
        try:
            (notif_id, channel_id, hour, minute, notif_timezone, description,
             notification_type, next_notification, is_enabled, repeat_enabled, repeat_minutes, event_type) = notification

            # Parse next notification time
            next_time = datetime.fromisoformat(next_notification)
            next_time_tz = next_time.astimezone(timezone_obj)

            # Format time
            if use_user_timezone:
                timestamp = int(next_time.timestamp())
                time_str = f"<t:{timestamp}:t>"  # :t = short time only
            else:
                time_str = next_time_tz.strftime('%H:%M')

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

            # Get emoji for this event type
            emoji = get_event_icon(event_type) if event_type else "📅"

            # Strip "Notification" suffix if present (for backwards compatibility)
            if name.endswith(" Notification"):
                name = name[:-13]

            # Build line: **time** - emoji name
            line = f"**{time_str}** - {emoji} {name}"
            if show_channel:
                line += f" <#{channel_id}>"

            if not is_enabled:
                line += " ⚠️ [DISABLED]"

            return line

        except Exception as e:
            print(f"[ERROR] Failed to format event line: {e}")
            return "• Error formatting event"

    def _create_empty_schedule_embed(self, board_type: str, target_channel_id: int, settings: dict) -> discord.Embed:
        """Creates an embed for when no events are scheduled"""
        if board_type == 'channel':
            channel_text = f"from <#{target_channel_id}> "
        else:
            channel_text = ""

        if settings.get('use_user_timezone', 0):
            tz_info = f"Showing all upcoming events {channel_text}in your local timezone."
        else:
            tz_display = self._format_timezone_display(settings.get('timezone', 'UTC'))
            tz_info = f"Showing all upcoming events {channel_text}in {tz_display}."
        description = f"📅 **Upcoming Event Schedule**\n{tz_info}\n\n"

        if settings.get('filter_time_range'):
            description += f"No events in the next {settings['filter_time_range']} hours.\n\n"
        else:
            description += "No upcoming events scheduled.\n\n"

        description += "━━━━━━━━━━━━━━━━━━━━━━"

        tz = self._get_timezone_object(settings.get('timezone', 'UTC'))
        now = datetime.now(pytz.UTC).astimezone(tz)

        embed = discord.Embed(
            description=description,
            color=0x808080  # Gray
        )

        if settings.get('use_user_timezone', 0):
            tz_indicator = "(Local Time)"
        else:
            tz_indicator = f"({self._format_timezone_display(settings.get('timezone', 'UTC'))})"

        embed.set_footer(text=f"Last updated: {now.strftime('%b %d, %I:%M %p')} {tz_indicator}")

        return embed

    def _create_error_embed(self, error_message: str) -> discord.Embed:
        """Creates an error embed"""
        return discord.Embed(
            title="❌ Error",
            description=error_message,
            color=0xFF0000
        )

    def _get_timezone_object(self, tz_string: str):
        """Convert timezone string to a usable timezone object

        Handles:
            - "UTC" -> pytz.UTC
            - "Etc/GMT+3" -> pytz timezone
            - "UTC+05:30" -> Fixed offset timezone
        """
        from datetime import timezone, timedelta

        if tz_string == "UTC":
            return pytz.UTC
        elif tz_string.startswith("UTC+") or tz_string.startswith("UTC-"):
            # Parse fractional offset like UTC+05:30
            try:
                sign = 1 if tz_string[3] == '+' else -1
                parts = tz_string[4:].split(':')
                if len(parts) == 2:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    total_minutes = sign * (hours * 60 + minutes)
                    return timezone(timedelta(minutes=total_minutes))
                else:
                    # Shouldn't happen with our validation, but fallback
                    return pytz.UTC
            except:
                return pytz.UTC
        else:
            # Etc/GMT zones or other standard pytz timezones
            try:
                return pytz.timezone(tz_string)
            except:
                return pytz.UTC

    def _format_timezone_display(self, tz_zone: str) -> str:
        """Convert timezone name to user-friendly format

        Examples:
            Etc/GMT-3 -> UTC+3
            Etc/GMT+5 -> UTC-5
            UTC+05:30 -> UTC+5:30
            UTC -> UTC
        """
        if tz_zone == "UTC":
            return "UTC"
        elif tz_zone.startswith("UTC+") or tz_zone.startswith("UTC-"):
            # Already in user-friendly format (fractional offsets like UTC+05:30)
            # Convert to cleaner format: UTC+05:30 -> UTC+5:30
            try:
                sign = tz_zone[3]  # + or -
                parts = tz_zone[4:].split(':')
                if len(parts) == 2:
                    hours = int(parts[0])
                    minutes = int(parts[1])
                    if minutes == 0:
                        return f"UTC{sign}{hours}"
                    else:
                        return f"UTC{sign}{hours}:{minutes:02d}"
                else:
                    return tz_zone
            except:
                return tz_zone
        elif tz_zone.startswith("Etc/GMT"):
            # Etc/GMT zones are inverted: Etc/GMT-3 is actually UTC+3
            offset_str = tz_zone.replace("Etc/GMT", "")
            try:
                offset = int(offset_str)
                # Invert the offset back
                actual_offset = -offset
                if actual_offset == 0:
                    return "UTC"
                return f"UTC{actual_offset:+d}"
            except ValueError:
                return tz_zone  # Fallback to original if parsing fails
        else:
            # For other timezones, just return as-is
            return tz_zone

    def _get_total_pages_from_footer(self, footer_text: str) -> int:
        """Extract total pages from embed footer text"""
        try:
            if "Page" in footer_text:
                import re
                match = re.search(r'Page \d+ of (\d+)', footer_text)
                if match:
                    return int(match.group(1))
        except:
            pass
        return 1

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

            # Create pagination view
            total_pages = self._get_total_pages_from_footer(embed.footer.text if embed.footer else "")
            view = ScheduleBoardPaginationView(self, board_id, current_page=0, total_pages=total_pages)

            # Edit message
            await message.edit(embed=embed, view=view)

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

    async def show_main_menu(self, interaction: discord.Interaction, force_new: bool = False):
        """Shows the main schedule board management menu

        Args:
            interaction: The Discord interaction
            force_new: If True, always creates a new ephemeral message instead of editing
        """
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

            # If force_new is True, always send a new ephemeral message
            if force_new:
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            # Otherwise, try to edit the existing message
            elif interaction.response.is_done():
                await interaction.edit_original_response(embed=embed, view=view)
            else:
                await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"[ERROR] Error showing main menu: {e}")
            traceback.print_exc()
            try:
                await interaction.response.send_message(
                    "❌ An error occurred while loading the menu.",
                    ephemeral=True
                )
            except discord.InteractionResponded:
                await interaction.followup.send(
                    "❌ An error occurred while loading the menu.",
                    ephemeral=True
                )

class ScheduleBoardPaginationView(discord.ui.View):
    """Persistent pagination view for schedule boards"""
    def __init__(self, cog, board_id: int, current_page: int = 0, total_pages: int = 1):
        super().__init__(timeout=None)
        self.cog = cog
        self.board_id = board_id
        self.current_page = current_page
        self.total_pages = total_pages

        # Remove buttons if not needed
        if current_page <= 0:
            self.remove_item(self.previous_button)
        if current_page >= total_pages - 1:
            self.remove_item(self.next_button)

    @discord.ui.button(label="◀️ Previous", style=discord.ButtonStyle.secondary, custom_id="schedule_prev", row=0)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Go to previous page
            new_page = max(0, self.current_page - 1)

            # Generate new embed
            embed = await self.cog.generate_schedule_embed(self.board_id, page=new_page)

            # Get total pages from footer
            total_pages = self._get_total_pages_from_embed(embed)

            # Create new view with updated page
            new_view = ScheduleBoardPaginationView(self.cog, self.board_id, new_page, total_pages)

            # Update message
            await interaction.response.edit_message(embed=embed, view=new_view)

        except Exception as e:
            print(f"[ERROR] Pagination error: {e}")
            traceback.print_exc()
            await interaction.response.send_message("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="▶️ Next", style=discord.ButtonStyle.secondary, custom_id="schedule_next", row=0)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Go to next page
            new_page = self.current_page + 1

            # Generate new embed
            embed = await self.cog.generate_schedule_embed(self.board_id, page=new_page)

            # Get total pages from footer
            total_pages = self._get_total_pages_from_embed(embed)

            # Create new view with updated page
            new_view = ScheduleBoardPaginationView(self.cog, self.board_id, new_page, total_pages)

            # Update message
            await interaction.response.edit_message(embed=embed, view=new_view)

        except Exception as e:
            print(f"[ERROR] Pagination error: {e}")
            traceback.print_exc()
            await interaction.response.send_message("❌ An error occurred!", ephemeral=True)

    def _get_total_pages_from_embed(self, embed) -> int:
        """Extract total pages from embed footer"""
        try:
            footer = embed.footer.text
            if "Page" in footer:
                # Extract "Page X of Y"
                import re
                match = re.search(r'Page \d+ of (\d+)', footer)
                if match:
                    return int(match.group(1))
        except:
            pass
        return 1

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
            # Show board type selection view
            view = CreateBoardTypeView(self.cog, self.guild_id)
            embed = discord.Embed(
                title="📅 Create Schedule Board - Step 1",
                description=(
                    "Choose the type of schedule board you want to create:\n\n"
                    "**Board Types**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🌐 **Server-Wide Board**\n"
                    "└ Displays all notifications across all channels in the server\n"
                    "└ Perfect for a central overview of all upcoming events\n"
                    "📢 **Per-Channel Board**\n"
                    "└ Displays notifications for a specific channel only\n"
                    "└ Keeps channel-specific events organized\n"
                    "└ Ideal for dedicated event channels (e.g., Bear Trap only)\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"[ERROR] Error in create board button: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Manage Boards", emoji="⚙️", style=discord.ButtonStyle.secondary, row=0)
    async def manage_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            view = BoardSelectionView(self.cog, self.guild_id, self.boards, interaction.guild)
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

class CreateBoardTypeView(discord.ui.View):
    """Step 1: Select board type with buttons"""
    def __init__(self, cog, guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Server-Wide Board", emoji="🌐", style=discord.ButtonStyle.primary, row=0)
    async def server_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.proceed_to_channel_selection(interaction, "server")
        except Exception as e:
            print(f"[ERROR] Error in server board button: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Per-Channel Board", emoji="📢", style=discord.ButtonStyle.primary, row=0)
    async def channel_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.proceed_to_channel_selection(interaction, "channel")
        except Exception as e:
            print(f"[ERROR] Error in channel board button: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Back", emoji="◀️", style=discord.ButtonStyle.secondary, row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Return to main schedule board menu
            await self.cog.show_main_menu(interaction)
        except Exception as e:
            print(f"[ERROR] Error in back button: {e}")
            traceback.print_exc()

    async def proceed_to_channel_selection(self, interaction: discord.Interaction, board_type: str):
        """Proceed to Step 2: Channel selection"""
        view = CreateBoardChannelSelectView(self.cog, self.guild_id, board_type)

        # Build step description based on board type
        if board_type == 'channel':
            step_description = "**Step 2a:** Select which channel to track notifications for\n**Step 2b:** Select where to post the board"
        else:
            step_description = "**Step 2:** Select where to post the board"

        embed = discord.Embed(
            title="📅 Create Schedule Board - Step 2",
            description=(
                f"**Board Type:** {board_type.capitalize()}\n\n"
                f"{step_description}"
            ),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)

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
            await interaction.response.defer()
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
        view = CreateBoardSettingsView(
            self.cog,
            self.guild_id,
            self.board_type,
            self.target_channel_id,
            self.display_channel_id,
            interaction.user.id
        )

        target_info = f"<#{self.target_channel_id}>" if self.board_type == "channel" else "all channels"
        embed = discord.Embed(
            title="📅 Create Schedule Board - Step 3",
            description=(
                f"**Board Type:** {self.board_type.capitalize()}\n"
                f"**Tracking:** {target_info}\n"
                f"**Posted in:** <#{self.display_channel_id}>\n\n"
                "**Button Functions:**\n"
                "• **🔢 Max Events** - Set maximum number of events displayed on the schedule\n"
                "• **🌍 Timezone** - Select timezone for displaying event times\n"
                "• **🌐 User Timezone** - Show times in each user's local timezone\n"
                "• **👁️ Show Disabled** - Include or exclude disabled notifications from the schedule\n"
                "• **📌 Pin Message** - Automatically pin the schedule board message in the channel\n"
                "• **🔄 Show Repeating** - Show or hide future repetitions of events on the schedule\n"
                "• **🔄 Hide Daily Reset** - Exclude Daily Reset from the schedule to reduce clutter\n\n"
                "**Current Settings:**\n"
                f"• Max Events: {view.max_events}\n"
                f"• Timezone: {view.timezone}\n"
                f"• User Timezone: {'Yes' if view.use_user_timezone else 'No'}\n"
                f"• Show Disabled: {'Yes' if view.show_disabled else 'No'}\n"
                f"• Pin Message: {'Yes' if view.auto_pin else 'No'}\n"
                f"• Show Repeating: {'Yes' if view.show_repeating_events else 'No'}\n"
                f"• Hide Daily Reset: {'Yes' if view.hide_daily_reset else 'No'}\n\n"
                "Use the buttons below to adjust settings, then click **Create Board**."
            ),
            color=discord.Color.blue()
        )
        await interaction.response.edit_message(embed=embed, view=view)

class CreateBoardSettingsView(discord.ui.View):
    """Step 3: Configure board settings with buttons"""
    def __init__(self, cog, guild_id: int, board_type: str, target_channel_id: int,
                 display_channel_id: int, creator_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.board_type = board_type
        self.target_channel_id = target_channel_id
        self.display_channel_id = display_channel_id
        self.creator_id = creator_id

        # Default settings
        self.max_events = 15
        self.timezone = "UTC"
        self.show_disabled = False
        self.auto_pin = True
        self.show_repeating_events = True
        self.use_user_timezone = False
        self.hide_daily_reset = True

    @discord.ui.button(label="Max Events (15)", emoji="🔢", style=discord.ButtonStyle.secondary, row=0)
    async def max_events_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            parent_view = self

            class MaxEventsModal(discord.ui.Modal, title="Max Events to Show"):
                def __init__(self, parent):
                    super().__init__()
                    self.parent = parent

                    self.max_events_input = discord.ui.TextInput(
                        label="Max Events (1-100)",
                        placeholder="Enter a number between 1 and 100",
                        default=str(parent.max_events),
                        max_length=3,
                        required=True
                    )
                    self.add_item(self.max_events_input)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    try:
                        value = int(self.max_events_input.value.strip())
                        if value < 1 or value > 100:
                            await modal_interaction.response.send_message(
                                "❌ Max events must be between 1 and 100!",
                                ephemeral=True
                            )
                            return

                        self.parent.max_events = value
                        self.parent.max_events_button.label = f"Max Events ({value})"
                        await self.parent.refresh_embed(modal_interaction)

                    except ValueError:
                        await modal_interaction.response.send_message(
                            "❌ Please enter a valid number!",
                            ephemeral=True
                        )

            modal = MaxEventsModal(parent_view)
            await interaction.response.send_modal(modal)

        except Exception as e:
            print(f"[ERROR] Error in max events button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Timezone (UTC)", emoji="🌍", style=discord.ButtonStyle.secondary, row=0)
    async def timezone_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            parent_view = self

            class TimezoneModal(discord.ui.Modal, title="Set Timezone"):
                def __init__(self, parent):
                    super().__init__()
                    self.parent = parent

                    # Get display timezone or fallback to stored timezone
                    current_tz = getattr(parent, 'timezone_display', parent.timezone)

                    self.timezone_input = discord.ui.TextInput(
                        label="Timezone (UTC±X or UTC±H:MM)",
                        placeholder="e.g., UTC+3, UTC-5, UTC+5:30",
                        default=current_tz,
                        max_length=12,
                        required=True
                    )
                    self.add_item(self.timezone_input)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    try:
                        tz_input = self.timezone_input.value.strip()

                        # Convert UTC+X or UTC-X to appropriate timezone format (supports decimals like UTC+5.5)
                        if tz_input.upper() == "UTC":
                            tz_name = "UTC"
                            display_name = "UTC"
                        elif tz_input.upper().startswith("UTC+") or tz_input.upper().startswith("UTC-"):
                            # Extract offset (supports both decimal like 5.5 and HH:MM like 5:30)
                            offset_str = tz_input[3:]  # Remove "UTC"

                            # Parse offset - support both formats
                            if ':' in offset_str:
                                # HH:MM format (e.g., "5:30", "-5:45")
                                parts = offset_str.split(':')
                                if len(parts) != 2:
                                    await modal_interaction.response.send_message(
                                        "❌ Invalid time format! Use HH:MM (e.g., 5:30)",
                                        ephemeral=True
                                    )
                                    return
                                try:
                                    hours = int(parts[0])
                                    minutes = int(parts[1])
                                    if minutes < 0 or minutes >= 60:
                                        await modal_interaction.response.send_message(
                                            "❌ Minutes must be between 0 and 59!",
                                            ephemeral=True
                                        )
                                        return
                                    # Convert to decimal (preserve sign from hours)
                                    offset = hours + (minutes / 60.0 if hours >= 0 else -minutes / 60.0)
                                except ValueError:
                                    await modal_interaction.response.send_message(
                                        "❌ Invalid time format! Use HH:MM (e.g., 5:30)",
                                        ephemeral=True
                                    )
                                    return
                            else:
                                # Decimal format (e.g., "5.5", "-5.75")
                                try:
                                    offset = float(offset_str)
                                except ValueError:
                                    await modal_interaction.response.send_message(
                                        "❌ Invalid offset! Use decimal (5.5) or HH:MM (5:30) format",
                                        ephemeral=True
                                    )
                                    return

                            # Validate offset range
                            if offset < -12 or offset > 14:
                                await modal_interaction.response.send_message(
                                    "❌ Timezone offset must be between UTC-12 and UTC+14!",
                                    ephemeral=True
                                )
                                return

                            # Check if it's a whole hour or fractional
                            if offset == int(offset):
                                # Whole hour - use Etc/GMT zones (inverted)
                                inverted_offset = -int(offset)
                                if inverted_offset == 0:
                                    tz_name = "UTC"
                                else:
                                    tz_name = f"Etc/GMT{inverted_offset:+d}"
                            else:
                                # Fractional offset (e.g., 5.5 for India, 9.5 for Australia)
                                # Store in standard UTC+HH:MM format
                                sign = "+" if offset >= 0 else "-"
                                abs_offset = abs(offset)
                                hours = int(abs_offset)
                                minutes = int((abs_offset - hours) * 60)
                                tz_name = f"UTC{sign}{hours:02d}:{minutes:02d}"

                            display_name = tz_input.upper()
                        else:
                            await modal_interaction.response.send_message(
                                "❌ Invalid timezone format! Use UTC, UTC+3, UTC-5, UTC+5.5, etc.",
                                ephemeral=True
                            )
                            return

                        # Validate the timezone (for Etc/GMT zones)
                        if tz_name.startswith("Etc/GMT"):
                            try:
                                _ = pytz.timezone(tz_name)
                            except:
                                await modal_interaction.response.send_message(
                                    "❌ Invalid timezone!",
                                    ephemeral=True
                                )
                                return

                        self.parent.timezone = tz_name
                        self.parent.timezone_display = display_name
                        self.parent.timezone_button.label = f"Timezone ({display_name})"
                        await self.parent.refresh_embed(modal_interaction)

                    except Exception as e:
                        await modal_interaction.response.send_message(
                            f"❌ Invalid timezone: {str(e)}",
                            ephemeral=True
                        )

            modal = TimezoneModal(parent_view)
            await interaction.response.send_modal(modal)

        except Exception as e:
            print(f"[ERROR] Error in timezone button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="User Timezone: No", emoji="🌐", style=discord.ButtonStyle.secondary, row=0)
    async def use_user_timezone_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.use_user_timezone = not self.use_user_timezone
            button.label = f"User Timezone: {'Yes' if self.use_user_timezone else 'No'}"
            button.style = discord.ButtonStyle.primary if self.use_user_timezone else discord.ButtonStyle.secondary
            # Disable timezone button when user timezone is enabled
            self.timezone_button.disabled = self.use_user_timezone
            await self.refresh_embed(interaction)
        except Exception as e:
            print(f"[ERROR] Error in use user timezone button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Show Disabled: No", emoji="👁️", style=discord.ButtonStyle.secondary, row=1)
    async def show_disabled_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.show_disabled = not self.show_disabled
            button.label = f"Show Disabled: {'Yes' if self.show_disabled else 'No'}"
            button.style = discord.ButtonStyle.primary if self.show_disabled else discord.ButtonStyle.secondary
            await self.refresh_embed(interaction)
        except Exception as e:
            print(f"[ERROR] Error in show disabled button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Pin Message: Yes", emoji="📌", style=discord.ButtonStyle.primary, row=0)
    async def auto_pin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.auto_pin = not self.auto_pin
            button.label = f"Pin Message: {'Yes' if self.auto_pin else 'No'}"
            button.style = discord.ButtonStyle.primary if self.auto_pin else discord.ButtonStyle.secondary
            await self.refresh_embed(interaction)
        except Exception as e:
            print(f"[ERROR] Error in auto pin button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Show Repeating: Yes", emoji="🔄", style=discord.ButtonStyle.primary, row=1)
    async def show_repeating_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.show_repeating_events = not self.show_repeating_events
            button.label = f"Show Repeating: {'Yes' if self.show_repeating_events else 'No'}"
            button.style = discord.ButtonStyle.primary if self.show_repeating_events else discord.ButtonStyle.secondary
            await self.refresh_embed(interaction)
        except Exception as e:
            print(f"[ERROR] Error in show repeating button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Hide Daily Reset: Yes", emoji="🔄", style=discord.ButtonStyle.primary, row=1)
    async def hide_daily_reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.hide_daily_reset = not self.hide_daily_reset
            button.label = f"Hide Daily Reset: {'Yes' if self.hide_daily_reset else 'No'}"
            button.style = discord.ButtonStyle.primary if self.hide_daily_reset else discord.ButtonStyle.secondary
            await self.refresh_embed(interaction)
        except Exception as e:
            print(f"[ERROR] Error in hide daily reset button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Create Board", emoji="✅", style=discord.ButtonStyle.success, row=2)
    async def create_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Create settings dict
            settings = {
                'max_events': self.max_events,
                'timezone': self.timezone,
                'show_disabled': self.show_disabled,
                'auto_pin': self.auto_pin,
                'show_repeating_events': self.show_repeating_events,
                'use_user_timezone': self.use_user_timezone,
                'hide_daily_reset': self.hide_daily_reset
            }

            # Defer while creating board
            await interaction.response.defer()

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

            # Edit the existing message
            target_info = f"<#{self.target_channel_id}>" if self.board_type == "channel" else "all channels"
            timezone_display = getattr(self, 'timezone_display', 'UTC')

            success_embed = discord.Embed(
                title="✅ Schedule Board Created!",
                description=(
                    f"**Type:** {self.board_type.capitalize()}\n"
                    f"**Tracking:** {target_info}\n"
                    f"**Posted in:** <#{self.display_channel_id}>\n"
                    f"**Board ID:** {board_id}\n\n"
                    f"**Settings:**\n"
                    f"• Max Events: {self.max_events}\n"
                    f"• Timezone: {timezone_display}\n"
                    f"• Show Disabled: {'Yes' if self.show_disabled else 'No'}\n"
                    f"• Pin Message: {'Yes' if self.auto_pin else 'No'}"
                ),
                color=discord.Color.green()
            )

            # Create a view with back button
            success_view = BoardCreatedSuccessView(self.cog, self.guild_id)
            await interaction.edit_original_response(embed=success_embed, view=success_view)

        except Exception as e:
            print(f"[ERROR] Error creating board: {e}")
            traceback.print_exc()
            await interaction.followup.send("❌ An error occurred!", ephemeral=True)

    @discord.ui.button(label="Cancel", emoji="❌", style=discord.ButtonStyle.danger, row=2)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_main_menu(interaction)
        except Exception as e:
            print(f"[ERROR] Error in cancel button: {e}")
            traceback.print_exc()

    async def refresh_embed(self, interaction: discord.Interaction):
        """Refresh the embed to show updated settings"""
        try:
            target_info = f"<#{self.target_channel_id}>" if self.board_type == "channel" else "all channels"
            timezone_display = getattr(self, 'timezone_display', 'UTC')

            embed = discord.Embed(
                title="📅 Create Schedule Board - Step 3",
                description=(
                    f"**Board Type:** {self.board_type.capitalize()}\n"
                    f"**Tracking:** {target_info}\n"
                    f"**Posted in:** <#{self.display_channel_id}>\n\n"
                    "**Button Functions:**\n"
                    "• **🔢 Max Events** - Set maximum number of events displayed on the schedule\n"
                    "• **🌍 Timezone** - Select timezone for displaying event times\n"
                    "• **🌐 User Timezone** - Show times in each user's local timezone\n"
                    "• **👁️ Show Disabled** - Include or exclude disabled notifications from the schedule\n"
                    "• **📌 Pin Message** - Automatically pin the schedule board message in the channel\n"
                    "• **🔄 Show Repeating** - Show or hide future repetitions of events on the schedule\n"
                    "• **🔄 Hide Daily Reset** - Exclude Daily Reset from the schedule to reduce clutter\n\n"
                    "**Current Settings:**\n"
                    f"• Max Events: {self.max_events}\n"
                    f"• Timezone: {timezone_display}\n"
                    f"• User Timezone: {'Yes' if self.use_user_timezone else 'No'}\n"
                    f"• Show Disabled: {'Yes' if self.show_disabled else 'No'}\n"
                    f"• Pin Message: {'Yes' if self.auto_pin else 'No'}\n"
                    f"• Show Repeating: {'Yes' if self.show_repeating_events else 'No'}\n"
                    f"• Hide Daily Reset: {'Yes' if self.hide_daily_reset else 'No'}\n\n"
                    "Use the buttons below to adjust settings, then click **Create Board**."
                ),
                color=discord.Color.blue()
            )
            await interaction.response.edit_message(embed=embed, view=self)
        except Exception as e:
            print(f"[ERROR] Error refreshing embed: {e}")
            traceback.print_exc()

class BoardCreatedSuccessView(discord.ui.View):
    """View shown after successfully creating a board"""
    def __init__(self, cog, guild_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id

    @discord.ui.button(label="Back to Menu", emoji="🏠", style=discord.ButtonStyle.primary, row=0)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.show_main_menu(interaction)
        except Exception as e:
            print(f"[ERROR] Error returning to menu: {e}")
            traceback.print_exc()

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
            label="Pin Message? (yes/no)",
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
                if max_events < 1 or max_events > 100:
                    raise ValueError("Max events must be between 1 and 100")
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid max events! Please enter a number between 1 and 100.",
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
    def __init__(self, cog, guild_id: int, boards: list, guild: discord.Guild = None):
        super().__init__(timeout=None)
        self.cog = cog
        self.guild_id = guild_id
        self.boards = boards
        self.guild = guild

        # Create select menu with boards
        if boards:
            options = []
            for board in boards[:25]:  # Discord limit
                board_id, board_type, target_channel_id, display_channel_id = board

                # Get channel names instead of IDs
                if guild:
                    if board_type == "channel" and target_channel_id:
                        target_channel = guild.get_channel(target_channel_id)
                        target_name = f"#{target_channel.name}" if target_channel else f"#unknown-{target_channel_id}"
                    else:
                        target_name = "All Channels"

                    display_channel = guild.get_channel(display_channel_id)
                    display_name = f"#{display_channel.name}" if display_channel else f"#unknown-{display_channel_id}"
                else:
                    # Fallback if guild not provided
                    target_name = f"#{target_channel_id}" if board_type == "channel" else "All Channels"
                    display_name = f"#{display_channel_id}"

                # Create label with channel name
                label = f"{board_type.capitalize()} Board"
                if board_type == "channel":
                    label += f" ({target_name})"

                description = f"Posted in {display_name} | ID: {board_id}"

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

        # Check if this is a per-channel board
        cog.cursor.execute("SELECT board_type FROM notification_schedule_boards WHERE id = ?", (board_id,))
        result = cog.cursor.fetchone()
        self.board_type = result[0] if result else None

        # Hide "Change Tracking" button for server-wide boards
        if self.board_type != "channel":
            for item in self.children:
                if hasattr(item, 'label') and item.label == "Change Tracking":
                    self.remove_item(item)
                    break

    async def create_embed(self) -> discord.Embed:
        """Creates embed showing board info"""
        try:
            self.cog.cursor.execute("""
                SELECT board_type, target_channel_id, channel_id, max_events,
                       show_disabled, auto_pin, timezone, created_at, show_repeating_events
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
             show_disabled, auto_pin, timezone, created_at, show_repeating_events) = result

            target_info = f"<#{target_channel_id}>" if board_type == "channel" else "All channels"

            embed = discord.Embed(
                title=f"📋 Managing Board #{self.board_id}",
                description=(
                    f"**Type:** {board_type.capitalize()}\n"
                    f"**Tracking:** {target_info}\n"
                    f"**Posted in:** <#{display_channel_id}>\n\n"
                    f"**Settings:**\n"
                    f"• Max Events: {max_events}\n"
                    f"• Timezone: {self.cog._format_timezone_display(timezone)}\n"
                    f"• Show Disabled: {'Yes' if show_disabled else 'No'}\n"
                    f"• Pin Message: {'Yes' if auto_pin else 'No'}\n"
                    f"• Show Repeating: {'Yes' if show_repeating_events else 'No'}\n\n"
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
            view = EditBoardSettingsView(self.cog, self.board_id, self.guild_id)
            embed = await view._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"[ERROR] Error in edit settings: {e}")
            traceback.print_exc()

    async def change_target_channel_callback(self, interaction: discord.Interaction):
        """Callback for changing which channel to monitor (per-channel boards only)"""
        try:
            channel_select = discord.ui.ChannelSelect(
                placeholder="Select channel to monitor events from",
                channel_types=[discord.ChannelType.text],
                min_values=1,
                max_values=1
            )

            async def channel_callback(select_interaction: discord.Interaction):
                await select_interaction.response.defer()
                new_target_channel_id = int(select_interaction.data["values"][0])

                # Update target channel in database
                self.cog.cursor.execute("""
                    UPDATE notification_schedule_boards
                    SET target_channel_id = ?
                    WHERE id = ?
                """, (new_target_channel_id, self.board_id))
                self.cog.conn.commit()

                # Update the board
                await self.cog.update_schedule_board(self.board_id)

                # Refresh the view
                embed = await self.create_embed()
                await select_interaction.edit_original_response(embed=embed, view=self)

            channel_select.callback = channel_callback

            view = discord.ui.View(timeout=60)
            view.add_item(channel_select)

            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="🔄 Change Tracking Channel",
                    description="Select which channel's events this board should display:",
                    color=discord.Color.blue()
                ),
                view=view
            )

        except Exception as e:
            print(f"[ERROR] Error in change tracking channel: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Move Board", emoji="📤", style=discord.ButtonStyle.secondary, row=0)
    async def move_board_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            channel_select = discord.ui.ChannelSelect(
                placeholder="Select new channel to post the board",
                channel_types=[discord.ChannelType.text],
                min_values=1,
                max_values=1
            )

            async def channel_callback(select_interaction: discord.Interaction):
                await select_interaction.response.defer()
                new_channel_id = int(select_interaction.data["values"][0])

                success, error = await self.cog.move_schedule_board(self.board_id, new_channel_id)

                if error:
                    await select_interaction.followup.send(f"❌ Failed to move: {error}", ephemeral=True)
                    return

                # Refresh the board management view (no confirmation message)
                embed = await self.create_embed()
                await select_interaction.edit_original_response(embed=embed, view=self)

            channel_select.callback = channel_callback

            view = discord.ui.View(timeout=60)
            view.add_item(channel_select)

            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="📤 Move Board",
                    description="Select where to post this schedule board:",
                    color=discord.Color.blue()
                ),
                view=view
            )

        except Exception as e:
            print(f"[ERROR] Error in move board: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Change Tracking", emoji="🔄", style=discord.ButtonStyle.secondary, row=0)
    async def change_tracking_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Change which channel to monitor (only for per-channel boards)"""
        # This button is only visible for per-channel boards, hiding is done in __init__
        await self.change_target_channel_callback(interaction)

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

class EditBoardSettingsView(discord.ui.View):
    """Interactive view to edit board settings with buttons"""
    def __init__(self, cog, board_id: int, guild_id: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.board_id = board_id
        self.guild_id = guild_id

        # Load current settings
        self._load_settings()
        self._update_button_labels()
        self._update_button_styles()

    def _load_settings(self):
        """Load current settings from database"""
        self.cog.cursor.execute("""
            SELECT max_events, timezone, show_disabled, auto_pin, show_repeating_events, use_user_timezone, hide_daily_reset
            FROM notification_schedule_boards
            WHERE id = ?
        """, (self.board_id,))
        result = self.cog.cursor.fetchone()

        if result:
            self.max_events, self.timezone, self.show_disabled, self.auto_pin, self.show_repeating_events, self.use_user_timezone, self.hide_daily_reset = result
            # Handle NULL values
            self.use_user_timezone = self.use_user_timezone if self.use_user_timezone is not None else 0
            self.hide_daily_reset = self.hide_daily_reset if self.hide_daily_reset is not None else 1
        else:
            # Defaults if not found
            self.max_events = 15
            self.timezone = "UTC"
            self.show_disabled = 0
            self.auto_pin = 1
            self.show_repeating_events = 1
            self.use_user_timezone = 0
            self.hide_daily_reset = 1

    def _update_button_labels(self):
        """Update button labels to show current values"""
        # Update max events button
        self.max_events_button.label = f"Max Events ({self.max_events})"

        # Update timezone button
        tz_display = self.cog._format_timezone_display(self.timezone)
        self.timezone_button.label = f"Timezone ({tz_display})"

    def _update_button_styles(self):
        """Update toggle button styles based on current settings"""
        # Update show_disabled button
        self.show_disabled_button.label = f"Show Disabled: {'Yes' if self.show_disabled else 'No'}"
        self.show_disabled_button.style = discord.ButtonStyle.primary if self.show_disabled else discord.ButtonStyle.secondary

        # Update auto_pin button
        self.auto_pin_button.label = f"Pin Message: {'Yes' if self.auto_pin else 'No'}"
        self.auto_pin_button.style = discord.ButtonStyle.primary if self.auto_pin else discord.ButtonStyle.secondary

        # Update show_repeating_events button
        self.show_repeating_button.label = f"Show Repeating: {'Yes' if self.show_repeating_events else 'No'}"
        self.show_repeating_button.style = discord.ButtonStyle.primary if self.show_repeating_events else discord.ButtonStyle.secondary

        # Update use_user_timezone button
        self.use_user_timezone_button.label = f"User Timezone: {'Yes' if self.use_user_timezone else 'No'}"
        self.use_user_timezone_button.style = discord.ButtonStyle.primary if self.use_user_timezone else discord.ButtonStyle.secondary

        # Update hide_daily_reset button
        self.hide_daily_reset_button.label = f"Hide Daily Reset: {'Yes' if self.hide_daily_reset else 'No'}"
        self.hide_daily_reset_button.style = discord.ButtonStyle.primary if self.hide_daily_reset else discord.ButtonStyle.secondary

        # Update button visibility based on use_user_timezone
        self.timezone_button.disabled = bool(self.use_user_timezone)

    async def _create_settings_embed(self) -> discord.Embed:
        """Create embed showing current settings"""
        tz_display = self.cog._format_timezone_display(self.timezone)

        # Build timezone description based on use_user_timezone
        if self.use_user_timezone:
            tz_line = f"🌍 **Timezone:** {tz_display} (not used for display)\n└ Event calculations use this timezone"
        else:
            tz_line = f"🌍 **Timezone:** {tz_display}\n└ Times displayed in this timezone"

        embed = discord.Embed(
            title=f"⚙️ Edit Board Settings - Board #{self.board_id}",
            description=(
                "🔢 **Max Events:** {max}\n"
                "└ Maximum number of events to display per page\n\n"
                "{tz_line}\n\n"
                "🌐 **User Timezone:** {user_tz}\n"
                "└ Show times in each user's local timezone\n\n"
                "👁️ **Show Disabled:** {disabled}\n"
                "└ Include disabled events in schedule\n\n"
                "📌 **Pin Message:** {pin}\n"
                "└ Keep this message pinned in channel\n\n"
                "🔄 **Show Repeating:** {repeat}\n"
                "└ Display future occurrences of repeating events\n\n"
                "🔄 **Hide Daily Reset:** {hide_reset}\n"
                "└ Exclude Daily Reset from the schedule to reduce clutter\n\n"
                "Click the buttons below to adjust settings."
            ).format(
                max=self.max_events,
                tz_line=tz_line,
                user_tz='Yes' if self.use_user_timezone else 'No',
                disabled='Yes' if self.show_disabled else 'No',
                pin='Yes' if self.auto_pin else 'No',
                repeat='Yes' if self.show_repeating_events else 'No',
                hide_reset='Yes' if self.hide_daily_reset else 'No'
            ),
            color=discord.Color.blue()
        )
        return embed

    @discord.ui.button(label="Max Events (15)", emoji="🔢", style=discord.ButtonStyle.secondary, row=0)
    async def max_events_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Edit max events through modal"""
        try:
            parent_view = self

            class MaxEventsModal(discord.ui.Modal, title="Edit Max Events"):
                def __init__(self):
                    super().__init__()
                    self.max_events_input = discord.ui.TextInput(
                        label="Max Events to Show (1-100)",
                        default=str(parent_view.max_events),
                        max_length=3,
                        required=True
                    )
                    self.add_item(self.max_events_input)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    try:
                        max_events = int(self.max_events_input.value.strip())
                        if max_events < 1 or max_events > 100:
                            raise ValueError()

                        # Update database
                        parent_view.cog.cursor.execute("""
                            UPDATE notification_schedule_boards
                            SET max_events = ?
                            WHERE id = ?
                        """, (max_events, parent_view.board_id))
                        parent_view.cog.conn.commit()

                        # Update view state
                        parent_view.max_events = max_events
                        parent_view.max_events_button.label = f"Max Events ({max_events})"

                        # Refresh embed
                        embed = await parent_view._create_settings_embed()
                        await modal_interaction.response.edit_message(embed=embed, view=parent_view)

                        # Update the board
                        await parent_view.cog.update_schedule_board(parent_view.board_id)

                    except ValueError:
                        await modal_interaction.response.send_message(
                            "❌ Max events must be a number between 1 and 100!",
                            ephemeral=True
                        )

            await interaction.response.send_modal(MaxEventsModal())

        except Exception as e:
            print(f"[ERROR] Error in max events button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Timezone (UTC)", emoji="🌍", style=discord.ButtonStyle.secondary, row=0)
    async def timezone_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Edit timezone through modal"""
        try:
            parent_view = self

            class TimezoneModal(discord.ui.Modal, title="Edit Timezone"):
                def __init__(self):
                    super().__init__()
                    self.timezone_input = discord.ui.TextInput(
                        label="Timezone (UTC±X or UTC±H:MM)",
                        placeholder="e.g., UTC+3, UTC-5, UTC+5:30",
                        default=parent_view.cog._format_timezone_display(parent_view.timezone),
                        max_length=12,
                        required=True
                    )
                    self.add_item(self.timezone_input)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    try:
                        tz_input = self.timezone_input.value.strip()

                        # Parse timezone (same logic as before)
                        if tz_input.upper() == "UTC":
                            tz_name = "UTC"
                        elif tz_input.upper().startswith("UTC+") or tz_input.upper().startswith("UTC-"):
                            offset_str = tz_input[3:]
                            if ':' in offset_str:
                                parts = offset_str.split(':')
                                hours = int(parts[0])
                                minutes = int(parts[1])
                                offset_hours = hours + (minutes / 60.0) if hours >= 0 else hours - (minutes / 60.0)
                            else:
                                offset_hours = float(offset_str)

                            if offset_hours >= 0:
                                tz_name = f"Etc/GMT-{int(offset_hours)}"
                            else:
                                tz_name = f"Etc/GMT+{int(abs(offset_hours))}"
                        else:
                            raise ValueError("Invalid timezone format")

                        # Validate timezone
                        pytz.timezone(tz_name)

                        # Update database
                        parent_view.cog.cursor.execute("""
                            UPDATE notification_schedule_boards
                            SET timezone = ?
                            WHERE id = ?
                        """, (tz_name, parent_view.board_id))
                        parent_view.cog.conn.commit()

                        # Update view state
                        parent_view.timezone = tz_name
                        tz_display = parent_view.cog._format_timezone_display(tz_name)
                        parent_view.timezone_button.label = f"Timezone ({tz_display})"

                        # Refresh embed
                        embed = await parent_view._create_settings_embed()
                        await modal_interaction.response.edit_message(embed=embed, view=parent_view)

                        # Update the board
                        await parent_view.cog.update_schedule_board(parent_view.board_id)

                    except Exception as e:
                        await modal_interaction.response.send_message(
                            f"❌ Invalid timezone format! Use UTC±X format (e.g., UTC+3, UTC-5).",
                            ephemeral=True
                        )

            await interaction.response.send_modal(TimezoneModal())

        except Exception as e:
            print(f"[ERROR] Error in timezone button: {e}")
            traceback.print_exc()

    @discord.ui.button(label="User Timezone: No", emoji="🌐", style=discord.ButtonStyle.secondary, row=0)
    async def use_user_timezone_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle user timezone setting"""
        try:
            # Toggle value
            self.use_user_timezone = 0 if self.use_user_timezone else 1

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET use_user_timezone = ?
                WHERE id = ?
            """, (self.use_user_timezone, self.board_id))
            self.cog.conn.commit()

            # Update button styles (this will also update timezone button visibility)
            self._update_button_styles()

            # Refresh embed
            embed = await self._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=self)

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

        except Exception as e:
            print(f"[ERROR] Error toggling user timezone: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Show Disabled: No", emoji="👁️", style=discord.ButtonStyle.secondary, row=1)
    async def show_disabled_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle show disabled events"""
        try:
            # Toggle value
            self.show_disabled = 0 if self.show_disabled else 1

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET show_disabled = ?
                WHERE id = ?
            """, (self.show_disabled, self.board_id))
            self.cog.conn.commit()

            # Update button style
            self._update_button_styles()

            # Refresh embed
            embed = await self._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=self)

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

        except Exception as e:
            print(f"[ERROR] Error toggling show disabled: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Pin Message: Yes", emoji="📌", style=discord.ButtonStyle.primary, row=1)
    async def auto_pin_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle pin message"""
        try:
            # Toggle value
            self.auto_pin = 0 if self.auto_pin else 1

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET auto_pin = ?
                WHERE id = ?
            """, (self.auto_pin, self.board_id))
            self.cog.conn.commit()

            # Get the board's message to pin/unpin it
            self.cog.cursor.execute("""
                SELECT channel_id, message_id FROM notification_schedule_boards
                WHERE id = ?
            """, (self.board_id,))
            result = self.cog.cursor.fetchone()

            if result:
                channel_id, message_id = result
                channel = self.cog.bot.get_channel(channel_id)

                if channel:
                    try:
                        message = await channel.fetch_message(message_id)

                        if self.auto_pin:
                            # Pin the message
                            if not message.pinned:
                                await message.pin()
                        else:
                            # Unpin the message
                            if message.pinned:
                                await message.unpin()
                    except discord.Forbidden:
                        # Bot lacks pin/unpin permissions
                        pass
                    except discord.NotFound:
                        # Message not found
                        pass
                    except Exception as e:
                        print(f"[ERROR] Error pinning/unpinning message: {e}")

            # Update button style
            self._update_button_styles()

            # Refresh embed
            embed = await self._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=self)

        except Exception as e:
            print(f"[ERROR] Error toggling pin message: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Show Repeating: Yes", emoji="🔄", style=discord.ButtonStyle.primary, row=1)
    async def show_repeating_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle show repeating events"""
        try:
            # Toggle value
            self.show_repeating_events = 0 if self.show_repeating_events else 1

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET show_repeating_events = ?
                WHERE id = ?
            """, (self.show_repeating_events, self.board_id))
            self.cog.conn.commit()

            # Update button style
            self._update_button_styles()

            # Refresh embed
            embed = await self._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=self)

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

        except Exception as e:
            print(f"[ERROR] Error toggling show repeating: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Hide Daily Reset: Yes", emoji="🔄", style=discord.ButtonStyle.primary, row=2)
    async def hide_daily_reset_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Toggle hide daily reset events"""
        try:
            # Toggle value
            self.hide_daily_reset = 0 if self.hide_daily_reset else 1

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET hide_daily_reset = ?
                WHERE id = ?
            """, (self.hide_daily_reset, self.board_id))
            self.cog.conn.commit()

            # Update button style
            self._update_button_styles()

            # Refresh embed
            embed = await self._create_settings_embed()
            await interaction.response.edit_message(embed=embed, view=self)

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

        except Exception as e:
            print(f"[ERROR] Error toggling hide daily reset: {e}")
            traceback.print_exc()

    @discord.ui.button(label="Done", emoji="✅", style=discord.ButtonStyle.success, row=2)
    async def done_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Return to board management view"""
        try:
            view = BoardManagementView(self.cog, self.guild_id, self.board_id)
            embed = await view.create_embed()
            await interaction.response.edit_message(embed=embed, view=view)
        except Exception as e:
            print(f"[ERROR] Error in done button: {e}")
            traceback.print_exc()

class EditBoardSettingsModal(discord.ui.Modal):
    """Modal to edit board settings"""
    def __init__(self, cog, board_id: int):
        super().__init__(title="Edit Board Settings")
        self.cog = cog
        self.board_id = board_id

        # Load current settings
        cog.cursor.execute("""
            SELECT max_events, timezone, show_disabled, auto_pin, show_repeating_events
            FROM notification_schedule_boards
            WHERE id = ?
        """, (board_id,))
        result = cog.cursor.fetchone()

        if result:
            max_events, timezone, show_disabled, auto_pin, show_repeating_events = result

            self.max_events = discord.ui.TextInput(
                label="Max Events to Show",
                default=str(max_events),
                max_length=3,
                required=False
            )
            self.add_item(self.max_events)

            self.timezone = discord.ui.TextInput(
                label="Timezone (UTC±X or UTC±H:MM)",
                placeholder="e.g., UTC+3, UTC-5, UTC+5:30",
                default=cog._format_timezone_display(timezone),
                max_length=12,
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

            self.show_repeating_events = discord.ui.TextInput(
                label="Show Repeating Events? (yes/no)",
                default="yes" if show_repeating_events else "no",
                max_length=3,
                required=False
            )
            self.add_item(self.show_repeating_events)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse and validate timezone (support UTC+X format including decimals and HH:MM)
            tz_input = self.timezone.value.strip()
            try:
                if tz_input.upper() == "UTC":
                    tz_name = "UTC"
                elif tz_input.upper().startswith("UTC+") or tz_input.upper().startswith("UTC-"):
                    # Parse UTC offset (supports both decimal like 5.5 and HH:MM like 5:30)
                    offset_str = tz_input[3:]

                    # Parse offset - support both formats
                    if ':' in offset_str:
                        # HH:MM format (e.g., "5:30", "-5:45")
                        parts = offset_str.split(':')
                        if len(parts) != 2:
                            await interaction.response.send_message(
                                "❌ Invalid time format! Use HH:MM (e.g., 5:30)",
                                ephemeral=True
                            )
                            return
                        try:
                            hours = int(parts[0])
                            minutes = int(parts[1])
                            if minutes < 0 or minutes >= 60:
                                await interaction.response.send_message(
                                    "❌ Minutes must be between 0 and 59!",
                                    ephemeral=True
                                )
                                return
                            # Convert to decimal (preserve sign from hours)
                            offset = hours + (minutes / 60.0 if hours >= 0 else -minutes / 60.0)
                        except ValueError:
                            await interaction.response.send_message(
                                "❌ Invalid time format! Use HH:MM (e.g., 5:30)",
                                ephemeral=True
                            )
                            return
                    else:
                        # Decimal format (e.g., "5.5", "-5.75")
                        try:
                            offset = float(offset_str)
                        except ValueError:
                            await interaction.response.send_message(
                                "❌ Invalid offset! Use decimal (5.5) or HH:MM (5:30) format",
                                ephemeral=True
                            )
                            return

                    if offset < -12 or offset > 14:
                        await interaction.response.send_message(
                            "❌ Timezone offset must be between UTC-12 and UTC+14!",
                            ephemeral=True
                        )
                        return

                    # Check if it's a whole hour or fractional
                    if offset == int(offset):
                        # Whole hour - use Etc/GMT zones (inverted)
                        inverted_offset = -int(offset)
                        if inverted_offset == 0:
                            tz_name = "UTC"
                        else:
                            tz_name = f"Etc/GMT{inverted_offset:+d}"
                    else:
                        # Fractional offset (e.g., 5.5 for India, 9.5 for Australia)
                        # Store in standard UTC+HH:MM format
                        sign = "+" if offset >= 0 else "-"
                        abs_offset = abs(offset)
                        hours = int(abs_offset)
                        minutes = int((abs_offset - hours) * 60)
                        tz_name = f"UTC{sign}{hours:02d}:{minutes:02d}"
                else:
                    await interaction.response.send_message(
                        "❌ Invalid timezone format! Use UTC, UTC+3, UTC-5, UTC+5.5, etc.",
                        ephemeral=True
                    )
                    return
            except (ValueError, pytz.exceptions.UnknownTimeZoneError) as e:
                await interaction.response.send_message(
                    f"❌ Invalid timezone: {str(e)}",
                    ephemeral=True
                )
                return

            # Validate max events
            try:
                max_events = int(self.max_events.value.strip())
                if max_events < 1 or max_events > 100:
                    raise ValueError()
            except ValueError:
                await interaction.response.send_message(
                    "❌ Max events must be between 1 and 100!",
                    ephemeral=True
                )
                return

            # Parse yes/no
            show_disabled = self.show_disabled.value.strip().lower() in ["yes", "y", "true", "1"]
            show_repeating_events = self.show_repeating_events.value.strip().lower() in ["yes", "y", "true", "1"]

            # Defer the response while we update
            await interaction.response.defer()

            # Update database
            self.cog.cursor.execute("""
                UPDATE notification_schedule_boards
                SET max_events = ?, timezone = ?, show_disabled = ?, show_repeating_events = ?
                WHERE id = ?
            """, (max_events, tz_name, 1 if show_disabled else 0, 1 if show_repeating_events else 0, self.board_id))
            self.cog.conn.commit()

            # Update the board
            await self.cog.update_schedule_board(self.board_id)

            # Refresh the board management view with updated data
            view = BoardManagementView(self.cog, self.cog.cursor.execute(
                "SELECT guild_id FROM notification_schedule_boards WHERE id = ?",
                (self.board_id,)
            ).fetchone()[0], self.board_id)

            embed = await view.create_embed()
            await interaction.edit_original_response(embed=embed, view=view)

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