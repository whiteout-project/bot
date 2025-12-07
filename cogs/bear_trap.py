import discord
from discord.ext import commands
import sqlite3
from datetime import datetime, timedelta
import pytz
import os
import asyncio
import json
import traceback
import time
from .bear_event_types import get_event_types, get_event_icon

class BearTrap(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.db_path = 'db/beartime.sqlite'
        os.makedirs('db', exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        self.cursor = self.conn.cursor()

        # Enable WAL mode for better concurrency with other cogs
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.commit()

        # Rate limiting for channel unavailable warnings
        self.channel_warning_timestamps = {}
        self.channel_warning_interval = 300

        # repeat_minutes value -1 means weekday-based repeat, 0 means no repeat
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS bear_notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                hour INTEGER NOT NULL,
                minute INTEGER NOT NULL,
                timezone TEXT NOT NULL,
                description TEXT NOT NULL,
                notification_type INTEGER NOT NULL,
                mention_type TEXT NOT NULL,
                repeat_enabled INTEGER NOT NULL DEFAULT 0,
                repeat_minutes INTEGER DEFAULT 0,
                is_enabled INTEGER DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_by INTEGER NOT NULL,
                last_notification TIMESTAMP,
                next_notification TIMESTAMP
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS notification_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id INTEGER NOT NULL,
                notification_time INTEGER NOT NULL,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (notification_id) REFERENCES bear_notifications(id) ON DELETE CASCADE
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS bear_notification_embeds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id INTEGER NOT NULL,
                title TEXT,
                description TEXT,
                color INTEGER,
                image_url TEXT,
                thumbnail_url TEXT,
                footer TEXT,
                author TEXT,
                mention_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (notification_id) REFERENCES bear_notifications(id) ON DELETE CASCADE
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS notification_days (
                notification_id INTEGER,
                weekday TEXT,
                FOREIGN KEY (notification_id) REFERENCES bear_notifications(id) ON DELETE CASCADE
            )
        """)

        # Fix corrupted weekday-based repeats: "fixed" string was silently converted to 0 by SQLite.
        self.cursor.execute("""
            UPDATE bear_notifications
            SET repeat_minutes = -1
            WHERE id IN (SELECT DISTINCT notification_id FROM notification_days)
            AND repeat_minutes = 0
        """)
        self.conn.commit()

        try:
            self.cursor.execute("SELECT mention_message FROM bear_notification_embeds LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notification_embeds ADD COLUMN mention_message TEXT")

        try:
            self.cursor.execute("SELECT event_type FROM bear_notifications LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notifications ADD COLUMN event_type TEXT")
        try:
            self.cursor.execute("SELECT wizard_batch_id FROM bear_notifications LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notifications ADD COLUMN wizard_batch_id TEXT")
        try:
            self.cursor.execute("SELECT instance_identifier FROM bear_notifications LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notifications ADD COLUMN instance_identifier TEXT")

        # Message deletion settings
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS bear_trap_settings (
                guild_id INTEGER PRIMARY KEY,
                delete_messages_enabled INTEGER DEFAULT 1,
                default_delete_delay_minutes INTEGER DEFAULT 60,
                show_daily_reset_on_schedule INTEGER DEFAULT 0
            )
        """)

        # Add custom delete delay column to notifications
        try:
            self.cursor.execute("SELECT custom_delete_delay_minutes FROM bear_notifications LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notifications ADD COLUMN custom_delete_delay_minutes INTEGER DEFAULT NULL")

        # Add message tracking columns to notification_history
        try:
            self.cursor.execute("SELECT message_id FROM notification_history LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_history ADD COLUMN message_id BIGINT DEFAULT NULL")

        try:
            self.cursor.execute("SELECT channel_id FROM notification_history LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_history ADD COLUMN channel_id BIGINT DEFAULT NULL")

        try:
            self.cursor.execute("SELECT scheduled_delete_at FROM notification_history LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_history ADD COLUMN scheduled_delete_at TIMESTAMP DEFAULT NULL")

        try:
            self.cursor.execute("SELECT deleted_at FROM notification_history LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE notification_history ADD COLUMN deleted_at TIMESTAMP DEFAULT NULL")

        # Add schedule board settings column
        try:
            self.cursor.execute("SELECT show_daily_reset_on_schedule FROM bear_trap_settings LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_trap_settings ADD COLUMN show_daily_reset_on_schedule INTEGER DEFAULT 0")

        # Initialize default settings for all guilds with notifications
        self.cursor.execute("""
            INSERT OR IGNORE INTO bear_trap_settings (guild_id, delete_messages_enabled, default_delete_delay_minutes, show_daily_reset_on_schedule)
            SELECT DISTINCT guild_id, 1, 60, 0 FROM bear_notifications
        """)

        self.conn.commit()

    async def cog_load(self):

        self.notification_task = asyncio.create_task(self.check_notifications())
        self.deletion_task = asyncio.create_task(self.check_message_deletions())

    async def cog_unload(self):

        if hasattr(self, 'notification_task'):
            self.notification_task.cancel()
        if hasattr(self, 'deletion_task'):
            self.deletion_task.cancel()

    def should_warn_about_channel(self, channel_id: int) -> bool:
        """Check if we should warn about this channel being unavailable."""
        current_time = time.time()
        last_warning = self.channel_warning_timestamps.get(channel_id, 0)
        
        if current_time - last_warning >= self.channel_warning_interval:
            self.channel_warning_timestamps[channel_id] = current_time
            return True
        return False

    async def save_notification(self, guild_id: int, channel_id: int, start_date: datetime,
                                hour: int, minute: int, timezone: str, description: str,
                                created_by: int, notification_type: int, mention_type: str,
                                repeat_enabled: bool, repeat_minutes: int = 0,
                                selected_weekdays: list[int] = None, event_type: str = None, wizard_batch_id: str = None, instance_identifier: str = None, skip_board_update: bool = False) -> int:
        try:
            embed_data = None
            notification_description = description

            if description.startswith("CUSTOM_TIMES:"):
                parts = description.split("|", 1)
                notification_description = description

                if len(parts) > 1 and "EMBED_MESSAGE:" in parts[1]:
                    if hasattr(self, 'current_embed_data'):
                        embed_data = self.current_embed_data
            elif "EMBED_MESSAGE:" in description:
                if hasattr(self, 'current_embed_data'):
                    embed_data = self.current_embed_data
                    title = embed_data.get("title", "true")
                    notification_description = f"EMBED_MESSAGE:{title}"

            tz = pytz.timezone(timezone)
            next_notification = start_date.replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
                tzinfo=tz
            )

            self.cursor.execute("""
                INSERT INTO bear_notifications
                (guild_id, channel_id, hour, minute, timezone, description, notification_type,
                mention_type, repeat_enabled, repeat_minutes, created_by, next_notification, event_type, wizard_batch_id, instance_identifier)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (guild_id, channel_id, hour, minute, timezone, notification_description, notification_type,
                  mention_type, 1 if repeat_enabled else 0, repeat_minutes, created_by,
                  next_notification.isoformat(), event_type, wizard_batch_id, instance_identifier))

            notification_id = self.cursor.lastrowid

            if embed_data:
                await self.save_notification_embed(notification_id, embed_data)
            if repeat_minutes == -1:
                await self.save_notification_fixed(notification_id, selected_weekdays)

            self.conn.commit()

            # Notify schedule boards of new notification (skip if bulk creating)
            if not skip_board_update:
                schedule_cog = self.bot.get_cog("BearTrapSchedule")
                if schedule_cog:
                    await schedule_cog.on_notification_created(guild_id, channel_id)

            return notification_id
        except Exception as e:
            print(f"Error saving notification: {e}")
            raise

    async def update_notification(self, notification_id: int, hour: int, minute: int, timezone: str,
                                  description: str, notification_type: int, mention_type: str,
                                  repeat_minutes: int = 0, selected_weekdays: list[int] = None,
                                  event_type: str = None, embed_data: dict = None,
                                  instance_identifier: str = None, skip_board_update: bool = False) -> bool:
        """Update an existing notification"""
        try:
            notification_description = description
            if description.startswith("CUSTOM_TIMES:"):
                notification_description = description
            elif "EMBED_MESSAGE:" in description:
                title = embed_data.get("title", "true") if embed_data else "true"
                notification_description = f"EMBED_MESSAGE:{title}"
            tz = pytz.timezone(timezone)
            self.cursor.execute("SELECT next_notification FROM bear_notifications WHERE id = ?", (notification_id,))
            row = self.cursor.fetchone()
            if row:
                current_next = datetime.fromisoformat(row[0])
                next_notification = current_next.replace(hour=hour, minute=minute, second=0, microsecond=0)
            else:
                next_notification = datetime.now(tz).replace(hour=hour, minute=minute, second=0, microsecond=0)
            self.cursor.execute("""
                UPDATE bear_notifications
                SET hour = ?, minute = ?, timezone = ?, description = ?, notification_type = ?,
                    mention_type = ?, repeat_minutes = ?, event_type = ?, next_notification = ?,
                    instance_identifier = ?
                WHERE id = ?
            """, (hour, minute, timezone, notification_description, notification_type,
                  mention_type, repeat_minutes, event_type, next_notification.isoformat(),
                  instance_identifier, notification_id))
            if embed_data:
                self.cursor.execute("DELETE FROM bear_notification_embeds WHERE notification_id = ?", (notification_id,))
                await self.save_notification_embed(notification_id, embed_data)
            if repeat_minutes == -1 and selected_weekdays:
                self.cursor.execute("DELETE FROM notification_days WHERE notification_id = ?", (notification_id,))
                await self.save_notification_fixed(notification_id, selected_weekdays)
            self.conn.commit()
            if not skip_board_update:
                schedule_cog = self.bot.get_cog("BearTrapSchedule")
                if schedule_cog:
                    self.cursor.execute("SELECT guild_id, channel_id FROM bear_notifications WHERE id = ?", (notification_id,))
                    row = self.cursor.fetchone()
                    if row:
                        await schedule_cog.on_notification_created(row[0], row[1])
            return True
        except Exception as e:
            print(f"Error updating notification: {e}")
            return False

    async def save_notification_embed(self, notification_id: int, embed_data: dict) -> bool:
        try:
            self.cursor.execute("""
                INSERT INTO bear_notification_embeds 
                (notification_id, title, description, color, image_url, thumbnail_url, footer, author, mention_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                notification_id,
                embed_data.get('title'),
                embed_data.get('description'),
                int(embed_data.get('color', discord.Color.blue().value)),
                embed_data.get('image_url'),
                embed_data.get('thumbnail_url'),
                embed_data.get('footer'),
                embed_data.get('author'),
                embed_data.get('mention_message')
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error saving embed: {e}")
            return False

    async def save_notification_fixed(self, notification_id: int, weekdays: list[int]):
        try:
            if not weekdays:
                raise ValueError("Weekdays list is empty")

            sorted_days = sorted(weekdays)
            weekday = "|".join(str(d) for d in sorted_days)

            self.cursor.execute("""
                INSERT INTO notification_days (notification_id, weekday)
                VALUES (?, ?)
            """, (notification_id, weekday))

            self.conn.commit()
        except Exception as e:
            print(f"Error saving fixed weekdays: {e}")
            raise

    async def get_notification_embed(self, notification_id: int) -> dict:
        try:
            self.cursor.execute("""
                SELECT title, description, color, image_url, thumbnail_url, footer, author, mention_message
                FROM bear_notification_embeds 
                WHERE notification_id = ?
            """, (notification_id,))

            result = self.cursor.fetchone()
            if result:
                return {
                    'title': result[0],
                    'description': result[1],
                    'color': result[2],
                    'image_url': result[3],
                    'thumbnail_url': result[4],
                    'footer': result[5],
                    'author': result[6],
                    'mention_message': result[7]
                }
            return None
        except Exception as e:
            print(f"Error getting embed: {e}")
            return None

    async def check_message_deletions(self):
        """Background task to delete messages when their scheduled time arrives"""
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:
                now = datetime.now(pytz.UTC)

                # Find messages ready to delete
                self.cursor.execute("""
                    SELECT id, message_id, channel_id
                    FROM notification_history
                    WHERE scheduled_delete_at IS NOT NULL
                      AND scheduled_delete_at <= ?
                      AND deleted_at IS NULL
                      AND message_id IS NOT NULL
                """, (now.isoformat(),))

                rows = self.cursor.fetchall()

                for history_id, message_id, channel_id in rows:
                    try:
                        channel = self.bot.get_channel(channel_id)
                        if channel:
                            try:
                                msg = await channel.fetch_message(message_id)
                                await msg.delete()
                            except (discord.NotFound, discord.Forbidden, discord.HTTPException):
                                # Message already deleted, no permission, or other error
                                pass
                    except Exception as e:
                        print(f"Error deleting message {message_id}: {e}")
                    finally:
                        # Mark as deleted regardless
                        self.cursor.execute("""
                            UPDATE notification_history
                            SET deleted_at = ?
                            WHERE id = ?
                        """, (now.isoformat(), history_id))

                self.conn.commit()

            except Exception as e:
                print(f"Error in message deletion checker: {e}")

            await asyncio.sleep(10)  # Check every 10 seconds

    async def check_notifications(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:

                self.cursor.execute("""
                    SELECT id, guild_id, channel_id, hour, minute, timezone, description,
                           notification_type, mention_type, repeat_enabled, repeat_minutes,
                           is_enabled, created_at, created_by, last_notification, next_notification,
                           event_type, instance_identifier, custom_delete_delay_minutes
                    FROM bear_notifications
                    WHERE is_enabled = 1 AND next_notification IS NOT NULL
                """)
                notifications = self.cursor.fetchall()

                now = datetime.now(pytz.UTC)
                for notification in notifications:
                    try:
                        await self.process_notification(notification)
                    except Exception as e:
                        print(f"Error processing notification {notification[0]}: {e}")
                        continue

            except Exception as e:
                print(f"Error in notification checker: {e}")

            await asyncio.sleep(0.1)

    def get_guild_deletion_settings(self, guild_id: int) -> tuple[bool, int]:
        """Get deletion settings for a guild. Returns (enabled, default_delay_minutes)"""
        self.cursor.execute("""
            SELECT delete_messages_enabled, default_delete_delay_minutes
            FROM bear_trap_settings
            WHERE guild_id = ?
        """, (guild_id,))
        row = self.cursor.fetchone()
        if row:
            return (bool(row[0]), row[1])
        # Default if not found
        return (True, 60)

    def calculate_delete_time(self, guild_id: int, event_type: str,
                              custom_delete_delay: int, notification_times: list, current_time: int,
                              sent_at: datetime) -> datetime | None:
        """Calculate when to delete a message. Returns None for non-last notifications."""
        is_last = (current_time == min(notification_times))

        if not is_last:
            # Non-last notifications will be deleted when next one is sent
            return None

        # This is the last notification - calculate delete time
        deletion_enabled, default_delay = self.get_guild_deletion_settings(guild_id)

        if not deletion_enabled:
            return None  # Deletion disabled

        # Determine delay in minutes
        delay_minutes = None

        # 1. Check for per-notification custom delay
        if custom_delete_delay is not None:
            delay_minutes = custom_delete_delay
        # 2. Check for event type duration
        elif event_type:
            from .bear_event_types import get_event_config
            event_config = get_event_config(event_type)
            if event_config:
                event_duration = event_config.get("duration_minutes", default_delay)
                # If event has 0 duration (like Daily Reset), use default delay instead
                delay_minutes = event_duration if event_duration > 0 else default_delay
            else:
                delay_minutes = default_delay
        # 3. Fall back to guild default
        else:
            delay_minutes = default_delay

        return sent_at + timedelta(minutes=delay_minutes)

    async def store_message_for_deletion(self, notification_id: int, notification_time: int,
                                          channel_id: int, message_id: int,
                                          scheduled_delete_at: datetime | None):
        """Store a sent message ID for later deletion"""
        current_time_str = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:%M:%S')
        delete_at_str = scheduled_delete_at.isoformat() if scheduled_delete_at else None

        self.cursor.execute("""
            INSERT INTO notification_history
            (notification_id, notification_time, message_id, channel_id, scheduled_delete_at, sent_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (notification_id, notification_time, message_id, channel_id, delete_at_str, current_time_str))

    async def delete_previous_notifications(self, notification_id: int, channel_id: int):
        """Delete messages from previous notifications (for non-last notifications)"""
        try:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                return

            # Find messages that should be deleted on next notification (scheduled_delete_at IS NULL)
            self.cursor.execute("""
                SELECT id, message_id FROM notification_history
                WHERE notification_id = ?
                  AND scheduled_delete_at IS NULL
                  AND deleted_at IS NULL
                  AND message_id IS NOT NULL
            """, (notification_id,))

            rows = self.cursor.fetchall()
            for history_id, message_id in rows:
                try:
                    msg = await channel.fetch_message(message_id)
                    await msg.delete()
                except (discord.NotFound, discord.Forbidden, discord.HTTPException) as e:
                    # Message already deleted, no permission, or other error
                    pass
                finally:
                    # Mark as deleted regardless
                    self.cursor.execute("""
                        UPDATE notification_history
                        SET deleted_at = ?
                        WHERE id = ?
                    """, (datetime.now(pytz.UTC).isoformat(), history_id))

            self.conn.commit()
        except Exception as e:
            print(f"Error deleting previous notifications: {e}")

    async def process_notification(self, notification):
        id = None  # Initialize to avoid UnboundLocalError in exception handler
        try:
            (id, guild_id, channel_id, hour, minute, timezone, description,
             notification_type, mention_type, repeat_enabled, repeat_minutes,
             is_enabled, created_at, created_by, last_notification,
             next_notification, event_type, instance_identifier, custom_delete_delay_minutes) = notification

            if not is_enabled:
                return

            channel = self.bot.get_channel(channel_id)
            if not channel:
                if self.should_warn_about_channel(channel_id):
                    print(f"Warning: Channel {channel_id} not found for notification {id}.")
                return

            tz = pytz.timezone(timezone)
            now = datetime.now(tz)
            next_time = datetime.fromisoformat(next_notification)

            if next_time < now:
                if repeat_enabled:
                    if isinstance(repeat_minutes, int) and repeat_minutes > 0:
                        # Handle repeated notifications: Move next_time forward by missed intervals
                        time_diff = (now - next_time).total_seconds() / 60
                        periods_passed = int(time_diff / repeat_minutes) + 1
                        next_time = next_time + timedelta(minutes=repeat_minutes * periods_passed)

                    elif repeat_minutes == -1:
                        self.cursor.execute("""
                                    SELECT weekday FROM notification_days
                                    WHERE notification_id = ?
                                """, (id,))
                        rows = self.cursor.fetchall()
                        notification_days = set()

                        for row in rows:
                            parts = row[0].split('|')
                            notification_days.update(int(p) for p in parts if p)

                        for next_day in range(1, 8):
                            potential_day = now + timedelta(days=next_day)
                            if potential_day.weekday() in notification_days:
                                next_time = potential_day.replace(hour=hour, minute=minute, second=0, microsecond=0)
                                break

                elif repeat_minutes == 0:
                    # Handle non-repeating notifications: Keep time, but set date to today
                    next_time = next_time.replace(year=now.year, month=now.month, day=now.day)

                    # If the updated time is still in the past, move it to tomorrow
                    if next_time < now:
                        next_time = next_time + timedelta(days=1)

                self.cursor.execute("""
                    UPDATE bear_notifications 
                    SET next_notification = ? 
                    WHERE id = ?
                """, (next_time.isoformat(), id))
                self.conn.commit()
                return

            time_until = next_time - now
            minutes_until = time_until.total_seconds() / 60

            if time_until.total_seconds() < -0.1:
                return

            notification_times = []

            if notification_type == 1:
                notification_times = [30, 10, 5, 0]
            elif notification_type == 2:
                notification_times = [10, 5, 0]
            elif notification_type == 3:
                notification_times = [5, 0]
            elif notification_type == 4:
                notification_times = [5]
            elif notification_type == 5:
                notification_times = [0]
            elif notification_type == 6:
                if description.startswith("CUSTOM_TIMES:"):
                    times_str = description.split("CUSTOM_TIMES:")[1].split("|")[0]
                    if ',' in times_str:
                        notification_times = [int(t.strip()) for t in times_str.split(',')]
                    else:
                        notification_times = [int(t.strip()) for t in times_str.split('-')]
                    description = description.split("|")[1]

            should_notify = False
            current_time = None

            for notify_time in notification_times:
                time_diff = abs(minutes_until - notify_time)
                if time_diff < 0.1:
                    thirty_seconds_ago = (now - timedelta(seconds=30)).strftime('%Y-%m-%d %H:%M:%S')

                    self.cursor.execute("""
                        SELECT COUNT(*) FROM notification_history 
                        WHERE notification_id = ? 
                        AND notification_time = ? 
                        AND sent_at >= ?
                    """, (id, notify_time, thirty_seconds_ago))

                    count = self.cursor.fetchone()[0]
                    if count == 0:
                        should_notify = True
                        current_time = notify_time
                    break

            if should_notify:
                # Delete previous notifications before sending new ones
                await self.delete_previous_notifications(id, channel_id)

                # Track message IDs for deletion
                sent_message_ids = []

                mention_text = ""
                if mention_type == "everyone":
                    mention_text = "@everyone"
                elif mention_type.startswith("role_"):
                    role_id = int(mention_type.split("_")[1])
                    role = channel.guild.get_role(role_id)
                    if role:
                        mention_text = role.mention
                    else:
                        mention_text = f"Role {role_id}"
                elif mention_type.startswith("member_"):
                    member_id = int(mention_type.split("_")[1])
                    member = await channel.guild.fetch_member(member_id)
                    if member:
                        mention_text = member.mention
                    else:
                        mention_text = f"Member {member_id}"

                rounded_time = round(minutes_until)

                if rounded_time == 1:
                    time_unit = "minute"
                elif rounded_time < 60:
                    time_unit = "minutes"
                elif rounded_time == 60:
                    rounded_time = 1
                    time_unit = "hour"
                elif rounded_time < 1440:
                    rounded_time = round(rounded_time / 60)
                    time_unit = "hours"
                elif rounded_time == 1440:
                    rounded_time = 1
                    time_unit = "day"
                else:
                    rounded_time = round(rounded_time / 1440)
                    time_unit = "days"

                time_text = f"{rounded_time} {time_unit}"

                # Calculate event name, date, and time placeholders
                event_name = event_type if event_type else "Event"

                # Get event emoji from event_types config
                event_emoji = ""
                if event_type:
                    from .bear_event_types import get_event_icon
                    event_emoji = get_event_icon(event_type)

                # Format event time in user's timezone
                event_datetime = next_time.astimezone(tz)
                event_time = event_datetime.strftime("%H:%M")

                # Format event date as "MMM DD" (e.g., "Nov 15")
                event_date = event_datetime.strftime("%b %d")

                # Check if event has instance-specific descriptions
                actual_description = description
                if event_type and instance_identifier and "EMBED_MESSAGE:" not in description:
                    from .bear_event_types import get_event_config
                    event_config = get_event_config(event_type)
                    if event_config and "descriptions" in event_config:
                        descriptions_dict = event_config["descriptions"]
                        # instance_identifier could be "legion1", "legion2", "teleport_window", "battle_start", etc.
                        if instance_identifier in descriptions_dict:
                            actual_description = descriptions_dict[instance_identifier]

                if "EMBED_MESSAGE:" in description:
                    try:
                        embed_data = await self.get_notification_embed(id)

                        if embed_data:
                            try:
                                embed = discord.Embed()

                                try:
                                    color_value = embed_data.get("color")
                                    if color_value is not None and str(color_value).strip() != '':
                                        embed.color = int(color_value)
                                    else:
                                        embed.color = discord.Color.blue()
                                except (ValueError, TypeError):
                                    embed.color = discord.Color.blue()

                                title = embed_data.get("title", "")
                                if title and isinstance(title, str):
                                    title = title.replace("%t", time_text)
                                    title = title.replace("{time}", time_text)
                                    title = title.replace("%n", event_name)
                                    title = title.replace("%e", event_time)
                                    title = title.replace("%d", event_date)
                                    title = title.replace("%i", event_emoji)
                                    if "@tag" in title:
                                        title = title.replace("@tag", mention_text)
                                    embed.title = title

                                description = embed_data.get("description", "")

                                # Override with instance-specific description if available
                                if event_type and instance_identifier:
                                    from .bear_event_types import get_event_config
                                    event_config = get_event_config(event_type)
                                    if event_config and "descriptions" in event_config:
                                        descriptions_dict = event_config["descriptions"]
                                        if instance_identifier in descriptions_dict:
                                            description = descriptions_dict[instance_identifier]

                                if description and isinstance(description, str):
                                    description = description.replace("%t", time_text)
                                    description = description.replace("{time}", time_text)
                                    description = description.replace("%n", event_name)
                                    description = description.replace("%e", event_time)
                                    description = description.replace("%d", event_date)
                                    description = description.replace("%i", event_emoji)
                                    if "@tag" in description:
                                        description = description.replace("@tag", mention_text)
                                    embed.description = description

                                image_url = embed_data.get("image_url", "")
                                if image_url and isinstance(image_url,
                                                            str) and image_url.strip() and image_url.startswith(
                                        ('http://', 'https://')):
                                    embed.set_image(url=image_url)

                                thumbnail_url = embed_data.get("thumbnail_url", "")
                                if thumbnail_url and isinstance(thumbnail_url,
                                                                str) and thumbnail_url.strip() and thumbnail_url.startswith(
                                        ('http://', 'https://')):
                                    embed.set_thumbnail(url=thumbnail_url)

                                footer_text = embed_data.get("footer", "")
                                if footer_text and isinstance(footer_text, str):
                                    footer_text = footer_text.replace("%t", time_text)
                                    footer_text = footer_text.replace("{time}", time_text)
                                    footer_text = footer_text.replace("%n", event_name)
                                    footer_text = footer_text.replace("%e", event_time)
                                    footer_text = footer_text.replace("%d", event_date)
                                    footer_text = footer_text.replace("%i", event_emoji)
                                    if "@tag" in footer_text:
                                        footer_text = footer_text.replace("@tag", mention_text)
                                    embed.set_footer(text=footer_text)

                                author_text = embed_data.get("author", "")
                                if author_text and isinstance(author_text, str):
                                    author_text = author_text.replace("%t", time_text)
                                    author_text = author_text.replace("{time}", time_text)
                                    author_text = author_text.replace("%n", event_name)
                                    author_text = author_text.replace("%e", event_time)
                                    author_text = author_text.replace("%d", event_date)
                                    author_text = author_text.replace("%i", event_emoji)
                                    if "@tag" in author_text:
                                        author_text = author_text.replace("@tag", mention_text)
                                    embed.set_author(name=author_text)

                                if embed.to_dict():
                                    if mention_text:
                                        mention_message = embed_data.get("mention_message", "")
                                        if mention_message and "@tag" in mention_message:
                                            mention_message = mention_message.replace("@tag", mention_text)
                                            mention_message = mention_message.replace("%t", time_text)
                                            mention_message = mention_message.replace("{time}", time_text)
                                            mention_message = mention_message.replace("%n", event_name)
                                            mention_message = mention_message.replace("%e", event_time)
                                            mention_message = mention_message.replace("%d", event_date)
                                            mention_message = mention_message.replace("%i", event_emoji)
                                            msg = await channel.send(mention_message)
                                            sent_message_ids.append(msg.id)
                                        else:
                                            mention_text = mention_text.replace("%t", time_text)
                                            mention_text = mention_text.replace("{time}", time_text)
                                            msg = await channel.send(mention_text)
                                            sent_message_ids.append(msg.id)
                                    msg = await channel.send(embed=embed)
                                    sent_message_ids.append(msg.id)
                                else:
                                    if rounded_time > 0:
                                        msg = await channel.send(
                                            f"{mention_text} ⏰ **Notification** will start in **{time_text}**!")
                                    else:
                                        msg = await channel.send(f"{mention_text} ⏰ **Notification**")
                                    sent_message_ids.append(msg.id)
                            except Exception as e:
                                print(f"Error creating embed: {e}")
                                if rounded_time > 0:
                                    msg = await channel.send(
                                        f"{mention_text} ⏰ **Error sending embed notification** will start in **{time_text}**!")
                                else:
                                    msg = await channel.send(f"{mention_text} ⏰ **Error sending embed notification**")
                                sent_message_ids.append(msg.id)
                    except Exception as e:
                        print(f"Error creating embed: {e}")
                        if rounded_time > 0:
                            msg = await channel.send(
                                f"{mention_text} ⏰ **Error sending embed notification** will start in **{time_text}**!")
                        else:
                            msg = await channel.send(f"{mention_text} ⏰ **Error sending embed notification**")
                        sent_message_ids.append(msg.id)
                else:
                    actual_description = description
                    if description.startswith("CUSTOM_TIMES:"):
                        parts = description.split("|", 1)
                        if len(parts) > 1:
                            actual_description = parts[1]

                    if actual_description.startswith("PLAIN_MESSAGE:"):
                        actual_description = actual_description.replace("PLAIN_MESSAGE:", "", 1)

                    if "@tag" in actual_description or "%t" in actual_description or "{time}" in actual_description or "%n" in actual_description or "%e" in actual_description or "%d" in actual_description or "%i" in actual_description:
                        message = actual_description
                        if "@tag" in message:
                            message = message.replace("@tag", mention_text)
                        if "%t" in message:
                            message = message.replace("%t", time_text)
                        if "{time}" in message:
                            message = message.replace("{time}", time_text)
                        if "%n" in message:
                            message = message.replace("%n", event_name)
                        if "%e" in message:
                            message = message.replace("%e", event_time)
                        if "%d" in message:
                            message = message.replace("%d", event_date)
                        if "%i" in message:
                            message = message.replace("%i", event_emoji)
                        msg = await channel.send(message)
                        sent_message_ids.append(msg.id)
                    else:
                        if rounded_time > 0:
                            msg = await channel.send(
                                f"{mention_text} ⏰ **{actual_description}** will start in **{time_text}**!")
                        else:
                            msg = await channel.send(f"{mention_text} ⏰ **{actual_description}**")
                        sent_message_ids.append(msg.id)

                # Calculate when to delete messages
                scheduled_delete_at = self.calculate_delete_time(
                    guild_id, event_type, custom_delete_delay_minutes,
                    notification_times, current_time, now
                )

                # Store each sent message for deletion
                for message_id in sent_message_ids:
                    await self.store_message_for_deletion(
                        id, current_time, channel_id, message_id, scheduled_delete_at
                    )

                self.cursor.execute("""
                    UPDATE bear_notifications 
                    SET last_notification = ? 
                    WHERE id = ?
                """, (now.isoformat(), id))

                # Handle notification disabling for non-repeating notifications
                should_disable = False
                if not repeat_enabled and current_time == min(notification_times):
                    should_disable = True
                elif rounded_time == 0 and not repeat_enabled:
                    should_disable = True

                if should_disable:
                    # Build informative message
                    event_display = event_type if event_type else "Custom"
                    time_str = f"{hour:02d}:{minute:02d}"
                    desc_preview = description[:50] + "..." if len(description) > 50 else description
                    if "EMBED_MESSAGE:" in desc_preview:
                        desc_preview = "Embed notification"
                    elif desc_preview.startswith("CUSTOM_TIMES:"):
                        parts = desc_preview.split("|", 1)
                        if len(parts) > 1:
                            desc_preview = parts[1][:50]

                    print(f"[INFO] Notification {id} - {event_display} {time_str} ({desc_preview}) was disabled since it is not set to repeat")

                    self.cursor.execute("""
                        UPDATE bear_notifications
                        SET is_enabled = 0
                        WHERE id = ?
                    """, (id,))

                elif rounded_time == 0 and repeat_enabled:
                    if isinstance(repeat_minutes, int) and repeat_minutes > 0:
                        current_next = datetime.fromisoformat(next_notification)
                        next_time = current_next + timedelta(minutes=repeat_minutes)

                    elif repeat_minutes == -1:
                        self.cursor.execute("""
                                    SELECT weekday FROM notification_days
                                    WHERE notification_id = ?
                                """, (id,))
                        rows = self.cursor.fetchall()
                        notification_days = set()

                        for row in rows:
                            parts = row[0].split('|')
                            notification_days.update(int(p) for p in parts if p)

                        for next_day in range(1, 8):
                            potential_day = now + timedelta(days=next_day)
                            if potential_day.weekday() in notification_days:
                                next_time = potential_day.replace(hour=hour, minute=minute, second=0, microsecond=0)
                                break

                    self.cursor.execute("""
                        UPDATE bear_notifications
                        SET next_notification = ?
                        WHERE id = ?
                    """, (next_time.isoformat(), id))

                self.conn.commit()

                # Notify schedule boards after sending notification
                schedule_cog = self.bot.get_cog("BearTrapSchedule")
                if schedule_cog:
                    await schedule_cog.on_notification_sent(guild_id, channel_id)

        except Exception as e:
            notif_id = id if id is not None else "unknown"
            error_msg = f"[ERROR] Error processing notification {notif_id}: {str(e)}\nType: {type(e)}\nTrace: {traceback.format_exc()}"
            print(error_msg)

    async def get_notifications(self, guild_id: int) -> list:
        try:
            self.cursor.execute("""
                SELECT id, guild_id, channel_id, hour, minute, timezone, description,
                       notification_type, mention_type, repeat_enabled, repeat_minutes,
                       is_enabled, created_at, created_by, last_notification, next_notification, event_type, custom_delete_delay_minutes
                FROM bear_notifications
                WHERE guild_id = ?
                ORDER BY
                    CASE
                        WHEN next_notification >= CURRENT_TIMESTAMP THEN 0
                        ELSE 1
                    END,
                    next_notification
            """, (guild_id,))
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error getting notifications: {e}")
            return []

    async def delete_notification(self, notification_id):
        try:
            # Ensure we're using the same connection as toggle_notification
            self.cursor.execute("""SELECT id, guild_id, channel_id FROM bear_notifications WHERE id = ?""", (notification_id,))
            result = self.cursor.fetchone()
            if not result:
                return False  # If the notification doesn't exist, return False

            notif_id, guild_id, channel_id = result

            # If the notification exists, proceed to delete
            self.cursor.execute("""DELETE FROM bear_notifications WHERE id = ?""", (notification_id,))
            self.conn.commit()  # Commit the changes using the same connection as toggle_notification

            # Notify schedule boards of deletion
            schedule_cog = self.bot.get_cog("BearTrapSchedule")
            if schedule_cog:
                await schedule_cog.on_notification_deleted(guild_id, channel_id)

            return True
        except Exception as e:
            print(f"[ERROR] Error deleting notification {notification_id}: {e}")
            return False

    def get_wizard_notifications_for_channel(self, guild_id: int, channel_id: int) -> dict:
        """Get all wizard-created notifications for a channel, mapped by event type"""
        try:
            wizard_batch_id = f"wizard_{guild_id}_{channel_id}"
            self.cursor.execute("""
                SELECT id, event_type, hour, minute, timezone, notification_type, mention_type,
                       repeat_minutes, description
                FROM bear_notifications
                WHERE guild_id = ? AND channel_id = ? AND wizard_batch_id = ?
            """, (guild_id, channel_id, wizard_batch_id))
            notifications = {}
            for row in self.cursor.fetchall():
                if row[1]:
                    notifications[row[1]] = {
                        "id": row[0],
                        "event_type": row[1],
                        "hour": row[2],
                        "minute": row[3],
                        "timezone": row[4],
                        "notification_type": row[5],
                        "mention_type": row[6],
                        "repeat_minutes": row[7],
                        "description": row[8]
                    }
            return notifications
        except Exception as e:
            print(f"Error getting wizard notifications: {e}")
            return {}

    def get_all_wizard_notifications_for_channel(self, guild_id: int, channel_id: int) -> list:
        """Get ALL wizard-created notifications for a channel as a list (includes all instances)"""
        try:
            wizard_batch_id = f"wizard_{guild_id}_{channel_id}"
            self.cursor.execute("""
                SELECT id, event_type, hour, minute, timezone, notification_type, mention_type,
                       repeat_minutes, description, instance_identifier, is_enabled
                FROM bear_notifications
                WHERE guild_id = ? AND channel_id = ? AND wizard_batch_id = ?
            """, (guild_id, channel_id, wizard_batch_id))
            notifications = []
            for row in self.cursor.fetchall():
                notifications.append({
                    "id": row[0],
                    "event_type": row[1],
                    "hour": row[2],
                    "minute": row[3],
                    "timezone": row[4],
                    "notification_type": row[5],
                    "mention_type": row[6],
                    "repeat_minutes": row[7],
                    "description": row[8],
                    "instance_identifier": row[9],
                    "is_enabled": row[10]
                })
            return notifications
        except Exception as e:
            print(f"Error getting all wizard notifications: {e}")
            return []

    def delete_wizard_notifications_for_channel(self, guild_id: int, channel_id: int, event_types_to_keep: list = None) -> int:
        """Delete wizard notifications that are no longer needed"""
        try:
            wizard_batch_id = f"wizard_{guild_id}_{channel_id}"
            if event_types_to_keep:
                placeholders = ",".join(["?"] * len(event_types_to_keep))
                self.cursor.execute(f"""
                    DELETE FROM bear_notifications
                    WHERE guild_id = ? AND channel_id = ? AND wizard_batch_id = ?
                    AND event_type NOT IN ({placeholders})
                """, (guild_id, channel_id, wizard_batch_id, *event_types_to_keep))
            else:
                self.cursor.execute("""
                    DELETE FROM bear_notifications
                    WHERE guild_id = ? AND channel_id = ? AND wizard_batch_id = ?
                """, (guild_id, channel_id, wizard_batch_id))
            deleted_count = self.cursor.rowcount
            self.conn.commit()
            return deleted_count
        except Exception as e:
            print(f"Error deleting wizard notifications: {e}")
            return 0

    async def toggle_notification(self, notification_id: int, enabled: bool, skip_board_update: bool = False) -> bool:
        try:

            self.cursor.execute("""
                SELECT is_enabled, guild_id, channel_id FROM bear_notifications WHERE id = ?
            """, (notification_id,))
            result = self.cursor.fetchone()
            if not result:
                return False

            old_enabled, guild_id, channel_id = result

            self.cursor.execute("""
                UPDATE bear_notifications
                SET is_enabled = ?
                WHERE id = ?
            """, (1 if enabled else 0, notification_id))
            self.conn.commit()

            # Notify schedule boards of toggle
            if not skip_board_update:
                schedule_cog = self.bot.get_cog("BearTrapSchedule")
                if schedule_cog:
                    await schedule_cog.on_notification_toggled(guild_id, channel_id)

            return True
        except Exception as e:
            print(f"Error toggling notification: {e}")
            return False

    async def show_bear_trap_menu(self, interaction: discord.Interaction):
        try:
            embed = discord.Embed(
                title="🔔 Notification System",
                description=(
                    "The Notification System can be used to create notifications that will alert players of upcoming events. "
                    "It is fully customizable and can be used for any type of event. Use the buttons below to get started.\n\n"
                    "**Available Operations**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "🧙 **Setup Wizard**\n"
                    "└ Quick and easy setup for all common event notifications\n"
                    "└ The Wizard will guide you step-by-step through the process\n"
                    "└ Re-run the wizard on a channel to update existing notifications there\n\n"
                    "⏰ **Custom Notification**\n"
                    "└ Set up a new notification with custom time, message, mentions, and repeat options\n"
                    "└ Supports both plain messages and rich embeds\n"
                    "└ Perfect for any events not covered by the Wizard\n\n"
                    "📋 **Manage Notifications**\n"
                    "└ View all existing notifications\n"
                    "└ Edit, enable/disable or delete them\n\n"
                    "📅 **Schedule Boards**\n"
                    "└ Create live schedule boards that display upcoming notifications\n"
                    "└ Auto-updates when notifications are created, edited, or deleted\n"
                    "└ Supports server-wide or per-channel boards with customizable settings\n\n"
                    "📚 **Event Templates**\n"
                    "└ Browse pre-built event templates\n"
                    "└ View and modify default notification designs\n\n"
                    "⚙️ **Settings**\n"
                    "└ Configure whether posted notifications are auto-deleted\n"
                    "└ Set the default time after which notifications are deleted\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━"
                ),
                color=discord.Color.gold()
            )

            embed.set_footer(text="Last Updated")
            embed.timestamp = datetime.now()

            view = BearTrapView(self)

            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                pass

        except Exception as e:
            print(f"Error in show_bear_trap_menu: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred. Please try again.",
                    ephemeral=True
                )

    async def check_admin(self, interaction: discord.Interaction) -> bool:
        try:
            conn = sqlite3.connect('db/settings.sqlite')
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM admin WHERE id = ?", (interaction.user.id,))
            is_admin = cursor.fetchone() is not None
            conn.close()

            if not is_admin:
                await interaction.response.send_message("❌ You don't have permission to use this command!",
                                                        ephemeral=True)
                return False
            return True
        except Exception as e:
            print(f"Error in admin check: {e}")
            return False

    async def show_channel_selection(self, interaction: discord.Interaction, start_date, hour, minute, timezone,
                                     message_data, channels, event_type=None):
        try:
            embed = discord.Embed(
                title="📢 Select Channel",
                description=(
                    "Choose a channel to send notifications:\n\n"
                    "Select a text channel from the dropdown menu below.\n"
                    "Make sure the bot has permission to send messages in the selected channel."
                ),
                color=discord.Color.blue()
            )

            view = ChannelSelectView(
                self,
                start_date,
                hour,
                minute,
                timezone,
                message_data,
                interaction.message,
                event_type=event_type
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in show_channel_selection: {e}")
            await interaction.followup.send(
                "❌ An error occurred while showing channel selection!",
                ephemeral=True
            )

class RepeatOptionView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, description, channel_id, notification_type,
                 mention_type, original_message, event_type=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.description = description
        self.channel_id = channel_id
        self.notification_type = notification_type
        self.mention_type = mention_type
        self.original_message = original_message
        self.event_type = event_type

    @discord.ui.button(label="No Repeat", style=discord.ButtonStyle.danger, custom_id="no_repeat")
    async def no_repeat_button(self, interaction, button):
        await self.save_notification(interaction, False)

    @discord.ui.button(label="Custom Interval", style=discord.ButtonStyle.primary, custom_id="custom_interval")
    async def custom_interval_button(self, interaction, button):
        modal = RepeatIntervalModal(self)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Specific days", style=discord.ButtonStyle.primary, custom_id="fixed_days")
    async def fixed_days_button(self, interaction, button):
        view = DaysMenu(self)
        await interaction.response.edit_message(content="🗓️ Select the days you'd like to get notifications on:", view=view)

    async def save_notification(self, interaction, repeat, repeat_minutes=0, interval_text=None, selected_weekdays=None):
        try:
            notification_id = await self.cog.save_notification(
                guild_id=interaction.guild_id,
                channel_id=self.channel_id,
                start_date=self.start_date,
                hour=self.hour,
                minute=self.minute,
                timezone=self.timezone,
                description=self.description,
                created_by=interaction.user.id,
                notification_type=self.notification_type,
                mention_type=self.mention_type,
                repeat_enabled=repeat,
                repeat_minutes=repeat_minutes,
                selected_weekdays=selected_weekdays,
                event_type=self.event_type
            )

            notification_types = {
                1: "Sends notifications at 30 minutes, 10 minutes, 5 minutes before and when time's up",
                2: "Sends notifications at 10 minutes, 5 minutes before and when time's up",
                3: "Sends notifications at 5 minutes before and when time's up",
                4: "Sends notification only 5 minutes before",
                5: "Sends notification only when time's up",
                6: "Sends notifications at custom times"
            }

            if self.mention_type == "everyone":
                mention_display = "@everyone"
            elif self.mention_type.startswith("role_"):
                role_id = int(self.mention_type.split('_')[1])
                role = interaction.guild.get_role(role_id)
                # Avoid nested f-strings for Python 3.9+ compatibility
                if role:
                    mention_display = f"@{role.name}"
                else:
                    mention_display = f"Role: {role_id}"
            elif self.mention_type.startswith("member_"):
                member_id = int(self.mention_type.split('_')[1])
                member = interaction.guild.get_member(member_id)
                # Avoid nested f-strings for Python 3.9+ compatibility
                if member:
                    mention_display = f"@{member.display_name}"
                else:
                    mention_display = f"Member: {member_id}"
            else:
                mention_display = "No Mention"

            if not repeat:
                repeat_text = "❌ No repeat"
            elif interval_text:
                repeat_text = f"🔄 Repeats every {interval_text}"
            elif repeat_minutes == -1:  # Weekday-based repeat (days stored in notification_days table)
                repeat_text = "🔄 Repeats on selected weekdays"
            else:
                minutes = repeat_minutes
                if minutes == 1:
                    repeat_text = "🔄 Repeats every minute"
                elif minutes == 60:
                    repeat_text = "🔄 Repeats every hour"
                elif minutes == 1440:
                    repeat_text = "🔄 Repeats daily"
                elif minutes == 2880:
                    repeat_text = "🔄 Repeats every 2 days"
                elif minutes == 4320:
                    repeat_text = "🔄 Repeats every 3 days"
                elif minutes == 10080:
                    repeat_text = "🔄 Repeats weekly"
                else:
                    repeat_text = f"🔄 Repeats every {minutes} minutes"

            # Display event type with icon
            if self.event_type:
                event_icon = get_event_icon(self.event_type)
                event_display = f"{event_icon} {self.event_type}"
            else:
                event_display = "📅 Custom"

            embed = discord.Embed(
                title="✅ Notification Set Successfully",
                description=(
                    f"**📅 Date:** {self.start_date.strftime('%d/%m/%Y')}\n"
                    f"**⏰ Time:** {self.hour:02d}:{self.minute:02d} {self.timezone}\n"
                    f"**📢 Channel:** <#{self.channel_id}>\n"
                    f"**🎯 Event Type:** {event_display}\n"
                    f"**📝 Description:** {self.description.split('|')[-1] if '|' in self.description else self.description}\n\n"
                    f"**⚙️ Notification Type**\n{notification_types[self.notification_type]}\n\n"
                    f"**👥 Mentions:** {mention_display}\n"
                    f"**🔄 Repeat:** {repeat_text}"
                ),
                color=discord.Color.green()
            )

            embed.set_footer(text="Created at")
            embed.timestamp = datetime.now()

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=None
            )

        except Exception as e:
            print(f"Error saving notification: {e}")
            await interaction.followup.send(
                "❌ An error occurred while saving the notification.",
                ephemeral=True
            )

class RepeatIntervalModal(discord.ui.Modal):
    def __init__(self, repeat_view: RepeatOptionView):
        super().__init__(title="Set Repeat Interval")
        self.repeat_view = repeat_view

        self.months = discord.ui.TextInput(
            label="Months",
            placeholder="Enter number of months (e.g., 1)",
            min_length=0,
            max_length=2,
            required=False,
            default="0",
            style=discord.TextStyle.short
        )

        self.weeks = discord.ui.TextInput(
            label="Weeks",
            placeholder="Enter number of weeks (e.g., 2)",
            min_length=0,
            max_length=2,
            required=False,
            default="0",
            style=discord.TextStyle.short
        )

        self.days = discord.ui.TextInput(
            label="Days",
            placeholder="Enter number of days (e.g., 3)",
            min_length=0,
            max_length=2,
            required=False,
            default="0",
            style=discord.TextStyle.short
        )

        self.hours = discord.ui.TextInput(
            label="Hours",
            placeholder="Enter number of hours (e.g., 12)",
            min_length=0,
            max_length=2,
            required=False,
            default="0",
            style=discord.TextStyle.short
        )

        self.minutes = discord.ui.TextInput(
            label="Minutes",
            placeholder="Enter number of minutes (e.g., 30)",
            min_length=0,
            max_length=2,
            required=False,
            default="0",
            style=discord.TextStyle.short
        )

        for item in [self.months, self.weeks, self.days, self.hours, self.minutes]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            try:
                months = int(self.months.value)
                weeks = int(self.weeks.value)
                days = int(self.days.value)
                hours = int(self.hours.value)
                minutes = int(self.minutes.value)
            except ValueError:
                await interaction.response.send_message(
                    "❌ Please enter valid numbers for all fields!",
                    ephemeral=True
                )
                return

            if not any([months > 0, weeks > 0, days > 0, hours > 0, minutes > 0]):
                await interaction.response.send_message(
                    "❌ Please enter at least one time interval greater than 0!",
                    ephemeral=True
                )
                return

            total_minutes = (months * 30 * 24 * 60) + (weeks * 7 * 24 * 60) + (days * 24 * 60) + (hours * 60) + minutes

            interval_parts = []
            if months > 0:
                interval_parts.append(f"{months} month{'s' if months > 1 else ''}")
            if weeks > 0:
                interval_parts.append(f"{weeks} week{'s' if weeks > 1 else ''}")
            if days > 0:
                interval_parts.append(f"{days} day{'s' if days > 1 else ''}")
            if hours > 0:
                interval_parts.append(f"{hours} hour{'s' if hours > 1 else ''}")
            if minutes > 0:
                interval_parts.append(f"{minutes} minute{'s' if minutes > 1 else ''}")

            if len(interval_parts) > 1:
                interval_text = ", ".join(interval_parts[:-1]) + " and " + interval_parts[-1]
            else:
                interval_text = interval_parts[0]

            await self.repeat_view.save_notification(interaction, True, total_minutes, interval_text)

        except Exception as e:
            print(f"Error in repeat interval modal: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while setting the repeat interval.",
                ephemeral=True
            )

class DaysMenu(discord.ui.View):
    def __init__(self, repeat_view):
        super().__init__(timeout=300)
        self.repeat_view = repeat_view
        self.selected_days = []

        self.day_select = discord.ui.Select(
            placeholder="Select days of the week",
            min_values=1,
            max_values=7,
            options=[
                discord.SelectOption(label="Monday", value="Monday"),
                discord.SelectOption(label="Tuesday", value="Tuesday"),
                discord.SelectOption(label="Wednesday", value="Wednesday"),
                discord.SelectOption(label="Thursday", value="Thursday"),
                discord.SelectOption(label="Friday", value="Friday"),
                discord.SelectOption(label="Saturday", value="Saturday"),
                discord.SelectOption(label="Sunday", value="Sunday"),
            ],
            custom_id="days_of_week_select"
        )
        self.day_select.callback = self.on_select
        self.add_item(self.day_select)

        self.add_item(ConfirmDaysButton(self))

    async def on_select(self, interaction: discord.Interaction):
        self.selected_days = self.day_select.values
        await interaction.response.defer()

class ConfirmDaysButton(discord.ui.Button):
    def __init__(self, days_menu_view):
        super().__init__(label="Confirm", style=discord.ButtonStyle.success)
        self.days_menu_view = days_menu_view

    async def callback(self, interaction: discord.Interaction):
        days = self.days_menu_view.selected_days
        if not days:
            await interaction.response.send_message("Please select at least one day.", ephemeral=True)
            return

        weekdays_index = {
            "Monday": 0, "Tuesday": 1, "Wednesday": 2,
            "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6
        }
        selected_weekdays = [weekdays_index[d] for d in days]

        repeat_view = self.days_menu_view.repeat_view

        interval_text = "" + ", ".join(days[:-1]) + " and " + days[-1]

        await repeat_view.save_notification(
            interaction,
            repeat=True,
            repeat_minutes=-1,
            interval_text=interval_text,
            selected_weekdays=selected_weekdays
        )

class TextInputModal(discord.ui.Modal):
    def __init__(self, title, label, placeholder, default_value="", max_length=None, style=discord.TextStyle.short):
        super().__init__(title=title)
        self.value = None
        self.input = discord.ui.TextInput(
            label=label,
            placeholder=placeholder,
            default=default_value,
            max_length=max_length,
            style=style
        )
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        self.value = self.input.value
        await interaction.response.defer()

class EmbedEditorView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, original_message, event_type=None):
        super().__init__(timeout=None)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.original_message = original_message
        self.event_type = event_type  # Store event_type for threading through the flow
        self.embed_data = {
            "title": "⏰ Bear Trap",
            "description": "Add a description...",
            "color": discord.Color.blue().value,
            "footer": "Notification System",
            "author": "Bear Trap",
            "mention_message": ""
        }

    async def update_embed(self, interaction: discord.Interaction):
        try:
            # Sample values for preview - use actual event data when available
            example_time = "30 minutes"
            example_name = self.event_type if self.event_type else "Bear Trap"
            example_emoji = get_event_icon(self.event_type) if self.event_type else "🐻"
            example_event_time = f"{self.hour:02d}:{self.minute:02d}"
            example_date = self.start_date.strftime("%b %d") if self.start_date else "Dec 06"

            def replace_variables(text):
                """Replace all notification variables with sample values for preview."""
                return (text
                    .replace("%t", example_time)
                    .replace("{time}", example_time)
                    .replace("%n", example_name)
                    .replace("%e", example_event_time)
                    .replace("%d", example_date)
                    .replace("%i", example_emoji))

            embed = discord.Embed(color=self.embed_data.get("color", discord.Color.blue().value))

            if "title" in self.embed_data:
                embed.title = replace_variables(self.embed_data["title"])
            if "description" in self.embed_data:
                embed.description = replace_variables(self.embed_data["description"])
            if "footer" in self.embed_data:
                embed.set_footer(text=replace_variables(self.embed_data["footer"]))
            if "author" in self.embed_data:
                embed.set_author(name=replace_variables(self.embed_data["author"]))
            if "image_url" in self.embed_data and self.embed_data["image_url"]:
                embed.set_image(url=self.embed_data["image_url"])
            if "thumbnail_url" in self.embed_data and self.embed_data["thumbnail_url"]:
                embed.set_thumbnail(url=self.embed_data["thumbnail_url"])

            mention_preview = self.embed_data.get('mention_message', '@tag')
            if mention_preview:
                mention_preview = replace_variables(mention_preview)

            content = (
                "📝 **Embed Editor**\n\n"
                "**Available variables:** `%t` (time left), `%n` (name), `%e` (event time), `%d` (date), `%i` (emoji), `@tag` (mention)\n\n"
                f"**Preview values:** {example_emoji} {example_name} at {example_event_time} on {example_date}, {example_time} remaining\n\n"
                f"**Mention Message Preview:**\n{mention_preview}\n"
            )

            if not interaction.response.is_done():
                await interaction.response.edit_message(content=content, embed=embed, view=self)
            else:
                await interaction.followup.edit_message(message_id=interaction.message.id, content=content, embed=embed,
                                                        view=self)

        except Exception as e:
            print(f"Error updating embed: {e}")
            try:
                await interaction.followup.send("❌ An error occurred while updating the embed!", ephemeral=True)
            except:
                pass

    @discord.ui.button(label="Mention Message", style=discord.ButtonStyle.secondary, row=1)
    async def edit_mention_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Edit Mention Message",
                label="Mention Message",
                placeholder="Example: Hey @tag time! (@tag will be replaced with the actual mention)",
                default_value=self.embed_data.get("mention_message", ""),
                max_length=2000
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                self.embed_data["mention_message"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in edit_mention_message: {e}")
            await interaction.followup.send("❌ An error occurred while editing the mention message!", ephemeral=True)

    @discord.ui.button(label="Title", style=discord.ButtonStyle.primary, row=0)
    async def edit_title(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Edit Title",
                label="New Title",
                placeholder="Example: %i %n at %e starts in %t!",
                default_value=self.embed_data.get("title", ""),
                max_length=256
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                self.embed_data["title"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in edit_title: {e}")
            await interaction.followup.send("❌ An error occurred while editing the title!", ephemeral=True)

    @discord.ui.button(label="Description", style=discord.ButtonStyle.primary, row=0)
    async def edit_description(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Edit Description",
                label="New Description",
                placeholder="Example: Get ready for Bear! Only %t remaining.",
                default_value=self.embed_data.get("description", ""),
                max_length=4000,
                style=discord.TextStyle.paragraph
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                self.embed_data["description"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in edit_description: {e}")
            await interaction.followup.send("❌ An error occurred while editing the description!", ephemeral=True)

    @discord.ui.button(label="Color", style=discord.ButtonStyle.success, row=0)
    async def edit_color(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            current_color = self.embed_data.get('color', discord.Color.blue().value)
            current_hex = f"#{hex(current_color)[2:].zfill(6)}"

            modal = TextInputModal(
                title="Color Code",
                label="Hex Color Code",
                placeholder="#FF0000",
                default_value=current_hex,
                max_length=7
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                try:
                    hex_value = modal.value.strip('#')
                    color_value = int(hex_value, 16)
                    self.embed_data["color"] = color_value
                    await self.update_embed(interaction)
                except ValueError:
                    await interaction.followup.send("❌ Invalid color code! Example: #FF0000", ephemeral=True)

        except Exception as e:
            print(f"Error in edit_color: {e}")
            await interaction.followup.send("❌ An error occurred while editing the color!", ephemeral=True)

    @discord.ui.button(label="Footer", style=discord.ButtonStyle.secondary, row=1)
    async def edit_footer(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Edit Footer",
                label="Footer Text",
                placeholder="Example: Notification System",
                default_value=self.embed_data.get("footer", ""),
                max_length=2048
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                self.embed_data["footer"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in edit_footer: {e}")
            await interaction.followup.send("❌ An error occurred while editing the footer!", ephemeral=True)

    @discord.ui.button(label="Author", style=discord.ButtonStyle.secondary, row=1)
    async def edit_author(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Edit Author",
                label="Author Text",
                placeholder="Example: Bear Trap",
                default_value=self.embed_data.get("author", ""),
                max_length=256
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                self.embed_data["author"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in edit_author: {e}")
            await interaction.followup.send("❌ An error occurred while editing the author!", ephemeral=True)

    @discord.ui.button(label="Add Image", style=discord.ButtonStyle.secondary, row=2)
    async def add_image(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Image URL",
                label="Image URL",
                placeholder="https://example.com/image.png",
                default_value=self.embed_data.get("image_url", ""),
                max_length=1000
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                if not modal.value.startswith(('http://', 'https://')):
                    await interaction.followup.send("❌ Invalid URL! URL must start with 'http://' or 'https://'.",
                                                    ephemeral=True)
                    return

                self.embed_data["image_url"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in add_image: {e}")
            await interaction.followup.send("❌ An error occurred while adding the image!", ephemeral=True)

    @discord.ui.button(label="Add Thumbnail", style=discord.ButtonStyle.secondary, row=2)
    async def add_thumbnail(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = TextInputModal(
                title="Thumbnail URL",
                label="Thumbnail URL",
                placeholder="https://example.com/thumbnail.png",
                default_value=self.embed_data.get("thumbnail_url", ""),
                max_length=1000
            )
            await interaction.response.send_modal(modal)
            await modal.wait()

            if modal.value:
                if not modal.value.startswith(('http://', 'https://')):
                    await interaction.followup.send("❌ Invalid URL! URL must start with 'http://' or 'https://'.",
                                                    ephemeral=True)
                    return

                self.embed_data["thumbnail_url"] = modal.value
                await self.update_embed(interaction)

        except Exception as e:
            print(f"Error in add_thumbnail: {e}")
            await interaction.followup.send("❌ An error occurred while adding the thumbnail!", ephemeral=True)

    @discord.ui.button(label="Confirm ✅", style=discord.ButtonStyle.green, row=3)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            self.cog.current_embed_data = self.embed_data

            embed_data = "EMBED_MESSAGE:true"

            await self.cog.show_channel_selection(
                interaction,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                embed_data,
                interaction.guild.text_channels,
                event_type=self.event_type
            )

        except Exception as e:
            print(f"Error in confirm button: {e}")
            try:
                await interaction.followup.send(
                    "❌ An error occurred while confirming the embed! Please try again.",
                    ephemeral=True
                )
            except:
                pass

class MessageTypeView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.original_message = None

    @discord.ui.button(label="Embed Message", style=discord.ButtonStyle.primary, emoji="📝", row=0)
    async def embed_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Show event type selection view first
            embed = discord.Embed(
                title="📋 Select Event Type",
                description=(
                    "Select an event type to use its template, "
                    "or leave **Custom** for the default values.\n\n"
                    "Templates will pre-fill the embed editor with title, description, "
                    "and images from the selected event's template."
                ),
                color=discord.Color.blue()
            )

            view = EventTypeSelectView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                interaction.message
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in embed_message: {e}")
            await interaction.followup.send(
                "❌ An error occurred while starting the event type selection!",
                ephemeral=True
            )

    @discord.ui.button(label="Plain Message", style=discord.ButtonStyle.secondary, emoji="✍️", row=0)
    async def plain_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = discord.ui.Modal(title="Message Content")
        message_content = discord.ui.TextInput(
            label="Message",
            placeholder="Enter notification message... You can use @tag for mentions and %t or {time} for time",
            style=discord.TextStyle.paragraph,
            required=True,
            max_length=2000
        )
        modal.add_item(message_content)

        async def modal_submit(modal_interaction):
            channels = interaction.guild.text_channels
            await self.cog.show_channel_selection(
                modal_interaction,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                f"PLAIN_MESSAGE:{message_content.value}",
                channels
            )

        modal.on_submit = modal_submit
        await interaction.response.send_modal(modal)

class EventTypeSelectView(discord.ui.View):
    """View for selecting event type when creating embed notifications"""

    def __init__(self, cog, start_date, hour, minute, timezone, original_message):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.original_message = original_message
        self.selected_event_type = None  # None means Custom

        # Add the event type dropdown
        self.add_item(EventTypeDropdown(self))

    @discord.ui.button(label="Continue", style=discord.ButtonStyle.primary, emoji="➡️", row=1)
    async def continue_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            # Get template data if an event type was selected
            template_data = None
            if self.selected_event_type:
                # Get templates cog to fetch template defaults
                templates_cog = self.cog.bot.get_cog("BearTrapTemplates")
                if templates_cog:
                    # Get templates for this event type
                    templates = templates_cog.get_templates_by_event_type(self.selected_event_type)
                    if templates:
                        # Get full template data using the first template's ID
                        template_data = templates_cog.get_template(templates[0]["template_id"])

            # Build default embed data
            if template_data:
                embed_data = {
                    "title": template_data.get("embed_title") or f"{self.selected_event_type} Notification",
                    "description": template_data.get("embed_description") or f"Get ready for {self.selected_event_type}! Only %t remaining.",
                    "color": int(template_data.get("embed_color") or 0x3498db),
                    "footer": "Notification System",
                    "image_url": template_data.get("embed_image_url") or "",
                    "thumbnail_url": template_data.get("embed_thumbnail_url") or ""
                }
            else:
                # Default custom embed
                embed_data = {
                    "title": "Bear Trap Notification",
                    "description": "Get ready for Bear! Only %t remaining.",
                    "color": discord.Color.blue().value,
                    "footer": "Notification System"
                }

            # Sample values for preview
            example_time = "30 minutes"
            example_name = self.selected_event_type if self.selected_event_type else "Event"
            example_emoji = get_event_icon(self.selected_event_type) if self.selected_event_type else "📅"
            example_event_time = f"{self.hour:02d}:{self.minute:02d}"
            example_date = self.start_date.strftime("%b %d") if self.start_date else "Dec 06"

            def replace_vars(text):
                if not text:
                    return text
                return (text
                    .replace("%t", example_time)
                    .replace("{time}", example_time)
                    .replace("%n", example_name)
                    .replace("%e", example_event_time)
                    .replace("%d", example_date)
                    .replace("%i", example_emoji))

            # Create preview embed with variables replaced
            embed = discord.Embed(
                title=replace_vars(embed_data["title"]),
                description=replace_vars(embed_data["description"]),
                color=embed_data["color"]
            )
            embed.set_footer(text=replace_vars(embed_data.get("footer", "Notification System")))
            if embed_data.get("image_url"):
                embed.set_image(url=embed_data["image_url"])
            if embed_data.get("thumbnail_url"):
                embed.set_thumbnail(url=embed_data["thumbnail_url"])

            content = (
                "📝 **Embed Editor**\n\n"
                "**Available variables:** `%t` (time left), `%n` (name), `%e` (event time), `%d` (date), `%i` (emoji), `@tag` (mention)\n\n"
                f"**Preview values:** {example_emoji} {example_name} at {example_event_time} on {example_date}, {example_time} remaining"
            )

            # Create embed editor view with event_type and template_data
            view = EmbedEditorView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                self.original_message,
                event_type=self.selected_event_type
            )
            view.embed_data = embed_data

            await interaction.response.edit_message(
                content=content,
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in EventTypeSelectView continue: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(
                "❌ An error occurred while loading the embed editor!",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌", row=1)
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="❌ Notification creation cancelled.",
            embed=None,
            view=None
        )

class EventTypeDropdown(discord.ui.Select):
    """Dropdown for selecting event type"""

    def __init__(self, parent_view: EventTypeSelectView):
        self.parent_view = parent_view

        # Build options: Custom first, then all event types
        options = [
            discord.SelectOption(
                label="Custom (default)",
                value="custom",
                emoji="📅",
                description="Create a notification with default values",
                default=True
            )
        ]

        # Add all event types with their icons
        event_types = get_event_types()
        for event_type in event_types:
            icon = get_event_icon(event_type)
            options.append(discord.SelectOption(
                label=event_type,
                value=event_type,
                emoji=icon,
                description=f"Use {event_type} template"
            ))

        super().__init__(
            placeholder="Select an event type...",
            options=options,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        selected = self.values[0]
        if selected == "custom":
            self.parent_view.selected_event_type = None
        else:
            self.parent_view.selected_event_type = selected

        # Update the dropdown to show the selection
        for option in self.options:
            option.default = (option.value == selected)

        await interaction.response.edit_message(view=self.parent_view)

class TimeSelectModal(discord.ui.Modal):
    def __init__(self, cog: BearTrap):
        super().__init__(title="Set Notification Time")
        self.cog = cog

        current_utc = datetime.now(pytz.UTC)

        self.start_date = discord.ui.TextInput(
            label="Start Date (DD/MM/YYYY)",
            placeholder="Enter start date (e.g., 25/03/2024)",
            min_length=8,
            max_length=10,
            required=True,
            default=current_utc.strftime("%d/%m/%Y")
        )

        self.hour = discord.ui.TextInput(
            label="Hour (0-23)",
            placeholder="Enter hour (e.g., 14)",
            min_length=1,
            max_length=2,
            required=True,
            default=current_utc.strftime("%H")
        )

        self.minute = discord.ui.TextInput(
            label="Minute (0-59)",
            placeholder="Enter minute (e.g., 30)",
            min_length=1,
            max_length=2,
            required=True,
            default=current_utc.strftime("%M")
        )

        self.timezone = discord.ui.TextInput(
            label="Timezone",
            placeholder="Enter timezone (e.g., UTC, Europe/Istanbul)",
            min_length=1,
            max_length=50,
            required=True,
            default="UTC"
        )

        for item in [self.start_date, self.hour, self.minute, self.timezone]:
            self.add_item(item)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            try:
                timezone = pytz.timezone(self.timezone.value)
            except pytz.exceptions.UnknownTimeZoneError:
                await interaction.response.send_message(
                    "❌ Invalid timezone! Please use a valid timezone (e.g., UTC, Europe/Istanbul).",
                    ephemeral=True
                )
                return

            try:
                start_date = datetime.strptime(self.start_date.value, "%d/%m/%Y")
                now = datetime.now(timezone)
                start_date = timezone.localize(start_date)

                if start_date.date() < now.date():
                    await interaction.response.send_message(
                        "❌ Start date cannot be in the past for the selected timezone!",
                        ephemeral=True
                    )
                    return
            except ValueError:
                await interaction.response.send_message(
                    "❌ Invalid date format! Please use DD/MM/YYYY format.",
                    ephemeral=True
                )
                return

            hour = int(self.hour.value)
            minute = int(self.minute.value)

            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("Invalid time format")

            view = MessageTypeView(
                self.cog,
                start_date,
                hour,
                minute,
                self.timezone.value
            )

            embed = discord.Embed(
                title="📝 Select Message Type",
                description=(
                    "How should your notification message look?\n\n"
                    "**📝 Embed Message**\n"
                    "• Customizable title\n"
                    "• Rich text format\n"
                    "• Custom color selection\n"
                    "• Footer and author\n\n"
                    "**✍️ Plain Message**\n"
                    "• Simple text format\n"
                    "• Quick creation"
                ),
                color=discord.Color.blue()
            )

            await interaction.response.send_message(
                embed=embed,
                view=view,
                ephemeral=True
            )

        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid time format! Please use numbers for hour (0-23) and minute (0-59).",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in time modal: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while setting the time.",
                ephemeral=True
            )

class NotificationTypeView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, channel_id, original_message, event_type=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.channel_id = channel_id
        self.original_message = original_message
        self.event_type = event_type

    @discord.ui.button(label="30m, 10m, 5m & Time", style=discord.ButtonStyle.primary, custom_id="type_1", row=0)
    async def type_1(self, interaction, button):
        await self.show_mention_type_menu(interaction, 1)

    @discord.ui.button(label="10m, 5m & Time", style=discord.ButtonStyle.primary, custom_id="type_2", row=0)
    async def type_2(self, interaction, button):
        await self.show_mention_type_menu(interaction, 2)

    @discord.ui.button(label="5m & Time", style=discord.ButtonStyle.primary, custom_id="type_3", row=1)
    async def type_3(self, interaction, button):
        await self.show_mention_type_menu(interaction, 3)

    @discord.ui.button(label="Only 5m", style=discord.ButtonStyle.primary, custom_id="type_4", row=1)
    async def type_4(self, interaction, button):
        await self.show_mention_type_menu(interaction, 4)

    @discord.ui.button(label="Only Time", style=discord.ButtonStyle.primary, custom_id="type_5", row=1)
    async def type_5(self, interaction, button):
        await self.show_mention_type_menu(interaction, 5)

    @discord.ui.button(label="Custom Times", style=discord.ButtonStyle.success, custom_id="type_6", row=2)
    async def type_6(self, interaction, button):
        modal = CustomTimesModal(self.cog, self.start_date, self.hour, self.minute, self.timezone, self.message_data,
                                 self.channel_id, self.original_message, event_type=self.event_type)
        await interaction.response.send_modal(modal)

    async def show_mention_type_menu(self, interaction, notification_type):
        try:
            embed = discord.Embed(
                title="📢 Select Mention Type",
                description=(
                    "Choose how to mention users:\n\n"
                    "1️⃣ @everyone\n"
                    "2️⃣ Specific Role\n"
                    "3️⃣ Specific Member\n"
                    "4️⃣ No Mention"
                ),
                color=discord.Color.blue()
            )

            view = MentionTypeView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                self.message_data,
                self.channel_id,
                notification_type,
                self.original_message,
                event_type=self.event_type
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )
        except Exception as e:
            print(f"Error in show_mention_type_menu: {e}")
            await interaction.followup.send(
                "❌ An error occurred while showing mention options!",
                ephemeral=True
            )

class CustomTimesModal(discord.ui.Modal):
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, channel_id, original_message, event_type=None):
        super().__init__(title="Set Custom Notification Times")
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.channel_id = channel_id
        self.original_message = original_message
        self.event_type = event_type

        self.custom_times = discord.ui.TextInput(
            label="Custom Notification Times",
            placeholder="Enter times in minutes (e.g., 60-20-15-4-2 or 60-20-15-4-2-0)",
            min_length=1,
            max_length=50,
            required=True,
            style=discord.TextStyle.short
        )
        self.add_item(self.custom_times)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            times_str = self.custom_times.value.strip()
            times = [int(t) for t in times_str.split('-')]

            if not all(isinstance(t, int) and t >= 0 for t in times):
                raise ValueError("All times must be non-negative integers")

            if not times:
                raise ValueError("At least one time must be specified")

            if not all(times[i] > times[i + 1] for i in range(len(times) - 1)):
                raise ValueError("Times must be in descending order")

            embed = discord.Embed(
                title="📢 Select Mention Type",
                description=(
                    "Choose how to mention users:\n\n"
                    "1️⃣ @everyone\n"
                    "2️⃣ Specific Role\n"
                    "3️⃣ Specific Member\n"
                    "4️⃣ No Mention"
                ),
                color=discord.Color.blue()
            )

            view = MentionTypeView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                f"CUSTOM_TIMES:{'-'.join(map(str, times))}|{self.message_data}",
                self.channel_id,
                6,
                self.original_message,
                event_type=self.event_type
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )

        except ValueError as e:
            await interaction.response.send_message(
                f"❌ Invalid input: {str(e)}",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in custom times modal: {e}")
            await interaction.followup.send(
                "❌ An error occurred while processing custom times.",
                ephemeral=True
            )

class MentionTypeView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, channel_id, notification_type,
                 original_message, event_type=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.channel_id = channel_id
        self.notification_type = notification_type
        self.original_message = original_message
        self.event_type = event_type

    async def show_mention_type_menu(self, interaction, mention_type):
        try:
            embed = discord.Embed(
                title="🔄 Repeat Settings",
                description=(
                    "**Configure Notification Repeat**\n\n"
                    "Choose how often you want this notification to repeat:\n\n"
                    "- No Repeat: Notification will be sent only once\n"
                    "- Custom Interval: Set a custom repeat interval (minutes/hours/days/weeks/months)\n"
                    "- Specific days: Choose which days of the week you want to get notifications on"
                ),
                color=discord.Color.blue()
            )

            view = RepeatOptionView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                self.message_data,
                self.channel_id,
                self.notification_type,
                mention_type,
                self.original_message,
                event_type=self.event_type
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )
        except Exception as e:
            print(f"Error in show_mention_type_menu: {e}")
            await interaction.followup.send(
                "❌ An error occurred while showing mention options!",
                ephemeral=True
            )

    @discord.ui.button(label="@everyone", style=discord.ButtonStyle.danger, emoji="📢", row=0)
    async def everyone_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.show_mention_type_menu(interaction, "everyone")
        except Exception as e:
            print(f"Error in everyone button: {e}")
            await interaction.followup.send(
                "❌ An error occurred while setting @everyone mention!",
                ephemeral=True
            )

    @discord.ui.button(label="Select Member", style=discord.ButtonStyle.primary, emoji="👤", row=0)
    async def member_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            select = discord.ui.UserSelect(
                placeholder="Select a member to mention",
                min_values=1,
                max_values=1
            )

            async def user_select_callback(select_interaction):
                try:
                    selected_user_id = select_interaction.data["values"][0]
                    await self.show_mention_type_menu(select_interaction, f"member_{selected_user_id}")
                except Exception as e:
                    print(f"Error in user selection: {e}")
                    await select_interaction.followup.send(
                        "❌ An error occurred while selecting the member!",
                        ephemeral=True
                    )

            select.callback = user_select_callback
            view = discord.ui.View(timeout=300)
            view.add_item(select)

            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="👤 Select Member",
                    description="Choose a member to mention:",
                    color=discord.Color.blue()
                ),
                view=view
            )
        except Exception as e:
            print(f"Error in member button: {e}")
            await interaction.followup.send(
                "❌ An error occurred while showing member selection!",
                ephemeral=True
            )

    @discord.ui.button(label="Select Role", style=discord.ButtonStyle.success, emoji="👥", row=0)
    async def role_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            select = discord.ui.RoleSelect(
                placeholder="Select a role to mention",
                min_values=1,
                max_values=1
            )

            async def role_select_callback(select_interaction):
                try:
                    selected_role_id = select_interaction.data["values"][0]
                    await self.show_mention_type_menu(select_interaction, f"role_{selected_role_id}")
                except Exception as e:
                    print(f"Error in role selection: {e}")
                    await select_interaction.followup.send(
                        "❌ An error occurred while selecting the role!",
                        ephemeral=True
                    )

            select.callback = role_select_callback
            view = discord.ui.View(timeout=300)
            view.add_item(select)

            await interaction.response.edit_message(
                embed=discord.Embed(
                    title="👥 Select Role",
                    description="Choose a role to mention:",
                    color=discord.Color.blue()
                ),
                view=view
            )
        except Exception as e:
            print(f"Error in role button: {e}")
            await interaction.followup.send(
                "❌ An error occurred while showing role selection!",
                ephemeral=True
            )

    @discord.ui.button(label="No Mention", style=discord.ButtonStyle.secondary, emoji="🔕", row=0)
    async def no_mention_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.show_mention_type_menu(interaction, "none")
        except Exception as e:
            print(f"Error in no mention button: {e}")
            await interaction.followup.send(
                "❌ An error occurred while setting no mention!",
                ephemeral=True
            )

class MentionSelectMenu(discord.ui.Select):
    def __init__(self, view):
        self.parent_view = view

        options = []

        options.append(
            discord.SelectOption(
                label="@everyone",
                value="everyone",
                description="Mention everyone in the server",
                emoji="📢"
            )
        )

        options.append(
            discord.SelectOption(
                label="No Mention",
                value="none",
                description="Don't mention anyone",
                emoji="🔕"
            )
        )

        guild = view.original_message.guild
        roles = sorted(
            [role for role in guild.roles if not role.is_default() and not role.managed],
            key=lambda r: r.position,
            reverse=True
        )

        for role in roles:
            options.append(
                discord.SelectOption(
                    label=role.name,
                    value=f"role_{role.id}",
                    description=f"Role with {len(role.members)} members",
                    emoji="👥"
                )
            )

        members = sorted(
            [member for member in guild.members if not member.bot],
            key=lambda m: m.display_name.lower()
        )

        for member in members:
            options.append(
                discord.SelectOption(
                    label=member.display_name,
                    value=f"member_{member.id}",
                    description=f"@{member.name}",
                    emoji="👤"
                )
            )

        super().__init__(
            placeholder="🔍 Search and select who to mention...",
            min_values=1,
            max_values=1,
            options=options,
            row=0
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            selected_value = self.values[0]

            await self.parent_view.show_mention_type_menu(interaction, selected_value)

        except Exception as e:
            print(f"Error in mention selection: {e}")
            await interaction.followup.send(
                "❌ An error occurred while processing your selection!",
                ephemeral=True
            )

class SettingsView(discord.ui.View):
    def __init__(self, cog, delete_enabled: bool, default_delay: int):
        super().__init__(timeout=300)
        self.cog = cog
        self.delete_enabled = delete_enabled
        self.default_delay = default_delay
        self.conn = sqlite3.connect('db/beartime.sqlite')
        self.cursor = self.conn.cursor()

    def build_settings_embed(self):
        """Build the settings embed with current values"""
        embed = discord.Embed(
            title="⚙️ Bear Trap Settings",
            description="Configure global message deletion settings",
            color=discord.Color.blue()
        )

        # Message Deletion section
        deletion_status = "✅ Enabled" if self.delete_enabled else "❌ Disabled"
        embed.add_field(
            name="📨 Message Deletion",
            value=f"Status: {deletion_status}\nDefault Delay: {self.default_delay} minutes",
            inline=False
        )

        embed.set_footer(text="Use the buttons below to modify settings")
        return embed

    @discord.ui.button(
        label="Toggle Message Deletion",
        emoji="🗑️",
        style=discord.ButtonStyle.primary,
        row=0
    )
    async def toggle_deletion(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            new_value = 0 if self.delete_enabled else 1
            self.cursor.execute("""
                UPDATE bear_trap_settings
                SET delete_messages_enabled = ?
                WHERE guild_id = ?
            """, (new_value, interaction.guild_id))
            self.conn.commit()

            self.delete_enabled = bool(new_value)

            await interaction.response.edit_message(embed=self.build_settings_embed(), view=self)

        except Exception as e:
            print(f"Error toggling deletion: {e}")
            traceback.print_exc()
            await interaction.response.send_message(
                "❌ An error occurred while updating settings.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Set Default Delay",
        emoji="⏱️",
        style=discord.ButtonStyle.primary,
        row=0
    )
    async def set_default_delay(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            modal = discord.ui.Modal(title="Set Default Deletion Delay")
            delay_input = discord.ui.TextInput(
                label="Delay in Minutes",
                placeholder="Enter delay in minutes (e.g., 60)",
                default=str(self.default_delay),
                required=True,
                max_length=5
            )
            modal.add_item(delay_input)

            async def modal_callback(modal_interaction: discord.Interaction):
                try:
                    new_delay = int(delay_input.value)
                    if new_delay < 1:
                        await modal_interaction.response.send_message(
                            "❌ Delay must be at least 1 minute.",
                            ephemeral=True
                        )
                        return

                    self.cursor.execute("""
                        UPDATE bear_trap_settings
                        SET default_delete_delay_minutes = ?
                        WHERE guild_id = ?
                    """, (new_delay, modal_interaction.guild_id))
                    self.conn.commit()

                    self.default_delay = new_delay

                    await modal_interaction.response.edit_message(embed=self.build_settings_embed(), view=self)

                except ValueError:
                    await modal_interaction.response.send_message(
                        "❌ Please enter a valid number.",
                        ephemeral=True
                    )
                except Exception as e:
                    print(f"Error in modal callback: {e}")
                    traceback.print_exc()
                    await modal_interaction.response.send_message(
                        "❌ An error occurred while updating the delay.",
                        ephemeral=True
                    )

            modal.on_submit = modal_callback
            await interaction.response.send_modal(modal)

        except Exception as e:
            print(f"Error opening delay modal: {e}")
            traceback.print_exc()
            await interaction.response.send_message(
                "❌ An error occurred while opening the settings modal.",
                ephemeral=True
            )

class BearTrapView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        self.conn = sqlite3.connect('db/beartime.sqlite')
        self.cursor = self.conn.cursor()

    @discord.ui.button(
        label="Setup Wizard",
        emoji="🧙",
        style=discord.ButtonStyle.success,
        custom_id="setup_wizard",
        row=0
    )
    async def setup_wizard_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            wizard_cog = self.cog.bot.get_cog("BearTrapWizard")
            if wizard_cog:
                await wizard_cog.show_wizard(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Setup Wizard not found. Don't worry, I'm sure he will arrive precisely when he means to.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading Setup Wizard: {e}")
            await interaction.response.send_message(
                "❌ We couldn't summon the Setup Wizard. Try summoning him off and on again?",
                ephemeral=True
            ) 

    @discord.ui.button(
        label="Custom Notification",
        emoji="⏰",
        style=discord.ButtonStyle.success,
        custom_id="set_time",
        row=0
    )
    async def set_time_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return

        try:
            modal = TimeSelectModal(self.cog)
            await interaction.response.send_modal(modal)

        except Exception as e:
            error_msg = f"[ERROR] Error in set time button: {str(e)}\nType: {type(e)}\nTrace: {traceback.format_exc()}"
            print(error_msg)

            try:
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        "❌ An error occurred!",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        "❌ An error occurred!",
                        ephemeral=True
                    )
            except Exception as notify_error:
                print(f"[ERROR] Failed to notify user about error: {notify_error}")

    @discord.ui.button(
        label="Manage Notifications",
        emoji="📋",
        style=discord.ButtonStyle.primary,
        custom_id="manage_notification",
        row=0
    )
    async def manage_notification_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            notifications = await self.cog.get_notifications(interaction.guild_id)
            original_notifications = notifications.copy()
            search_keywords = []
            if not notifications:
                await interaction.response.send_message(
                    "❌ No notifications found in this server.",
                    ephemeral=True
                )
                return

            page_size = 25
            total_pages = (len(notifications) // page_size) + (1 if len(notifications) % page_size != 0 else 0)
            current_page = 0

            def get_page_option(page):
                start = page * page_size
                end = start + page_size
                page_notifications = notifications[start:end]

                options = []
                for notif in page_notifications:
                    status_emoji = "🟢" if notif[11] else "🔴"
                    status = "Enabled" if notif[11] else "Disabled"

                    # Check if channel exists
                    channel = interaction.guild.get_channel(notif[2])
                    channel_warning = "⚠️ " if not channel else ""
                    channel_name = f"#{channel.name}" if channel else "Unknown"

                    # Get event type from database column (index 16)
                    event_type = notif[16] if notif[16] else None

                    # Get event emoji and display name
                    if event_type:
                        from bear_event_types import get_event_icon
                        event_emoji = get_event_icon(event_type)
                        display_name = event_type
                    else:
                        # Custom notification - get title from description or embed
                        event_emoji = "📝"
                        notification_desc = notif[6]  # description field

                        if "EMBED_MESSAGE:" in notification_desc:
                            # Try to get embed title
                            try:
                                self.cog.cursor.execute("""
                                    SELECT title FROM bear_notification_embeds
                                    WHERE notification_id = ?
                                """, (notif[0],))
                                embed_row = self.cog.cursor.fetchone()
                                if embed_row and embed_row[0]:
                                    display_name = f"{embed_row[0]} (Custom)"
                                else:
                                    display_name = "Custom Notification"
                            except:
                                display_name = "Custom Notification"
                        elif notification_desc.startswith("CUSTOM_TIMES:"):
                            # Extract description after CUSTOM_TIMES:
                            parts = notification_desc.split("|", 1)
                            if len(parts) > 1:
                                custom_desc = parts[1].replace("PLAIN_MESSAGE:", "").strip()
                                display_name = f"{custom_desc[:30]} (Custom)" if custom_desc else "Custom Notification"
                            else:
                                display_name = "Custom Notification"
                        else:
                            # Plain notification
                            plain_desc = notification_desc.replace("PLAIN_MESSAGE:", "").strip()
                            display_name = f"{plain_desc[:30]} (Custom)" if plain_desc else "Custom Notification"

                    # Format next occurrence time
                    import pytz
                    from datetime import datetime
                    if notif[15]:  # next_notification
                        try:
                            next_time = datetime.fromisoformat(notif[15])
                            tz = pytz.timezone(notif[5])
                            next_time_local = next_time.astimezone(tz)
                            time_display = next_time_local.strftime("%m/%d %H:%M")
                        except:
                            time_display = f"{notif[3]:02d}:{notif[4]:02d}"
                    else:
                        time_display = f"{notif[3]:02d}:{notif[4]:02d}"

                    # Build label: [Emoji] [Event Type/Title] - [Time]
                    label = f"{channel_warning}{event_emoji} {display_name} - {time_display}"
                    description = f"{status_emoji} {status} | {channel_name} | ID: {notif[0]}"

                    # Avoid nested f-strings for Python 3.9+ compatibility
                    if "EMBED_MESSAGE:" in notif[6]:
                        option_value = f"{notif[0]}|embed"
                    else:
                        option_value = f"{notif[0]}|plain"

                    options.append(
                        discord.SelectOption(
                            label=label[:100],  # Discord max 100 chars
                            description=description[:100],
                            value=option_value
                        )
                    )
                if len(options) > 25:
                    options = options[:25]
                return options

            select = discord.ui.Select(
                placeholder=f"Page {current_page + 1}/{total_pages} — Select a notification to view",
                options=get_page_option(current_page)
            )

            class PaginationButton(discord.ui.Button):
                def __init__(self, label, page_change):
                    super().__init__(label=label, style=discord.ButtonStyle.primary)
                    self.page_change = page_change

                async def callback(self, interaction: discord.Interaction):
                    nonlocal current_page
                    new_page = current_page + self.page_change
                    if 0 <= new_page < total_pages:
                        current_page = new_page

                        new_options = get_page_option(current_page)
                        select.options = new_options
                        select.placeholder = f"Page {current_page + 1}/{total_pages} — Select a notification to view"

                        prev_button.disabled = current_page == 0
                        next_button.disabled = current_page == total_pages - 1

                        await interaction.response.edit_message(
                            view=view
                        )

            prev_button = PaginationButton(label="⬅️ Previous", page_change=-1)
            prev_button.disabled = current_page == 0
            next_button = PaginationButton(label="Next ➡️", page_change=1)

            class SearchButton(discord.ui.Button):
                def __init__(self, label, cog):
                    super().__init__(label=label, style=discord.ButtonStyle.primary)
                    self.cog = cog

                async def callback(self, interaction: discord.Interaction):
                    class SearchModal(discord.ui.Modal, title="Search Notifications"):
                        keyword = discord.ui.TextInput(
                            label="Search Term",
                            placeholder="Enter text to search for..."
                        )

                        async def on_submit(modal_self, interaction: discord.Interaction):
                            nonlocal notifications, current_page, total_pages

                            keyword_value = modal_self.keyword.value
                            keyword_lower = keyword_value.lower()
                            filtered = []
                            
                            for n in notifications:
                                if "EMBED_MESSAGE:" in n[6]:
                                    button_self.cog.cursor.execute("SELECT title FROM bear_notification_embeds WHERE notification_id = ?", (n[0],))
                                    embed_data = button_self.cog.cursor.fetchone()
                                    display_text = embed_data[0] if embed_data and embed_data[0] else "Embed Message"
                                else:
                                    display_text = n[6].split('|')[-1] if '|' in n[6] else n[6]
                                    if display_text.startswith("PLAIN_MESSAGE:"):
                                        display_text = display_text.replace("PLAIN_MESSAGE:", "", 1)
                                
                                if keyword_lower in display_text.lower():
                                    filtered.append(n)

                            if not filtered:
                                if search_keywords:
                                    prev_keywords_display = " and ".join(f"`{k}`" for k in search_keywords)
                                    message = (
                                        f"❌ No notifications found with `{keyword_value}` "
                                        f"among those already filtered by: {prev_keywords_display}"
                                    )
                                else:
                                    message = f"❌ No notifications found for keyword `{keyword_value}`."

                                await interaction.response.send_message(message, ephemeral=True)
                                return

                            search_keywords.append(keyword_value)

                            notifications = filtered
                            current_page = 0
                            total_pages = (len(notifications) // page_size) + (
                                1 if len(notifications) % page_size != 0 else 0
                            )

                            select.options = get_page_option(current_page)
                            select.placeholder = f"Page {current_page + 1}/{total_pages} — Select a notification to view"

                            reset_button.disabled = not search_keywords
                            prev_button.disabled = current_page == 0
                            next_button.disabled = current_page == total_pages - 1

                            keywords_display = " and ".join(f"`{k}`" for k in search_keywords)
                            content_message = f"🔍 Showing notifications that contain the keyword(s): {keywords_display}"

                            await interaction.response.edit_message(content=content_message, view=view)

                    button_self = self
                    await interaction.response.send_modal(SearchModal())

            search_button = SearchButton(label="🔍 Search", cog=self.cog)

            class ResetButton(discord.ui.Button):
                def __init__(self, label):
                    super().__init__(label=label, style=discord.ButtonStyle.secondary)

                async def callback(self, interaction: discord.Interaction):
                    nonlocal notifications, original_notifications, current_page, total_pages

                    notifications = original_notifications.copy()
                    search_keywords.clear()
                    current_page = 0
                    total_pages = (len(notifications) // page_size) + (1 if len(notifications) % page_size != 0 else 0)

                    select.options = get_page_option(current_page)
                    select.placeholder = f"Page {current_page + 1}/{total_pages} — Select a notification to view"

                    reset_button.disabled = not search_keywords
                    prev_button.disabled = current_page == 0
                    next_button.disabled = current_page == total_pages - 1

                    await interaction.response.edit_message(content="Showing all notifications.", view=view)

            reset_button = ResetButton(label="🔄 Reset Filter")
            reset_button.disabled = not search_keywords

            async def select_callback(select_interaction):
                try:
                    selected_value = select_interaction.data["values"][0]
                    notification_id, notif_type = selected_value.split("|")
                    notification_id = int(notification_id)

                    selected_notif = next(n for n in notifications if n[0] == notification_id)

                    notification_types = {
                        1: "Sends notifications at 30 minutes, 10 minutes, 5 minutes before and when time's up",
                        2: "Sends notifications at 10 minutes, 5 minutes before and when time's up",
                        3: "Sends notifications at 5 minutes before and when time's up",
                        4: "Sends notification only 5 minutes before",
                        5: "Sends notification only when time's up",
                        6: "Sends notifications at custom times"
                    }
                    notification_type_desc = notification_types.get(selected_notif[7], "Unknown Type")

                    mention_display = selected_notif[8]
                    if mention_display.startswith("role_"):
                        mention_display = f"<@&{mention_display.split('_')[1]}>"
                    elif mention_display.startswith("member_"):
                        mention_display = f"<@{mention_display.split('_')[1]}>"
                    elif mention_display == "everyone":
                        mention_display = "@everyone"
                    elif mention_display == "none":
                        mention_display = "No mention"

                    repeat_minutes = selected_notif[10]
                    time_units = [
                        ("month", 43200),
                        ("week", 10080),
                        ("day", 1440),
                        ("hour", 60),
                        ("minute", 1),
                    ]

                    formatted_repeat = "❌ No repeat"
                    if isinstance(repeat_minutes, int) and repeat_minutes > 0:
                        result = []
                        for name, unit in time_units:
                            value = repeat_minutes // unit
                            if value > 0:
                                result.append(f"{value} {name}{'s' if value > 1 else ''}")
                                repeat_minutes %= unit
                        formatted_repeat = " and ".join(result)

                    elif repeat_minutes == -1:
                        self.cursor.execute("""
                                SELECT weekday FROM notification_days
                                WHERE notification_id = ?
                            """, (selected_notif[0],))
                        rows = self.cursor.fetchall()

                        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
                        day_set = set()
                        for row in rows:
                            for part in row[0].split('|'):
                                if part.strip().isdigit():
                                    day_set.add(int(part))

                        sorted_days = sorted(day_set)
                        day_list = [weekday_names[day] for day in sorted_days]

                        if len(day_list) == 1:
                            formatted_repeat = f"Every {day_list[0]}"
                        else:
                            formatted_repeat = "Every " + ", ".join(day_list[:-1]) + " and " + day_list[-1]

                    # Check if channel exists
                    channel = select_interaction.guild.get_channel(selected_notif[2])
                    if channel:
                        channel_display = f"<#{selected_notif[2]}>"
                    else:
                        channel_display = f"⚠️ #unknown-channel (Deleted or Inaccessible)"

                    # Format delete delay display
                    custom_delay = selected_notif[17] if len(selected_notif) > 17 else None
                    if custom_delay is not None:
                        delete_delay_display = f"{custom_delay} minutes (custom)"
                    else:
                        delete_delay_display = "Using default delay"

                    details_embed = discord.Embed(
                        title=f"📋 Notification Details",
                        description=(
                            f"**📅 Next Notification date:** {datetime.fromisoformat(selected_notif[15]).strftime('%d/%m/%Y')}\n"
                            f"**⏰ Time:** {selected_notif[3]:02d}:{selected_notif[4]:02d} ({selected_notif[5]})\n"
                            f"**📢 Channel:** {channel_display}\n"
                            f"**📝 Description:** {selected_notif[6]}\n\n"
                            f"**⚙️ Notification Type:** \n{notification_type_desc}\n\n"
                            f"**👥 Mention:** {mention_display}\n"
                            f"**🔄 Repeat:** {formatted_repeat}\n"
                            f"**🧹 Message Cleanup:** {delete_delay_display}\n"),
                        color=discord.Color.blue()
                    )

                    view = discord.ui.View()

                    class PreviewButton(discord.ui.Button):
                        def __init__(self, cog, notification_id):
                            super().__init__(label="👀 Preview", style=discord.ButtonStyle.primary)
                            self.cog = cog
                            self.notification_id = notification_id

                        async def callback(self, interaction: discord.Interaction):
                            try:
                                self.cog.cursor.execute(
                                    """SELECT channel_id, hour, minute, description, mention_type, next_notification, event_type
                                       FROM bear_notifications WHERE id = ?""",
                                    (self.notification_id,)
                                )
                                selected_notif = self.cog.cursor.fetchone()

                                if not selected_notif:
                                    await interaction.response.send_message("❌ Notification not found.", ephemeral=True)
                                    return

                                channel_id, hours, minutes, description, mention_type, next_notification, event_type = selected_notif

                                # Sample values for preview variable replacement
                                example_time = "30 minutes"
                                example_name = event_type if event_type else "Event"
                                example_emoji = get_event_icon(event_type) if event_type else "📅"
                                example_event_time = f"{hours:02d}:{minutes:02d}"
                                try:
                                    next_dt = datetime.fromisoformat(next_notification.replace("+00:00", ""))
                                    example_date = next_dt.strftime("%b %d")
                                except:
                                    example_date = "Dec 06"

                                def replace_vars(text):
                                    if not text:
                                        return text
                                    return (text
                                        .replace("%t", example_time)
                                        .replace("{time}", example_time)
                                        .replace("%n", example_name)
                                        .replace("%e", example_event_time)
                                        .replace("%d", example_date)
                                        .replace("%i", example_emoji))

                                embed_data = None
                                if "EMBED_MESSAGE:" in description:
                                    self.cog.cursor.execute("""
                                        SELECT title, description, color, image_url, thumbnail_url, footer, author, mention_message
                                        FROM bear_notification_embeds WHERE notification_id = ?
                                    """, (self.notification_id,))
                                    embed_result = self.cog.cursor.fetchone()

                                    if embed_result:
                                        embed_data = {
                                            'title': embed_result[0],
                                            'description': embed_result[1],
                                            'color': embed_result[2],
                                            'image_url': embed_result[3],
                                            'thumbnail_url': embed_result[4],
                                            'footer': embed_result[5],
                                            'author': embed_result[6],
                                            'mention_message': embed_result[7]
                                        }
                                mention_display = ""
                                if mention_type.startswith("role_"):
                                    mention_display = f"<@&{mention_type.split('_')[1]}>"
                                elif mention_type.startswith("member_"):
                                    mention_display = f"<@{mention_type.split('_')[1]}>"
                                elif mention_type == "everyone":
                                    mention_display = "@everyone"
                                elif mention_type == "none":
                                    mention_display = ""

                                preview_embed = None
                                if embed_data:
                                    mention_preview = embed_data['mention_message'] if embed_data[
                                        'mention_message'] else ""
                                    mention_preview = replace_vars(mention_preview).replace("@tag", mention_display)

                                    preview_embed = discord.Embed(
                                        title=replace_vars(embed_data['title']) if embed_data['title'] else "No Title",
                                        description=replace_vars(embed_data['description']) if embed_data[
                                            'description'] else "No Description",
                                        color=embed_data['color'] if embed_data['color'] else discord.Color.blue()
                                    )

                                    if embed_data['image_url']:
                                        preview_embed.set_image(url=embed_data['image_url'])
                                    if embed_data['thumbnail_url']:
                                        preview_embed.set_thumbnail(url=embed_data['thumbnail_url'])
                                    if embed_data['footer']:
                                        preview_embed.set_footer(text=replace_vars(embed_data['footer']))
                                    if embed_data['author']:
                                        preview_embed.set_author(name=replace_vars(embed_data['author']))

                                    # Create copyable JSON data for the embed
                                    copyable_data = {
                                        'title': embed_data['title'],
                                        'description': embed_data['description'],
                                        'color': embed_data['color'],
                                        'footer': embed_data['footer'],
                                        'author': embed_data['author'],
                                        'image_url': embed_data['image_url'],
                                        'thumbnail_url': embed_data['thumbnail_url'],
                                        'mention_message': embed_data['mention_message']
                                    }

                                    embed_json = json.dumps(copyable_data, indent=2)

                                    # Create view with a "Show Code" button
                                    view = discord.ui.View()
                                    view.add_item(ShowCodeButton(embed_json))

                                    await interaction.response.send_message(
                                        content=mention_preview,
                                        embed=preview_embed,
                                        view=view,
                                        ephemeral=True
                                    )
                                else:
                                    message_preview = description.split("PLAIN_MESSAGE:", 1)[-1].strip()
                                    message_preview = replace_vars(message_preview).replace("@tag", mention_display)

                                    await interaction.response.send_message(
                                        content=message_preview,
                                        ephemeral=True
                                    )

                            except Exception as e:
                                print(f"[ERROR] Exception in PreviewButton: {e}")
                                await interaction.response.send_message(
                                    "❌ An error occurred while fetching the preview.", ephemeral=True)

                    class ShowCodeButton(discord.ui.Button):
                        def __init__(self, embed_json):
                            super().__init__(label="💾 Show Code", style=discord.ButtonStyle.secondary)
                            self.embed_json = embed_json

                        async def callback(self, interaction: discord.Interaction):
                            await interaction.response.send_message(
                                content=f"```json\n{self.embed_json}\n```",
                                ephemeral=True
                            )

                    class AdvancedSettingsButton(discord.ui.Button):
                        def __init__(self, cog, notification_id):
                            super().__init__(label="🧹 Message Cleanup", style=discord.ButtonStyle.secondary)
                            self.cog = cog
                            self.notification_id = notification_id

                        async def callback(self, interaction: discord.Interaction):
                            try:
                                # Get current custom delete delay
                                self.cog.cursor.execute("""
                                    SELECT custom_delete_delay_minutes
                                    FROM bear_notifications
                                    WHERE id = ?
                                """, (self.notification_id,))
                                row = self.cog.cursor.fetchone()
                                current_delay = row[0] if row and row[0] is not None else None

                                modal = discord.ui.Modal(title="Message Cleanup Settings")
                                delay_input = discord.ui.TextInput(
                                    label="Custom Delete Delay (minutes)",
                                    placeholder="Leave empty to use default delay (60 min)",
                                    default=str(current_delay) if current_delay is not None else "",
                                    required=False,
                                    max_length=5
                                )
                                modal.add_item(delay_input)

                                async def modal_callback(modal_interaction: discord.Interaction):
                                    try:
                                        if delay_input.value.strip():
                                            new_delay = int(delay_input.value)
                                            if new_delay < 1:
                                                await modal_interaction.response.send_message(
                                                    "❌ Delay must be at least 1 minute.",
                                                    ephemeral=True
                                                )
                                                return
                                        else:
                                            # Empty = use default
                                            new_delay = None

                                        self.cog.cursor.execute("""
                                            UPDATE bear_notifications
                                            SET custom_delete_delay_minutes = ?
                                            WHERE id = ?
                                        """, (new_delay, self.notification_id))
                                        self.cog.conn.commit()

                                        # Refresh the notification list to get updated data
                                        notifications = await self.cog.get_notifications(modal_interaction.guild_id)
                                        selected_notif = next(n for n in notifications if n[0] == self.notification_id)

                                        # Update delete delay display
                                        custom_delay = selected_notif[17] if len(selected_notif) > 17 else None
                                        if custom_delay is not None:
                                            delete_delay_display = f"{custom_delay} minutes (custom)"
                                        else:
                                            delete_delay_display = "Using default delay"

                                        # Get current embed and update it
                                        current_embed = modal_interaction.message.embeds[0]

                                        # Rebuild description with updated delete delay
                                        lines = current_embed.description.split('\n')
                                        updated_lines = []
                                        for line in lines:
                                            if line.startswith("**🧹 Message Cleanup:**"):
                                                updated_lines.append(f"**🧹 Message Cleanup:** {delete_delay_display}")
                                            else:
                                                updated_lines.append(line)

                                        current_embed.description = '\n'.join(updated_lines)

                                        await modal_interaction.response.edit_message(embed=current_embed)

                                    except ValueError:
                                        await modal_interaction.response.send_message(
                                            "❌ Please enter a valid number.",
                                            ephemeral=True
                                        )
                                    except Exception as e:
                                        print(f"Error updating custom delay: {e}")
                                        traceback.print_exc()
                                        await modal_interaction.response.send_message(
                                            "❌ An error occurred while updating settings.",
                                            ephemeral=True
                                        )

                                modal.on_submit = modal_callback
                                await interaction.response.send_modal(modal)

                            except Exception as e:
                                print(f"Error opening advanced settings: {e}")
                                traceback.print_exc()
                                await interaction.response.send_message(
                                    "❌ An error occurred while opening advanced settings.",
                                    ephemeral=True
                                )

                    class DeleteButton(discord.ui.Button):
                        def __init__(self, cog, notification_id):
                            super().__init__(label="🗑️ Delete", style=discord.ButtonStyle.danger)
                            self.cog = cog
                            self.notification_id = notification_id

                        async def callback(self, interaction: discord.Interaction):
                            try:
                                confirm_view = discord.ui.View()

                                confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.danger)
                                cancel_button = discord.ui.Button(label="Cancel", style=discord.ButtonStyle.primary)

                                async def confirm_callback(interaction: discord.Interaction):
                                    try:
                                        result = await self.cog.delete_notification(self.notification_id)

                                        if result:
                                            new_view = discord.ui.View()

                                            for row in interaction.message.components:
                                                for item in row.children:
                                                    if isinstance(item, discord.ui.Button) and item.label not in [
                                                        "Confirm", "Cancel"]:
                                                        new_view.add_item(
                                                            item)

                                            await interaction.response.edit_message(view=new_view)
                                            await interaction.followup.send("✅ Successfully deleted.", ephemeral=True)

                                        else:
                                            print(f"[DEBUG] Deletion failed for notification_id {self.notification_id}")
                                            await interaction.response.send_message(
                                                "❌ Failed to delete the notification.", ephemeral=True
                                            )

                                    except Exception as e:
                                        print(f"[ERROR] Exception in confirm_callback: {e}")
                                        await interaction.response.send_message(
                                            "❌ An error occurred while deleting the notification.", ephemeral=True
                                        )

                                async def cancel_callback(interaction: discord.Interaction):
                                    try:
                                        await interaction.response.edit_message(
                                            content=(
                                                "- **🔍 Search:** Filter the menu options based on specific keywords\n"
                                                "- **📝 Edit:** Modify notification details.\n"
                                                "- **🚨 Notification is active/inactive:** Toggles between enabling or disabling the notification.\n"
                                                "  - -# Click to toggle between enabling or disabling.\n"
                                                "  - -# Enabling a non-repeating notification will keep its time but change its date to today's date or tomorrow if the time had passed.\n"
                                                "- **👀 Preview:** See how the notification will look when it's sent.\n"
                                                "- **🗑️ Delete:** Remove the selected notification.\n\n"
                                            ),
                                            view=view
                                        )
                                    except Exception as e:
                                        print(f"[ERROR] Exception in cancel callback: {e}")

                                confirm_button.callback = confirm_callback
                                cancel_button.callback = cancel_callback
                                confirm_view.add_item(confirm_button)
                                confirm_view.add_item(cancel_button)

                                await interaction.response.edit_message(
                                    content="Are you sure you want to delete this notification?",
                                    view=confirm_view
                                )

                            except Exception as e:
                                print(f"[ERROR] Exception in DeleteButton callback: {e}")
                                await interaction.response.send_message(
                                    "❌ An error occurred while attempting to delete the notification.",
                                    ephemeral=True
                                )

                    class EditButton(discord.ui.Button):
                        def __init__(self):
                            super().__init__(label="📝 Edit", style=discord.ButtonStyle.primary)

                        async def callback(self, button_interaction: discord.Interaction):
                            editor_cog = self.view.editor_cog
                            if editor_cog:
                                try:
                                    await editor_cog.start_edit_process(button_interaction, notification_id)
                                except Exception as e:
                                    print(f"Error in starting edit process: {e}")
                            else:
                                await button_interaction.response.send_message(
                                    "❌ Editor module not found!",
                                    ephemeral=True
                                )

                    class ToggleButton(discord.ui.Button):
                        def __init__(self, cog, notification_id, edit_button, select):
                            self.cog = cog
                            self.notification_id = notification_id
                            self.edit_button = edit_button
                            self.select = select

                            self.cog.cursor.execute("""
                                SELECT is_enabled FROM bear_notifications WHERE id = ? 
                            """, (self.notification_id,))
                            current_status = self.cog.cursor.fetchone()

                            initial_label = "🟢 Notification is active" if current_status and current_status[
                                0] else "🔴 Notification is inactive"
                            super().__init__(label=initial_label,
                                             style=discord.ButtonStyle.success if current_status and current_status[
                                                 0] else discord.ButtonStyle.danger)

                        async def callback(self, interaction: discord.Interaction):
                            try:
                                self.cog.cursor.execute("""
                                    SELECT is_enabled FROM bear_notifications WHERE id = ? 
                                """, (self.notification_id,))
                                current_status = self.cog.cursor.fetchone()

                                if current_status is None:
                                    await interaction.response.send_message("❌ Notification not found.", ephemeral=True)
                                    return

                                new_status = not bool(current_status[0])

                                result = await self.cog.toggle_notification(self.notification_id, new_status)

                                if result:
                                    new_label = "🟢 Notification is active" if new_status else "🔴 Notification is inactive"
                                    new_style = discord.ButtonStyle.success if new_status else discord.ButtonStyle.danger
                                    self.label = new_label
                                    self.style = new_style

                                    await interaction.response.edit_message(view=view)

                                else:
                                    await interaction.response.send_message("❌ Failed to toggle notification.",
                                                                            ephemeral=True)

                            except Exception as e:
                                print(f"[ERROR] Exception in ToggleButton callback: {e}")
                                await interaction.response.send_message(
                                    "❌ An error occurred while toggling notification!", ephemeral=True
                                )

                    class ChangeChannelButton(discord.ui.Button):
                        def __init__(self, cog, notification_id, channel_exists):
                            self.cog = cog
                            self.notification_id = notification_id
                            # Only show button if channel doesn't exist
                            if not channel_exists:
                                super().__init__(label="📝 Change Channel", style=discord.ButtonStyle.primary)
                            else:
                                super().__init__(label="📝 Change Channel", style=discord.ButtonStyle.secondary)

                        async def callback(self, interaction: discord.Interaction):
                            try:
                                # Get current notification data
                                self.cog.cursor.execute("""
                                    SELECT channel_id, hour, minute, timezone, description, mention_type,
                                           repeat_minutes, next_notification, notification_type
                                    FROM bear_notifications WHERE id = ?
                                """, (self.notification_id,))
                                notif_data = self.cog.cursor.fetchone()

                                if not notif_data:
                                    await interaction.response.send_message("❌ Notification not found.", ephemeral=True)
                                    return

                                # Create channel selector view
                                channel_select = discord.ui.ChannelSelect(
                                    placeholder="Select a new channel for this notification",
                                    channel_types=[discord.ChannelType.text],
                                    min_values=1,
                                    max_values=1
                                )

                                async def channel_select_callback(select_interaction: discord.Interaction):
                                    try:
                                        new_channel_id = int(select_interaction.data["values"][0])
                                        new_channel = select_interaction.guild.get_channel(new_channel_id)

                                        # Check if bot has permissions in the new channel
                                        if not new_channel.permissions_for(select_interaction.guild.me).send_messages:
                                            await select_interaction.response.send_message(
                                                "❌ I don't have permission to send messages in that channel!",
                                                ephemeral=True
                                            )
                                            return

                                        # Update the notification's channel
                                        self.cog.cursor.execute("""
                                            UPDATE bear_notifications
                                            SET channel_id = ?
                                            WHERE id = ?
                                        """, (new_channel_id, self.notification_id))
                                        self.cog.conn.commit()

                                        await select_interaction.response.send_message(
                                            f"✅ Notification channel updated to <#{new_channel_id}>!",
                                            ephemeral=True
                                        )

                                    except Exception as e:
                                        print(f"[ERROR] Error updating channel: {e}")
                                        await select_interaction.response.send_message(
                                            "❌ An error occurred while updating the channel.",
                                            ephemeral=True
                                        )

                                channel_select.callback = channel_select_callback
                                temp_view = discord.ui.View()
                                temp_view.add_item(channel_select)

                                await interaction.response.send_message(
                                    "Select a new channel for this notification:",
                                    view=temp_view,
                                    ephemeral=True
                                )

                            except Exception as e:
                                print(f"[ERROR] Exception in ChangeChannelButton callback: {e}")
                                await interaction.response.send_message(
                                    "❌ An error occurred!",
                                    ephemeral=True
                                )

                    view.add_item(select)
                    if total_pages > 1:
                        view.add_item(prev_button)
                        view.add_item(next_button)
                    view.add_item(search_button)
                    view.add_item(reset_button)
                    view.add_item(EditButton())
                    view.add_item(ToggleButton(self.cog, notification_id, EditButton(), select))
                    view.add_item(PreviewButton(self.cog, notification_id))
                    view.add_item(AdvancedSettingsButton(self.cog, notification_id))
                    view.add_item(ChangeChannelButton(self.cog, notification_id, channel is not None))
                    view.add_item(DeleteButton(self.cog, notification_id))

                    editor_cog = self.cog.bot.get_cog('NotificationEditor')
                    view.editor_cog = editor_cog

                    # Build help text - add Change Channel info if channel is missing
                    help_text = (
                        "- **🔍 Search:** Filter the menu options based on specific keywords\n"
                        "- **📝 Edit:** Modify notification details.\n"
                        "- **⚙️ Notification is active/inactive:** Toggles between enabling or disabling the notification.\n"
                        "  - -# Click to toggle between enabling or disabling.\n"
                        "  - -# Enabling a non-repeating notification will keep its time but change its date to today's date or tomorrow if the time had passed.\n"
                        "- **👀 Preview:** See how the notification will look when it's sent.\n"
                        "- **🧹 Message Cleanup:** Configure custom message deletion delay for this notification.\n"
                    )

                    if not channel:
                        help_text += "- **📝 Change Channel:** ⚠️ Update the channel for this notification (current channel is unavailable).\n"
                    else:
                        help_text += "- **📝 Change Channel:** Update the channel for this notification.\n"

                    help_text += "- **🗑️ Delete:** Remove the selected notification.\n\n"

                    await select_interaction.response.edit_message(
                        content=help_text,
                        embed=details_embed,
                        view=view
                    )

                except Exception as e:
                    print(f"[ERROR] Error in select callback: {e}")
                    await select_interaction.response.send_message(
                        "❌ An error occurred while editing notification!",
                        ephemeral=True
                    )

            select.callback = select_callback

            view = discord.ui.View()
            view.add_item(select)
            if total_pages > 1:
                view.add_item(prev_button)
                view.add_item(next_button)
            view.add_item(search_button)
            view.add_item(reset_button)

            await interaction.response.send_message(
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"[ERROR] Error in manage_notification button: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while starting the edit process!",
                ephemeral=True
            )


    @discord.ui.button(
        label="Schedule Boards",
        emoji="📅",
        style=discord.ButtonStyle.primary,
        custom_id="schedule_boards",
        row=1
    )
    async def schedule_boards_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            schedule_cog = self.cog.bot.get_cog("BearTrapSchedule")
            if schedule_cog:
                await schedule_cog.show_main_menu(interaction, force_new=True)
            else:
                await interaction.response.send_message(
                    "❌ Schedule board system is not loaded!",
                    ephemeral=True
                )
        except Exception as e:
            print(f"[ERROR] Error in schedule boards button: {e}")
            traceback.print_exc()
            await interaction.response.send_message(
                "❌ An error occurred while loading schedule boards!",
                ephemeral=True
            )

    @discord.ui.button(
        label="Event Templates",
        emoji="📚",
        style=discord.ButtonStyle.primary,
        custom_id="browse_templates",
        row=1
    )
    async def notification_templates_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            templates_cog = self.cog.bot.get_cog("BearTrapTemplates")
            if templates_cog:
                await templates_cog.show_templates(interaction)
            else:
                await interaction.response.send_message(
                    "❌ Notification Templates module not found.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"Error loading templates: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while loading templates.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Settings",
        emoji="⚙️",
        style=discord.ButtonStyle.secondary,
        custom_id="settings",
        row=1
    )
    async def settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            # Get current settings
            self.cursor.execute("""
                SELECT delete_messages_enabled, default_delete_delay_minutes
                FROM bear_trap_settings
                WHERE guild_id = ?
            """, (interaction.guild_id,))
            row = self.cursor.fetchone()

            if row:
                delete_enabled, default_delay = row
            else:
                # Create default settings
                delete_enabled, default_delay = 1, 60
                self.cursor.execute("""
                    INSERT INTO bear_trap_settings (guild_id, delete_messages_enabled, default_delete_delay_minutes)
                    VALUES (?, ?, ?)
                """, (interaction.guild_id, delete_enabled, default_delay))
                self.conn.commit()

            # Create settings view
            settings_view = SettingsView(self.cog, delete_enabled, default_delay)

            await interaction.response.send_message(
                embed=settings_view.build_settings_embed(),
                view=settings_view,
                ephemeral=True
            )
        except Exception as e:
            print(f"Error loading settings: {e}")
            traceback.print_exc()
            await interaction.response.send_message(
                "❌ An error occurred while loading settings.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Main Menu",
        emoji="🏠",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self.cog.check_admin(interaction):
            return
        try:
            alliance_cog = self.cog.bot.get_cog("Alliance")
            if alliance_cog:
                await alliance_cog.show_main_menu(interaction)
        except Exception as e:
            print(f"Error returning to main menu: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while returning to main menu.",
                ephemeral=True
            )

class ChannelSelectView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, original_message, event_type=None):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.original_message = original_message
        self.event_type = event_type

        self.add_item(ChannelSelectMenu(self))

class ChannelSelectMenu(discord.ui.ChannelSelect):
    def __init__(self, view):
        self.parent_view = view
        super().__init__(
            placeholder="Select a channel for notifications",
            channel_types=[
                discord.ChannelType.text,
                discord.ChannelType.private,
                discord.ChannelType.news,
                discord.ChannelType.forum,
                discord.ChannelType.news_thread,
                discord.ChannelType.public_thread,
                discord.ChannelType.private_thread,
                discord.ChannelType.stage_voice
            ],
            min_values=1,
            max_values=1
        )

    async def callback(self, interaction: discord.Interaction):
        try:
            channel = self.values[0]
            actual_channel = interaction.guild.get_channel(channel.id)
            if not actual_channel.permissions_for(interaction.guild.me).send_messages:
                await interaction.response.send_message(
                    "❌ I don't have permission to send messages in this channel!",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title="⏰ Select Notification Type",
                description=(
                    "Choose when to send notifications:\n\n"
                    "**30m, 10m, 5m & Time**\n"
                    "• 30 minutes before\n"
                    "• 10 minutes before\n"
                    "• 5 minutes before\n"
                    "• When time's up\n\n"
                    "**10m, 5m & Time**\n"
                    "• 10 minutes before\n"
                    "• 5 minutes before\n"
                    "• When time's up\n\n"
                    "**5m & Time**\n"
                    "• 5 minutes before\n"
                    "• When time's up\n\n"
                    "**Only 5m**\n"
                    "• Only 5 minutes before\n\n"
                    "**Only Time**\n"
                    "• Only when time's up\n\n"
                    "**Custom Times**\n"
                    "• Set your own notification times"
                ),
                color=discord.Color.blue()
            )

            view = NotificationTypeView(
                self.parent_view.cog,
                self.parent_view.start_date,
                self.parent_view.hour,
                self.parent_view.minute,
                self.parent_view.timezone,
                self.parent_view.message_data,
                channel.id,
                self.parent_view.original_message,
                event_type=self.parent_view.event_type
            )

            await interaction.response.edit_message(
                content=None,
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in channel select callback: {e}")
            try:
                await interaction.response.send_message(
                    "❌ An error occurred while processing your selection!",
                    ephemeral=True
                )
            except discord.InteractionResponded:
                await interaction.followup.send(
                    "❌ An error occurred while processing your selection!",
                    ephemeral=True
                )

async def setup(bot):
    await bot.add_cog(BearTrap(bot))