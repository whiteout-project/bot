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

class BearTrap(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        self.db_path = 'db/beartime.sqlite'
        os.makedirs('db', exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # Rate limiting for channel unavailable warnings
        self.channel_warning_timestamps = {}
        self.channel_warning_interval = 300

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

        try:
            self.cursor.execute("SELECT mention_message FROM bear_notification_embeds LIMIT 1")
        except sqlite3.OperationalError:
            self.cursor.execute("ALTER TABLE bear_notification_embeds ADD COLUMN mention_message TEXT")

        self.conn.commit()

    async def cog_load(self):

        self.notification_task = asyncio.create_task(self.check_notifications())

    async def cog_unload(self):

        if hasattr(self, 'notification_task'):
            self.notification_task.cancel()

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
                                repeat_48h: bool, repeat_minutes: int = 0,
                                selected_weekdays: list[int] = None) -> int:
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
                mention_type, repeat_enabled, repeat_minutes, created_by, next_notification)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (guild_id, channel_id, hour, minute, timezone, notification_description, notification_type,
                  mention_type, 1 if repeat_48h else 0, repeat_minutes, created_by,
                  next_notification.isoformat()))

            notification_id = self.cursor.lastrowid

            if embed_data:
                await self.save_notification_embed(notification_id, embed_data)
            if repeat_minutes == "fixed":
                await self.save_notification_fixed(notification_id, selected_weekdays)

            self.conn.commit()
            return notification_id
        except Exception as e:
            print(f"Error saving notification: {e}")
            raise

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

    async def check_notifications(self):
        await self.bot.wait_until_ready()
        while not self.bot.is_closed():
            try:

                self.cursor.execute("""
                    SELECT * FROM bear_notifications 
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

    async def process_notification(self, notification):
        try:
            (id, guild_id, channel_id, hour, minute, timezone, description,
             notification_type, mention_type, repeat_enabled, repeat_minutes,
             is_enabled, created_at, created_by, last_notification,
             next_notification) = notification

            weekly_repeat_days = []
            if repeat_enabled and repeat_minutes == 0:
                self.cursor.execute("SELECT weekday FROM notification_days WHERE notification_id = ?", (id,))
                weekly_repeat_days = [row[0] for row in self.cursor.fetchall()]


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

                    elif repeat_minutes == "fixed":
                        self.cursor.execute("""
                                    SELECT weekday FROM notification_days
                                    WHERE notification_id = ?
                                """, (id,))
                        rows = self.cursor.fetchall()
                        notification_days = set()

                        for row in rows:
                            parts = row[0].split('|')
                            notification_days.update(int(p) for p in parts)

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
                                    if "@tag" in title:
                                        title = title.replace("@tag", mention_text)
                                    embed.title = title

                                description = embed_data.get("description", "")
                                if description and isinstance(description, str):
                                    description = description.replace("%t", time_text)
                                    description = description.replace("{time}", time_text)
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
                                    if "@tag" in footer_text:
                                        footer_text = footer_text.replace("@tag", mention_text)
                                    embed.set_footer(text=footer_text)

                                author_text = embed_data.get("author", "")
                                if author_text and isinstance(author_text, str):
                                    author_text = author_text.replace("%t", time_text)
                                    author_text = author_text.replace("{time}", time_text)
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
                                            await channel.send(mention_message)
                                        else:
                                            mention_text = mention_text.replace("%t", time_text)
                                            mention_text = mention_text.replace("{time}", time_text)
                                            await channel.send(mention_text)
                                    await channel.send(embed=embed)
                                else:
                                    if rounded_time > 0:
                                        await channel.send(
                                            f"{mention_text} ⏰ **Notification** will start in **{time_text}**!")
                                    else:
                                        await channel.send(f"{mention_text} ⏰ **Notification**")
                            except Exception as e:
                                print(f"Error creating embed: {e}")
                                if rounded_time > 0:
                                    await channel.send(
                                        f"{mention_text} ⏰ **Error sending embed notification** will start in **{time_text}**!")
                                else:
                                    await channel.send(f"{mention_text} ⏰ **Error sending embed notification**")
                    except Exception as e:
                        print(f"Error creating embed: {e}")
                        if rounded_time > 0:
                            await channel.send(
                                f"{mention_text} ⏰ **Error sending embed notification** will start in **{time_text}**!")
                        else:
                            await channel.send(f"{mention_text} ⏰ **Error sending embed notification**")
                else:
                    actual_description = description
                    if description.startswith("CUSTOM_TIMES:"):
                        parts = description.split("|", 1)
                        if len(parts) > 1:
                            actual_description = parts[1]

                    if actual_description.startswith("PLAIN_MESSAGE:"):
                        actual_description = actual_description.replace("PLAIN_MESSAGE:", "", 1)

                    if "@tag" in actual_description or "%t" in actual_description or "{time}" in actual_description:
                        message = actual_description
                        if "@tag" in message:
                            message = message.replace("@tag", mention_text)
                        if "%t" in message:
                            message = message.replace("%t", time_text)
                        if "{time}" in message:
                            message = message.replace("{time}", time_text)
                        await channel.send(message)
                    else:
                        if rounded_time > 0:
                            await channel.send(
                                f"{mention_text} ⏰ **{actual_description}** will start in **{time_text}**!")
                        else:
                            await channel.send(f"{mention_text} ⏰ **{actual_description}**")

                current_time_str = now.strftime('%Y-%m-%d %H:%M:%S')
                self.cursor.execute("""
                    INSERT INTO notification_history (notification_id, notification_time, sent_at)
                    VALUES (?, ?, ?)
                """, (id, current_time, current_time_str))

                self.cursor.execute("""
                    UPDATE bear_notifications 
                    SET last_notification = ? 
                    WHERE id = ?
                """, (now.isoformat(), id))

                if not repeat_enabled and current_time == min(notification_times):
                    print(f"Warning: (current_time: {current_time}) repeat isnt enabled and last notification was sent for notification {id} at {notification_times}. Disabling the notification")
                    self.cursor.execute("""
                        UPDATE bear_notifications 
                        SET is_enabled = 0 
                        WHERE id = ?
                    """, (id,))

                if rounded_time == 0:
                    if repeat_enabled:
                        if isinstance(repeat_minutes, int) and repeat_minutes > 0:
                            current_next = datetime.fromisoformat(next_notification)
                            next_time = current_next + timedelta(minutes=repeat_minutes)

                        elif repeat_minutes == "fixed":
                            self.cursor.execute("""
                                        SELECT weekday FROM notification_days
                                        WHERE notification_id = ?
                                    """, (id,))
                            rows = self.cursor.fetchall()
                            notification_days = set()

                            for row in rows:
                                parts = row[0].split('|')
                                notification_days.update(int(p) for p in parts)

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

                    else:
                        print(f"Warning: (current_time: {current_time}) repeat isnt enabled (repeat = {repeat_enabled}) or repeat minutes arent > 0 (repeat minutes = {repeat_minutes}) for notification {id}. Disabling notification")
                        self.cursor.execute("""
                            UPDATE bear_notifications 
                            SET is_enabled = 0 
                            WHERE id = ?
                        """, (id,))

                self.conn.commit()

        except Exception as e:
            error_msg = f"[ERROR] Error processing notification {id}: {str(e)}\nType: {type(e)}\nTrace: {traceback.format_exc()}"
            print(error_msg)

    async def get_notifications(self, guild_id: int) -> list:
        try:
            self.cursor.execute("""
                SELECT * FROM bear_notifications 
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
            self.cursor.execute("""SELECT id FROM bear_notifications WHERE id = ?""", (notification_id,))
            result = self.cursor.fetchone()
            if not result:
                return False  # If the notification doesn't exist, return False

            # If the notification exists, proceed to delete
            self.cursor.execute("""DELETE FROM bear_notifications WHERE id = ?""", (notification_id,))
            self.conn.commit()  # Commit the changes using the same connection as toggle_notification
            return True
        except Exception as e:
            print(f"[ERROR] Error deleting notification {notification_id}: {e}")
            return False

    async def toggle_notification(self, notification_id: int, enabled: bool) -> bool:
        try:

            self.cursor.execute("""
                SELECT is_enabled FROM bear_notifications WHERE id = ?
            """, (notification_id,))
            result = self.cursor.fetchone()
            if not result:
                return False

            self.cursor.execute("""
                UPDATE bear_notifications 
                SET is_enabled = ? 
                WHERE id = ?
            """, (1 if enabled else 0, notification_id))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error toggling notification: {e}")
            return False

    def get_world_times(self):
        current_utc = datetime.now(pytz.UTC)
        times = {
            "UTC": current_utc,
            "US/Pacific": current_utc.astimezone(pytz.timezone('US/Pacific')),
            "US/Eastern": current_utc.astimezone(pytz.timezone('US/Eastern')),
            "Europe/London": current_utc.astimezone(pytz.timezone('Europe/London')),
            "Europe/Istanbul": current_utc.astimezone(pytz.timezone('Europe/Istanbul')),
            "Asia/Tokyo": current_utc.astimezone(pytz.timezone('Asia/Tokyo')),
        }
        return times

    async def show_bear_trap_menu(self, interaction: discord.Interaction):
        try:
            times = self.get_world_times()
            time_display = "\n".join([
                f"🌍 **{zone}:** {time.strftime('%H:%M:%S')}"
                for zone, time in times.items()
            ])

            embed = discord.Embed(
                title="🔔 Notification System",
                description=(
                    "Configure time notification settings:\n\n"
                    "**Current World Times**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{time_display}\n\n"
                    "**Available Operations**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "⏰ **Set Time**\n"
                    "└ Configure notification time\n"
                    "└ Not just for Bear! Use it for any event:\n"
                    "   Bear - KE - Frostfire - CJ and everything else\n"
                    "└ Add unlimited notifications\n\n"
                    "📋 **Manage notification**\n"
                    "└ Edit, Enable/disable, see a preview, and delete\n\n"
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
                                     message_data, channels):
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
                interaction.message
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
                 mention_type, original_message):
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
                repeat_48h=repeat,
                repeat_minutes=repeat_minutes,
                selected_weekdays=selected_weekdays
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
                mention_display = f"@{role.name}" if role else f"Role: {role_id}"
            elif self.mention_type.startswith("member_"):
                member_id = int(self.mention_type.split('_')[1])
                member = interaction.guild.get_member(member_id)
                mention_display = f"@{member.display_name}" if member else f"Member: {member_id}"
            else:
                mention_display = "No Mention"

            if not repeat:
                repeat_text = "❌ No repeat"
            elif interval_text:
                repeat_text = f"🔄 Repeats every {interval_text}"
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

            embed = discord.Embed(
                title="✅ Notification Set Successfully",
                description=(
                    f"**📅 Date:** {self.start_date.strftime('%d/%m/%Y')}\n"
                    f"**⏰ Time:** {self.hour:02d}:{self.minute:02d} {self.timezone}\n"
                    f"**📢 Channel:** <#{self.channel_id}>\n"
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
            repeat_minutes="fixed",
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
    def __init__(self, cog, start_date, hour, minute, timezone, original_message):
        super().__init__(timeout=None)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.original_message = original_message
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
            example_time = "30 minutes"
            embed = discord.Embed(color=self.embed_data.get("color", discord.Color.blue().value))

            if "title" in self.embed_data:
                title = self.embed_data["title"].replace("%t", example_time).replace("{time}", example_time)
                embed.title = title
            if "description" in self.embed_data:
                description = self.embed_data["description"].replace("%t", example_time).replace("{time}", example_time)
                embed.description = description
            if "footer" in self.embed_data:
                footer = self.embed_data["footer"].replace("%t", example_time).replace("{time}", example_time)
                embed.set_footer(text=footer)
            if "author" in self.embed_data:
                author = self.embed_data["author"].replace("%t", example_time).replace("{time}", example_time)
                embed.set_author(name=author)
            if "image_url" in self.embed_data and self.embed_data["image_url"]:
                embed.set_image(url=self.embed_data["image_url"])
            if "thumbnail_url" in self.embed_data and self.embed_data["thumbnail_url"]:
                embed.set_thumbnail(url=self.embed_data["thumbnail_url"])

            mention_preview = self.embed_data.get('mention_message', '@tag')
            if mention_preview:
                mention_preview = mention_preview.replace("%t", example_time)
                mention_preview = mention_preview.replace("{time}", example_time)

            content = (
                "📝 **Embed Editor**\n\n"
                "**Note:** \n"
                "• Use `%t` or `{time}` to show the remaining time\n"
                "• Use `@tag` for mentions (will be replaced with the actual mention)\n"
                "• You can use these in title, description, footer, and author fields\n"
                "• Time will automatically show with appropriate units (minutes/hours/days)\n\n"
                f"Currently showing '{example_time}' as an example.\n\n"
                f"**Current Mention Message Preview:**\n"
                f"{mention_preview}\n\n"
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
                placeholder="Example: ⏰ Bear starts in {time}!",
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
                placeholder="Example: Get ready for Bear! Only {time} remaining.",
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
                interaction.guild.text_channels
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
            example_time = "30 minutes"

            embed = discord.Embed(
                title="Bear Trap Notification",
                description="Get ready for Bear! Only %t remaining.",
                color=discord.Color.blue()
            )
            embed.set_footer(text="Notification System")

            content = (
                "📝 **Embed Editor**\n\n"
                "**Note:** \n"
                "• Use `%t` or `{time}` to show the remaining time\n"
                "• Use `@tag` for mentions (will be replaced with the actual mention)\n"
                "• You can use these in title, description, footer, and author fields\n"
                "• Time will automatically show with appropriate units (minutes/hours/days)\n\n"
                f"Currently showing '{example_time}' as an example."
            )

            view = EmbedEditorView(
                self.cog,
                self.start_date,
                self.hour,
                self.minute,
                self.timezone,
                interaction.message
            )
            view.embed_data = {
                "title": embed.title,
                "description": embed.description,
                "color": embed.color.value,
                "footer": "Notification System"
            }

            await interaction.response.edit_message(
                content=content,
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in embed_message: {e}")
            await interaction.followup.send(
                "❌ An error occurred while starting the embed editor!",
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
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, channel_id, original_message):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.channel_id = channel_id
        self.original_message = original_message

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
                                 self.channel_id, self.original_message)
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
                self.original_message
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
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, channel_id, original_message):
        super().__init__(title="Set Custom Notification Times")
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.channel_id = channel_id
        self.original_message = original_message

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
                self.original_message
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
                 original_message):
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
                self.original_message
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


class BearTrapView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog
        self.conn = sqlite3.connect('db/beartime.sqlite')
        self.cursor = self.conn.cursor()

    @discord.ui.button(
        label="Set Time",
        emoji="⏰",
        style=discord.ButtonStyle.primary,
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

    @discord.ui.button(
        label="Manage notification",
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
                    status = "🟢 Enabled" if notif[11] else "🔴 Disabled"

                    if "EMBED_MESSAGE:" in notif[6]:
                        self.cog.cursor.execute("""
                            SELECT title, description 
                            FROM bear_notification_embeds 
                            WHERE notification_id = ? 
                        """, (notif[0],))
                        embed_data = self.cog.cursor.fetchone()

                        if embed_data and embed_data[0]:
                            display_description = f"📝 Embed: {embed_data[0]}"
                        else:
                            display_description = "📝 Embed Message"
                    else:
                        display_description = notif[6].split('|')[-1] if '|' in notif[6] else notif[6]
                        if display_description.startswith("PLAIN_MESSAGE:"):
                            display_description = display_description.replace("PLAIN_MESSAGE:", "✍️ ")

                    options.append(
                        discord.SelectOption(
                            label=f"{notif[3]:02d}:{notif[4]:02d} - {display_description[:30]}",
                            description=f"ID: {notif[0]} | {status}",
                            value=f"{notif[0]}|embed" if "EMBED_MESSAGE:" in notif[6] else f"{notif[0]}|plain"
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

                    elif repeat_minutes == "fixed":
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

                    details_embed = discord.Embed(
                        title=f"📋 Notification Details",
                        description=(
                            f"**📅 Next Notification date:** {datetime.fromisoformat(selected_notif[15]).strftime('%d/%m/%Y')}\n"
                            f"**⏰ Time:** {selected_notif[3]:02d}:{selected_notif[4]:02d} ({selected_notif[5]})\n"
                            f"**📢 Channel:** <#{selected_notif[2]}>\n"
                            f"**📝 Description:** {selected_notif[6]}\n\n"
                            f"**⚙️ Notification Type:** \n{notification_type_desc}\n\n"
                            f"**👥 Mention:** {mention_display}\n"
                            f"**🔄 Repeat:** {formatted_repeat}\n"),
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
                                    """SELECT channel_id, hour, minute, description, mention_type, next_notification
                                       FROM bear_notifications WHERE id = ?""",
                                    (self.notification_id,)
                                )
                                selected_notif = self.cog.cursor.fetchone()

                                if not selected_notif:
                                    await interaction.response.send_message("❌ Notification not found.", ephemeral=True)
                                    return

                                channel_id, hours, minutes, description, mention_type, next_notification = selected_notif

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
                                    mention_preview = mention_preview.replace("@tag", mention_display)

                                    preview_embed = discord.Embed(
                                        title=embed_data['title'] if embed_data['title'] else "No Title",
                                        description=embed_data['description'] if embed_data[
                                            'description'] else "No Description",
                                        color=embed_data['color'] if embed_data['color'] else discord.Color.blue()
                                    )

                                    if embed_data['image_url']:
                                        preview_embed.set_image(url=embed_data['image_url'])
                                    if embed_data['thumbnail_url']:
                                        preview_embed.set_thumbnail(url=embed_data['thumbnail_url'])
                                    if embed_data['footer']:
                                        preview_embed.set_footer(text=embed_data['footer'])
                                    if embed_data['author']:
                                        preview_embed.set_author(name=embed_data['author'])

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
                                    message_preview = message_preview.replace("@tag", mention_display)

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

                    view.add_item(select)
                    if total_pages > 1:
                        view.add_item(prev_button)
                        view.add_item(next_button)
                    view.add_item(search_button)
                    view.add_item(reset_button)
                    view.add_item(EditButton())
                    view.add_item(ToggleButton(self.cog, notification_id, EditButton(), select))
                    view.add_item(PreviewButton(self.cog, notification_id))
                    view.add_item(DeleteButton(self.cog, notification_id))

                    editor_cog = self.cog.bot.get_cog('NotificationEditor')
                    view.editor_cog = editor_cog

                    await select_interaction.response.edit_message(
                        content=(
                            "- **🔍 Search:** Filter the menu options based on specific keywords\n"
                            "- **📝 Edit:** Modify notification details.\n"
                            "- **⚙️ Notification is active/inactive:** Toggles between enabling or disabling the notification.\n"
                            "  - -# Click to toggle between enabling or disabling.\n"
                            "  - -# Enabling a non-repeating notification will keep its time but change its date to today's date or tomorrow if the time had passed.\n"
                            "- **👀 Preview:** See how the notification will look when it's sent.\n"
                            "- **🗑️ Delete:** Remove the selected notification.\n\n"
                        ),
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


class ChannelSelectView(discord.ui.View):
    def __init__(self, cog, start_date, hour, minute, timezone, message_data, original_message):
        super().__init__(timeout=300)
        self.cog = cog
        self.start_date = start_date
        self.hour = hour
        self.minute = minute
        self.timezone = timezone
        self.message_data = message_data
        self.original_message = original_message

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
                self.parent_view.original_message
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