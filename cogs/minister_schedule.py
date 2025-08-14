import discord
from discord import app_commands
from discord.ext import commands
import asyncio
import sqlite3
import aiohttp
import hashlib
from aiohttp_socks import ProxyConnector
import time

SECRET = 'tB87#kPtkxqOS2'

class ChannelSelectView(discord.ui.View):
    def __init__(self, bot, context: str):
        super().__init__(timeout=None)
        self.add_item(ChannelSelect(bot, context))

class ChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, bot, context: str):
        self.bot = bot
        self.context = context

        super().__init__(
            placeholder="Select a channel...",
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
        selected_channel = self.values[0]
        channel_id = selected_channel.id

        try:
            svs_conn = sqlite3.connect("db/svs.sqlite")
            svs_cursor = svs_conn.cursor()
            
            # Check if we're updating a minister channel
            if self.context.endswith("channel"):
                # Get the activity name from the context (e.g., "Construction Day channel" -> "Construction Day")
                activity_name = self.context.replace(" channel", "")
                
                # Check if this is one of the minister activity channels
                if activity_name in ["Construction Day", "Research Day", "Troops Training Day"]:
                    # Get the old channel ID if it exists
                    svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (self.context,))
                    old_channel_row = svs_cursor.fetchone()
                    
                    if old_channel_row:
                        old_channel_id = int(old_channel_row[0])
                        # Get the message ID for this activity
                        svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (activity_name,))
                        message_row = svs_cursor.fetchone()
                        
                        if message_row and old_channel_id != channel_id:
                            # Delete the old message if channel has changed
                            message_id = int(message_row[0])
                            guild = interaction.guild
                            if guild:
                                old_channel = guild.get_channel(old_channel_id)
                                if old_channel:
                                    try:
                                        old_message = await old_channel.fetch_message(message_id)
                                        await old_message.delete()
                                    except:
                                        pass  # Message might already be deleted
                            
                            # Remove the message reference so it will be recreated in the new channel
                            svs_cursor.execute("DELETE FROM reference WHERE context=?", (activity_name,))
            
            # Update the channel reference
            svs_cursor.execute("""
                INSERT INTO reference (context, context_id)
                VALUES (?, ?)
                ON CONFLICT(context) DO UPDATE SET context_id = excluded.context_id;
            """, (self.context, channel_id))
            svs_conn.commit()
            
            # Trigger message update in the new channel
            if self.context.endswith("channel"):
                activity_name = self.context.replace(" channel", "")
                if activity_name in ["Construction Day", "Research Day", "Troops Training Day"]:
                    minister_menu_cog = self.bot.get_cog("MinisterMenu")
                    if minister_menu_cog:
                        await minister_menu_cog.update_channel_message(activity_name)
            
            svs_conn.close()

            # Check if this is being called from the minister menu system
            minister_menu_cog = self.bot.get_cog("MinisterMenu")
            if minister_menu_cog and self.context.endswith("channel"):
                # Return to channel configuration menu with confirmation
                embed = discord.Embed(
                    title="📝 Channel Setup",
                    description=(
                        f"✅ **{self.context}** set to <#{channel_id}>\n\n"
                        "Configure channels for minister scheduling:\n\n"
                        "**Channel Types**\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n"
                        "🔨 **Construction Channel** - Shows available Construction Day slots\n"
                        "🔬 **Research Channel** - Shows available Research Day slots\n"
                        "⚔️ **Training Channel** - Shows available Training Day slots\n"
                        "📄 **Log Channel** - Receives add/remove notifications\n"
                        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        "Select a channel type to configure:"
                    ),
                    color=discord.Color.green()
                )

                # Get the ChannelConfigurationView from minister_menu
                import sys
                minister_menu_module = minister_menu_cog.__class__.__module__
                ChannelConfigurationView = getattr(sys.modules[minister_menu_module], 'ChannelConfigurationView')
                
                view = ChannelConfigurationView(self.bot, minister_menu_cog)
                
                await interaction.response.edit_message(
                    content=None, # Clear the "Select a channel for..." content
                    embed=embed,
                    view=view
                )
            else:
                # Fallback for other contexts
                await interaction.response.edit_message(
                    content=f"✅ `{self.context}` set to <#{channel_id}>.\n\nChannel configured successfully!",
                    view=None
                )

        except Exception as e:
            try:
                await interaction.response.send_message(
                    f"❌ Failed to update:\n```{e}```",
                    ephemeral=True
                )
            except discord.InteractionResponded:
                await interaction.followup.send(
                    f"❌ Failed to update:\n```{e}```",
                    ephemeral=True
                )

class MinisterSchedule(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.users_conn = sqlite3.connect('db/users.sqlite')
        self.users_cursor = self.users_conn.cursor()
        self.settings_conn = sqlite3.connect('db/settings.sqlite')
        self.settings_cursor = self.settings_conn.cursor()
        self.alliance_conn = sqlite3.connect('db/alliance.sqlite')
        self.alliance_cursor = self.alliance_conn.cursor()
        self.svs_conn = sqlite3.connect("db/svs.sqlite")
        self.svs_cursor = self.svs_conn.cursor()

        self.svs_cursor.execute("""
                    CREATE TABLE IF NOT EXISTS appointments (
                        fid INTEGER,
                        appointment_type TEXT,
                        time TEXT,
                        alliance INTEGER,
                        PRIMARY KEY (fid, appointment_type)
                    );
                """)
        self.svs_cursor.execute("""
                    CREATE TABLE IF NOT EXISTS reference (
                        context TEXT PRIMARY KEY,
                        context_id INTEGER
                    );
                """)

        self.svs_conn.commit()

    async def fetch_user_data(self, fid, proxy=None):
        url = 'https://wos-giftcode-api.centurygame.com/api/player'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        current_time = int(time.time() * 1000)
        form = f"fid={fid}&time={current_time}"
        sign = hashlib.md5((form + SECRET).encode('utf-8')).hexdigest()
        form = f"sign={sign}&{form}"

        try:
            connector = ProxyConnector.from_url(proxy) if proxy else None
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(url, headers=headers, data=form, ssl=False) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return response.status
        except Exception as e:
            return None

    async def send_embed_to_channel(self, embed):
        """Sends the embed message to a specific channel."""
        log_channel_id = await self.get_channel_id("minister log channel")
        log_channel = self.bot.get_channel(log_channel_id)

        if log_channel:
            await log_channel.send(embed=embed)
        else:
            print(f"Error: Could not find the log channel please change it to a valid channel")

    async def is_admin(self, user_id: int) -> bool:
        if user_id == self.bot.owner_id:
            return True
        self.settings_cursor.execute("SELECT 1 FROM admin WHERE id=?", (user_id,))
        return self.settings_cursor.fetchone() is not None

    async def show_minister_channel_menu(self, interaction: discord.Interaction):
        # Redirect to the MinisterMenu cog
        minister_cog = self.bot.get_cog("MinisterMenu")
        if minister_cog:
            await minister_cog.show_minister_channel_menu(interaction)
        else:
            await interaction.response.send_message(
                "❌ Minister Menu module not found.",
                ephemeral=True
            )

    # Autocomplete handler for appointment type
    async def appointment_autocomplete(self, interaction: discord.Interaction, current: str):
        try:
            choices = [
                discord.app_commands.Choice(name="Construction Day", value="Construction Day"),
                discord.app_commands.Choice(name="Research Day", value="Research Day"),
                discord.app_commands.Choice(name="Troops Training Day", value="Troops Training Day")
            ]
            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()]
            else:
                filtered_choices = choices

            return filtered_choices
        except Exception as e:
            print(f"Error in appointment type autocomplete: {e}")
            return []

    # Autocomplete handler for names
    async def fid_autocomplete(self, interaction: discord.Interaction, current: str):
        try:
            # Fetch selected appointment type from interaction
            appointment_type = next(
                (option["value"] for option in interaction.data.get("options", []) if option["name"] == "appointment_type"),
                None
            )

            if not appointment_type:
                return []  # If no appointment type is selected, return an empty list

            # Fetch all registered users
            self.users_cursor.execute("SELECT fid, nickname FROM users")
            users = self.users_cursor.fetchall()

            # Fetch users already booked for the selected appointment type
            self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=?", (appointment_type,))
            booked_fids = {row[0] for row in self.svs_cursor.fetchall()}  # Convert to a set for quick lookup

            # Filter out booked users
            available_choices = [
                discord.app_commands.Choice(name=f"{nickname} ({fid})", value=str(fid))
                for fid, nickname in users if fid not in booked_fids
            ]

            # Apply search filtering if the user is typing
            if current:
                filtered_choices = [choice for choice in available_choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = available_choices[:25]

            return filtered_choices
        except Exception as e:
            print(f"Autocomplete for fid failed: {e}")
            return []

    # Autocomplete handler for registered names
    async def registered_fid_autocomplete(self, interaction: discord.Interaction, current: str):
        try:
            # Fetch selected appointment type from interaction
            appointment_type = next(
                (option["value"] for option in interaction.data.get("options", []) if option["name"] == "appointment_type"),
                None
            )

            if not appointment_type:
                return []

            # Fetch users already booked for the selected appointment type
            self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type = ?", (appointment_type,))
            fids = [row[0] for row in self.svs_cursor.fetchall()]
            if not fids:
                return []

            placeholders = ",".join("?" for _ in fids)
            query = f"SELECT fid, nickname FROM users WHERE fid IN ({placeholders})"
            self.users_cursor.execute(query, fids)

            registered_users = self.users_cursor.fetchall()

            # Create choices list
            choices = [
                discord.app_commands.Choice(name=f"{nickname} ({fid})", value=str(fid))
                for fid, nickname in registered_users
            ]

            # Apply search filtering if the user is typing
            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()][:25]
            else:
                filtered_choices = choices[:25]

            return filtered_choices
        except Exception as e:
            print(f"Autocomplete for registered fid failed: {e}")
            return []

    # Autocomplete handler for time
    async def time_autocomplete(self, interaction: discord.Interaction, current: str):
        try:
            appointment_type = next(
                (option["value"] for option in interaction.data.get("options", []) if option["name"] == "appointment_type"),
                None
            )

            if not appointment_type:
                return []

            # Get booked times
            self.svs_cursor.execute("SELECT time FROM appointments WHERE appointment_type=?", (appointment_type,))
            booked_times = {row[0] for row in self.svs_cursor.fetchall()}

            # Generate valid 30-minute interval times in order
            available_times = []
            for hour in range(24):
                for minute in (0, 30):
                    time_slot = f"{hour:02}:{minute:02}"
                    if time_slot not in booked_times:
                        available_times.append(time_slot)

            # Ensure user input is properly formatted (normalize input)
            if current:
                # Normalize single-digit hours (e.g., "1:00" -> "01:00")
                parts = current.split(":")
                if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                    formatted_input = f"{int(parts[0]):02}:{int(parts[1]):02}"
                else:
                    return []  # Invalid format

                # Ensure input is valid 30-minute interval
                valid_times = {f"{hour:02}:{minute:02}" for hour in range(24) for minute in (0, 30)}
                if formatted_input not in valid_times:
                    return []

                # Filter choices based on input
                filtered_choices = [
                    discord.app_commands.Choice(name=time, value=time)
                    for time in available_times if formatted_input in time
                ][:25]
            else:
                filtered_choices = [
                    discord.app_commands.Choice(name=time, value=time)
                    for time in available_times
                ][:25]

            return filtered_choices
        except Exception as e:
            print(f"Error in time autocomplete: {e}")
            return []

    # Autocomplete handler for choices of what to show
    async def choice_autocomplete(self, interaction: discord.Interaction, current: str):
        try:
            choices = [
                discord.app_commands.Choice(name="Show full minister list", value="all"),
                discord.app_commands.Choice(name="Show available slots only", value="available only")
            ]

            if current:
                filtered_choices = [choice for choice in choices if current.lower() in choice.name.lower()]
            else:
                filtered_choices = choices

            return filtered_choices
        except Exception as e:
            print(f"Error in all_or_available autocomplete: {e}")
            return []

    # handler for looping through all times and updating fids to current nickname
    async def update_time_list(self, booked_times, progress_callback=None):
        """
        Generates a list of time slots with their booking details, fetching nicknames from the API.
        Implements rate limit handling and batch processing.
        """
        time_list = []
        booked_fids = {}

        fids_to_fetch = {fid for fid, _ in booked_times.values() if fid}
        fetched_data = {}  # Cache API responses

        for hour in range(24):
            for minute in (0, 30):
                time_slot = f"{hour:02}:{minute:02}"
                booked_fid, booked_alliance = booked_times.get(time_slot, ("", ""))

                booked_nickname = "Unknown"
                if booked_fid:
                    # Check cache first
                    if booked_fid not in fetched_data:
                        while True:
                            if progress_callback:
                                await progress_callback(len(fetched_data), len(fids_to_fetch), waiting=False)

                            data = await self.fetch_user_data(booked_fid)
                            if isinstance(data, dict) and "data" in data:
                                fetched_data[booked_fid] = data["data"].get("nickname", "Unknown")
                                if progress_callback: # Immediate progress update after successful fetch
                                    await progress_callback(len(fetched_data), len(fids_to_fetch), waiting=False)
                                break
                            elif data == 429:
                                if progress_callback:
                                    await progress_callback(len(fetched_data), len(fids_to_fetch), waiting=True)
                                await asyncio.sleep(60) # Rate limit, wait and retry
                            else:
                                fetched_data[booked_fid] = "Unknown"
                                if progress_callback: # Immediate progress update even for failed fetch
                                    await progress_callback(len(fetched_data), len(fids_to_fetch), waiting=False)
                                break

                    booked_nickname = fetched_data.get(booked_fid, "Unknown")

                    # Fetch alliance name
                    self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (booked_alliance,))
                    alliance_data = self.alliance_cursor.fetchone()
                    booked_alliance_name = alliance_data[0] if alliance_data else "Unknown"

                    time_list.append(f"`{time_slot}` - [{booked_alliance_name}]`{booked_nickname}` - `{booked_fid}`")
                else:
                    time_list.append(f"`{time_slot}` - ")

                booked_fids[time_slot] = booked_fid

                # Update progress after processing each time slot
                if progress_callback:
                    await progress_callback(len(fetched_data), len(fids_to_fetch), waiting=False)

        return time_list, booked_fids

    # handler for looping through all times without updating fids
    def generate_time_list(self, booked_times):
        """
        Generates a list of time slots with their booking details.
        """
        time_list = []
        booked_fids = {}
        for hour in range(24):
            for minute in (0, 30):
                time_slot = f"{hour:02}:{minute:02}"
                booked_fid, booked_alliance = booked_times.get(time_slot, ("", ""))
                booked_nickname = ""
                if booked_fid:
                    self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (booked_fid,))
                    user = self.users_cursor.fetchone()
                    booked_nickname = user[0] if user else f"ID: {booked_fid}"

                    self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (booked_alliance,))
                    alliance_data = self.alliance_cursor.fetchone()
                    booked_alliance_name = alliance_data[0] if alliance_data else "Unknown"

                    time_list.append(f"`{time_slot}` - [{booked_alliance_name}]`{booked_nickname}` - `{booked_fid}`")
                else:
                    time_list.append(f"`{time_slot}` - ")
                booked_fids[time_slot] = booked_fid

        return time_list, booked_fids

    # handler for looping through available times
    def generate_available_time_list(self, booked_times):
        """
        Generates a list of only available (non-booked) time slots.
        """
        time_list = []
        for hour in range(24):
            for minute in (0, 30):
                time_slot = f"{hour:02}:{minute:02}"
                if time_slot not in booked_times:  # Only add unbooked slots
                    time_list.append(f"`{time_slot}` - ")

        return time_list
    
    # handler for looping through unavailable times
    def generate_booked_time_list(self, booked_times):
        """
        Generates a list of only booked time slots with their details.
        """
        time_list = []
        for hour in range(24):
            for minute in (0, 30):
                time_slot = f"{hour:02}:{minute:02}"
                if time_slot in booked_times:
                    booked_fid, booked_alliance = booked_times[time_slot]
                    booked_nickname = ""
                    if booked_fid:
                        self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (booked_fid,))
                        user = self.users_cursor.fetchone()
                        booked_nickname = user[0] if user else f"ID: {booked_fid}"

                        self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (booked_alliance,))
                        alliance_data = self.alliance_cursor.fetchone()
                        booked_alliance_name = alliance_data[0] if alliance_data else "Unknown"

                        time_list.append(f"`{time_slot}` - [{booked_alliance_name}]`{booked_nickname}` - `{booked_fid}`")

        return time_list

    # handler to get minister channel
    async def get_channel_id(self, context: str):
        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (context,))
        row = self.svs_cursor.fetchone()
        return int(row[0]) if row else None

    # handler to get minister message from channel to edit it
    async def get_or_create_message(self, context: str, message_content: str, channel: discord.TextChannel):
        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (context,))
        row = self.svs_cursor.fetchone()

        if row:
            message_id = int(row[0])
            try:
                message = await channel.fetch_message(message_id)
                await message.edit(content=message_content)
                return message
            except discord.NotFound:
                pass

        # Send a new message if none found
        new_message = await channel.send(message_content)
        self.svs_cursor.execute(
            "REPLACE INTO reference (context, context_id) VALUES (?, ?)",
            (context, new_message.id)
        )
        self.svs_conn.commit()
        return new_message

    # handler to get guild id
    async def get_log_guild(self, log_guild: discord.Guild) -> discord.Guild | None:
        self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", ("minister guild id",))
        row = self.svs_cursor.fetchone()

        if not row:
            # Save the current guild as main guild if not found
            if log_guild:
                self.svs_cursor.execute(
                    "INSERT INTO reference (context, context_id) VALUES (?, ?)",
                    ("minister guild id", log_guild.id)
                )
                self.svs_conn.commit()
                return log_guild
            else:
                return None
        else:
            guild_id = int(row[0])
            guild = self.bot.get_guild(guild_id)
            if guild:
                return guild
            else:
                return None

    @discord.app_commands.command(name='minister_add', description='Book an appointment slot for a user.')
    @app_commands.autocomplete(appointment_type=appointment_autocomplete, fid=fid_autocomplete, time=time_autocomplete)
    async def minister_add(self, interaction: discord.Interaction, appointment_type: str, fid: str, time: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        try:
            log_guild = await self.get_log_guild(interaction.guild)

            if not log_guild:
                await interaction.followup.send(
                    "Could not find the minister log guild. Make sure the bot is in that server.\n\nIf issue persists, run the `/settings` command --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server")
                return

            # Check minister and log channels
            context = f"{appointment_type}"
            channel_context = f"{appointment_type} channel"
            log_context = "minister log channel"

            channel_id = await self.get_channel_id(channel_context)
            log_channel_id = await self.get_channel_id(log_context)

            channel = log_guild.get_channel(channel_id)
            log_channel = log_guild.get_channel(log_channel_id)

            if (not channel or not log_channel) and interaction.guild.id != log_guild.id:
                await interaction.followup.send(
                    f"Minister channels or log channel are missing. This command must be run in the server:`{log_guild}` to configure missing channels.\n\n"
                    f"If you want to change that to another server, run `/settings` --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server"
                )
                return

            if not channel:
                try:
                    await interaction.followup.send(
                        content=f"Please select a channel to use for `{appointment_type}` notifications:",
                        view=ChannelSelectView(self.bot, channel_context)
                    )
                    return
                except Exception as e:
                    print(f"Failed to select channel: {e}")
                    await interaction.followup.send(f"Could not select the channel: {e}")
                    return

            if not log_channel:
                try:
                    await interaction.followup.send(
                        content=f"Please select a log channel to use:",
                        view=ChannelSelectView(self.bot, log_context)
                    )
                    return
                except Exception as e:
                    print(f"Failed to select channel: {e}")
                    await interaction.followup.send(f"Could not select the channel: {e}")
                    return

            # Normalize time input to always be HH:MM format
            try:
                hours, minutes = map(int, time.split(":"))
                normalized_time = f"{hours:02}:{minutes:02}"
            except ValueError:
                await interaction.followup.send("Invalid time format. Please use HH:MM (e.g., 08:00, 14:30).")
                return

            # Validate 30-minute interval times
            if minutes not in {0, 30}:
                await interaction.followup.send("Invalid time. Appointments can only be booked in 30-minute intervals (e.g., 08:00, 08:30).")
                return

            # Retrieve alliance_id based on fid
            self.users_cursor.execute("SELECT alliance, nickname FROM users WHERE fid=?", (fid,))
            user_data = self.users_cursor.fetchone()

            if not user_data:
                await interaction.followup.send(f"This ID {fid} is not registered.")
                return

            alliance_id, nickname = user_data

            # Retrieve alliance name from alliance_list
            self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
            alliance_result = self.alliance_cursor.fetchone()

            if not alliance_result:
                await interaction.followup.send("Alliance not found for this user.")
                return

            alliance_name = alliance_result[0]

            # Check if the user is already booked for the same appointment type
            self.svs_cursor.execute("SELECT time FROM appointments WHERE fid=? AND appointment_type=?", (fid, appointment_type))
            existing_booking = self.svs_cursor.fetchone()
            if existing_booking:
                await interaction.followup.send(f"{nickname} already has an appointment for {appointment_type} at {existing_booking[0]}.")
                return

            # Check if the time is already booked for this appointment type
            self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=? AND time=?", (appointment_type, normalized_time))
            conflicting_booking = self.svs_cursor.fetchone()
            if conflicting_booking:
                booked_fid = conflicting_booking[0]
                self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (booked_fid,))
                booked_user = self.users_cursor.fetchone()
                booked_nickname = booked_user[0] if booked_user else "Unknown"
                await interaction.followup.send(f"The time {normalized_time} for {appointment_type} is already taken by {booked_nickname}.")
                return

            # Book the slot with the retrieved alliance info
            self.svs_cursor.execute("INSERT INTO appointments (fid, appointment_type, time, alliance) VALUES (?, ?, ?, ?)",
                                      (fid, appointment_type, normalized_time, alliance_id))
            self.svs_conn.commit()

            # Try to get the avatar image
            try:
                data = await self.fetch_user_data(fid)

                if isinstance(data, int) and data == 429:
                    # Rate limit hit
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
                elif data and "data" in data and "avatar_image" in data["data"]:
                    avatar_image = data["data"]["avatar_image"]
                else:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"

            except Exception as e:
                avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"

            # Send embed confirmation to log channel
            embed = discord.Embed(
                title=f"Player added to {appointment_type}",
                description=f"{nickname} ({fid}) from **{alliance_name}** at {normalized_time}",
                color=discord.Color.green()
            )
            embed.set_thumbnail(url=avatar_image)
            embed.set_author(name=f"Added by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)

            await self.send_embed_to_channel(embed)
            await interaction.followup.send(f"Added {nickname} to {time}")

            # Update the appointment list
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (appointment_type,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
            time_list = self.generate_available_time_list(booked_times)

            available_slots = len(time_list) > 0  # True if there are open slots, False if all are booked

            message_content = f"**{appointment_type}** available slots:\n" + "\n".join(
                time_list) if available_slots else f"All appointment slots are filled for {appointment_type}"

            # Update existing message or send a new one in the selected channel
            await self.get_or_create_message(context, message_content, channel)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            await interaction.followup.send(f"An unexpected error occurred while processing the request: {e}")

    @discord.app_commands.command(name='minister_remove', description='Cancel an appointment slot for a user.')
    @app_commands.autocomplete(appointment_type=appointment_autocomplete, fid=registered_fid_autocomplete)
    async def minister_remove(self, interaction: discord.Interaction, appointment_type: str, fid: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)

        try:
            log_guild = await self.get_log_guild(interaction.guild)

            if not log_guild:
                await interaction.followup.send(
                    "Could not find the minister log guild. Make sure the bot is in that server.\n\nIf issue persists, run the `/settings` command --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server")
                return

            # Check minister and log channels
            context = f"{appointment_type}"
            channel_context = f"{appointment_type} channel"
            log_context = "minister log channel"

            channel_id = await self.get_channel_id(channel_context)
            log_channel_id = await self.get_channel_id(log_context)

            channel = log_guild.get_channel(channel_id)
            log_channel = log_guild.get_channel(log_channel_id)

            if (not channel or not log_channel) and interaction.guild.id != log_guild.id:
                await interaction.followup.send(
                    f"Minister channels or log channel are missing. This command must be run in the server:`{log_guild}` to configure missing channels.\n\n"
                    f"If you want to change that to another server, run `/settings` --> Other Features --> Minister Scheduling --> Delete Server ID and try again in the desired server"
                )
                return

            if not channel:
                try:
                    await interaction.followup.send(
                        content=f"Please select a channel to use for `{appointment_type}` notifications:",
                        view=ChannelSelectView(self.bot, channel_context)
                    )
                    return
                except Exception as e:
                    print(f"Failed to select channel: {e}")
                    await interaction.followup.send(f"Could not select the channel: {e}")
                    return

            if not log_channel:
                try:
                    await interaction.followup.send(
                        content=f"Please select a log channel to use:",
                        view=ChannelSelectView(self.bot, log_context)
                    )
                    return
                except Exception as e:
                    print(f"Failed to select channel: {e}")
                    await interaction.followup.send(f"Could not select the channel: {e}")
                    return

            # Check if the user is booked for the appointment type
            self.svs_cursor.execute("SELECT * FROM appointments WHERE fid=? AND appointment_type=?", (fid, appointment_type))
            booking = self.svs_cursor.fetchone()

            # Fetch nickname for the user
            self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
            user = self.users_cursor.fetchone()
            nickname = user[0] if user else "Unknown"
            
            if not booking:
                await interaction.followup.send(f"{nickname} is not on the minister list for {appointment_type}.")
                return

            # Remove the appointment
            self.svs_cursor.execute("DELETE FROM appointments WHERE fid=? AND appointment_type=?", (fid, appointment_type))
            self.svs_conn.commit()

            # Try to get the avatar image
            try:
                data = await self.fetch_user_data(fid)

                if isinstance(data, int) and data == 429:
                    # Rate limit hit
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"
                elif data and "data" in data and "avatar_image" in data["data"]:
                    avatar_image = data["data"]["avatar_image"]
                else:
                    avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"

            except Exception as e:
                avatar_image = "https://gof-formal-avatar.akamaized.net/avatar-dev/2023/07/17/1001.png"

            # Send embed confirmation to log channel
            embed = discord.Embed(
                title=f"Player removed from {appointment_type}",
                description=f"{nickname} ({fid})",
                color=discord.Color.red()
            )
            embed.set_thumbnail(url=avatar_image)
            embed.set_author(name=f"Removed by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)

            await self.send_embed_to_channel(embed)
            await interaction.followup.send(f"Removed {nickname}")

            # Send the list of times for the selected appointment type
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (appointment_type,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
            time_list = self.generate_available_time_list(booked_times)

            message_content = f"**{appointment_type}** available slots:\n" + "\n".join(time_list)

            # Update existing message or send a new one in the selected channel
            await self.get_or_create_message(context, message_content, channel)

        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send(f"An error occurred while canceling the slot: {e}")

    @discord.app_commands.command(name='minister_clear_all', description='Cancel all appointments for a selected appointment type.')
    @app_commands.autocomplete(appointment_type=appointment_autocomplete)
    async def minister_clear_all(self, interaction: discord.Interaction, appointment_type: str):
        if not await self.is_admin(interaction.user.id):
            await interaction.response.send_message("You do not have permission to use this command.", ephemeral=True)
            return
        await interaction.response.defer()

        try:
            # Send a confirmation prompt
            embed = discord.Embed(
                title=f"⚠️ Confirm clearing {appointment_type} list.",
                description=f"Are you sure you want to remove all minister appointment slots for: {appointment_type}?\n"
                            f"**🚨This action cannot be undone and all names will be removed🚨**.\n"
                            f"You have 10 seconds to reply with 'Yes' to confirm or 'No' to cancel.",
                color=discord.Color.orange()
            )
            confirmation_message = await interaction.followup.send(embed=embed)

            # Wait for user confirmation
            def check(message):
                return message.author == interaction.user and message.channel == interaction.channel

            try:
                response = await self.bot.wait_for('message', check=check, timeout=10.0)

                if response.content.lower() == "yes":
                    # Retrieve booked times before deletion
                    self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (appointment_type,))
                    booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}
                
                    # Generate available times list
                    time_list, _ = self.generate_time_list(booked_times)
                    message_content = f"**Previous {appointment_type} schedule** (before clearing):\n" + "\n".join(time_list)
                    await interaction.followup.send(message_content, ephemeral=True)

                    # Regenerate empty list of available times
                    booked_times = {}
                    time_list = self.generate_available_time_list(booked_times)

                    context = f"{appointment_type}"
                    channel_context = f"{appointment_type} channel"

                    message_content = f"**{appointment_type}** available slots:\n" + "\n".join(time_list)

                    # Get the channel and message to update
                    self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (context,))
                    msg_row = self.svs_cursor.fetchone()

                    self.svs_cursor.execute("SELECT context_id FROM reference WHERE context=?", (channel_context,))
                    channel_row = self.svs_cursor.fetchone()

                    if msg_row and channel_row:
                        message_id = int(msg_row[0])
                        channel_id = int(channel_row[0])
                        log_guild = await self.get_log_guild(interaction.guild)
                        channel = log_guild.get_channel(channel_id) or await self.bot.fetch_channel(channel_id)
                        message = await channel.fetch_message(message_id)
                        await message.edit(content=message_content)

                    else:
                        await confirmation_message.reply(f"[Warning] Could not find message or channel for {appointment_type}, skipping message update.\n\nNext time you run the `/minister_add` command that channel will be used")

                    self.svs_cursor.execute("DELETE FROM appointments WHERE appointment_type=?", (appointment_type,))
                    self.svs_conn.commit()

                    embed = discord.Embed(
                        title=f"Cleared {appointment_type} list",
                        description=f"All appointments for {appointment_type} have been successfully removed.",
                        color=discord.Color.red()
                    )
                    embed.set_author(name=f"Cleared by {interaction.user.display_name}", icon_url=interaction.user.avatar.url)

                    await self.send_embed_to_channel(embed)
                    await confirmation_message.reply(f"✅ Deleted all {appointment_type} appointments.")
                else:
                    await confirmation_message.reply(f"Cancelled the action. Nothing was removed from {appointment_type}.")

            except asyncio.TimeoutError:
                await confirmation_message.reply(f"<@{interaction.user.id}> did not respond in time. The action has been cancelled.")

        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send(f"An error occurred while clearing the appointments: {e}", ephemeral=True)
        
    @discord.app_commands.command(name='minister_list', description='View the schedule for an appointment type.')
    @app_commands.autocomplete(appointment_type=appointment_autocomplete, all_or_available=choice_autocomplete)
    @app_commands.describe(
        appointment_type="The type of minister appointment to view.",
        all_or_available="Show full schedule or only available slots.", 
        update="Default: False. Whether to update names via API or not. Will take some time if enabled."
    )
    async def minister_list(self, interaction: discord.Interaction, appointment_type: str, all_or_available: str, update: bool = False):
        try:
            await interaction.response.defer()

            # Fetch the booked times for the specific appointment type
            self.svs_cursor.execute("SELECT time, fid, alliance FROM appointments WHERE appointment_type=?", (appointment_type,))
            booked_times = {row[0]: (row[1], row[2]) for row in self.svs_cursor.fetchall()}

            if all_or_available == "all":
                if update:
                    async def update_progress(checked, total, waiting):
                        if checked % 1 == 0:
                            color = discord.Color.orange() if waiting else discord.Color.green()
                            title = "waiting 60 seconds before continuing" if waiting else "Updating names"
                            embed = discord.Embed(
                                title=title,
                                description=f"Checked {checked}/{total} minister appointees",
                                color=color
                            )
                            try:
                                await interaction.edit_original_response(embed=embed)
                            except discord.NotFound:
                                print("Interaction expired before progress update.")

                    # Fetch updated data via API
                    time_list, _ = await self.update_time_list(booked_times, update_progress)
                else:
                    # Use database method
                    time_list, _ = self.generate_time_list(booked_times)

                # Format the time list for the embed
                time_list = "\n".join(time_list)

                if time_list:
                    embed = discord.Embed(
                        title=f"Schedule for {appointment_type}",
                        description=time_list,
                        color=discord.Color.blue()
                    )
                    try:
                        await interaction.edit_original_response(embed=embed)
                    except discord.NotFound:
                        print("Interaction expired before final update.")

            elif all_or_available == "available only":
                available_slots = self.generate_available_time_list(booked_times)
                if available_slots:
                    time_list = "\n".join(available_slots)
                    await interaction.followup.send(f"{appointment_type} available slots:\n{time_list}")
                else:
                    await interaction.followup.send(f"All appointment slots are filled for {appointment_type}")

        except Exception as e:
            print(f"An error occurred: {e}")
            await interaction.followup.send(f"An error occurred while fetching the schedule: {e}")

async def setup(bot):
    await bot.add_cog(MinisterSchedule(bot))