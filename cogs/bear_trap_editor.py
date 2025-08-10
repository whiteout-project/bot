from logging import exception
import discord
from discord.ext import commands
import sqlite3
from datetime import datetime, timedelta
import re
import traceback

def format_repeat_interval(repeat_minutes, notification_id=None) -> str:
    if repeat_minutes == 0:
        return "❌ No repeat"

    if repeat_minutes == "fixed":
        if notification_id is None:
            return "Custom Days"

        conn = sqlite3.connect("db/beartime.sqlite")
        cursor = conn.cursor()
        cursor.execute("""
            SELECT weekday FROM notification_days
            WHERE notification_id = ?
        """, (notification_id,))
        rows = cursor.fetchall()
        conn.close()

        weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_set = set()

        for row in rows:
            for part in row[0].split('|'):
                if part.strip().isdigit():
                    day_set.add(int(part))

        if not day_set:
            return "❌ No days selected"

        sorted_days = sorted(day_set)
        day_list = [weekday_names[day] for day in sorted_days]

        if len(day_list) == 1:
            return f"Every {day_list[0]}"
        else:
            return "Every " + ", ".join(day_list[:-1]) + " and " + day_list[-1]

    try:
        minutes = int(repeat_minutes)
    except ValueError:
        return "Invalid repeat interval"

    time_units = [
        ("month", 43200),
        ("week", 10080),
        ("day", 1440),
        ("hour", 60),
        ("minute", 1),
    ]

    result = []
    for name, unit in time_units:
        value = minutes // unit
        if value > 0:
            result.append(f"{value} {name}{'s' if value > 1 else ''}")
            minutes %= unit

    return " and ".join(result)

def format_mention(mention: str) -> str:
    """Formats mention strings into Discord mention syntax."""
    if mention.startswith("role_"):
        role_id = mention.split("_")[1]
        return f"<@&{role_id}>"
    elif mention.startswith("member_"):
        user_id = mention.split("_")[1]
        return f"<@{user_id}>"
    elif mention == "everyone":
        return "@everyone"
    else:
        return "No Mention"

def format_notification_type(notification_type: int) -> str:
    """Returns a formatted string for the given notification type."""
    notification_types = {
        1: "Sends notifications at 30 minutes, 10 minutes, 5 minutes before and when time's up",
        2: "Sends notifications at 10 minutes, 5 minutes before and when time's up",
        3: "Sends notifications at 5 minutes before and when time's up",
        4: "Sends notification only 5 minutes before",
        5: "Sends notification only when time's up",
        6: "Sends notifications at custom times"
    }
    return notification_types.get(notification_type, "Unknown notification type")

class EmbedFieldModal(discord.ui.Modal):
    def __init__(self, parent_view, field_name, label, placeholder, default="", style=discord.TextStyle.short,
                 max_length=1024, required=False):
        super().__init__(title=f"Edit {field_name}")

        self.parent_view = parent_view
        self.field_name = field_name

        self.input_field = discord.ui.TextInput(
            label=label,
            placeholder=placeholder,
            default=default,
            style=style,
            max_length=max_length,
            required=required,
        )
        self.add_item(self.input_field)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()
        try:
            value = self.input_field.value
            if self.field_name == "color":
                if value.startswith("#"):
                    value = value[1:]
                try:
                    int_color = int(value, 16)
                except ValueError:
                    await interaction.response.send_message("Invalid hex color code!", ephemeral=True)
                    return

                self.parent_view.color = int_color
            else:
                setattr(self.parent_view, self.field_name, value)

            await self.parent_view.cog.update_embed_notification(self.parent_view)
            await self.parent_view.update_embed_view(interaction)
        except Exception as e:
            print(f"Error in modal for {self.field_name}: {e}")
            await interaction.followup.send(f"An error occurred! {e}", ephemeral=True)

class EmbedDataView(discord.ui.View):
    def __init__(self, cog, notification_id, title, description, color, image_url, thumbnail_url, footer, author,
                 mention_message):
        super().__init__(timeout=None)
        self.cog = cog
        self.notification_id = notification_id
        self.title = title
        self.embed_description = description
        self.color = color
        self.image_url = image_url
        self.thumbnail_url = thumbnail_url
        self.footer = footer
        self.author = author
        self.mention_message = mention_message
        self.message = None

    async def update_embed_view(self, interaction: discord.Interaction):
        """Update the embed message when changes are made."""
        embed = discord.Embed(
            title=self.title,
            description=self.embed_description,
            color=self.color,
        )
        if self.footer:
            embed.set_footer(text=self.footer)
        if self.author:
            embed.set_author(name=self.author)
        if self.image_url:
            embed.set_image(url=self.image_url)
        if self.thumbnail_url:
            embed.set_thumbnail(url=self.thumbnail_url)

        await self.message.edit(content=self.mention_message, embed=embed, view=self)

    @discord.ui.button(label="Title", style=discord.ButtonStyle.primary)
    async def edit_title(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="title",
                label="Embed Title",
                placeholder="Enter notification title",
                default=self.title or "",
                max_length=256,
                required=True
            )
        )

    @discord.ui.button(label="Description", style=discord.ButtonStyle.primary)
    async def edit_description(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="embed_description",
                label="Embed Description",
                placeholder="Enter notification description",
                default=self.embed_description or "",
                max_length=4000,
                style=discord.TextStyle.paragraph,
                required=True
            )
        )

    @discord.ui.button(label="Color", style=discord.ButtonStyle.success)
    async def edit_color(self, interaction: discord.Interaction, button: discord.ui.Button):
        default_hex = ""
        if self.color:
            default_hex = f"#{hex(self.color)[2:].zfill(6)}"

        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="color",
                label="Embed hex code",
                placeholder="Enter hex code",
                default=default_hex,
                max_length=7,
                required=True
            )
        )

    @discord.ui.button(label="Mention message", style=discord.ButtonStyle.secondary)
    async def edit_mention_message(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="mention_message",
                label="mention message",
                placeholder="Enter notification message... You can use @tag for mentions and %t or {time} for time",
                default=self.mention_message or "",
                required=False
            )
        )

    @discord.ui.button(label="Footer", style=discord.ButtonStyle.secondary)
    async def edit_footer(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="footer",
                label="Embed Footer",
                placeholder="Enter Footer",
                default=self.footer or "",
                max_length=2048,
                required=False
            )
        )

    @discord.ui.button(label="Author", style=discord.ButtonStyle.secondary)
    async def edit_author(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="author",
                label="Embed Author",
                placeholder="Enter Author message",
                default=self.author or "",
                max_length=256,
                required=False
            )
        )

    @discord.ui.button(label="Add Image", style=discord.ButtonStyle.secondary)
    async def edit_image_url(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="image_url",
                label="Embed Image",
                placeholder="Enter image url",
                default=self.image_url or "",
                required=False
            )
        )

    @discord.ui.button(label="Add Thumbnail", style=discord.ButtonStyle.secondary)
    async def edit_thumbnail_url(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EmbedFieldModal(
                parent_view=self,
                field_name="thumbnail_url",
                label="Embed Thumbnail URL",
                placeholder="Enter Thumbnail URL",
                default=self.thumbnail_url or "",
                required=False
            )
        )

    @discord.ui.button(label="Edit Notification settings", style=discord.ButtonStyle.primary, emoji="⚙️")
    async def notification_setting(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        conn = sqlite3.connect("db/beartime.sqlite")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT channel_id, hour, minute, description, mention_type, repeat_minutes, next_notification, timezone, notification_type FROM bear_notifications WHERE id = ?",
            (self.notification_id,))
        result = cursor.fetchone()
        conn.close()

        if not result:
            await interaction.followup.send("❌ Notification not found in database.", ephemeral=True)
            return

        channel_id, hours, minutes, description, mention, repeat, next_notification, timezone, notification_type = result

        new_view = PlainEditorView(
            cog=self.cog,
            notification_id=self.notification_id,
            channel_id=channel_id,
            hours=hours,
            minutes=minutes,
            description=description,
            mention=mention,
            repeat=repeat,
            next_notification=next_notification,
            timezone=timezone,
            notification_type=notification_type
        )
        new_view.message = self.message

        next_notification_date = datetime.fromisoformat(next_notification).strftime("%d/%m/%Y")
        formatted_repeat = format_repeat_interval(repeat, self.notification_id)
        formatted_mention = format_mention(mention)
        formatted_type = format_notification_type(notification_type)

        embed = discord.Embed(
            title="Editing Notification",
            description=(
                f"**📅 Next Notification date:** {next_notification_date}\n"
                f"**⏰ Time:** {hours:02d}:{minutes:02d} ({timezone})\n"
                f"**📢 Channel:** <#{channel_id}>\n"
                f"**📝 Description:** {description}\n\n"
                f"**⚙️ Notification Type**\n{formatted_type}\n\n"
                f"**👥 Mention:** {formatted_mention}\n"
                f"**🔄 Repeat:** {formatted_repeat}\n"
            ),
            color=discord.Color.blue(),
        )

        await self.message.edit(content=None, embed=embed, view=new_view)

class PlainEditorView(discord.ui.View):
    def __init__(self, cog, notification_id, channel_id, hours, minutes, description, mention, repeat,
                 next_notification, timezone, notification_type):
        super().__init__(timeout=None)
        self.cog = cog
        self.notification_id = notification_id
        self.channel_id = channel_id
        self.hours = hours
        self.minutes = minutes
        self.description = description
        self.mention = mention
        self.repeat = repeat
        self.next_notification = next_notification
        self.timezone = timezone
        self.notification_type = notification_type
        self.message = None

        if self.repeat == "fixed":
            try:
                conn = sqlite3.connect("db/beartime.sqlite")
                cursor = conn.cursor()
                cursor.execute("SELECT weekday FROM notification_days WHERE notification_id = ?", (self.notification_id,))
                weekday_value = cursor.fetchone()
                if weekday_value:
                    self.weekdays = row[0]
                conn.close()
            except Exception as e:
                print(f"Failed to load weekdays: {e}")

        for child in self.children:
            if isinstance(child, discord.ui.Button) and child.custom_id == "description_button":
                if "EMBED_MESSAGE" in self.description:
                    child.label = "Edit Embed"
                    child.emoji = "📝"
                elif "PLAIN_MESSAGE" in self.description:
                    child.label = "Description"
                else:
                    child.label = "Description"

    async def update_embed(self, interaction: discord.Interaction):
        """Update the embed message when changes are made."""
        next_notification_date = datetime.fromisoformat(self.next_notification).strftime("%d/%m/%Y")
        formatted_repeat = format_repeat_interval(self.repeat, self.notification_id)
        formatted_mention = format_mention(self.mention)
        formatted_type = format_notification_type(self.notification_type)
        embed = discord.Embed(
            title="Editing Notification",
            description=(
                f"**📅 Next Notification date:** {next_notification_date}\n"
                f"**⏰ Time:** {self.hours:02d}:{self.minutes:02d} ({self.timezone})\n"
                f"**📢 Channel:** <#{self.channel_id}>\n"
                f"**📝 Description:** {self.description}\n\n"
                f"**⚙️ Notification Type**\n{formatted_type}\n\n"
                f"**👥 Mention:** {formatted_mention}\n"
                f"**🔄 Repeat:** {formatted_repeat}\n"
            ),
            color=discord.Color.blue(),
        )
        await self.message.edit(embed=embed, view=self)

    @discord.ui.button(label="Description", style=discord.ButtonStyle.primary, custom_id="description_button")
    async def description_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if "EMBED_MESSAGE" in self.description:
            button.label = "Edit Embed"
            button.emoji = "📝"
            # await interaction.response.defer()
            try:
                await self.cog.start_edit_process(interaction, self.notification_id, original_message=self.message)
            except Exception as e:
                print(f"error: {e}")
        elif "PLAIN_MESSAGE" in self.description:
            button.label = "Description"

            class DescriptionModal(discord.ui.Modal, title="Edit Description"):
                def __init__(self, parent_view):
                    super().__init__()
                    self.parent_view = parent_view

                    # Extract the existing PLAIN_MESSAGE part if it exists
                    parts = parent_view.description.split("|")
                    plain_message_part = next((p for p in parts if p.startswith("PLAIN_MESSAGE:")), "PLAIN_MESSAGE:")
                    saved_description = plain_message_part.replace("PLAIN_MESSAGE:", "")

                    self.description = discord.ui.TextInput(label="Message",
                                                            placeholder="Enter notification message... You can use @tag for mentions and %t or {time} for time",
                                                            style=discord.TextStyle.paragraph, required=True,
                                                            default=saved_description, max_length=2000)
                    self.add_item(self.description)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    await modal_interaction.response.defer()
                    try:
                        # Preserve CUSTOM_TIMES if it exists
                        parts = self.parent_view.description.split("|")
                        updated_parts = [p for p in parts if not p.startswith("PLAIN_MESSAGE:")]
                        updated_parts.append(
                            f"PLAIN_MESSAGE:{self.description.value}")  # Update only the PLAIN_MESSAGE part

                        self.parent_view.description = "|".join(updated_parts)  # Reassemble

                        await self.parent_view.cog.update_notification(self.parent_view)
                        await self.parent_view.update_embed(modal_interaction)
                    except Exception as e:
                        print(f"Error in DescriptionModal: {e}")
                        await modal_interaction.followup.send("❌ An error occurred!", ephemeral=True)

            await interaction.response.send_modal(DescriptionModal(self))

    @discord.ui.button(label="Channel", style=discord.ButtonStyle.primary)
    async def edit_channel(self, interaction: discord.Interaction, button: discord.ui.Button):
        channel_select = discord.ui.ChannelSelect(
            placeholder="Select a channel for notifications",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1
        )

        async def channel_select_callback(select_interaction: discord.Interaction):
            await select_interaction.response.defer()
            selected_channel_id = select_interaction.data["values"][0]
            self.channel_id = int(selected_channel_id)

            await self.cog.update_notification(self)
            await self.update_embed(select_interaction)

        channel_select.callback = channel_select_callback
        view = discord.ui.View()
        view.add_item(channel_select)

        await interaction.response.send_message("Select a new channel:", view=view, ephemeral=True)

    @discord.ui.button(label="Time", style=discord.ButtonStyle.primary)
    async def edit_time(self, interaction: discord.Interaction, button: discord.ui.Button):

        class TimeModal(discord.ui.Modal, title="Edit Notification Time"):
            def __init__(self, parent_view):
                super().__init__()
                self.parent_view = parent_view
                next_notification_str = parent_view.next_notification.replace("+00:00", "")
                current_dt = datetime.strptime(next_notification_str, "%Y-%m-%dT%H:%M:%S")
                saved_date = current_dt.strftime("%d/%m/%Y")
                saved_hour = str(current_dt.hour)
                saved_minute = str(current_dt.minute)

                self.date = discord.ui.TextInput(label="Date (DD/MM/YYYY)", required=True, default=saved_date)
                self.hour = discord.ui.TextInput(label="Hour (0-23)", required=True, default=saved_hour)
                self.minute = discord.ui.TextInput(label="Minute (0-59)", required=True, default=saved_minute)

                self.add_item(self.date)
                self.add_item(self.hour)
                self.add_item(self.minute)

            async def on_submit(self, modal_interaction: discord.Interaction):
                await modal_interaction.response.defer()
                try:
                    new_hours = int(self.hour.value.strip())
                    new_minutes = int(self.minute.value.strip())
                    new_date = self.date.value.strip() if self.date.value else None

                    if not hasattr(self.parent_view, "next_notification"):
                        await modal_interaction.followup.send("❌ Error: `next_notification` is missing!",
                                                              ephemeral=True)
                        return

                    current_dt = datetime.strptime(self.parent_view.next_notification, "%Y-%m-%dT%H:%M:%S+00:00")
                    new_dt = current_dt.replace(hour=new_hours, minute=new_minutes)

                    if new_date:
                        try:
                            day, month, year = map(int, new_date.split("/"))
                            new_dt = new_dt.replace(day=day, month=month, year=year)
                        except ValueError:
                            await modal_interaction.followup.send("❌ Invalid date format! Use DD/MM/YYYY.",
                                                                  ephemeral=True)
                            return

                    self.parent_view.hours = new_hours
                    self.parent_view.minutes = new_minutes
                    self.parent_view.next_notification = new_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")

                    await self.parent_view.cog.update_notification(self.parent_view)
                    await self.parent_view.update_embed(modal_interaction)

                except ValueError:
                    await modal_interaction.followup.send("❌ Invalid input! Please enter numbers only.", ephemeral=True)
                except Exception as e:
                    print(f"Error in TimeModal: {e}")
                    await modal_interaction.followup.send("❌ An error occurred!", ephemeral=True)

        try:
            await interaction.response.send_modal(TimeModal(self))
        except Exception as e:
            print(f"Error sending modal: {e}")

    @discord.ui.button(label="Repeat", style=discord.ButtonStyle.primary)
    async def edit_repeat(self, interaction: discord.Interaction, button: discord.ui.Button):
        class RepeatOptionsView(discord.ui.View):
            def __init__(self, parent_view):
                super().__init__(timeout=None)
                self.parent_view = parent_view

                custom_button = discord.ui.Button(label="Custom Intervals", style=discord.ButtonStyle.secondary)
                custom_button.callback = lambda i: send_custom_modal(i, self.parent_view)
                self.add_item(custom_button)

                specific_button = discord.ui.Button(label="Specific Days", style=discord.ButtonStyle.secondary)
                specific_button.callback = lambda i: send_day_selector(i, self.parent_view)
                self.add_item(specific_button)

        async def send_day_selector(interaction: discord.Interaction, parent_view):
            try:
                await interaction.response.defer()

                class DaysView(discord.ui.View):
                    def __init__(self):
                        super().__init__(timeout=None)
                        self.parent_view = parent_view
                        self.selected_days = []

                        self.select = discord.ui.Select(
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
                            ]
                        )
                        self.select.callback = self.on_select
                        self.add_item(self.select)

                        confirm_button = discord.ui.Button(label="Confirm", style=discord.ButtonStyle.success)
                        confirm_button.callback = self.confirm_days
                        self.add_item(confirm_button)

                    async def on_select(self, interaction: discord.Interaction):
                        self.selected_days = self.select.values
                        await interaction.response.defer()

                    async def confirm_days(self, interaction: discord.Interaction):
                        await interaction.response.defer()

                        if not self.selected_days:
                            await interaction.followup.send("Please select at least one day.", ephemeral=True)
                            return

                        weekdays_index = {
                            "Monday": 0, "Tuesday": 1, "Wednesday": 2,
                            "Thursday": 3, "Friday": 4, "Saturday": 5, "Sunday": 6
                        }
                        selected_weekdays = [weekdays_index[d] for d in self.selected_days]
                        sorted_days = sorted(selected_weekdays)

                        self.parent_view.repeat = "fixed"
                        self.parent_view.weekdays = "|".join(str(d) for d in sorted_days)

                        await self.parent_view.cog.update_notification(self.parent_view)
                        await self.parent_view.update_embed(interaction)

                await interaction.edit_original_response(
                    content="Select specific days for the notification:",
                    view=DaysView()
                )

            except Exception as e:
                print(f"Error in send_day_selector: {e}")

        async def send_custom_modal(interaction: discord.Interaction, parent_view):
            class CustomRepeatModal(discord.ui.Modal, title="Edit Repeat Interval"):
                def __init__(self):
                    super().__init__()
                    self.parent_view = parent_view
                    self.month = discord.ui.TextInput(label="Months", required=False, default="0")
                    self.week = discord.ui.TextInput(label="Weeks", required=False, default="0")
                    self.day = discord.ui.TextInput(label="Days", required=False, default="0")
                    self.hour = discord.ui.TextInput(label="Hours", required=False, default="0")
                    self.minute = discord.ui.TextInput(label="Minutes", required=False, default="0")
                    self.add_item(self.month)
                    self.add_item(self.week)
                    self.add_item(self.day)
                    self.add_item(self.hour)
                    self.add_item(self.minute)

                async def on_submit(self, modal_interaction: discord.Interaction):
                    await modal_interaction.response.defer()
                    try:
                        repeat_minutes = (
                                int(self.month.value) * 43200 +
                                int(self.week.value) * 10080 +
                                int(self.day.value) * 1440 +
                                int(self.hour.value) * 60 +
                                int(self.minute.value)
                        )

                        self.parent_view.repeat = repeat_minutes

                        await self.parent_view.cog.update_notification(self.parent_view)
                        await self.parent_view.update_embed(modal_interaction)

                    except Exception as e:
                        print(f"Error in CustomRepeatModal: {e}")
                        await modal_interaction.followup.send("❌ An error occurred!", ephemeral=True)

            await interaction.response.send_modal(CustomRepeatModal())

        view = RepeatOptionsView(self)

        await interaction.response.send_message(
            content="Choose how you want to repeat the notification:\n"
                    "*  Custom intervals --> Every 2 days, 1 week, 1 month, etc\n"
                    "*  Specific days --> Every Sunday, Sunday and Tuesday, etc",
            view=view,
            ephemeral=True,
        )

    @discord.ui.button(label="Mention", style=discord.ButtonStyle.primary)
    async def edit_mention(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = discord.ui.View()

        async def mention_callback(mention_interaction: discord.Interaction, mention_type: str):
            await mention_interaction.response.defer()

            if mention_type == "everyone":
                self.mention = "everyone"
            elif mention_type == "none":
                self.mention = "none"
            elif mention_type == "role":
                role_select = discord.ui.RoleSelect(
                    placeholder="🔍 Search and select who to mention...",
                    min_values=1,
                    max_values=1
                )

                async def role_select_callback(select_interaction: discord.Interaction):
                    await select_interaction.response.defer()
                    selected_role_id = select_interaction.data["values"][0]
                    self.mention = f"role_{selected_role_id}"
                    await self.cog.update_notification(self)
                    await self.update_embed(select_interaction)

                role_select.callback = role_select_callback
                role_view = discord.ui.View()
                role_view.add_item(role_select)

                await mention_interaction.followup.send("Select a role:", view=role_view, ephemeral=True)
                return

            elif mention_type == "member":
                user_select = discord.ui.UserSelect(
                    placeholder="🔍 Search and select who to mention...",
                    min_values=1,
                    max_values=1
                )

                async def user_select_callback(select_interaction: discord.Interaction):
                    await select_interaction.response.defer()
                    selected_user_id = select_interaction.data["values"][0]
                    self.mention = f"member_{selected_user_id}"
                    await self.cog.update_notification(self)
                    await self.update_embed(select_interaction)

                user_select.callback = user_select_callback
                user_view = discord.ui.View()
                user_view.add_item(user_select)

                await mention_interaction.followup.send("Select a user:", view=user_view, ephemeral=True)
                return

            # Update the mention type and refresh the embed
            await self.cog.update_notification(self)
            await self.update_embed(mention_interaction)

        # Create buttons for mention types
        for label, mention_type in [("📢 everyone", "everyone"), ("👥 Select Role", "role"),
                                    ("👤 Select Member", "member"), ("🔕 No Mention", "none")]:
            btn = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary)

            async def button_callback(inter: discord.Interaction, t=mention_type):
                await mention_callback(inter, t)

            btn.callback = button_callback
            view.add_item(btn)

        await interaction.response.send_message("Choose mention type:", view=view, ephemeral=True)

    @discord.ui.button(label="Notification Ping", style=discord.ButtonStyle.primary)
    async def edit_notification_ping(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show options for setting when the ping is sent."""
        view = discord.ui.View()

        options = [
            ("30m, 10m, 5m & Time", 1),
            ("10m, 5m & Time", 2),
            ("5m & Time", 3),
            ("Only 5m", 4),
            ("Only Time", 5),
            ("Custom Times", 6),
        ]

        for label, value in options:
            async def callback(interaction: discord.Interaction, value=value):
                self.notification_type = value
                if value == 6:  # Custom Times button pressed
                    class CustomTimeModal(discord.ui.Modal, title="Enter Custom Notification Times"):
                        def __init__(self, parent_view):
                            super().__init__()
                            self.parent_view = parent_view
                            self.times_input = discord.ui.TextInput(
                                label="Enter times (e.g., 20-10-3-2-1-0)",
                                placeholder="Separate times with '-'",
                                required=True
                            )
                            self.add_item(self.times_input)

                        async def on_submit(self, modal_interaction: discord.Interaction):
                            new_times = self.times_input.value.strip()

                            # ✅ Validate format (only numbers and dashes allowed)
                            if not all(c.isdigit() or c == '-' for c in new_times):
                                await modal_interaction.response.send_message(
                                    "❌ Invalid format! Use numbers separated by '-'.", ephemeral=True)
                                return

                            # ✅ Check if description contains "CUSTOM_TIMES:"
                            if "CUSTOM_TIMES:" in self.parent_view.description:
                                # Replace existing CUSTOM_TIMES section
                                self.parent_view.description = re.sub(
                                    r"CUSTOM_TIMES:[^\|]+\|", f"CUSTOM_TIMES:{new_times}|", self.parent_view.description
                                )
                            else:
                                # Add CUSTOM_TIMES at the beginning
                                self.parent_view.description = f"CUSTOM_TIMES:{new_times}|{self.parent_view.description}"

                            # ✅ Update notification and embed
                            await self.parent_view.cog.update_notification(self.parent_view)
                            await self.parent_view.update_embed(modal_interaction)
                            await modal_interaction.response.defer()

                    return await interaction.response.send_modal(CustomTimeModal(self))

                await interaction.response.defer()

                # ✅ Remove "CUSTOM_TIMES" if any other option is chosen
                if "CUSTOM_TIMES:" in self.description:
                    self.description = self.description.split("|", 1)[-1]

                await self.cog.update_notification(self)
                await self.update_embed(interaction)

            button = discord.ui.Button(label=label, style=discord.ButtonStyle.secondary)
            button.callback = callback
            view.add_item(button)

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
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class NotificationEditor(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def start_edit_process(self, interaction: discord.Interaction, notification_id: int,
                                 original_message: discord.Message = None):
        conn = sqlite3.connect("db/beartime.sqlite")
        cursor = conn.cursor()
        cursor.execute(
            "SELECT channel_id, hour, minute, description, mention_type, repeat_minutes, next_notification, timezone, notification_type FROM bear_notifications WHERE id = ?",
            (notification_id,))
        result = cursor.fetchone()

        if not result:
            await interaction.response.send_message("❌ Notification ID not found.", ephemeral=True)
            return

        channel_id, hours, minutes, description, mention, repeat, next_notification, timezone, notification_type = result
        if "EMBED_MESSAGE" in description:
            cursor.execute(
                "SELECT title, description, color, image_url, thumbnail_url, footer, author, mention_message FROM bear_notification_embeds WHERE notification_id = ?",
                (notification_id,))
            embed_results = cursor.fetchone()
            title, embed_description, color, image_url, thumbnail_url, footer, author, mention_message = embed_results

            view = EmbedDataView(self, notification_id, title, embed_description, color, image_url, thumbnail_url,
                                 footer, author, mention_message)

            embed = discord.Embed(
                title=title,
                description=embed_description,
                color=color,
            )
            if footer:
                embed.set_footer(text=footer)
            if author:
                embed.set_author(name=author)
            if image_url:
                embed.set_image(url=image_url)
            if thumbnail_url:
                embed.set_thumbnail(url=thumbnail_url)

            await interaction.response.defer()
            if original_message:
                await original_message.edit(content=mention_message, embed=embed, view=view)
                message = original_message
            else:
                message = await interaction.followup.send(content=mention_message, embed=embed, view=view,
                                                          ephemeral=True)

        elif "PLAIN_MESSAGE" in description:
            try:
                view = PlainEditorView(self, notification_id, channel_id, hours, minutes, description, mention, repeat,
                                       next_notification, timezone, notification_type)

                next_notification_date = datetime.fromisoformat(next_notification).strftime("%d/%m/%Y")
                formatted_repeat = format_repeat_interval(repeat, notification_id)
                formatted_mention = format_mention(mention)
                formatted_type = format_notification_type(notification_type)
                embed = discord.Embed(
                    title="Editing Notification",
                    description=(
                        f"**📅 Next Notification date:** {next_notification_date}\n"
                        f"**⏰ Time:** {hours:02d}:{minutes:02d} ({timezone})\n"
                        f"**📢 Channel:** <#{channel_id}>\n"
                        f"**📝 Description:** {description}\n\n"
                        f"**⚙️ Notification Type**\n{formatted_type}\n\n"
                        f"**👥 Mention:** {formatted_mention}\n"
                        f"**🔄 Repeat:** {formatted_repeat}\n"
                    ),
                    color=discord.Color.blue(),
                )
                await interaction.response.defer()
                message = await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            except Exception as e:
                print(f"[ERROR] During PLAIN_MESSAGE handling: {e}")
                await interaction.followup.send(f"An error occurred in PLAIN_MESSAGE section. {e}", ephemeral=True)
                return
        else:
            print(f"No known format matched, description is {description}")

        view.message = message

    async def update_notification(self, view):
        conn = sqlite3.connect("db/beartime.sqlite")
        cursor = conn.cursor()

        if view.repeat == "fixed":
            cursor.execute("DELETE FROM notification_days WHERE notification_id = ?", (view.notification_id,))

            weekday = getattr(view, "weekdays", "")
            cursor.execute("INSERT INTO notification_days (notification_id, weekday) VALUES (?, ?)",(view.notification_id, weekday))
        else:
            cursor.execute("DELETE FROM notification_days WHERE notification_id = ?", (view.notification_id,))

        cursor.execute(
            "UPDATE bear_notifications SET channel_id = ?, hour = ?, minute = ?, description = ?, mention_type = ?, repeat_minutes = ?, next_notification = ?, notification_type = ? WHERE id = ?",
            (view.channel_id, view.hours, view.minutes, view.description, view.mention, view.repeat,
             view.next_notification, view.notification_type, view.notification_id)
        )
        conn.commit()
        conn.close()

    async def update_embed_notification(self, view):
        conn = sqlite3.connect("db/beartime.sqlite")
        cursor = conn.cursor()

        cursor.execute(
            "UPDATE bear_notification_embeds SET title = ?, description = ?, color = ?, image_url = ?, thumbnail_url = ?, footer = ?, author = ?, mention_message = ? WHERE notification_id = ?",
            (view.title, view.embed_description, view.color, view.image_url, view.thumbnail_url, view.footer,
             view.author, view.mention_message, view.notification_id)
        )
        conn.commit()
        conn.close()

async def setup(bot):
    await bot.add_cog(NotificationEditor(bot))
