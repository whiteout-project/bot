import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
import json

class ArchiveDetailsView(discord.ui.View):
    def __init__(self, bot, cog, archive_id, archive_info, type_counts):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archive_id = archive_id
        self.archive_info = archive_info
        self.type_counts = type_counts  # List of (appointment_type, count) tuples

    @discord.ui.button(label="View Construction", style=discord.ButtonStyle.primary, emoji="🔨", row=0)
    async def view_construction(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_appointments(interaction, self.archive_id, "Construction Day")

    @discord.ui.button(label="View Research", style=discord.ButtonStyle.primary, emoji="🔬", row=0)
    async def view_research(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_appointments(interaction, self.archive_id, "Research Day")

    @discord.ui.button(label="View Training", style=discord.ButtonStyle.primary, emoji="⚔️", row=0)
    async def view_training(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_appointments(interaction, self.archive_id, "Troops Training Day")

    @discord.ui.button(label="View Change History", style=discord.ButtonStyle.secondary, emoji="📜", row=1)
    async def view_history(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_change_history(interaction, self.archive_id)

    @discord.ui.button(label="Delete Archive", style=discord.ButtonStyle.danger, emoji="🗑️", row=1)
    async def delete_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_delete_archive_confirmation(interaction, self.archive_id, self.archive_info)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️", row=2)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_list(interaction)

class ArchiveAppointmentsView(discord.ui.View):
    def __init__(self, bot, cog, archive_id, appointment_type, appointments, page=0):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archive_id = archive_id
        self.appointment_type = appointment_type
        self.appointments = appointments  # List of (time, fid, nickname, alliance) tuples sorted by time
        self.page = page
        self.max_page = (len(appointments) - 1) // 25 if appointments else 0

        self.update_components()

    def update_components(self):
        self.clear_items()

        # Pagination buttons
        if self.max_page > 0:
            prev_button = discord.ui.Button(
                label="◀️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page == 0,
                row=0
            )
            prev_button.callback = self.prev_page_callback
            self.add_item(prev_button)

            next_button = discord.ui.Button(
                label="▶️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page >= self.max_page,
                row=0
            )
            next_button.callback = self.next_page_callback
            self.add_item(next_button)

        # Post to Channel button
        post_button = discord.ui.Button(
            label="Post to Channel",
            style=discord.ButtonStyle.success,
            emoji="📤",
            row=1 if self.max_page > 0 else 0
        )
        post_button.callback = self.post_to_channel_callback
        self.add_item(post_button)

        # Back button
        back_button = discord.ui.Button(
            label="Back",
            style=discord.ButtonStyle.primary,
            emoji="⬅️",
            row=1 if self.max_page > 0 else 0
        )
        back_button.callback = self.back_button_callback
        self.add_item(back_button)

    async def prev_page_callback(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self.update_components()
        await self.cog.update_appointments_embed(interaction, self)

    async def next_page_callback(self, interaction: discord.Interaction):
        self.page = min(self.max_page, self.page + 1)
        self.update_components()
        await self.cog.update_appointments_embed(interaction, self)

    async def post_to_channel_callback(self, interaction: discord.Interaction):
        await self.cog.show_channel_selector_for_post(interaction, self.archive_id, self.appointment_type, self.appointments)

    async def back_button_callback(self, interaction: discord.Interaction):
        await self.cog.show_archive_details(interaction, self.archive_id)

class ArchiveListView(discord.ui.View):
    def __init__(self, bot, cog, archives, page=0):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archives = archives
        self.page = page
        self.max_page = (len(archives) - 1) // 10 if archives else 0

        self.update_components()

    def update_components(self):
        self.clear_items()

        # Calculate page boundaries
        start_idx = self.page * 10
        end_idx = min(start_idx + 10, len(self.archives))
        page_archives = self.archives[start_idx:end_idx]

        # Add archive selection dropdown
        if page_archives:
            options = []
            for archive_id, name, created_at, created_by_name, count in page_archives:
                created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M")
                label = f"{name} ({created_date})"[:100]
                description = f"By {created_by_name} - {count} appointments"[:100]
                options.append(discord.SelectOption(
                    label=label,
                    value=str(archive_id),
                    description=description
                ))

            select = discord.ui.Select(
                placeholder=f"Select an archive... (Page {self.page + 1}/{self.max_page + 1})",
                options=options
            )
            select.callback = self.archive_select_callback
            self.add_item(select)

        # Pagination buttons
        if self.max_page > 0:
            prev_button = discord.ui.Button(
                label="◀️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page == 0,
                row=1
            )
            prev_button.callback = self.prev_page_callback
            self.add_item(prev_button)

            next_button = discord.ui.Button(
                label="▶️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page >= self.max_page,
                row=1
            )
            next_button.callback = self.next_page_callback
            self.add_item(next_button)

        # Back button
        back_button = discord.ui.Button(
            label="Back",
            style=discord.ButtonStyle.primary,
            emoji="⬅️",
            row=2 if self.max_page > 0 else 1
        )
        back_button.callback = self.back_button_callback
        self.add_item(back_button)

    async def archive_select_callback(self, interaction: discord.Interaction):
        archive_id = int(interaction.data['values'][0])
        await self.cog.show_archive_details(interaction, archive_id)

    async def prev_page_callback(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self.update_components()
        await interaction.response.edit_message(view=self)

    async def next_page_callback(self, interaction: discord.Interaction):
        self.page = min(self.max_page, self.page + 1)
        self.update_components()
        await interaction.response.edit_message(view=self)

    async def back_button_callback(self, interaction: discord.Interaction):
        await self.cog.show_archive_menu(interaction)

class ClearAfterSaveView(discord.ui.View):
    def __init__(self, bot, cog, archive_id):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archive_id = archive_id

    @discord.ui.button(label="Yes, Clear All", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def yes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.clear_all_after_archive(interaction, self.archive_id)

    @discord.ui.button(label="No, Keep Appointments", style=discord.ButtonStyle.secondary, emoji="❌")
    async def no_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_menu(interaction)

    @discord.ui.button(label="Back to Main Menu", style=discord.ButtonStyle.primary, emoji="⬅️")
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        minister_menu_cog = self.bot.get_cog("MinisterMenu")
        if minister_menu_cog:
            await minister_menu_cog.show_minister_channel_menu(interaction)

class DeleteArchiveConfirmView(discord.ui.View):
    def __init__(self, bot, cog, archive_id, archive_info):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archive_id = archive_id
        self.archive_info = archive_info

    @discord.ui.button(label="Confirm Delete", style=discord.ButtonStyle.danger, emoji="⚠️")
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.delete_archive(interaction, self.archive_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_details(interaction, self.archive_id)

class PostArchiveChannelSelect(discord.ui.ChannelSelect):
    def __init__(self, cog, archive_id, appointment_type, appointments):
        super().__init__(
            placeholder="Select a channel to post to...",
            channel_types=[discord.ChannelType.text],
            min_values=1,
            max_values=1
        )
        self.cog = cog
        self.archive_id = archive_id
        self.appointment_type = appointment_type
        self.appointments = appointments

    async def callback(self, interaction: discord.Interaction):
        selected_channel = self.values[0]
        # Fetch the actual channel object from the guild
        channel = interaction.guild.get_channel(selected_channel.id)
        if not channel:
            await interaction.response.send_message("❌ Could not access the selected channel.", ephemeral=True)
            return
        await self.cog.post_archive_to_channel(interaction, channel, self.archive_id, self.appointment_type, self.appointments)

class PostArchiveChannelView(discord.ui.View):
    def __init__(self, bot, cog, archive_id, appointment_type, appointments):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.archive_id = archive_id
        self.appointment_type = appointment_type
        self.appointments = appointments

        # Add channel select
        self.add_item(PostArchiveChannelSelect(cog, archive_id, appointment_type, appointments))

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️", row=1)
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_appointments(interaction, self.archive_id, self.appointment_type)

class SaveArchiveModal(discord.ui.Modal, title="Save Minister Schedule Archive"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

        # Generate default name based on current date
        default_name = datetime.now().strftime("SvS %Y-%m-%d")

        self.archive_name = discord.ui.TextInput(
            label="Archive Name",
            placeholder="Enter a name for this archive...",
            default=default_name,
            required=True,
            max_length=100
        )
        self.add_item(self.archive_name)

    async def on_submit(self, interaction: discord.Interaction):
        archive_name = self.archive_name.value.strip()
        if not archive_name:
            archive_name = datetime.now().strftime("SvS %Y-%m-%d")

        await self.cog.save_current_schedule(interaction, archive_name)

class ArchiveMenuView(discord.ui.View):
    def __init__(self, bot, cog):
        super().__init__(timeout=None)
        self.bot = bot
        self.cog = cog

    @discord.ui.button(label="Save Current Schedule", style=discord.ButtonStyle.success, emoji="💾")
    async def save_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = SaveArchiveModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="View Archives", style=discord.ButtonStyle.primary, emoji="📚")
    async def view_archives(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_archive_list(interaction)

    @discord.ui.button(label="Current Change History", style=discord.ButtonStyle.primary, emoji="📜")
    async def view_history(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_change_history(interaction)

    @discord.ui.button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️")
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        minister_menu_cog = self.bot.get_cog("MinisterMenu")
        if minister_menu_cog:
            await minister_menu_cog.show_minister_channel_menu(interaction)

class ChangeHistoryView(discord.ui.View):
    def __init__(self, bot, cog, history_records, page=0, archive_id=None):
        super().__init__(timeout=7200)
        self.bot = bot
        self.cog = cog
        self.history_records = history_records
        self.page = page
        self.archive_id = archive_id
        self.max_page = (len(history_records) - 1) // 25 if history_records else 0

        self.update_components()

    def update_components(self):
        self.clear_items()

        # Pagination buttons
        if self.max_page > 0:
            prev_button = discord.ui.Button(
                label="◀️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page == 0
            )
            prev_button.callback = self.prev_page_callback
            self.add_item(prev_button)

            next_button = discord.ui.Button(
                label="▶️",
                style=discord.ButtonStyle.secondary,
                disabled=self.page >= self.max_page
            )
            next_button.callback = self.next_page_callback
            self.add_item(next_button)

        # Back button
        back_button = discord.ui.Button(
            label="Back",
            style=discord.ButtonStyle.primary,
            emoji="⬅️",
            row=1 if self.max_page > 0 else 0
        )
        back_button.callback = self.back_button_callback
        self.add_item(back_button)

    async def prev_page_callback(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self.update_components()
        await self.cog.update_history_embed(interaction, self.history_records, self.page, self.archive_id, self)

    async def next_page_callback(self, interaction: discord.Interaction):
        self.page = min(self.max_page, self.page + 1)
        self.update_components()
        await self.cog.update_history_embed(interaction, self.history_records, self.page, self.archive_id, self)

    async def back_button_callback(self, interaction: discord.Interaction):
        if self.archive_id:
            await self.cog.show_archive_details(interaction, self.archive_id)
        else:
            await self.cog.show_archive_menu(interaction)

class MinisterArchive(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.svs_conn = sqlite3.connect("db/svs.sqlite")
        self.svs_cursor = self.svs_conn.cursor()
        self.users_conn = sqlite3.connect('db/users.sqlite')
        self.users_cursor = self.users_conn.cursor()

        # Create archive tables
        self.svs_cursor.execute("""
            CREATE TABLE IF NOT EXISTS minister_archives (
                archive_id INTEGER PRIMARY KEY AUTOINCREMENT,
                archive_name TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL,
                created_by_id INTEGER NOT NULL,
                created_by_name TEXT NOT NULL
            );
        """)

        self.svs_cursor.execute("""
            CREATE TABLE IF NOT EXISTS minister_archive_appointments (
                archive_id INTEGER NOT NULL,
                fid INTEGER NOT NULL,
                appointment_type TEXT NOT NULL,
                time TEXT NOT NULL,
                alliance INTEGER NOT NULL,
                nickname TEXT NOT NULL,
                FOREIGN KEY (archive_id) REFERENCES minister_archives(archive_id)
            );
        """)

        self.svs_cursor.execute("""
            CREATE TABLE IF NOT EXISTS minister_change_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                archive_id INTEGER,
                timestamp TIMESTAMP NOT NULL,
                discord_user_id INTEGER NOT NULL,
                discord_username TEXT NOT NULL,
                action_type TEXT NOT NULL,
                appointment_type TEXT,
                fid INTEGER,
                nickname TEXT,
                old_time TEXT,
                new_time TEXT,
                alliance_name TEXT,
                additional_data TEXT,
                FOREIGN KEY (archive_id) REFERENCES minister_archives(archive_id)
            );
        """)

        self.svs_conn.commit()

    async def is_global_admin(self, user_id: int) -> bool:
        """Check if user is a global admin"""
        minister_menu_cog = self.bot.get_cog("MinisterMenu")
        if minister_menu_cog:
            is_admin, is_global_admin, _ = await minister_menu_cog.get_admin_permissions(user_id)
            return is_global_admin
        return False

    async def show_archive_menu(self, interaction: discord.Interaction):
        """Show the main archive menu"""
        # Check global admin permission
        if not await self.is_global_admin(interaction.user.id):
            await interaction.response.send_message("❌ Only Global Admins can access archives.", ephemeral=True)
            return

        embed = discord.Embed(
            title="📚 Minister Schedule Archives",
            description=(
                "Manage minister schedule archives and change history:\n\n"
                "Available Operations\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💾 **Save Current Schedule**\n"
                "└ Archive current minister appointments\n\n"
                "📚 **View Archives**\n"
                "└ Browse previously saved archives\n\n"
                "📜 **Current Change History**\n"
                "└ See all changes made since the last archive\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.blue()
        )

        view = ArchiveMenuView(self.bot, self)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def save_current_schedule(self, interaction: discord.Interaction, archive_name: str):
        """Save the current minister schedule to an archive"""
        await interaction.response.defer()

        try:
            # Get current appointments
            self.svs_cursor.execute("SELECT fid, appointment_type, time, alliance FROM appointments")
            appointments = self.svs_cursor.fetchall()

            if not appointments:
                await interaction.followup.send("❌ No appointments to archive.", ephemeral=True)
                return

            # Create archive record
            created_at = datetime.now().isoformat()
            created_by_id = interaction.user.id
            created_by_name = interaction.user.display_name

            self.svs_cursor.execute("""
                INSERT INTO minister_archives (archive_name, created_at, created_by_id, created_by_name)
                VALUES (?, ?, ?, ?)
            """, (archive_name, created_at, created_by_id, created_by_name))

            archive_id = self.svs_cursor.lastrowid

            # Save appointments with nicknames
            for fid, appointment_type, time, alliance in appointments:
                # Get nickname from users table
                self.users_cursor.execute("SELECT nickname FROM users WHERE fid=?", (fid,))
                user_result = self.users_cursor.fetchone()
                nickname = user_result[0] if user_result else f"ID: {fid}"

                self.svs_cursor.execute("""
                    INSERT INTO minister_archive_appointments
                    (archive_id, fid, appointment_type, time, alliance, nickname)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (archive_id, fid, appointment_type, time, alliance, nickname))

            self.svs_conn.commit()

            # Assign change history entries to this archive (only those before archive creation time)
            self.svs_cursor.execute("""
                SELECT MAX(archive_id) FROM minister_archives WHERE archive_id < ?
            """, (archive_id,))
            last_archive_result = self.svs_cursor.fetchone()
            last_archive_id = last_archive_result[0] if last_archive_result and last_archive_result[0] else None

            if last_archive_id:
                self.svs_cursor.execute("""
                    SELECT created_at FROM minister_archives WHERE archive_id = ?
                """, (last_archive_id,))
                last_archive_time = self.svs_cursor.fetchone()[0]

                self.svs_cursor.execute("""
                    UPDATE minister_change_history
                    SET archive_id = ?
                    WHERE archive_id IS NULL AND timestamp > ? AND timestamp <= ?
                """, (archive_id, last_archive_time, created_at))
            else:
                self.svs_cursor.execute("""
                    UPDATE minister_change_history
                    SET archive_id = ?
                    WHERE archive_id IS NULL AND timestamp <= ?
                """, (archive_id, created_at))

            self.svs_conn.commit()

            # Log archive creation in change history (after commit)
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                additional_data = json.dumps({
                    "archive_id": archive_id,
                    "archive_name": archive_name,
                    "appointment_count": len(appointments)
                })

                await minister_schedule_cog.log_change(
                    action_type="archive_created",
                    user=interaction.user,
                    appointment_type=None,
                    fid=None,
                    nickname=None,
                    old_time=None,
                    new_time=None,
                    alliance_name=None,
                    additional_data=additional_data,
                    archive_id=archive_id
                )

            # Show confirmation and prompt for clearing
            embed = discord.Embed(
                title="✅ Archive Created Successfully",
                description=(
                    f"**Archive Name:** {archive_name}\n"
                    f"**Archive ID:** {archive_id}\n"
                    f"**Appointments Saved:** {len(appointments)}\n"
                    f"**Created By:** {created_by_name}\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "**Would you like to clear all minister appointments to prepare for the next SvS?**\n\n"
                    "⚠️ This will remove all current appointments across Construction, Research, and Training days."
                ),
                color=discord.Color.green()
            )
            embed.set_footer(text=f"Created at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            view = ClearAfterSaveView(self.bot, self, archive_id)
            await interaction.followup.send(embed=embed, view=view)

        except Exception as e:
            await interaction.followup.send(f"❌ Error creating archive: {e}", ephemeral=True)

    async def clear_all_after_archive(self, interaction: discord.Interaction, archive_id: int):
        """Clear all minister appointments after archiving"""
        await interaction.response.defer()

        try:
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if not minister_schedule_cog:
                await interaction.followup.send("❌ Minister Schedule module not found.", ephemeral=True)
                return

            cleared_total = 0
            for appointment_type in ["Construction Day", "Research Day", "Troops Training Day"]:
                # Get appointments before deletion for logging
                self.svs_cursor.execute("SELECT fid FROM appointments WHERE appointment_type=?", (appointment_type,))
                fids = [row[0] for row in self.svs_cursor.fetchall()]
                cleared_total += len(fids)

                # Delete appointments
                self.svs_cursor.execute("DELETE FROM appointments WHERE appointment_type=?", (appointment_type,))

            self.svs_conn.commit()

            # Log the clear action
            additional_data = json.dumps({
                "archive_id": archive_id,
                "cleared_count": cleared_total,
                "post_archive_clear": True
            })

            await minister_schedule_cog.log_change(
                action_type="clear_all",
                user=interaction.user,
                appointment_type="All Types",
                fid=None,
                nickname=None,
                old_time=None,
                new_time=None,
                alliance_name=None,
                additional_data=additional_data
            )

            # Update all channel messages
            minister_menu_cog = self.bot.get_cog("MinisterMenu")
            if minister_menu_cog:
                for activity_name in ["Construction Day", "Research Day", "Troops Training Day"]:
                    await minister_menu_cog.update_channel_message(activity_name)

            # Send log to minister log channel
            embed = discord.Embed(
                title="🗑️ All Minister Appointments Cleared",
                description=f"All {cleared_total} appointments cleared after archiving.\n\nReady for next SvS prep week.",
                color=discord.Color.orange()
            )
            embed.set_author(
                name=f"Cleared by {interaction.user.display_name}",
                icon_url=interaction.user.avatar.url if interaction.user.avatar else None
            )
            await minister_schedule_cog.send_embed_to_channel(embed)

            # Return to main menu
            embed = discord.Embed(
                title="✅ Appointments Cleared",
                description=f"Successfully cleared {cleared_total} minister appointments.\n\nThe system is now ready for the next SvS prep week.",
                color=discord.Color.green()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)

            minister_menu_cog = self.bot.get_cog("MinisterMenu")
            if minister_menu_cog:
                await minister_menu_cog.show_minister_channel_menu(interaction)

        except Exception as e:
            await interaction.followup.send(f"❌ Error clearing appointments: {e}", ephemeral=True)

    async def show_archive_list(self, interaction: discord.Interaction):
        """Show list of all archives"""
        try:
            # Get all archives with appointment counts
            self.svs_cursor.execute("""
                SELECT
                    ma.archive_id,
                    ma.archive_name,
                    ma.created_at,
                    ma.created_by_name,
                    COUNT(maa.fid) as appointment_count
                FROM minister_archives ma
                LEFT JOIN minister_archive_appointments maa ON ma.archive_id = maa.archive_id
                GROUP BY ma.archive_id
                ORDER BY ma.created_at DESC
            """)
            archives = self.svs_cursor.fetchall()

            if not archives:
                embed = discord.Embed(
                    title="📚 Minister Schedule Archives",
                    description="No archives found.\n\nUse the **Save Current Schedule** button to create your first archive.",
                    color=discord.Color.blue()
                )
                view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️")

                async def back_callback(inter: discord.Interaction):
                    await self.show_archive_menu(inter)

                back_button.callback = back_callback
                view.add_item(back_button)

                if not interaction.response.is_done():
                    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
                else:
                    await interaction.response.edit_message(embed=embed, view=view)
                return

            embed = discord.Embed(
                title="📚 Minister Schedule Archives",
                description=f"**Total Archives:** {len(archives)}\n\nSelect an archive to view details:",
                color=discord.Color.blue()
            )

            view = ArchiveListView(self.bot, self, archives)

            if not interaction.response.is_done():
                await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            else:
                await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error loading archives: {e}", ephemeral=True)

    async def show_archive_details(self, interaction: discord.Interaction, archive_id: int):
        """Show details of a specific archive"""
        try:
            # Get archive info
            self.svs_cursor.execute("""
                SELECT archive_name, created_at, created_by_name
                FROM minister_archives
                WHERE archive_id = ?
            """, (archive_id,))
            archive_info = self.svs_cursor.fetchone()

            if not archive_info:
                await interaction.response.send_message("❌ Archive not found.", ephemeral=True)
                return

            archive_name, created_at, created_by_name = archive_info

            # Get appointments by type
            self.svs_cursor.execute("""
                SELECT appointment_type, COUNT(*) as count
                FROM minister_archive_appointments
                WHERE archive_id = ?
                GROUP BY appointment_type
            """, (archive_id,))
            type_counts = self.svs_cursor.fetchall()

            type_breakdown = "\n".join([f"• {atype}: {count} appointments" for atype, count in type_counts])
            total_count = sum(count for _, count in type_counts)

            created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")

            embed = discord.Embed(
                title=f"📚 Archive: {archive_name}",
                description=(
                    f"**Archive ID:** {archive_id}\n"
                    f"**Created:** {created_date}\n"
                    f"**Created By:** {created_by_name}\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"**Total Appointments:** {total_count}\n\n"
                    f"{type_breakdown if type_breakdown else 'No appointments in this archive.'}\n\n"
                    "Click a button below to view appointments for each day:"
                ),
                color=discord.Color.blue()
            )

            view = ArchiveDetailsView(self.bot, self, archive_id, archive_info, type_counts)

            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                await interaction.edit_original_response(embed=embed, view=view)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error loading archive details: {e}", ephemeral=True)

    async def show_archive_appointments(self, interaction: discord.Interaction, archive_id: int, appointment_type: str):
        """Show detailed appointment list for a specific activity day in an archive"""
        try:
            # Get alliance database connection
            alliance_conn = sqlite3.connect('db/alliance.sqlite')
            alliance_cursor = alliance_conn.cursor()

            # Get appointments for this archive and appointment type
            self.svs_cursor.execute("""
                SELECT time, fid, nickname, alliance
                FROM minister_archive_appointments
                WHERE archive_id = ? AND appointment_type = ?
                ORDER BY time
            """, (archive_id, appointment_type))
            appointments = self.svs_cursor.fetchall()

            if not appointments:
                embed = discord.Embed(
                    title=f"📋 {appointment_type} Appointments",
                    description=f"No appointments found for {appointment_type} in this archive.",
                    color=discord.Color.blue()
                )
                view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️")

                async def back_callback(inter: discord.Interaction):
                    await self.show_archive_details(inter, archive_id)

                back_button.callback = back_callback
                view.add_item(back_button)

                await interaction.response.edit_message(embed=embed, view=view)
                alliance_conn.close()
                return

            # Create view with appointments
            view = ArchiveAppointmentsView(self.bot, self, archive_id, appointment_type, appointments)
            await self.update_appointments_embed(interaction, view)

            alliance_conn.close()

        except Exception as e:
            await interaction.response.send_message(f"❌ Error loading appointments: {e}", ephemeral=True)

    async def update_appointments_embed(self, interaction: discord.Interaction, view: ArchiveAppointmentsView):
        """Update the appointments embed with paginated records"""
        try:
            # Get alliance database connection
            alliance_conn = sqlite3.connect('db/alliance.sqlite')
            alliance_cursor = alliance_conn.cursor()

            # Calculate page boundaries
            start_idx = view.page * 25
            end_idx = min(start_idx + 25, len(view.appointments))
            page_appointments = view.appointments[start_idx:end_idx]

            # Build appointment lines
            appointment_lines = []
            for time, fid, nickname, alliance_id in page_appointments:
                # Get alliance name
                alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
                alliance_result = alliance_cursor.fetchone()
                alliance_name = alliance_result[0] if alliance_result else "Unknown"

                # Format line like current schedule display
                appointment_lines.append(f"`{time}` - [{alliance_name}] {nickname} - {fid}")

            description = "\n".join(appointment_lines) if appointment_lines else "No appointments to display."

            embed = discord.Embed(
                title=f"📋 {view.appointment_type} - Archive Appointments",
                description=description,
                color=discord.Color.blue()
            )
            embed.set_footer(text=f"Page {view.page + 1}/{view.max_page + 1} • Total: {len(view.appointments)} appointments")

            try:
                await interaction.response.edit_message(embed=embed, view=view)
            except discord.InteractionResponded:
                await interaction.edit_original_response(embed=embed, view=view)

            alliance_conn.close()

        except Exception as e:
            print(f"Error updating appointments embed: {e}")

    async def show_channel_selector_for_post(self, interaction: discord.Interaction, archive_id: int, appointment_type: str, appointments):
        """Show channel selector for posting archive appointments"""
        embed = discord.Embed(
            title=f"📤 Post {appointment_type} to Channel",
            description=(
                f"Select a channel to post the archived **{appointment_type}** appointments.\n\n"
                f"**Total Appointments:** {len(appointments)}\n\n"
                "This will create a formatted message in the selected channel showing all appointments."
            ),
            color=discord.Color.green()
        )

        view = PostArchiveChannelView(self.bot, self, archive_id, appointment_type, appointments)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def post_archive_to_channel(self, interaction: discord.Interaction, channel: discord.TextChannel, archive_id: int, appointment_type: str, appointments):
        """Post archive appointments to selected channel"""
        await interaction.response.defer()

        try:
            # Get alliance database connection
            alliance_conn = sqlite3.connect('db/alliance.sqlite')
            alliance_cursor = alliance_conn.cursor()

            # Get archive info
            self.svs_cursor.execute("""
                SELECT archive_name, created_at
                FROM minister_archives
                WHERE archive_id = ?
            """, (archive_id,))
            archive_info = self.svs_cursor.fetchone()

            if not archive_info:
                await interaction.followup.send("❌ Archive not found.", ephemeral=True)
                return

            archive_name, created_at = archive_info
            created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d")

            # Build appointment list
            appointment_lines = []
            for time, fid, nickname, alliance_id in appointments:
                # Get alliance name
                alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id=?", (alliance_id,))
                alliance_result = alliance_cursor.fetchone()
                alliance_name = alliance_result[0] if alliance_result else "Unknown"

                # Format line like current schedule display
                appointment_lines.append(f"`{time}` - [{alliance_name}] {nickname} - {fid}")

            # Split into chunks if too long (Discord embed description limit is 4096 characters)
            description_text = "\n".join(appointment_lines)

            if len(description_text) > 4000:
                # Split into multiple embeds if needed
                chunks = []
                current_chunk = []
                current_length = 0

                for line in appointment_lines:
                    line_length = len(line) + 1  # +1 for newline
                    if current_length + line_length > 4000:
                        chunks.append("\n".join(current_chunk))
                        current_chunk = [line]
                        current_length = line_length
                    else:
                        current_chunk.append(line)
                        current_length += line_length

                if current_chunk:
                    chunks.append("\n".join(current_chunk))

                # Post first embed with header
                embed = discord.Embed(
                    title=f"📋 {appointment_type} - {archive_name}",
                    description=chunks[0],
                    color=discord.Color.blue()
                )
                embed.set_footer(text=f"Archive from {created_date} • Part 1/{len(chunks)} • Total: {len(appointments)} appointments")
                await channel.send(embed=embed)

                # Post remaining chunks
                for i, chunk in enumerate(chunks[1:], start=2):
                    embed = discord.Embed(
                        description=chunk,
                        color=discord.Color.blue()
                    )
                    embed.set_footer(text=f"Part {i}/{len(chunks)}")
                    await channel.send(embed=embed)
            else:
                # Single embed
                embed = discord.Embed(
                    title=f"📋 {appointment_type} - {archive_name}",
                    description=description_text,
                    color=discord.Color.blue()
                )
                embed.set_footer(text=f"Archive from {created_date} • Total: {len(appointments)} appointments")
                await channel.send(embed=embed)

            alliance_conn.close()

            # Show success message
            success_embed = discord.Embed(
                title="✅ Posted to Channel",
                description=f"Successfully posted **{len(appointments)} {appointment_type}** appointments to {channel.mention}",
                color=discord.Color.green()
            )

            await interaction.followup.send(embed=success_embed, ephemeral=True)

            # Return to appointments view
            await self.show_archive_appointments(interaction, archive_id, appointment_type)

        except Exception as e:
            await interaction.followup.send(f"❌ Error posting to channel: {e}", ephemeral=True)

    async def show_delete_archive_confirmation(self, interaction: discord.Interaction, archive_id: int, archive_info):
        """Show confirmation dialog before deleting an archive"""
        archive_name, created_at, created_by_name = archive_info
        created_date = datetime.fromisoformat(created_at).strftime("%Y-%m-%d %H:%M:%S")

        # Get appointment count
        self.svs_cursor.execute("""
            SELECT COUNT(*) FROM minister_archive_appointments WHERE archive_id = ?
        """, (archive_id,))
        appointment_count = self.svs_cursor.fetchone()[0]

        embed = discord.Embed(
            title="⚠️ Delete Archive Confirmation",
            description=(
                f"**Are you sure you want to delete this archive?**\n\n"
                f"**Archive Name:** {archive_name}\n"
                f"**Archive ID:** {archive_id}\n"
                f"**Created:** {created_date}\n"
                f"**Created By:** {created_by_name}\n"
                f"**Appointments:** {appointment_count}\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚠️ **This action cannot be undone!**\n\n"
                "This will permanently delete:\n"
                f"• All {appointment_count} archived appointments\n"
                "• Associated change history for this archive\n"
                "• Archive metadata"
            ),
            color=discord.Color.red()
        )

        view = DeleteArchiveConfirmView(self.bot, self, archive_id, archive_info)

        try:
            await interaction.response.edit_message(embed=embed, view=view)
        except discord.InteractionResponded:
            await interaction.edit_original_response(embed=embed, view=view)

    async def delete_archive(self, interaction: discord.Interaction, archive_id: int):
        """Delete an archive and all associated data"""
        await interaction.response.defer()

        try:
            # Get archive info for logging
            self.svs_cursor.execute("""
                SELECT archive_name, created_at, created_by_name
                FROM minister_archives
                WHERE archive_id = ?
            """, (archive_id,))
            archive_info = self.svs_cursor.fetchone()

            if not archive_info:
                await interaction.followup.send("❌ Archive not found.", ephemeral=True)
                return

            archive_name, created_at, created_by_name = archive_info

            # Count appointments before deletion
            self.svs_cursor.execute("""
                SELECT COUNT(*) FROM minister_archive_appointments WHERE archive_id = ?
            """, (archive_id,))
            appointment_count = self.svs_cursor.fetchone()[0]

            # Delete all associated data
            # 1. Delete archived appointments
            self.svs_cursor.execute("""
                DELETE FROM minister_archive_appointments WHERE archive_id = ?
            """, (archive_id,))

            # 2. Delete change history associated with this archive
            self.svs_cursor.execute("""
                DELETE FROM minister_change_history WHERE archive_id = ?
            """, (archive_id,))

            # 3. Delete the archive record itself
            self.svs_cursor.execute("""
                DELETE FROM minister_archives WHERE archive_id = ?
            """, (archive_id,))

            self.svs_conn.commit()

            # Log the deletion to minister log channel
            minister_schedule_cog = self.bot.get_cog("MinisterSchedule")
            if minister_schedule_cog:
                embed = discord.Embed(
                    title="🗑️ Archive Deleted",
                    description=(
                        f"**Archive:** {archive_name}\n"
                        f"**Archive ID:** {archive_id}\n"
                        f"**Appointments Deleted:** {appointment_count}\n"
                        f"**Originally Created:** {datetime.fromisoformat(created_at).strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"**Originally Created By:** {created_by_name}"
                    ),
                    color=discord.Color.dark_red()
                )
                embed.set_author(
                    name=f"Deleted by {interaction.user.display_name}",
                    icon_url=interaction.user.avatar.url if interaction.user.avatar else None
                )
                await minister_schedule_cog.send_embed_to_channel(embed)

            # Show success message and return to archive list
            success_embed = discord.Embed(
                title="✅ Archive Deleted Successfully",
                description=(
                    f"**Archive:** {archive_name}\n"
                    f"**Archive ID:** {archive_id}\n\n"
                    f"Deleted {appointment_count} appointments and all associated data."
                ),
                color=discord.Color.green()
            )

            await interaction.followup.send(embed=success_embed, ephemeral=True)
            await self.show_archive_list(interaction)

        except Exception as e:
            await interaction.followup.send(f"❌ Error deleting archive: {e}", ephemeral=True)

    async def show_change_history(self, interaction: discord.Interaction, archive_id: int = None):
        """Show change history (for specific archive or all current changes)"""
        try:
            # Get change history
            if archive_id:
                self.svs_cursor.execute("""
                    SELECT
                        timestamp, discord_username, action_type, appointment_type,
                        fid, nickname, old_time, new_time, alliance_name, additional_data
                    FROM minister_change_history
                    WHERE archive_id = ?
                    ORDER BY timestamp DESC
                """, (archive_id,))
            else:
                self.svs_cursor.execute("""
                    SELECT
                        timestamp, discord_username, action_type, appointment_type,
                        fid, nickname, old_time, new_time, alliance_name, additional_data
                    FROM minister_change_history
                    WHERE archive_id IS NULL
                    ORDER BY timestamp DESC
                """)

            history_records = self.svs_cursor.fetchall()

            if not history_records:
                title = f"📜 Change History - Archive #{archive_id}" if archive_id else "📜 Current Change History"
                embed = discord.Embed(
                    title=title,
                    description="No change history found.",
                    color=discord.Color.blue()
                )
                view = discord.ui.View(timeout=7200)
                back_button = discord.ui.Button(label="Back", style=discord.ButtonStyle.primary, emoji="⬅️")

                async def back_callback(inter: discord.Interaction):
                    if archive_id:
                        await self.show_archive_details(inter, archive_id)
                    else:
                        await self.show_archive_menu(inter)

                back_button.callback = back_callback
                view.add_item(back_button)

                await interaction.response.edit_message(embed=embed, view=view)
                return

            view = ChangeHistoryView(self.bot, self, history_records, page=0, archive_id=archive_id)
            await self.update_history_embed(interaction, history_records, 0, archive_id, view)

        except Exception as e:
            await interaction.response.send_message(f"❌ Error loading change history: {e}", ephemeral=True)

    async def show_archive_change_history(self, interaction: discord.Interaction, archive_id: int):
        """Show change history for a specific archive"""
        await self.show_change_history(interaction, archive_id)

    async def update_history_embed(self, interaction: discord.Interaction, history_records, page, archive_id, view):
        """Update the history embed with paginated records"""
        start_idx = page * 25
        end_idx = min(start_idx + 25, len(history_records))
        page_records = history_records[start_idx:end_idx]

        history_lines = []
        action_emojis = {
            "add": "➕",
            "remove": "➖",
            "reschedule": "🔄",
            "clear_all": "🗑️",
            "time_slot_mode_change": "🕐",
            "archive_created": "💾"
        }

        for timestamp, username, action_type, appointment_type, fid, nickname, old_time, new_time, alliance_name, additional_data in page_records:
            emoji = action_emojis.get(action_type, "📝")
            dt = datetime.fromisoformat(timestamp).strftime("%m/%d %H:%M")

            if action_type == "add":
                history_lines.append(f"{emoji} `{dt}` **{username}** added **{nickname}** to {appointment_type} at `{new_time}`")
            elif action_type == "remove":
                history_lines.append(f"{emoji} `{dt}` **{username}** removed **{nickname}** from {appointment_type}")
            elif action_type == "reschedule":
                history_lines.append(f"{emoji} `{dt}` **{username}** moved **{nickname}** from `{old_time}` to `{new_time}` ({appointment_type})")
            elif action_type == "clear_all":
                history_lines.append(f"{emoji} `{dt}` **{username}** cleared {appointment_type} appointments")
            elif action_type == "time_slot_mode_change":
                history_lines.append(f"{emoji} `{dt}` **{username}** changed time slot mode")
            elif action_type == "archive_created":
                if additional_data:
                    try:
                        data = json.loads(additional_data)
                        archive_name = data.get("archive_name", "Unknown")
                        history_lines.append(f"{emoji} `{dt}` **{username}** created archive: **{archive_name}**")
                    except:
                        history_lines.append(f"{emoji} `{dt}` **{username}** created an archive")
                else:
                    history_lines.append(f"{emoji} `{dt}` **{username}** created an archive")
            else:
                history_lines.append(f"{emoji} `{dt}` **{username}** - {action_type}")

        title = f"📜 Change History - Archive #{archive_id}" if archive_id else "📜 Current Change History"
        description = "\n".join(history_lines) if history_lines else "No changes to display."

        embed = discord.Embed(
            title=title,
            description=description,
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"Page {page + 1}/{((len(history_records) - 1) // 25) + 1} • Total: {len(history_records)} changes")

        if not interaction.response.is_done():
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
        else:
            await interaction.response.edit_message(embed=embed, view=view)

async def setup(bot):
    await bot.add_cog(MinisterArchive(bot))