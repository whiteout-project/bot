import discord
from discord.ext import commands
import sqlite3
from datetime import datetime
from .alliance_member_operations import AllianceSelectView
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

class Changes(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn_settings = sqlite3.connect('db/settings.sqlite')
        self.c_settings = self.conn_settings.cursor()
        self.conn = sqlite3.connect('db/changes.sqlite')
        self.cursor = self.conn.cursor()
        self._create_tables()
        
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

    def _create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS furnace_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fid INTEGER,
                old_value INTEGER,
                new_value INTEGER,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()

    def cog_unload(self):
        if hasattr(self, 'cursor'):
            self.cursor.close()
        if hasattr(self, 'conn'):
            self.conn.close()

    async def show_alliance_history_menu(self, interaction: discord.Interaction):
        try:
            embed = discord.Embed(
                title=f"{theme.listIcon} Alliance History Menu",
                description=(
                    f"**Available Operations**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.stoveIcon} **Furnace Changes**\n"
                    f"└ View furnace level changes\n\n"
                    f"{theme.editListIcon} **Nickname Changes**\n"
                    f"└ View nickname history\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = HistoryView(self)
            await interaction.response.edit_message(embed=embed, view=view)
            
        except Exception as e:
            if not any(error_code in str(e) for error_code in ["10062", "40060"]):
                print(f"Show alliance history menu error: {e}")

    async def show_furnace_history(self, interaction: discord.Interaction, fid: int):
        try:
            self.cursor.execute("""
                SELECT old_furnace_lv, new_furnace_lv, change_date 
                FROM furnace_changes 
                WHERE fid = ? 
                ORDER BY change_date DESC
            """, (fid,))
            
            changes = self.cursor.fetchall()
            
            if not changes:
                await interaction.followup.send(
                    "No furnace changes found for this player.",
                    ephemeral=True
                )
                return

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT nickname, furnace_lv FROM users WHERE fid = ?", (fid,))
                user_info = cursor.fetchone()
                nickname = user_info[0] if user_info else "Unknown"
                current_level = user_info[1] if user_info else 0

            embed = discord.Embed(
                title=f"{theme.levelIcon} Furnace Level History",
                description=(
                    f"**Player:** `{nickname}`\n"
                    f"**ID:** `{fid}`\n"
                    f"**Current Level:** `{self.level_mapping.get(current_level, str(current_level))}`\n"
                    f"{theme.upperDivider}\n"
                ),
                color=theme.emColor1
            )

            for old_level, new_level, change_date in changes:
                old_level_str = self.level_mapping.get(int(old_level), str(old_level))
                new_level_str = self.level_mapping.get(int(new_level), str(new_level))
                embed.add_field(
                    name=f"Level Change at {change_date}",
                    value=f"{theme.stoveOldIcon} `{old_level_str}` ➜ {theme.stoveIcon} `{new_level_str}`",
                    inline=False
                )

            await interaction.followup.send(embed=embed)

        except Exception as e:
            print(f"Error in show_furnace_history: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while displaying the furnace history.",
                ephemeral=True
            )

    async def show_nickname_history(self, interaction: discord.Interaction, fid: int):
        try:
            self.cursor.execute("""
                SELECT old_nickname, new_nickname, change_date 
                FROM nickname_changes 
                WHERE fid = ? 
                ORDER BY change_date DESC
            """, (fid,))
            
            changes = self.cursor.fetchall()
            
            if not changes:
                await interaction.followup.send(
                    "No nickname changes found for this player.",
                    ephemeral=True
                )
                return

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT nickname, furnace_lv FROM users WHERE fid = ?", (fid,))
                user_info = cursor.fetchone()
                nickname = user_info[0] if user_info else "Unknown"
                current_level = user_info[1] if user_info else 0

            embed = discord.Embed(
                title=f"{theme.editListIcon} Nickname History",
                description=(
                    f"**Player:** `{nickname}`\n"
                    f"**ID:** `{fid}`\n"
                    f"**Current Level:** `{self.level_mapping.get(current_level, str(current_level))}`\n"
                    f"{theme.upperDivider}\n"
                ),
                color=theme.emColor1
            )

            for old_name, new_name, change_date in changes:
                embed.add_field(
                    name=f"Nickname Change at {change_date}",
                    value=f"{theme.avatarOldIcon} `{old_name}` ➜ {theme.avatarIcon} `{new_name}`",
                    inline=False
                )

            await interaction.followup.send(embed=embed)

        except Exception as e:
            print(f"Error in show_nickname_history: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while displaying the nickname history.",
                ephemeral=True
            )

    async def show_member_list_nickname(self, interaction: discord.Interaction, alliance_id: int):
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_name = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY furnace_lv DESC, nickname
                """, (alliance_id,))
                members = cursor.fetchall()

            if not members:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No members found in this alliance.",
                    ephemeral=True
                )
                return

            embed = discord.Embed(
                title=f"{theme.editListIcon} {alliance_name} - Member List",
                description=(
                    f"Select a member to view nickname history:\n"
                    f"{theme.upperDivider}\n"
                    f"Total Members: {len(members)}\n"
                    f"Current Page: 1/{(len(members) + 24) // 25}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = MemberListViewNickname(self, members, alliance_name)
            
            await interaction.response.edit_message(
                embed=embed,
                view=view
            )

        except Exception as e:
            print(f"Error in show_member_list_nickname: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while displaying the member list.",
                ephemeral=True
            )

    async def show_recent_changes(self, interaction: discord.Interaction, alliance_name: str, hours: int):
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                alliance_id = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname 
                    FROM users 
                    WHERE alliance = ?
                """, (alliance_id,))
                members = {fid: name for fid, name in cursor.fetchall()}

            self.cursor.execute("""
                SELECT fid, old_furnace_lv, new_furnace_lv, change_date 
                FROM furnace_changes 
                WHERE fid IN ({})
                AND change_date >= datetime('now', '-{} hours')
                ORDER BY change_date DESC
            """.format(','.join('?' * len(members)), hours), tuple(members.keys()))
            
            changes = self.cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    f"No level changes found in the last {hours} hour(s) for {alliance_name}.",
                    ephemeral=True
                )
                return

            chunks = [changes[i:i + 25] for i in range(0, len(changes), 25)]
            
            view = RecentChangesView(chunks, members, self.level_mapping, alliance_name, hours)
            await interaction.followup.send(embed=view.get_embed(), view=view)

        except Exception as e:
            print(f"Error in show_recent_changes: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing recent changes.",
                ephemeral=True
            )

    async def show_recent_nickname_changes(self, interaction: discord.Interaction, alliance_name: str, hours: int):
        try:
            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT alliance_id FROM alliance_list WHERE name = ?", (alliance_name,))
                alliance_id = cursor.fetchone()[0]

            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname 
                    FROM users 
                    WHERE alliance = ?
                """, (alliance_id,))
                members = {fid: name for fid, name in cursor.fetchall()}

            self.cursor.execute("""
                SELECT fid, old_nickname, new_nickname, change_date 
                FROM nickname_changes 
                WHERE fid IN ({})
                AND change_date >= datetime('now', '-{} hours')
                ORDER BY change_date DESC
            """.format(','.join('?' * len(members)), hours), tuple(members.keys()))
            
            changes = self.cursor.fetchall()

            if not changes:
                await interaction.followup.send(
                    f"No nickname changes found in the last {hours} hour(s) for {alliance_name}.",
                    ephemeral=True
                )
                return

            chunks = [changes[i:i + 25] for i in range(0, len(changes), 25)]
            
            view = RecentNicknameChangesView(chunks, members, alliance_name, hours)
            await interaction.followup.send(embed=view.get_embed(), view=view)

        except Exception as e:
            print(f"Error in show_recent_nickname_changes: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing recent changes.",
                ephemeral=True
            )

class HistoryView(discord.ui.View):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        self.current_page = 0
        self.members_per_page = 25
        self.level_mapping = cog.level_mapping

    @discord.ui.button(
        label="Furnace Changes",
        emoji=f"{theme.stoveIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="furnace_changes",
        row=0
    )
    async def furnace_changes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            alliances, is_global = PermissionManager.get_admin_alliances(
                interaction.user.id,
                interaction.guild_id
            )

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliances found for your permissions.",
                    ephemeral=True
                )
                return

            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            select_embed = discord.Embed(
                title=f"{theme.stoveIcon} Furnace Changes",
                description=(
                    f"Select an alliance to view furnace changes:\n\n"
                    f"**Permission Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                    f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                    f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = AllianceSelectView(alliances_with_counts, self.cog, page=0, context="furnace_history")

            async def alliance_callback(select_interaction: discord.Interaction):
                try:
                    alliance_id = int(view.current_select.values[0])
                    await self.member_callback(select_interaction, alliance_id)
                except Exception as e:
                    print(f"Error in alliance selection: {e}")
                    if not select_interaction.response.is_done():
                        await select_interaction.response.send_message(
                            f"{theme.deniedIcon} An error occurred while processing your selection.",
                            ephemeral=True
                        )
                    else:
                        await select_interaction.followup.send(
                            f"{theme.deniedIcon} An error occurred while processing your selection.",
                            ephemeral=True
                        )

            view.callback = alliance_callback
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"Error in furnace_changes_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing the request.",
                ephemeral=True
            )

    async def member_callback(self, interaction: discord.Interaction, alliance_id: int):
        try:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("""
                    SELECT fid, nickname, furnace_lv 
                    FROM users 
                    WHERE alliance = ? 
                    ORDER BY furnace_lv DESC, nickname
                """, (alliance_id,))
                members = cursor.fetchall()

            if not members:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No members found in this alliance.",
                    ephemeral=True
                )
                return

            with sqlite3.connect('db/alliance.sqlite') as alliance_db:
                cursor = alliance_db.cursor()
                cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
                alliance_name = cursor.fetchone()[0]

            view = MemberListView(self.cog, members, alliance_name)
            
            embed = discord.Embed(
                title=f"{theme.levelIcon} {alliance_name} - Member List",
                description=(
                    f"Select a member to view furnace history:\n"
                    f"{theme.upperDivider}\n"
                    f"Total Members: {len(members)}\n"
                    f"Current Page: 1/{view.total_pages}\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            print(f"Error in member_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing member list.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing member list.",
                    ephemeral=True
                )

    @discord.ui.button(
        label="Nickname Changes",
        emoji=f"{theme.editListIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="nickname_changes",
        row=0
    )
    async def nickname_changes_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            alliances, is_global = PermissionManager.get_admin_alliances(
                interaction.user.id,
                interaction.guild_id
            )

            if not alliances:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} No alliances found for your permissions.",
                    ephemeral=True
                )
                return

            alliances_with_counts = []
            for alliance_id, name in alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            select_embed = discord.Embed(
                title=f"{theme.editListIcon} Alliance Selection - Nickname Changes",
                description=(
                    f"Select an alliance to view nickname changes:\n\n"
                    f"**Permission Details**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.userIcon} **Access Level:** `{'Global Admin' if is_global else 'Alliance Admin'}`\n"
                    f"{theme.searchIcon} **Access Type:** `{'All Alliances' if is_global else 'Assigned Alliances'}`\n"
                    f"{theme.chartIcon} **Available Alliances:** `{len(alliances)}`\n"
                    f"{theme.lowerDivider}"
                ),
                color=theme.emColor1
            )

            view = AllianceSelectView(alliances_with_counts, self.cog, page=0, context="nickname_history")

            async def alliance_callback(select_interaction: discord.Interaction):
                try:
                    alliance_id = int(view.current_select.values[0])
                    await self.cog.show_member_list_nickname(select_interaction, alliance_id)
                except Exception as e:
                    print(f"Error in alliance selection: {e}")
                    if not select_interaction.response.is_done():
                        await select_interaction.response.send_message(
                            f"{theme.deniedIcon} An error occurred while processing your selection.",
                            ephemeral=True
                        )
                    else:
                        await select_interaction.followup.send(
                            f"{theme.deniedIcon} An error occurred while processing your selection.",
                            ephemeral=True
                        )

            view.callback = alliance_callback
            
            await interaction.response.send_message(
                embed=select_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            print(f"Error in nickname_changes_button: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing the request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Main Menu",
        emoji=f"{theme.homeIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu",
        row=1
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.show_main_menu(interaction)

    async def show_main_menu(self, interaction: discord.Interaction):
        try:
            alliance_cog = self.cog.bot.get_cog("Alliance")
            if alliance_cog:
                await alliance_cog.show_main_menu(interaction)
            else:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while returning to the main menu.",
                    ephemeral=True
                )
        except Exception as e:
            print(f"[ERROR] Main Menu error in changes: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "An error occurred while returning to the main menu.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    "An error occurred while returning to the main menu.",
                    ephemeral=True
                )

    async def last_hour_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, hours=1)
        except Exception as e:
            print(f"Error in last_hour_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def last_day_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, hours=24)
        except Exception as e:
            print(f"Error in last_day_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def custom_time_callback(self, interaction: discord.Interaction):
        try:
            modal = CustomTimeModal(self.cog, self.alliance_name)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"Error in custom_time_callback: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing the time input.",
                ephemeral=True
            )

class MemberListView(discord.ui.View):
    def __init__(self, cog, members, alliance_name):
        super().__init__()
        self.cog = cog
        self.members = members
        self.alliance_name = alliance_name
        self.current_page = 0
        self.total_pages = (len(members) + 24) // 25
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        start_idx = self.current_page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"Select a member (Page {self.current_page + 1}/{self.total_pages})",
            options=[
                discord.SelectOption(
                    label=f"{name}",
                    value=str(fid),
                    description=f"ID: {fid} | Level: {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}"
                ) for fid, name, furnace_lv in current_members
            ],
            row=0
        )

        async def member_callback(interaction):
            try:
                fid = int(select.values[0])
                await interaction.response.defer()
                await self.cog.show_furnace_history(interaction, fid)
            except Exception as e:
                print(f"Error in member_callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while showing furnace history.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while showing furnace history.",
                        ephemeral=True
                    )

        select.callback = member_callback
        self.add_item(select)

        last_hour_button = discord.ui.Button(
            label="Last Hour Changes",
            emoji=f"{theme.timeIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_hour",
            row=1
        )
        last_hour_button.callback = self.last_hour_callback
        self.add_item(last_hour_button)

        last_day_button = discord.ui.Button(
            label="Last 24h Changes",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_day",
            row=1
        )
        last_day_button.callback = self.last_day_callback
        self.add_item(last_day_button)

        custom_time_button = discord.ui.Button(
            label="Custom Time",
            emoji=f"{theme.settingsIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="custom_time",
            row=1
        )
        custom_time_button.callback = self.custom_time_callback
        self.add_item(custom_time_button)

        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="Previous",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="previous",
                disabled=self.current_page == 0,
                row=2
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="Next",
                emoji=f"{theme.forwardIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next",
                disabled=self.current_page == self.total_pages - 1,
                row=2
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

        search_button = discord.ui.Button(
            label="Search by ID",
            emoji=f"{theme.searchIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="search_fid",
            row=2
        )
        search_button.callback = self.search_callback
        self.add_item(search_button)

    async def last_hour_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, hours=1)
        except Exception as e:
            print(f"Error in last_hour_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def last_day_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, hours=24)
        except Exception as e:
            print(f"Error in last_day_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def custom_time_callback(self, interaction: discord.Interaction):
        try:
            modal = CustomTimeModal(self.cog, self.alliance_name)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"Error in custom_time_callback: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing the time input.",
                ephemeral=True
            )

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = max(0, self.current_page - 1)
        await self.update_page(interaction)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await self.update_page(interaction)

    async def search_callback(self, interaction: discord.Interaction):
        modal = FurnaceHistoryIDSearchModal(self.cog)
        await interaction.response.send_modal(modal)

    async def update_page(self, interaction: discord.Interaction):
        self.update_view()
        
        embed = discord.Embed(
            title=f"{theme.levelIcon} {self.alliance_name} - Member List",
            description=(
                f"Select a member to view furnace history:\n"
                f"{theme.upperDivider}\n"
                f"Total Members: {len(self.members)}\n"
                f"Current Page: {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await interaction.response.edit_message(embed=embed, view=self)

class FurnaceHistoryIDSearchModal(discord.ui.Modal, title="Search by ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        self.fid = discord.ui.TextInput(
            label="ID",
            placeholder="Enter ID number...",
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            fid = int(self.fid.value)
            await interaction.response.defer()
            await self.cog.show_furnace_history(interaction, fid)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid ID format. Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in FurnaceHistoryIDSearchModal on_submit: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )

class MemberListViewNickname(discord.ui.View):
    def __init__(self, cog, members, alliance_name):
        super().__init__()
        self.cog = cog
        self.members = members
        self.alliance_name = alliance_name
        self.current_page = 0
        self.total_pages = (len(members) + 24) // 25
        self.update_view()

    def update_view(self):
        self.clear_items()
        
        start_idx = self.current_page * 25
        end_idx = min(start_idx + 25, len(self.members))
        current_members = self.members[start_idx:end_idx]

        select = discord.ui.Select(
            placeholder=f"Select a member (Page {self.current_page + 1}/{self.total_pages})",
            options=[
                discord.SelectOption(
                    label=f"{name}",
                    value=str(fid),
                    description=f"ID: {fid} | Level: {self.cog.level_mapping.get(furnace_lv, str(furnace_lv))}"
                ) for fid, name, furnace_lv in current_members
            ],
            row=0
        )

        async def member_callback(interaction):
            try:
                fid = int(select.values[0])
                await interaction.response.defer()
                await self.cog.show_nickname_history(interaction, fid)
            except Exception as e:
                print(f"Error in member_callback: {e}")
                if not interaction.response.is_done():
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while showing nickname history.",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} An error occurred while showing nickname history.",
                        ephemeral=True
                    )

        select.callback = member_callback
        self.add_item(select)

        last_hour_button = discord.ui.Button(
            label="Last Hour Changes",
            emoji=f"{theme.timeIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_hour_nick",
            row=1
        )
        last_hour_button.callback = self.last_hour_callback
        self.add_item(last_hour_button)

        last_day_button = discord.ui.Button(
            label="Last 24h Changes",
            emoji=f"{theme.calendarIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="last_day_nick",
            row=1
        )
        last_day_button.callback = self.last_day_callback
        self.add_item(last_day_button)

        custom_time_button = discord.ui.Button(
            label="Custom Time",
            emoji=f"{theme.settingsIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="custom_time_nick",
            row=1
        )
        custom_time_button.callback = self.custom_time_callback
        self.add_item(custom_time_button)

        if self.total_pages > 1:
            previous_button = discord.ui.Button(
                label="Previous",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="previous_nick",
                disabled=self.current_page == 0,
                row=2
            )
            previous_button.callback = self.previous_callback
            self.add_item(previous_button)

            next_button = discord.ui.Button(
                label="Next",
                emoji=f"{theme.forwardIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="next_nick",
                disabled=self.current_page == self.total_pages - 1,
                row=2
            )
            next_button.callback = self.next_callback
            self.add_item(next_button)

        search_button = discord.ui.Button(
            label="Search by ID",
            emoji=f"{theme.searchIcon}",
            style=discord.ButtonStyle.primary,
            custom_id="search_fid_nick",
            row=2
        )
        search_button.callback = self.search_callback
        self.add_item(search_button)

    async def last_hour_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, hours=1)
        except Exception as e:
            print(f"Error in last_hour_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def last_day_callback(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, hours=24)
        except Exception as e:
            print(f"Error in last_day_callback: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while showing recent changes.",
                    ephemeral=True
                )

    async def custom_time_callback(self, interaction: discord.Interaction):
        try:
            modal = CustomTimeModalNickname(self.cog, self.alliance_name)
            await interaction.response.send_modal(modal)
        except Exception as e:
            print(f"Error in custom_time_callback: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} An error occurred while showing the time input.",
                ephemeral=True
            )

    async def previous_callback(self, interaction: discord.Interaction):
        self.current_page = max(0, self.current_page - 1)
        await self.update_page(interaction)

    async def next_callback(self, interaction: discord.Interaction):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await self.update_page(interaction)

    async def search_callback(self, interaction: discord.Interaction):
        modal = NicknameHistoryIDSearchModal(self.cog)
        await interaction.response.send_modal(modal)

    async def update_page(self, interaction: discord.Interaction):
        self.update_view()
        
        embed = discord.Embed(
            title=f"{theme.editListIcon} {self.alliance_name} - Member List",
            description=(
                f"Select a member to view nickname history:\n"
                f"{theme.upperDivider}\n"
                f"Total Members: {len(self.members)}\n"
                f"Current Page: {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        await interaction.response.edit_message(embed=embed, view=self)

class NicknameHistoryIDSearchModal(discord.ui.Modal, title="Search by ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        self.fid = discord.ui.TextInput(
            label="ID",
            placeholder="Enter ID number...",
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            fid = int(self.fid.value)
            await interaction.response.defer()
            await self.cog.show_nickname_history(interaction, fid)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Invalid ID format. Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in NicknameHistoryIDSearchModal on_submit: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred while searching for the player.",
                    ephemeral=True
                )

class CustomTimeModal(discord.ui.Modal, title="Custom Time Range"):
    def __init__(self, cog, alliance_name):
        super().__init__()
        self.cog = cog
        self.alliance_name = alliance_name
        self.hours = discord.ui.TextInput(
            label="Hours (1-24)",
            placeholder="Enter number of hours (max 24)...",
            required=True,
            min_length=1,
            max_length=2
        )
        self.add_item(self.hours)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hours = int(self.hours.value)
            if hours < 1 or hours > 24:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a number between 1 and 24.",
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await self.cog.show_recent_changes(interaction, self.alliance_name, hours)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in CustomTimeModal on_submit: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing your request.",
                ephemeral=True
            )

class RecentChangesView(discord.ui.View):
    def __init__(self, chunks, members, level_mapping, alliance_name, hours):
        super().__init__()
        self.chunks = chunks
        self.members = members
        self.level_mapping = level_mapping
        self.alliance_name = alliance_name
        self.hours = hours
        self.current_page = 0
        self.total_pages = len(chunks)
        
        self.update_buttons()

    def get_embed(self):
        embed = discord.Embed(
            title=f"{theme.levelIcon} Recent Level Changes - {self.alliance_name}",
            description=(
                f"Showing changes in the last {self.hours} hour(s)\n"
                f"{theme.upperDivider}\n"
                f"Total Changes: {sum(len(chunk) for chunk in self.chunks)}\n"
                f"Page {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1
        )

        for fid, old_value, new_value, timestamp in self.chunks[self.current_page]:
            old_level = self.level_mapping.get(int(old_value), str(old_value))
            new_level = self.level_mapping.get(int(new_value), str(new_value))
            embed.add_field(
                name=f"{self.members[fid]} (ID: {fid})",
                value=f"{theme.stoveOldIcon} `{old_level}` ➜ {theme.stoveIcon} `{new_level}`\n{theme.timeIcon} {timestamp}",
                inline=False
            )

        if self.total_pages > 1:
            embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")

        return embed

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == self.total_pages - 1

    @discord.ui.button(label="Previous", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary, custom_id="previous")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Next", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

class RecentNicknameChangesView(discord.ui.View):
    def __init__(self, chunks, members, alliance_name, hours):
        super().__init__()
        self.chunks = chunks
        self.members = members
        self.alliance_name = alliance_name
        self.hours = hours
        self.current_page = 0
        self.total_pages = len(chunks)
        
        self.update_buttons()

    def get_embed(self):
        embed = discord.Embed(
            title=f"{theme.editListIcon} Recent Nickname Changes - {self.alliance_name}",
            description=(
                f"Showing changes in the last {self.hours} hour(s)\n"
                f"{theme.upperDivider}\n"
                f"Total Changes: {sum(len(chunk) for chunk in self.chunks)}\n"
                f"Page {self.current_page + 1}/{self.total_pages}\n"
                f"{theme.lowerDivider}\n"
            ),
            color=theme.emColor1
        )

        for fid, old_name, new_name, timestamp in self.chunks[self.current_page]:
            embed.add_field(
                name=f"{self.members[fid]} (ID: {fid})",
                value=f"{theme.avatarOldIcon} `{old_name}` ➜ {theme.avatarIcon} `{new_name}`\n{theme.timeIcon} {timestamp}",
                inline=False
            )

        if self.total_pages > 1:
            embed.set_footer(text=f"Page {self.current_page + 1} of {self.total_pages}")

        return embed

    def update_buttons(self):
        self.previous_button.disabled = self.current_page == 0
        self.next_button.disabled = self.current_page == self.total_pages - 1

    @discord.ui.button(label="Previous", emoji=f"{theme.prevIcon}", style=discord.ButtonStyle.secondary, custom_id="previous_nick_recent")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

    @discord.ui.button(label="Next", emoji=f"{theme.nextIcon}", style=discord.ButtonStyle.secondary, custom_id="next_nick_recent")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.get_embed(), view=self)

class CustomTimeModalNickname(discord.ui.Modal, title="Custom Time Range"):
    def __init__(self, cog, alliance_name):
        super().__init__()
        self.cog = cog
        self.alliance_name = alliance_name
        self.hours = discord.ui.TextInput(
            label="Hours (1-24)",
            placeholder="Enter number of hours (max 24)...",
            required=True,
            min_length=1,
            max_length=2
        )
        self.add_item(self.hours)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            hours = int(self.hours.value)
            if hours < 1 or hours > 24:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Please enter a number between 1 and 24.",
                    ephemeral=True
                )
                return
            
            await interaction.response.defer()
            await self.cog.show_recent_nickname_changes(interaction, self.alliance_name, hours)
                
        except ValueError:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please enter a valid number.",
                ephemeral=True
            )
        except Exception as e:
            print(f"Error in CustomTimeModalNickname on_submit: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing your request.",
                ephemeral=True
            )

async def setup(bot):
    await bot.add_cog(Changes(bot)) 