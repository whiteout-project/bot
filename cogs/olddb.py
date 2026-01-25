import discord
from discord.ext import commands
from discord import app_commands, ui
import sqlite3
import os
from .pimp_my_bot import theme

class AllianceSelect(ui.Select):
    def __init__(self, alliances):
        options = [
            discord.SelectOption(label=name, value=str(alliance_id))
            for alliance_id, name in alliances
        ]
        super().__init__(placeholder="Select Alliance", options=options)

class AllianceView(ui.View):
    def __init__(self, alliances):
        super().__init__()
        self.add_item(AllianceSelect(alliances))

class DatabaseVersionSelect(ui.View):
    def __init__(self):
        super().__init__()

    @discord.ui.button(label="V2 Database", style=discord.ButtonStyle.primary)
    async def v2_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self.bot.get_cog('DatabaseTransfer')
        await interaction.response.defer(ephemeral=True)
        await cog.transfer_v2_database(interaction)

    @discord.ui.button(label="V3 Database", style=discord.ButtonStyle.primary)
    async def v3_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        cog = self.bot.get_cog('DatabaseTransfer')
        await interaction.response.defer(ephemeral=True)
        await cog.olddatabase(interaction)

class DatabaseTransfer(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    async def transfer_old_database(self, interaction: discord.Interaction):
        warning_embed = discord.Embed(
            title="Warning",
            description="Please do not mix V2 and V3 databases!\nMake sure to place the database you want to transfer in the same folder as main.py and ensure its name is gift_db.sqlite.",
            color=discord.Color.yellow()
        )
        view = DatabaseVersionSelect()
        view.bot = self.bot
        await interaction.response.send_message(embeds=[warning_embed], view=view, ephemeral=True)

    async def check_alliances(self):
        conn = sqlite3.connect('db/alliance.sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT alliance_id, name FROM alliance_list")
        alliances = cursor.fetchall()
        conn.close()
        return alliances

    async def olddatabase(self, interaction: discord.Interaction):
        embed = discord.Embed(title="Database Transfer", color=discord.Color.orange())

        if not os.path.exists('gift_db.sqlite'):
            embed.add_field(name="Status", value="gift_db.sqlite not found.", inline=False)
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed.add_field(name="Status", value="Database transfer in progress...", inline=False)
        message = await interaction.followup.send(embed=embed, ephemeral=True)

        transfer_steps = [
            ("gift_db.sqlite", "admin", "settings.sqlite"),
            ("gift_db.sqlite", "alliance_channels", "alliance.sqlite"),
            ("gift_db.sqlite", "alliance_intervals", "alliance.sqlite"),
            ("gift_db.sqlite", "alliance_list", "alliance.sqlite"),
            ("gift_db.sqlite", "botsettings", "settings.sqlite"),
            ("gift_db.sqlite", "furnace_changes", "changes.sqlite"),
            ("gift_db.sqlite", "nickname_changes", "changes.sqlite"),
            ("gift_db.sqlite", "gift_codes", "giftcode.sqlite"),
            ("gift_db.sqlite", "user_giftcodes", "giftcode.sqlite"),
            ("gift_db.sqlite", "users", "users.sqlite")
        ]

        db_connections = {}
        
        for source, table, destination in transfer_steps:
            destination_path = f'db/{destination}'
            
            if source not in db_connections:
                db_connections[source] = sqlite3.connect(source)
            if destination_path not in db_connections:
                db_connections[destination_path] = sqlite3.connect(destination_path)

            try:
                source_conn = db_connections[source]
                destination_conn = db_connections[destination_path]
                
                source_cursor = source_conn.cursor()
                destination_cursor = destination_conn.cursor()
                
                source_cursor.execute(f"SELECT * FROM {table}")
                rows = source_cursor.fetchall()
                row_count = len(rows)

                if table == "admin":
                    for row in rows:
                        destination_cursor.execute("INSERT OR REPLACE INTO admin (id, is_initial) VALUES (?, ?)", row)
                elif table == "alliance_channels":
                    destination_cursor.executemany("INSERT OR REPLACE INTO alliancesettings (alliance_id, channel_id) VALUES (?, ?)", rows)
                elif table == "alliance_intervals":
                    for alliance_id, interval in rows:
                        destination_cursor.execute("UPDATE alliancesettings SET interval = ? WHERE alliance_id = ?", (interval, alliance_id))
                elif table == "alliance_list":
                    destination_cursor.executemany("INSERT OR REPLACE INTO alliance_list (alliance_id, name) VALUES (?, ?)", rows)
                elif table == "botsettings":
                    for row in rows:
                        destination_cursor.execute("INSERT OR REPLACE INTO botsettings (id, channelid) VALUES (?, ?)", (row[0], row[1]))
                elif table == "furnace_changes":
                    destination_cursor.executemany("INSERT OR REPLACE INTO furnace_changes (id, fid, old_furnace_lv, new_furnace_lv, change_date) VALUES (?, ?, ?, ?, ?)", rows)
                elif table == "nickname_changes":
                    destination_cursor.executemany("INSERT OR REPLACE INTO nickname_changes (id, fid, old_nickname, new_nickname, change_date) VALUES (?, ?, ?, ?, ?)", rows)
                elif table == "gift_codes":
                    destination_cursor.executemany("INSERT OR REPLACE INTO gift_codes (giftcode, date) VALUES (?, ?)", rows)
                elif table == "user_giftcodes":
                    destination_cursor.executemany("INSERT OR REPLACE INTO user_giftcodes (fid, giftcode, status) VALUES (?, ?, ?)", rows)
                elif table == "users":
                    reorganized_rows = []
                    for row in rows:
                        reorganized_row = (row[0], row[1], row[2], row[4], row[5], row[3])
                        reorganized_rows.append(reorganized_row)
                    destination_cursor.executemany("INSERT OR REPLACE INTO users (fid, nickname, furnace_lv, kid, stove_lv_content, alliance) VALUES (?, ?, ?, ?, ?, ?)", reorganized_rows)

                embed.add_field(name=f"Step {table}", value=f"Transferred {row_count} rows ✔", inline=False)
                await message.edit(embed=embed)
                destination_conn.commit()

            except Exception as e:
                embed.add_field(name=f"Error at {table}", value=f"{str(e)}", inline=False)
                await message.edit(embed=embed)
            
        for conn in db_connections.values():
            conn.close()

        embed.color = discord.Color.green()
        embed.add_field(name="Status", value="All database transfers completed successfully!", inline=False)
        await message.edit(embed=embed)

    async def transfer_v2_data(self, interaction: discord.Interaction, alliance_id: int):
        embed = discord.Embed(title="Database Transfer (V2)", color=discord.Color.orange())
        message = await interaction.followup.send(embed=embed, ephemeral=True)

        transfer_steps = [
            ("gift_db.sqlite", "furnace_changes", "changes.sqlite"),
            ("gift_db.sqlite", "nickname_changes", "changes.sqlite"),
            ("gift_db.sqlite", "gift_codes", "giftcode.sqlite"),
            ("gift_db.sqlite", "user_giftcodes", "giftcode.sqlite"),
            ("gift_db.sqlite", "users", "users.sqlite")
        ]

        db_connections = {}
        
        for source, table, destination in transfer_steps:
            destination_path = f'db/{destination}'
            
            if source not in db_connections:
                db_connections[source] = sqlite3.connect(source)
            if destination_path not in db_connections:
                db_connections[destination_path] = sqlite3.connect(destination_path)

            try:
                source_conn = db_connections[source]
                destination_conn = db_connections[destination_path]
                
                source_cursor = source_conn.cursor()
                destination_cursor = destination_conn.cursor()
                
                source_cursor.execute(f"SELECT * FROM {table}")
                rows = source_cursor.fetchall()
                row_count = len(rows)

                if table == "users":
                    for row in rows:
                        fid, nickname, furnace_lv = row
                        destination_cursor.execute(
                            "INSERT OR REPLACE INTO users (fid, nickname, furnace_lv, alliance) VALUES (?, ?, ?, ?)",
                            (fid, nickname, furnace_lv, alliance_id)
                        )
                else:
                    if table == "furnace_changes":
                        destination_cursor.executemany("INSERT OR REPLACE INTO furnace_changes (id, fid, old_furnace_lv, new_furnace_lv, change_date) VALUES (?, ?, ?, ?, ?)", rows)
                    elif table == "nickname_changes":
                        destination_cursor.executemany("INSERT OR REPLACE INTO nickname_changes (id, fid, old_nickname, new_nickname, change_date) VALUES (?, ?, ?, ?, ?)", rows)
                    elif table == "gift_codes":
                        destination_cursor.executemany("INSERT OR REPLACE INTO gift_codes (giftcode, date) VALUES (?, ?)", rows)
                    elif table == "user_giftcodes":
                        destination_cursor.executemany("INSERT OR REPLACE INTO user_giftcodes (fid, giftcode, status) VALUES (?, ?, ?)", rows)

                embed.add_field(name=f"Step {table}", value=f"Transferred {row_count} rows ✔", inline=False)
                await message.edit(embed=embed)
                destination_conn.commit()

            except Exception as e:
                embed.add_field(name=f"Error at {table}", value=f"{str(e)}", inline=False)
                await message.edit(embed=embed)
            
        for conn in db_connections.values():
            conn.close()

        embed.color = discord.Color.green()
        embed.add_field(name="Status", value="All database transfers completed successfully!", inline=False)
        await message.edit(embed=embed)

    async def transfer_v2_database(self, interaction: discord.Interaction):
        alliances = await self.check_alliances()
        
        if not alliances:
            embed = discord.Embed(
                title="Database Transfer (V2)",
                description="Please create an alliance before transferring the database!",
                color=discord.Color.red()
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return

        embed = discord.Embed(
            title="Database Transfer (V2)",
            description="Please select the alliance to transfer users to:",
            color=theme.emColor1
        )
        view = AllianceView(alliances)
        
        async def alliance_callback(interaction: discord.Interaction):
            await interaction.response.defer(ephemeral=True)
            alliance_id = int(view.children[0].values[0])
            await self.transfer_v2_data(interaction, alliance_id)
        
        view.children[0].callback = alliance_callback
        await interaction.followup.send(embed=embed, view=view, ephemeral=True)

async def setup(bot):
    await bot.add_cog(DatabaseTransfer(bot))
