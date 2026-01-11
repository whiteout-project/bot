import discord
from discord.ext import commands, tasks
import sqlite3
import os
import zipfile
import datetime
import tempfile
import pyzipper
import shutil
import traceback
from .permission_handler import PermissionManager
from .pimp_my_bot import theme

class BackupOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = "db/backup.sqlite"
        self.backup_dir = "backups"
        self.log_path = "log/backuplog.txt"
        os.makedirs("log", exist_ok=True)
        os.makedirs(self.backup_dir, exist_ok=True)
        self.setup_database()
        self.automatic_backup_loop.start()

    def setup_database(self):
        os.makedirs("db", exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS backup_passwords (
                discord_id TEXT PRIMARY KEY,
                backup_password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()

    def cog_unload(self):
        self.automatic_backup_loop.cancel()

    def get_disk_space_info(self):
        """Get disk space information in MB"""
        try:
            # Get disk usage for the current directory
            total, used, free = shutil.disk_usage(".")
            return {
                'total_mb': total / (1024 * 1024),
                'used_mb': used / (1024 * 1024),
                'free_mb': free / (1024 * 1024)
            }
        except Exception as e:
            print(f"Error getting disk space: {e}")
            return None

    def estimate_backup_size(self):
        """Estimate the size of a backup in MB"""
        try:
            total_size = 0
            for file in os.listdir("db"):
                if file.endswith(".sqlite"):
                    file_path = os.path.join("db", file)
                    total_size += os.path.getsize(file_path)
            
            estimated_compressed = total_size * 1.2 # 20% overhead for compression and packaging
            return estimated_compressed / (1024 * 1024)
        except Exception as e:
            print(f"Error estimating backup size: {e}")
            return 50  # Conservative default of 50MB

    def can_create_backup(self, save_locally=True):
        """Check if we have enough space to create a backup"""
        space_info = self.get_disk_space_info()
        if not space_info:
            return False, "Cannot determine disk space"
        
        estimated_size = self.estimate_backup_size()
        
        if save_locally:
            required_space = estimated_size + 50  # 50MB buffer for local saves
        else:
            required_space = estimated_size + 10  # 10MB buffer for DM sends (backup deleted after send)
        
        if space_info['free_mb'] < required_space:
            return False, f"Insufficient disk space. Need {required_space:.1f}MB, have {space_info['free_mb']:.1f}MB"
        
        if not save_locally and estimated_size > 24: # Check if backup would exceed Discord's 25MB limit for DM
            return False, f"Backup too large for Discord ({estimated_size:.1f}MB > 24MB limit)"
        
        return True, "OK"

    def log_backup(self, admin_id: str, success: bool, backup_type: str, method: str, filename: str = None, error_message: str = None):
        try:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_message = f"[{timestamp}] "
            log_message += f"Type: {backup_type} | Method: {method} | "
            log_message += f"Admin ID: {admin_id} | "
            log_message += f"Status: {theme.verifiedIcon + ' Success' if success else theme.deniedIcon + ' Failed'}"
            if filename:
                log_message += f" | File: {filename}"
            if error_message:
                log_message += f" | Error: {error_message}"
            log_message += "\n"
            log_message += f"{theme.upperDivider}\n"

            with open(self.log_path, 'a', encoding='utf-8') as log_file:
                log_file.write(log_message)
        except Exception as e:
            print(f"Logging error: {e}")

    @tasks.loop(hours=3)
    async def automatic_backup_loop(self):
        try:
            conn = sqlite3.connect("db/settings.sqlite")
            cursor = conn.cursor()
            cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
            global_admins = cursor.fetchall()
            conn.close()

            # For automatic backups, always save locally to avoid spamming DMs
            can_backup, reason = self.can_create_backup(save_locally=True)
            if not can_backup:
                print(f"Automatic backup skipped: {reason}")
                for admin_id in global_admins:
                    self.log_backup(str(admin_id[0]), False, "Automatic Backup", "Local", None, reason)
                return

            for admin_id in global_admins:
                admin_id = admin_id[0]
                try:
                    filename = await self.create_backup(str(admin_id), "Automatic", save_locally=True)
                    if filename:
                        self.log_backup(str(admin_id), True, "Automatic Backup", "Local", filename)
                        await self.cleanup_old_backups("automatic", keep=2) # Clean old automatic backups (keep last 2)
                    else:
                        self.log_backup(str(admin_id), False, "Automatic Backup", "Local", None, "Backup creation failed")
                except Exception as e:
                    self.log_backup(str(admin_id), False, "Automatic Backup", "Local", None, str(e))

        except Exception as e:
            print(f"Automatic backup error: {e}")

    @automatic_backup_loop.before_loop
    async def before_automatic_backup(self):
        await self.bot.wait_until_ready()

    async def show_backup_menu(self, interaction: discord.Interaction):
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_admin or not is_global:
            await interaction.response.send_message(f"{theme.deniedIcon} This menu is only available for Global Admins!", ephemeral=True)
            return

        # Get system info
        space_info = self.get_disk_space_info()
        estimated_backup_size = self.estimate_backup_size()
        backup_files = self.get_backup_files()
        
        embed = discord.Embed(
            title=f"{theme.saveIcon} Backup System",
            description=(
                f"**System Status**\n"
                f"{theme.upperDivider}\n"
                f"{theme.saveIcon} **Free Space:** {space_info['free_mb']:.1f} MB\n" if space_info else f"{theme.saveIcon} **Free Space:** Unknown\n"
                f"{theme.chartIcon} **Estimated Backup Size:** {estimated_backup_size:.1f} MB\n"
                f"{theme.documentIcon} **Local Backups:** {len(backup_files)} files\n"
                f"{theme.alarmClockIcon} **Auto Backup:** Every 3 hours (local)\n"
                f"{theme.lowerDivider}\n\n"
                f"**Operations:**\n"
                f"• Set backup password\n"
                f"• Create manual backup\n"
                f"• View/manage local backups\n"
                f"• Clean old backups"
            ),
            color=theme.emColor1
        )

        if space_info and space_info['free_mb'] < 100: # Warning if space is low
            embed.add_field(
                name=f"{theme.warnIcon} Low Disk Space Warning",
                value=f"Only {space_info['free_mb']:.1f} MB free. Consider cleaning old backups.",
                inline=False
            )

        view = BackupView(self)
        await interaction.response.edit_message(embed=embed, view=view)

    def get_backup_files(self):
        """Get list of all local backup files"""
        backup_files = []
        try:
            for file in os.listdir(self.backup_dir):
                if file.endswith('.zip'):
                    backup_files.append(os.path.join(self.backup_dir, file))
        except:
            pass
        return sorted(backup_files, key=os.path.getmtime, reverse=True)

    async def create_backup(self, user_id: str, backup_type: str = "Manual", save_locally: bool = True):
        try:
            # Get password
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT backup_password FROM backup_passwords WHERE discord_id = ?", (user_id,))
            password_result = cursor.fetchone()
            conn.close()

            backup_password = password_result[0] if password_result else None

            timestamp = datetime.datetime.now()
            backup_name = f"{backup_type.lower()}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
            
            if save_locally:
                # Save to local backups folder
                if backup_password:
                    filename = f"{backup_name}_encrypted.zip"
                    filepath = os.path.join(self.backup_dir, filename)
                    
                    with pyzipper.AESZipFile(filepath, 'w', compression=pyzipper.ZIP_LZMA, encryption=pyzipper.WZ_AES) as zf:
                        zf.setpassword(backup_password.encode())
                        
                        for file in os.listdir("db"):
                            if file.endswith(".sqlite"):
                                file_path = os.path.join("db", file)
                                zf.write(file_path, file)
                        
                        readme_content = f"""Encrypted Local Backup
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Created: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}
User ID: {user_id}
Type: {backup_type}
Contains: All SQLite database files
Encryption: AES (Password Protected)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

To restore:
1. Extract this ZIP file using your backup password
2. Replace your db/ folder contents with these files
3. Restart the bot

🤖 WOS Discord Bot Backup System
"""
                        zf.writestr("README.txt", readme_content)
                else:
                    filename = f"{backup_name}.zip"
                    filepath = os.path.join(self.backup_dir, filename)
                    
                    with zipfile.ZipFile(filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                        for file in os.listdir("db"):
                            if file.endswith(".sqlite"):
                                file_path = os.path.join("db", file)
                                zf.write(file_path, file)
                        
                        readme_content = f"""Local Backup
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Created: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}
User ID: {user_id}
Type: {backup_type}
Contains: All SQLite database files
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

To restore:
1. Extract this ZIP file
2. Replace your db/ folder contents with these files
3. Restart the bot

🤖 WOS Discord Bot Backup System
"""
                        zf.writestr("README.txt", readme_content)
                
                return filename
            
            else:
                # Send via DM - create temporary file
                with tempfile.TemporaryDirectory() as temp_dir:
                    if backup_password:
                        filename = f"{backup_name}_encrypted.zip"
                        temp_filepath = os.path.join(temp_dir, filename)
                        
                        with pyzipper.AESZipFile(temp_filepath, 'w', compression=pyzipper.ZIP_LZMA, encryption=pyzipper.WZ_AES) as zf:
                            zf.setpassword(backup_password.encode())
                            
                            for file in os.listdir("db"):
                                if file.endswith(".sqlite"):
                                    file_path = os.path.join("db", file)
                                    zf.write(file_path, file)
                            
                            readme_content = f"""Discord Backup
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Created: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}
User ID: {user_id}
Type: {backup_type}
Contains: All SQLite database files
Encryption: AES (Password Protected)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

To restore:
1. Extract this ZIP file using your backup password
2. Replace your db/ folder contents with these files
3. Restart the bot

⚠️ This backup expires in 30 days from Discord

🤖 WOS Discord Bot Backup System
"""
                            zf.writestr("README.txt", readme_content)
                    else:
                        filename = f"{backup_name}.zip"
                        temp_filepath = os.path.join(temp_dir, filename)
                        
                        with zipfile.ZipFile(temp_filepath, 'w', zipfile.ZIP_DEFLATED) as zf:
                            for file in os.listdir("db"):
                                if file.endswith(".sqlite"):
                                    file_path = os.path.join("db", file)
                                    zf.write(file_path, file)
                            
                            readme_content = f"""Discord Backup
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Created: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}
User ID: {user_id}
Type: {backup_type}
Contains: All SQLite database files
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

To restore:
1. Extract this ZIP file
2. Replace your db/ folder contents with these files
3. Restart the bot

⚠️ This backup expires in 30 days from Discord

🤖 WOS Discord Bot Backup System
"""
                            zf.writestr("README.txt", readme_content)
                    
                    # Check file size before sending
                    file_size = os.path.getsize(temp_filepath)
                    if file_size > 24 * 1024 * 1024:
                        return None
                    
                    try: # Send to user via DM
                        user = await self.bot.fetch_user(int(user_id))
                        dm_channel = user.dm_channel or await user.create_dm()
                        
                        embed = discord.Embed(
                            title=f"{theme.saveIcon} Database Backup",
                            description=(
                                f"**Backup Details**\n"
                                f"{theme.upperDivider}\n"
                                f"{theme.calendarIcon} **Created:** {timestamp.strftime('%Y-%m-%d %H:%M:%S')}\n"
                                f"{theme.documentIcon} **Type:** {backup_type}\n"
                                f"{theme.shieldIcon} **Password Protected:** {'Yes' if backup_password else 'No'}\n"
                                f"{theme.chartIcon} **File Size:** {file_size / 1024 / 1024:.2f} MB\n"
                                f"{theme.lowerDivider}\n\n"
                                f"{theme.warnIcon} **Important:**\n"
                                f"• {'Use your backup password to open this file' if backup_password else 'This file is not password protected'}\n"
                                f"• Store this file in a secure location\n"
                                f"• This backup expires in 30 days from Discord"
                            ),
                            color=theme.emColor3
                        )

                        with open(temp_filepath, 'rb') as f:
                            file = discord.File(f, filename=filename)
                            await dm_channel.send(embed=embed, file=file)

                        return filename

                    except Exception as e:
                        print(f"Error sending backup via DM: {e}")
                        return None

        except Exception as e:
            print(f"Backup creation error: {e}")
            traceback.print_exc()
            return None

    async def cleanup_old_backups(self, backup_type: str, keep: int = 2):
        """Clean up old local backups, keeping only the most recent ones"""
        try:
            backup_files = []
            for file in os.listdir(self.backup_dir):
                if file.startswith(backup_type.lower()) and file.endswith('.zip'):
                    filepath = os.path.join(self.backup_dir, file)
                    backup_files.append((filepath, os.path.getmtime(filepath)))
            
            # Sort by modification time (newest first)
            backup_files.sort(key=lambda x: x[1], reverse=True)
            
            # Remove old files
            removed_count = 0
            for filepath, _ in backup_files[keep:]:
                try:
                    os.remove(filepath)
                    removed_count += 1
                except Exception as e:
                    print(f"Error removing {filepath}: {e}")
            
            return removed_count
        except Exception as e:
            print(f"Cleanup error: {e}")
            return 0

class BackupView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(label="Set Password", emoji=f"{theme.lockIcon}", style=discord.ButtonStyle.primary, row=0)
    async def set_password(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BackupPasswordModal(self.cog)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="Create Backup", emoji=f"{theme.saveIcon}", style=discord.ButtonStyle.success, row=0)
    async def create_backup(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Show choice between DM and local save
        embed = discord.Embed(
            title=f"{theme.saveIcon} Create Backup",
            description=(
                f"Choose how you want to receive your backup:\n\n"
                f"**{theme.messageIcon} Direct Message**\n"
                f"• Sent to your DMs immediately\n"
                f"• Limited to 24MB (Discord limit)\n"
                f"• Expires in 30 days\n\n"
                f"**{theme.saveIcon} Save Locally**\n"
                f"• Saved to server's backup folder\n"
                f"• No size limit (uses server storage)\n"
                f"• Permanent until manually deleted"
            ),
            color=theme.emColor1
        )
        
        view = BackupChoiceView(self.cog, interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="View Local Backups", emoji=f"{theme.listIcon}", style=discord.ButtonStyle.primary, row=0)
    async def view_backups(self, interaction: discord.Interaction, button: discord.ui.Button):
        backup_files = self.cog.get_backup_files()
        
        if not backup_files:
            await interaction.response.send_message(f"{theme.deniedIcon} No local backup files found!", ephemeral=True)
            return

        embed = discord.Embed(
            title=f"{theme.listIcon} Local Backup Files",
            color=theme.emColor1
        )

        total_size = 0
        for i, filepath in enumerate(backup_files[:10]): # Show last 10
            filename = os.path.basename(filepath)
            file_size = os.path.getsize(filepath)
            total_size += file_size
            file_size_mb = file_size / (1024 * 1024)
            mod_time = datetime.datetime.fromtimestamp(os.path.getmtime(filepath))

            embed.add_field(
                name=f"{theme.documentIcon} {filename}",
                value=f"{theme.chartIcon} {file_size_mb:.2f} MB\n{theme.alarmClockIcon} {mod_time.strftime('%Y-%m-%d %H:%M:%S')}",
                inline=True
            )

        embed.add_field(
            name=f"{theme.chartIcon} Summary",
            value=f"Total shown: {total_size / 1024 / 1024:.2f} MB\nFiles displayed: {min(len(backup_files), 10)} of {len(backup_files)}",
            inline=False
        )

        view = BackupManageView(self.cog)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="Main Menu", emoji=f"{theme.homeIcon}", style=discord.ButtonStyle.secondary, row=1)
    async def main_menu(self, interaction: discord.Interaction, button: discord.ui.Button):
        other_features_cog = self.cog.bot.get_cog("OtherFeatures")
        if other_features_cog:
            await other_features_cog.show_other_features_menu(interaction)

class BackupChoiceView(discord.ui.View):
    def __init__(self, cog, user_id):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id

    @discord.ui.button(label="Send to DM", emoji=f"{theme.messageIcon}", style=discord.ButtonStyle.primary)
    async def send_dm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} This is not your menu!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        
        # Check if we can create DM backup
        can_backup, reason = self.cog.can_create_backup(save_locally=False)
        if not can_backup:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Cannot Create DM Backup",
                description=reason,
                color=theme.emColor2
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        filename = await self.cog.create_backup(str(self.user_id), "Manual", save_locally=False)
        
        if filename:
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Backup Sent",
                description=f"Backup `{filename}` has been sent to your direct messages!",
                color=theme.emColor3
            )
            self.cog.log_backup(str(self.user_id), True, "Manual Backup", "DM", filename)
        else:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Backup Failed",
                description="Failed to create or send backup. Check file size and try local save instead.",
                color=theme.emColor2
            )
            self.cog.log_backup(str(self.user_id), False, "Manual Backup", "DM", None, "Creation/send failed")
        
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="Save Locally", emoji=f"{theme.saveIcon}", style=discord.ButtonStyle.success)
    async def save_local(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} This is not your menu!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        
        # Check if we can create local backup
        can_backup, reason = self.cog.can_create_backup(save_locally=True)
        if not can_backup:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Cannot Create Local Backup",
                description=reason,
                color=theme.emColor2
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
            return
        
        filename = await self.cog.create_backup(str(self.user_id), "Manual", save_locally=True)
        
        if filename:
            file_path = os.path.join(self.cog.backup_dir, filename)
            file_size = os.path.getsize(file_path) / (1024 * 1024)
            
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Local Backup Created",
                description=(
                    f"**Backup Details:**\n"
                    f"{theme.documentIcon} **File:** {filename}\n"
                    f"{theme.chartIcon} **Size:** {file_size:.2f} MB\n"
                    f"{theme.pinIcon} **Location:** `{os.path.abspath(file_path)}`\n\n"
                    f"Use 'View Local Backups' to manage your saved backups."
                ),
                color=theme.emColor3
            )
            self.cog.log_backup(str(self.user_id), True, "Manual Backup", "Local", filename)
        else:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Backup Failed",
                description="Failed to create local backup. Check disk space and try again.",
                color=theme.emColor2
            )
            self.cog.log_backup(str(self.user_id), False, "Manual Backup", "Local", None, "Creation failed")
        
        await interaction.followup.send(embed=embed, ephemeral=True)

class BackupManageView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=60)
        self.cog = cog

    @discord.ui.button(label="Clean Old Backups", emoji=f"{theme.cleanIcon}", style=discord.ButtonStyle.secondary)
    async def clean_backups(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        manual_removed = await self.cog.cleanup_old_backups("manual", keep=2)
        auto_removed = await self.cog.cleanup_old_backups("automatic", keep=2)
        
        space_info = self.cog.get_disk_space_info()
        
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Cleanup Complete",
            description=(
                f"**Files Removed:**\n"
                f"• Manual backups: {manual_removed}\n"
                f"• Automatic backups: {auto_removed}\n\n"
                f"**Retention Policy:**\n"
                f"• Kept last 2 manual backups\n"
                f"• Kept last 2 automatic backups\n\n"
                f"**Current Free Space:** {space_info['free_mb']:.1f} MB" if space_info else ""
            ),
            color=theme.emColor3
        )
        
        await interaction.followup.send(embed=embed, ephemeral=True)

class BackupPasswordModal(discord.ui.Modal, title="Set Backup Password"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog

    password = discord.ui.TextInput(
        label="Backup Password",
        placeholder="Enter a secure password (leave empty to remove password)...",
        min_length=0,
        max_length=50,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        password_value = self.password.value.strip()
        
        conn = sqlite3.connect(self.cog.db_path)
        cursor = conn.cursor()
        
        if password_value:
            cursor.execute(
                "INSERT OR REPLACE INTO backup_passwords (discord_id, backup_password) VALUES (?, ?)",
                (str(interaction.user.id), password_value)
            )
            message = "Your backup password has been saved successfully!"
            title = f"{theme.verifiedIcon} Password Set"
        else:
            cursor.execute("DELETE FROM backup_passwords WHERE discord_id = ?", (str(interaction.user.id),))
            message = "Your backup password has been removed. Future backups will not be encrypted."
            title = f"{theme.verifiedIcon} Password Removed"
        
        conn.commit()
        conn.close()

        embed = discord.Embed(
            title=title,
            description=message,
            color=theme.emColor3
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot):
    await bot.add_cog(BackupOperations(bot))