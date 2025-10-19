import discord
from discord.ext import commands, tasks
import sqlite3
import asyncio
from datetime import datetime
from colorama import Fore, Style
import os
import traceback
import logging
from logging.handlers import RotatingFileHandler
from .login_handler import LoginHandler

level_mapping = {
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

class Control(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn_alliance = sqlite3.connect('db/alliance.sqlite')
        self.conn_users = sqlite3.connect('db/users.sqlite')
        self.conn_changes = sqlite3.connect('db/changes.sqlite')
        
        # Setup logger for alliance control
        self.logger = logging.getLogger('alliance_control')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        
        # Clear existing handlers
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        
        # Create log directory if it doesn't exist
        os.makedirs('log', exist_ok=True)
        
        # Rotating file handler for alliance control logs
        # maxBytes = 1MB (1024 * 1024), backupCount = 1
        file_handler = RotatingFileHandler(
            'log/alliance_control.txt',
            maxBytes=1024*1024,  # 1MB
            backupCount=1,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        
        # Formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.cursor_alliance = self.conn_alliance.cursor()
        self.cursor_users = self.conn_users.cursor()
        self.cursor_changes = self.conn_changes.cursor()
        
        self.conn_settings = sqlite3.connect('db/settings.sqlite')
        self.cursor_settings = self.conn_settings.cursor()
        self.cursor_settings.execute("""
            CREATE TABLE IF NOT EXISTS auto (
                id INTEGER PRIMARY KEY,
                value INTEGER DEFAULT 1
            )
        """)
        
        self.cursor_settings.execute("SELECT COUNT(*) FROM auto")
        if self.cursor_settings.fetchone()[0] == 0:
            self.cursor_settings.execute("INSERT INTO auto (value) VALUES (1)")
        self.conn_settings.commit()
        
        # Add control settings columns to alliancesettings if they don't exist
        self.cursor_alliance.execute("PRAGMA table_info(alliancesettings)")
        columns = [col[1] for col in self.cursor_alliance.fetchall()]
        
        if 'auto_remove_on_transfer' not in columns:
            self.cursor_alliance.execute("""
                ALTER TABLE alliancesettings 
                ADD COLUMN auto_remove_on_transfer INTEGER DEFAULT 0
            """)
        
        if 'notify_on_transfer' not in columns:
            self.cursor_alliance.execute("""
                ALTER TABLE alliancesettings 
                ADD COLUMN notify_on_transfer INTEGER DEFAULT 0
            """)
        
        self.conn_alliance.commit()
        
        self.db_lock = asyncio.Lock()
        self.proxies = self.load_proxies()
        self.alliance_tasks = {}
        self.is_running = {}
        self.monitor_started = False
        
        # Initialize login handler for centralized queue management
        self.login_handler = LoginHandler()

    def load_proxies(self):
        proxies = []
        if os.path.exists('proxy.txt'):
            with open('proxy.txt', 'r') as f:
                proxies = [f"socks4://{line.strip()}" for line in f if line.strip()]
        return proxies
    
    def get_auto_remove_setting(self, alliance_id):
        """Get the auto_remove_on_transfer setting for a specific alliance"""
        self.cursor_alliance.execute("""
            SELECT auto_remove_on_transfer 
            FROM alliancesettings 
            WHERE alliance_id = ?
        """, (alliance_id,))
        result = self.cursor_alliance.fetchone()
        # Default to 0 (disabled) if not set
        return result[0] if result and result[0] is not None else 0
    
    def get_transfer_notification_setting(self, alliance_id):
        """Get the notify_on_transfer setting for a specific alliance"""
        self.cursor_alliance.execute("""
            SELECT notify_on_transfer 
            FROM alliancesettings 
            WHERE alliance_id = ?
        """, (alliance_id,))
        result = self.cursor_alliance.fetchone()
        # Default to 0 (disabled) if not set
        return result[0] if result and result[0] is not None else 0

    async def fetch_user_data(self, fid, proxy=None):
        """Fetch user data using the centralized login handler"""
        result = await self.login_handler.fetch_player_data(fid, use_proxy=proxy)
        
        if result['status'] == 'success':
            # Return in the old format for compatibility
            return {'data': result['data']}
        elif result['status'] == 'rate_limited':
            return 429
        elif result['status'] == 'not_found':
            return {'error': 'not_found', 'fid': fid}
        else:
            return {'error': result.get('error_message', 'Unknown error'), 'fid': fid}

    async def remove_invalid_fid(self, fid: str, reason: str):
        """Safely remove an invalid ID from the database with logging"""
        try:
            async with self.db_lock:
                # Get user info before deletion for logging
                self.cursor_users.execute("SELECT nickname, alliance FROM users WHERE fid = ?", (fid,))
                user_info = self.cursor_users.fetchone()
                
                if user_info:
                    nickname, alliance_id = user_info
                    
                    # Delete from users table
                    self.cursor_users.execute("DELETE FROM users WHERE fid = ?", (fid,))
                    self.conn_users.commit()
                    
                    # Log the deletion to alliance control log
                    self.logger.warning(f"[AUTO-CLEANUP] Removed invalid ID {fid} (nickname: {nickname}) - Reason: {reason}")
                    
                    return True, nickname
        except Exception as e:
            self.logger.error(f"Failed to remove invalid ID {fid}: {str(e)}")
            return False, None

    async def check_agslist(self, channel, alliance_id, interaction=None, interaction_message=None, alliance_name=None, is_batch=False, batch_info=None):
        async with self.db_lock:
            self.cursor_users.execute("SELECT fid, nickname, furnace_lv, stove_lv_content, kid FROM users WHERE alliance = ?", (alliance_id,))
            users = self.cursor_users.fetchall()

            if not users:
                return

        total_users = len(users)
        checked_users = 0

        self.cursor_alliance.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
        alliance_name_from_db = self.cursor_alliance.fetchone()[0]
        # Use provided name if available, otherwise use from database
        if not alliance_name:
            alliance_name = alliance_name_from_db

        start_time = datetime.now()
        self.logger.info(f"{alliance_name} Alliance Control started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Update ephemeral message at start if provided
        if interaction_message:
            try:
                if is_batch and batch_info:
                    # For batch processing (all alliances)
                    status_embed = discord.Embed(
                        title="🔄 Alliance Control Operation",
                        description=(
                            "━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📊 **Type:** All Alliances ({batch_info['total']} total)\n"
                            f"🏰 **Currently Processing:** {alliance_name}\n"
                            f"📍 **Progress:** {batch_info['current']}/{batch_info['total']} alliances\n"
                            f"⏰ **Started:** <t:{int(start_time.timestamp())}:R>\n"
                            "━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.blue()
                    )
                else:
                    # For single alliance processing
                    status_embed = discord.Embed(
                        title="🔄 Alliance Control Operation",
                        description=(
                            "━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📊 **Type:** Single Alliance\n"
                            f"🏰 **Alliance:** {alliance_name}\n"
                            f"📍 **Status:** In Progress\n"
                            f"⏰ **Started:** <t:{int(start_time.timestamp())}:R>\n"
                            f"📢 **Results Channel:** {channel.mention}\n"
                            "━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.blue()
                    )
                await interaction_message.edit(embed=status_embed)
            except Exception as e:
                self.logger.warning(f"Could not update interaction message at start: {e}")
        
        async with self.db_lock:
            with sqlite3.connect('db/settings.sqlite') as settings_db:
                cursor = settings_db.cursor()
                cursor.execute("SELECT value FROM auto LIMIT 1")
                result = cursor.fetchone()
                auto_value = result[0] if result else 1
        
        embed = discord.Embed(
            title=f"🏰 {alliance_name} Alliance Control",
            description="🔍 Checking for changes in member status...",
            color=discord.Color.blue()
        )
        embed.add_field(
            name="📊 Status",
            value=f"⏳ Control started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            inline=False
        )
        embed.add_field(
            name="📈 Progress",
            value=f"✨ Members checked: {checked_users}/{total_users}",
            inline=False
        )
        embed.set_footer(text="⚡ Automatic Alliance Control System")
        
        message = None
        if auto_value == 1:
            message = await channel.send(embed=embed)

        furnace_changes, nickname_changes, kid_changes, check_fail_list = [], [], [], []

        def safe_list(input_list): # Avoid issues with list indexing
            if not isinstance(input_list, list):
                return []
            return [str(item) for item in input_list if item]

        i = 0
        while i < total_users:
            batch_users = users[i:i+20]
            for fid, old_nickname, old_furnace_lv, old_stove_lv_content, old_kid in batch_users:
                data = await self.fetch_user_data(fid)
                
                if data == 429:
                    # Get wait time from login handler
                    wait_time = self.login_handler._get_wait_time()
                    
                    embed.description = f"⚠️ API Rate Limit! Waiting {wait_time:.1f} seconds...\n📊 Progress: {checked_users}/{total_users} members"
                    embed.color = discord.Color.orange()
                    if message:
                        await message.edit(embed=embed)
                    
                    await asyncio.sleep(wait_time)
                    
                    embed.description = "🔍 Checking for changes in member status..."
                    embed.color = discord.Color.blue()
                    if message:
                        await message.edit(embed=embed)
                    data = await self.fetch_user_data(fid)
                
                if isinstance(data, dict):
                    if 'error' in data:
                        # Handle error responses (including 40004)
                        error_msg = data.get('error', 'Unknown error')
                        
                        # Check if this is a permanently invalid ID (not found)
                        if error_msg == 'not_found':
                            # Auto-remove the invalid ID
                            removed, old_nickname = await self.remove_invalid_fid(fid, "Player does not exist (error 40004)")
                            if removed:
                                check_fail_list.append(f"❌ `{fid}` ({old_nickname}) - Player not found (Auto-removed)")
                            else:
                                check_fail_list.append(f"❌ `{fid}` - Player not found (Failed to remove)")
                        else:
                            # For other errors, just report without removing
                            check_fail_list.append(f"❌ `{fid}` - {error_msg}")
                            self.logger.warning(f"Failed to check ID {fid}: {error_msg}")
                        
                        checked_users += 1
                    elif 'data' in data:
                        # Process successful response
                        user_data = data['data']
                        new_furnace_lv = user_data['stove_lv']
                        new_nickname = user_data['nickname'].strip()
                        new_kid = user_data.get('kid', 0)
                        new_stove_lv_content = user_data['stove_lv_content']
                        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        async with self.db_lock:
                            if new_stove_lv_content != old_stove_lv_content:
                                self.cursor_users.execute("UPDATE users SET stove_lv_content = ? WHERE fid = ?", (new_stove_lv_content, fid))
                                self.conn_users.commit()

                            if old_kid != new_kid:
                                kid_changes.append(f"👤 {old_nickname} has transferred to a new state\n🔄 Old State: {old_kid}\n🆕 New State: {new_kid}")
                                
                                # Check if auto-removal is enabled for this alliance
                                auto_remove = self.get_auto_remove_setting(alliance_id)
                                notify_on_transfer = self.get_transfer_notification_setting(alliance_id)
                                
                                if auto_remove:
                                    # Remove user from alliance when auto-removal is enabled
                                    self.cursor_users.execute("DELETE FROM users WHERE fid = ?", (fid,))
                                    self.conn_users.commit()
                                    
                                    # Only notify if notifications are enabled for auto-removal
                                    if notify_on_transfer:
                                        self.cursor_settings.execute("SELECT id FROM admin WHERE is_initial = 1")
                                        admin_data = self.cursor_settings.fetchone()
                                        
                                        if admin_data:
                                            user = await self.bot.fetch_user(admin_data[0])
                                            if user:
                                                await user.send(f"❌ {old_nickname} ({fid}) was removed from the users table due to state transfer.")
                                else:
                                    # Just update kid without removing (default behavior)
                                    self.cursor_users.execute("UPDATE users SET kid = ? WHERE fid = ?", (new_kid, fid))
                                    self.conn_users.commit()

                            if new_furnace_lv != old_furnace_lv:
                                new_furnace_display = level_mapping.get(new_furnace_lv, new_furnace_lv)
                                old_furnace_display = level_mapping.get(old_furnace_lv, old_furnace_lv)
                                self.cursor_changes.execute("INSERT INTO furnace_changes (fid, old_furnace_lv, new_furnace_lv, change_date) VALUES (?, ?, ?, ?)",
                                                             (fid, old_furnace_lv, new_furnace_lv, current_time))
                                self.conn_changes.commit()
                                self.cursor_users.execute("UPDATE users SET furnace_lv = ? WHERE fid = ?", (new_furnace_lv, fid))
                                self.conn_users.commit()
                                furnace_changes.append(f"👤 **{old_nickname}**\n🔥 `{old_furnace_display}` ➡️ `{new_furnace_display}`")

                            if new_nickname.lower() != old_nickname.lower().strip():
                                self.cursor_changes.execute("INSERT INTO nickname_changes (fid, old_nickname, new_nickname, change_date) VALUES (?, ?, ?, ?)",
                                                             (fid, old_nickname, new_nickname, current_time))
                                self.conn_changes.commit()
                                self.cursor_users.execute("UPDATE users SET nickname = ? WHERE fid = ?", (new_nickname, fid))
                                self.conn_users.commit()
                                nickname_changes.append(f"📝 `{old_nickname}` ➡️ `{new_nickname}`")

                        checked_users += 1
                embed.set_field_at(
                    1,
                    name="📈 Progress",
                    value=f"✨ Members checked: {checked_users}/{total_users}",
                    inline=False
                )
                if message:
                    await message.edit(embed=embed)

            i += 20

        end_time = datetime.now()
        duration = end_time - start_time

        if furnace_changes or nickname_changes or kid_changes or check_fail_list:
            if furnace_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"🔥 **{alliance_name}** Furnace Level Changes",
                    description=safe_list(furnace_changes),
                    color=discord.Color.orange(),
                    footer=f"📊 Total Changes: {len(furnace_changes)}"
                )

            if nickname_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"📝 **{alliance_name}** Nickname Changes",
                    description=safe_list(nickname_changes),
                    color=discord.Color.blue(),
                    footer=f"📊 Total Changes: {len(nickname_changes)}"
                )

            if kid_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"🌍 **{alliance_name}** State Transfer Notifications",
                    description=safe_list(kid_changes),
                    color=discord.Color.green(),
                    footer=f"📊 Total Changes: {len(kid_changes)}"
                )

            if check_fail_list:
                # Count auto-removed entries
                auto_removed_count = sum(1 for item in check_fail_list if "Auto-removed" in item)
                
                footer_text = f"📊 Total Issues: {len(check_fail_list)}"
                if auto_removed_count > 0:
                    footer_text += f" | 🗑️ Auto-removed: {auto_removed_count}"
                
                await self.send_embed(
                    channel=channel,
                    title=f"❌ **{alliance_name}** Invalid Members Detected",
                    description=safe_list(check_fail_list),
                    color=discord.Color.red(),
                    footer=footer_text
                )

            embed.color = discord.Color.green()
            embed.set_field_at(
                0,
                name="📊 Final Status",
                value=f"✅ Control completed with changes\n⏰ {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
                inline=False
            )
            embed.add_field(
                name="⏱️ Duration",
                value=str(duration),
                inline=True
            )
            embed.add_field(
                name="📈 Total Changes",
                value=f"🔄 {len(furnace_changes) + len(nickname_changes) + len(kid_changes)} changes detected" + (f"\n🗑️ {sum(1 for item in check_fail_list if 'Auto-removed' in item)} invalid IDs removed" if any('Auto-removed' in item for item in check_fail_list) else "") + (f"\n❌ {sum(1 for item in check_fail_list if 'Auto-removed' not in item)} check failures" if any('Auto-removed' not in item for item in check_fail_list) else ""),
                inline=True
            )
        else:
            embed.color = discord.Color.green()
            embed.set_field_at(
                0,
                name="📊 Final Status",
                value=f"✅ Control completed successfully\n⏰ {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n📝 No changes detected",
                inline=False
            )
            embed.add_field(
                name="⏱️ Duration",
                value=str(duration),
                inline=True
            )

        if message:
            await message.edit(embed=embed)
        self.logger.info(f"{alliance_name} Alliance Control completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"{alliance_name} Alliance Total Duration: {duration}")
        
        # Update ephemeral message at completion if provided
        if interaction_message:
            try:
                changes_detected = bool(furnace_changes or nickname_changes or kid_changes or check_fail_list)
                
                if is_batch and batch_info:
                    # Check if this is the last alliance in the batch
                    if batch_info['current'] == batch_info['total']:
                        # Final completion message for all alliances
                        status_embed = discord.Embed(
                            title="✅ Alliance Control Complete",
                            description=(
                                "━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"📊 **Type:** All Alliances ({batch_info['total']} total)\n"
                                f"🏰 **Alliances:** {batch_info['total']} processed\n"
                                f"✅ **Status:** Completed\n"
                                f"📈 **Latest Alliance:** {alliance_name}\n"
                                f"⏱️ **Duration:** {duration.total_seconds():.1f} seconds\n"
                                "━━━━━━━━━━━━━━━━━━━━━━"
                            ),
                            color=discord.Color.green()
                        )
                    else:
                        # Still processing other alliances - just update progress
                        status_embed = discord.Embed(
                            title="🔄 Alliance Control Operation",
                            description=(
                                "━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"📊 **Type:** All Alliances ({batch_info['total']} total)\n"
                                f"🏰 **Completed:** {alliance_name}\n"
                                f"📍 **Progress:** {batch_info['current']}/{batch_info['total']} alliances\n"
                                f"📈 **Changes in {alliance_name}:** {'Yes' if changes_detected else 'No'}\n"
                                "━━━━━━━━━━━━━━━━━━━━━━"
                            ),
                            color=discord.Color.blue()
                        )
                else:
                    # Single alliance completion
                    status_embed = discord.Embed(
                        title="✅ Alliance Control Complete",
                        description=(
                            "━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"📊 **Type:** Single Alliance\n"
                            f"🏰 **Alliance:** {alliance_name}\n"
                            f"✅ **Status:** Completed\n"
                            f"📈 **Changes Detected:** {'Yes' if changes_detected else 'No'}\n"
                            f"⏱️ **Duration:** {duration.total_seconds():.1f} seconds\n"
                            "━━━━━━━━━━━━━━━━━━━━━━"
                        ),
                        color=discord.Color.green()
                    )
                
                await interaction_message.edit(embed=status_embed)
            except Exception as e:
                self.logger.warning(f"Could not update interaction message at completion: {e}")

    async def send_embed(self, channel, title, description, color, footer):
        if isinstance(description, str):
            description = [description]

        current_chunk = []
        current_length = 0

        for desc in description:
            desc_length = len(desc) + 2

            if current_length + desc_length > 2000:
                embed = discord.Embed(
                    title=title,
                    description="\n\n".join(current_chunk),
                    color=color
                )
                embed.set_footer(text="Alliance Control System")
                await channel.send(embed=embed)
                current_chunk = [desc]
                current_length = desc_length
            else:
                current_chunk.append(desc)
                current_length += desc_length

        if current_chunk:
            embed = discord.Embed(
                title=title,
                description="\n\n".join(current_chunk),
                color=color
            )
            embed.set_footer(text=footer)
            await channel.send(embed=embed)

    async def schedule_alliance_check(self, channel, alliance_id, current_interval):
        try:
            await asyncio.sleep(current_interval * 60)
            
            while self.is_running.get(alliance_id, False):
                try:
                    async with self.db_lock:
                        self.cursor_alliance.execute("""
                            SELECT interval 
                            FROM alliancesettings 
                            WHERE alliance_id = ?
                        """, (alliance_id,))
                        result = self.cursor_alliance.fetchone()
                        
                        if not result or result[0] == 0:
                            print(f"[CONTROL] Stopping checks for alliance {alliance_id} - interval disabled")
                            self.is_running[alliance_id] = False
                            break
                        
                        new_interval = result[0]
                        if new_interval != current_interval:
                            print(f"[CONTROL] Interval changed for alliance {alliance_id}: {current_interval} -> {new_interval}")
                            self.is_running[alliance_id] = False
                            self.alliance_tasks[alliance_id] = asyncio.create_task(
                                self.schedule_alliance_check(channel, alliance_id, new_interval)
                            )
                            break

                    await self.login_handler.queue_operation({
                        'type': 'alliance_control',
                        'callback': lambda ch=channel, aid=alliance_id: self.check_agslist(ch, aid, interaction_message=None),
                        'description': f'Scheduled control check for alliance {alliance_id}',
                        'alliance_id': alliance_id
                    })
                    
                    await asyncio.sleep(current_interval * 60)
                    
                except Exception as e:
                    print(f"[ERROR] Error in schedule_alliance_check for alliance {alliance_id}: {e}")
                    await asyncio.sleep(60)
                    
        except Exception as e:
            print(f"[ERROR] Fatal error in schedule_alliance_check for alliance {alliance_id}: {e}")
            traceback.print_exc()

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.monitor_started:
            print("[CONTROL] Starting monitor...")
            
            # Check API availability
            await self.login_handler.check_apis_availability()
            print(f"[CONTROL] {self.login_handler.get_mode_text()}")
            
            # Start the centralized queue processor
            await self.login_handler.start_queue_processor()
            
            self.monitor_alliance_changes.start()
            await self.start_alliance_checks()
            self.monitor_started = True
            self.logger.info("Monitor and queue processor started successfully")

    async def start_alliance_checks(self):
        try:
            for task in self.alliance_tasks.values():
                if not task.done():
                    task.cancel()
            self.alliance_tasks.clear()
            self.is_running.clear()

            async with self.db_lock:
                self.cursor_alliance.execute("""
                    SELECT alliance_id, channel_id, interval 
                    FROM alliancesettings
                    WHERE interval > 0
                """)
                alliances = self.cursor_alliance.fetchall()

                if not alliances:
                    print("[CONTROL] No alliances with intervals found")
                    return

                print(f"[CONTROL] Found {len(alliances)} alliances with intervals")
                
                for alliance_id, channel_id, interval in alliances:
                    channel = self.bot.get_channel(channel_id)
                    if channel is not None:
                        print(f"[CONTROL] Scheduling alliance {alliance_id} with interval {interval} minutes")
                        
                        # Don't queue an immediate check - let the schedule handle it
                        self.is_running[alliance_id] = True
                        self.alliance_tasks[alliance_id] = asyncio.create_task(
                            self.schedule_alliance_check(channel, alliance_id, interval)
                        )
                        
                        await asyncio.sleep(0.5)  # Small delay to prevent overwhelming the system
                    else:
                        print(f"[CONTROL] Channel not found for alliance {alliance_id}")

        except Exception as e:
            print(f"[ERROR] Error in start_alliance_checks: {e}")
            traceback.print_exc()

    async def cog_load(self):
        try:
            print("[MONITOR] Cog loaded successfully")
        except Exception as e:
            print(f"[ERROR] Error in cog_load: {e}")
            import traceback
            print(traceback.format_exc())

    @tasks.loop(minutes=1)
    async def monitor_alliance_changes(self):
        try:
            async with self.db_lock:
                self.cursor_alliance.execute("SELECT alliance_id, channel_id, interval FROM alliancesettings")
                current_settings = {
                    alliance_id: (channel_id, interval)
                    for alliance_id, channel_id, interval in self.cursor_alliance.fetchall()
                }

                for alliance_id, (channel_id, interval) in current_settings.items():
                    task_exists = alliance_id in self.alliance_tasks
                    
                    if interval == 0 and task_exists:
                        self.is_running[alliance_id] = False
                        if not self.alliance_tasks[alliance_id].done():
                            self.alliance_tasks[alliance_id].cancel()
                        del self.alliance_tasks[alliance_id]
                        continue

                    if interval > 0 and (not task_exists or self.alliance_tasks[alliance_id].done()):
                        channel = self.bot.get_channel(channel_id)
                        if channel is not None:
                            self.is_running[alliance_id] = True
                            self.alliance_tasks[alliance_id] = asyncio.create_task(
                                self.schedule_alliance_check(channel, alliance_id, interval)
                            )

                for alliance_id in list(self.alliance_tasks.keys()):
                    if alliance_id not in current_settings:
                        self.is_running[alliance_id] = False
                        if not self.alliance_tasks[alliance_id].done():
                            self.alliance_tasks[alliance_id].cancel()
                        del self.alliance_tasks[alliance_id]

        except Exception as e:
            print(f"[ERROR] Error in monitor_alliance_changes: {e}")
            import traceback
            print(traceback.format_exc())

    @monitor_alliance_changes.before_loop
    async def before_monitor_alliance_changes(self):
        await self.bot.wait_until_ready()

    @monitor_alliance_changes.after_loop
    async def after_monitor_alliance_changes(self):
        if self.monitor_alliance_changes.failed():
            print(Fore.RED + "Monitor alliance changes task failed. Restarting..." + Style.RESET_ALL)
            self.monitor_alliance_changes.restart()

async def setup(bot):
    control_cog = Control(bot)
    await bot.add_cog(control_cog)