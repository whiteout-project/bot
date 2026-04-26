"""
Automatic alliance data synchronization. Periodically fetches player data from the WOS API.
"""
import discord
from discord.ext import commands, tasks
import sqlite3
import asyncio
from datetime import datetime
from colorama import Fore, Style
import os
import traceback
import logging
from .login_handler import LoginHandler
from .pimp_my_bot import theme
from .process_queue import ALLIANCE_SYNC, PreemptedException

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

class AllianceSync(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.conn_alliance = sqlite3.connect('db/alliance.sqlite', timeout=30.0, check_same_thread=False)
        self.conn_users = sqlite3.connect('db/users.sqlite', timeout=30.0, check_same_thread=False)
        self.conn_changes = sqlite3.connect('db/changes.sqlite', timeout=30.0, check_same_thread=False)

        # Use centralized alliance logger
        self.logger = logging.getLogger('alliance')

        self.cursor_alliance = self.conn_alliance.cursor()
        self.cursor_users = self.conn_users.cursor()
        self.cursor_changes = self.conn_changes.cursor()

        # Enable WAL mode for better concurrent access
        self.conn_alliance.execute("PRAGMA journal_mode=WAL")
        self.conn_alliance.execute("PRAGMA synchronous=NORMAL")
        self.conn_users.execute("PRAGMA journal_mode=WAL")
        self.conn_users.execute("PRAGMA synchronous=NORMAL")
        self.conn_changes.execute("PRAGMA journal_mode=WAL")
        self.conn_changes.execute("PRAGMA synchronous=NORMAL")

        self.conn_settings = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.cursor_settings = self.conn_settings.cursor()
        self.conn_settings.execute("PRAGMA journal_mode=WAL")
        self.conn_settings.execute("PRAGMA synchronous=NORMAL")

        # Add update settings columns to alliancesettings if they don't exist
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

        if 'start_time' not in columns:
            self.cursor_alliance.execute("""
                ALTER TABLE alliancesettings
                ADD COLUMN start_time TEXT DEFAULT NULL
            """)

        if 'keep_control_log' not in columns:
            self.cursor_alliance.execute("""
                ALTER TABLE alliancesettings
                ADD COLUMN keep_control_log INTEGER DEFAULT 1
            """)

        if 'show_sync_message' not in columns:
            self.cursor_alliance.execute("""
                ALTER TABLE alliancesettings
                ADD COLUMN show_sync_message INTEGER DEFAULT 1
            """)
            try:
                self.cursor_settings.execute("SELECT value FROM auto LIMIT 1")
                prior_global = self.cursor_settings.fetchone()
                if prior_global is not None:
                    self.cursor_alliance.execute(
                        "UPDATE alliancesettings SET show_sync_message = ?",
                        (int(prior_global[0]) if prior_global[0] is not None else 1,),
                    )
            except Exception as e:
                self.logger.warning(
                    f"show_sync_message backfill from legacy auto.value failed: {e}. "
                    f"All alliances will default to ON."
                )

        self.conn_alliance.commit()

        # Create invalid_id_tracker table for 3-strike removal system
        self.cursor_settings.execute("""
            CREATE TABLE IF NOT EXISTS invalid_id_tracker (
                fid TEXT PRIMARY KEY,
                alliance_id TEXT,
                nickname TEXT,
                fail_count INTEGER DEFAULT 1,
                first_failure TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_failure TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn_settings.commit()

        self.db_lock = asyncio.Lock()
        self.proxies = self.load_proxies()
        self.alliance_tasks = {}
        self.is_running = {}
        self.monitor_started = False
        self.current_task_settings = {}  # {alliance_id: (channel_id, interval, start_time)}

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

    def get_keep_control_log_setting(self, alliance_id):
        """Get the keep_control_log setting for a specific alliance"""
        self.cursor_alliance.execute("""
            SELECT keep_control_log
            FROM alliancesettings
            WHERE alliance_id = ?
        """, (alliance_id,))
        result = self.cursor_alliance.fetchone()
        return result[0] if result and result[0] is not None else 1

    def increment_invalid_counter(self, fid: str, alliance_id: str, nickname: str) -> int:
        """Increment the 40004 error counter for a player. Returns new fail_count."""
        self.cursor_settings.execute(
            "SELECT fail_count FROM invalid_id_tracker WHERE fid = ?", (fid,)
        )
        result = self.cursor_settings.fetchone()

        if result:
            new_count = result[0] + 1
            self.cursor_settings.execute("""
                UPDATE invalid_id_tracker
                SET fail_count = ?, last_failure = CURRENT_TIMESTAMP, nickname = ?, alliance_id = ?
                WHERE fid = ?
            """, (new_count, nickname, alliance_id, fid))
        else:
            new_count = 1
            self.cursor_settings.execute("""
                INSERT INTO invalid_id_tracker (fid, alliance_id, nickname, fail_count)
                VALUES (?, ?, ?, 1)
            """, (fid, alliance_id, nickname))

        self.conn_settings.commit()
        return new_count

    def reset_invalid_counter(self, fid: str):
        """Reset/remove the 40004 error counter for a player (called on successful check)."""
        self.cursor_settings.execute("DELETE FROM invalid_id_tracker WHERE fid = ?", (fid,))
        self.conn_settings.commit()

    def get_invalid_count(self, fid: str) -> int:
        """Get current fail count for a player."""
        self.cursor_settings.execute(
            "SELECT fail_count FROM invalid_id_tracker WHERE fid = ?", (fid,)
        )
        result = self.cursor_settings.fetchone()
        return result[0] if result else 0

    def is_connection_error(self, error_msg: str) -> bool:
        """Check if error message indicates a network/connection issue vs actual player error"""
        network_indicators = [
            'timeout',
            'connection',
            'connect',
            'timed out',
            'refused',
            'unreachable',
            'reset',
            'dns',
            'network',
            'socket',
            'ssl',
            'certificate',
            'host',
            '403',
            'forbidden',
            '429',
            '502',
            '503',
            '504',
        ]
        error_lower = error_msg.lower()
        return any(indicator in error_lower for indicator in network_indicators)

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

    async def check_agslist(self, channel, alliance_id, interaction=None, interaction_message=None, alliance_name=None, is_batch=False, batch_info=None, progress_message=None, process_id=None):
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
        self.logger.info(f"{alliance_name} Alliance Sync started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Update ephemeral message at start if provided
        if interaction_message:
            try:
                if is_batch and batch_info:
                    # For batch processing (all alliances)
                    status_embed = discord.Embed(
                        title=f"{theme.refreshIcon} Alliance Sync Operation",
                        description=(
                            f"{theme.upperDivider}\n"
                            f"{theme.chartIcon} **Type:** All Alliances ({batch_info['total']} total)\n"
                            f"{theme.allianceIcon} **Currently Processing:** {alliance_name}\n"
                            f"{theme.pinIcon} **Progress:** {batch_info['current']}/{batch_info['total']} alliances\n"
                            f"{theme.timeIcon} **Started:** <t:{int(start_time.timestamp())}:R>\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor1
                    )
                else:
                    # For single alliance processing
                    status_embed = discord.Embed(
                        title=f"{theme.refreshIcon} Alliance Sync Operation",
                        description=(
                            f"{theme.upperDivider}\n"
                            f"{theme.chartIcon} **Type:** Single Alliance\n"
                            f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                            f"{theme.pinIcon} **Status:** In Progress\n"
                            f"{theme.timeIcon} **Started:** <t:{int(start_time.timestamp())}:R>\n"
                            f"{theme.announceIcon} **Results Channel:** {channel.mention}\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor1
                    )
                await interaction_message.edit(embed=status_embed)
            except Exception as e:
                self.logger.warning(f"Could not update interaction message at start: {e}")
        
        async with self.db_lock:
            self.cursor_alliance.execute(
                "SELECT show_sync_message FROM alliancesettings WHERE alliance_id = ?",
                (alliance_id,),
            )
            row = self.cursor_alliance.fetchone()
            auto_value = row[0] if row and row[0] is not None else 1
        
        embed = discord.Embed(
            title=f"{theme.allianceIcon} {alliance_name} Alliance Sync",
            description=f"{theme.searchIcon} Checking for changes in member status...",
            color=theme.emColor1
        )
        embed.add_field(
            name=f"{theme.chartIcon} Status",
            value=f"{theme.hourglassIcon} Sync started at {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            inline=False
        )
        embed.add_field(
            name=f"{theme.chartIcon} Progress",
            value=f"{theme.verifiedIcon} Members checked: {checked_users}/{total_users}",
            inline=False
        )
        embed.set_footer(text=f"{theme.boltIcon} Automatic Alliance Sync System")
        
        message = None
        if progress_message is not None:
            # Recovery path: re-use the message from the pre-restart run so
            # the channel doesn't accumulate a stuck "Members checked: X/Y"
            # embed next to a fresh one from the resumed attempt.
            message = progress_message
            embed.description = f"{theme.refreshIcon} Sync resumed after bot restart — re-checking all members…"
            try:
                await message.edit(embed=embed)
            except Exception as e:
                self.logger.warning(f"Could not edit resumed progress message: {e}")
                message = None

        if message is None and auto_value == 1:
            message = await channel.send(embed=embed)
            # Checkpoint the message id so if this run gets interrupted and
            # recovered, the next attempt can re-use the same message.
            if process_id is not None and message is not None:
                pq = self.bot.get_cog('ProcessQueue')
                if pq:
                    try:
                        pq.update_details(process_id, {
                            'channel_id': channel.id,
                            'progress_message_id': message.id,
                        })
                    except Exception as e:
                        self.logger.warning(f"Could not checkpoint progress message id: {e}")

        furnace_changes, nickname_changes, kid_changes, check_fail_list = [], [], [], []
        members_to_remove = []  # Track members that should be removed for bulk check
        connection_errors = []  # Track network/connection issues separately (not invalid members)

        def safe_list(input_list): # Avoid issues with list indexing
            if not isinstance(input_list, list):
                return []
            return [str(item) for item in input_list if item]

        # Cooperative preemption: yield to higher-priority work between members
        process_queue_cog = self.bot.get_cog('ProcessQueue')

        i = 0
        while i < total_users:
            batch_users = users[i:i+20]
            for fid, old_nickname, old_furnace_lv, old_stove_lv_content, old_kid in batch_users:
                # Check for higher-priority work
                if process_queue_cog and process_queue_cog.should_preempt():
                    self.logger.info(f"AllianceSync: Preempting sync for {alliance_name} - higher priority work waiting")
                    raise PreemptedException()

                data = await self.fetch_user_data(fid)
                
                if data == 429:
                    # Get wait time from login handler
                    wait_time = self.login_handler._get_wait_time()
                    
                    embed.description = f"{theme.warnIcon} API Rate Limit! Waiting {wait_time:.1f} seconds...\n📊 Progress: {checked_users}/{total_users} members"
                    embed.color = discord.Color.orange()
                    if message:
                        await message.edit(embed=embed)
                    
                    await asyncio.sleep(wait_time)
                    
                    embed.description = f"{theme.searchIcon} Checking for changes in member status..."
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
                            fail_count = self.increment_invalid_counter(fid, alliance_id, old_nickname)

                            if fail_count >= 3:  # Silently track failures 1 and 2, remove after 3
                                members_to_remove.append((fid, old_nickname, "Player does not exist (3x confirmed)"))
                                check_fail_list.append(f"{theme.deniedIcon} `{fid}` ({old_nickname}) - Player not found 3x in a row - Pending removal")
                        elif self.is_connection_error(error_msg):
                            # Network/connection issue - NOT an invalid member, just track FID for summary
                            connection_errors.append(fid)
                            self.logger.warning(f"Connection issue checking ID {fid}: {error_msg}")
                        else:
                            # For other API errors, report without removing
                            check_fail_list.append(f"{theme.deniedIcon} `{fid}` - {error_msg}")
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

                        # Reset 40004 error counter on successful check
                        self.reset_invalid_counter(fid)

                        async with self.db_lock:
                            if new_stove_lv_content != old_stove_lv_content:
                                self.cursor_users.execute("UPDATE users SET stove_lv_content = ? WHERE fid = ?", (new_stove_lv_content, fid))
                                self.conn_users.commit()

                            if old_kid != new_kid:
                                kid_changes.append(f"{theme.userIcon} {old_nickname} has transferred to a new state\n{theme.stateOldIcon} Old State: {old_kid}\n{theme.stateIcon} New State: {new_kid}")
                                
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
                                                await user.send(f"{theme.deniedIcon} {old_nickname} `{fid}` was removed from the users table due to state transfer.")
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
                                furnace_changes.append(f"{theme.userIcon} **{old_nickname}**\n{theme.stoveOldIcon} `{old_furnace_display}` {theme.forwardIcon} {theme.stoveIcon} `{new_furnace_display}`")

                            if new_nickname.lower() != old_nickname.lower().strip():
                                self.cursor_changes.execute("INSERT INTO nickname_changes (fid, old_nickname, new_nickname, change_date) VALUES (?, ?, ?, ?)",
                                                             (fid, old_nickname, new_nickname, current_time))
                                self.conn_changes.commit()
                                self.cursor_users.execute("UPDATE users SET nickname = ? WHERE fid = ?", (new_nickname, fid))
                                self.conn_users.commit()
                                nickname_changes.append(f"{theme.avatarOldIcon} `{old_nickname}` {theme.forwardIcon} {theme.avatarIcon} `{new_nickname}`")

                        checked_users += 1
                embed.set_field_at(
                    1,
                    name=f"{theme.chartIcon} Progress",
                    value=f"{theme.verifiedIcon} Members checked: {checked_users}/{total_users}",
                    inline=False
                )
                if message:
                    await message.edit(embed=embed)

            i += 20

        # Bulk removal safeguard - check if we're removing too many members
        removal_count = len(members_to_remove)
        removal_percentage = (removal_count / total_users * 100) if total_users > 0 else 0

        # Only apply safeguard if alliance has at least 5 members and would remove >20%
        if total_users >= 5 and removal_percentage > 20:
            self.logger.error(f"BULK REMOVAL BLOCKED: Attempted to remove {removal_count}/{total_users} members ({removal_percentage:.1f}%) from alliance {alliance_id}")

            # Send alert to channel
            alert_embed = discord.Embed(
                title=f"{theme.warnIcon} BULK REMOVAL BLOCKED - SAFETY TRIGGERED",
                description=(
                    f"**Alliance Check Safety System Activated**\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                    f"{theme.userIcon} **Total Members:** {total_users}\n"
                    f"{theme.deniedIcon} **Attempted Removals:** {removal_count}\n"
                    f"{theme.chartIcon} **Percentage:** {removal_percentage:.1f}%\n"
                    f"{theme.allianceIcon} **Threshold:** 20%\n\n"
                    f"**Reason:** Removing more than 20% of members suggests a potential API issue.\n\n"
                    f"**Members that would have been removed:**\n"
                    + "\n".join([f"• `{fid}` ({nickname})" for fid, nickname, _ in members_to_remove[:10]])
                    + (f"\n• ... and {removal_count - 10} more" if removal_count > 10 else "")
                    + f"\n\n{theme.warnIcon} **Action Required:** Please verify these members manually or wait for API issues to resolve."
                ),
                color=theme.emColor2
            )
            alert_embed.set_footer(text=f"{theme.allianceIcon} Automatic Safety System | No members were removed")
            await channel.send(embed=alert_embed)

            # Update check_fail_list to show blocked status instead of pending
            for i, item in enumerate(check_fail_list):
                if "Pending removal" in item:
                    check_fail_list[i] = item.replace("Pending removal", "Removal blocked (safety)")
        else:
            # Safe to proceed with removals
            if members_to_remove:
                self.logger.info(f"Proceeding with removal of {removal_count} members from alliance {alliance_id} ({removal_percentage:.1f}%)")

                for fid, nickname, reason in members_to_remove:
                    removed, _ = await self.remove_invalid_fid(fid, reason)

                    # Update check_fail_list to show actual removal status
                    for i, item in enumerate(check_fail_list):
                        if f"`{fid}`" in item and "Pending removal" in item:
                            if removed:
                                check_fail_list[i] = item.replace("Pending removal", "Removed")
                            else:
                                check_fail_list[i] = item.replace("Pending removal", "Failed to remove")
                            break

        end_time = datetime.now()
        duration = end_time - start_time

        if furnace_changes or nickname_changes or kid_changes or check_fail_list or connection_errors:
            if furnace_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"{theme.levelIcon} **{alliance_name}** Furnace Level Changes",
                    description=safe_list(furnace_changes),
                    color=discord.Color.orange(),
                    footer=f"{theme.chartIcon} Total Changes: {len(furnace_changes)}"
                )

            if nickname_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"{theme.editListIcon} **{alliance_name}** Nickname Changes",
                    description=safe_list(nickname_changes),
                    color=theme.emColor1,
                    footer=f"{theme.chartIcon} Total Changes: {len(nickname_changes)}"
                )

            if kid_changes:
                await self.send_embed(
                    channel=channel,
                    title=f"{theme.stateIcon} **{alliance_name}** State Transfer Notifications",
                    description=safe_list(kid_changes),
                    color=theme.emColor3,
                    footer=f"{theme.chartIcon} Total Changes: {len(kid_changes)}"
                )

            if check_fail_list:
                # Count removed entries
                removed_count = sum(1 for item in check_fail_list if "- Removed" in item)

                footer_text = f"{theme.chartIcon} Total Issues: {len(check_fail_list)}"
                if removed_count > 0:
                    footer_text += f" | {theme.trashIcon} Removed: {removed_count}"
                
                await self.send_embed(
                    channel=channel,
                    title=f"{theme.deniedIcon} **{alliance_name}** Invalid Members Detected",
                    description=safe_list(check_fail_list),
                    color=theme.emColor2,
                    footer=footer_text
                )

            if connection_errors:
                # Connection issues are informational - members NOT removed
                if len(connection_errors) <= 5:
                    # Show specific IDs for small numbers
                    description = "\n".join([f"{theme.warnIcon} `{fid}` - Connection issue" for fid in connection_errors])
                else:
                    # Show summary for large numbers (API likely down)
                    description = (
                        f"{theme.chartIcon} **{len(connection_errors)}** member(s) had connection issues\n"
                        f"{theme.linkIcon} Unable to reach game API - these members will be checked on next scheduled run\n\n"
                        f"Members NOT affected - no data was changed."
                    )
                await self.send_embed(
                    channel=channel,
                    title=f"{theme.warnIcon} **{alliance_name}** Connection Issues",
                    description=description,
                    color=discord.Color.orange(),
                    footer=f"{theme.chartIcon} {len(connection_errors)} connection issue(s) - Members NOT affected"
                )

            embed.color = discord.Color.green()
            embed.set_field_at(
                0,
                name=f"{theme.chartIcon} Final Status",
                value=f"{theme.verifiedIcon} Sync completed with changes\n{theme.alarmClockIcon} {end_time.strftime('%Y-%m-%d %H:%M:%S')}",
                inline=False
            )
            embed.add_field(
                name=f"{theme.hourglassIcon} Duration",
                value=str(duration),
                inline=True
            )
            # Build the value string without nested f-strings for Python 3.9+ compatibility
            total_changes = len(furnace_changes) + len(nickname_changes) + len(kid_changes)
            changes_text = f"{theme.refreshIcon} {total_changes} changes detected"

            # Add removed count if any
            removed_count = sum(1 for item in check_fail_list if '- Removed' in item)
            if removed_count > 0:
                changes_text += f"\n{theme.trashIcon} {removed_count} invalid IDs removed"

            # Add check failures count if any
            check_failure_count = sum(1 for item in check_fail_list if '- Removed' not in item)
            if check_failure_count > 0:
                changes_text += f"\n{theme.deniedIcon} {check_failure_count} check failures"

            # Add connection issues count if any (informational only)
            if connection_errors:
                changes_text += f"\n{theme.warnIcon} {len(connection_errors)} connection issue(s)"

            embed.add_field(
                name=f"{theme.chartIcon} Total Changes",
                value=changes_text,
                inline=True
            )
        else:
            embed.color = discord.Color.green()
            embed.set_field_at(
                0,
                name=f"{theme.chartIcon} Final Status",
                value=f"{theme.verifiedIcon} Sync completed successfully\n{theme.alarmClockIcon} {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n{theme.listIcon} No changes detected",
                inline=False
            )
            embed.add_field(
                name=f"{theme.hourglassIcon} Duration",
                value=str(duration),
                inline=True
            )

        if message:
            keep_log = self.get_keep_control_log_setting(alliance_id)
            if keep_log:
                await message.edit(embed=embed)
            else:
                try:
                    await message.delete()
                except discord.NotFound:
                    pass
        self.logger.info(f"{alliance_name} Alliance Sync completed at {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"{alliance_name} Alliance Total Duration: {duration}")
        
        # Update ephemeral message at completion if provided
        if interaction_message:
            try:
                changes_detected = bool(furnace_changes or nickname_changes or kid_changes or check_fail_list or connection_errors)
                
                if is_batch and batch_info:
                    # Check if this is the last alliance in the batch
                    if batch_info['current'] == batch_info['total']:
                        # Final completion message for all alliances
                        status_embed = discord.Embed(
                            title=f"{theme.verifiedIcon} Alliance Sync Complete",
                            description=(
                                f"{theme.upperDivider}\n"
                                f"{theme.chartIcon} **Type:** All Alliances ({batch_info['total']} total)\n"
                                f"{theme.allianceIcon} **Alliances:** {batch_info['total']} processed\n"
                                f"{theme.verifiedIcon} **Status:** Completed\n"
                                f"{theme.chartIcon} **Latest Alliance:** {alliance_name}\n"
                                f"{theme.timeIcon} **Duration:** {duration.total_seconds():.1f} seconds\n"
                                f"{theme.lowerDivider}"
                            ),
                            color=theme.emColor3
                        )
                    else:
                        # Still processing other alliances - just update progress
                        status_embed = discord.Embed(
                            title=f"{theme.refreshIcon} Alliance Sync Operation",
                            description=(
                                f"{theme.upperDivider}\n"
                                f"{theme.chartIcon} **Type:** All Alliances ({batch_info['total']} total)\n"
                                f"{theme.allianceIcon} **Completed:** {alliance_name}\n"
                                f"{theme.pinIcon} **Progress:** {batch_info['current']}/{batch_info['total']} alliances\n"
                                f"{theme.chartIcon} **Changes in {alliance_name}:** {'Yes' if changes_detected else 'No'}\n"
                                f"{theme.lowerDivider}"
                            ),
                            color=theme.emColor1
                        )
                else:
                    # Single alliance completion
                    status_embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Alliance Sync Complete",
                        description=(
                            f"{theme.upperDivider}\n"
                            f"{theme.chartIcon} **Type:** Single Alliance\n"
                            f"{theme.allianceIcon} **Alliance:** {alliance_name}\n"
                            f"{theme.verifiedIcon} **Status:** Completed\n"
                            f"{theme.chartIcon} **Changes Detected:** {'Yes' if changes_detected else 'No'}\n"
                            f"{theme.timeIcon} **Duration:** {duration.total_seconds():.1f} seconds\n"
                            f"{theme.lowerDivider}"
                        ),
                        color=theme.emColor3
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
                embed.set_footer(text="Alliance Sync System")
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

    def _calculate_initial_delay(self, start_time: str, interval: int) -> int:
        """Calculate seconds until next scheduled run based on start_time.

        If start_time is set (HH:MM format, UTC), calculate delay until that time.
        If the time has passed today, calculate when the next interval-aligned run would be.
        If start_time is None, return 0 (start immediately with interval delay).
        """
        if not start_time:
            return interval * 60  # No start_time, use interval as initial delay

        try:
            from datetime import timezone
            now = datetime.now(timezone.utc)
            hour, minute = map(int, start_time.split(':'))

            # Create target time for today
            target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

            # If target time has passed, find the next interval-aligned occurrence
            if target <= now:
                # Calculate how many intervals have passed since target
                seconds_since_target = (now - target).total_seconds()
                interval_seconds = interval * 60
                intervals_passed = int(seconds_since_target / interval_seconds) + 1
                target = target.replace(day=target.day) + __import__('datetime').timedelta(seconds=intervals_passed * interval_seconds)

            delay_seconds = (target - now).total_seconds()
            return max(0, int(delay_seconds))
        except (ValueError, AttributeError) as e:
            self.logger.error(f"Invalid start_time format '{start_time}': {e}")
            print(f"[SYNC] Invalid start_time format '{start_time}': {e}")
            return interval * 60  # Fall back to interval delay

    async def schedule_alliance_check(self, alliance_id):
        """Schedule periodic alliance checks. Settings are fetched fresh from DB."""
        try:
            # Get initial settings
            cached = self.current_task_settings.get(alliance_id)
            if not cached:
                print(f"[SYNC] No cached settings for alliance {alliance_id}, stopping")
                return

            channel_id, interval, start_time = cached

            # Calculate initial delay based on start_time
            initial_delay = self._calculate_initial_delay(start_time, interval)
            if initial_delay > 0:
                await asyncio.sleep(initial_delay)

            while self.is_running.get(alliance_id, False):
                try:
                    # Fetch fresh settings from cache (updated by monitor loop)
                    cached = self.current_task_settings.get(alliance_id)
                    if not cached:
                        print(f"[SYNC] Alliance {alliance_id} removed from settings, stopping")
                        break

                    channel_id, interval, start_time = cached

                    # Get the channel fresh each time
                    channel = self.bot.get_channel(channel_id)
                    if channel is None:
                        print(f"[SYNC] Channel {channel_id} not found for alliance {alliance_id}")
                        await asyncio.sleep(60)
                        continue

                    # Queue the scheduled control check via ProcessQueue, but only
                    # if a prior scheduled sync for this alliance hasn't already
                    # been run / is still waiting its turn. Prevents duplicate
                    # rows piling up when higher-priority work monopolises the
                    # queue.
                    process_queue = self.bot.get_cog('ProcessQueue')
                    if process_queue:
                        if process_queue.has_queued_or_active('alliance_sync', alliance_id=alliance_id):
                            self.logger.info(
                                f"[SYNC] Skipping scheduled sync for alliance {alliance_id}: "
                                f"previous sync still queued/active"
                            )
                        else:
                            process_queue.enqueue(
                                action='alliance_sync',
                                priority=ALLIANCE_SYNC,
                                alliance_id=alliance_id,
                                details={'channel_id': channel_id},
                            )
                    else:
                        self.logger.error(f"ProcessQueue not available, cannot enqueue scheduled sync for alliance {alliance_id}")

                    # Sleep for the interval
                    await asyncio.sleep(interval * 60)

                except asyncio.CancelledError:
                    self.logger.info(f"Task cancelled for alliance {alliance_id}")
                    raise
                except Exception as e:
                    self.logger.error(f"Error in schedule_alliance_check for alliance {alliance_id}: {e}")
                    print(f"[ERROR] Error in schedule_alliance_check for alliance {alliance_id}: {e}")
                    await asyncio.sleep(60)

        except asyncio.CancelledError:
            self.logger.info(f"Schedule task cancelled for alliance {alliance_id}")
        except Exception as e:
            self.logger.error(f"Fatal error in schedule_alliance_check for alliance {alliance_id}: {e}")
            print(f"[ERROR] Fatal error in schedule_alliance_check for alliance {alliance_id}: {e}")
            traceback.print_exc()

    async def handle_alliance_control_process(self, process):
        """ProcessQueue handler for alliance_control actions (manual user-triggered checks)."""
        details = process.get('details', {})
        alliance_id = process.get('alliance_id')
        channel_id = details.get('channel_id')
        alliance_name = details.get('alliance_name')
        is_batch = details.get('is_batch', False)
        batch_info = details.get('batch_info')

        if not alliance_id or not channel_id:
            self.logger.error(f"alliance_control process {process['id']} missing alliance_id or channel_id")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.logger.error(f"alliance_control process {process['id']}: channel {channel_id} not found")
            return

        # Look up live runtime context (interaction_message) — None if missing or after restart
        process_queue = self.bot.get_cog('ProcessQueue')
        runtime = process_queue.get_runtime_context(process['id']) if process_queue else {}
        interaction_message = runtime.get('interaction_message')

        await self.check_agslist(
            channel,
            alliance_id,
            interaction_message=interaction_message,
            alliance_name=alliance_name,
            is_batch=is_batch,
            batch_info=batch_info,
        )

    async def handle_alliance_sync_process(self, process):
        """ProcessQueue handler for alliance_sync actions (scheduled background checks)."""
        details = process.get('details', {})
        alliance_id = process.get('alliance_id')
        channel_id = details.get('channel_id')

        if not alliance_id or not channel_id:
            self.logger.error(f"alliance_sync process {process['id']} missing alliance_id or channel_id")
            return

        channel = self.bot.get_channel(channel_id)
        if not channel:
            self.logger.error(f"alliance_sync process {process['id']}: channel {channel_id} not found")
            return

        # Recovery path: if a prior attempt posted a progress message and got
        # interrupted, re-fetch it so check_agslist can update the existing
        # embed instead of leaving it stuck while a fresh one runs beside it.
        progress_message = None
        msg_id = details.get('progress_message_id')
        if msg_id:
            try:
                progress_message = await channel.fetch_message(int(msg_id))
                self.logger.info(
                    f"alliance_sync {process['id']}: resuming with existing message {msg_id}"
                )
            except Exception as e:
                self.logger.warning(
                    f"alliance_sync {process['id']}: could not fetch prior progress message {msg_id} ({e})"
                )

        await self.check_agslist(
            channel, alliance_id,
            interaction_message=None,
            progress_message=progress_message,
            process_id=process['id'],
        )

    @commands.Cog.listener()
    async def on_ready(self):
        if not self.monitor_started:
            self.logger.info("Starting monitor...")

            # Check API availability
            await self.login_handler.check_apis_availability()
            self.logger.info(self.login_handler.get_mode_text(for_console=True))

            # Register handlers with the ProcessQueue cog
            process_queue_cog = self.bot.get_cog('ProcessQueue')
            if process_queue_cog:
                process_queue_cog.register_handler('alliance_control', self.handle_alliance_control_process)
                process_queue_cog.register_handler('alliance_sync', self.handle_alliance_sync_process)
                self.logger.info("AllianceSync: Registered alliance_control and alliance_sync handlers with ProcessQueue")
            else:
                self.logger.error("AllianceSync: ProcessQueue cog not found, alliance operations will not work")

            self.monitor_alliance_changes.start()
            await self.start_alliance_checks()
            self.monitor_started = True
            self.logger.info("Monitor and handlers registered successfully")

    async def start_alliance_checks(self):
        try:
            for task in self.alliance_tasks.values():
                if not task.done():
                    task.cancel()
            self.alliance_tasks.clear()
            self.is_running.clear()
            self.current_task_settings.clear()

            async with self.db_lock:
                self.cursor_alliance.execute("""
                    SELECT s.alliance_id, s.channel_id, s.interval, s.start_time, a.name
                    FROM alliancesettings s
                    JOIN alliance_list a ON s.alliance_id = a.alliance_id
                    WHERE s.interval > 0
                """)
                alliances = self.cursor_alliance.fetchall()

                if not alliances:
                    self.logger.info("No alliances with intervals found")
                    return

                scheduled_alliances = []
                for alliance_id, channel_id, interval, start_time, alliance_name in alliances:
                    channel = self.bot.get_channel(channel_id)
                    if channel is not None:
                        scheduled_alliances.append(alliance_name)
                        self.is_running[alliance_id] = True
                        self.current_task_settings[alliance_id] = (channel_id, interval, start_time)
                        self.alliance_tasks[alliance_id] = asyncio.create_task(
                            self.schedule_alliance_check(alliance_id)
                        )

                        await asyncio.sleep(0.5)  # Small delay to prevent overwhelming the system
                    else:
                        self.logger.warning(f"Channel not found for alliance {alliance_id}")

                if scheduled_alliances:
                    msg = f"Scheduled controls for {len(scheduled_alliances)} alliance(s): {', '.join(scheduled_alliances)}"
                    print(f"[SYNC] {msg}")
                    self.logger.info(msg)

        except Exception as e:
            self.logger.error(f"Error in start_alliance_checks: {e}")
            print(f"[ERROR] Error in start_alliance_checks: {e}")
            traceback.print_exc()

    async def cog_load(self):
        try:
            self.logger.info("Alliance sync cog loaded")
        except Exception as e:
            self.logger.error(f"Error in cog_load: {e}")

    @tasks.loop(minutes=1)
    async def monitor_alliance_changes(self):
        try:
            async with self.db_lock:
                self.cursor_alliance.execute("SELECT alliance_id, channel_id, interval, start_time FROM alliancesettings")
                current_settings = {
                    alliance_id: (channel_id, interval, start_time)
                    for alliance_id, channel_id, interval, start_time in self.cursor_alliance.fetchall()
                }

                for alliance_id, (channel_id, interval, start_time) in current_settings.items():
                    task_exists = alliance_id in self.alliance_tasks
                    cached_settings = self.current_task_settings.get(alliance_id)

                    # If interval is 0, stop the task
                    if interval == 0 and task_exists:
                        print(f"[SYNC] Stopping alliance {alliance_id} - interval set to 0")
                        self.is_running[alliance_id] = False
                        if not self.alliance_tasks[alliance_id].done():
                            self.alliance_tasks[alliance_id].cancel()
                        del self.alliance_tasks[alliance_id]
                        if alliance_id in self.current_task_settings:
                            del self.current_task_settings[alliance_id]
                        continue

                    # Check if settings changed (channel, interval, or start_time)
                    settings_changed = cached_settings and cached_settings != (channel_id, interval, start_time)
                    if settings_changed and task_exists:
                        old_channel, old_interval, old_start = cached_settings
                        print(f"[SYNC] Settings changed for alliance {alliance_id}:")
                        if old_channel != channel_id:
                            print(f"  Channel: {old_channel} -> {channel_id}")
                        if old_interval != interval:
                            print(f"  Interval: {old_interval} -> {interval}")
                        if old_start != start_time:
                            print(f"  Start time: {old_start} -> {start_time}")
                        # Cancel existing task to restart with new settings
                        self.is_running[alliance_id] = False
                        if not self.alliance_tasks[alliance_id].done():
                            self.alliance_tasks[alliance_id].cancel()
                        del self.alliance_tasks[alliance_id]
                        task_exists = False

                    # Start new task if needed
                    if interval > 0 and (not task_exists or self.alliance_tasks[alliance_id].done()):
                        channel = self.bot.get_channel(channel_id)
                        if channel is not None:
                            self.logger.info(f"Starting task for alliance {alliance_id} (interval: {interval}min, start_time: {start_time})")
                            self.is_running[alliance_id] = True
                            self.current_task_settings[alliance_id] = (channel_id, interval, start_time)
                            self.alliance_tasks[alliance_id] = asyncio.create_task(
                                self.schedule_alliance_check(alliance_id)
                            )

                # Clean up tasks for removed alliances
                for alliance_id in list(self.alliance_tasks.keys()):
                    if alliance_id not in current_settings:
                        print(f"[SYNC] Removing task for deleted alliance {alliance_id}")
                        self.is_running[alliance_id] = False
                        if not self.alliance_tasks[alliance_id].done():
                            self.alliance_tasks[alliance_id].cancel()
                        del self.alliance_tasks[alliance_id]
                        if alliance_id in self.current_task_settings:
                            del self.current_task_settings[alliance_id]

        except Exception as e:
            self.logger.error(f"Error in monitor_alliance_changes: {e}")
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
            # Cancel per-alliance schedulers and wait for them to exit so the
            # restarted monitor doesn't spawn duplicates racing against zombies.
            pending = [t for t in self.alliance_tasks.values() if t and not t.done()]
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            self.alliance_tasks.clear()
            self.is_running.clear()
            self.monitor_alliance_changes.restart()

async def setup(bot):
    await bot.add_cog(AllianceSync(bot))