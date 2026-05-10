"""
Gift code orchestrator cog. Delegates to gift_redemption, gift_channels,
gift_settings, gift_views, and gift_operationsapi for the heavy lifting.
- gift_captchasolver: ONNX-based CAPTCHA solver for gift code redemption
"""

import discord
from discord.ext import commands, tasks
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib
import json
from datetime import datetime
import sqlite3
import asyncio
import re
import os
import traceback
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .gift_operationsapi import GiftCodeAPI
from .gift_captchasolver import GiftCaptchaSolver
from .pimp_my_bot import theme, safe_edit_message
from . import gift_redemption
from . import gift_channels
from . import gift_settings
from .gift_views import GiftView


class GiftOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Use centralized loggers
        self.logger = logging.getLogger('gift')  # Operations log -> gift.log
        self.giftlog = logging.getLogger('redemption')  # Redemption summaries -> redemption.log

        self.logger.info("GiftOperations Cog initializing...")

        os.makedirs('db', exist_ok=True)

        if hasattr(bot, 'conn'):
            self.conn = bot.conn
            self.cursor = self.conn.cursor()
        else:
            self.conn = sqlite3.connect('db/giftcode.sqlite', timeout=30.0)
            self.cursor = self.conn.cursor()

            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=10000")
            self.conn.execute("PRAGMA temp_store=MEMORY")
            self.conn.commit()

        # API Setup
        self.api = GiftCodeAPI(bot)

        # Gift Code Control Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS giftcodecontrol (
                alliance_id INTEGER PRIMARY KEY,
                status INTEGER DEFAULT 0
            )
        """)
        self.conn.commit()

        # Settings DB Connection
        self.settings_conn = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.settings_cursor = self.settings_conn.cursor()

        # Alliance DB Connection
        self.alliance_conn = sqlite3.connect('db/alliance.sqlite', timeout=30.0, check_same_thread=False)
        self.alliance_cursor = self.alliance_conn.cursor()

        # Gift Code Channel Table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS giftcode_channel (
                alliance_id INTEGER,
                channel_id INTEGER,
                PRIMARY KEY (alliance_id)
            )
        """)
        self.conn.commit()

        # Add scan_history column if it doesn't exist (defaults to 0/False)
        try:
            self.cursor.execute("ALTER TABLE giftcode_channel ADD COLUMN scan_history INTEGER DEFAULT 0")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        # Add validation_status column to gift_codes table if it doesn't exist
        try:
            self.cursor.execute("ALTER TABLE gift_codes ADD COLUMN validation_status TEXT DEFAULT 'pending'")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        # Add priority column to giftcodecontrol table if it doesn't exist
        try:
            self.cursor.execute("ALTER TABLE giftcodecontrol ADD COLUMN priority INTEGER DEFAULT 0")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass

        # WOS API URLs and Key
        self.wos_player_info_url = "https://wos-giftcode-api.centurygame.com/api/player"
        self.wos_giftcode_url = "https://wos-giftcode-api.centurygame.com/api/gift_code"
        self.wos_captcha_url = "https://wos-giftcode-api.centurygame.com/api/captcha"
        self.wos_giftcode_redemption_url = "https://wos-giftcode.centurygame.com"
        self.wos_encrypt_key = "tB87#kPtkxqOS2"

        # Retry Configuration for Requests
        self.retry_config = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )

        # Initialization of Locks and Cooldowns
        self.captcha_solver = None
        self._validation_lock = asyncio.Lock()
        self.last_validation_attempt_time = 0
        self.validation_cooldown = 5
        self._last_cleanup_date = None

        self.test_captcha_cooldowns = {}
        self.test_captcha_delay = 60

        # Batch redemption tracking (in-memory only, for live progress messages)
        self.redemption_batches = {}

        self.processing_stats = {
            "ocr_solver_calls": 0,
            "ocr_valid_format": 0,
            "captcha_submissions": 0,
            "server_validation_success": 0,
            "server_validation_failure": 0,
            "total_fids_processed": 0,
            "total_processing_time": 0.0
        }

        # Captcha Solver Initialization
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS ocr_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enabled INTEGER DEFAULT 1
                )""")
            self.settings_conn.commit()

            self.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings = self.settings_cursor.fetchone()
            save_captcha = getattr(self.bot, 'save_captcha', 0)

            if ocr_settings:
                enabled = ocr_settings[0]
                if enabled == 1:
                    self.logger.info("GiftOps __init__: OCR is enabled. Initializing ONNX solver...")
                    self.captcha_solver = GiftCaptchaSolver(save_images=save_captcha)
                    if not self.captcha_solver.is_initialized:
                        self.logger.error("GiftOps __init__: ONNX solver FAILED to initialize.")
                        self.captcha_solver = None
                    else:
                        self.logger.info("GiftOps __init__: ONNX solver initialized successfully.")
                else:
                    self.logger.info("GiftOps __init__: OCR is disabled in settings.")
            else:
                self.logger.warning("GiftOps __init__: No OCR settings found in DB. Inserting defaults.")
                self.settings_cursor.execute("INSERT INTO ocr_settings (enabled) VALUES (1)")
                self.settings_conn.commit()
                self.captcha_solver = GiftCaptchaSolver(save_images=save_captcha)
                if not self.captcha_solver.is_initialized:
                    self.logger.error("GiftOps __init__: ONNX solver FAILED to initialize with defaults.")
                    self.captcha_solver = None
                else:
                    self.logger.info("GiftOps __init__: ONNX solver initialized successfully.")

        except ImportError as lib_err:
            self.logger.exception(f"GiftOps __init__: Missing required library for OCR: {lib_err}. Captcha solving disabled.")
            self.captcha_solver = None
        except Exception as e:
            self.logger.exception(f"GiftOps __init__: Unexpected error during Captcha solver setup: {e}")
            self.captcha_solver = None

        # Test ID Settings Table
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_fid_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_fid TEXT NOT NULL
                )
            """)
            self.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.settings_cursor.fetchone()
            if not result:
                self.settings_cursor.execute("INSERT INTO test_fid_settings (test_fid) VALUES (?)", ("244886619",))
                self.settings_conn.commit()
                self.logger.info("Initialized default test ID (244886619) in database")
        except Exception as e:
            self.logger.exception(f"Error setting up test ID table: {e}")

    # ── Utility methods ─────────────────────────────────────────────────

    async def _execute_with_retry(self, operation, *args, max_retries=3, delay=0.1):
        for attempt in range(max_retries):
            try:
                return operation(*args)
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and attempt < max_retries - 1:
                    self.logger.warning(f"Database locked, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    delay *= 2
                else:
                    raise

    def cog_unload(self):
        if hasattr(self, 'periodic_validation_loop') and self.periodic_validation_loop.is_running():
            self.periodic_validation_loop.cancel()
        for conn_name in ['settings_conn', 'alliance_conn']:
            if hasattr(self, conn_name):
                try:
                    getattr(self, conn_name).close()
                except Exception:
                    pass

    def clean_gift_code(self, giftcode):
        import unicodedata
        cleaned = ''.join(char for char in giftcode if unicodedata.category(char)[0] != 'C')
        return cleaned.strip()

    # ── Event listeners ─────────────────────────────────────────────────

    @commands.Cog.listener()
    async def on_ready(self):
        self.logger.info("GiftOps Cog: on_ready triggered.")
        try:
            # Clean up old columns from ocr_settings if present
            try:
                self.logger.info("Checking ocr_settings table schema...")
                conn_info = sqlite3.connect('db/settings.sqlite')
                cursor_info = conn_info.cursor()
                cursor_info.execute("PRAGMA table_info(ocr_settings)")
                columns = [col[1] for col in cursor_info.fetchall()]
                columns_to_drop = []
                if 'use_gpu' in columns: columns_to_drop.append('use_gpu')
                if 'gpu_device' in columns: columns_to_drop.append('gpu_device')
                if 'save_images' in columns: columns_to_drop.append('save_images')

                if columns_to_drop:
                    sqlite_version = sqlite3.sqlite_version_info
                    if sqlite_version >= (3, 35, 0):
                        self.logger.info(f"Found old columns {columns_to_drop}. Attempting removal.")
                        for col_name in columns_to_drop:
                            try:
                                self.settings_cursor.execute(f"ALTER TABLE ocr_settings DROP COLUMN {col_name}")
                                self.logger.info(f"Successfully dropped column: {col_name}")
                            except Exception as drop_err:
                                self.logger.error(f"Error dropping column {col_name}: {drop_err}")
                        self.settings_conn.commit()
                    else:
                        self.logger.warning(f"Found old columns {columns_to_drop}, but SQLite {sqlite3.sqlite_version} doesn't support DROP COLUMN.")
                else:
                    self.logger.info("ocr_settings table schema is up to date.")
                conn_info.close()
            except Exception as schema_err:
                self.logger.error(f"Error during ocr_settings schema check/cleanup: {schema_err}")

            self.logger.info("Setting up ocr_settings table...")
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS ocr_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enabled INTEGER DEFAULT 1
                )
            """)
            self.settings_conn.commit()

            self.settings_cursor.execute("SELECT COUNT(*) FROM ocr_settings")
            count = self.settings_cursor.fetchone()[0]
            if count == 0:
                self.settings_cursor.execute("INSERT INTO ocr_settings (enabled) VALUES (1)")
                self.settings_conn.commit()

            if self.captcha_solver is None:
                self.logger.warning("Captcha solver not initialized in __init__, attempting again in on_ready...")
                self.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
                ocr_settings = self.settings_cursor.fetchone()
                if ocr_settings:
                    enabled = ocr_settings[0]
                    if enabled == 1:
                        try:
                            save_captcha = getattr(self.bot, 'save_captcha', 0)
                            self.captcha_solver = GiftCaptchaSolver(save_images=save_captcha)
                            if not self.captcha_solver.is_initialized:
                                self.captcha_solver = None
                        except Exception:
                            self.captcha_solver = None

            self.logger.info("Validating gift code channels...")
            self.cursor.execute("SELECT channel_id, alliance_id FROM giftcode_channel")
            channel_configs = self.cursor.fetchall()

            invalid_channels = []
            for channel_id, alliance_id in channel_configs:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    invalid_channels.append(channel_id)
                elif not isinstance(channel, discord.TextChannel):
                    invalid_channels.append(channel_id)
                elif not channel.permissions_for(channel.guild.me).send_messages:
                    self.logger.warning(f"Missing send message permissions in channel {channel_id}.")

            if invalid_channels:
                unique_invalid = list(set(invalid_channels))
                placeholders = ','.join('?' * len(unique_invalid))
                try:
                    self.cursor.execute(f"DELETE FROM giftcode_channel WHERE channel_id IN ({placeholders})", unique_invalid)
                    self.conn.commit()
                except sqlite3.Error as db_err:
                    self.logger.exception(f"DATABASE ERROR removing invalid channels: {db_err}")

            if not self.periodic_validation_loop.is_running():
                self.periodic_validation_loop.start()

            # Register handlers with the ProcessQueue cog
            process_queue_cog = self.bot.get_cog('ProcessQueue')
            if process_queue_cog:
                process_queue_cog.register_handler(
                    'gift_validate',
                    lambda process: gift_redemption.handle_gift_validate_process(self, process)
                )
                process_queue_cog.register_handler(
                    'gift_redeem',
                    lambda process: gift_redemption.handle_gift_redeem_process(self, process)
                )
                self.logger.info("GiftOps: Registered gift_validate and gift_redeem handlers with ProcessQueue")
            else:
                self.logger.error("GiftOps: ProcessQueue cog not found, gift code operations will not work")

            self.logger.info("GiftOps Cog: on_ready setup finished successfully.")

        except sqlite3.Error as db_err:
            self.logger.exception(f"DATABASE ERROR during on_ready setup: {db_err}")
        except Exception as e:
            self.logger.exception(f"UNEXPECTED ERROR during on_ready setup: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        try:
            if message.author.bot or not message.guild:
                return

            self.cursor.execute("SELECT alliance_id FROM giftcode_channel WHERE channel_id = ?", (message.channel.id,))
            channel_info = self.cursor.fetchone()
            if not channel_info:
                return

            content = message.content.strip()
            if not content:
                return

            giftcode = None
            if len(content.split()) == 1:
                if re.match(r'^[a-zA-Z0-9]+$', content):
                    giftcode = content
            else:
                code_match = re.search(r'Code:\s*(\S+)', content, re.IGNORECASE)
                if code_match:
                    giftcode = code_match.group(1)

            if giftcode:
                giftcode = self.clean_gift_code(giftcode)

            if not giftcode:
                return

            self.logger.info(f"GiftOps: [on_message] Detected potential code '{giftcode}' in channel {message.channel.id}")
            await gift_redemption.enqueue_validation(self, giftcode, "channel", message, message.channel)

        except Exception as e:
            self.logger.exception(f"GiftOps: UNEXPECTED Error in on_message handler: {str(e)}")
            traceback.print_exc()
            try:
                self.giftlog.info(f"on_message error: {e}\n{traceback.format_exc()}")
            except Exception:
                pass

    # ── Background task ─────────────────────────────────────────────────

    @tasks.loop(seconds=7200)
    async def periodic_validation_loop(self):
        await gift_redemption.periodic_validation_loop_body(self)

    @periodic_validation_loop.before_loop
    async def before_periodic_validation_loop(self):
        await gift_redemption.before_periodic_validation_loop_body(self)

    # ── Menu entry point ──────────────────────────────────────────────

    async def show_gift_menu(self, interaction: discord.Interaction):
        gift_menu_embed = discord.Embed(
            title=f"{theme.giftIcon} Gift Code Operations",
            description=(
                "Here you can manage everything related to gift code redemption.\n\n"
                "The bot automatically retrieves new gift codes from our distribution API. "
                f"Codes are validated periodically, and automatically removed if they become invalid.\n\n"
                f"If you're new here, you'll want to head to **Settings** and configure some things:\n"
                f"- If you want codes to be automatically redeemed, go to **Auto Redemption** and enable it.\n"
                f"- You can set up a channel via **Channel Management** where the bot will scan for new codes.\n"
                f"- You can also adjust the order in which alliances redeem gift codes via **Redemption Priority**.\n\n"
                f"**Available Operations**\n"
                f"{theme.upperDivider}\n"
                f"{theme.giftIcon} **Add Gift Code**\n"
                f"└ Manually input a new gift code\n\n"
                f"{theme.listIcon} **List Gift Codes**\n"
                f"└ View all active, valid codes\n\n"
                f"{theme.targetIcon} **Redeem Gift Code**\n"
                f"└ Redeem gift code(s) for one or more alliances\n\n"
                f"{theme.settingsIcon} **Settings**\n"
                f"└ Set up a gift code channel, configure auto redemption, and more...\n\n"
                f"{theme.deniedIcon} **Delete Gift Code**\n"
                f"└ Remove existing codes (rarely needed)\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )
        view = GiftView(self, interaction.user.id)
        await safe_edit_message(interaction, embed=gift_menu_embed, view=view)

    # ── Delegation to gift_redemption ─────────────────────────────────

    async def validate_gift_code_immediately(self, giftcode, source="unknown"):
        return await gift_redemption.validate_gift_code_immediately(self, giftcode, source)

    def encode_data(self, data):
        return gift_redemption.encode_data(self, data)

    def batch_insert_user_giftcodes(self, user_giftcode_data):
        return gift_redemption.batch_insert_user_giftcodes(self, user_giftcode_data)

    def batch_update_gift_codes_validation(self, giftcodes_to_validate):
        return gift_redemption.batch_update_gift_codes_validation(self, giftcodes_to_validate)

    def batch_get_user_giftcode_status(self, giftcode, fids):
        return gift_redemption.batch_get_user_giftcode_status(self, giftcode, fids)

    def mark_code_invalid(self, giftcode):
        return gift_redemption.mark_code_invalid(self, giftcode)

    def batch_process_alliance_results(self, results_batch):
        return gift_redemption.batch_process_alliance_results(self, results_batch)

    async def get_stove_info_wos(self, player_id):
        return await gift_redemption.get_stove_info_wos(self, player_id)

    async def attempt_gift_code_with_api(self, player_id, giftcode, session):
        return await gift_redemption.attempt_gift_code_with_api(self, player_id, giftcode, session)

    async def claim_giftcode_rewards_wos(self, player_id, giftcode):
        return await gift_redemption.claim_giftcode_rewards_wos(self, player_id, giftcode)

    async def scan_historical_messages(self, channel, alliance_id):
        return await gift_redemption.scan_historical_messages(self, channel, alliance_id)

    async def fetch_captcha(self, player_id, session=None):
        return await gift_redemption.fetch_captcha(self, player_id, session)

    async def use_giftcode_for_alliance(self, alliance_id, giftcode):
        MEMBER_PROCESS_DELAY = 1.0
        API_RATE_LIMIT_COOLDOWN = 60.0
        CAPTCHA_CYCLE_COOLDOWN = 60.0
        MAX_RETRY_CYCLES = 10

        self.logger.info(f"\nGiftOps: Starting use_giftcode_for_alliance for Alliance {alliance_id}, Code {giftcode}")

        try:
            # Initialize error tracking for summary
            error_summary = {}
            
            # Initial Setup (Get channel, alliance name)
            self.alliance_cursor.execute("SELECT channel_id FROM alliancesettings WHERE alliance_id = ?", (alliance_id,))
            channel_result = self.alliance_cursor.fetchone()
            self.alliance_cursor.execute("SELECT name FROM alliance_list WHERE alliance_id = ?", (alliance_id,))
            name_result = self.alliance_cursor.fetchone()

            if not channel_result or not name_result:
                self.logger.error(f"GiftOps: Could not find channel or name for alliance {alliance_id}.")
                return False
            
            channel_id, alliance_name = channel_result[0], name_result[0]
            channel = self.bot.get_channel(channel_id)

            if not channel:
                self.logger.error(f"GiftOps: Bot cannot access channel {channel_id} for alliance {alliance_name}.")
                return False

            # Check if OCR is enabled
            self.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings_row = self.settings_cursor.fetchone()
            ocr_enabled = ocr_settings_row[0] if ocr_settings_row else 0
            
            if not (ocr_enabled == 1 and self.captcha_solver):
                error_embed = discord.Embed(
                    title=f"{theme.deniedIcon} OCR/Captcha Solver Disabled",
                    description=(
                        f"**Gift Code:** `{giftcode}`\n"
                        f"**Alliance:** `{alliance_name}`\n\n"
                        f"{theme.warnIcon} Gift code redemption requires the OCR/captcha solver to be enabled.\n"
                        f"Please enable it first using the settings command."
                    ),
                    color=theme.emColor2
                )
                await channel.send(embed=error_embed)
                self.logger.info(f"GiftOps: Skipping alliance {alliance_id} - OCR disabled or solver not ready")
                return False

            # Check if this code has been validated before
            self.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
            master_code_status_row = self.cursor.fetchone()
            master_code_status = master_code_status_row[0] if master_code_status_row else None
            final_invalid_reason_for_embed = None

            if master_code_status == 'invalid':
                self.logger.info(f"GiftOps: Code {giftcode} is already marked as 'invalid' in the database.")
                final_invalid_reason_for_embed = "Code previously marked as invalid"
            else:
                # If not marked 'invalid' in master table, check with test ID if status is 'pending' or for other cached issues
                test_fid = self.get_test_fid()
                self.cursor.execute("SELECT status FROM user_giftcodes WHERE fid = ? AND giftcode = ?", (test_fid, giftcode))
                validation_fid_status_row = self.cursor.fetchone()

                if validation_fid_status_row:
                    fid_status = validation_fid_status_row[0]
                    if fid_status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                        self.logger.info(f"GiftOps: Code {giftcode} known to be invalid via test ID (status: {fid_status}). Marking invalid.")
                        self.mark_code_invalid(giftcode)
                        if hasattr(self, 'api') and self.api:
                            asyncio.create_task(self.api.remove_giftcode(giftcode, from_validation=True))
                        
                        reason_map_fid = {
                            "TIME_ERROR": "Code has expired (TIME_ERROR)",
                            "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                            "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                        }
                        final_invalid_reason_for_embed = reason_map_fid.get(fid_status, f"Code invalid ({fid_status})")

            if final_invalid_reason_for_embed:
                error_embed = discord.Embed(
                    title=f"{theme.deniedIcon} Gift Code Invalid",
                    description=(
                        f"**Gift Code Details**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                        f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                        f"{theme.deniedIcon} **Status:** {final_invalid_reason_for_embed}\n"
                        f"{theme.editListIcon} **Action:** Code status is 'invalid' in database\n"
                        f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"{theme.lowerDivider}\n"
                    ),
                    color=theme.emColor2
                )
                await channel.send(embed=error_embed)
                return False

            # Get Members
            with sqlite3.connect('db/users.sqlite') as users_conn:
                users_cursor = users_conn.cursor()
                users_cursor.execute("SELECT fid, nickname FROM users WHERE alliance = ?", (str(alliance_id),))
                members = users_cursor.fetchall()
            if not members:
                self.logger.info(f"GiftOps: No members found for alliance {alliance_id} ({alliance_name}).")
                return False

            total_members = len(members)
            self.logger.info(f"GiftOps: Found {total_members} members for {alliance_name}.")

            # Initialize State
            processed_count = 0
            success_count = 0
            received_count = 0
            failed_count = 0
            successful_users = []
            already_used_users = []
            failed_users_dict = {}

            retry_queue = []
            active_members_to_process = []
            
            # Batch Processing
            batch_results = []
            batch_size = 10

            # Check Cache & Populate Initial List
            member_ids = [m[0] for m in members]
            cached_member_statuses = self.batch_get_user_giftcode_status(giftcode, member_ids)

            for fid, nickname in members:
                if fid in cached_member_statuses:
                    status = cached_member_statuses[fid]
                    if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
                        received_count += 1
                        already_used_users.append(nickname)
                    processed_count += 1
                else:
                    active_members_to_process.append((fid, nickname, 0))
            self.logger.info(f"GiftOps: Pre-processed {len(cached_member_statuses)} members from cache. {len(active_members_to_process)} remaining.")

            # Progress Embed
            embed = discord.Embed(title=f"{theme.giftIcon} Gift Code Redemption: {giftcode}", color=theme.emColor1)
            def update_embed_description(include_errors=False):
                base_description = (
                    f"**Status for Alliance:** `{alliance_name}`\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.membersIcon} **Total Members:** `{total_members}`\n"
                    f"{theme.verifiedIcon} **Success:** `{success_count}`\n"
                    f"{theme.infoIcon} **Already Redeemed:** `{received_count}`\n"
                    f"{theme.refreshIcon} **Retrying:** `{len(retry_queue)}`\n"
                    f"{theme.deniedIcon} **Failed:** `{failed_count}`\n"
                    f"{theme.hourglassIcon} **Processed:** `{processed_count}/{total_members}`\n"
                    f"{theme.lowerDivider}\n"
                )
                
                if include_errors and failed_count > 0:
                    non_success_errors = {k: v for k, v in error_summary.items() if k != "SUCCESS"}
                    if non_success_errors:
                        # Define user-friendly messages for each error type
                        error_descriptions = {
                            "TOO_POOR_SPEND_MORE": f"{theme.warnIcon} **" + "{count}" + "** members failed to spend enough to reach VIP12.",
                            "TOO_SMALL_SPEND_MORE": f"{theme.warnIcon} **" + "{count}" + "** members failed due to insufficient furnace level.",
                            "TIMEOUT_RETRY": f"{theme.timeIcon} **" + "{count}" + "** members were staring into the void, until the void finally timed out on them.",
                            "LOGIN_EXPIRED_MID_PROCESS": f"{theme.lockIcon} **" + "{count}" + "** members login failed mid-process. How'd that even happen?",
                            "LOGIN_FAILED": f"{theme.lockIcon} **" + "{count}" + "** members failed due to login issues. Try logging it off and on again!",
                            "CAPTCHA_SOLVING_FAILED": f"{theme.robotIcon} **" + "{count}" + "** members lost the battle against CAPTCHA. You sure those weren't just bots?",
                            "CAPTCHA_SOLVER_ERROR": f"{theme.settingsIcon} **" + "{count}" + "** members failed due to a CAPTCHA solver issue. We're still trying to solve that one.",
                            "OCR_DISABLED": f"{theme.deniedIcon} **" + "{count}" + "** members failed since OCR is disabled. Try turning it on first!",
                            "SIGN_ERROR": f"{theme.lockIcon} **" + "{count}" + "** members failed due to a signature error. Something went wrong.",
                            "ERROR": f"{theme.deniedIcon} **" + "{count}" + "** members failed due to a general error. Might want to check the logs.",
                            "UNKNOWN_API_RESPONSE": f"{theme.infoIcon} **" + "{count}" + "** members failed with an unknown API response. Say what?",
                            "CONNECTION_ERROR": f"{theme.globeIcon} **" + "{count}" + "** members failed due to bot connection issues. Did the admin trip over the cable again?"
                        }
                        
                        base_description += "\n**Error Breakdown:**\n"
                        
                        # Build message for each error type
                        for error_type, count in sorted(non_success_errors.items(), key=lambda x: x[1], reverse=True):
                            if error_type in error_descriptions:
                                base_description += error_descriptions[error_type].format(count=count) + "\n"
                            else:
                                # Handle any unexpected error types
                                base_description += f"❗ **{count}** members failed with status: {error_type}\n"
                
                return base_description
            embed.description = update_embed_description()
            try: status_message = await channel.send(embed=embed)
            except Exception as e: self.logger.exception(f"GiftOps: Error sending initial status embed: {e}"); return False

            # Main Processing Loop
            last_embed_update = time.time()
            code_is_invalid = False

            while active_members_to_process or retry_queue:
                if code_is_invalid:
                    self.logger.info(f"GiftOps: Code {giftcode} detected as invalid, stopping redemption.")
                    break
                    
                current_time = time.time()

                # Dequeue Ready Retries
                ready_to_retry = []
                remaining_in_queue = []
                for item in retry_queue:
                    if current_time >= item[3]:
                        ready_to_retry.append(item[:3])
                    else:
                        remaining_in_queue.append(item)
                retry_queue = remaining_in_queue
                active_members_to_process.extend(ready_to_retry)

                if not active_members_to_process:
                    if retry_queue:
                        next_retry_ts = min(item[3] for item in retry_queue)
                        wait_time = max(0.1, next_retry_ts - current_time)
                        await asyncio.sleep(wait_time)
                    else:
                        break
                    continue

                # Process One Member
                fid, nickname, current_cycle_count = active_members_to_process.pop(0)

                self.logger.info(f"GiftOps: Processing ID {fid} ({nickname}), Cycle {current_cycle_count + 1}/{MAX_RETRY_CYCLES}")

                response_status = "ERROR"
                try:
                    await asyncio.sleep(random.uniform(MEMBER_PROCESS_DELAY * 0.7, MEMBER_PROCESS_DELAY * 1.3))
                    response_status = await self.claim_giftcode_rewards_wos(fid, giftcode)
                except Exception as claim_err:
                    self.logger.exception(f"GiftOps: Unexpected error during claim for {fid}: {claim_err}")
                    response_status = "ERROR"

                # Check if code is invalid
                if response_status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                    code_is_invalid = True
                    self.logger.info(f"GiftOps: Code {giftcode} became invalid (status: {response_status}) while processing {fid}. Marking as invalid in DB.")
                    
                    # Mark as invalid
                    self.mark_code_invalid(giftcode)
                    
                    if hasattr(self, 'api') and self.api:
                        asyncio.create_task(self.api.remove_giftcode(giftcode, from_validation=True))

                    reason_map_runtime = {
                        "TIME_ERROR": "Code has expired (TIME_ERROR)",
                        "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                        "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                    }
                    status_reason_runtime = reason_map_runtime.get(response_status, f"Code invalid ({response_status})")
                    
                    embed.title = f"{theme.deniedIcon} Gift Code Invalid: {giftcode}" 
                    embed.color = discord.Color.red()
                    embed.description = (
                        f"**Gift Code Redemption Halted**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                        f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                        f"{theme.deniedIcon} **Reason:** {status_reason_runtime}\n"
                        f"{theme.editListIcon} **Action:** Code marked as invalid in database. Remaining members for this alliance will not be processed.\n"
                        f"{theme.chartIcon} **Processed before halt:** {processed_count}/{total_members}\n"
                        f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"{theme.lowerDivider}\n"
                    )
                    embed.clear_fields()

                    try:
                        await status_message.edit(embed=embed)
                    except Exception as embed_edit_err:
                        self.logger.warning(f"GiftOps: Failed to update progress embed to show code invalidation: {embed_edit_err}")
                    
                    if fid not in failed_users_dict:
                        processed_count +=1 
                        failed_count +=1
                        failed_users_dict[fid] = (nickname, f"Led to code invalidation ({response_status})", current_cycle_count + 1)
                    continue
                
                if response_status == "SIGN_ERROR":
                    self.logger.error(f"GiftOps: Sign error detected (likely wrong encrypt key). Stopping redemption for alliance {alliance_id}.")
                    
                    embed.title = f"{theme.settingsIcon} Sign Error: {giftcode}"
                    embed.color = discord.Color.red()
                    embed.description = (
                        f"**Bot Configuration Error**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{giftcode}`\n"
                        f"{theme.allianceIcon} **Alliance:** `{alliance_name}`\n"
                        f"{theme.settingsIcon} **Reason:** Sign Error (check bot config/encrypt key)\n"
                        f"{theme.editListIcon} **Action:** Redemption stopped. Check bot configuration.\n"
                        f"{theme.chartIcon} **Processed before halt:** {processed_count}/{total_members}\n"
                        f"{theme.timeIcon} **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"{theme.lowerDivider}\n"
                    )
                    embed.clear_fields()
                    
                    try:
                        await status_message.edit(embed=embed)
                    except Exception as embed_edit_err:
                        self.logger.warning(f"GiftOps: Failed to update progress embed for sign error: {embed_edit_err}")

                    break

                # Handle Response
                mark_processed = False
                add_to_failed = False
                queue_for_retry = False
                retry_delay = 0

                if response_status == "SUCCESS":
                    success_count += 1
                    successful_users.append(nickname)
                    batch_results.append((fid, giftcode, response_status))
                    mark_processed = True
                elif response_status in ["RECEIVED", "SAME TYPE EXCHANGE"]:
                    received_count += 1
                    already_used_users.append(nickname)
                    batch_results.append((fid, giftcode, response_status))
                    mark_processed = True
                elif response_status == "OCR_DISABLED":
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = "OCR Disabled"
                    error_summary["OCR_DISABLED"] = error_summary.get("OCR_DISABLED", 0) + 1
                elif response_status in ["SOLVER_ERROR", "CAPTCHA_FETCH_ERROR"]:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Solver Error ({response_status})"
                    error_summary["CAPTCHA_SOLVER_ERROR"] = error_summary.get("CAPTCHA_SOLVER_ERROR", 0) + 1
                elif response_status in ["LOGIN_FAILED", "LOGIN_EXPIRED_MID_PROCESS", "ERROR", "UNKNOWN_API_RESPONSE"]:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Processing Error ({response_status})"
                    error_summary[response_status] = error_summary.get(response_status, 0) + 1
                elif response_status == "TIMEOUT_RETRY":
                    queue_for_retry = True
                    retry_delay = API_RATE_LIMIT_COOLDOWN
                    fail_reason = "API Rate Limited"
                    if current_cycle_count + 1 >= MAX_RETRY_CYCLES: # Track as error if this is the final attempt
                        error_summary["TIMEOUT_RETRY"] = error_summary.get("TIMEOUT_RETRY", 0) + 1
                elif response_status == "TOO_POOR_SPEND_MORE":
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = "VIP level too low"
                    error_summary["TOO_POOR_SPEND_MORE"] = error_summary.get("TOO_POOR_SPEND_MORE", 0) + 1
                elif response_status == "TOO_SMALL_SPEND_MORE":
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = "Furnace level too low"
                    error_summary["TOO_SMALL_SPEND_MORE"] = error_summary.get("TOO_SMALL_SPEND_MORE", 0) + 1
                elif response_status == "CAPTCHA_TOO_FREQUENT":
                    # Queue for retry with rate limit delay (60s max)
                    queue_for_retry = True
                    retry_delay = 60.0
                    fail_reason = "Captcha API rate limited (too frequent)"
                    self.logger.info(f"GiftOps: ID {fid} hit CAPTCHA_TOO_FREQUENT. Queuing for retry in {retry_delay:.1f}s.")
                    if current_cycle_count + 1 >= MAX_RETRY_CYCLES:
                        error_summary["CAPTCHA_TOO_FREQUENT"] = error_summary.get("CAPTCHA_TOO_FREQUENT", 0) + 1
                elif response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"]:
                    if current_cycle_count + 1 < MAX_RETRY_CYCLES:
                        queue_for_retry = True
                        retry_delay = CAPTCHA_CYCLE_COOLDOWN
                        fail_reason = "Captcha Cycle Failed"
                        self.logger.info(f"GiftOps: ID {fid} failed captcha cycle {current_cycle_count + 1}. Queuing for retry cycle {current_cycle_count + 2} in {retry_delay}s.")
                    else:
                        add_to_failed = True
                        mark_processed = True
                        fail_reason = f"Failed after {MAX_RETRY_CYCLES} captcha cycles (Last Status: {response_status})"
                        self.logger.info(f"GiftOps: Max ({MAX_RETRY_CYCLES}) retry cycles reached for ID {fid}. Marking as failed.")
                        # Track based on error type
                        if response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED"]:
                            error_summary["CAPTCHA_SOLVING_FAILED"] = error_summary.get("CAPTCHA_SOLVING_FAILED", 0) + 1
                        else:  # OCR_FAILED_ATTEMPT
                            error_summary["CAPTCHA_SOLVER_ERROR"] = error_summary.get("CAPTCHA_SOLVER_ERROR", 0) + 1
                else:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Unhandled status: {response_status}"
                    error_summary[response_status] = error_summary.get(response_status, 0) + 1

                # Update State Based on Outcome
                if mark_processed:
                    processed_count += 1
                    if add_to_failed:
                        failed_count += 1
                        cycle_failed_on = current_cycle_count + 1 if response_status not in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"] or (current_cycle_count + 1 >= MAX_RETRY_CYCLES) else MAX_RETRY_CYCLES
                        failed_users_dict[fid] = (nickname, fail_reason, cycle_failed_on)
                
                if queue_for_retry:
                    retry_after_ts = time.time() + retry_delay
                    cycle_for_next_retry = current_cycle_count + 1 if response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"] else current_cycle_count
                    retry_queue.append((fid, nickname, cycle_for_next_retry, retry_after_ts))
                
                # Batch process results when reaching batch size
                if len(batch_results) >= batch_size:
                    self.batch_process_alliance_results(batch_results)
                    batch_results = []

                # Update Embed Periodically
                current_time = time.time()
                if current_time - last_embed_update > 5 and not code_is_invalid:
                    embed.description = update_embed_description()
                    try:
                        await status_message.edit(embed=embed)
                        last_embed_update = current_time
                    except Exception as embed_edit_err:
                        self.logger.warning(f"GiftOps: WARN - Failed to edit progress embed: {embed_edit_err}")

            # Final Embed Update
            if not code_is_invalid:
                self.logger.info(f"GiftOps: Alliance {alliance_id} processing loop finished. Preparing final update.")
                final_title = f"{theme.giftIcon} Gift Code Process Complete: {giftcode}"
                final_color = discord.Color.green() if failed_count == 0 and total_members > 0 else \
                              discord.Color.orange() if success_count > 0 or received_count > 0 else \
                              discord.Color.red()
                if total_members == 0:
                    final_title = f"{theme.infoIcon} No Members to Process for Code: {giftcode}"
                    final_color = discord.Color.light_grey()

                embed.title = final_title
                embed.color = final_color
                embed.description = update_embed_description(include_errors=True)

                try:
                    await status_message.edit(embed=embed)
                    self.logger.info(f"GiftOps: Successfully edited final status embed for alliance {alliance_id}.")
                except discord.NotFound:
                    self.logger.warning(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: Original message not found.")
                except discord.Forbidden:
                    self.logger.warning(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: Missing permissions.")
                except Exception as final_embed_err:
                    self.logger.exception(f"GiftOps: WARN - Failed to edit final progress embed for alliance {alliance_id}: {final_embed_err}")

            summary_lines = [
                "\n",
                "--- Redemption Summary Start ---",
                f"Alliance: {alliance_name} ({alliance_id})",
                f"Gift Code: {giftcode}",
            ]
            try:
                master_status_log = self.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,)).fetchone()
                summary_lines.append(f"Master Code Status at Log Time: {master_status_log[0] if master_status_log else 'NOT_FOUND_IN_DB'}")
            except Exception as e_log:
                summary_lines.append(f"Master Code Status at Log Time: Error fetching - {e_log}")

            summary_lines.extend([
                f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "------------------------",
                f"Total Members: {total_members}",
                f"Successful: {success_count}",
                f"Already Redeemed: {received_count}",
                f"Failed: {failed_count}",
                "------------------------",
            ])

            if successful_users:
                summary_lines.append(f"\nSuccessful Users ({len(successful_users)}):")
                summary_lines.extend(successful_users)

            if already_used_users:
                summary_lines.append(f"\nAlready Redeemed Users ({len(already_used_users)}):")
                summary_lines.extend(already_used_users)

            final_failed_log_details = []
            if code_is_invalid and retry_queue:
                 for f_fid, f_nick, f_cycle, _ in retry_queue:
                     if f_fid not in failed_users_dict:
                         final_failed_log_details.append(f"- {f_nick} ({f_fid}): Halted in retry (Next Cycle: {f_cycle})")
            
            for fid_failed, (nick_failed, reason_failed, cycles_attempted) in failed_users_dict.items():
                final_failed_log_details.append(f"- {nick_failed} ({fid_failed}): {reason_failed} (Cycles Attempted: {cycles_attempted})")
            
            if final_failed_log_details:
                summary_lines.append(f"\nFailed Users ({len(final_failed_log_details)}):")
                summary_lines.extend(final_failed_log_details)

            summary_lines.append("--- Redemption Summary End ---\n")
            summary_log_message = "\n".join(summary_lines)
            self.logger.info(summary_log_message)
            
            # Process any remaining batch results
            if batch_results:
                self.batch_process_alliance_results(batch_results)
                batch_results = []
            
            return True
        
        except Exception as e:
            self.logger.exception(f"GiftOps: UNEXPECTED ERROR in use_giftcode_for_alliance for {alliance_id}/{giftcode}: {str(e)}")
            self.logger.exception(f"Traceback: {traceback.format_exc()}")
            try:
                if 'channel' in locals() and channel: await channel.send(f"{theme.warnIcon} An unexpected error occurred processing `{giftcode}` for {alliance_name}.")
            except Exception: pass
            return False

class CreateGiftCodeModal(discord.ui.Modal):
    def __init__(self, cog):
        super().__init__(title="Create Gift Code")
        self.cog = cog
        
        self.giftcode = discord.ui.TextInput(
            label="Gift Code",
            placeholder="Enter the gift code",
            required=True,
            min_length=4,
            max_length=20
        )
        self.add_item(self.giftcode)
    
    async def on_submit(self, interaction: discord.Interaction):
        logger = self.cog.logger
        await interaction.response.defer(ephemeral=True)

        code = self.cog.clean_gift_code(self.giftcode.value)
        logger.info(f"[CreateGiftCodeModal] Code entered: {code}")
        final_embed = discord.Embed(title=f"{theme.giftIcon} Gift Code Creation Result")

        # Check if code already exists
        self.cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
        if self.cog.cursor.fetchone():
            logger.info(f"[CreateGiftCodeModal] Code {code} already exists in DB.")
            final_embed.title = f"{theme.infoIcon} Gift Code Exists"
            final_embed.description = (
                f"**Gift Code Details**\n{theme.upperDivider}\n"
                f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                f"{theme.verifiedIcon} **Status:** Code already exists in database.\n"
                f"{theme.lowerDivider}\n"
            )
            final_embed.color = discord.Color.blue()
        else: # Validate the code immediately
            logger.info(f"[CreateGiftCodeModal] Validating code {code} before adding to DB.")
            
            validation_embed = discord.Embed(
                title=f"{theme.refreshIcon} Validating Gift Code...",
                description=f"Checking if `{code}` is valid...",
                color=theme.emColor1
            )
            await interaction.edit_original_response(embed=validation_embed)
            
            is_valid, validation_msg = await self.cog.validate_gift_code_immediately(code, "button")
            
            if is_valid: # Valid code - send to API and add to DB
                logger.info(f"[CreateGiftCodeModal] Code '{code}' validated successfully.")

                if hasattr(self.cog, 'api') and self.cog.api:
                    asyncio.create_task(self.cog.api.add_giftcode(code))

                await self.cog._process_auto_use(code)

                self.cog.cursor.execute("SELECT COUNT(*) FROM giftcodecontrol WHERE status = 1")
                auto_count = self.cog.cursor.fetchone()[0]

                final_embed.title = f"{theme.verifiedIcon} Gift Code Validated"
                final_embed.description = (
                    f"**Gift Code Details**\n{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                    f"{theme.verifiedIcon} **Status:** {validation_msg}\n"
                    f"{theme.editListIcon} **Action:** Added to database and sent to API\n"
                    f"{theme.refreshIcon} **Auto-redemption:** {'Queued for ' + str(auto_count) + ' alliance(s)' if auto_count else 'Disabled'}\n"
                    f"{theme.lowerDivider}\n"
                )
                final_embed.color = discord.Color.green()
                
            elif is_valid is False: # Invalid code - do not add
                logger.warning(f"[CreateGiftCodeModal] Code '{code}' is invalid: {validation_msg}")
                
                final_embed.title = f"{theme.deniedIcon} Invalid Gift Code"
                final_embed.description = (
                    f"**Gift Code Details**\n{theme.upperDivider}\n"
                    f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                    f"{theme.deniedIcon} **Status:** {validation_msg}\n"
                    f"{theme.editListIcon} **Action:** Code not added to database\n"
                    f"{theme.lowerDivider}\n"
                )
                final_embed.color = discord.Color.red()
                
            else: # Validation inconclusive - add as pending
                logger.warning(f"[CreateGiftCodeModal] Code '{code}' validation inconclusive: {validation_msg}")
                
                try:
                    date = datetime.now().strftime("%Y-%m-%d")
                    self.cog.cursor.execute(
                        "INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                        (code, date, "pending")
                    )
                    self.cog.conn.commit()
                    
                    final_embed.title = f"{theme.warnIcon} Gift Code Added (Pending)"
                    final_embed.description = (
                        f"**Gift Code Details**\n{theme.upperDivider}\n"
                        f"{theme.giftIcon} **Gift Code:** `{code}`\n"
                        f"{theme.warnIcon} **Status:** {validation_msg}\n"
                        f"{theme.editListIcon} **Action:** Added for later validation\n"
                        f"{theme.lowerDivider}\n"
                    )
                    final_embed.color = discord.Color.yellow()
                    
                except sqlite3.Error as db_err:
                    logger.exception(f"[CreateGiftCodeModal] DB Error inserting code '{code}': {db_err}")
                    final_embed.title = f"{theme.deniedIcon} Database Error"
                    final_embed.description = f"Failed to save gift code `{code}` to the database. Please check logs."
                    final_embed.color = discord.Color.red()

        try:
            await interaction.edit_original_response(embed=final_embed)
            logger.info(f"[CreateGiftCodeModal] Final result embed sent for code {code}.")
        except Exception as final_edit_err:
            logger.exception(f"[CreateGiftCodeModal] Failed to edit interaction with final result for {code}: {final_edit_err}")

class DeleteGiftCodeModal(discord.ui.Modal, title="Delete Gift Code"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        
    giftcode = discord.ui.TextInput(
        label="Gift Code",
        placeholder="Enter the gift code to delete",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        code = self.giftcode.value
        
        self.cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
        if not self.cog.cursor.fetchone():
            await interaction.response.send_message(
                f"{theme.deniedIcon} Gift code not found!",
                ephemeral=True
            )
            return
            
        self.cog.cursor.execute("DELETE FROM gift_codes WHERE giftcode = ?", (code,))
        self.cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (code,))
        self.cog.conn.commit()
        
        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Gift Code Deleted",
            description=f"Gift code `{code}` has been deleted successfully.",
            color=theme.emColor3
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

class TestIDModal(discord.ui.Modal, title="Change Test ID"):
    def __init__(self, cog):
        super().__init__()
        self.cog = cog
        
        try:
            self.cog.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.cog.settings_cursor.fetchone()
            current_fid = result[0] if result else "244886619"
        except Exception:
            current_fid = "244886619"
        
        self.test_fid = discord.ui.TextInput(
            label="Enter New Player ID",
            placeholder="Example: 244886619",
            default=current_fid,
            required=True,
            min_length=1,
            max_length=20
        )
        self.add_item(self.test_fid)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Defer the response since we'll make an API call to validate
            await interaction.response.defer(ephemeral=True)
            
            new_fid = self.test_fid.value.strip()
            
            if not new_fid.isdigit():
                await interaction.followup.send(f"{theme.deniedIcon} Invalid ID format. Please enter a numeric ID.", ephemeral=True)
                return
            
            is_valid, message = await self.cog.verify_test_fid(new_fid)
            
            if is_valid:
                success = await self.cog.update_test_fid(new_fid)
                
                if success:
                    embed = discord.Embed(
                        title=f"{theme.verifiedIcon} Test ID Updated",
                        description=(
                            f"**Test ID Configuration**\n"
                            f"{theme.upperDivider}\n"
                            f"{theme.fidIcon} **ID:** `{new_fid}`\n"
                            f"{theme.verifiedIcon} **Status:** Validated\n"
                            f"{theme.editListIcon} **Action:** Updated in database\n"
                            f"{theme.lowerDivider}\n"
                        ),
                        color=theme.emColor3
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    
                    await self.cog.show_ocr_settings(interaction)
                else:
                    await interaction.followup.send(f"{theme.deniedIcon} Failed to update test ID in database. Check logs for details.", ephemeral=True)
            else:
                embed = discord.Embed(
                    title=f"{theme.deniedIcon} Invalid Test ID",
                    description=(
                        f"**Test ID Validation**\n"
                        f"{theme.upperDivider}\n"
                        f"{theme.fidIcon} **ID:** `{new_fid}`\n"
                        f"{theme.deniedIcon} **Status:** Invalid ID\n"
                        f"{theme.editListIcon} **Reason:** {message}\n"
                        f"{theme.lowerDivider}\n"
                    ),
                    color=theme.emColor2
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                
        except Exception as e:
            self.cog.logger.exception(f"Error updating test ID: {e}")
            await interaction.followup.send(f"{theme.deniedIcon} An error occurred: {str(e)}", ephemeral=True)

class GiftView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=7200)
        self.cog = cog

    @discord.ui.button(
        label="Add Gift Code",
        style=discord.ButtonStyle.green,
        custom_id="create_gift",
        emoji=f"{theme.giftIcon}",
        row=0
    )
    async def create_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.create_gift_code(interaction)

    @discord.ui.button(
        label="List Gift Codes",
        style=discord.ButtonStyle.blurple,
        custom_id="list_gift",
        emoji=f"{theme.listIcon}",
        row=0
    )
    async def list_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.list_gift_codes(interaction)

    @discord.ui.button(
        label="Redeem Gift Code",
        emoji=f"{theme.targetIcon}",
        style=discord.ButtonStyle.primary,
        custom_id="use_gift_alliance",
        row=0
    )
    async def use_gift_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            admin_info = await self.cog.get_admin_info(interaction.user.id)
            if not admin_info:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} You are not authorized to perform this action.",
                    ephemeral=True
                )
                return

            available_alliances = await self.cog.get_available_alliances(interaction)
            if not available_alliances:
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title=f"{theme.deniedIcon} No Available Alliances",
                        description="You don't have access to any alliances.",
                        color=theme.emColor2
                    ),
                    ephemeral=True
                )
                return

            alliances_with_counts = []
            for alliance_id, name in available_alliances:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

            alliance_embed = discord.Embed(
                title=f"{theme.targetIcon} Redeem Gift Code",
                description=(
                    f"Select an alliance to use gift code:\n\n"
                    f"**Alliance List**\n"
                    f"{theme.upperDivider}\n"
                    f"Select an alliance from the list below:\n"
                ),
                color=theme.emColor1
            )

            view = AllianceSelectView(alliances_with_counts, self.cog, context="giftcode")

            view.current_select.options.insert(0, discord.SelectOption(
                label="ALL ALLIANCES",
                value="all",
                description=f"Apply to all {len(alliances_with_counts)} alliances",
                emoji=theme.globeIcon
            ))

            async def alliance_callback(select_interaction: discord.Interaction, alliance_id=None):
                try:
                    # If alliance_id is provided (from ID search modal), use it directly
                    if alliance_id is not None:
                        selected_value = str(alliance_id)
                    else:
                        selected_value = view.current_select.values[0]

                    if selected_value == "all":
                        # Get alliances ordered by priority
                        alliance_ids = [aid for aid, _, _ in alliances_with_counts]
                        placeholders = ','.join('?' * len(alliance_ids))
                        self.cog.cursor.execute(f"""
                            SELECT alliance_id FROM giftcodecontrol
                            WHERE alliance_id IN ({placeholders})
                            ORDER BY priority ASC, alliance_id ASC
                        """, alliance_ids)
                        prioritized = [row[0] for row in self.cog.cursor.fetchall()]
                        # Add any alliances not in giftcodecontrol at the end, ordered by ID
                        remaining = sorted([aid for aid in alliance_ids if aid not in prioritized])
                        all_alliances = prioritized + remaining
                    else:
                        alliance_id = int(selected_value)
                        all_alliances = [alliance_id]
                    
                    self.cog.cursor.execute("""
                        SELECT giftcode, date FROM gift_codes
                        WHERE validation_status != 'invalid'
                        ORDER BY date DESC
                    """)
                    gift_codes = self.cog.cursor.fetchall()

                    if not gift_codes:
                        await select_interaction.response.edit_message(
                            content="No active gift codes available.",
                            embed=None,
                            view=None
                        )
                        return

                    giftcode_embed = discord.Embed(
                        title=f"{theme.giftIcon} Select Gift Code",
                        description=(
                            f"Select a gift code to use:\n\n"
                            f"**Gift Code List**\n"
                            f"{theme.upperDivider}\n"
                            f"Select a gift code from the list below:\n"
                        ),
                        color=theme.emColor1
                    )

                    select_giftcode = discord.ui.Select(
                        placeholder="Select a gift code",
                        options=[
                            discord.SelectOption(
                                label=f"Code: {code}",
                                value=code,
                                description=f"Created: {date}",
                                emoji=theme.giftIcon
                            ) for code, date in gift_codes
                        ]
                    )

                    # Add ALL CODES option at the beginning
                    select_giftcode.options.insert(0, discord.SelectOption(
                        label="ALL CODES",
                        value="all_codes",
                        description=f"Redeem all {len(gift_codes)} active codes",
                        emoji=theme.packageIcon
                    ))

                    async def giftcode_callback(giftcode_interaction: discord.Interaction):
                        try:
                            selected_code_value = giftcode_interaction.data["values"][0]

                            # Handle ALL CODES selection
                            if selected_code_value == "all_codes":
                                selected_codes = [code for code, date in gift_codes]
                                code_display = f"ALL ({len(selected_codes)} codes)"
                            else:
                                selected_codes = [selected_code_value]
                                code_display = f"`{selected_code_value}`"

                            alliance_display = 'ALL' if selected_value == 'all' else next((name for aid, name, _ in alliances_with_counts if aid == alliance_id), 'Unknown')
                            total_redemptions = len(selected_codes) * len(all_alliances)

                            confirm_embed = discord.Embed(
                                title=f"{theme.warnIcon} Confirm Gift Code Usage",
                                description=(
                                    f"Are you sure you want to use {'these gift codes' if len(selected_codes) > 1 else 'this gift code'}?\n\n"
                                    f"**Details**\n"
                                    f"{theme.upperDivider}\n"
                                    f"{theme.giftIcon} **Gift Code{'s' if len(selected_codes) > 1 else ''}:** {code_display}\n"
                                    f"{theme.allianceIcon} **Alliances:** {alliance_display} ({len(all_alliances)})\n"
                                    f"{theme.chartIcon} **Total redemptions:** {total_redemptions}\n"
                                    f"{theme.lowerDivider}\n"
                                ),
                                color=discord.Color.yellow()
                            )

                            confirm_view = discord.ui.View()
                            
                            async def confirm_callback(button_interaction: discord.Interaction):
                                try:
                                    # Defer first so followup.send works for batch progress
                                    await button_interaction.response.defer()

                                    await self.cog.add_manual_redemption_to_queue(
                                        selected_codes, all_alliances, button_interaction
                                    )

                                    queue_status = await self.cog.get_queue_status()

                                    alliance_names = []
                                    for aid in all_alliances[:3]:  # Show first 3 alliance names
                                        name = next((n for a_id, n, _ in alliances_with_counts if a_id == aid), 'Unknown')
                                        alliance_names.append(name)

                                    alliance_list = ", ".join(alliance_names)
                                    if len(all_alliances) > 3:
                                        alliance_list += f" and {len(all_alliances) - 3} more"

                                    queue_summary = []
                                    your_position = None

                                    for code, items in queue_status['queue_by_code'].items():
                                        alliance_count = len([i for i in items if i.get('alliance_id')])

                                        if code in selected_codes and your_position is None:
                                            your_position = min(i['position'] for i in items)

                                        queue_summary.append(f"• `{code}` - {alliance_count} alliance{'s' if alliance_count != 1 else ''}")

                                    queue_info = "\n".join(queue_summary) if queue_summary else "Queue is empty"

                                    queue_embed = discord.Embed(
                                        title=f"{theme.verifiedIcon} Redemptions Queued Successfully",
                                        description=(
                                            f"Gift code redemptions added to the queue.\n\n"
                                            f"**Your Redemption**\n"
                                            f"{theme.upperDivider}\n"
                                            f"{theme.giftIcon} **Gift Code{'s' if len(selected_codes) > 1 else ''}:** {code_display}\n"
                                            f"{theme.allianceIcon} **Alliances:** {alliance_list}\n"
                                            f"{theme.chartIcon} **Total redemptions:** {len(selected_codes) * len(all_alliances)}\n"
                                            f"{theme.lowerDivider}\n\n"
                                            f"**Full Queue Details**\n"
                                            f"{queue_info}\n\n"
                                            f"{theme.chartIcon} **Total items in queue:** {queue_status['queue_length']}\n"
                                            f"{theme.pinIcon} **Your position:** #{your_position if your_position else 'Processing'}\n\n"
                                            f"{theme.infoIcon} You'll receive notifications as each alliance is processed."
                                        ),
                                        color=theme.emColor3
                                    )
                                    queue_embed.set_footer(text="Gift codes are processed sequentially to prevent issues.")

                                    await button_interaction.edit_original_response(
                                        embed=queue_embed,
                                        view=None
                                    )

                                except Exception as e:
                                    self.logger.exception(f"Error queueing gift code redemptions: {e}")
                                    await button_interaction.followup.send(
                                        f"{theme.deniedIcon} An error occurred while queueing the gift code redemptions.",
                                        ephemeral=True
                                    )

                            async def cancel_callback(button_interaction: discord.Interaction):
                                cancel_embed = discord.Embed(
                                    title=f"{theme.deniedIcon} Operation Cancelled",
                                    description="The gift code usage has been cancelled.",
                                    color=theme.emColor2
                                )
                                await button_interaction.response.edit_message(
                                    embed=cancel_embed,
                                    view=None
                                )

                            confirm_button = discord.ui.Button(
                                label="Confirm",
                                style=discord.ButtonStyle.success,
                                emoji=f"{theme.verifiedIcon}"
                            )
                            cancel_button = discord.ui.Button(
                                label="Cancel",
                                style=discord.ButtonStyle.danger,
                                emoji=f"{theme.deniedIcon}"
                            )

                            confirm_button.callback = confirm_callback
                            cancel_button.callback = cancel_callback

                            confirm_view.add_item(confirm_button)
                            confirm_view.add_item(cancel_button)

                            await giftcode_interaction.response.edit_message(
                                embed=confirm_embed,
                                view=confirm_view
                            )
                        except Exception as e:
                            self.logger.exception(f"Gift code callback error: {e}")
                            await giftcode_interaction.response.send_message(
                                f"{theme.deniedIcon} An error occurred while processing the gift code.",
                                ephemeral=True
                            )

                    select_giftcode.callback = giftcode_callback
                    giftcode_view = discord.ui.View()
                    giftcode_view.add_item(select_giftcode)

                    await select_interaction.response.edit_message(
                        embed=giftcode_embed,
                        view=giftcode_view
                    )
                except Exception as e:
                    self.logger.exception(f"Alliance callback error: {e}")
                    await select_interaction.response.send_message(
                        f"{theme.deniedIcon} An error occurred while processing the alliance selection.",
                        ephemeral=True
                    )

            view.current_select.callback = alliance_callback
            await interaction.response.send_message(
                embed=alliance_embed,
                view=view,
                ephemeral=True
            )
        except Exception as e:
            self.logger.exception(f"Use gift alliance button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing the request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Settings",
        style=discord.ButtonStyle.secondary,
        custom_id="gift_code_settings",
        emoji=f"{theme.settingsIcon}",
        row=1
    )
    async def gift_code_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_settings_menu(interaction)

    @discord.ui.button(
        label="Delete Gift Code",
        emoji=f"{theme.trashIcon}",
        style=discord.ButtonStyle.danger,
        custom_id="delete_gift",
        row=1
    )
    async def delete_gift_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.delete_gift_code(interaction)
        except Exception as e:
            self.logger.exception(f"Delete gift button error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} An error occurred while processing delete request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Main Menu",
        emoji=f"{theme.homeIcon}",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu",
        row=2
    )
    async def main_menu_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            alliance_cog = self.cog.bot.get_cog("Alliance")
            if alliance_cog:
                try:
                    await interaction.message.edit(content=None, embed=None, view=None)
                except:
                    pass
                await alliance_cog.show_main_menu(interaction)
        except:
            pass

class SettingsMenuView(discord.ui.View):
    def __init__(self, cog, is_global: bool = False):
        super().__init__(timeout=7200)
        self.cog = cog
        self.is_global = is_global

        # Disable global-admin-only buttons for non-global admins
        if not is_global:
            for child in self.children:
                if isinstance(child, discord.ui.Button) and child.label in [
                    "Redemption Priority", "CAPTCHA Settings"
                ]:
                    child.disabled = True

    @discord.ui.button(
        label="Channel Management",
        style=discord.ButtonStyle.green,
        custom_id="channel_management",
        emoji=f"{theme.announceIcon}",
        row=0
    )
    async def channel_management_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.manage_channel_settings(interaction)

    @discord.ui.button(
        label="Automatic Redemption",
        style=discord.ButtonStyle.primary,
        custom_id="auto_gift_settings",
        emoji=f"{theme.giftIcon}",
        row=0
    )
    async def auto_gift_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.setup_giftcode_auto(interaction)

    @discord.ui.button(
        label="Redemption Priority",
        style=discord.ButtonStyle.primary,
        custom_id="redemption_priority",
        emoji=f"{theme.chartIcon}",
        row=0
    )
    async def redemption_priority_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_redemption_priority(interaction)

    @discord.ui.button(
        label="Channel History Scan",
        style=discord.ButtonStyle.secondary,
        custom_id="channel_history_scan",
        emoji=f"{theme.searchIcon}",
        row=1
    )
    async def channel_history_scan_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.channel_history_scan(interaction)

    @discord.ui.button(
        label="CAPTCHA Settings",
        style=discord.ButtonStyle.secondary,
        custom_id="captcha_settings",
        emoji=f"{theme.settingsIcon}",
        row=1
    )
    async def captcha_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_ocr_settings(interaction)

    @discord.ui.button(
        label="Back",
        style=discord.ButtonStyle.secondary,
        custom_id="back_to_main",
        emoji=f"{theme.backIcon}",
        row=2
    )
    async def back_to_main_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_gift_menu(interaction)

class RedemptionPriorityView(discord.ui.View):
    def __init__(self, cog, alliances_with_priority):
        super().__init__(timeout=7200)
        self.cog = cog
        self.alliances = alliances_with_priority  # List of (alliance_id, name, priority)
        self.selected_alliance_id = None

        # Alliance select menu
        options = [
            discord.SelectOption(
                label=f"{idx}. {name}",
                value=str(alliance_id),
                description=f"Priority position {idx}"
            )
            for idx, (alliance_id, name, _) in enumerate(self.alliances, 1)
        ]

        if options:
            self.alliance_select = discord.ui.Select(
                placeholder="Select an alliance to move",
                options=options[:25],  # Discord limit
                row=0
            )
            self.alliance_select.callback = self.alliance_select_callback
            self.add_item(self.alliance_select)

    async def alliance_select_callback(self, interaction: discord.Interaction):
        self.selected_alliance_id = int(self.alliance_select.values[0])

        # Update embed to show selected alliance with marker
        embed = discord.Embed(
            title=f"{theme.chartIcon} Redemption Priority",
            description="Configure the order in which alliances receive gift codes.\nSelect an alliance and use the buttons to change its position.",
            color=theme.emColor1
        )

        priority_list = []
        for idx, (alliance_id, name, _) in enumerate(self.alliances, 1):
            marker = " ◀" if alliance_id == self.selected_alliance_id else ""
            priority_list.append(f"`{idx}.` **{name}**{marker}")

        embed.add_field(
            name="Current Priority Order",
            value="\n".join(priority_list) if priority_list else "No alliances configured",
            inline=False
        )

        await interaction.response.edit_message(embed=embed, view=self)

    @discord.ui.button(label="Move Up", style=discord.ButtonStyle.primary, emoji=f"{theme.upIcon}", row=1)
    async def move_up_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alliance_id:
            await interaction.response.send_message("Please select an alliance first.", ephemeral=True)
            return

        # Find current position
        current_idx = next((i for i, (aid, _, _) in enumerate(self.alliances) if aid == self.selected_alliance_id), None)
        if current_idx is None or current_idx == 0:
            await interaction.response.send_message("Alliance is already at the top.", ephemeral=True)
            return

        # Swap with the alliance above
        await self._swap_priorities(current_idx, current_idx - 1)
        await self._refresh_view(interaction)

    @discord.ui.button(label="Move Down", style=discord.ButtonStyle.primary, emoji=f"{theme.downIcon}", row=1)
    async def move_down_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_alliance_id:
            await interaction.response.send_message("Please select an alliance first.", ephemeral=True)
            return

        # Find current position
        current_idx = next((i for i, (aid, _, _) in enumerate(self.alliances) if aid == self.selected_alliance_id), None)
        if current_idx is None or current_idx >= len(self.alliances) - 1:
            await interaction.response.send_message("Alliance is already at the bottom.", ephemeral=True)
            return

        # Swap with the alliance below
        await self._swap_priorities(current_idx, current_idx + 1)
        await self._refresh_view(interaction)

    @discord.ui.button(label="Done", style=discord.ButtonStyle.secondary, emoji=f"{theme.verifiedIcon}", row=1)
    async def done_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            embed=discord.Embed(
                title=f"{theme.chartIcon} Priority Updated",
                description="Redemption priority order has been saved.",
                color=theme.emColor3
            ),
            view=None
        )

    async def _swap_priorities(self, idx1, idx2):
        """Swap the priorities of two alliances in the list and database."""
        alliance1_id, name1, priority1 = self.alliances[idx1]
        alliance2_id, name2, priority2 = self.alliances[idx2]

        # Assign new sequential priorities based on position
        new_priority1 = idx2 + 1
        new_priority2 = idx1 + 1

        # Update database
        self.cog.cursor.execute("""
            INSERT INTO giftcodecontrol (alliance_id, status, priority)
            VALUES (?, 0, ?)
            ON CONFLICT(alliance_id) DO UPDATE SET priority = excluded.priority
        """, (alliance1_id, new_priority1))

        self.cog.cursor.execute("""
            INSERT INTO giftcodecontrol (alliance_id, status, priority)
            VALUES (?, 0, ?)
            ON CONFLICT(alliance_id) DO UPDATE SET priority = excluded.priority
        """, (alliance2_id, new_priority2))

        self.cog.conn.commit()

        # Swap in local list
        self.alliances[idx1] = (alliance1_id, name1, new_priority1)
        self.alliances[idx2] = (alliance2_id, name2, new_priority2)
        self.alliances[idx1], self.alliances[idx2] = self.alliances[idx2], self.alliances[idx1]

    async def _refresh_view(self, interaction: discord.Interaction):
        """Refresh the embed and view after a priority change."""
        # Rebuild embed
        embed = discord.Embed(
            title=f"{theme.chartIcon} Redemption Priority",
            description="Configure the order in which alliances receive gift codes.\nSelect an alliance and use the buttons to change its position.",
            color=theme.emColor1
        )

        priority_list = []
        for idx, (alliance_id, name, _) in enumerate(self.alliances, 1):
            marker = " ◀" if alliance_id == self.selected_alliance_id else ""
            priority_list.append(f"`{idx}.` **{name}**{marker}")

        embed.add_field(
            name="Current Priority Order",
            value="\n".join(priority_list) if priority_list else "No alliances configured",
            inline=False
        )

        # Rebuild select options
        options = [
            discord.SelectOption(
                label=f"{idx}. {name}",
                value=str(alliance_id),
                description=f"Priority position {idx}"
            )
            for idx, (alliance_id, name, _) in enumerate(self.alliances, 1)
        ]

        if options:
            self.alliance_select.options = options[:25]

        await interaction.response.edit_message(embed=embed, view=self)

class ClearCacheConfirmView(discord.ui.View):
    def __init__(self, parent_cog):
        super().__init__(timeout=60)
        self.parent_cog = parent_cog

    @discord.ui.button(label="Confirm Clear", style=discord.ButtonStyle.danger, emoji=f"{theme.verifiedIcon}")
    async def confirm_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        try: # Clear the user_giftcodes table
            self.parent_cog.cursor.execute("DELETE FROM user_giftcodes")
            deleted_count = self.parent_cog.cursor.rowcount
            self.parent_cog.conn.commit()
            
            success_embed = discord.Embed(
                title=f"{theme.verifiedIcon} Redemption Cache Cleared",
                description=f"Successfully deleted {deleted_count:,} redemption records.\n\nUsers can now attempt to redeem gift codes again.",
                color=theme.emColor3
            )
            
            self.parent_cog.logger.info(f"Redemption cache cleared by user {interaction.user.id}: {deleted_count} records deleted")
            
            await interaction.response.edit_message(embed=success_embed, view=None)
            
        except Exception as e:
            self.parent_cog.logger.exception(f"Error clearing redemption cache: {e}")
            error_embed = discord.Embed(
                title=f"{theme.deniedIcon} Error",
                description=f"Failed to clear redemption cache: {str(e)}",
                color=theme.emColor2
            )
            try:
                await interaction.response.edit_message(embed=error_embed, view=None)
            except discord.InteractionResponded:
                await interaction.followup.edit_message(interaction.message.id, embed=error_embed, view=None)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji=f"{theme.deniedIcon}")
    async def cancel_clear(self, interaction: discord.Interaction, button: discord.ui.Button):
        cancel_embed = discord.Embed(
            title=f"{theme.deniedIcon} Operation Cancelled",
            description="Redemption cache was not cleared.",
            color=theme.emColor1
        )
        await interaction.response.edit_message(embed=cancel_embed, view=None)

    async def on_timeout(self):
        """Handle timeout by disabling all buttons"""
        for item in self.children:
            item.disabled = True
        try:
            timeout_embed = discord.Embed(
                title=f"{theme.timeIcon} Timeout",
                description="Confirmation timed out. Redemption cache was not cleared.",
                color=discord.Color.orange()
            )
        except:
            pass

class OCRSettingsView(discord.ui.View):
    def __init__(self, cog, ocr_settings, onnx_available):
        super().__init__(timeout=7200)
        self.cog = cog
        self.enabled = ocr_settings[0]
        self.save_images_setting = ocr_settings[1]
        self.onnx_available = onnx_available
        self.disable_controls = not onnx_available

        # Row 0: Enable/Disable Button, Test Button
        self.enable_ocr_button_item = discord.ui.Button(
            emoji=f"{theme.verifiedIcon}" if self.enabled == 1 else "🚫",
            custom_id="enable_ocr", row=0,
            label="Disable CAPTCHA Solver" if self.enabled == 1 else "Enable CAPTCHA Solver",
            style=discord.ButtonStyle.danger if self.enabled == 1 else discord.ButtonStyle.success,
            disabled=self.disable_controls
        )
        self.enable_ocr_button_item.callback = self.enable_ocr_button
        self.add_item(self.enable_ocr_button_item)

        self.test_ocr_button_item = discord.ui.Button(
            label="Test CAPTCHA Solver", style=discord.ButtonStyle.secondary, emoji=f"{theme.testIcon}",
            custom_id="test_ocr", row=0,
            disabled=self.disable_controls
        )
        self.test_ocr_button_item.callback = self.test_ocr_button
        self.add_item(self.test_ocr_button_item)

        # Add the Change Test ID Button
        self.change_test_fid_button_item = discord.ui.Button(
            label="Change Test ID", style=discord.ButtonStyle.primary, emoji=f"{theme.refreshIcon}",
            custom_id="change_test_fid", row=0,
            disabled=self.disable_controls
        )
        self.change_test_fid_button_item.callback = self.change_test_fid_button
        self.add_item(self.change_test_fid_button_item)

        # Add the Clear Redemption Cache Button
        self.clear_cache_button_item = discord.ui.Button(
            label="Clear Redemption Cache", style=discord.ButtonStyle.danger, emoji=f"{theme.trashIcon}",
            custom_id="clear_redemption_cache", row=1,
            disabled=self.disable_controls
        )
        self.clear_cache_button_item.callback = self.clear_redemption_cache_button
        self.add_item(self.clear_cache_button_item)

        # Row 2: Image Save Select Menu
        self.image_save_select_item = discord.ui.Select(
            placeholder="Select Captcha Image Saving Option",
            min_values=1, max_values=1, row=2, custom_id="image_save_select",
            options=[
                discord.SelectOption(label="Don't Save Any Images", value="0", description="Fastest, no disk usage"),
                discord.SelectOption(label="Save Only Failed Captchas", value="1", description="For debugging server rejects"),
                discord.SelectOption(label="Save Only Successful Captchas", value="2", description="To see what worked"),
                discord.SelectOption(label="Save All Captchas (High Disk Usage!)", value="3", description="Comprehensive debugging")
            ],
            disabled=self.disable_controls
        )
        for option in self.image_save_select_item.options:
            option.default = (str(self.save_images_setting) == option.value)
        self.image_save_select_item.callback = self.image_save_select_callback
        self.add_item(self.image_save_select_item)

    async def change_test_fid_button(self, interaction: discord.Interaction):
        """Handle the change test ID button click."""
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        await interaction.response.send_modal(TestIDModal(self.cog))

    async def enable_ocr_button(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        new_enabled = 1 if self.enabled == 0 else 0
        success, message = await self.cog.update_ocr_settings(interaction, enabled=new_enabled)
        await self.cog.show_ocr_settings(interaction)

    async def test_ocr_button(self, interaction: discord.Interaction):
        logger = self.cog.logger
        user_id = interaction.user.id
        current_time = time.time()

        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        if not self.cog.captcha_solver or not self.cog.captcha_solver.is_initialized:
            await interaction.response.send_message(f"{theme.deniedIcon} CAPTCHA solver is not initialized. Ensure OCR is enabled.", ephemeral=True)
            return

        last_test_time = self.cog.test_captcha_cooldowns.get(user_id, 0)
        if current_time - last_test_time < self.cog.test_captcha_delay:
            remaining_time = int(self.cog.test_captcha_delay - (current_time - last_test_time))
            await interaction.response.send_message(f"{theme.deniedIcon} Please wait {remaining_time} more seconds before testing again.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        logger.info(f"[Test Button] User {user_id} triggered test.")
        self.cog.test_captcha_cooldowns[user_id] = current_time

        captcha_image_base64 = None
        image_bytes = None
        error = None
        captcha_code = None
        success = False
        method = "N/A"
        confidence = 0.0
        solve_duration = 0.0
        test_fid = self.cog.get_test_fid()
        session = None

        try:
            logger.info(f"[Test Button] First logging in with test ID {test_fid}...")
            session, response_stove_info = await self.cog.get_stove_info_wos(player_id=test_fid)
            
            try:
                player_info_json = response_stove_info.json()
                if player_info_json.get("msg") != "success":
                    logger.error(f"[Test Button] Login failed for test ID {test_fid}: {player_info_json.get('msg')}")
                    await interaction.followup.send(f"{theme.deniedIcon} Login failed with test ID {test_fid}. Please check if the ID is valid.", ephemeral=True)
                    return
                logger.info(f"[Test Button] Successfully logged in with test ID {test_fid}")
            except Exception as json_err:
                logger.error(f"[Test Button] Error parsing login response: {json_err}")
                await interaction.followup.send(f"{theme.deniedIcon} Error processing login response.", ephemeral=True)
                return
            
            logger.info(f"[Test Button] Fetching captcha for test ID {test_fid} using established session...")
            captcha_image_base64, error = await self.cog.fetch_captcha(test_fid, session=session)
            logger.info(f"[Test Button] Captcha fetch result: Error='{error}', HasImage={captcha_image_base64 is not None}")

            if error:
                await interaction.followup.send(f"{theme.deniedIcon} Error fetching test captcha from the API: `{error}`", ephemeral=True)
                return

            if captcha_image_base64:
                try:
                    if captcha_image_base64.startswith("data:image"):
                        img_b64_data = captcha_image_base64.split(",", 1)[1]
                    else:
                        img_b64_data = captcha_image_base64
                    image_bytes = base64.b64decode(img_b64_data)
                    logger.info("[Test Button] Successfully decoded base64 image.")
                except Exception as decode_err:
                    logger.error(f"[Test Button] Failed to decode base64 image: {decode_err}")
                    await interaction.followup.send(f"{theme.deniedIcon} Failed to decode captcha image data.", ephemeral=True)
                    return
            else:
                logger.error("[Test Button] Captcha fetch returned no image data.")
                await interaction.followup.send(f"{theme.deniedIcon} Failed to retrieve captcha image data from API.", ephemeral=True)
                return

            if image_bytes:
                logger.info("[Test Button] Solving fetched captcha...")
                start_solve_time = time.time()
                captcha_code, success, method, confidence, _ = await self.cog.captcha_solver.solve_captcha(
                    image_bytes, fid=f"test-{user_id}", attempt=0
                )
                solve_duration = time.time() - start_solve_time
                log_confidence_str = f'{confidence:.2f}' if isinstance(confidence, float) else 'N/A'
                logger.info(f"[Test Button] Solve result: Success={success}, Code='{captcha_code}', Method='{method}', Conf={log_confidence_str}. Duration: {solve_duration:.2f}s")
            else:
                 logger.error("[Test Button] Logic error: image_bytes is None before solving.")
                 await interaction.followup.send(f"{theme.deniedIcon} Internal error before solving captcha.", ephemeral=True)
                 return

            confidence_str = f'{confidence:.2f}' if isinstance(confidence, float) else 'N/A'
            embed = discord.Embed(
                title=f"{theme.searchIcon} CAPTCHA Solver Test Results (ONNX)",
                description=(
                    f"**Test Summary**\n{theme.upperDivider}\n"
                    f"{theme.robotIcon} **OCR Success:** {f'{theme.verifiedIcon} Yes' if success else f'{theme.deniedIcon} No'}\n"
                    f"{theme.searchIcon} **Recognized Code:** `{captcha_code if success and captcha_code else 'N/A'}`\n"
                    f"{theme.chartIcon} **Confidence:** `{confidence_str}`\n"
                    f"{theme.timeIcon} **Solve Time:** `{solve_duration:.2f}s`\n"
                    f"{theme.lowerDivider}\n"
                ), color=theme.emColor3 if success else discord.Color.red()
            )

            save_path_str = None
            save_error_str = None
            try:
                self.cog.settings_cursor.execute("SELECT save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
                save_setting_row = self.cog.settings_cursor.fetchone()
                current_save_mode = save_setting_row[0] if save_setting_row else 0

                should_save_img = False
                save_tag = "UNKNOWN"
                if success and current_save_mode in [2, 3]:
                    should_save_img = True
                    save_tag = captcha_code if captcha_code else "SUCCESS_NOCDE"
                elif not success and current_save_mode in [1, 3]:
                    should_save_img = True
                    save_tag = "FAILED"

                if should_save_img and image_bytes:
                    logger.info(f"[Test Button] Attempting to save image based on mode {current_save_mode}. Status success={success}, tag='{save_tag}'")
                    captcha_dir = self.cog.captcha_solver.captcha_dir
                    safe_tag = re.sub(r'[\\/*?:"<>|]', '_', save_tag)
                    timestamp = int(time.time())

                    if success:
                         base_filename = f"{safe_tag}.png"
                    else:
                         base_filename = f"FAIL_{safe_tag}_{timestamp}.png"

                    test_path = os.path.join(captcha_dir, base_filename)

                    counter = 1
                    orig_path = test_path
                    while os.path.exists(test_path) and counter <= 100:
                        name, ext = os.path.splitext(orig_path)
                        test_path = f"{name}_{counter}{ext}"
                        counter += 1

                    if counter > 100:
                        save_error_str = f"Could not find unique filename for {base_filename} after 100 tries."
                        logger.warning(f"[Test Button] {save_error_str}")
                    else:
                        os.makedirs(captcha_dir, exist_ok=True)
                        with open(test_path, "wb") as f:
                            f.write(image_bytes)
                        save_path_str = os.path.basename(test_path)
                        logger.info(f"[Test Button] Saved test captcha image to {test_path}")

            except Exception as img_save_err:
                logger.exception(f"[Test Button] Error saving test image: {img_save_err}")
                save_error_str = f"Error during saving: {img_save_err}"

            if save_path_str:
                embed.add_field(name="📸 Captcha Image Saved", value=f"`{save_path_str}` in `{os.path.relpath(self.cog.captcha_solver.captcha_dir)}`", inline=False)
            elif save_error_str:
                embed.add_field(name=f"{theme.warnIcon} Image Save Error", value=save_error_str, inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"[Test Button] Test completed for user {user_id}.")

        except requests.exceptions.ConnectionError:
            logger.warning(f"[Test Button] Connection error for user {user_id}. WOS API may be unavailable.")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: Unable to reach WOS API. Please check your internet connection.", ephemeral=True)
            except Exception:
                pass
        except requests.exceptions.Timeout:
            logger.warning(f"[Test Button] Timeout for user {user_id}. WOS API may be slow.")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: Request timed out. WOS API may be overloaded or unavailable.", ephemeral=True)
            except Exception:
                pass
        except requests.exceptions.RequestException as e:
            logger.warning(f"[Test Button] Request error for user {user_id}: {type(e).__name__}")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} Connection error: {type(e).__name__}. Please try again later.", ephemeral=True)
            except Exception:
                pass
        except Exception as e:
            logger.exception(f"[Test Button] UNEXPECTED Error during test for user {user_id}: {e}")
            try:
                await interaction.followup.send(f"{theme.deniedIcon} An unexpected error occurred during the test: `{e}`. Please check the bot logs.", ephemeral=True)
            except Exception as followup_err:
                logger.error(f"[Test Button] Failed to send final error followup to user {user_id}: {followup_err}")
        finally:
            if session:
                session.close()

    async def clear_redemption_cache_button(self, interaction: discord.Interaction):
        """Handle the clear redemption cache button click."""
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        # Create confirmation embed
        embed = discord.Embed(
            title=f"{theme.warnIcon} Clear Redemption Cache",
            description=(
                "This will **permanently delete** all gift code redemption records from the database.\n\n"
                "**What this does:**\n"
                "• Removes all entries from the `user_giftcodes` table\n"
                "• Allows users to attempt redeeming gift codes again\n"
                "• Useful for development testing and image collection\n\n"
                "**Warning:** This action cannot be undone!"
            ),
            color=discord.Color.orange()
        )

        # Get current count for display
        try:
            self.cog.cursor.execute("SELECT COUNT(*) FROM user_giftcodes")
            current_count = self.cog.cursor.fetchone()[0]
            embed.add_field(
                name=f"{theme.chartIcon} Current Records",
                value=f"{current_count:,} redemption records will be deleted",
                inline=False
            )
        except Exception as e:
            self.cog.logger.error(f"Error getting user_giftcodes count: {e}")
            embed.add_field(
                name=f"{theme.chartIcon} Current Records", 
                value="Unable to count records",
                inline=False
            )

        # Create confirmation view
        confirm_view = ClearCacheConfirmView(self.cog)
        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

    async def image_save_select_callback(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message(f"{theme.deniedIcon} Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True) 
        
        try:
            selected_value = int(interaction.data["values"][0])
        
            success, message = await self.cog.update_ocr_settings(
                interaction=interaction,
                save_images=selected_value
            )

            if success:
                self.save_images_setting = selected_value
                for option in self.image_save_select_item.options:
                    option.default = (str(self.save_images_setting) == option.value)
            else:
                await interaction.followup.send(f"{theme.deniedIcon} {message}", ephemeral=True)

        except ValueError:
            await interaction.followup.send(f"{theme.deniedIcon} Invalid selection value for image saving.", ephemeral=True)
        except Exception as e:
            self.cog.logger.exception("Error processing image save selection in OCRSettingsView.")
            await interaction.followup.send(f"{theme.deniedIcon} An error occurred while updating image saving settings.", ephemeral=True)
        
        async def update_task(save_images_value):
            self.cog.logger.info(f"Task started: Updating OCR save_images to {save_images_value}")
            _success, _message = await self.cog.update_ocr_settings(
                interaction=None,
                save_images=save_images_value
            )
            self.cog.logger.info(f"Task finished: update_ocr_settings returned success={_success}, message='{_message}'")
            return _success, _message

        update_job = asyncio.create_task(update_task(selected_value))
        initial_followup_message = f"{theme.hourglassIcon} Your settings are being updated... Please wait."
        try:
            progress_message = await interaction.followup.send(initial_followup_message, ephemeral=True)
        except discord.HTTPException as e:
            self.cog.logger.error(f"Failed to send initial followup for image save: {e}")
            return

        try:
            success, message_from_task = await asyncio.wait_for(update_job, timeout=60.0)
        except asyncio.TimeoutError:
            self.cog.logger.error("Timeout waiting for OCR settings update task to complete.")
            await progress_message.edit(content="⌛️ Timed out waiting for settings to update. Please try again or check logs.")
            return
        except Exception as e_task:
            self.cog.logger.exception(f"Exception in OCR settings update task: {e_task}")
            await progress_message.edit(content=f"{theme.deniedIcon} An error occurred during the update: {e_task}")
            return

        if success:
            self.cog.logger.info(f"OCR settings update successful: {message_from_task}")
            self.cog.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings_new = self.cog.settings_cursor.fetchone()
            if ocr_settings_new:
                self.save_images_setting = ocr_settings_new[1]
                for option in self.image_save_select_item.options:
                    option.default = (str(self.save_images_setting) == option.value)
            
            try:
                new_embed = interaction.message.embeds[0] if interaction.message.embeds else None

                await interaction.edit_original_response(
                    content=None,
                    embed=new_embed, 
                    view=self
                )
                await progress_message.edit(content=f"{theme.verifiedIcon} {message_from_task}")
            except discord.NotFound:
                 self.cog.logger.warning("Original message or progress message for OCR settings not found for final update.")
            except Exception as e_edit_final:
                 self.cog.logger.exception(f"Error editing messages after successful OCR settings update: {e_edit_final}")
                 await progress_message.edit(content=f"{theme.verifiedIcon} {message_from_task}\n{theme.warnIcon} Couldn't fully refresh the view.")

        else:
            self.cog.logger.error(f"OCR settings update failed: {message_from_task}")
            await progress_message.edit(content=f"{theme.deniedIcon} {message_from_task}")

async def setup(bot):
    await bot.add_cog(GiftOperations(bot))
