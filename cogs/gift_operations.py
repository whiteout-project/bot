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

async def setup(bot):
    await bot.add_cog(GiftOperations(bot))
