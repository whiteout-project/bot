"""
Gift code orchestrator cog. Delegates to gift_redemption, gift_channels,
gift_settings, gift_views, and gift_operationsapi for the heavy lifting.
- gift_captchasolver: ONNX-based CAPTCHA solver for gift code redemption
"""

import discord
from discord.ext import commands
import sqlite3
from discord.ext import tasks
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

    async def claim_giftcode_rewards_wos(self, player_id, giftcode, *, skip_cache: bool = False):
        return await gift_redemption.claim_giftcode_rewards_wos(self, player_id, giftcode, skip_cache=skip_cache)

    async def scan_historical_messages(self, channel, alliance_id):
        return await gift_redemption.scan_historical_messages(self, channel, alliance_id)

    async def fetch_captcha(self, player_id, session=None):
        return await gift_redemption.fetch_captcha(self, player_id, session)

    async def use_giftcode_for_alliance(self, alliance_id, giftcode):
        return await gift_redemption.use_giftcode_for_alliance(self, alliance_id, giftcode)

    async def cleanup_old_invalid_codes(self):
        return await gift_redemption.cleanup_old_invalid_codes(self)

    async def _process_auto_use(self, giftcode):
        return await gift_redemption._process_auto_use(self, giftcode)

    async def add_manual_redemption_to_queue(self, giftcodes, alliance_ids, interaction):
        return await gift_redemption.add_manual_redemption_to_queue(self, giftcodes, alliance_ids, interaction)

    async def get_queue_status(self):
        return await gift_redemption.get_queue_status(self)

    async def validate_gift_codes(self, interaction):
        return await gift_redemption.validate_gift_codes(self, interaction)

    async def handle_success(self, message, giftcode):
        from .gift_views import handle_success as _handle
        return await _handle(self, message, giftcode)

    async def handle_already_received(self, message, giftcode):
        from .gift_views import handle_already_received as _handle
        return await _handle(self, message, giftcode)

    # ── Delegation to gift_channels ───────────────────────────────────

    async def setup_gift_channel(self, interaction):
        return await gift_channels.setup_gift_channel(self, interaction)

    async def manage_channel_settings(self, interaction):
        return await gift_channels.manage_channel_settings(self, interaction)

    async def delete_gift_channel(self, interaction):
        return await gift_channels.delete_gift_channel(self, interaction)

    async def delete_gift_channel_for_alliance(self, interaction, alliance_id):
        return await gift_channels.delete_gift_channel_for_alliance(self, interaction, alliance_id)

    async def channel_history_scan(self, interaction):
        return await gift_channels.channel_history_scan(self, interaction)

    # ── Delegation to gift_settings ───────────────────────────────────

    async def verify_test_fid(self, fid):
        return await gift_settings.verify_test_fid(self, fid)

    async def update_test_fid(self, new_fid):
        return await gift_settings.update_test_fid(self, new_fid)

    def get_test_fid(self):
        return gift_settings.get_test_fid(self)

    async def get_validation_fid(self):
        return await gift_settings.get_validation_fid(self)

    async def show_ocr_settings(self, interaction):
        return await gift_settings.show_ocr_settings(self, interaction)

    async def update_ocr_settings(self, interaction, enabled=None):
        return await gift_settings.update_ocr_settings(self, interaction, enabled)

    async def show_redemption_priority(self, interaction):
        return await gift_settings.show_redemption_priority(self, interaction)

    async def setup_giftcode_auto(self, interaction):
        return await gift_settings.setup_giftcode_auto(self, interaction)

    # ── Delegation to gift_views ──────────────────────────────────────

    async def create_gift_code(self, interaction):
        from .gift_views import create_gift_code as _create
        return await _create(self, interaction)

    async def list_gift_codes(self, interaction):
        from .gift_views import list_gift_codes as _list
        return await _list(self, interaction)

    async def delete_gift_code(self, interaction):
        from .gift_views import delete_gift_code as _delete
        return await _delete(self, interaction)

    async def show_settings_menu(self, interaction):
        from .gift_views import show_settings_menu as _show
        return await _show(self, interaction)

    async def get_admin_info(self, user_id):
        from .gift_views import get_admin_info as _info
        return await _info(self, user_id)

    async def get_alliance_names(self, user_id, is_global=False):
        from .gift_views import get_alliance_names as _names
        return await _names(self, user_id, is_global)

    async def get_available_alliances(self, interaction):
        from .gift_views import get_available_alliances as _alliances
        return await _alliances(self, interaction)


async def setup(bot):
    await bot.add_cog(GiftOperations(bot))
