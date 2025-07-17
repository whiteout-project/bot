import discord
from discord.ext import commands
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import hashlib
import json
from datetime import datetime
import sqlite3
from discord.ext import tasks
import asyncio
import sys
import base64
import re
import os
import traceback
import time
import random
import logging
import logging.handlers
from .alliance_member_operations import AllianceSelectView
from .alliance import PaginatedChannelView
from .gift_operationsapi import GiftCodeAPI
from .gift_captchasolver import GiftCaptchaSolver

class GiftOperations(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        
        # Logger Setup for gift_ops.txt
        self.logger = logging.getLogger('gift_ops')
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False  # Prevent propagation to root logger
        log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        log_dir = 'log'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file_path = os.path.join(log_dir, 'gift_ops.txt')
        self.log_directory = log_dir

        file_handler = logging.handlers.RotatingFileHandler(
            log_file_path, maxBytes=3 * 1024 * 1024, backupCount=1, encoding='utf-8'
        )
        file_handler.setFormatter(log_formatter)
        if not self.logger.hasHandlers():
            self.logger.addHandler(file_handler)

        # Logger Setup for giftlog.txt
        self.giftlog = logging.getLogger("giftlog")
        self.giftlog.setLevel(logging.INFO)
        self.giftlog.propagate = False

        giftlog_file = os.path.join(log_dir, 'giftlog.txt')
        giftlog_handler = logging.handlers.RotatingFileHandler(
            giftlog_file, maxBytes=3 * 1024 * 1024, backupCount=1, encoding='utf-8'
        )
        giftlog_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
        if not self.giftlog.hasHandlers():
            self.giftlog.addHandler(giftlog_handler)

        self.logger.info("GiftOperations Cog initializing...")

        if hasattr(bot, 'conn'):
            self.conn = bot.conn
            self.cursor = self.conn.cursor()
        else:
            if not os.path.exists('db'):
                os.makedirs('db')
            self.conn = sqlite3.connect('db/giftcode.sqlite')
            self.cursor = self.conn.cursor()

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
        if not os.path.exists('db'): os.makedirs('db')
        self.settings_conn = sqlite3.connect('db/settings.sqlite')
        self.settings_cursor = self.settings_conn.cursor()

        # Alliance DB Connection
        if not os.path.exists('db'): os.makedirs('db')
        self.alliance_conn = sqlite3.connect('db/alliance.sqlite')
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

        # Add validation_status column to gift_codes table if it doesn't exist
        try:
            self.cursor.execute("ALTER TABLE gift_codes ADD COLUMN validation_status TEXT DEFAULT 'pending'")
            self.conn.commit()
        except sqlite3.OperationalError:
            # Column already exists
            pass

        # WOS API URLs and Key
        self.wos_player_info_url = "https://wos-giftcode-api.centurygame.com/api/player"
        self.wos_giftcode_url = "https://wos-giftcode-api.centurygame.com/api/gift_code"
        self.wos_captcha_url = "https://wos-giftcode-api.centurygame.com/api/captcha"
        self.wos_giftcode_redemption_url = "https://wos-giftcode.centurygame.com"
        self.wos_encrypt_key = "tB87#kPtkxqOS2"

        # Retry Configuration for Requests
        self.retry_config = Retry(
            total=10,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )

        # Initialization of Locks and Cooldowns
        self.captcha_solver = None
        self._validation_lock = asyncio.Lock()
        self.last_validation_attempt_time = 0
        self.validation_cooldown = 5
        self.test_captcha_cooldowns = {} # User ID: last test timestamp for test button
        self.test_captcha_delay = 60

        self.processing_stats = {
        "ocr_solver_calls": 0,       # Times solver.solve_captcha was called
        "ocr_valid_format": 0,     # Times solver returned success=True
        "captcha_submissions": 0,  # Times a solved code was sent to API
        "server_validation_success": 0, # Captcha accepted by server (not CAPTCHA_ERROR)
        "server_validation_failure": 0, # Captcha rejected by server (CAPTCHA_ERROR)
        "total_fids_processed": 0,   # Count of completed claim_giftcode calls
        "total_processing_time": 0.0 # Sum of durations for completed calls
        }

        # Captcha Solver Initialization Attempt
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS ocr_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enabled INTEGER DEFAULT 1,
                    save_images INTEGER DEFAULT 0
                    -- Remove use_gpu and gpu_device columns if they existed
                )""")
            self.settings_conn.commit()

            # Load latest OCR settings
            self.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings = self.settings_cursor.fetchone()

            if ocr_settings:
                enabled, save_images = ocr_settings
                if enabled == 1:
                    self.logger.info("GiftOps __init__: OCR is enabled. Initializing ONNX solver...")
                    self.captcha_solver = GiftCaptchaSolver(save_images=save_images)
                    if not self.captcha_solver.is_initialized:
                        self.logger.error("GiftOps __init__: ONNX solver FAILED to initialize.")
                        self.captcha_solver = None
                    else:
                        self.logger.info("GiftOps __init__: ONNX solver initialized successfully.")
                else:
                    self.logger.info("GiftOps __init__: OCR is disabled in settings.")
            else:
                self.logger.warning("GiftOps __init__: No OCR settings found in DB. Inserting defaults (Enabled=1, SaveImages=0).")
                self.settings_cursor.execute("""
                    INSERT INTO ocr_settings (enabled, save_images) VALUES (1, 0)
                """)
                self.settings_conn.commit()
                self.logger.info("GiftOps __init__: Attempting initialization with default settings...")
                self.captcha_solver = GiftCaptchaSolver(save_images=0)
                if not self.captcha_solver.is_initialized:
                    self.logger.error("GiftOps __init__: ONNX solver FAILED to initialize with defaults.")
                    self.captcha_solver = None
                else: # Ensure success is logged here for the CI
                    self.logger.info("GiftOps __init__: ONNX solver initialized successfully.")

        except ImportError as lib_err:
            self.logger.exception(f"GiftOps __init__: ERROR - Missing required library for OCR (likely onnxruntime): {lib_err}. Captcha solving disabled.")
            self.captcha_solver = None
        except Exception as e:
            self.logger.exception(f"GiftOps __init__: Unexpected error during Captcha solver setup: {e}")
            self.logger.exception(f"Traceback: {traceback.format_exc()}")
            self.captcha_solver = None

        # Test FID Settings Table
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_fid_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_fid TEXT NOT NULL
                )
            """)
            
            self.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.settings_cursor.fetchone()
            
            if not result: # Insert the default test FID if no entry exists
                self.settings_cursor.execute("INSERT INTO test_fid_settings (test_fid) VALUES (?)", ("244886619",))
                self.settings_conn.commit()
                self.logger.info("Initialized default test FID (244886619) in database")
        except Exception as e:
            self.logger.exception(f"Error setting up test FID table: {e}")

    def clean_gift_code(self, giftcode):
        """Remove invisible Unicode characters (like RLM) that can contaminate gift codes"""
        import unicodedata
        cleaned = ''.join(char for char in giftcode if unicodedata.category(char)[0] != 'C')
        return cleaned.strip()
    
    @commands.Cog.listener()
    async def on_ready(self):
        """
        Handles cog setup when the bot is ready.
        Initializes database tables, loads OCR settings, initializes the captcha solver if enabled,
        validates gift code channels, and starts the background task loop.
        """
        self.logger.info("GiftOps Cog: on_ready triggered.")
        try:
            try:
                self.logger.info("Checking ocr_settings table schema...")
                conn_info = sqlite3.connect('db/settings.sqlite')
                cursor_info = conn_info.cursor()
                cursor_info.execute("PRAGMA table_info(ocr_settings)")
                columns = [col[1] for col in cursor_info.fetchall()]
                columns_to_drop = []
                if 'use_gpu' in columns: columns_to_drop.append('use_gpu')
                if 'gpu_device' in columns: columns_to_drop.append('gpu_device')
                    
                if columns_to_drop:
                    sqlite_version = sqlite3.sqlite_version_info
                    if sqlite_version >= (3, 35, 0):
                        self.logger.info(f"Found old columns {columns_to_drop} in ocr_settings. SQLite version {sqlite3.sqlite_version} supports DROP COLUMN. Attempting removal.")
                        for col_name in columns_to_drop:
                            try:
                                self.settings_cursor.execute(f"ALTER TABLE ocr_settings DROP COLUMN {col_name}")
                                self.logger.info(f"Successfully dropped column: {col_name}")
                            except Exception as drop_err:
                                self.logger.error(f"Error dropping column {col_name}: {drop_err}")
                        self.settings_conn.commit()
                    else:
                        self.logger.warning(f"Found old columns {columns_to_drop} in ocr_settings, but SQLite version {sqlite3.sqlite_version} (< 3.35.0) does not support DROP COLUMN easily. Columns will be ignored.")
                else:
                    self.logger.info("ocr_settings table schema is up to date.")
                conn_info.close()
            except Exception as schema_err:
                self.logger.error(f"Error during ocr_settings schema check/cleanup: {schema_err}")

            # OCR Settings Table Setup
            self.logger.info("Setting up ocr_settings table (ensuring correct schema)...")
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS ocr_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    enabled INTEGER DEFAULT 1,
                    save_images INTEGER DEFAULT 0
                )
            """)
            self.settings_conn.commit()
            self.logger.info("ocr_settings table checked/created.")

            # Initialize Default OCR Settings if Needed
            self.settings_cursor.execute("SELECT COUNT(*) FROM ocr_settings")
            count = self.settings_cursor.fetchone()[0]
            if count == 0:
                self.logger.info("No OCR settings found, inserting defaults (Enabled=1, SaveImages=0)...")
                self.settings_cursor.execute("""
                    INSERT INTO ocr_settings (enabled, save_images) VALUES (1, 0)
                """)
                self.settings_conn.commit()
                self.logger.info("Default OCR settings inserted.")
            else:
                self.logger.info(f"Found {count} existing OCR settings row(s). Using the latest.")

            # Load OCR Settings and Initialize Solver
            if self.captcha_solver is None:
                self.logger.warning("Captcha solver not initialized in __init__, attempting again in on_ready...")
                self.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
                ocr_settings = self.settings_cursor.fetchone()

                if ocr_settings:
                    enabled, save_images_setting = ocr_settings
                    self.logger.info(f"on_ready loaded settings: Enabled={enabled}, SaveImages={save_images_setting}")
                    if enabled == 1:
                        self.logger.info("OCR is enabled, attempting ONNX initialization...")
                        try:
                            self.captcha_solver = GiftCaptchaSolver(save_images=save_images_setting)
                            if not self.captcha_solver.is_initialized:
                                self.logger.error("ONNX solver FAILED to initialize in on_ready.")
                                self.captcha_solver = None
                        except Exception as e:
                            self.logger.exception("Failed to initialize Captcha Solver in on_ready.")
                            self.captcha_solver = None
                    else:
                        self.logger.info("OCR is disabled in settings (checked in on_ready).")
                else:
                    self.logger.warning("Could not load OCR settings from database in on_ready.")
            else:
                self.logger.info("Captcha solver was already initialized.")

            # Gift Code Channel Validation
            self.logger.info("Validating gift code channels...")
            self.cursor.execute("SELECT channel_id, alliance_id FROM giftcode_channel")
            channel_configs = self.cursor.fetchall()
            self.logger.info(f"Found {len(channel_configs)} gift code channel configurations in DB.")

            invalid_channels = []
            for channel_id, alliance_id in channel_configs:
                channel = self.bot.get_channel(channel_id)
                if not channel:
                    self.logger.warning(f"Channel ID {channel_id} (Alliance: {alliance_id}) is invalid or bot cannot access it. Marking for removal.")
                    invalid_channels.append(channel_id)
                elif not isinstance(channel, discord.TextChannel):
                    self.logger.warning(f"Channel ID {channel_id} (Alliance: {alliance_id}) is not a Text Channel. Marking for removal.")
                    invalid_channels.append(channel_id)
                elif not channel.permissions_for(channel.guild.me).send_messages:
                    self.logger.warning(f"Missing send message permissions in channel {channel_id}. Functionality may be limited.")

            if invalid_channels:
                unique_invalid_channels = list(set(invalid_channels))
                self.logger.info(f"Removing {len(unique_invalid_channels)} invalid channel configurations from database: {unique_invalid_channels}")
                placeholders = ','.join('?' * len(unique_invalid_channels))
                try:
                    self.cursor.execute(f"DELETE FROM giftcode_channel WHERE channel_id IN ({placeholders})", unique_invalid_channels)
                    self.conn.commit()
                    self.logger.info("Successfully removed invalid channel configurations.")
                except sqlite3.Error as db_err:
                    self.logger.exception(f"DATABASE ERROR removing invalid channels from database: {db_err}")
            else:
                self.logger.info("All configured gift code channels appear valid.")

            # Start Background Task Loop
            if not self.check_channels_loop.is_running():
                self.logger.info("Starting check_channels_loop background task...")
                self.check_channels_loop.start()
                self.logger.info("check_channels_loop started.")
            else:
                self.logger.info("check_channels_loop is already running.")

            self.logger.info("GiftOps Cog: on_ready setup finished successfully.")

        except sqlite3.Error as db_err:
            self.logger.exception(f"DATABASE ERROR during on_ready setup: {db_err}")
        except Exception as e:
            self.logger.exception(f"UNEXPECTED ERROR during on_ready setup: {e}")

    @discord.ext.commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        log_file_path = os.path.join(self.log_directory, 'giftlog.txt')
        try:
            if message.author.bot or not message.guild:
                return

            self.cursor.execute("SELECT alliance_id FROM giftcode_channel WHERE channel_id = ?", (message.channel.id,))
            channel_info = self.cursor.fetchone()
            if not channel_info:
                return

            try:
                self.logger.info(f"GiftOps: [on_message] Running API sync for channel {message.channel.id}")
                await self.api.validate_and_clean_giftcode_file()
                await self.api.sync_with_api()
                self.logger.info(f"GiftOps: [on_message] API sync complete.")
            except Exception as e:
                self.logger.exception(f"Error during API sync triggered by on_message: {str(e)}")

            content = message.content.strip()
            if not content:
                return

            # Extract potential gift code
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
                self.logger.debug(f"[on_message] No valid gift code format found in message {message.id}")
                return

            log_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.logger.info(f"{log_timestamp} GiftOps: [on_message] Detected potential code '{giftcode}' in channel {message.channel.id} (Msg ID: {message.id})")

            # Check if code already exists
            self.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
            if self.cursor.fetchone():
                self.logger.info(f"GiftOps: [on_message] Code '{giftcode}' already exists in database.")
                reply_embed = discord.Embed(title="ℹ️ Gift Code Already Known", color=discord.Color.blue())
                reply_embed.description=(
                        f"**Gift Code Details**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"👤 **Sender:** {message.author.mention}\n"
                        f"🎁 **Gift Code:** `{giftcode}`\n"
                        f"📝 **Status:** Already in database.\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n")
                reaction_to_add = "ℹ️"
            else:
                # Add code without validation
                self.logger.info(f"GiftOps: [on_message] Adding new code '{giftcode}' to database as pending validation.")
                self.cursor.execute(
                    "INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                    (giftcode, datetime.now().strftime("%Y-%m-%d"), "pending")
                )
                self.conn.commit()

                # Don't send to API until validated
                self.logger.info(f"GiftOps: [on_message] Code '{giftcode}' added as pending - will send to API after validation.")

                self.cursor.execute("SELECT alliance_id FROM giftcodecontrol WHERE status = 1")
                auto_alliances = self.cursor.fetchall()
                if auto_alliances:
                    self.logger.info(f"GiftOps: [on_message] Triggering auto-use for {len(auto_alliances)} alliances for code '{giftcode}'.")
                    for alliance in auto_alliances:
                        await self.use_giftcode_for_alliance(alliance[0], giftcode)

                else:
                    self.logger.info(f"GiftOps: [on_message] No alliances configured for auto-use.")

                reply_embed = discord.Embed(title="✅ Gift Code Added", color=discord.Color.green())
                reply_embed.description=(
                    f"**Gift Code Details**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👤 **Sender:** {message.author.mention}\n"
                    f"🎁 **Gift Code:** `{giftcode}`\n"
                    f"📝 **Status:** Added to database (will be validated on first use).\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n")
                reaction_to_add = "✅"

            # Add reaction and reply
            if reaction_to_add:
                try:
                    await message.add_reaction(reaction_to_add)
                except (discord.Forbidden, discord.NotFound):
                    self.logger.error(f"GiftOps: [on_message] Failed to add reaction '{reaction_to_add}' to message {message.id}")
                except Exception as react_err:
                    self.logger.exception(f"GiftOps: [on_message] Unexpected error adding reaction: {react_err}")

            if reply_embed:
                try:
                    await message.reply(embed=reply_embed, mention_author=False)
                except (discord.Forbidden, discord.NotFound):
                    self.logger.error(f"GiftOps: [on_message] Failed to reply to message {message.id}")
                except Exception as reply_err:
                    self.logger.exception(f"GiftOps: [on_message] Unexpected error replying: {reply_err}")

        except Exception as e:
            self.logger.exception(f"GiftOps: UNEXPECTED Error in on_message handler: {str(e)}")
            traceback.print_exc()
            error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_details = traceback.format_exc()
            log_message_handler = (
                f"\n--- ERROR in on_message Handler ({error_timestamp}) ---\n"
                f"Message ID: {message.id if 'message' in locals() else 'N/A'}\n"
                f"Channel ID: {message.channel.id if 'message' in locals() else 'N/A'}\n"
                f"Error: {str(e)}\n"
                f"Traceback:\n{error_details}\n"
                f"---------------------------------------------------------\n"
            )
            try:
                self.giftlog.info(log_message_handler.strip())
            except Exception as log_e:
                self.logger.exception(f"GiftOps: CRITICAL - Failed to write on_message handler error log: {log_e}")

    async def verify_test_fid(self, fid):
        """
        Verify that a FID is valid by attempting to login to the account.
        
        Args:
            fid (str): The FID to verify
            
        Returns:
            tuple: (is_valid, message) where is_valid is a boolean and message is a string
        """
        try:
            self.logger.info(f"Verifying test FID: {fid}")
            
            session, response_stove_info = self.get_stove_info_wos(player_id=fid)
            
            try:
                player_info_json = response_stove_info.json()
            except json.JSONDecodeError:
                self.logger.error(f"Invalid JSON response when verifying FID {fid}")
                return False, "Invalid response from server"
            
            login_successful = player_info_json.get("msg") == "success"
            
            if login_successful:
                try:
                    nickname = player_info_json.get("data", {}).get("nickname", "Unknown")
                    furnace_lv = player_info_json.get("data", {}).get("stove_lv", "Unknown")
                    self.logger.info(f"Test FID {fid} is valid. Nickname: {nickname}, Level: {furnace_lv}")
                    return True, "Valid account"
                except Exception as e:
                    self.logger.exception(f"Error parsing player info for FID {fid}: {e}")
                    return True, "Valid account (but error getting details)"
            else:
                error_msg = player_info_json.get("msg", "Unknown error")
                self.logger.info(f"Test FID {fid} is invalid. Error: {error_msg}")
                return False, f"Login failed: {error_msg}"
        
        except Exception as e:
            self.logger.exception(f"Error verifying test FID {fid}: {e}")
            return False, f"Verification error: {str(e)}"

    async def update_test_fid(self, new_fid):
        """
        Update the test FID in the database.
        
        Args:
            new_fid (str): The new test FID
            
        Returns:
            bool: True if update was successful, False otherwise
        """
        try:
            self.logger.info(f"Updating test FID to: {new_fid}")
            
            self.settings_cursor.execute("""
                INSERT INTO test_fid_settings (test_fid) VALUES (?)
            """, (new_fid,))
            self.settings_conn.commit()
            
            self.logger.info(f"Test FID updated successfully to {new_fid}")
            return True
        
        except sqlite3.Error as db_err:
            self.logger.exception(f"Database error updating test FID: {db_err}")
            return False
        except Exception as e:
            self.logger.exception(f"Unexpected error updating test FID: {e}")
            return False

    def get_test_fid(self):
        """
        Get the current test FID from the database.
        
        Returns:
            str: The current test FID, or the default "244886619" if not found
        """
        try:
            self.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.settings_cursor.fetchone()
            return result[0] if result else "244886619"
        except Exception as e:
            self.logger.exception(f"Error getting test FID: {e}")
            return "244886619"

    def encode_data(self, data, debug_sign_error=False):
        secret = self.wos_encrypt_key
        sorted_keys = sorted(data.keys())
        encoded_data = "&".join(
            [
                f"{key}={json.dumps(data[key]) if isinstance(data[key], dict) else data[key]}"
                for key in sorted_keys
            ]
        )
        sign = hashlib.md5(f"{encoded_data}{secret}".encode()).hexdigest()

        if debug_sign_error: # Debug logging for sign error when requested
            self.logger.error(f"[SIGN ERROR DEBUG] Input data: {data}")
            self.logger.error(f"[SIGN ERROR DEBUG] Encoded data: {encoded_data}")
            self.logger.error(f"[SIGN ERROR DEBUG] String being hashed: {encoded_data}{secret}")
            self.logger.error(f"[SIGN ERROR DEBUG] Secret key: {secret}")
            self.logger.error(f"[SIGN ERROR DEBUG] Generated signature: {sign}")
            self.logger.error(f"[SIGN ERROR DEBUG] Final payload: {{'sign': '{sign}', **{data}}}")
        
        return {"sign": sign, **data}

    def get_stove_info_wos(self, player_id):
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=self.retry_config))

        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": self.wos_giftcode_redemption_url,
        }

        data_to_encode = {
            "fid": f"{player_id}",
            "time": f"{int(datetime.now().timestamp())}",
        }
        data = self.encode_data(data_to_encode)

        response_stove_info = session.post(
            self.wos_player_info_url,
            headers=headers,
            data=data,
        )
        return session, response_stove_info

    async def claim_giftcode_rewards_wos(self, player_id, giftcode):

        giftcode = self.clean_gift_code(giftcode)
        process_start_time = time.time()
        status = "ERROR"
        image_bytes = None
        captcha_code = None
        method = "N/A"

        try:
            # Cache Check
            test_fid = self.get_test_fid()
            if player_id != test_fid:
                self.cursor.execute("SELECT status FROM user_giftcodes WHERE fid = ? AND giftcode = ?", (player_id, giftcode))
                existing_record = self.cursor.fetchone()
                if existing_record:
                    if existing_record[0] in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE", "TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                        self.logger.info(f"CACHE HIT - User {player_id} code '{giftcode}' status: {existing_record[0]}")
                        return existing_record[0]

            # Get Player Info
            session, response_stove_info = self.get_stove_info_wos(player_id=player_id)
            log_entry_player = f"\n{datetime.now()} API REQUEST - Player Info\nPlayer ID: {player_id}\n"
            try:
                response_json_player = response_stove_info.json()
                log_entry_player += f"Response Code: {response_stove_info.status_code}\nResponse JSON:\n{json.dumps(response_json_player, indent=2)}\n"
            except json.JSONDecodeError:
                log_entry_player += f"Response Code: {response_stove_info.status_code}\nResponse Text (Not JSON): {response_stove_info.text[:500]}...\n"
            log_entry_player += "-" * 50 + "\n"
            self.giftlog.info(log_entry_player.strip())

            try:
                player_info_json = response_stove_info.json()
            except json.JSONDecodeError:
                player_info_json = {}
            login_successful = player_info_json.get("msg") == "success"

            if not login_successful:
                status = "LOGIN_FAILED"
                log_message = f"{datetime.now()} Login failed for FID {player_id}: {player_info_json.get('msg', 'Unknown')}\n"
                self.giftlog.info(log_message.strip())
                return status

            # Check if OCR Enabled and Solver Ready
            self.settings_cursor.execute("SELECT enabled FROM ocr_settings ORDER BY id DESC LIMIT 1")
            ocr_settings_row = self.settings_cursor.fetchone()
            ocr_enabled = ocr_settings_row[0] if ocr_settings_row else 0

            if not (ocr_enabled == 1 and self.captcha_solver):
                status = "OCR_DISABLED" if ocr_enabled == 0 else "SOLVER_ERROR"
                log_msg = f"{datetime.now()} Skipping captcha: OCR disabled (Enabled={ocr_enabled}) or Solver not ready ({self.captcha_solver is None}) for FID {player_id}.\n"
                self.logger.info(log_msg.strip())
                return status

            # Captcha Fetching and Solving Loop
            self.logger.info(f"GiftOps: OCR enabled and solver initialized for FID {player_id}.")
            self.captcha_solver.reset_run_stats()
            max_ocr_attempts = 4

            for attempt in range(max_ocr_attempts):
                self.logger.info(f"GiftOps: Attempt {attempt + 1}/{max_ocr_attempts} to fetch/solve captcha for FID {player_id}")
                captcha_image_base64, error = await self.fetch_captcha(player_id, session)
                
                if error:
                    status = "TIMEOUT_RETRY" if error == "CAPTCHA_TOO_FREQUENT" else "CAPTCHA_FETCH_ERROR"
                    self.giftlog.info(f"{datetime.now()} Captcha fetch error for {player_id}: {error}\n")
                    break
                
                if captcha_image_base64 and not error:
                    try:
                        if captcha_image_base64.startswith("data:image"):
                            img_b64_data = captcha_image_base64.split(",", 1)[1]
                        else:
                            img_b64_data = captcha_image_base64
                        image_bytes = base64.b64decode(img_b64_data)
                    except Exception as decode_err:
                        self.logger.error(f"Failed to decode base64 image for FID {player_id}: {decode_err}")
                        status = "CAPTCHA_FETCH_ERROR"
                        break
                else:
                    image_bytes = None

                if image_bytes:
                    self.processing_stats["ocr_solver_calls"] += 1
                    captcha_code, success, method, confidence, _ = await self.captcha_solver.solve_captcha(
                    image_bytes, fid=player_id, attempt=attempt)
                    if success:
                        self.processing_stats["ocr_valid_format"] += 1
                else:
                    self.logger.warning(f"Skipping OCR attempt for FID {player_id} as image_bytes is None.")
                    status = "CAPTCHA_FETCH_ERROR"
                    break

                if not success:
                    self.giftlog.info(f"{datetime.now()} OCR failed for FID {player_id} on attempt {attempt + 1}\n")
                    status = "OCR_FAILED_ATTEMPT"
                    if attempt == max_ocr_attempts - 1:
                        status = "MAX_CAPTCHA_ATTEMPTS_REACHED"
                    else:
                        status = "OCR_FAILED_ATTEMPT"
                    if status == "MAX_CAPTCHA_ATTEMPTS_REACHED": break
                    else: continue

                self.giftlog.info(f"{datetime.now()} OCR solved for {player_id}: {captcha_code} (meth:{method}, conf:{confidence:.2f}, att:{attempt+1})\n")

                data_to_encode = {"fid": f"{player_id}", "cdk": giftcode, "captcha_code": captcha_code, "time": f"{int(datetime.now().timestamp()*1000)}"}
                data = self.encode_data(data_to_encode)
                self.processing_stats["captcha_submissions"] += 1
                response_giftcode = session.post(self.wos_giftcode_url, data=data)

                log_entry_redeem = f"\n{datetime.now()} API REQ - Gift Code Redeem\nFID:{player_id}, Code:{giftcode}, Captcha:{captcha_code}\n"
                try:
                    response_json_redeem = response_giftcode.json()
                    log_entry_redeem += f"Resp Code: {response_giftcode.status_code}\nResponse JSON:\n{json.dumps(response_json_redeem, indent=2)}\n"
                except json.JSONDecodeError:
                    response_json_redeem = {}
                    log_entry_redeem += f"Resp Code: {response_giftcode.status_code}\nResponse Text (Not JSON): {response_giftcode.text[:500]}...\n"
                log_entry_redeem += "-" * 50 + "\n"
                self.giftlog.info(log_entry_redeem.strip())

                msg = response_json_redeem.get("msg", "Unknown Error").strip('.')
                err_code = response_json_redeem.get("err_code")

                is_captcha_error = (msg == "CAPTCHA CHECK ERROR" and err_code == 40103)
                is_captcha_rate_limit = (msg == "CAPTCHA CHECK TOO FREQUENT" and err_code == 40101)
                if is_captcha_error:
                    self.processing_stats["server_validation_failure"] += 1
                    status = "CAPTCHA_INVALID"
                elif not is_captcha_rate_limit:
                    self.processing_stats["server_validation_success"] += 1
                
                if msg == "CAPTCHA CHECK ERROR" and err_code == 40103:
                    status = "CAPTCHA_INVALID"
                elif msg == "CAPTCHA CHECK TOO FREQUENT" and err_code == 40101:
                    status = "TIMEOUT_RETRY"
                elif msg == "NOT LOGIN":
                    status = "LOGIN_EXPIRED_MID_PROCESS"
                elif msg == "SUCCESS":
                    status = "SUCCESS"
                elif msg == "RECEIVED" and err_code == 40008:
                    status = "RECEIVED"
                elif msg == "SAME TYPE EXCHANGE" and err_code == 40011:
                    status = "SAME TYPE EXCHANGE"
                elif msg == "TIME ERROR" and err_code == 40007:
                    status = "TIME_ERROR"
                elif msg == "CDK NOT FOUND" and err_code == 40014:
                    status = "CDK_NOT_FOUND"
                elif msg == "USED" and err_code == 40005:
                    status = "USAGE_LIMIT"
                elif msg == "TIMEOUT RETRY" and err_code == 40004:
                    status = "TIMEOUT_RETRY"
                elif "sign error" in msg.lower():
                    status = "SIGN_ERROR"
                    # Log the request that caused the sign error for debugging purposes
                    self.logger.error(f"[SIGN ERROR] Sign error detected for FID {player_id}, code {giftcode}")
                    self.logger.error(f"[SIGN ERROR] Original request data: fid={player_id}, cdk={giftcode}, captcha_code={captcha_code}, time={int(datetime.now().timestamp()*1000)}")
                    debug_data_to_encode = {"fid": f"{player_id}", "cdk": giftcode, "captcha_code": captcha_code, "time": f"{int(datetime.now().timestamp()*1000)}"}
                    self.encode_data(debug_data_to_encode, debug_sign_error=True)
                    self.logger.error(f"[SIGN ERROR] Response that caused sign error: {response_json_redeem}")
                else:
                    status = "UNKNOWN_API_RESPONSE"
                    self.giftlog.info(f"Unknown API response for {player_id}: msg='{msg}', err_code={err_code}\n")

                if player_id != self.get_test_fid() and status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
                    try:
                        self.cursor.execute("""
                            INSERT OR REPLACE INTO user_giftcodes (fid, giftcode, status)
                            VALUES (?, ?, ?)
                        """, (player_id, giftcode, status))
                        
                        self.cursor.execute("""
                            UPDATE gift_codes 
                            SET validation_status = 'validated' 
                            WHERE giftcode = ? AND validation_status = 'pending'
                        """, (giftcode,))
                        
                        if self.cursor.rowcount > 0: # If this code was just validated for the first time, send to API
                            self.logger.info(f"Code '{giftcode}' validated for the first time - sending to API")
                            try:
                                asyncio.create_task(self.api.add_giftcode(giftcode))
                            except Exception as api_err:
                                self.logger.exception(f"Error sending validated code '{giftcode}' to API: {api_err}")
                        
                        self.conn.commit()
                        self.giftlog.info(f"DATABASE - Saved/Updated status for User {player_id}, Code '{giftcode}', Status {status}\n")
                    except Exception as db_err:
                        self.giftlog.exception(f"DATABASE ERROR saving/replacing status for {player_id}/{giftcode}: {db_err}\n")
                        self.giftlog.exception(f"STACK TRACE: {traceback.format_exc()}\n")

                if status == "CAPTCHA_INVALID":
                    if attempt == max_ocr_attempts - 1:
                        self.logger.warning(f"GiftOps: Max OCR attempts reached after CAPTCHA_INVALID for FID {player_id}.")
                        break
                    else:
                        self.logger.info(f"GiftOps: CAPTCHA_INVALID for FID {player_id} on attempt {attempt + 1}. Retrying fetch/solve...")
                        await asyncio.sleep(random.uniform(1.5, 2.5))
                        continue
                else:
                    break
                
        except Exception as e:
            error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_details = traceback.format_exc()
            log_message = (
                f"\n--- UNEXPECTED ERROR in claim_giftcode_rewards_wos ({error_timestamp}) ---\n"
                f"Player ID: {player_id}, Gift Code: {giftcode}\nError: {str(e)}\n"
                f"Traceback:\n{error_details}\n"
                f"---------------------------------------------------------------------\n"
            )
            self.logger.exception(f"GiftOps: UNEXPECTED Error claiming code {giftcode} for FID {player_id}. Details logged.")
            try:
                self.giftlog.error(log_message.strip())
            except Exception as log_e: self.logger.exception(f"GiftOps: CRITICAL - Failed to write unexpected error log: {log_e}")
            status = "ERROR"

        finally:
            process_end_time = time.time()
            duration = process_end_time - process_start_time
            self.processing_stats["total_fids_processed"] += 1
            self.processing_stats["total_processing_time"] += duration
            self.logger.info(f"GiftOps: claim_giftcode_rewards_wos completed for FID {player_id}. Status: {status}, Duration: {duration:.3f}s")

        # Image save handling
        if image_bytes and self.captcha_solver and self.captcha_solver.save_images_mode > 0:
            save_mode = self.captcha_solver.save_images_mode
            should_save = False
            filename_base = None
            log_prefix = ""

            is_success = status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]
            is_fail_server = status == "CAPTCHA_INVALID"

            if is_success and save_mode in [2, 3]:
                should_save = True
                log_prefix = f"Captcha OK (Solver: {method})"
                solved_code_str = captcha_code if captcha_code else "UNKNOWN_SOLVE"
                filename_base = f"{solved_code_str}.png"
            elif is_fail_server and save_mode in [1, 3]:
                should_save = True
                log_prefix = f"Captcha Fail Server (Solver: {method} -> {status})"
                solved_code_str = captcha_code if captcha_code else "UNKNOWN_SENT"
                timestamp = int(time.time())
                filename_base = f"FAIL_SERVER_{solved_code_str}_{timestamp}.png"

            if should_save and filename_base:
                try:
                    save_path = os.path.join(self.captcha_solver.captcha_dir, filename_base)
                    counter = 1
                    base, ext = os.path.splitext(filename_base)
                    while os.path.exists(save_path) and counter <= 100:
                        save_path = os.path.join(self.captcha_solver.captcha_dir, f"{base}_{counter}{ext}")
                        counter += 1

                    if counter > 100:
                        self.logger.warning(f"Could not find unique filename for {filename_base} after 100 tries. Discarding image.")
                    else:
                        with open(save_path, "wb") as f:
                            f.write(image_bytes)
                        self.logger.info(f"GiftOps: {log_prefix} - Saved captcha image as {os.path.basename(save_path)}")

                except Exception as save_err:
                    self.logger.exception(f"GiftOps: Error saving captcha image ({filename_base}): {save_err}")

        self.logger.info(f"GiftOps: Final status for FID {player_id} / Code '{giftcode}': {status}")
        return status

    @tasks.loop(seconds=1800)
    async def check_channels_loop(self):
        log_file_path = os.path.join(self.log_directory, 'giftlog.txt')
        loop_start_time = datetime.now()
        self.logger.info(f"\nGiftOps: check_channels_loop running at {loop_start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        try:
            # Step 1: Get Valid Channels
            self.cursor.execute("SELECT channel_id, alliance_id FROM giftcode_channel")
            channel_configs = self.cursor.fetchall()
            if not channel_configs: return

            valid_channels = []
            invalid_channel_ids_to_remove = []
            self.logger.info(f"GiftOps: [Loop] Validating {len(channel_configs)} configured channels...")
            for channel_id, alliance_id in channel_configs:
                channel = self.bot.get_channel(channel_id)
                if isinstance(channel, discord.TextChannel):
                    perms = channel.permissions_for(channel.guild.me)
                    if perms.read_message_history and perms.add_reactions and perms.send_messages:
                        valid_channels.append(channel)
                    else:
                        self.logger.warning(f"GiftOps: [Loop] WARNING - Missing permissions in channel {channel_id}. Skipping.")
                else:
                    self.logger.warning(f"GiftOps: [Loop] WARNING - Channel ID {channel_id} (Alliance: {alliance_id}) invalid. Marking for removal.")
                    invalid_channel_ids_to_remove.append(channel_id)

            if invalid_channel_ids_to_remove:
                unique_invalid_ids = list(set(invalid_channel_ids_to_remove))
                self.logger.info(f"GiftOps: [Loop] Removing {len(unique_invalid_ids)} invalid channel configs: {unique_invalid_ids}")
                placeholders = ','.join('?' * len(unique_invalid_ids))
                try:
                    self.cursor.execute(f"DELETE FROM giftcode_channel WHERE channel_id IN ({placeholders})", unique_invalid_ids)
                    self.conn.commit()
                except sqlite3.Error as db_err:
                    self.logger.exception(f"GiftOps: [Loop] DB ERROR removing invalid channels: {db_err}")

            if not valid_channels:
                self.logger.info("GiftOps: [Loop] No valid gift code channels found.")
                return

            # Step 2: Collect Processable Messages and Unique Codes
            all_processable_messages = []
            fetch_limit = 75

            for channel in valid_channels:
                self.logger.info(f"GiftOps: [Loop] Scanning channel {channel.id} ({channel.name})...")
                try:
                    # Find last definitive reaction timestamp
                    last_definitive_reaction_time = None
                    async for msg_hist in channel.history(limit=fetch_limit, oldest_first=False):
                        if msg_hist.reactions:
                            if any(reaction.me and str(reaction.emoji) in ["✅", "❌", "⚠️", "❓", "ℹ️"] for reaction in msg_hist.reactions):
                                last_definitive_reaction_time = msg_hist.created_at
                                break

                    # Determine messages needing potential processing
                    channel_messages = []
                    if last_definitive_reaction_time:
                        channel_messages = [msg async for msg in channel.history(limit=fetch_limit, after=last_definitive_reaction_time, oldest_first=True)]
                    else:
                        channel_messages = [msg async for msg in channel.history(limit=fetch_limit, oldest_first=True)]

                    all_processable_messages.extend(channel_messages)

                except discord.Forbidden as forbidden_err:
                    self.logger.exception(f"GiftOps: [Loop] ERROR - Perms error in channel {channel.id}: {forbidden_err}.")
                except Exception as channel_err:
                    self.logger.exception(f"GiftOps: [Loop] ERROR - scanning channel {channel.id}: {channel_err}")
                    traceback.print_exc()

            # Step 3: Filter Messages and Extract Unique Codes
            unique_codes_to_validate = set()
            processed_code_statuses = {}
            code_message_map = {}

            self.logger.info(f"GiftOps: [Loop] Processing {len(all_processable_messages)} potential messages across all channels.")
            for message in all_processable_messages:
                if message.author == self.bot.user or not message.content:
                    continue

                bot_reactions = {str(reaction.emoji) for reaction in message.reactions if reaction.me}
                if bot_reactions.intersection(["✅", "❌", "⚠️", "❓", "ℹ️"]):
                    continue

                content = message.content.strip()
                giftcode = None
                if len(content.split()) == 1:
                    if re.match(r'^[a-zA-Z0-9]+$', content):
                        giftcode = content
                else:
                    code_match = re.search(r'Code:\s*(\S+)', content, re.IGNORECASE)
                    if code_match:
                        potential_code = code_match.group(1)
                        if re.match(r'^[a-zA-Z0-9]+$', potential_code):
                            giftcode = potential_code

                if giftcode:
                    giftcode = self.clean_gift_code(giftcode)
                if not giftcode:
                    continue

                if giftcode not in code_message_map:
                    code_message_map[giftcode] = []
                code_message_map[giftcode].append(message)

                if giftcode not in processed_code_statuses:
                    unique_codes_to_validate.add(giftcode)

            self.logger.info(f"GiftOps: [Loop] Processing {len(unique_codes_to_validate)} unique codes...")
            codes_added_this_run = set()

            for code in unique_codes_to_validate:
                # Check if already in main gift_codes DB
                self.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
                if self.cursor.fetchone():
                    self.logger.info(f"GiftOps: [Loop] Code {code} already in gift_codes DB.")
                    processed_code_statuses[code] = "SUCCESS"
                    continue
                
                # Add new code without validation
                self.logger.info(f"GiftOps: [Loop] Adding new code '{code}' to database as pending validation.")
                try:
                    self.cursor.execute("INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)", 
                                    (code, datetime.now().strftime("%Y-%m-%d"), "pending"))
                    self.conn.commit()
                    codes_added_this_run.add(code)
                    processed_code_statuses[code] = "PENDING"

                    self.logger.info(f"GiftOps: [Loop] Code '{code}' added as pending - will send to API after validation.")

                    self.cursor.execute("SELECT alliance_id FROM giftcodecontrol WHERE status = 1")
                    auto_alliances = self.cursor.fetchall()
                    if auto_alliances:
                        self.logger.info(f"GiftOps: [Loop] Triggering auto-use for {len(auto_alliances)} alliances for '{code}'.")
                        for alliance in auto_alliances:
                            await self.use_giftcode_for_alliance(alliance[0], code)
                except sqlite3.Error as db_ins_err:
                    self.logger.exception(f"GiftOps: [Loop] DB ERROR inserting code '{code}': {db_ins_err}")
                    processed_code_statuses[code] = "ERROR"

            # Step 5: Apply reactions based on status
            self.logger.info(f"GiftOps: [Loop] Applying reactions to messages...")
            for code, status in processed_code_statuses.items():
                if code not in code_message_map: 
                    continue

                reaction_to_add = "✅" if status == "SUCCESS" else "⚠️"
                
                for message in code_message_map[code]:
                    if message.reactions:
                        for reaction in message.reactions:
                            if reaction.me:
                                try:
                                    await message.remove_reaction(reaction.emoji, self.bot.user)
                                except (discord.Forbidden, discord.NotFound): 
                                    pass

                    if reaction_to_add:
                        try:
                            await message.add_reaction(reaction_to_add)
                        except (discord.Forbidden, discord.NotFound):
                            self.logger.exception(f"GiftOps: [Loop] Failed to add reaction '{reaction_to_add}' to msg {message.id}")

            loop_end_time = datetime.now()
            self.logger.info(f"GiftOps: check_channels_loop finished at {loop_end_time.strftime('%Y-%m-%d %H:%M:%S')}. Duration: {loop_end_time - loop_start_time}\n")

        except Exception as e:
            self.logger.exception(f"GiftOps: [Loop] FATAL ERROR in check_channels_loop: {str(e)}")
            self.logger.exception(f"Traceback: {traceback.format_exc()}")
            error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            error_details = traceback.format_exc()
            log_message_loop = (
                f"\n--- FATAL ERROR in check_channels_loop ({error_timestamp}) ---\n"
                f"Error: {str(e)}\nTraceback:\n{error_details}\n"
                f"------------------------------------------------------------\n"
            )
            try:
                self.giftlog.info(log_message_loop.strip())
            except Exception as log_e:
                self.logger.exception(f"GiftOps: CRITICAL - Failed to write loop error log: {log_e}")

    @check_channels_loop.before_loop
    async def before_check_channels_loop(self):
        self.logger.info("GiftOps: Waiting for bot to be ready before starting check_channels_loop...")
        await self.bot.wait_until_ready()
        self.logger.info("GiftOps: Bot is ready, check_channels_loop starting.")

    async def fetch_captcha(self, player_id, session=None):
        """Fetch a captcha image for a player ID."""
        if session is None:
            session = requests.Session()
            session.mount("https://", HTTPAdapter(max_retries=self.retry_config))
            
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/x-www-form-urlencoded",
            "origin": self.wos_giftcode_redemption_url,
        }
        
        data_to_encode = {
            "fid": player_id,
            "time": f"{int(datetime.now().timestamp() * 1000)}",
            "init": "0"
        }
        data = self.encode_data(data_to_encode)
        
        try:
            response = session.post(
                self.wos_captcha_url,
                headers=headers,
                data=data,
            )
            
            if response.status_code == 200:
                captcha_data = response.json()
                if captcha_data.get("code") == 1 and captcha_data.get("msg") == "CAPTCHA GET TOO FREQUENT.":
                    return None, "CAPTCHA_TOO_FREQUENT"
                    
                if "data" in captcha_data and "img" in captcha_data["data"]:
                    return captcha_data["data"]["img"], None
            
            return None, "CAPTCHA_FETCH_ERROR"
        except Exception as e:
            self.logger.exception(f"Error fetching captcha: {e}")
            return None, f"CAPTCHA_EXCEPTION: {str(e)}"

    async def show_ocr_settings(self, interaction: discord.Interaction):
            """Show OCR settings menu."""
            try:
                self.settings_cursor.execute("SELECT is_initial FROM admin WHERE id = ?", (interaction.user.id,))
                admin_info = self.settings_cursor.fetchone()

                if not admin_info or admin_info[0] != 1:
                    error_msg = "❌ You don't have permission to access OCR settings."
                    if interaction.response.is_done():
                        await interaction.followup.send(error_msg, ephemeral=True)
                    else:
                        await interaction.response.send_message(error_msg, ephemeral=True)
                    return

                self.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
                ocr_settings = self.settings_cursor.fetchone()

                if not ocr_settings:
                    self.logger.warning("No OCR settings found in DB, inserting defaults.")
                    self.settings_cursor.execute("INSERT INTO ocr_settings (enabled, save_images) VALUES (1, 0)")
                    self.settings_conn.commit()
                    ocr_settings = (1, 0)

                enabled, save_images_setting = ocr_settings
                current_test_fid = self.get_test_fid()

                onnx_available = False
                solver_status_msg = "N/A"
                if self.captcha_solver:
                    if self.captcha_solver.is_initialized:
                        onnx_available = True
                        solver_status_msg = "Initialized & Ready"
                    elif hasattr(self.captcha_solver, 'is_initialized'):
                        onnx_available = True
                        solver_status_msg = "Initialization Failed (Check Logs)"
                    else:
                        solver_status_msg = "Error (Instance missing flags)"
                else:
                    try:
                        import onnxruntime
                        onnx_available = True
                        solver_status_msg = "Disabled or Init Failed"
                    except ImportError:
                        onnx_available = False
                        solver_status_msg = "onnxruntime library missing"

                save_options_text = {
                    0: "❌ None", 1: "⚠️ Failed Only", 2: "✅ Success Only", 3: "💾 All"
                }
                save_images_display = save_options_text.get(save_images_setting, f"Unknown ({save_images_setting})")

                embed = discord.Embed(
                    title="🔍 CAPTCHA Solver Settings (ONNX)",
                    description=(
                        f"Configure the automatic CAPTCHA solver for gift code redemption.\n\n"
                        f"**Current Settings**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🤖 **OCR Enabled:** {'✅ Yes' if enabled == 1 else '❌ No'}\n"
                        f"💾 **Save CAPTCHA Images:** {save_images_display}\n"
                        f"🆔 **Test FID:** `{current_test_fid}`\n"
                        f"📦 **ONNX Runtime:** {'✅ Found' if onnx_available else '❌ Missing'}\n"
                        f"⚙️ **Solver Status:** `{solver_status_msg}`\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    ),
                    color=discord.Color.blue()
                )

                if not onnx_available:
                    embed.add_field(
                        name="⚠️ Missing Library",
                        value=(
                            "ONNX Runtime and required libraries are needed for CAPTCHA solving.\n"
                            "The model files must be in the bot/models/ directory.\n"
                            "Try installing dependencies:\n"
                            "```pip install onnxruntime pillow numpy\n"
                        ), inline=False
                    )

                stats_lines = []
                stats_lines.append("**Captcha Solver (Raw Format):**")
                ocr_calls = self.processing_stats['ocr_solver_calls']
                ocr_valid = self.processing_stats['ocr_valid_format']
                ocr_format_rate = (ocr_valid / ocr_calls * 100) if ocr_calls > 0 else 0
                stats_lines.append(f"• Solver Calls: `{ocr_calls}`")
                stats_lines.append(f"• Valid Format Returns: `{ocr_valid}` ({ocr_format_rate:.1f}%)")

                stats_lines.append("\n**Redemption Process (Server Side):**")
                submissions = self.processing_stats['captcha_submissions']
                server_success = self.processing_stats['server_validation_success']
                server_fail = self.processing_stats['server_validation_failure']
                total_server_val = server_success + server_fail
                server_pass_rate = (server_success / total_server_val * 100) if total_server_val > 0 else 0
                stats_lines.append(f"• Captcha Submissions: `{submissions}`")
                stats_lines.append(f"• Server Validation Success: `{server_success}`")
                stats_lines.append(f"• Server Validation Failure: `{server_fail}`")
                stats_lines.append(f"• Server Pass Rate: `{server_pass_rate:.1f}%`")

                total_fids = self.processing_stats['total_fids_processed']
                total_time = self.processing_stats['total_processing_time']
                avg_time = (total_time / total_fids if total_fids > 0 else 0)
                stats_lines.append(f"• Avg. FID Processing Time: `{avg_time:.2f}s` (over `{total_fids}` FIDs)")

                embed.add_field(
                    name="📊 Processing Statistics (Since Bot Start)",
                    value="\n".join(stats_lines),
                    inline=False
                )

                embed.add_field(
                    name="⚠️ Important Note",
                    value="Saving images (especially 'All') can consume significant disk space over time.",
                    inline=False
                )

                view = OCRSettingsView(self, ocr_settings, onnx_available)

                if interaction.response.is_done():
                    try:
                        await interaction.edit_original_response(embed=embed, view=view)
                    except discord.NotFound:
                        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                    except Exception as e_edit:
                        self.logger.exception(f"Error editing original response in show_ocr_settings: {e_edit}")
                        await interaction.followup.send(embed=embed, view=view, ephemeral=True)
                else:
                    await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

            except sqlite3.Error as db_err:
                self.logger.exception(f"Database error in show_ocr_settings: {db_err}")
                error_message = "❌ A database error occurred while loading OCR settings."
                if interaction.response.is_done(): await interaction.followup.send(error_message, ephemeral=True)
                else: await interaction.response.send_message(error_message, ephemeral=True)
            except Exception as e:
                self.logger.exception(f"Error showing OCR settings: {e}")
                traceback.print_exc()
                error_message = "❌ An unexpected error occurred while loading OCR settings."
                if interaction.response.is_done():
                    await interaction.followup.send(error_message, ephemeral=True)
                else:
                    await interaction.response.send_message(error_message, ephemeral=True)

    async def update_ocr_settings(self, interaction, enabled=None, save_images=None):
        """Update OCR settings in the database and reinitialize the solver if needed."""
        try:
            self.settings_cursor.execute("SELECT enabled, save_images FROM ocr_settings ORDER BY id DESC LIMIT 1")
            current_settings = self.settings_cursor.fetchone()
            if not current_settings:
                current_settings = (1, 0)

            current_enabled, current_save_images = current_settings

            target_enabled = enabled if enabled is not None else current_enabled
            target_save_images = save_images if save_images is not None else current_save_images

            self.settings_cursor.execute("""
                UPDATE ocr_settings SET enabled = ?, save_images = ?
                WHERE id = (SELECT MAX(id) FROM ocr_settings)
                """, (target_enabled, target_save_images))
            if self.settings_cursor.rowcount == 0:
                self.settings_cursor.execute("""
                    INSERT INTO ocr_settings (enabled, save_images) VALUES (?, ?)
                    """, (target_enabled, target_save_images))
            self.settings_conn.commit()
            self.logger.info(f"GiftOps: Updated OCR settings in DB -> Enabled={target_enabled}, SaveImages={target_save_images}")

            message_suffix = "Settings updated."
            reinitialize_solver = False

            if enabled is not None and enabled != current_enabled:
                reinitialize_solver = True
                message_suffix = f"Solver has been {'enabled' if target_enabled == 1 else 'disabled'}."
            
            if save_images is not None and self.captcha_solver and self.captcha_solver.is_initialized:
                self.captcha_solver.save_images_mode = target_save_images
                self.logger.info(f"GiftOps: Updated live captcha_solver.save_images_mode to {target_save_images}")
                if not reinitialize_solver:
                    message_suffix = "Image saving preference updated."

            if reinitialize_solver:
                self.captcha_solver = None
                if target_enabled == 1:
                    self.logger.info("GiftOps: OCR is being enabled/reinitialized...")
                    try:
                        self.captcha_solver = GiftCaptchaSolver(save_images=target_save_images)
                        if self.captcha_solver.is_initialized:
                            self.logger.info("GiftOps: ONNX solver reinitialized successfully.")
                            message_suffix += " Solver reinitialized."
                        else:
                            self.logger.error("GiftOps: ONNX solver FAILED to reinitialize.")
                            message_suffix += " Solver reinitialization failed."
                            self.captcha_solver = None
                            return False, f"CAPTCHA solver settings updated. {message_suffix}"
                    except ImportError as imp_err:
                        self.logger.exception(f"GiftOps: ERROR - Reinitialization failed: Missing library {imp_err}")
                        message_suffix += f" Solver initialization failed (Missing Library: {imp_err})."
                        self.captcha_solver = None
                        return False, f"CAPTCHA solver settings updated. {message_suffix}"
                    except Exception as e:
                        self.logger.exception(f"GiftOps: ERROR - Reinitialization failed: {e}")
                        message_suffix += f" Solver initialization failed ({e})."
                        self.captcha_solver = None
                        return False, f"CAPTCHA solver settings updated. {message_suffix}"
                else:
                    self.logger.info("GiftOps: OCR disabled, solver instance removed/kept None.")

            return True, f"CAPTCHA solver settings: {message_suffix}"

        except sqlite3.Error as db_err:
            self.logger.exception(f"Database error updating OCR settings: {db_err}")
            return False, f"Database error updating OCR settings: {db_err}"
        except Exception as e:
            self.logger.exception(f"Unexpected error updating OCR settings: {e}")
            return False, f"Unexpected error updating OCR settings: {e}"

    async def validate_gift_codes(self):
        try:
            self.cursor.execute("SELECT giftcode, validation_status FROM gift_codes WHERE validation_status != 'invalid'")
            all_codes = self.cursor.fetchall()
            
            self.settings_cursor.execute("SELECT id FROM admin WHERE is_initial = 1")
            admin_ids = [row[0] for row in self.settings_cursor.fetchall()]
            
            if not all_codes:
                self.logger.info("[validate_gift_codes] No codes found needing validation.")
                return

            for giftcode, current_db_status in all_codes:
                if current_db_status == 'invalid':
                    self.logger.info(f"[validate_gift_codes] Skipping already invalid code: {giftcode}")
                    continue

                self.logger.info(f"[validate_gift_codes] Validating code: {giftcode} (current DB status: {current_db_status})")
                test_fid = self.get_test_fid()
                status = await self.claim_giftcode_rewards_wos(test_fid, giftcode)

                if status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                    self.logger.info(f"[validate_gift_codes] Code {giftcode} found to be invalid with status: {status}. Updating DB.")
                    
                    self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                    test_fid = self.get_test_fid()
                    self.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ? AND fid = ?", (giftcode, test_fid))
                    self.conn.commit()
                    
                    if hasattr(self, 'api') and self.api:
                        asyncio.create_task(self.api.remove_giftcode(giftcode, from_validation=True))

                    reason_map = {
                        "TIME_ERROR": "Code has expired (TIME_ERROR)",
                        "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                        "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                    }
                    detailed_reason = reason_map.get(status, f"Code invalid ({status})")

                    admin_embed = discord.Embed(
                        title="🎁 Gift Code Invalidated",
                        description=(
                            f"**Gift Code Details**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"🎁 **Gift Code:** `{giftcode}`\n"
                            f"❌ **Status:** {detailed_reason}\n"
                            f"📝 **Action:** Code marked as invalid in database\n"
                            f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        ),
                        color=discord.Color.orange()
                    )
                    
                    for admin_id in admin_ids:
                        try:
                            admin_user = await self.bot.fetch_user(admin_id)
                            if admin_user:
                                await admin_user.send(embed=admin_embed)
                        except Exception as e:
                            self.logger.exception(f"Error sending message to admin {admin_id}: {str(e)}")
                
                elif status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"] and current_db_status == 'pending':
                    self.logger.info(f"[validate_gift_codes] Code {giftcode} confirmed valid. Updating status to 'validated'.")
                    self.cursor.execute("UPDATE gift_codes SET validation_status = 'validated' WHERE giftcode = ? AND validation_status = 'pending'", (giftcode,))
                    self.conn.commit()

                    if hasattr(self, 'api') and self.api:
                        asyncio.create_task(self.api.add_giftcode(giftcode))
                    
                await asyncio.sleep(60)
                
        except Exception as e:
            self.logger.exception(f"Error in validate_gift_codes: {str(e)}")

    async def handle_success(self, message, giftcode):
        test_fid = self.get_test_fid()
        status = await self.claim_giftcode_rewards_wos(test_fid, giftcode)
        
        if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
            self.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
            if not self.cursor.fetchone():
                self.cursor.execute("INSERT INTO gift_codes (giftcode, date) VALUES (?, ?)", (giftcode, datetime.now()))
                self.conn.commit()
                
                try:
                    asyncio.create_task(self.api.add_giftcode(giftcode))
                except:
                    pass
                
                await message.add_reaction("✅")
                await message.reply("Gift code successfully added.", mention_author=False)
        elif status == "TIME_ERROR":
            await self.handle_time_error(message)
        elif status == "CDK_NOT_FOUND":
            await self.handle_cdk_not_found(message)
        elif status == "USAGE_LIMIT":
            await message.add_reaction("❌")
            await message.reply("Usage limit has been reached for this code.", mention_author=False)

    async def handle_already_received(self, message, giftcode):
        test_fid = self.get_test_fid()
        status = await self.claim_giftcode_rewards_wos(test_fid, giftcode)
        
        if status in ["SUCCESS", "RECEIVED", "SAME TYPE EXCHANGE"]:
            self.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
            if not self.cursor.fetchone():
                self.cursor.execute("INSERT INTO gift_codes (giftcode, date) VALUES (?, ?)", (giftcode, datetime.now()))
                self.conn.commit()
                
                try:
                    asyncio.create_task(self.api.add_giftcode(giftcode))
                except:
                    pass
                
                await message.add_reaction("✅")
                await message.reply("Gift code successfully added.", mention_author=False)
        elif status == "TIME_ERROR":
            await self.handle_time_error(message)
        elif status == "CDK_NOT_FOUND":
            await self.handle_cdk_not_found(message)
        elif status == "USAGE_LIMIT":
            await message.add_reaction("❌")
            await message.reply("Usage limit has been reached for this code.", mention_author=False)

    async def handle_cdk_not_found(self, message):
        await message.add_reaction("❌")
        await message.reply("The gift code is incorrect.", mention_author=False)

    async def handle_time_error(self, message):
        await message.add_reaction("❌")
        await message.reply("Gift code expired.", mention_author=False)

    async def handle_timeout_retry(self, message, giftcode):
        self.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (giftcode,))
        if not self.cursor.fetchone():
            await message.add_reaction("⏳")

    async def get_admin_info(self, user_id):
        self.settings_cursor.execute("""
            SELECT id, is_initial FROM admin WHERE id = ?
        """, (user_id,))
        return self.settings_cursor.fetchone()

    async def get_alliance_names(self, user_id, is_global=False):
        if is_global:
            self.alliance_cursor.execute("SELECT name FROM alliance_list")
            return [row[0] for row in self.alliance_cursor.fetchall()]
        else:
            self.settings_cursor.execute("""
                SELECT alliances_id FROM adminserver WHERE admin = ?
            """, (user_id,))
            alliance_ids = [row[0] for row in self.settings_cursor.fetchall()]
            
            if alliance_ids:
                placeholders = ','.join('?' * len(alliance_ids))
                self.alliance_cursor.execute(f"""
                    SELECT name FROM alliance_list 
                    WHERE alliance_id IN ({placeholders})
                """, alliance_ids)
                return [row[0] for row in self.alliance_cursor.fetchall()]
            return []

    async def get_available_alliances(self, interaction: discord.Interaction):
        user_id = interaction.user.id
        guild_id = interaction.guild_id if interaction.guild else None

        admin_info = await self.get_admin_info(user_id)
        if not admin_info:
            return []

        is_global = admin_info[1] == 1

        if is_global:
            self.alliance_cursor.execute("SELECT alliance_id, name FROM alliance_list")
            return self.alliance_cursor.fetchall()

        if guild_id:
            self.alliance_cursor.execute("""
                SELECT DISTINCT alliance_id, name 
                FROM alliance_list 
                WHERE discord_server_id = ?
            """, (guild_id,))
            guild_alliances = self.alliance_cursor.fetchall()

            self.settings_cursor.execute("""
                SELECT alliances_id FROM adminserver WHERE admin = ?
            """, (user_id,))
            special_alliance_ids = [row[0] for row in self.settings_cursor.fetchall()]

            if special_alliance_ids:
                placeholders = ','.join('?' * len(special_alliance_ids))
                self.alliance_cursor.execute(f"""
                    SELECT alliance_id, name FROM alliance_list 
                    WHERE alliance_id IN ({placeholders})
                """, special_alliance_ids)
                special_alliances = self.alliance_cursor.fetchall()
            else:
                special_alliances = []

            all_alliances = list(set(guild_alliances + special_alliances))
            return all_alliances

        return []

    async def setup_gift_channel(self, interaction: discord.Interaction):
        admin_info = await self.get_admin_info(interaction.user.id)
        if not admin_info:
            await interaction.response.send_message(
                "❌ You are not authorized to perform this action.",
                ephemeral=True
            )
            return

        available_alliances = await self.get_available_alliances(interaction)
        if not available_alliances:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="❌ No Available Alliances",
                    description="You don't have access to any alliances.",
                    color=discord.Color.red()
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

        self.cursor.execute("SELECT alliance_id, channel_id FROM giftcode_channel")
        current_channels = dict(self.cursor.fetchall())

        alliance_embed = discord.Embed(
            title="📢 Gift Code Channel Setup",
            description=(
                "Please select an alliance to set up gift code channel:\n\n"
                "**Alliance List**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "Select an alliance from the list below:\n"
            ),
            color=discord.Color.blue()
        )

        view = AllianceSelectView(alliances_with_counts, self)

        async def alliance_callback(select_interaction: discord.Interaction):
            try:
                alliance_id = int(view.current_select.values[0])
                
                channel_embed = discord.Embed(
                    title="📢 Gift Code Channel Setup",
                    description=(
                        "**Instructions:**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        "Please select a channel for gift codes\n\n"
                        "**Page:** 1/1\n"
                        f"**Total Channels:** {len(select_interaction.guild.text_channels)}"
                    ),
                    color=discord.Color.blue()
                )

                async def channel_select_callback(channel_interaction: discord.Interaction):
                    try:
                        channel_id = int(channel_interaction.data["values"][0])
                        
                        self.cursor.execute("""
                            INSERT OR REPLACE INTO giftcode_channel (alliance_id, channel_id)
                            VALUES (?, ?)
                        """, (alliance_id, channel_id))
                        self.conn.commit()

                        alliance_name = next((name for aid, name in available_alliances if aid == alliance_id), "Unknown Alliance")

                        success_embed = discord.Embed(
                            title="✅ Gift Code Channel Set",
                            description=(
                                f"Successfully set gift code channel:\n\n"
                                f"🏰 **Alliance:** {alliance_name}\n"
                                f"📝 **Channel:** <#{channel_id}>\n"
                            ),
                            color=discord.Color.green()
                        )

                        await channel_interaction.response.edit_message(
                            embed=success_embed,
                            view=None
                        )

                    except Exception as e:
                        self.logger.exception(f"Error setting gift code channel: {e}")
                        await channel_interaction.response.send_message(
                            "❌ An error occurred while setting the gift code channel.",
                            ephemeral=True
                        )

                channels = select_interaction.guild.text_channels
                channel_view = PaginatedChannelView(channels, channel_select_callback)

                if not select_interaction.response.is_done():
                    await select_interaction.response.edit_message(
                        embed=channel_embed,
                        view=channel_view
                    )
                else:
                    await select_interaction.message.edit(
                        embed=channel_embed,
                        view=channel_view
                    )

            except Exception as e:
                self.logger.exception(f"Error in alliance selection: {e}")
                if not select_interaction.response.is_done():
                    await select_interaction.response.send_message(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )
                else:
                    await select_interaction.followup.send(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )

        view.callback = alliance_callback

        await interaction.response.send_message(
            embed=alliance_embed,
            view=view,
            ephemeral=True
        )

    async def show_gift_menu(self, interaction: discord.Interaction):
        gift_menu_embed = discord.Embed(
            title="🎁 Gift Code Operations",
            description=(
                "Please select an operation:\n\n"
                "**Available Operations**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "🎫 **Create Gift Code**\n"
                "└ Input a new gift code\n\n"
                "🔍 **CAPTCHA Settings**\n"
                "└ Configure OCR and image saving options\n\n"
                "📋 **List Gift Codes**\n"
                "└ View all active, valid codes\n\n"
                "❌ **Delete Gift Code**\n"
                "└ Remove existing codes\n\n"
                "📢 **Gift Code Channel**\n"
                "└ Set the channel to monitor for gift codes\n\n"
                "⚙️ **Auto Gift Settings**\n"
                "└ Configure automatic gift code usage\n\n"
                "🗑️ **Delete Gift Channel**\n"
                "└ Clear the configured gift code channel\n\n"
                "🎯 **Use Gift Code for Alliance**\n"
                "└ Redeem a gift code for one or more alliances\n"
                "━━━━━━━━━━━━━━━━━━━━━━"
            ),
            color=discord.Color.gold()
        )

        view = GiftView(self)
        try:
            await interaction.response.edit_message(embed=gift_menu_embed, view=view)
        except discord.InteractionResponded:
            pass
        except Exception:
            pass

    async def create_gift_code(self, interaction: discord.Interaction):
        self.settings_cursor.execute("SELECT 1 FROM admin WHERE id = ?", (interaction.user.id,))
        if not self.settings_cursor.fetchone():
            await interaction.response.send_message(
                "❌ You are not authorized to create gift codes.",
                ephemeral=True
            )
            return

        modal = CreateGiftCodeModal(self)
        try:
            await interaction.response.send_modal(modal)
        except Exception as e:
            self.logger.exception(f"Error showing modal: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred while showing the gift code creation form.",
                    ephemeral=True
                )

    async def list_gift_codes(self, interaction: discord.Interaction):
        self.cursor.execute("""
            SELECT 
                gc.giftcode,
                gc.date,
                COUNT(DISTINCT ugc.fid) as used_count
            FROM gift_codes gc
            LEFT JOIN user_giftcodes ugc ON gc.giftcode = ugc.giftcode
            WHERE gc.validation_status != 'invalid'
            GROUP BY gc.giftcode
            ORDER BY gc.date DESC
        """)
        
        codes = self.cursor.fetchall()
        
        if not codes:
            await interaction.response.send_message(
                "No active gift codes found in the database.",
                ephemeral=True
            )
            return

        embed = discord.Embed(
            title="🎁 Active Gift Codes",
            description="Gift codes that are pending validation or have been validated.",
            color=discord.Color.blue()
        )

        for code, date, used_count in codes:
            embed.add_field(
                name=f"Code: {code}",
                value=f"Created: {date}\nUsed by: {used_count} users",
                inline=False
            )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def delete_gift_code(self, interaction: discord.Interaction):
        try:
            settings_conn = sqlite3.connect('db/settings.sqlite')
            settings_cursor = settings_conn.cursor()
            
            settings_cursor.execute("""
                SELECT 1 FROM admin 
                WHERE id = ? AND is_initial = 1
            """, (interaction.user.id,))
            
            is_admin = settings_cursor.fetchone()
            settings_cursor.close()
            settings_conn.close()

            if not is_admin:
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title="❌ Unauthorized Access",
                        description="This action requires Global Admin privileges.",
                        color=discord.Color.red()
                    ),
                    ephemeral=True
                )
                return

            self.cursor.execute("""
                SELECT 
                    gc.giftcode,
                    gc.date,
                    COUNT(DISTINCT ugc.fid) as used_count
                FROM gift_codes gc
                LEFT JOIN user_giftcodes ugc ON gc.giftcode = ugc.giftcode
                GROUP BY gc.giftcode
                ORDER BY gc.date DESC
            """)
            
            codes = self.cursor.fetchall()
            
            if not codes:
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title="❌ No Gift Codes",
                        description="There are no gift codes in the database to delete.",
                        color=discord.Color.red()
                    ),
                    ephemeral=True
                )
                return

            select = discord.ui.Select(
                placeholder="Select a gift code to delete",
                options=[
                    discord.SelectOption(
                        label=f"Code: {code}",
                        description=f"Created: {date} | Used by: {used_count} users",
                        value=code
                    ) for code, date, used_count in codes
                ]
            )

            async def select_callback(select_interaction):
                selected_code = select_interaction.data["values"][0]
                
                confirm = discord.ui.Button(
                    style=discord.ButtonStyle.danger,
                    label="Confirm Delete",
                    custom_id="confirm"
                )
                cancel = discord.ui.Button(
                    style=discord.ButtonStyle.secondary,
                    label="Cancel",
                    custom_id="cancel"
                )

                async def button_callback(button_interaction):
                    try:
                        if button_interaction.data.get('custom_id') == "confirm":
                            try:
                                self.cursor.execute("DELETE FROM gift_codes WHERE giftcode = ?", (selected_code,))
                                self.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (selected_code,))
                                self.conn.commit()
                                
                                success_embed = discord.Embed(
                                    title="✅ Gift Code Deleted",
                                    description=(
                                        f"**Deletion Details**\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        f"🎁 **Gift Code:** `{selected_code}`\n"
                                        f"👤 **Deleted by:** {button_interaction.user.mention}\n"
                                        f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                    ),
                                    color=discord.Color.green()
                                )
                                
                                await button_interaction.response.edit_message(
                                    embed=success_embed,
                                    view=None
                                )
                                
                            except Exception as e:
                                await button_interaction.response.send_message(
                                    "❌ An error occurred while deleting the gift code.",
                                    ephemeral=True
                                )

                        else:
                            cancel_embed = discord.Embed(
                                title="❌ Deletion Cancelled",
                                description="The gift code deletion was cancelled.",
                                color=discord.Color.red()
                            )
                            await button_interaction.response.edit_message(
                                embed=cancel_embed,
                                view=None
                            )

                    except Exception as e:
                        self.logger.exception(f"Button callback error: {str(e)}")
                        try:
                            await button_interaction.response.send_message(
                                "❌ An error occurred while processing the request.",
                                ephemeral=True
                            )
                        except:
                            await button_interaction.followup.send(
                                "❌ An error occurred while processing the request.",
                                ephemeral=True
                            )

                confirm.callback = button_callback
                cancel.callback = button_callback

                confirm_view = discord.ui.View()
                confirm_view.add_item(confirm)
                confirm_view.add_item(cancel)

                confirmation_embed = discord.Embed(
                    title="⚠️ Confirm Deletion",
                    description=(
                        f"**Gift Code Details**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎁 **Selected Code:** `{selected_code}`\n"
                        f"⚠️ **Warning:** This action cannot be undone!\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    ),
                    color=discord.Color.yellow()
                )

                await select_interaction.response.edit_message(
                    embed=confirmation_embed,
                    view=confirm_view
                )

            select.callback = select_callback
            view = discord.ui.View()
            view.add_item(select)

            initial_embed = discord.Embed(
                title="🗑️ Delete Gift Code",
                description=(
                    f"**Instructions**\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"1️⃣ Select a gift code from the menu below\n"
                    f"2️⃣ Confirm your selection\n"
                    f"3️⃣ The code will be permanently deleted\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                ),
                color=discord.Color.blue()
            )

            await interaction.response.send_message(
                embed=initial_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            self.logger.exception(f"Delete gift code error: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred while processing the request.",
                ephemeral=True
            )

    async def delete_gift_channel(self, interaction: discord.Interaction):
        admin_info = await self.get_admin_info(interaction.user.id)
        if not admin_info:
            await interaction.response.send_message(
                "❌ You are not authorized to perform this action.",
                ephemeral=True
            )
            return

        available_alliances = await self.get_available_alliances(interaction)
        if not available_alliances:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="❌ No Available Alliances",
                    description="You don't have access to any alliances.",
                    color=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        self.cursor.execute("SELECT alliance_id, channel_id FROM giftcode_channel")
        current_channels = dict(self.cursor.fetchall())

        alliances_with_counts = []
        for alliance_id, name in available_alliances:
            if alliance_id in current_channels:
                with sqlite3.connect('db/users.sqlite') as users_db:
                    cursor = users_db.cursor()
                    cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                    member_count = cursor.fetchone()[0]
                    alliances_with_counts.append((alliance_id, name, member_count))

        if not alliances_with_counts:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="❌ No Channels Set",
                    description="There are no gift code channels set for your alliances.",
                    color=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        remove_embed = discord.Embed(
            title="🗑️ Remove Gift Code Channel",
            description=(
                "Select an alliance to remove its gift code channel:\n\n"
                "**Current Log Channels**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "Select an alliance from the list below:\n"
            ),
            color=discord.Color.red()
        )

        view = AllianceSelectView(alliances_with_counts, self)

        async def alliance_callback(select_interaction: discord.Interaction):
            try:
                alliance_id = int(view.current_select.values[0])
                
                self.cursor.execute("SELECT channel_id FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                channel_id = self.cursor.fetchone()[0]
                
                alliance_name = next((name for aid, name in available_alliances if aid == alliance_id), "Unknown Alliance")
                
                confirm_embed = discord.Embed(
                    title="⚠️ Confirm Removal",
                    description=(
                        f"Are you sure you want to remove the gift code channel for:\n\n"
                        f"🏰 **Alliance:** {alliance_name}\n"
                        f"📝 **Channel:** <#{channel_id}>\n\n"
                        "This action cannot be undone!"
                    ),
                    color=discord.Color.yellow()
                )

                confirm_view = discord.ui.View()
                
                async def confirm_callback(button_interaction: discord.Interaction):
                    try:
                        self.cursor.execute("DELETE FROM giftcode_channel WHERE alliance_id = ?", (alliance_id,))
                        self.conn.commit()

                        success_embed = discord.Embed(
                            title="✅ Gift Code Channel Removed",
                            description=(
                                f"Successfully removed gift code channel for:\n\n"
                                f"🏰 **Alliance:** {alliance_name}\n"
                                f"📝 **Channel:** <#{channel_id}>"
                            ),
                            color=discord.Color.green()
                        )

                        await button_interaction.response.edit_message(
                            embed=success_embed,
                            view=None
                        )

                    except Exception as e:
                        self.logger.exception(f"Error removing gift code channel: {e}")
                        await button_interaction.response.send_message(
                            "❌ An error occurred while removing the gift code channel.",
                            ephemeral=True
                        )

                async def cancel_callback(button_interaction: discord.Interaction):
                    cancel_embed = discord.Embed(
                        title="❌ Removal Cancelled",
                        description="The gift code channel removal has been cancelled.",
                        color=discord.Color.red()
                    )
                    await button_interaction.response.edit_message(
                        embed=cancel_embed,
                        view=None
                    )

                confirm_button = discord.ui.Button(
                    label="Confirm",
                    emoji="✅",
                    style=discord.ButtonStyle.danger,
                    custom_id="confirm_remove"
                )
                confirm_button.callback = confirm_callback

                cancel_button = discord.ui.Button(
                    label="Cancel",
                    emoji="❌",
                    style=discord.ButtonStyle.secondary,
                    custom_id="cancel_remove"
                )
                cancel_button.callback = cancel_callback

                confirm_view.add_item(confirm_button)
                confirm_view.add_item(cancel_button)

                if not select_interaction.response.is_done():
                    await select_interaction.response.edit_message(
                        embed=confirm_embed,
                        view=confirm_view
                    )
                else:
                    await select_interaction.message.edit(
                        embed=confirm_embed,
                        view=confirm_view
                    )

            except Exception as e:
                self.logger.exception(f"Error in alliance selection: {e}")
                if not select_interaction.response.is_done():
                    await select_interaction.response.send_message(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )
                else:
                    await select_interaction.followup.send(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )

        view.callback = alliance_callback

        await interaction.response.send_message(
            embed=remove_embed,
            view=view,
            ephemeral=True
        )

    async def setup_giftcode_auto(self, interaction: discord.Interaction):
        admin_info = await self.get_admin_info(interaction.user.id)
        if not admin_info:
            await interaction.response.send_message(
                "❌ You are not authorized to perform this action.",
                ephemeral=True
            )
            return

        available_alliances = await self.get_available_alliances(interaction)
        if not available_alliances:
            await interaction.response.send_message(
                embed=discord.Embed(
                    title="❌ No Available Alliances",
                    description="You don't have access to any alliances.",
                    color=discord.Color.red()
                ),
                ephemeral=True
            )
            return

        self.cursor.execute("SELECT alliance_id, status FROM giftcodecontrol")
        current_status = dict(self.cursor.fetchall())

        alliances_with_counts = []
        for alliance_id, name in available_alliances:
            with sqlite3.connect('db/users.sqlite') as users_db:
                cursor = users_db.cursor()
                cursor.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (alliance_id,))
                member_count = cursor.fetchone()[0]
                alliances_with_counts.append((alliance_id, name, member_count))

        auto_gift_embed = discord.Embed(
            title="⚙️ Auto Gift Code Settings",
            description=(
                "Select an alliance to configure auto gift code:\n\n"
                "**Alliance List**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n"
                "Select an alliance from the list below:\n"
            ),
            color=discord.Color.blue()
        )

        view = AllianceSelectView(alliances_with_counts, self)
        
        view.current_select.options.insert(0, discord.SelectOption(
            label="ENABLE ALL ALLIANCES",
            value="enable_all",
            description="Enable auto gift code for all alliances",
            emoji="✅"
        ))
        
        view.current_select.options.insert(1, discord.SelectOption(
            label="DISABLE ALL ALLIANCES",
            value="disable_all",
            description="Disable auto gift code for all alliances",
            emoji="❌"
        ))

        async def alliance_callback(select_interaction: discord.Interaction):
            try:
                selected_value = view.current_select.values[0]
                
                if selected_value in ["enable_all", "disable_all"]:
                    status = 1 if selected_value == "enable_all" else 0
                    
                    for alliance_id, _, _ in alliances_with_counts:
                        self.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status) 
                            VALUES (?, ?) 
                            ON CONFLICT(alliance_id) 
                            DO UPDATE SET status = excluded.status
                            """,
                            (alliance_id, status)
                        )
                    self.conn.commit()

                    status_text = "enabled" if status == 1 else "disabled"
                    success_embed = discord.Embed(
                        title="✅ Auto Gift Code Setting Updated",
                        description=(
                            f"**Configuration Details**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"🌐 **Scope:** All Alliances\n"
                            f"📊 **Status:** Auto gift code {status_text}\n"
                            f"👤 **Updated by:** {select_interaction.user.mention}\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        ),
                        color=discord.Color.green()
                    )
                    
                    await select_interaction.response.edit_message(
                        embed=success_embed,
                        view=None
                    )
                    return

                alliance_id = int(selected_value)
                alliance_name = next((name for aid, name in available_alliances if aid == alliance_id), "Unknown")

                current_setting = "enabled" if current_status.get(alliance_id, 0) == 1 else "disabled"
                
                confirm_embed = discord.Embed(
                    title="⚙️ Auto Gift Code Configuration",
                    description=(
                        f"**Alliance Details**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🏰 **Alliance:** {alliance_name}\n"
                        f"📊 **Current Status:** Auto gift code is {current_setting}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"Do you want to enable or disable auto gift code for this alliance?"
                    ),
                    color=discord.Color.yellow()
                )

                confirm_view = discord.ui.View()
                
                async def button_callback(button_interaction: discord.Interaction):
                    try:
                        status = 1 if button_interaction.data['custom_id'] == "confirm" else 0
                        
                        self.cursor.execute(
                            """
                            INSERT INTO giftcodecontrol (alliance_id, status) 
                            VALUES (?, ?) 
                            ON CONFLICT(alliance_id) 
                            DO UPDATE SET status = excluded.status
                            """,
                            (alliance_id, status)
                        )
                        self.conn.commit()

                        status_text = "enabled" if status == 1 else "disabled"
                        success_embed = discord.Embed(
                            title="✅ Auto Gift Code Setting Updated",
                            description=(
                                f"**Configuration Details**\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"🏰 **Alliance:** {alliance_name}\n"
                                f"📊 **Status:** Auto gift code {status_text}\n"
                                f"👤 **Updated by:** {button_interaction.user.mention}\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            ),
                            color=discord.Color.green()
                        )
                        
                        await button_interaction.response.edit_message(
                            embed=success_embed,
                            view=None
                        )

                    except Exception as e:
                        self.logger.exception(f"Button callback error: {str(e)}")
                        if not button_interaction.response.is_done():
                            await button_interaction.response.send_message(
                                "❌ An error occurred while updating the settings.",
                                ephemeral=True
                            )
                        else:
                            await button_interaction.followup.send(
                                "❌ An error occurred while updating the settings.",
                                ephemeral=True
                            )

                confirm_button = discord.ui.Button(
                    label="Enable",
                    emoji="✅",
                    style=discord.ButtonStyle.success,
                    custom_id="confirm"
                )
                confirm_button.callback = button_callback

                deny_button = discord.ui.Button(
                    label="Disable",
                    emoji="❌",
                    style=discord.ButtonStyle.danger,
                    custom_id="deny"
                )
                deny_button.callback = button_callback

                confirm_view.add_item(confirm_button)
                confirm_view.add_item(deny_button)

                if not select_interaction.response.is_done():
                    await select_interaction.response.edit_message(
                        embed=confirm_embed,
                        view=confirm_view
                    )
                else:
                    await select_interaction.message.edit(
                        embed=confirm_embed,
                        view=confirm_view
                    )

            except Exception as e:
                self.logger.exception(f"Error in alliance selection: {e}")
                if not select_interaction.response.is_done():
                    await select_interaction.response.send_message(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )
                else:
                    await select_interaction.followup.send(
                        "❌ An error occurred while processing your selection.",
                        ephemeral=True
                    )

        view.callback = alliance_callback

        await interaction.response.send_message(
            embed=auto_gift_embed,
            view=view,
            ephemeral=True
        )

    async def use_giftcode_for_alliance(self, alliance_id, giftcode):
        MEMBER_PROCESS_DELAY = 1.0
        API_RATE_LIMIT_COOLDOWN = 60.0
        CAPTCHA_CYCLE_COOLDOWN = 60.0
        MAX_RETRY_CYCLES = 10

        self.logger.info(f"\nGiftOps: Starting use_giftcode_for_alliance for Alliance {alliance_id}, Code {giftcode}")

        try:
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

            # Check if this code has been validated before
            self.cursor.execute("SELECT validation_status FROM gift_codes WHERE giftcode = ?", (giftcode,))
            master_code_status_row = self.cursor.fetchone()
            master_code_status = master_code_status_row[0] if master_code_status_row else None
            final_invalid_reason_for_embed = None

            if master_code_status == 'invalid':
                self.logger.info(f"GiftOps: Code {giftcode} is already marked as 'invalid' in the database.")
                final_invalid_reason_for_embed = "Code previously marked as invalid"
            else:
                # If not marked 'invalid' in master table, check with test FID if status is 'pending' or for other cached issues
                test_fid = self.get_test_fid()
                self.cursor.execute("SELECT status FROM user_giftcodes WHERE fid = ? AND giftcode = ?", (test_fid, giftcode))
                validation_fid_status_row = self.cursor.fetchone()

                if validation_fid_status_row:
                    fid_status = validation_fid_status_row[0]
                    if fid_status in ["TIME_ERROR", "CDK_NOT_FOUND", "USAGE_LIMIT"]:
                        self.logger.info(f"GiftOps: Code {giftcode} known to be invalid via test FID (status: {fid_status}). Marking invalid.")
                        self.cursor.execute("UPDATE gift_codes SET validation_status = 'invalid' WHERE giftcode = ?", (giftcode,))
                        self.conn.commit()
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
                    title="❌ Gift Code Invalid",
                    description=(
                        f"**Gift Code Details**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎁 **Gift Code:** `{giftcode}`\n"
                        f"🏰 **Alliance:** `{alliance_name}`\n"
                        f"❌ **Status:** {final_invalid_reason_for_embed}\n"
                        f"📝 **Action:** Code status is 'invalid' in database\n"
                        f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    ),
                    color=discord.Color.red() # Red is fine for a hard stop
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

            # Check Cache & Populate Initial List
            member_ids = [m[0] for m in members]
            if member_ids: # Ensure member_ids is not empty to prevent SQL error
                placeholders = ','.join('?' * len(member_ids))
                self.cursor.execute(f"SELECT fid, status FROM user_giftcodes WHERE giftcode = ? AND fid IN ({placeholders})", (giftcode, *member_ids))
                cached_member_statuses = dict(self.cursor.fetchall())
            else:
                cached_member_statuses = {}

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
            embed = discord.Embed(title=f"🎁 Gift Code Redemption: {giftcode}", color=discord.Color.blue())
            def update_embed_description():
                return (
                    f"**Status for Alliance:** `{alliance_name}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"👥 **Total Members:** `{total_members}`\n"
                    f"✅ **Success:** `{success_count}`\n"
                    f"ℹ️ **Already Redeemed:** `{received_count}`\n"
                    f"🔄 **Retrying:** `{len(retry_queue)}`\n"
                    f"❌ **Failed:** `{failed_count}`\n"
                    f"⏳ **Processed:** `{processed_count}/{total_members}`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                )
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

                self.logger.info(f"GiftOps: Processing FID {fid} ({nickname}), Cycle {current_cycle_count + 1}/{MAX_RETRY_CYCLES}")

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
                    self.cursor.execute("""
                        UPDATE gift_codes 
                        SET validation_status = 'invalid' 
                        WHERE giftcode = ? AND validation_status != 'invalid'
                    """, (giftcode,))
                    self.conn.commit()
                    
                    if hasattr(self, 'api') and self.api:
                        asyncio.create_task(self.api.remove_giftcode(giftcode, from_validation=True))

                    reason_map_runtime = {
                        "TIME_ERROR": "Code has expired (TIME_ERROR)",
                        "CDK_NOT_FOUND": "Code not found or incorrect (CDK_NOT_FOUND)",
                        "USAGE_LIMIT": "Usage limit reached (USAGE_LIMIT)"
                    }
                    status_reason_runtime = reason_map_runtime.get(response_status, f"Code invalid ({response_status})")
                    
                    embed.title = f"❌ Gift Code Invalid: {giftcode}" 
                    embed.color = discord.Color.red()
                    embed.description = (
                        f"**Gift Code Redemption Halted**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎁 **Gift Code:** `{giftcode}`\n"
                        f"🏰 **Alliance:** `{alliance_name}`\n"
                        f"❌ **Reason:** {status_reason_runtime}\n"
                        f"📝 **Action:** Code marked as invalid in database. Remaining members for this alliance will not be processed.\n"
                        f"📊 **Processed before halt:** {processed_count}/{total_members}\n"
                        f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
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
                    
                    embed.title = f"⚙️ Sign Error: {giftcode}"
                    embed.color = discord.Color.red()
                    embed.description = (
                        f"**Bot Configuration Error**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🎁 **Gift Code:** `{giftcode}`\n"
                        f"🏰 **Alliance:** `{alliance_name}`\n"
                        f"⚙️ **Reason:** Sign Error (check bot config/encrypt key)\n"
                        f"📝 **Action:** Redemption stopped. Check bot configuration.\n"
                        f"📊 **Processed before halt:** {processed_count}/{total_members}\n"
                        f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
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
                    mark_processed = True
                elif response_status in ["RECEIVED", "SAME TYPE EXCHANGE"]:
                    received_count += 1
                    already_used_users.append(nickname)
                    mark_processed = True
                elif response_status in ["LOGIN_FAILED", "LOGIN_EXPIRED_MID_PROCESS", "ERROR", "UNKNOWN_API_RESPONSE", "OCR_DISABLED", "SOLVER_ERROR", "CAPTCHA_FETCH_ERROR"]:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Processing Error ({response_status})"
                elif response_status == "TIMEOUT_RETRY":
                    queue_for_retry = True
                    retry_delay = API_RATE_LIMIT_COOLDOWN
                    fail_reason = "API Rate Limited"
                elif response_status in ["CAPTCHA_INVALID", "MAX_CAPTCHA_ATTEMPTS_REACHED", "OCR_FAILED_ATTEMPT"]:
                    if current_cycle_count + 1 < MAX_RETRY_CYCLES:
                        queue_for_retry = True
                        retry_delay = CAPTCHA_CYCLE_COOLDOWN
                        fail_reason = "Captcha Cycle Failed"
                        self.logger.info(f"GiftOps: FID {fid} failed captcha cycle {current_cycle_count + 1}. Queuing for retry cycle {current_cycle_count + 2} in {retry_delay}s.")
                    else:
                        add_to_failed = True
                        mark_processed = True
                        fail_reason = f"Failed after {MAX_RETRY_CYCLES} captcha cycles (Last Status: {response_status})"
                        self.logger.info(f"GiftOps: Max ({MAX_RETRY_CYCLES}) retry cycles reached for FID {fid}. Marking as failed.")
                else:
                    add_to_failed = True
                    mark_processed = True
                    fail_reason = f"Unhandled status: {response_status}"

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
                final_title = f"🎁 Gift Code Process Complete: {giftcode}"
                final_color = discord.Color.green() if failed_count == 0 and total_members > 0 else \
                              discord.Color.orange() if success_count > 0 or received_count > 0 else \
                              discord.Color.red()
                if total_members == 0:
                    final_title = f"ℹ️ No Members to Process for Code: {giftcode}"
                    final_color = discord.Color.light_grey()

                embed.title = final_title
                embed.color = final_color
                embed.description = update_embed_description()

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
            return True
        
        except Exception as e:
            self.logger.exception(f"GiftOps: UNEXPECTED ERROR in use_giftcode_for_alliance for {alliance_id}/{giftcode}: {str(e)}")
            self.logger.exception(f"Traceback: {traceback.format_exc()}")
            try:
                if 'channel' in locals() and channel: await channel.send(f"⚠️ An unexpected error occurred processing `{giftcode}` for {alliance_name}.")
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
        logger.info("[CreateGiftCodeModal] on_submit started.")

        await interaction.response.defer(ephemeral=True)
        logger.info("[CreateGiftCodeModal] Interaction deferred.")

        code = self.cog.clean_gift_code(self.giftcode.value)
        logger.info(f"[CreateGiftCodeModal] Code entered: {code}")

        final_embed = discord.Embed(title="🎁 Gift Code Creation Result")

        # Check if code already exists
        self.cog.cursor.execute("SELECT 1 FROM gift_codes WHERE giftcode = ?", (code,))
        if self.cog.cursor.fetchone():
            logger.info(f"[CreateGiftCodeModal] Code {code} already exists in DB.")
            final_embed.title = "ℹ️ Gift Code Exists"
            final_embed.description = (
                f"**Gift Code Details**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                f"🎁 **Gift Code:** `{code}`\n"
                f"✅ **Status:** Code already exists in database.\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n"
            )
            final_embed.color = discord.Color.blue()
        else:
            # Add code (without validation)
            logger.info(f"[CreateGiftCodeModal] Adding code {code} to DB without validation.")
            date = datetime.now().strftime("%Y-%m-%d")
            try:
                self.cog.cursor.execute(
                    "INSERT INTO gift_codes (giftcode, date, validation_status) VALUES (?, ?, ?)",
                    (code, date, "pending")
                )
                self.cog.conn.commit()

                logger.info(f"[CreateGiftCodeModal] Code '{code}' added as pending - will send to API after validation.")

                final_embed.title = "✅ Gift Code Added"
                final_embed.description = (
                    f"**Gift Code Details**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🎁 **Gift Code:** `{code}`\n"
                    f"✅ **Status:** Added to database (will be validated on first use).\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                )
                final_embed.color = discord.Color.green()

            except sqlite3.Error as db_err:
                logger.exception(f"[CreateGiftCodeModal] DB Error inserting code '{code}': {db_err}")
                final_embed.title = "❌ Database Error"
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
                "❌ Gift code not found!",
                ephemeral=True
            )
            return
            
        self.cog.cursor.execute("DELETE FROM gift_codes WHERE giftcode = ?", (code,))
        self.cog.cursor.execute("DELETE FROM user_giftcodes WHERE giftcode = ?", (code,))
        self.cog.conn.commit()
        
        embed = discord.Embed(
            title="✅ Gift Code Deleted",
            description=f"Gift code `{code}` has been deleted successfully.",
            color=discord.Color.green()
        )
        
        await interaction.response.send_message(embed=embed, ephemeral=True)

class TestFIDModal(discord.ui.Modal, title="Change Test FID"):
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
            label="Enter New Player ID (FID)",
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
                await interaction.followup.send("❌ Invalid FID format. Please enter a numeric FID.", ephemeral=True)
                return
            
            is_valid, message = await self.cog.verify_test_fid(new_fid)
            
            if is_valid:
                success = await self.cog.update_test_fid(new_fid)
                
                if success:
                    embed = discord.Embed(
                        title="✅ Test FID Updated",
                        description=(
                            f"**Test FID Configuration**\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 **FID:** `{new_fid}`\n"
                            f"✅ **Status:** Validated\n"
                            f"📝 **Action:** Updated in database\n"
                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        ),
                        color=discord.Color.green()
                    )
                    await interaction.followup.send(embed=embed, ephemeral=True)
                    
                    await self.cog.show_ocr_settings(interaction)
                else:
                    await interaction.followup.send("❌ Failed to update test FID in database. Check logs for details.", ephemeral=True)
            else:
                embed = discord.Embed(
                    title="❌ Invalid Test FID",
                    description=(
                        f"**Test FID Validation**\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"🆔 **FID:** `{new_fid}`\n"
                        f"❌ **Status:** Invalid FID\n"
                        f"📝 **Reason:** {message}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    ),
                    color=discord.Color.red()
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
                
        except Exception as e:
            self.cog.logger.exception(f"Error updating test FID: {e}")
            await interaction.followup.send(f"❌ An error occurred: {str(e)}", ephemeral=True)

class GiftView(discord.ui.View):
    def __init__(self, cog):
        super().__init__(timeout=None)
        self.cog = cog

    @discord.ui.button(
        label="Create Gift Code",
        style=discord.ButtonStyle.green,
        custom_id="create_gift",
        emoji="🎫",
        row=0
    )
    async def create_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.create_gift_code(interaction)
        
    @discord.ui.button(
        label="CAPTCHA Settings",
        style=discord.ButtonStyle.primary,
        custom_id="ocr_settings",
        emoji="🔍",
        row=0
    )
    async def ocr_settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.show_ocr_settings(interaction)

    @discord.ui.button(
        label="List Gift Codes",
        style=discord.ButtonStyle.blurple,
        custom_id="list_gift",
        emoji="📋",
        row=0
    )
    async def list_gift(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.list_gift_codes(interaction)

    @discord.ui.button(
        label="Auto Gift Settings",
        style=discord.ButtonStyle.grey,
        custom_id="auto_gift_settings",
        emoji="⚙️",
        row=1
    )
    async def auto_gift_settings(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog.setup_giftcode_auto(interaction)

    @discord.ui.button(
        label="Delete Gift Code",
        emoji="❌",
        style=discord.ButtonStyle.danger,
        custom_id="delete_gift"
    )
    async def delete_gift_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.delete_gift_code(interaction)
        except Exception as e:
            self.logger.exception(f"Delete gift button error: {e}")
            await interaction.response.send_message(
                "❌ An error occurred while processing delete request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Gift Code Channel",
        emoji="📢",
        style=discord.ButtonStyle.primary,
        custom_id="gift_channel"
    )
    async def gift_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.setup_gift_channel(interaction)
        except Exception as e:
            self.logger.exception(f"Gift channel button error: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred while setting up gift channel.",
                    ephemeral=True
                )

    @discord.ui.button(
        label="Delete Gift Channel",
        emoji="🗑️",
        style=discord.ButtonStyle.danger,
        custom_id="delete_gift_channel"
    )
    async def delete_gift_channel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await self.cog.delete_gift_channel(interaction)
        except Exception as e:
            self.logger.exception(f"Delete gift channel button error: {e}")
            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "❌ An error occurred while deleting gift channel.",
                    ephemeral=True
                )

    @discord.ui.button(
        label="Use Gift Code for Alliance",
        emoji="🎯",
        style=discord.ButtonStyle.primary,
        custom_id="use_gift_alliance"
    )
    async def use_gift_alliance_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            admin_info = await self.cog.get_admin_info(interaction.user.id)
            if not admin_info:
                await interaction.response.send_message(
                    "❌ You are not authorized to perform this action.",
                    ephemeral=True
                )
                return

            available_alliances = await self.cog.get_available_alliances(interaction)
            if not available_alliances:
                await interaction.response.send_message(
                    embed=discord.Embed(
                        title="❌ No Available Alliances",
                        description="You don't have access to any alliances.",
                        color=discord.Color.red()
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
                title="🎯 Use Gift Code for Alliance",
                description=(
                    "Select an alliance to use gift code:\n\n"
                    "**Alliance List**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n"
                    "Select an alliance from the list below:\n"
                ),
                color=discord.Color.blue()
            )

            view = AllianceSelectView(alliances_with_counts, self.cog)
            
            view.current_select.options.insert(0, discord.SelectOption(
                label="ALL ALLIANCES",
                value="all",
                description=f"Apply to all {len(alliances_with_counts)} alliances",
                emoji="🌐"
            ))

            async def alliance_callback(select_interaction: discord.Interaction):
                try:
                    selected_value = view.current_select.values[0]
                    
                    if selected_value == "all":
                        all_alliances = [aid for aid, name, _ in alliances_with_counts]
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
                        title="🎁 Select Gift Code",
                        description=(
                            "Select a gift code to use:\n\n"
                            "**Gift Code List**\n"
                            "━━━━━━━━━━━━━━━━━━━━━━\n"
                            "Select a gift code from the list below:\n"
                        ),
                        color=discord.Color.blue()
                    )

                    select_giftcode = discord.ui.Select(
                        placeholder="Select a gift code",
                        options=[
                            discord.SelectOption(
                                label=f"Code: {code}",
                                value=code,
                                description=f"Created: {date}",
                                emoji="🎁"
                            ) for code, date in gift_codes
                        ]
                    )

                    async def giftcode_callback(giftcode_interaction: discord.Interaction):
                        try:
                            selected_code = giftcode_interaction.data["values"][0]
                            
                            confirm_embed = discord.Embed(
                                title="⚠️ Confirm Gift Code Usage",
                                description=(
                                    f"Are you sure you want to use this gift code?\n\n"
                                    f"**Details**\n"
                                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                    f"🎁 **Gift Code:** `{selected_code}`\n"
                                    f"🏰 **Alliances:** {'ALL' if selected_value == 'all' else next((name for aid, name, _ in alliances_with_counts if aid == alliance_id), 'Unknown')}\n"
                                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                ),
                                color=discord.Color.yellow()
                            )

                            confirm_view = discord.ui.View()
                            
                            async def confirm_callback(button_interaction: discord.Interaction):
                                try:
                                    await button_interaction.response.edit_message(
                                        content="Gift code redemption is starting.",
                                        embed=None,
                                        view=None
                                    )

                                    progress_embed = discord.Embed(
                                        title="🎁 Gift Code Distribution Progress",
                                        description=(
                                            f"**Overall Progress**\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                            f"🎁 **Gift Code:** `{selected_code}`\n"
                                            f"🏰 **Total Alliances:** `{len(all_alliances)}`\n"
                                            f"⏳ **Current Alliance:** `Starting...`\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        ),
                                        color=discord.Color.blue()
                                    )

                                    channel = button_interaction.channel
                                    progress_msg = await channel.send(embed=progress_embed)
                                    completed = 0

                                    for aid in all_alliances:
                                        alliance_name = next((name for a_id, name, _ in alliances_with_counts if a_id == aid), 'Unknown')
                                        
                                        progress_embed.description = (
                                            f"**Overall Progress**\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                            f"🎁 **Gift Code:** `{selected_code}`\n"
                                            f"🏰 **Total Alliances:** `{len(all_alliances)}`\n"
                                            f"⏳ **Current Alliance:** `{alliance_name}`\n"
                                            f"📊 **Progress:** `{completed}/{len(all_alliances)}`\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        )
                                        try:
                                            await progress_msg.edit(embed=progress_embed)
                                        except Exception as e:
                                            print(f"Could not update progress embed: {e}")
                                        
                                        result = await self.cog.use_giftcode_for_alliance(aid, selected_code)
                                        if result:
                                            completed += 1
                                        
                                        await asyncio.sleep(5)
                                    
                                    final_embed = discord.Embed(
                                        title="✅ Gift Code Distribution Complete",
                                        description=(
                                            f"**Final Status**\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                            f"🎁 **Gift Code:** `{selected_code}`\n"
                                            f"🏰 **Total Alliances:** `{len(all_alliances)}`\n"
                                            f"✅ **Completed:** `{completed}/{len(all_alliances)}`\n"
                                            f"⏰ **Time:** <t:{int(datetime.now().timestamp())}:R>\n"
                                            f"━━━━━━━━━━━━━━━━━━━━━━\n"
                                        ),
                                        color=discord.Color.green()
                                    )
                                    try:
                                        await progress_msg.edit(embed=final_embed)
                                    except Exception as e:
                                        print(f"Could not update final embed: {e}")

                                except Exception as e:
                                    self.logger.exception(f"Error using gift code: {e}")
                                    await button_interaction.followup.send(
                                        "❌ An error occurred while using the gift code.",
                                        ephemeral=True
                                    )

                            async def cancel_callback(button_interaction: discord.Interaction):
                                cancel_embed = discord.Embed(
                                    title="❌ Operation Cancelled",
                                    description="The gift code usage has been cancelled.",
                                    color=discord.Color.red()
                                )
                                await button_interaction.response.edit_message(
                                    embed=cancel_embed,
                                    view=None
                                )

                            confirm_button = discord.ui.Button(
                                label="Confirm",
                                emoji="✅",
                                style=discord.ButtonStyle.success,
                                custom_id="confirm"
                            )
                            confirm_button.callback = confirm_callback

                            cancel_button = discord.ui.Button(
                                label="Cancel",
                                emoji="❌",
                                style=discord.ButtonStyle.danger,
                                custom_id="cancel"
                            )
                            cancel_button.callback = cancel_callback

                            confirm_view.add_item(confirm_button)
                            confirm_view.add_item(cancel_button)

                            await giftcode_interaction.response.edit_message(
                                embed=confirm_embed,
                                view=confirm_view
                            )

                        except Exception as e:
                            self.logger.exception(f"Error in gift code selection: {e}")
                            if not giftcode_interaction.response.is_done():
                                await giftcode_interaction.response.send_message(
                                    "❌ An error occurred while processing your selection.",
                                    ephemeral=True
                                )
                            else:
                                await giftcode_interaction.followup.send(
                                    "❌ An error occurred while processing your selection.",
                                    ephemeral=True
                                )

                    select_giftcode.callback = giftcode_callback
                    giftcode_view = discord.ui.View()
                    giftcode_view.add_item(select_giftcode)

                    if not select_interaction.response.is_done():
                        await select_interaction.response.edit_message(
                            embed=giftcode_embed,
                            view=giftcode_view
                        )
                    else:
                        await select_interaction.message.edit(
                            embed=giftcode_embed,
                            view=giftcode_view
                        )

                except Exception as e:
                    self.logger.exception(f"Error in alliance selection: {e}")
                    if not select_interaction.response.is_done():
                        await select_interaction.response.send_message(
                            "❌ An error occurred while processing your selection.",
                            ephemeral=True
                        )
                    else:
                        await select_interaction.followup.send(
                            "❌ An error occurred while processing your selection.",
                            ephemeral=True
                        )

            view.callback = alliance_callback

            await interaction.response.send_message(
                embed=alliance_embed,
                view=view,
                ephemeral=True
            )

        except Exception as e:
            self.logger.exception(f"Error in use_gift_alliance_button: {str(e)}")
            await interaction.response.send_message(
                "❌ An error occurred while processing the request.",
                ephemeral=True
            )

    @discord.ui.button(
        label="Main Menu",
        emoji="🏠",
        style=discord.ButtonStyle.secondary,
        custom_id="main_menu"
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

class OCRSettingsView(discord.ui.View):
    def __init__(self, cog, ocr_settings, onnx_available):
        super().__init__(timeout=None)
        self.cog = cog
        self.enabled = ocr_settings[0]
        self.save_images_setting = ocr_settings[1]
        self.onnx_available = onnx_available
        self.disable_controls = not onnx_available

        # Row 0: Enable/Disable Button, Test Button
        self.enable_ocr_button_item = discord.ui.Button(
            emoji="✅" if self.enabled == 1 else "🚫",
            custom_id="enable_ocr", row=0,
            label="Disable CAPTCHA Solver" if self.enabled == 1 else "Enable CAPTCHA Solver",
            style=discord.ButtonStyle.danger if self.enabled == 1 else discord.ButtonStyle.success,
            disabled=self.disable_controls
        )
        self.enable_ocr_button_item.callback = self.enable_ocr_button
        self.add_item(self.enable_ocr_button_item)

        self.test_ocr_button_item = discord.ui.Button(
            label="Test CAPTCHA Solver", style=discord.ButtonStyle.secondary, emoji="🧪",
            custom_id="test_ocr", row=0,
            disabled=self.disable_controls
        )
        self.test_ocr_button_item.callback = self.test_ocr_button
        self.add_item(self.test_ocr_button_item)

        # Add the Change Test FID Button
        self.change_test_fid_button_item = discord.ui.Button(
            label="Change Test FID", style=discord.ButtonStyle.primary, emoji="🔄",
            custom_id="change_test_fid", row=0,
            disabled=self.disable_controls
        )
        self.change_test_fid_button_item.callback = self.change_test_fid_button
        self.add_item(self.change_test_fid_button_item)

        # Add the Clear Redemption Cache Button
        self.clear_cache_button_item = discord.ui.Button(
            label="Clear Redemption Cache", style=discord.ButtonStyle.danger, emoji="🗑️",
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
        """Handle the change test FID button click."""
        if not self.onnx_available:
            await interaction.response.send_message("❌ Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        await interaction.response.send_modal(TestFIDModal(self.cog))

    async def enable_ocr_button(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message("❌ Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
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
            await interaction.response.send_message("❌ Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return
        if not self.cog.captcha_solver or not self.cog.captcha_solver.is_initialized:
            await interaction.response.send_message("❌ CAPTCHA solver is not initialized. Ensure OCR is enabled.", ephemeral=True)
            return

        last_test_time = self.cog.test_captcha_cooldowns.get(user_id, 0)
        if current_time - last_test_time < self.cog.test_captcha_delay:
            remaining_time = int(self.cog.test_captcha_delay - (current_time - last_test_time))
            await interaction.response.send_message(f"❌ Please wait {remaining_time} more seconds before testing again.", ephemeral=True)
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

        try:
            logger.info(f"[Test Button] First logging in with test FID {test_fid}...")
            session, response_stove_info = self.cog.get_stove_info_wos(player_id=test_fid)
            
            try:
                player_info_json = response_stove_info.json()
                if player_info_json.get("msg") != "success":
                    logger.error(f"[Test Button] Login failed for test FID {test_fid}: {player_info_json.get('msg')}")
                    await interaction.followup.send(f"❌ Login failed with test FID {test_fid}. Please check if the FID is valid.", ephemeral=True)
                    return
                logger.info(f"[Test Button] Successfully logged in with test FID {test_fid}")
            except Exception as json_err:
                logger.error(f"[Test Button] Error parsing login response: {json_err}")
                await interaction.followup.send("❌ Error processing login response.", ephemeral=True)
                return
            
            logger.info(f"[Test Button] Fetching captcha for test FID {test_fid} using established session...")
            captcha_image_base64, error = await self.cog.fetch_captcha(test_fid, session=session)
            logger.info(f"[Test Button] Captcha fetch result: Error='{error}', HasImage={captcha_image_base64 is not None}")

            if error:
                await interaction.followup.send(f"❌ Error fetching test captcha from the API: `{error}`", ephemeral=True)
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
                    await interaction.followup.send("❌ Failed to decode captcha image data.", ephemeral=True)
                    return
            else:
                logger.error("[Test Button] Captcha fetch returned no image data.")
                await interaction.followup.send("❌ Failed to retrieve captcha image data from API.", ephemeral=True)
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
                 await interaction.followup.send("❌ Internal error before solving captcha.", ephemeral=True)
                 return

            confidence_str = f'{confidence:.2f}' if isinstance(confidence, float) else 'N/A'
            embed = discord.Embed(
                title="🧪 CAPTCHA Solver Test Results (ONNX)",
                description=(
                    f"**Test Summary**\n━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"🤖 **OCR Success:** {'✅ Yes' if success else '❌ No'}\n"
                    f"🔍 **Recognized Code:** `{captcha_code if success and captcha_code else 'N/A'}`\n"
                    f"📊 **Confidence:** `{confidence_str}`\n"
                    f"⏱️ **Solve Time:** `{solve_duration:.2f}s`\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                ), color=discord.Color.green() if success else discord.Color.red()
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
                embed.add_field(name="⚠️ Image Save Error", value=save_error_str, inline=False)

            await interaction.followup.send(embed=embed, ephemeral=True)
            logger.info(f"[Test Button] Test completed for user {user_id}.")

        except Exception as e:
            logger.exception(f"[Test Button] UNEXPECTED Error during test for user {user_id}: {e}")
            try:
                await interaction.followup.send(f"❌ An unexpected error occurred during the test: `{e}`. Please check the bot logs.", ephemeral=True)
            except Exception as followup_err:
                logger.error(f"[Test Button] Failed to send final error followup to user {user_id}: {followup_err}")

    async def clear_redemption_cache_button(self, interaction: discord.Interaction):
        """Handle the clear redemption cache button click."""
        if not self.onnx_available:
            await interaction.response.send_message("❌ Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
            return

        # Create confirmation embed
        embed = discord.Embed(
            title="⚠️ Clear Redemption Cache",
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
                name="📊 Current Records",
                value=f"{current_count:,} redemption records will be deleted",
                inline=False
            )
        except Exception as e:
            self.cog.logger.error(f"Error getting user_giftcodes count: {e}")
            embed.add_field(
                name="📊 Current Records", 
                value="Unable to count records",
                inline=False
            )

        # Create confirmation view
        class ClearCacheConfirmView(discord.ui.View):
            def __init__(self, parent_cog):
                super().__init__(timeout=30)
                self.parent_cog = parent_cog

            @discord.ui.button(label="Confirm Clear", style=discord.ButtonStyle.danger, emoji="✅")
            async def confirm_clear(self, button_interaction: discord.Interaction):
                try:
                    # Clear the user_giftcodes table
                    self.parent_cog.cursor.execute("DELETE FROM user_giftcodes")
                    deleted_count = self.parent_cog.cursor.rowcount
                    self.parent_cog.conn.commit()
                    
                    success_embed = discord.Embed(
                        title="✅ Redemption Cache Cleared",
                        description=f"Successfully deleted {deleted_count:,} redemption records.\n\nUsers can now attempt to redeem gift codes again.",
                        color=discord.Color.green()
                    )
                    
                    self.parent_cog.logger.info(f"Redemption cache cleared by user {button_interaction.user.id}: {deleted_count} records deleted")
                    
                    await button_interaction.response.edit_message(embed=success_embed, view=None)
                    
                except Exception as e:
                    self.parent_cog.logger.exception(f"Error clearing redemption cache: {e}")
                    error_embed = discord.Embed(
                        title="❌ Error",
                        description=f"Failed to clear redemption cache: {str(e)}",
                        color=discord.Color.red()
                    )
                    await button_interaction.response.edit_message(embed=error_embed, view=None)

            @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary, emoji="❌")
            async def cancel_clear(self, button_interaction: discord.Interaction):
                cancel_embed = discord.Embed(
                    title="❌ Operation Cancelled",
                    description="Redemption cache was not cleared.",
                    color=discord.Color.blue()
                )
                await button_interaction.response.edit_message(embed=cancel_embed, view=None)

        confirm_view = ClearCacheConfirmView(self.cog)
        await interaction.response.send_message(embed=embed, view=confirm_view, ephemeral=True)

    async def image_save_select_callback(self, interaction: discord.Interaction):
        if not self.onnx_available:
            await interaction.response.send_message("❌ Required library (onnxruntime) is not installed or failed to load.", ephemeral=True)
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
                await interaction.followup.send(f"❌ {message}", ephemeral=True)

        except ValueError:
            await interaction.followup.send("❌ Invalid selection value for image saving.", ephemeral=True)
        except Exception as e:
            self.cog.logger.exception("Error processing image save selection in OCRSettingsView.")
            await interaction.followup.send("❌ An error occurred while updating image saving settings.", ephemeral=True)
        
        async def update_task(save_images_value):
            self.cog.logger.info(f"Task started: Updating OCR save_images to {save_images_value}")
            _success, _message = await self.cog.update_ocr_settings(
                interaction=None,
                save_images=save_images_value
            )
            self.cog.logger.info(f"Task finished: update_ocr_settings returned success={_success}, message='{_message}'")
            return _success, _message

        update_job = asyncio.create_task(update_task(selected_value))
        initial_followup_message = "⏳ Your settings are being updated... Please wait."
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
            await progress_message.edit(content=f"❌ An error occurred during the update: {e_task}")
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
                await progress_message.edit(content=f"✅ {message_from_task}")
            except discord.NotFound:
                 self.cog.logger.warning("Original message or progress message for OCR settings not found for final update.")
            except Exception as e_edit_final:
                 self.cog.logger.exception(f"Error editing messages after successful OCR settings update: {e_edit_final}")
                 await progress_message.edit(content=f"✅ {message_from_task}\n⚠️ Couldn't fully refresh the view.")

        else:
            self.cog.logger.error(f"OCR settings update failed: {message_from_task}")
            await progress_message.edit(content=f"❌ {message_from_task}")

async def setup(bot):
    await bot.add_cog(GiftOperations(bot)) 