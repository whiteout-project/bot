"""
Gift code orchestrator cog. Delegates to gift_redemption, gift_channels,
gift_settings, gift_views, and gift_operationsapi for the heavy lifting.
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
from .pimp_my_bot import theme, safe_edit_message
from . import gift_redemption
from . import gift_state_resolver
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

        # WOS API URLs and key
        self.wos_giftcode_url = "https://wos-giftcode-api.centurygame.com/api/gift_code"
        self.wos_giftcode_redemption_url = "https://wos-giftcode.centurygame.com"
        self.wos_encrypt_key = "tB87#kPtkxqOS2"

        # Retry Configuration for Requests
        self.retry_config = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["POST", "GET"]
        )

        # Validation pacing (per-FID spacing so validation bursts don't hammer one FID).
        self._validation_lock = asyncio.Lock()
        self._last_validation_claim_by_fid = {}  # fid -> monotonic ts of its last validation probe
        self._priority_validation_pending = 0  # in-flight new-code validations; periodic loop yields while > 0
        self.last_validation_attempt_time = 0
        self.validation_cooldown = 5
        self._last_cleanup_date = None
        self._state_nudge_sent = False  # one-time boot nudge about member states

        # Batch redemption tracking (in-memory only, for live progress messages)
        self.redemption_batches = {}

        # Near-term gift-code re-validation: in-flight backoff tasks per code,
        # and codes whose auto-redemption has already been started (dedup).
        self._revalidation_tasks = {}
        self._auto_redeem_started = set()

        self.processing_stats = {
            "redemption_submissions": 0,
            "server_validation_success": 0,
            "server_validation_failure": 0,
            "total_fids_processed": 0,
            "total_processing_time": 0.0
        }

        # Test ID Settings Table
        try:
            self.settings_cursor.execute("""
                CREATE TABLE IF NOT EXISTS test_fid_settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    test_fid TEXT NOT NULL
                )
            """)
            cols = [r[1] for r in self.settings_cursor.execute("PRAGMA table_info(test_fid_settings)")]
            if "kid" not in cols:
                self.settings_cursor.execute("ALTER TABLE test_fid_settings ADD COLUMN kid INTEGER")
                self.settings_cursor.execute(
                    "UPDATE test_fid_settings SET kid = 312 WHERE test_fid = '45379845'")
            self.settings_cursor.execute("SELECT test_fid FROM test_fid_settings ORDER BY id DESC LIMIT 1")
            result = self.settings_cursor.fetchone()
            if not result:
                self.settings_cursor.execute(
                    "INSERT INTO test_fid_settings (test_fid, kid) VALUES (?, ?)", ("45379845", 312))
                self.settings_conn.commit()
                self.logger.info("Initialized default test ID (45379845) in database")
            self.settings_conn.commit()
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

    async def cog_unload(self):
        # GiftCodeAPI is not a cog - shut it down here or every reload leaks its sync loop.
        api = getattr(self, 'api', None)
        if api is not None:
            try:
                await api.cog_unload()
            except Exception as e:
                self.logger.error(f"Error shutting down GiftCodeAPI client: {e}")
                print(f"Error shutting down GiftCodeAPI client: {e}")
        if hasattr(self, 'periodic_validation_loop') and self.periodic_validation_loop.is_running():
            self.periodic_validation_loop.cancel()
        for task in list(getattr(self, '_revalidation_tasks', {}).values()):
            if not task.done():
                task.cancel()
        for conn_name in ['conn', 'settings_conn', 'alliance_conn']:
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

            # One-time nudge to global admins if member states need attention.
            if not self._state_nudge_sent:
                self._state_nudge_sent = True
                asyncio.create_task(self._notify_state_migration())

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
            # Crossposts from a followed announcement channel arrive bot-authored;
            # they carry official codes and must be processed. Other bots stay ignored.
            if (message.author.bot and not message.flags.is_crossposted) or not message.guild:
                return

            self.cursor.execute("SELECT alliance_id FROM giftcode_channel WHERE channel_id = ?", (message.channel.id,))
            channel_info = self.cursor.fetchone()
            if not channel_info:
                return

            content = message.content.strip()
            candidates = []
            if content:
                if len(content.split()) == 1:
                    if re.match(r'^[a-zA-Z0-9]+$', content):
                        candidates.append(content)
                else:
                    code_match = re.search(r'Code:\s*(\S+)', content, re.IGNORECASE)
                    if code_match:
                        candidates.append(code_match.group(1))
            candidates.extend(gift_redemption._extract_embed_codes(message))

            seen = set()
            for candidate in candidates:
                giftcode = self.clean_gift_code(candidate)
                if not giftcode or giftcode in seen:
                    continue
                seen.add(giftcode)
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
                f"{theme.archiveIcon} **Redemption History**\n"
                f"└ See which accounts redeemed, already had, or failed a code\n\n"
                f"{theme.settingsIcon} **Settings**\n"
                f"└ Set up a gift code channel, configure auto redemption, and more...\n\n"
                f"{theme.trashIcon} **Delete Gift Code**\n"
                f"└ Remove existing codes (rarely needed)\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )
        view = GiftView(self, interaction.user.id)
        await safe_edit_message(interaction, embed=gift_menu_embed, view=view)

    # ── Delegation to gift_redemption ─────────────────────────────────

    async def validate_gift_code_immediately(self, giftcode, source="unknown", force=False):
        return await gift_redemption.validate_gift_code_immediately(self, giftcode, source, force)

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

    async def claim_giftcode_rewards_wos(self, player_id, giftcode, *, skip_cache: bool = False):
        return await gift_redemption.claim_giftcode_rewards_wos(self, player_id, giftcode, skip_cache=skip_cache)

    def bind_alliance_states(self, *, only_unbound=True):
        """Bind clear-majority alliances to a state and auto-flag genuinely-mixed ones as
        multistate. Returns {'bound': [...], 'multistate': [...]}."""
        bound = gift_state_resolver.bind_all_alliances(only_unbound=only_unbound)
        flagged = gift_state_resolver.auto_flag_multistate()
        return {"bound": bound, "multistate": flagged}

    def assign_alliance_state_to_missing(self):
        """Fast no-API backfill: NULL-kid members inherit their alliance's bound state."""
        return gift_state_resolver.assign_alliance_kid_to_missing()

    async def resolve_remaining_missing_states(self, *, deep_sweep_max=0):
        """API-probe the members still missing a state and persist what we find."""
        fids = await asyncio.to_thread(gift_state_resolver.fids_missing_state)
        return await gift_state_resolver.resolve_and_persist(self, fids, deep_sweep_max=deep_sweep_max)

    async def _notify_state_migration(self):
        """Once per boot, DM global admins if member states need attention"""
        try:
            missing = await asyncio.to_thread(gift_state_resolver.fids_missing_state)
            survey = await asyncio.to_thread(gift_state_resolver.survey_alliance_bindings)
            unbound = [r for r in survey if r["current_kid"] is None and not r["multistate"]]
            if not missing and not unbound:
                return
            bindable = sum(1 for r in unbound if r["proposed_kid"] is not None)
            embed = discord.Embed(
                title=f"{theme.warnIcon} Member States Need Attention",
                description=(
                    "Gift code redemption now requires each member's state.\n\n"
                    f"{theme.upperDivider}\n"
                    f"{theme.membersIcon} **Members missing a state:** `{len(missing)}`\n"
                    f"{theme.allianceIcon} **Unbound alliances:** `{len(unbound)}` ({bindable} auto-bindable)\n"
                    f"{theme.lowerDivider}\n\n"
                    "Open **Alliance Management -> Member States** and run Auto-bind, "
                    "Assign, then Resolve. Members that already have a correct state redeem fine."
                ),
                color=theme.emColor2,
            )
            from . import gift_redemption
            await gift_redemption._dm_global_admins(self, embed)
        except Exception as e:
            self.logger.exception(f"Error sending state-migration nudge: {e}")

    async def scan_historical_messages(self, channel, alliance_id):
        return await gift_redemption.scan_historical_messages(self, channel, alliance_id)

    async def use_giftcode_for_alliance(self, alliance_id, giftcode):
        return await gift_redemption.use_giftcode_for_alliance(self, alliance_id, giftcode)

    async def cleanup_old_invalid_codes(self):
        return await gift_redemption.cleanup_old_invalid_codes(self)

    async def _process_auto_use(self, giftcode):
        return await gift_redemption._process_auto_use(self, giftcode)

    def schedule_revalidation(self, giftcode, source="unknown"):
        return gift_redemption.schedule_revalidation(self, giftcode, source)

    async def add_manual_redemption_to_queue(self, giftcodes, alliance_ids, interaction):
        return await gift_redemption.add_manual_redemption_to_queue(self, giftcodes, alliance_ids, interaction)

    async def get_queue_status(self):
        return await gift_redemption.get_queue_status(self)

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

    async def update_test_fid(self, new_fid, kid=None):
        return await gift_settings.update_test_fid(self, new_fid, kid)

    def get_test_fid(self):
        return gift_settings.get_test_fid(self)

    def clear_test_fid(self):
        return gift_settings.clear_test_fid(self)

    async def get_validation_fid(self):
        return await gift_settings.get_validation_fid(self)

    async def show_redemption_priority(self, interaction):
        return await gift_settings.show_redemption_priority(self, interaction)

    async def show_redemption_summary(self, interaction):
        return await gift_settings.show_redemption_summary(self, interaction)

    async def show_state_management(self, interaction):
        return await gift_settings.show_state_management(self, interaction)

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

    async def show_redeem_results(self, interaction):
        from .gift_redemption_results import show_redeem_results as _show
        return await _show(self, interaction)

    async def get_admin_info(self, user_id):
        from .gift_views import get_admin_info as _info
        return await _info(self, user_id)

    async def get_available_alliances(self, interaction):
        from .gift_views import get_available_alliances as _alliances
        return await _alliances(self, interaction)


async def setup(bot):
    await bot.add_cog(GiftOperations(bot))
