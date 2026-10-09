"""
Gift code orchestrator cog. Delegates to gift_redemption, gift_channels,
gift_settings, gift_views, and gift_operationsapi for the heavy lifting.
"""

import discord
from discord.ext import commands
import sqlite3
from discord.ext import tasks
import asyncio
import os
import traceback
import logging
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .gift_operationsapi import GiftCodeAPI
from .pimp_my_bot import theme, safe_edit_message
from . import gift_redemption
from . import gift_state_resolver
from . import onnx_lifecycle
from . import alliance_member_states
from . import process_queue
from . import gift_channels
from . import gift_settings
from .gift_views import GiftView

# raise_on_status=False: the final 5xx comes back as a response so callers can retry it later.
API_RETRY = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[500, 502, 503, 504],
    allowed_methods=["POST", "GET"],
    raise_on_status=False,
)


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
        self.retry_config = API_RETRY

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
        if hasattr(self, 'auto_kingdom_scan_loop') and self.auto_kingdom_scan_loop.is_running():
            self.auto_kingdom_scan_loop.cancel()
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

    def register_queue_handlers(self, process_queue_cog):
        process_queue_cog.register_handler(
            'gift_validate',
            lambda process: gift_redemption.handle_gift_validate_process(self, process)
        )
        process_queue_cog.register_handler(
            'gift_redeem',
            lambda process: gift_redemption.handle_gift_redeem_process(self, process)
        )
        process_queue_cog.register_handler(
            'gift_redeem_member',
            lambda process: gift_redemption.handle_member_redeem_process(self, process)
        )
        process_queue_cog.register_handler(
            'state_resolve',
            lambda process: gift_redemption.handle_state_resolve_process(self, process)
        )
        self.logger.info(
            "GiftOps: Registered gift_validate, gift_redeem, gift_redeem_member "
            "and state_resolve handlers with ProcessQueue")

    def _start_background_work(self):
        if not self.periodic_validation_loop.is_running():
            self.periodic_validation_loop.start()
        if not self.auto_kingdom_scan_loop.is_running():
            self.auto_kingdom_scan_loop.start()
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if process_queue_cog:
            self.register_queue_handlers(process_queue_cog)
        else:
            self.logger.error("GiftOps: ProcessQueue cog not found, gift code operations will not work")

    async def cog_load(self):
        # A reload never re-fires on_ready, so restart loops and handlers here when already connected.
        if self.bot.is_ready():
            self._start_background_work()

    async def _channel_is_gone(self, channel_id) -> bool:
        # Only a NotFound proves deletion; an outage or lost access must not wipe the config.
        try:
            await self.bot.fetch_channel(channel_id)
        except discord.NotFound:
            return True
        except discord.HTTPException as e:
            self.logger.warning(f"Gift code channel {channel_id} not reachable ({e}); keeping its config.")
        return False

    async def _prune_deleted_gift_channels(self):
        self.logger.info("Validating gift code channels...")
        self.cursor.execute("SELECT channel_id, alliance_id FROM giftcode_channel")
        channel_configs = self.cursor.fetchall()

        invalid_channels = []
        for channel_id, alliance_id in channel_configs:
            channel = self.bot.get_channel(channel_id)
            if not channel:
                if await self._channel_is_gone(channel_id):
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

    @commands.Cog.listener()
    async def on_ready(self):
        self.logger.info("GiftOps Cog: on_ready triggered.")
        try:
            self._start_background_work()

            # One-time nudge to global admins if member states need attention.
            if not self._state_nudge_sent:
                self._state_nudge_sent = True
                asyncio.create_task(self._notify_state_migration())

            await self._prune_deleted_gift_channels()

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

            candidates = gift_redemption._extract_content_codes(message.content)
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

    @tasks.loop(minutes=5)
    async def auto_kingdom_scan_loop(self):
        try:
            self.auto_kingdom_scan_tick()  # on the loop: ProcessQueue's cursor isn't thread-safe
        except Exception as e:
            self.logger.error(f"GiftOps: auto state scan check failed: {e}")
            print(f"GiftOps: auto state scan check failed: {e}")

    @auto_kingdom_scan_loop.before_loop
    async def before_auto_kingdom_scan_loop(self):
        await self.bot.wait_until_ready()

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
        """Fast no-API backfill: NULL-kid members inherit their alliance's bound state,
        then catch up on the codes they missed while stateless. Returns (assigned, caught_up)."""
        fids = gift_state_resolver.assign_alliance_kid_to_missing()
        caught = 0
        for fid in fids:
            try:
                if gift_redemption.enqueue_member_redemption(self, fid):
                    caught += 1
            except Exception as e:
                self.logger.warning(f"GiftOps: could not queue catch-up for FID {fid}: {e}")
        return len(fids), caught

    def queue_kingdom_scan(self, mode):
        """Queue the state scan ('now' or 'auto'). Returns members queued, 0 when nobody
        needs one, or None when a scan is already queued or running.

        Never run this inline - a single member can take hours. On the queue it sits below
        every other job and yields between probes, so an incoming gift code always redeems first."""
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if not process_queue_cog:
            self.logger.error("GiftOps: ProcessQueue cog not found; cannot queue a state scan")
            return None
        existing = self.kingdom_scan_status()
        if existing is not None and (mode == 'auto' or existing['mode'] == 'now'):
            return None
        targets = gift_state_resolver.scan_targets(auto=(mode == 'auto'))
        if not targets:
            return 0
        # One step above auto, so an admin's scan preempts a slow auto-scan instead of waiting days.
        priority = process_queue.STATE_RESOLVE - 1 if mode == 'now' else process_queue.STATE_RESOLVE
        process_queue_cog.enqueue(
            'state_resolve', priority,
            details={'mode': mode, 'remaining': targets, 'total': len(targets),
                     'resolved': 0, 'unresolved': 0},
        )
        self.logger.info(f"GiftOps: queued state scan ({mode}) for {len(targets)} member(s)")
        return len(targets)

    def kingdom_scan_status(self):
        """The queued or running scan's progress, or None when no scan is queued."""
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if not process_queue_cog:
            return None
        for job in process_queue_cog.get_queued_processes_by_action(
                'state_resolve', statuses=('queued', 'active')):
            return {**job['details'], 'mode': job['details'].get('mode', 'now'), 'status': job['status']}
        return None

    def auto_kingdom_scan_tick(self):
        """Queue an auto-scan when it's switched on and the bot has nothing else to do."""
        if not gift_state_resolver.scan_enabled():
            return False
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if not process_queue_cog:
            return False
        counts = process_queue_cog.queue_counts()
        if counts['queued'] or counts['active'] or onnx_lifecycle.any_model_in_use():
            return False
        return bool(self.queue_kingdom_scan('auto'))

    def member_catchup_status(self):
        """Pending code catch-ups: {'members': n, 'codes': n, 'done': n} or None when idle."""
        process_queue_cog = self.bot.get_cog('ProcessQueue')
        if not process_queue_cog:
            return None
        jobs = process_queue_cog.get_queued_processes_by_action(
            'gift_redeem_member', statuses=('queued', 'active'))
        if not jobs:
            return None
        return {
            'members': len(jobs),
            'codes': sum(len(j['details'].get('codes') or []) for j in jobs),
            'done': sum(j['details'].get('done', 0) for j in jobs),
        }

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
                    "Open **Alliance Management -> Member States**: give alliances their state "
                    "under **Alliance States**, then fix members under **Members to Fix**. "
                    "Members that already have a correct state redeem fine."
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
        return await alliance_member_states.show_state_management(self, interaction)

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
