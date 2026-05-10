"""
Bot health dashboard. Shows API status, database health, system info, and cleanup tools.
"""
import discord
from discord.ext import commands, tasks
import sqlite3
import os
import sys
import platform
import asyncio
import subprocess
import aiohttp
import zipfile
import shutil
from datetime import datetime, timezone, timedelta
import logging
from importlib.metadata import version as get_package_version, PackageNotFoundError
from packaging.version import parse as parse_version
import re
from .permission_handler import PermissionManager
from .pimp_my_bot import theme, safe_edit_message


# Health status constants
STATUS_HEALTHY = "healthy"
STATUS_WARNING = "warning"
STATUS_ERROR = "error"

# Thresholds
DB_SIZE_WARNING_MB = 100


def is_container() -> bool:
    """Check if running in a container (Docker, Kubernetes, Podman, LXC, systemd-nspawn)"""
    # Docker, Kubernetes, Podman - simple marker file checks
    marker_files = ["/.dockerenv", "/var/run/secrets/kubernetes.io", "/run/.containerenv"]
    if any(os.path.exists(path) for path in marker_files):
        return True

    # LXC - check init process environment
    try:
        with open("/proc/1/environ", "r") as f:
            if "container=lxc" in f.read():
                return True
    except (IOError, OSError):
        pass

    # Systemd-nspawn - check container type file
    try:
        with open("/run/systemd/container", "r") as f:
            if f.read().strip() == "systemd-nspawn":
                return True
    except (IOError, OSError):
        pass

    return False
DB_SIZE_ERROR_MB = 500
WAL_SIZE_WARNING_MB = 1
WAL_SIZE_ERROR_MB = 10
LOG_SIZE_WARNING_MB = 50
LOG_SIZE_ERROR_MB = 100
ORPHANED_LOGS_WARNING = 1
ORPHANED_LOGS_ERROR = 5
LATENCY_WARNING_MS = 250
LATENCY_ERROR_MS = 800
# Bot's actual disk needs: ~500 MB for backups + logs + update downloads.
# Tuned for small VPS hosts where 1 GB total is common.
DISK_FREE_WARNING_MB = 500
DISK_FREE_ERROR_MB = 100
DISK_USED_PCT_WARNING = 90
DISK_USED_PCT_ERROR = 95

# Active log file names (files that should not be archived)
ACTIVE_LOG_NAMES = [
    # Category logs written by the RotatingFileHandlers in main.setup_logging —
    # never archive these; Windows will WinError 32 on the open handle.
    'alliance.txt', 'bot.txt', 'gift.txt', 'notification.txt', 'redemption.txt',
    'rapidocr.txt',
    # Per-feature logs written directly by cogs
    'alliance_control.txt', 'alliance_memberlog.txt', 'alliance_sync.txt',
    'backuplog.txt', 'bear_trap.txt', 'db_maintenance.txt', 'gift_ops.txt',
    'gift_solver.txt', 'giftlog.txt', 'id_channel_log.txt', 'login_handler.txt',
    'notifications.txt', 'verification.txt', 'add_memberlog.txt',
]

# Helper/utility files in cogs folder that are NOT loaded as cogs directly
# These are imported by other cogs and should not be flagged as unused
HELPER_FILES = [
    'permission_handler',      # Permission checking utilities
    'login_handler',           # API login handling
    'gift_operationsapi',      # Gift code API class
    'gift_captchasolver',      # Captcha solving utilities
    'notification_event_types', # Notification constants/types
    'pimp_my_bot_editor',      # Theme editor utilities
    'pimp_my_bot_preview',     # Theme preview utilities
    'onnx_lifecycle',          # OCR model lazy-load + eviction (explicit safelist)
    '__init__',                # Package init file
]

# Packages from older bot versions (v1-v3) that may linger in user venvs.
# Detected during cleanup and pip-uninstalled if still installed.
LEGACY_PACKAGES_TO_REMOVE = [
    'ddddocr', 'easyocr', 'torch', 'torchvision', 'torchaudio',
]

# Filesystem leftovers from older versions or aborted updates.
LEGACY_DIRS = ['V1oldbot', 'V2Old', 'pictures']
LEGACY_FILES = ['autoupdateinfo.txt']

# Update-flow artifacts: removed if present at cleanup time. The update flow
# itself cleans these on success, so finding them means an aborted run.
UPDATE_ARTIFACTS_DIRS = ['update', 'cogs.bak']
UPDATE_ARTIFACTS_FILES = ['package.zip', 'main.py.bak', 'requirements.old']


class BotHealth(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = "db"
        self.log_path = "log"
        self.archive_path = "log/archive"
        self.settings_db_path = "db/settings.sqlite"
        self.gift_api_url = "http://gift-code-api.whiteout-bot.com/giftcode_api.php"
        self.gift_api_key = "super_secret_bot_token_nobody_will_ever_find"

        self.logger = logging.getLogger('bot')
        self.start_time = datetime.now(timezone.utc)
        # Cached bot directory size in MB; refreshed every 15 minutes by
        # update_bot_footprint_loop so dashboard renders stay instant.
        self._bot_footprint_mb: float | None = None
        # Cached API status results; refreshed by api_status_loop so the
        # dashboard doesn't depend on live HTTP calls (which can blow past
        # Discord's 3 s interaction timeout on a slow VPS).
        self._cached_wos_api: dict | None = None
        self._cached_gift_api: dict | None = None
        self._api_cache_at: datetime | None = None

        self._setup_database()
        self.maintenance_loop.start()
        self.update_bot_footprint_loop.start()
        self.api_status_loop.start()
        self.logger.info("[HEALTH] Bot Health cog initialized")

    def _setup_database(self):
        """Create/update health_config and restart_marker tables."""
        os.makedirs('db', exist_ok=True)
        os.makedirs(self.archive_path, exist_ok=True)

        conn = sqlite3.connect(self.settings_db_path, timeout=30.0)
        cursor = conn.cursor()

        # Check if old table exists and migrate
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='maintenance_config'")
        old_table_exists = cursor.fetchone() is not None

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS health_config (
                id INTEGER PRIMARY KEY DEFAULT 1,
                cleanup_hour INTEGER DEFAULT 3,
                cleanup_minute INTEGER DEFAULT 0,
                monthly_optimization_day INTEGER DEFAULT 0,
                notify_user_id INTEGER,
                last_cleanup_date TEXT,
                last_optimization_date TEXT,
                custom_helper_files TEXT DEFAULT ''
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS restart_marker (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                initiated_at REAL NOT NULL
            )
        """)

        # Add custom_helper_files column if it doesn't exist (migration)
        cursor.execute("PRAGMA table_info(health_config)")
        columns = [col[1] for col in cursor.fetchall()]
        if 'custom_helper_files' not in columns:
            cursor.execute("ALTER TABLE health_config ADD COLUMN custom_helper_files TEXT DEFAULT ''")

        # Insert default row if table is empty
        cursor.execute("SELECT COUNT(*) FROM health_config")
        if cursor.fetchone()[0] == 0:
            # Try to migrate from old config
            if old_table_exists:
                cursor.execute("""
                    INSERT INTO health_config (
                        cleanup_hour, cleanup_minute, monthly_optimization_day,
                        notify_user_id, last_cleanup_date, last_optimization_date
                    )
                    SELECT
                        maintenance_hour, maintenance_minute,
                        CASE WHEN auto_vacuum_enabled = 1 THEN auto_vacuum_day ELSE 0 END,
                        admin_user_id, last_checkpoint_date, last_vacuum_date
                    FROM maintenance_config WHERE id = 1
                """)
            else:
                cursor.execute("INSERT INTO health_config (id) VALUES (1)")

        conn.commit()
        conn.close()

    def get_config(self) -> dict:
        """Retrieve current health configuration"""
        conn = sqlite3.connect(self.settings_db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM health_config WHERE id = 1")
        row = cursor.fetchone()
        conn.close()

        if row:
            return dict(row)
        return {
            'cleanup_hour': 3,
            'cleanup_minute': 0,
            'monthly_optimization_day': 0,
            'notify_user_id': None,
            'last_cleanup_date': None,
            'last_optimization_date': None,
            'custom_helper_files': ''
        }

    def get_custom_helper_files(self) -> list:
        """Get list of custom helper files that should be excluded from cleanup"""
        config = self.get_config()
        custom_files = config.get('custom_helper_files', '')
        if not custom_files:
            return []
        # Split by comma or newline, strip whitespace, remove empty strings and .py extension
        files = []
        for f in custom_files.replace('\n', ',').split(','):
            f = f.strip()
            if f:
                # Remove .py extension if present
                if f.endswith('.py'):
                    f = f[:-3]
                files.append(f)
        return files

    def set_custom_helper_files(self, files: list):
        """Set list of custom helper files"""
        # Store as comma-separated, without .py extension
        clean_files = []
        for f in files:
            f = f.strip()
            if f:
                if f.endswith('.py'):
                    f = f[:-3]
                clean_files.append(f)
        self.update_config(custom_helper_files=','.join(clean_files))

    def update_config(self, **kwargs):
        """Update configuration values"""
        if not kwargs:
            return

        conn = sqlite3.connect(self.settings_db_path, timeout=30.0)
        cursor = conn.cursor()

        set_clauses = ", ".join([f"{k} = ?" for k in kwargs.keys()])
        values = list(kwargs.values())

        cursor.execute(f"UPDATE health_config SET {set_clauses} WHERE id = 1", values)
        conn.commit()
        conn.close()

    API_CACHE_TTL_SECONDS = 90  # refresh threshold; loop polls every 60s
    API_CACHE_MAX_AGE_SECONDS = 600  # after this, treat cache as too stale to show

    async def check_wos_api_status(self) -> dict:
        """Cached WOS API status. Returns cache when fresh; otherwise probes
        once and caches the result. Background api_status_loop keeps this warm
        so dashboard renders stay under Discord's 3 s interaction timeout."""
        if self._cached_wos_api and self._api_cache_at:
            age = (datetime.now(timezone.utc) - self._api_cache_at).total_seconds()
            if age < self.API_CACHE_TTL_SECONDS:
                return self._cached_wos_api
        result = await self._probe_wos_api()
        self._cached_wos_api = result
        self._api_cache_at = datetime.now(timezone.utc)
        return result

    async def _probe_wos_api(self) -> dict:
        """Actual live probe — only called by the cached wrapper or the
        background refresh loop."""
        try:
            login_handler_cog = self.bot.get_cog("LoginHandler")
            if login_handler_cog:
                handler = login_handler_cog.handler
            else:
                from .login_handler import LoginHandler
                handler = LoginHandler()

            status = await handler.check_apis_availability()

            if status['api1_available'] and status['api2_available']:
                return {'status': STATUS_HEALTHY, 'message': 'Dual-API (fast)'}
            elif status['api1_available'] or status['api2_available']:
                api_down = '2' if status['api1_available'] else '1'
                return {'status': STATUS_WARNING, 'message': f'Single-API (API {api_down} down)'}
            else:
                return {'status': STATUS_ERROR, 'message': 'Both APIs unavailable'}

        except Exception as e:
            self.logger.error(f"Error checking WOS API status: {e}")
            return {'status': STATUS_ERROR, 'message': f'Check failed: {str(e)[:30]}'}

    async def check_gift_distribution_api(self) -> dict:
        """Cached Gift Distribution API status. See check_wos_api_status."""
        if self._cached_gift_api and self._api_cache_at:
            age = (datetime.now(timezone.utc) - self._api_cache_at).total_seconds()
            if age < self.API_CACHE_TTL_SECONDS:
                return self._cached_gift_api
        result = await self._probe_gift_distribution_api()
        self._cached_gift_api = result
        self._api_cache_at = datetime.now(timezone.utc)
        return result

    async def _probe_gift_distribution_api(self) -> dict:
        """Actual live probe — only called by the cached wrapper or the
        background refresh loop."""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                start = datetime.now()
                headers = {'X-API-Key': self.gift_api_key}
                async with session.get(self.gift_api_url, headers=headers) as response:
                    elapsed = (datetime.now() - start).total_seconds()

                    if response.status == 200:
                        if elapsed > 3:
                            return {'status': STATUS_WARNING, 'message': f'Online (slow: {elapsed:.1f}s)'}
                        return {'status': STATUS_HEALTHY, 'message': 'Online'}
                    else:
                        return {'status': STATUS_ERROR, 'message': f'Error (HTTP {response.status})'}
        except asyncio.TimeoutError:
            return {'status': STATUS_ERROR, 'message': 'Timeout (>5s)'}
        except aiohttp.ClientError:
            return {'status': STATUS_ERROR, 'message': 'Connection failed'}
        except Exception as e:
            self.logger.error(f"Error checking Gift API status: {e}")
            return {'status': STATUS_ERROR, 'message': 'Check failed'}

    def get_database_health(self) -> dict:
        """Get database health status"""
        if not os.path.exists(self.db_path):
            return {'status': STATUS_ERROR, 'total_mb': 0, 'wal_mb': 0, 'message': 'DB folder missing'}

        total_size = 0
        wal_size = 0

        for filename in os.listdir(self.db_path):
            filepath = os.path.join(self.db_path, filename)
            if os.path.isfile(filepath):
                size = os.path.getsize(filepath)
                total_size += size
                if filename.endswith('-wal'):
                    wal_size += size

        total_mb = total_size / (1024 * 1024)
        wal_mb = wal_size / (1024 * 1024)

        config = self.get_config()
        last_cleanup = config.get('last_cleanup_date') or 'Never'

        # Determine status
        if total_mb > DB_SIZE_ERROR_MB or wal_mb > WAL_SIZE_ERROR_MB:
            status = STATUS_ERROR
        elif total_mb > DB_SIZE_WARNING_MB or wal_mb > WAL_SIZE_WARNING_MB:
            status = STATUS_WARNING
        else:
            status = STATUS_HEALTHY

        return {
            'status': status,
            'total_mb': total_mb,
            'wal_mb': wal_mb,
            'last_cleanup': last_cleanup,
            'message': f'{total_mb:.1f} MB'
        }

    def get_disk_health(self) -> dict:
        """Disk space at the bot's working directory. Status warns when free
        space is low so admins notice before backups/logs start failing.
        Tuned for small VPS hosts — error fires under 100 MB, warning under
        500 MB (the bot's stated minimum)."""
        try:
            total, used, free = shutil.disk_usage(".")
        except Exception:
            return {
                'status': STATUS_WARNING,
                'free_mb': 0, 'total_mb': 0, 'used_pct': 0,
                'message': 'Disk usage unavailable',
            }

        total_mb = total / (1024 * 1024)
        free_mb = free / (1024 * 1024)
        used_pct = (used / total) * 100 if total else 0

        if free_mb < DISK_FREE_ERROR_MB or used_pct >= DISK_USED_PCT_ERROR:
            status = STATUS_ERROR
        elif free_mb < DISK_FREE_WARNING_MB or used_pct >= DISK_USED_PCT_WARNING:
            status = STATUS_WARNING
        else:
            status = STATUS_HEALTHY

        def _fmt(mb):
            return f"{mb / 1024:.1f} GB" if mb >= 1024 else f"{mb:.0f} MB"

        return {
            'status': status,
            'free_mb': free_mb,
            'total_mb': total_mb,
            'used_pct': used_pct,
            'message': f"{_fmt(free_mb)} free of {_fmt(total_mb)} ({used_pct:.0f}% used)",
        }

    def get_log_health(self) -> dict:
        """Get log folder health status"""
        if not os.path.exists(self.log_path):
            return {'status': STATUS_HEALTHY, 'total_mb': 0, 'orphaned_count': 0, 'message': 'No logs'}

        total_size = 0
        orphaned_files = []

        for filename in os.listdir(self.log_path):
            filepath = os.path.join(self.log_path, filename)
            if os.path.isfile(filepath):
                total_size += os.path.getsize(filepath)

                # Check if orphaned
                if self._is_orphaned_log(filename):
                    orphaned_files.append(filename)

        total_mb = total_size / (1024 * 1024)
        orphaned_count = len(orphaned_files)

        # Determine status
        if total_mb > LOG_SIZE_ERROR_MB or orphaned_count > ORPHANED_LOGS_ERROR:
            status = STATUS_ERROR
        elif total_mb > LOG_SIZE_WARNING_MB or orphaned_count >= ORPHANED_LOGS_WARNING:
            status = STATUS_WARNING
        else:
            status = STATUS_HEALTHY

        return {
            'status': status,
            'total_mb': total_mb,
            'orphaned_count': orphaned_count,
            'orphaned_files': orphaned_files,
            'message': f'{total_mb:.1f} MB' + (f' ({orphaned_count} old files)' if orphaned_count else '')
        }

    def _is_orphaned_log(self, filename: str) -> bool:
        """Check if a log file is orphaned (rotated or old)"""
        # Skip archive folder
        if filename == 'archive':
            return False

        # Check for rotation patterns: .1, .2, -HOSTNAME.1, etc.
        if any(filename.endswith(f'.{i}') for i in range(1, 10)):
            return True
        if '-' in filename and any(f'.{i}' in filename for i in range(1, 10)):
            return True

        # Check if it's an active log name
        if filename in ACTIVE_LOG_NAMES:
            return False

        # Check file age (older than 7 days and not active)
        try:
            filepath = os.path.join(self.log_path, filename)
            mtime = datetime.fromtimestamp(os.path.getmtime(filepath), timezone.utc)
            if datetime.now(timezone.utc) - mtime > timedelta(days=7):
                if filename not in ACTIVE_LOG_NAMES:
                    return True
        except Exception:
            pass

        return False

    def _scan_imported_module_names(self) -> set:
        """Scan main.py and every cog file for sibling imports, return the set
        of cogs/ module names referenced anywhere in the project. Catches
        helpers that aren't registered as extensions but are still in use."""
        import re

        single_patterns = (
            re.compile(r'^\s*from\s+\.(\w+)\s+import', re.MULTILINE),
            re.compile(r'^\s*from\s+cogs\.(\w+)\s+import', re.MULTILINE),
            re.compile(r'^\s*import\s+cogs\.(\w+)', re.MULTILINE),
        )
        list_patterns = (
            re.compile(r'^\s*from\s+cogs\s+import\s+(.+)$', re.MULTILINE),
            re.compile(r'^\s*from\s+\.\s+import\s+(.+)$', re.MULTILINE),
        )

        files_to_scan = []
        if os.path.isfile('main.py'):
            files_to_scan.append('main.py')
        cogs_path = 'cogs'
        if os.path.isdir(cogs_path):
            for fn in os.listdir(cogs_path):
                if fn.endswith('.py'):
                    files_to_scan.append(os.path.join(cogs_path, fn))

        imported = set()
        for filepath in files_to_scan:
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    content = f.read()
            except Exception:
                continue
            for pat in single_patterns:
                for m in pat.finditer(content):
                    imported.add(m.group(1))
            for pat in list_patterns:
                for m in pat.finditer(content):
                    payload = m.group(1).split('#', 1)[0]
                    for piece in payload.split(','):
                        tokens = piece.strip().split()
                        if tokens and tokens[0].replace('_', '').isalnum():
                            imported.add(tokens[0])
        return imported

    def get_unused_cog_files(self) -> list:
        """Detect cog files that are neither loaded as extensions nor imported
        by any other module in the project. Custom-helper exceptions still
        apply on top of the import scan."""
        cogs_path = "cogs"
        if not os.path.exists(cogs_path):
            return []

        loaded_module_names = {
            ext[5:] for ext in self.bot.extensions.keys()
            if ext.startswith("cogs.")
        }
        all_helper_files = set(HELPER_FILES) | set(self.get_custom_helper_files())
        imported_names = self._scan_imported_module_names()

        unused_files = []
        for filename in os.listdir(cogs_path):
            if not filename.endswith('.py'):
                continue
            if filename.startswith('__') and filename != '__init__.py':
                continue

            module_name = filename[:-3]
            if module_name in all_helper_files:
                continue
            if module_name in loaded_module_names:
                continue
            if module_name in imported_names:
                continue

            unused_files.append(filename)

        return sorted(unused_files)

    async def archive_unused_cogs(self, files_to_archive: list) -> dict:
        """
        Archive unused cog files to cogs/archive folder.
        Returns dict with results.
        """
        results = {
            'archived': 0,
            'errors': [],
            'archived_files': []
        }

        if not files_to_archive:
            return results

        cogs_path = "cogs"
        archive_path = os.path.join(cogs_path, "old_cogs_archive")
        os.makedirs(archive_path, exist_ok=True)

        for filename in files_to_archive:
            src_path = os.path.join(cogs_path, filename)
            if not os.path.exists(src_path):
                continue

            try:
                # Add timestamp to avoid overwriting
                timestamp = datetime.now().strftime('%Y%m%d')
                archive_name = f"{filename[:-3]}_{timestamp}.py"
                dst_path = os.path.join(archive_path, archive_name)

                # If file already exists in archive, add counter
                counter = 1
                while os.path.exists(dst_path):
                    archive_name = f"{filename[:-3]}_{timestamp}_{counter}.py"
                    dst_path = os.path.join(archive_path, archive_name)
                    counter += 1

                shutil.move(src_path, dst_path)
                results['archived'] += 1
                results['archived_files'].append(filename)
                self.logger.info(f"Archived unused cog: {filename} -> {archive_name}")

            except Exception as e:
                results['errors'].append(f"{filename}: {e}")
                self.logger.error(f"Failed to archive cog {filename}: {e}")

        return results

    def get_system_health(self) -> dict:
        """Get system health info"""
        # Uptime
        uptime = datetime.now(timezone.utc) - self.start_time
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes = remainder // 60
        uptime_str = f"{days}d {hours}h {minutes}m" if days else f"{hours}h {minutes}m"

        # Latency
        latency_ms = round(self.bot.latency * 1000)
        if latency_ms > LATENCY_ERROR_MS:
            latency_status = STATUS_ERROR
        elif latency_ms > LATENCY_WARNING_MS:
            latency_status = STATUS_WARNING
        else:
            latency_status = STATUS_HEALTHY

        # Cogs
        loaded_cogs = len(self.bot.cogs)

        # Python/Platform info
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        platform_name = platform.system()

        # Container detection (Docker, Kubernetes, Podman, LXC, etc.)
        if is_container():
            platform_name = "Container"

        return {
            'uptime': uptime_str,
            'latency_ms': latency_ms,
            'latency_status': latency_status,
            'loaded_cogs': loaded_cogs,
            'python_version': python_version,
            'platform': platform_name
        }

    def get_requirements_health(self) -> dict:
        """Check installed packages against requirements.txt"""
        requirements_file = "requirements.txt"
        missing = []
        outdated = []
        ok_count = 0

        try:
            if not os.path.isfile(requirements_file):
                return {
                    'status': STATUS_WARNING,
                    'missing': [],
                    'outdated': [],
                    'ok_count': 0,
                    'total': 0,
                    'error': 'requirements.txt not found'
                }

            with open(requirements_file, 'r') as f:
                lines = f.readlines()

            for line in lines:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                # Parse requirement (e.g., "discord.py>=2.5.2" or "aiohttp>=3.11.18")
                match = re.match(r'^([a-zA-Z0-9_.-]+)\s*([<>=!]+)?\s*([0-9a-zA-Z.*]+)?', line)
                if not match:
                    continue

                package_name = match.group(1)
                operator = match.group(2)
                required_version = match.group(3)

                # Normalize package name for lookup (e.g., discord.py -> discord-py)
                lookup_name = package_name.replace('_', '-').lower()

                try:
                    installed_version = get_package_version(lookup_name)

                    # Check version if specified
                    if operator and required_version:
                        installed_v = parse_version(installed_version)
                        required_v = parse_version(required_version)

                        version_ok = True
                        if '>=' in operator:
                            version_ok = installed_v >= required_v
                        elif '==' in operator:
                            version_ok = installed_v == required_v
                        elif '>' in operator:
                            version_ok = installed_v > required_v

                        if not version_ok:
                            outdated.append({
                                'package': package_name,
                                'required': f"{operator}{required_version}",
                                'installed': installed_version
                            })
                        else:
                            ok_count += 1
                    else:
                        ok_count += 1

                except PackageNotFoundError:
                    missing.append(package_name)

            total = ok_count + len(missing) + len(outdated)

            # Determine status
            if missing:
                status = STATUS_ERROR
            elif outdated:
                status = STATUS_WARNING
            else:
                status = STATUS_HEALTHY

            return {
                'status': status,
                'missing': missing,
                'outdated': outdated,
                'ok_count': ok_count,
                'total': total,
                'error': None
            }

        except Exception as e:
            return {
                'status': STATUS_ERROR,
                'missing': [],
                'outdated': [],
                'ok_count': 0,
                'total': 0,
                'error': str(e)
            }

    def get_overall_status(self, db_health: dict, log_health: dict, system_health: dict,
                           wos_api: dict = None, gift_api: dict = None,
                           requirements: dict = None) -> str:
        """Determine overall health status"""
        statuses = [
            db_health['status'], log_health['status'], system_health['latency_status'],
            self.get_disk_health()['status'],
        ]

        if wos_api:
            statuses.append(wos_api['status'])
        if gift_api:
            statuses.append(gift_api['status'])
        if requirements:
            statuses.append(requirements['status'])

        if STATUS_ERROR in statuses:
            return STATUS_ERROR
        elif STATUS_WARNING in statuses:
            return STATUS_WARNING
        return STATUS_HEALTHY

    def _status_icon(self, status: str) -> str:
        """Get status icon"""
        if status == STATUS_HEALTHY:
            return theme.verifiedIcon
        elif status == STATUS_WARNING:
            return theme.warnIcon
        return theme.deniedIcon

    def _status_color(self, status: str) -> int:
        """Get embed color for status"""
        if status == STATUS_HEALTHY:
            return theme.emColor3  # Green
        elif status == STATUS_WARNING:
            return theme.emColor2  # Yellow/Orange
        return 0xFF0000  # Red

    async def run_cleanup(self) -> dict:
        """Routine unattended cleanup: WAL checkpoints, log archival, legacy
        artifacts, and stale update files. Cog archival is intentionally NOT
        done here — it requires admin confirmation via the manual Run Cleanup
        flow, since misdetection can archive in-use helpers."""
        results = {
            'db_cleaned_mb': 0,
            'logs_archived': 0,
            'legacy_artifacts_removed': [],
            'legacy_packages_removed': [],
            'errors': []
        }

        try:
            checkpoint_results = await self._checkpoint_all_databases()
            for r in checkpoint_results:
                if r.get('success') and not r.get('skipped'):
                    results['db_cleaned_mb'] += (r['wal_size_before'] - r['wal_size_after']) / (1024 * 1024)
        except Exception as e:
            results['errors'].append(f"DB cleanup: {e}")
            self.logger.error(f"Cleanup DB error: {e}")

        try:
            archived = await self._archive_orphaned_logs()
            results['logs_archived'] = archived
        except Exception as e:
            results['errors'].append(f"Log archival: {e}")
            self.logger.error(f"Cleanup log error: {e}")

        try:
            results['legacy_artifacts_removed'] = self._remove_legacy_artifacts()
        except Exception as e:
            results['errors'].append(f"Legacy artifacts: {e}")
            self.logger.error(f"Cleanup legacy artifacts error: {e}")

        try:
            results['legacy_packages_removed'] = await self._remove_legacy_packages()
        except Exception as e:
            results['errors'].append(f"Legacy packages: {e}")
            self.logger.error(f"Cleanup legacy packages error: {e}")

        self.update_config(last_cleanup_date=datetime.now(timezone.utc).date().isoformat())

        return results

    def _remove_legacy_artifacts(self) -> list:
        """Remove leftover directories and files from older bot versions and
        aborted updates. Returns the list of paths that were removed."""
        removed = []

        for dirname in LEGACY_DIRS + UPDATE_ARTIFACTS_DIRS:
            if os.path.isdir(dirname):
                try:
                    shutil.rmtree(dirname, onerror=self._on_rmtree_error)
                    removed.append(dirname)
                except Exception as e:
                    self.logger.warning(f"Could not remove dir {dirname}: {e}")

        for filename in LEGACY_FILES + UPDATE_ARTIFACTS_FILES:
            if os.path.isfile(filename):
                try:
                    os.remove(filename)
                    removed.append(filename)
                except Exception as e:
                    self.logger.warning(f"Could not remove file {filename}: {e}")

        # Stale db.bak / db.bak_<timestamp> created by the update flow.
        # Only remove if older than 7 days so a recent backup is still recoverable.
        cutoff = datetime.now(timezone.utc).timestamp() - 7 * 86400
        for entry in os.listdir('.'):
            if entry == 'db.bak' or entry.startswith('db.bak_'):
                full = os.path.abspath(entry)
                try:
                    if os.path.getmtime(full) < cutoff and os.path.isdir(full):
                        shutil.rmtree(full, onerror=self._on_rmtree_error)
                        removed.append(entry)
                except Exception as e:
                    self.logger.warning(f"Could not remove {entry}: {e}")

        return removed

    @staticmethod
    def _on_rmtree_error(func, path, _):
        """rmtree onerror hook: clear read-only bit (Windows) and retry."""
        try:
            import stat
            os.chmod(path, stat.S_IWRITE)
            func(path)
        except Exception:
            pass

    async def _remove_legacy_packages(self) -> list:
        """Pip-uninstall legacy packages from old bot versions if installed.
        Runs pip in a thread so the event loop isn't blocked."""
        installed_legacy = []
        for pkg in LEGACY_PACKAGES_TO_REMOVE:
            try:
                get_package_version(pkg)
                installed_legacy.append(pkg)
            except PackageNotFoundError:
                continue
            except Exception:
                continue

        removed = []
        for pkg in installed_legacy:
            try:
                proc = await asyncio.to_thread(
                    subprocess.run,
                    [sys.executable, "-m", "pip", "uninstall", "-y", pkg],
                    capture_output=True, timeout=300,
                )
                if proc.returncode == 0:
                    removed.append(pkg)
                else:
                    self.logger.warning(f"pip uninstall {pkg} returned {proc.returncode}")
            except Exception as e:
                self.logger.warning(f"Could not uninstall {pkg}: {e}")

        return removed

    async def _checkpoint_all_databases(self) -> list:
        """Checkpoint all databases"""
        results = []
        if not os.path.exists(self.db_path):
            return results

        db_files = [f for f in os.listdir(self.db_path) if f.endswith('.sqlite')]

        for db_file in db_files:
            db_path = os.path.join(self.db_path, db_file)
            result = await self._checkpoint_database(db_path)
            results.append(result)

        return results

    async def _checkpoint_database(self, db_path: str) -> dict:
        """Run WAL checkpoint on a single database"""
        result = {
            'database': os.path.basename(db_path),
            'success': False,
            'wal_size_before': 0,
            'wal_size_after': 0,
            'skipped': False,
            'error': None
        }

        wal_path = f"{db_path}-wal"

        try:
            if os.path.exists(wal_path):
                result['wal_size_before'] = os.path.getsize(wal_path)

            # Skip if WAL < 1KB
            if result['wal_size_before'] < 1024:
                result['success'] = True
                result['skipped'] = True
                return result

            def do_checkpoint():
                conn = sqlite3.connect(db_path, timeout=30.0)
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.close()

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, do_checkpoint)

            if os.path.exists(wal_path):
                result['wal_size_after'] = os.path.getsize(wal_path)

            result['success'] = True

        except Exception as e:
            result['error'] = str(e)
            self.logger.error(f"Checkpoint failed for {db_path}: {e}")

        return result

    async def _archive_orphaned_logs(self) -> int:
        """Archive orphaned log files and return count"""
        if not os.path.exists(self.log_path):
            return 0

        orphaned = []
        for filename in os.listdir(self.log_path):
            filepath = os.path.join(self.log_path, filename)
            if os.path.isfile(filepath) and self._is_orphaned_log(filename):
                orphaned.append(filepath)

        if not orphaned:
            return 0

        # Create archive
        os.makedirs(self.archive_path, exist_ok=True)
        archive_name = f"{datetime.now().strftime('%Y-%m-%d')}-cleanup.zip"
        archive_path = os.path.join(self.archive_path, archive_name)

        # If archive already exists today, append number
        counter = 1
        while os.path.exists(archive_path):
            archive_name = f"{datetime.now().strftime('%Y-%m-%d')}-cleanup-{counter}.zip"
            archive_path = os.path.join(self.archive_path, archive_name)
            counter += 1

        try:
            with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for filepath in orphaned:
                    zf.write(filepath, os.path.basename(filepath))

            # Delete originals after successful archive
            for filepath in orphaned:
                os.remove(filepath)

            self.logger.info(f"Archived {len(orphaned)} log files to {archive_name}")
            return len(orphaned)

        except Exception as e:
            self.logger.error(f"Failed to archive logs: {e}")
            raise

    async def run_optimization(self) -> dict:
        """Run deep optimization (VACUUM) on all databases"""
        results = {
            'space_recovered_mb': 0,
            'databases_optimized': 0,
            'errors': []
        }

        if not os.path.exists(self.db_path):
            return results

        db_files = [f for f in os.listdir(self.db_path) if f.endswith('.sqlite')]

        for db_file in db_files:
            db_path = os.path.join(self.db_path, db_file)
            try:
                size_before = os.path.getsize(db_path)

                def do_vacuum():
                    conn = sqlite3.connect(db_path, timeout=60.0)
                    conn.execute("VACUUM")
                    conn.close()

                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, do_vacuum)

                size_after = os.path.getsize(db_path)
                results['space_recovered_mb'] += (size_before - size_after) / (1024 * 1024)
                results['databases_optimized'] += 1

            except Exception as e:
                results['errors'].append(f"{db_file}: {e}")
                self.logger.error(f"VACUUM failed for {db_path}: {e}")

        self.update_config(last_optimization_date=datetime.now(timezone.utc).date().isoformat())
        return results

    async def cleanup_old_archives(self, days: int = 30):
        """Delete archives older than specified days"""
        if not os.path.exists(self.archive_path):
            return

        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        for filename in os.listdir(self.archive_path):
            filepath = os.path.join(self.archive_path, filename)
            if os.path.isfile(filepath):
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(filepath), timezone.utc)
                    if mtime < cutoff:
                        os.remove(filepath)
                        self.logger.info(f"Deleted old archive: {filename}")
                except Exception as e:
                    self.logger.error(f"Failed to delete old archive {filename}: {e}")

    @tasks.loop(minutes=1)
    async def maintenance_loop(self):
        """Check if it's time to run maintenance"""
        try:
            config = self.get_config()
            now = datetime.now(timezone.utc)

            # Check if it's cleanup time
            if now.hour == config['cleanup_hour'] and now.minute == config['cleanup_minute']:
                # Check if already ran today
                last_cleanup = config.get('last_cleanup_date')
                if last_cleanup == now.date().isoformat():
                    return

                self.logger.info("Starting scheduled cleanup")
                results = await self.run_cleanup()

                # Check if monthly optimization is due
                opt_day = config.get('monthly_optimization_day', 0)
                if opt_day > 0 and now.day == opt_day:
                    last_opt = config.get('last_optimization_date')
                    if last_opt != now.date().isoformat():
                        self.logger.info("Starting monthly optimization")
                        opt_results = await self.run_optimization()

                        # Notify admin
                        notify_id = config.get('notify_user_id')
                        if notify_id:
                            await self._notify_user(
                                notify_id,
                                f"{theme.verifiedIcon} **Monthly Optimization Complete**\n\n"
                                f"Recovered {opt_results['space_recovered_mb']:.1f} MB from "
                                f"{opt_results['databases_optimized']} databases."
                            )

                # Cleanup old archives
                await self.cleanup_old_archives(30)

        except Exception as e:
            self.logger.error(f"Error in maintenance loop: {e}")
            print(f"Error in maintenance loop: {e}")

    @maintenance_loop.before_loop
    async def before_maintenance_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(seconds=60)
    async def api_status_loop(self):
        """Probe both APIs in the background and stuff the result into the
        cache. Dashboard renders never wait on HTTP, eliminating the risk of
        blowing past Discord's 3 s interaction timeout."""
        try:
            wos_task = asyncio.create_task(self._probe_wos_api())
            gift_task = asyncio.create_task(self._probe_gift_distribution_api())
            wos_result, gift_result = await asyncio.gather(
                wos_task, gift_task, return_exceptions=True,
            )
            if not isinstance(wos_result, Exception):
                self._cached_wos_api = wos_result
            if not isinstance(gift_result, Exception):
                self._cached_gift_api = gift_result
            self._api_cache_at = datetime.now(timezone.utc)
        except Exception as e:
            self.logger.warning(f"API status refresh failed: {e}")

    @api_status_loop.before_loop
    async def before_api_status_loop(self):
        await self.bot.wait_until_ready()

    @tasks.loop(minutes=15)
    async def update_bot_footprint_loop(self):
        """Recompute the bot's directory size in a background thread. Cached
        result is read by the Health dashboard so renders stay instant even
        on installs with a fat venv."""
        try:
            mb = await asyncio.to_thread(self._compute_bot_footprint_mb)
            self._bot_footprint_mb = mb
        except Exception as e:
            self.logger.warning(f"Bot footprint walk failed: {e}")

    @update_bot_footprint_loop.before_loop
    async def before_update_bot_footprint_loop(self):
        await self.bot.wait_until_ready()

    @staticmethod
    def _compute_bot_footprint_mb() -> float:
        """Walk the bot directory and sum file sizes. Runs in a thread.
        Skips dev/VCS artifacts (hidden dirs, __pycache__) since they aren't
        part of the bot's runtime footprint and slow the walk significantly."""
        bot_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        total = 0
        for root, dirs, files in os.walk(bot_dir):
            dirs[:] = [d for d in dirs if not d.startswith('.') and d != '__pycache__']
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except (OSError, FileNotFoundError):
                    pass
        return total / (1024 * 1024)

    async def _notify_user(self, user_id: int, message: str):
        """Send DM to user"""
        try:
            user = await self.bot.fetch_user(user_id)
            if user:
                await user.send(message)
        except Exception as e:
            self.logger.error(f"Failed to notify user {user_id}: {e}")

    def cog_unload(self):
        """Cleanup when cog is unloaded"""
        self.maintenance_loop.cancel()
        if self.update_bot_footprint_loop.is_running():
            self.update_bot_footprint_loop.cancel()
        if self.api_status_loop.is_running():
            self.api_status_loop.cancel()
        self.logger.info("[HEALTH] Bot Health cog unloaded")

    def get_loaded_cogs(self) -> list:
        """Get list of loaded cog module names (without 'cogs.' prefix)"""
        cogs = []
        for ext in self.bot.extensions.keys():
            if ext.startswith("cogs."):
                cogs.append(ext[5:])  # Remove "cogs." prefix
        return sorted(cogs)

    async def reload_cogs(self, cog_names: list) -> dict:
        """
        Reload specified cogs.
        Returns dict with 'success', 'failed', and 'errors' lists.
        """
        results = {
            'success': [],
            'failed': [],
            'errors': {}
        }

        for cog_name in cog_names:
            ext_name = f"cogs.{cog_name}"

            # Skip bot_health - can't reload ourselves mid-operation
            if cog_name == "bot_health":
                results['failed'].append(cog_name)
                results['errors'][cog_name] = "Cannot reload bot_health while in use"
                continue

            try:
                # Check if extension is loaded
                if ext_name in self.bot.extensions:
                    await self.bot.reload_extension(ext_name)
                    results['success'].append(cog_name)
                    self.logger.info(f"Reloaded cog: {cog_name}")
                else:
                    # Try to load if not loaded
                    await self.bot.load_extension(ext_name)
                    results['success'].append(cog_name)
                    self.logger.info(f"Loaded cog: {cog_name}")

            except Exception as e:
                results['failed'].append(cog_name)
                results['errors'][cog_name] = str(e)[:100]
                self.logger.error(f"Failed to reload cog {cog_name}: {e}")
                print(f"Failed to reload cog {cog_name}: {e}")

        # Special handling: if pimp_my_bot was reloaded, refresh theme
        if "pimp_my_bot" in results['success']:
            try:
                from .pimp_my_bot import theme
                theme.load()
            except Exception as e:
                self.logger.warning(f"Could not refresh theme after reload: {e}")

        return results

    async def perform_restart(self, interaction: discord.Interaction,
                              allow_update: bool = False):
        """
        Perform a bot restart.
        - Container: Clean exit (orchestrator restarts).
        - Windows: Exit cleanly and print restart instructions. subprocess.Popen
          leaves the terminal in a bad state where the parent shell can race
          input with the spawned bot, so we don't auto-relaunch.
        - Linux/Mac: os.execl() for in-process replacement.

        When allow_update=True, the --no-update flag is filtered out of the
        relaunch args so the bot's startup updater can pick up a pending
        release.
        """
        self.logger.info(
            f"Bot restart initiated by user (allow_update={allow_update})"
        )
        is_windows_host = sys.platform == 'win32' and not is_container()
        if is_windows_host:
            title = f"{theme.refreshIcon} Stopping the bot..."
            description = (
                "The bot is stopping. On Windows the bot does not auto-restart "
                "— start it again on the host (`python main.py`) and this "
                "message will refresh once it's back online."
            )
        elif allow_update:
            title = f"{theme.refreshIcon} Restarting & Updating..."
            description = (
                "The bot is restarting and will install the pending update on "
                "startup. This may take a minute."
            )
        else:
            title = f"{theme.refreshIcon} Restarting..."
            description = "The bot is restarting. Please wait a moment."
        print(f"  {title.replace(chr(10), ' ')}")

        # Send confirmation message before restart
        try:
            embed = discord.Embed(
                title=title, description=description, color=theme.emColor1,
            )
            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                embed=embed,
                view=None
            )
        except Exception:
            pass

        # Persist where to update the message once the new bot is ready
        try:
            with sqlite3.connect(self.settings_db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "REPLACE INTO restart_marker (id, channel_id, message_id, initiated_at) "
                    "VALUES (1, ?, ?, ?)",
                    (interaction.channel_id, interaction.message.id,
                     datetime.now(timezone.utc).timestamp()),
                )
                conn.commit()
        except Exception as e:
            self.logger.warning(f"Could not persist restart marker: {e}")

        # Give Discord a moment to send the message
        await asyncio.sleep(1)

        # Filter --no-update when relaunching for an update
        relaunch_args = list(sys.argv)
        if allow_update:
            relaunch_args = [a for a in relaunch_args if a != "--no-update"]

        if is_container():
            self.logger.info("Exiting for container restart...")
            sys.exit(0)

        if is_windows_host:
            # Print clear restart instructions for the host operator and exit.
            venv_python = os.path.join("bot_venv", "Scripts", "python.exe")
            cmd_parts = [
                venv_python if os.path.exists(venv_python) else "python",
                *sys.argv,
            ]
            if allow_update and "--no-update" in cmd_parts:
                cmd_parts = [a for a in cmd_parts if a != "--no-update"]
            print()
            print("=" * 60)
            print("  Bot stopped. To restart, run:")
            print(f"    {' '.join(cmd_parts)}")
            print("=" * 60)
            print()
            sys.exit(0)

        self.logger.info("Self-restarting...")
        try:
            os.execl(sys.executable, sys.executable, *relaunch_args)
        except Exception as e:
            self.logger.error(f"Self-restart failed: {e}")
            print(f"Self-restart failed: {e}")
            sys.exit(1)

    @commands.Cog.listener()
    async def on_ready(self):
        """Update the restart confirmation embed once the new bot is ready."""
        if getattr(self, "_restart_marker_checked", False):
            return
        self._restart_marker_checked = True
        await self._consume_restart_marker()

    async def _consume_restart_marker(self):
        """Read and clear the restart marker, then update the original embed."""
        try:
            with sqlite3.connect(self.settings_db_path, timeout=30.0) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT channel_id, message_id, initiated_at FROM restart_marker WHERE id = 1"
                )
                row = cursor.fetchone()
                if not row:
                    return
                cursor.execute("DELETE FROM restart_marker WHERE id = 1")
                conn.commit()
        except Exception as e:
            self.logger.error(f"Error reading restart marker: {e}")
            return

        channel_id, message_id, initiated_at = row
        elapsed = datetime.now(timezone.utc).timestamp() - initiated_at

        # Stale marker — bot was offline long enough that the user has likely moved on
        if elapsed > 300:
            self.logger.info(f"Restart marker is stale ({int(elapsed)}s), discarding")
            return

        channel = self.bot.get_channel(channel_id)
        if channel is None:
            self.logger.warning(f"Channel {channel_id} not found for restart message update")
            return
        try:
            message = await channel.fetch_message(message_id)
        except discord.NotFound:
            self.logger.info("Restart message no longer exists, skipping update")
            return
        except discord.Forbidden:
            self.logger.warning(f"No permission to fetch restart message in channel {channel_id}")
            return
        except Exception as e:
            self.logger.error(f"Failed to fetch restart message: {e}")
            return

        embed = discord.Embed(
            title=f"{theme.verifiedIcon} Restart Complete",
            description=(
                f"The bot is back online. Took **{int(elapsed)}s**.\n\n"
                f"_Run `/settings` to continue managing the bot._"
            ),
            color=theme.emColor3,
        )
        try:
            await message.edit(embed=embed, view=None)
        except Exception as e:
            self.logger.error(f"Failed to update restart message: {e}")

    async def show_health_menu(self, interaction: discord.Interaction):
        """Show the main health dashboard"""
        try:
            is_admin, is_global = PermissionManager.is_admin(interaction.user.id)

            if not is_admin or not is_global:
                await interaction.response.send_message(
                    f"{theme.deniedIcon} Only Global Admins can access Bot Health.",
                    ephemeral=True
                )
                return

            # Show loading state
            await interaction.response.defer()

            # Gather health data (API checks are async)
            wos_api = await self.check_wos_api_status()
            gift_api = await self.check_gift_distribution_api()
            db_health = self.get_database_health()
            log_health = self.get_log_health()
            system_health = self.get_system_health()
            requirements = self.get_requirements_health()

            overall = self.get_overall_status(db_health, log_health, system_health, wos_api, gift_api, requirements)

            embed = self._build_health_embed(overall, wos_api, gift_api, db_health, log_health, system_health, requirements)
            view = HealthMenuView(self)

            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                embed=embed,
                view=view
            )

        except Exception as e:
            self.logger.error(f"Error showing health menu: {e}")
            print(f"Error showing health menu: {e}")
            try:
                await interaction.followup.send(
                    f"{theme.deniedIcon} An error occurred loading health status.",
                    ephemeral=True
                )
            except Exception:
                pass

    def _build_health_embed(self, overall: str, wos_api: dict, gift_api: dict,
                            db_health: dict, log_health: dict, system_health: dict,
                            requirements: dict = None) -> discord.Embed:
        """Build the health dashboard embed."""
        from . import onnx_lifecycle

        overall_text = (
            "Healthy" if overall == STATUS_HEALTHY
            else "Attention Needed" if overall == STATUS_WARNING
            else "Issues Detected"
        )

        # When healthy, drop the redundant green check; only show warn/error
        # icons so degraded items pop. Topic icons (clock, gear, etc) always show.
        def status_prefix(status: str) -> str:
            if status == STATUS_HEALTHY:
                return ""
            return f"{self._status_icon(status)} "

        deps_prefix = ""
        deps_text = None
        if requirements:
            if requirements.get('error'):
                deps_prefix = status_prefix(STATUS_ERROR)
                deps_text = requirements['error']
            else:
                issues = len(requirements['missing']) + len(requirements['outdated'])
                if issues == 0:
                    deps_text = f"{requirements['total']}/{requirements['total']} OK"
                else:
                    parts = []
                    if requirements['missing']:
                        parts.append(f"{len(requirements['missing'])} missing")
                    if requirements['outdated']:
                        parts.append(f"{len(requirements['outdated'])} outdated")
                    deps_prefix = status_prefix(requirements['status'])
                    deps_text = (
                        f"{requirements['ok_count']}/{requirements['total']} OK "
                        f"({', '.join(parts)})"
                    )

        ocr_status, ocr_summary = self._build_ocr_summary(
            onnx_lifecycle.get_status_lines()
        )
        disk_health = self.get_disk_health()

        def fmt_short(mb):
            return f"{mb / 1024:.0f} GB" if mb >= 1024 else f"{mb:.0f} MB"

        disk_msg = (
            f"{fmt_short(disk_health['free_mb'])} / {fmt_short(disk_health['total_mb'])} "
            f"· {disk_health['used_pct']:.0f}% used"
        )

        uptime = system_health['uptime']
        if uptime.startswith('0h '):
            uptime = uptime[3:]

        embed = discord.Embed(
            title=f"{theme.heartIcon} Bot Health",
            description="Status of services, system resources, and storage.",
            color=self._status_color(overall),
        )

        embed.add_field(
            name="Health",
            value=(
                f"{status_prefix(overall)}**Overall:** {overall_text}\n"
                f"{theme.ticketIcon} {status_prefix(wos_api['status'])}**Redemption API:** {wos_api['message']}\n"
                f"{theme.giftIcon} {status_prefix(gift_api['status'])}**Distribution API:** {gift_api['message']}\n"
                f"{theme.globeIcon} {status_prefix(ocr_status)}**OCR Engines:** {ocr_summary}"
            ),
            inline=True,
        )

        embed.add_field(
            name="System",
            value=(
                f"{theme.timeIcon} **Uptime:** {uptime}\n"
                f"{theme.boltIcon} {status_prefix(system_health['latency_status'])}**Latency:** {system_health['latency_ms']}ms\n"
                f"{theme.settingsIcon} **Cogs:** {system_health['loaded_cogs']}\n"
                f"{theme.infoIcon} **Python:** {system_health['python_version']} on {system_health['platform']}"
            ),
            inline=True,
        )

        if self._bot_footprint_mb is None:
            footprint_str = "—"
        else:
            footprint_str = fmt_short(self._bot_footprint_mb)

        storage_lines = [
            f"{theme.saveIcon} {status_prefix(disk_health['status'])}**Disk:** {disk_msg}",
            f"{theme.robotIcon} **Bot Footprint:** {footprint_str}",
            f"{theme.archiveIcon} {status_prefix(db_health['status'])}**Databases:** {db_health['message']}",
            f"{theme.documentIcon} {status_prefix(log_health['status'])}**Logs:** {log_health['message']}",
        ]
        if deps_text:
            storage_lines.append(
                f"{theme.packageIcon} {deps_prefix}**Dependencies:** {deps_text}"
            )
        embed.add_field(name="Storage", value="\n".join(storage_lines), inline=True)

        embed.add_field(
            name="Actions",
            value=(
                f"{theme.upperDivider}\n"
                f"{theme.cleanIcon} **Run Cleanup**\n"
                f"└ Reclaim DB space, archive old logs, sweep stale legacy files\n"
                f"{theme.refreshIcon} **Reload Cogs**\n"
                f"└ Reload code from disk without restarting the whole bot\n"
                f"{theme.refreshIcon} **Restart Bot**\n"
                f"└ Full restart — stops the bot and relaunches it\n"
                f"{theme.settingsIcon} **Settings**\n"
                f"└ Configure cleanup schedule and health thresholds\n"
                f"{theme.lowerDivider}"
            ),
            inline=False,
        )

        return embed

    def _build_ocr_summary(self, status_lines: list[dict]) -> tuple[str, str]:
        """Compact OCR engines summary for the Health section.

        Returns (status, message) — status uses STATUS_* constants and is fed
        into status_prefix so degraded states get a warn/error icon. Pinned
        engines (e.g. captcha) and unloaded-but-ready engines all count as
        'configured'; engines currently held by an active session count as
        'in use'."""
        if not status_lines:
            return STATUS_HEALTHY, "Not configured"
        total = len(status_lines)
        in_use = sum(1 for s in status_lines if s.get('refcount', 0) > 0)
        if in_use:
            msg = f"{total} configured · {in_use} in use"
        else:
            msg = f"{total} configured"
        return STATUS_HEALTHY, msg

    @staticmethod
    def _format_age(delta: timedelta) -> str:
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return "just now"
        if seconds < 3600:
            mins = seconds // 60
            return f"{mins} min ago"
        if seconds < 86400:
            hours = seconds // 3600
            return f"{hours}h ago"
        days = seconds // 86400
        return f"{days}d ago"


class HealthMenuView(discord.ui.View):
    """Main Bot Health menu — flat layout with inline restart confirmation."""

    def __init__(self, cog: BotHealth):
        super().__init__(timeout=7200)
        self.cog = cog
        self._confirming_restart = False
        self._build_components()

    def _build_components(self):
        self.clear_items()
        if self._confirming_restart:
            confirm_btn = discord.ui.Button(
                label="Confirm Restart",
                emoji=theme.warnIcon,
                style=discord.ButtonStyle.danger,
                row=0,
            )
            confirm_btn.callback = self._on_confirm_restart
            self.add_item(confirm_btn)

            cancel_btn = discord.ui.Button(
                label="Cancel",
                emoji=theme.deniedIcon,
                style=discord.ButtonStyle.secondary,
                row=0,
            )
            cancel_btn.callback = self._on_cancel_restart
            self.add_item(cancel_btn)
            return

        cleanup_btn = discord.ui.Button(
            label="Run Cleanup",
            emoji=theme.cleanIcon,
            style=discord.ButtonStyle.success,
            row=0,
        )
        cleanup_btn.callback = self._on_cleanup
        self.add_item(cleanup_btn)

        reload_btn = discord.ui.Button(
            label="Reload Cogs",
            emoji=theme.refreshIcon,
            style=discord.ButtonStyle.success,
            row=0,
        )
        reload_btn.callback = self._on_reload_cogs
        self.add_item(reload_btn)

        restart_btn = discord.ui.Button(
            label="Restart Bot",
            emoji=theme.refreshIcon,
            style=discord.ButtonStyle.danger,
            row=0,
        )
        restart_btn.callback = self._on_restart_request
        self.add_item(restart_btn)

        settings_btn = discord.ui.Button(
            label="Settings",
            emoji=theme.settingsIcon,
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        settings_btn.callback = self._on_settings
        self.add_item(settings_btn)

        back_btn = discord.ui.Button(
            label="Back",
            emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary,
            row=1,
        )
        back_btn.callback = self._on_back
        self.add_item(back_btn)

    def _build_restart_confirm_embed(self) -> discord.Embed:
        is_windows_host = sys.platform == 'win32' and not is_container()
        if is_windows_host:
            tail = (
                f"\n{theme.warnIcon} **Windows host detected.** The bot will "
                f"stop here — it does **not** auto-restart. Someone with "
                f"access to the host needs to start it again with "
                f"`python main.py`."
            )
        else:
            tail = "The bot will reconnect automatically."
        return discord.Embed(
            title=f"{theme.warnIcon} Restart Bot",
            description=(
                f"Are you sure you want to restart the bot?\n\n"
                f"**What will happen:**\n"
                f"• Active sessions and menus will stop working\n"
                f"• Running tasks will be cancelled\n"
                f"• Bot will be offline briefly during restart\n"
                f"• All data is saved — nothing will be lost\n\n"
                f"{tail}"
            ),
            color=0xFF0000,
        )

    async def _on_cleanup(self, interaction: discord.Interaction):
        await interaction.response.defer()

        progress_embed = discord.Embed(
            title=f"{theme.refreshIcon} Running Cleanup…",
            description=(
                "Optimizing databases, archiving old logs, and "
                "archiving any unused cog files. This may take a moment."
            ),
            color=theme.emColor1,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id, embed=progress_embed, view=None
        )

        results = await self.cog.run_cleanup()

        result_parts = []
        if results['db_cleaned_mb'] > 0:
            result_parts.append(f"Recovered {results['db_cleaned_mb']:.1f} MB from databases")
        else:
            result_parts.append("Databases already optimized")

        if results['logs_archived'] > 0:
            result_parts.append(f"Archived {results['logs_archived']} old log files")
        else:
            result_parts.append("No old logs to archive")

        legacy = results.get('legacy_artifacts_removed') or []
        if legacy:
            result_parts.append(f"Removed {len(legacy)} legacy item(s): {', '.join(legacy[:5])}")

        pkgs = results.get('legacy_packages_removed') or []
        if pkgs:
            result_parts.append(f"Uninstalled legacy package(s): {', '.join(pkgs)}")

        if results['errors']:
            result_parts.append(f"\n{theme.warnIcon} Some issues: {', '.join(results['errors'][:2])}")

        unused_cogs = self.cog.get_unused_cog_files()

        if unused_cogs:
            file_list = "\n".join(f"• `{f}`" for f in unused_cogs[:15])
            if len(unused_cogs) > 15:
                file_list += f"\n*…and {len(unused_cogs) - 15} more*"
            embed = discord.Embed(
                title=f"{theme.warnIcon} Review Unused Cog Files",
                description=(
                    f"**Database & log cleanup**\n"
                    f"{theme.upperDivider}\n"
                    + "\n".join(f"• {p}" for p in result_parts) + "\n"
                    f"{theme.lowerDivider}\n\n"
                    f"**{len(unused_cogs)}** file(s) in `cogs/` look unused — they're "
                    f"not loaded as cogs and aren't imported anywhere we could detect.\n\n"
                    f"{file_list}\n\n"
                    f"{theme.warnIcon} Files would be moved to `cogs/old_cogs_archive/` "
                    f"(recoverable, not deleted). Review carefully before confirming — "
                    f"any false positives can be added to **Manage Exceptions** first."
                ),
                color=theme.emColor2,
            )
            await interaction.followup.edit_message(
                message_id=interaction.message.id, embed=embed,
                view=CleanCogsConfirmView(self.cog, unused_cogs, self),
            )
            return

        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()
        requirements = self.cog.get_requirements_health()
        overall = self.cog.get_overall_status(
            db_health, log_health, system_health, wos_api, gift_api, requirements
        )
        embed = self.cog._build_health_embed(
            overall, wos_api, gift_api, db_health, log_health, system_health, requirements
        )
        embed.add_field(
            name=f"{theme.verifiedIcon} Cleanup Complete",
            value="\n".join(result_parts) + "\nNo unused cog files detected.",
            inline=False,
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id, embed=embed, view=self
        )

    async def _on_settings(self, interaction: discord.Interaction):
        config = self.cog.get_config()
        await interaction.response.send_modal(HealthSettingsModal(self.cog, config))

    async def _on_reload_cogs(self, interaction: discord.Interaction):
        loaded = self.cog.get_loaded_cogs()
        embed = discord.Embed(
            title=f"{theme.refreshIcon} Reload Cogs",
            description=(
                f"Select cogs to reload from the dropdown below.\n\n"
                f"**{len(loaded)}** cogs currently loaded.\n\n"
                f"{theme.warnIcon} *Reloading a cog will reset any in-memory state "
                f"(cached data, running tasks). Database data is preserved.*"
            ),
            color=theme.emColor1,
        )
        await interaction.response.edit_message(
            embed=embed,
            view=ReloadCogsView(self.cog, loaded, self),
        )

    async def _on_restart_request(self, interaction: discord.Interaction):
        self._confirming_restart = True
        self._build_components()
        await interaction.response.edit_message(
            embed=self._build_restart_confirm_embed(), view=self
        )

    async def _on_confirm_restart(self, interaction: discord.Interaction):
        await self.cog.perform_restart(interaction)

    async def _on_cancel_restart(self, interaction: discord.Interaction):
        self._confirming_restart = False
        self._build_components()
        # Re-fetch and re-render the dashboard
        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()
        requirements = self.cog.get_requirements_health()
        overall = self.cog.get_overall_status(
            db_health, log_health, system_health, wos_api, gift_api, requirements
        )
        embed = self.cog._build_health_embed(
            overall, wos_api, gift_api, db_health, log_health, system_health, requirements
        )
        await interaction.response.edit_message(embed=embed, view=self)

    async def _on_back(self, interaction: discord.Interaction):
        try:
            main_menu_cog = self.cog.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_maintenance(interaction)
        except Exception as e:
            self.cog.logger.error(f"Error returning to maintenance menu: {e}")
            print(f"Error returning to maintenance menu: {e}")


class CleanCogsConfirmView(discord.ui.View):
    """Confirmation view for cleaning unused cog files"""
    def __init__(self, cog: BotHealth, unused_files: list, parent_view: HealthMenuView):
        super().__init__(timeout=60)
        self.cog = cog
        self.unused_files = unused_files
        self.parent_view = parent_view

    @discord.ui.button(
        label="Archive Files",
        emoji=theme.verifiedIcon,
        style=discord.ButtonStyle.danger,
        custom_id="confirm_clean_cogs"
    )
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Show progress
        progress_embed = discord.Embed(
            title=f"{theme.refreshIcon} Archiving Files...",
            description="Moving unused files to archive folder.",
            color=theme.emColor1
        )
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=progress_embed, view=None)

        # Archive the files
        results = await self.cog.archive_unused_cogs(self.unused_files)

        # Build result embed
        if results['archived'] > 0:
            description = (
                f"Successfully archived **{results['archived']}** file(s) to `cogs/old_cogs_archive/`\n\n"
                f"**Archived files:**\n"
                + "\n".join([f"• `{f}`" for f in results['archived_files'][:10]])
                + (f"\n*...and {len(results['archived_files']) - 10} more*" if len(results['archived_files']) > 10 else "")
            )
            if results['errors']:
                description += f"\n\n{theme.warnIcon} **Some errors:**\n" + "\n".join(results['errors'][:3])

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Cleanup Complete",
                description=description,
                color=theme.emColor3
            )
        else:
            embed = discord.Embed(
                title=f"{theme.warnIcon} No Files Archived",
                description="Could not archive any files." + (f"\n\nErrors: {', '.join(results['errors'][:3])}" if results['errors'] else ""),
                color=theme.emColor2
            )

        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.parent_view)

    @discord.ui.button(
        label="Manage Exceptions",
        emoji=theme.settingsIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="manage_exceptions"
    )
    async def exceptions_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Open modal to manage custom helper file exceptions"""
        custom_files = self.cog.get_custom_helper_files()
        modal = CustomHelperFilesModal(self.cog, custom_files, self.unused_files, self.parent_view)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Cancel",
        emoji=theme.backIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="cancel_clean_cogs"
    )
    async def cancel_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Return to health dashboard
        await interaction.response.defer()

        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()

        overall = self.cog.get_overall_status(db_health, log_health, system_health, wos_api, gift_api)
        embed = self.cog._build_health_embed(overall, wos_api, gift_api, db_health, log_health, system_health)

        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self.parent_view)


class ReloadCogsView(discord.ui.View):
    """Paginated cog selector: selections persist across pages."""

    PAGE_SIZE = 25

    def __init__(self, cog: BotHealth, loaded_cogs: list, parent_view: HealthMenuView):
        super().__init__(timeout=7200)
        self.cog = cog
        self.loaded_cogs = loaded_cogs
        self.parent_view = parent_view
        self.selected_cogs: set[str] = set()
        self.page = 0
        self.max_page = max(0, (len(loaded_cogs) - 1) // self.PAGE_SIZE)
        self._build_components()

    def _page_slice(self) -> list:
        start = self.page * self.PAGE_SIZE
        return self.loaded_cogs[start:start + self.PAGE_SIZE]

    def _build_components(self):
        self.clear_items()
        page_cogs = self._page_slice()

        options = []
        for cog_name in page_cogs:
            is_selected = cog_name in self.selected_cogs
            if cog_name == "bot_health":
                options.append(discord.SelectOption(
                    label=cog_name, value=cog_name,
                    description="Cannot reload (in use)",
                    emoji=theme.deniedIcon, default=is_selected,
                ))
            else:
                options.append(discord.SelectOption(
                    label=cog_name, value=cog_name,
                    emoji=theme.settingsIcon, default=is_selected,
                ))

        if options:
            placeholder = (
                f"Select cogs (page {self.page + 1}/{self.max_page + 1})…"
                if self.max_page > 0 else "Select cogs to reload…"
            )
            if self.selected_cogs:
                placeholder = f"{len(self.selected_cogs)} selected · {placeholder}"
            select = discord.ui.Select(
                placeholder=placeholder,
                min_values=0,
                max_values=len(options),
                options=options,
                row=0,
            )
            select.callback = self.on_cog_select
            self.add_item(select)

        if self.max_page > 0:
            prev_btn = discord.ui.Button(
                label="", emoji=theme.prevIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=self.page == 0,
            )
            prev_btn.callback = self._prev_page
            self.add_item(prev_btn)

            next_btn = discord.ui.Button(
                label="", emoji=theme.nextIcon,
                style=discord.ButtonStyle.secondary, row=1,
                disabled=self.page >= self.max_page,
            )
            next_btn.callback = self._next_page
            self.add_item(next_btn)

        reload_selected = discord.ui.Button(
            label="Reload Selected", emoji=theme.refreshIcon,
            style=discord.ButtonStyle.primary, row=2,
        )
        reload_selected.callback = self._on_reload_selected
        self.add_item(reload_selected)

        reload_all = discord.ui.Button(
            label="Reload All", emoji=theme.refreshIcon,
            style=discord.ButtonStyle.danger, row=2,
        )
        reload_all.callback = self._on_reload_all
        self.add_item(reload_all)

        cancel = discord.ui.Button(
            label="Cancel", emoji=theme.backIcon,
            style=discord.ButtonStyle.secondary, row=2,
        )
        cancel.callback = self._on_cancel
        self.add_item(cancel)

    async def on_cog_select(self, interaction: discord.Interaction):
        page_set = set(self._page_slice())
        chosen = set(interaction.data.get('values', []))
        self.selected_cogs = (self.selected_cogs - page_set) | chosen
        self._build_components()
        await interaction.response.edit_message(view=self)

    async def _prev_page(self, interaction: discord.Interaction):
        self.page = max(0, self.page - 1)
        self._build_components()
        await interaction.response.edit_message(view=self)

    async def _next_page(self, interaction: discord.Interaction):
        self.page = min(self.max_page, self.page + 1)
        self._build_components()
        await interaction.response.edit_message(view=self)

    async def _on_reload_selected(self, interaction: discord.Interaction):
        if not self.selected_cogs:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select at least one cog to reload.",
                ephemeral=True,
            )
            return
        await self._perform_reload(interaction, sorted(self.selected_cogs))

    async def _on_reload_all(self, interaction: discord.Interaction):
        await self._perform_reload(interaction, self.loaded_cogs)

    async def _on_cancel(self, interaction: discord.Interaction):
        await interaction.response.defer()

        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()

        overall = self.cog.get_overall_status(db_health, log_health, system_health, wos_api, gift_api)
        embed = self.cog._build_health_embed(overall, wos_api, gift_api, db_health, log_health, system_health)

        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed, view=self.parent_view,
        )

    async def _perform_reload(self, interaction: discord.Interaction, cogs_to_reload: list):
        """Perform the actual reload operation"""
        await interaction.response.defer()

        # Show progress
        progress_embed = discord.Embed(
            title=f"{theme.refreshIcon} Reloading Cogs...",
            description=f"Reloading {len(cogs_to_reload)} cog(s)...",
            color=theme.emColor1
        )
        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=progress_embed,
            view=None
        )

        # Perform reload
        results = await self.cog.reload_cogs(cogs_to_reload)

        # Build results embed
        if results['success'] and not results['failed']:
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Reload Complete",
                description=f"Successfully reloaded **{len(results['success'])}** cog(s).",
                color=theme.emColor3
            )
            if len(results['success']) <= 10:
                embed.add_field(
                    name="Reloaded",
                    value="\n".join([f"• {c}" for c in results['success']]),
                    inline=False
                )
        elif results['failed'] and not results['success']:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Reload Failed",
                description=f"Failed to reload **{len(results['failed'])}** cog(s).",
                color=0xFF0000
            )
            error_text = "\n".join([f"• **{c}**: {results['errors'].get(c, 'Unknown error')}" for c in results['failed'][:5]])
            embed.add_field(name="Errors", value=error_text, inline=False)
        else:
            embed = discord.Embed(
                title=f"{theme.warnIcon} Partial Reload",
                description=f"Reloaded **{len(results['success'])}** cog(s), **{len(results['failed'])}** failed.",
                color=theme.emColor2
            )
            if results['success']:
                embed.add_field(
                    name="Succeeded",
                    value="\n".join([f"• {c}" for c in results['success'][:5]]),
                    inline=True
                )
            if results['failed']:
                error_text = "\n".join([f"• **{c}**: {results['errors'].get(c, '?')[:30]}" for c in results['failed'][:5]])
                embed.add_field(name="Failed", value=error_text, inline=True)

        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=self.parent_view
        )



class CustomHelperFilesModal(discord.ui.Modal, title="Manage Exceptions"):
    """Modal for managing custom helper files that should be excluded from cleanup"""
    def __init__(self, cog: BotHealth, current_files: list, unused_files: list, parent_view: HealthMenuView):
        super().__init__()
        self.cog = cog
        self.unused_files = unused_files
        self.parent_view = parent_view

        # Show current custom files
        current_value = '\n'.join(current_files) if current_files else ''

        self.helper_files = discord.ui.TextInput(
            label="Custom Exception Files (one per line)",
            style=discord.TextStyle.paragraph,
            placeholder="my_custom_helper\nmy_other_cog\n(without .py extension)",
            default=current_value,
            max_length=1000,
            required=False
        )
        self.add_item(self.helper_files)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse the input - split by newlines, clean up
            raw_input = self.helper_files.value.strip()
            if raw_input:
                files = [f.strip() for f in raw_input.split('\n') if f.strip()]
            else:
                files = []

            # Save to database
            self.cog.set_custom_helper_files(files)

            # Show confirmation and refresh the unused files list
            unused_cogs = self.cog.get_unused_cog_files()

            if not unused_cogs:
                embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Exceptions Updated",
                    description=(
                        f"Saved **{len(files)}** custom exception(s).\n\n"
                        f"No unused files remain after applying exceptions."
                    ),
                    color=theme.emColor3
                )
                await interaction.response.edit_message(embed=embed, view=self.parent_view)
            else:
                embed = discord.Embed(
                    title=f"{theme.trashIcon} Exceptions Updated",
                    description=(
                        f"Saved **{len(files)}** custom exception(s).\n\n"
                        f"**{len(unused_cogs)}** file(s) still flagged as unused:\n"
                        + "\n".join([f"• `{f}`" for f in unused_cogs[:15]])
                        + (f"\n*...and {len(unused_cogs) - 15} more*" if len(unused_cogs) > 15 else "")
                    ),
                    color=theme.emColor2
                )
                embed.set_footer(text="Files will be moved to cogs/old_cogs_archive/ (not deleted)")

                view = CleanCogsConfirmView(self.cog, unused_cogs, self.parent_view)
                await interaction.response.edit_message(embed=embed, view=view)

        except Exception as e:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Error",
                description=f"Failed to update exceptions: {e}",
                color=theme.emColor2
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)


class HealthSettingsModal(discord.ui.Modal, title="Health Settings"):
    """Simplified settings modal"""
    def __init__(self, cog: BotHealth, config: dict):
        super().__init__()
        self.cog = cog

        self.cleanup_time = discord.ui.TextInput(
            label="Daily Cleanup Time (HH:MM UTC)",
            placeholder="03:00",
            default=f"{config['cleanup_hour']:02d}:{config['cleanup_minute']:02d}",
            max_length=5,
            required=True
        )
        self.add_item(self.cleanup_time)

        self.monthly_day = discord.ui.TextInput(
            label="Monthly Deep Cleanup Day (1-28, 0=off)",
            placeholder="0",
            default=str(config.get('monthly_optimization_day', 0)),
            max_length=2,
            required=True
        )
        self.add_item(self.monthly_day)

        self.notify_id = discord.ui.TextInput(
            label="Notify User ID (for issue alerts)",
            placeholder="Leave empty to disable notifications",
            default=str(config['notify_user_id']) if config.get('notify_user_id') else "",
            max_length=20,
            required=False
        )
        self.add_item(self.notify_id)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse cleanup time
            time_parts = self.cleanup_time.value.strip().split(':')
            if len(time_parts) != 2:
                raise ValueError("Invalid time format. Use HH:MM")
            hour = int(time_parts[0])
            minute = int(time_parts[1])
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError("Invalid time. Hour must be 0-23, minute 0-59")

            # Parse monthly day
            monthly_day = int(self.monthly_day.value.strip())
            if not (0 <= monthly_day <= 28):
                raise ValueError("Monthly day must be 0-28 (0 to disable)")

            # Parse notify ID
            notify_id = None
            if self.notify_id.value.strip():
                notify_id = int(self.notify_id.value.strip())

            # Update config
            self.cog.update_config(
                cleanup_hour=hour,
                cleanup_minute=minute,
                monthly_optimization_day=monthly_day,
                notify_user_id=notify_id
            )

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Settings Updated",
                description=(
                    f"**Daily Cleanup:** {hour:02d}:{minute:02d} UTC\n"
                    f"**Monthly Deep Cleanup:** {'Day ' + str(monthly_day) if monthly_day else 'Disabled'}\n"
                    f"**Notifications:** {f'<@{notify_id}>' if notify_id else 'Disabled'}"
                ),
                color=theme.emColor3
            )

            view = HealthMenuView(self.cog)
            await interaction.response.edit_message(embed=embed, view=view)

        except ValueError as e:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Invalid Input",
                description=str(e),
                color=theme.emColor2
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        except Exception as e:
            embed = discord.Embed(
                title=f"{theme.deniedIcon} Error",
                description=f"Failed to update settings: {e}",
                color=theme.emColor2
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot):
    await bot.add_cog(BotHealth(bot))