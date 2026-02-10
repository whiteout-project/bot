"""
Bot Health Dashboard
Shows overall bot health status, manages automated DB maintenance, and provides manual file cleanup tools.
"""
import discord
from discord.ext import commands, tasks
import sqlite3
import os
import sys
import platform
import asyncio
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
LATENCY_WARNING_MS = 100
LATENCY_ERROR_MS = 500

# Active log file names (files that should not be archived)
ACTIVE_LOG_NAMES = [
    'alliance_control.txt', 'alliance_memberlog.txt', 'alliance_sync.txt',
    'backuplog.txt', 'bear_trap.txt', 'db_maintenance.txt', 'gift_ops.txt',
    'gift_solver.txt', 'giftlog.txt', 'id_channel_log.txt', 'login_handler.txt',
    'notifications.txt', 'verification.txt', 'add_memberlog.txt'
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
    '__init__',                # Package init file
]


class BotHealth(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.db_path = "db"
        self.log_path = "log"
        self.archive_path = "log/archive"
        self.settings_db_path = "db/settings.sqlite"
        self.gift_api_url = "http://gift-code-api.whiteout-bot.com/giftcode_api.php"

        self.logger = logging.getLogger('bot')
        self.start_time = datetime.now(timezone.utc)

        self._setup_database()
        self.maintenance_loop.start()
        self.logger.info("[HEALTH] Bot Health cog initialized")

    def _setup_database(self):
        """Create/update health_config table"""
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

    async def check_wos_api_status(self) -> dict:
        """Check WOS Player API status via login handler"""
        try:
            login_handler_cog = self.bot.get_cog("LoginHandler")
            if login_handler_cog:
                handler = login_handler_cog.handler
            else:
                from .login_handler import LoginHandler
                handler = LoginHandler()

            status = await handler.check_apis_availability()

            if status['api1_available'] and status['api2_available']:
                return {'status': STATUS_HEALTHY, 'message': 'Dual-API mode (fast)'}
            elif status['api1_available'] or status['api2_available']:
                api_down = '2' if status['api1_available'] else '1'
                return {'status': STATUS_WARNING, 'message': f'Single-API mode (API {api_down} down)'}
            else:
                return {'status': STATUS_ERROR, 'message': 'Both APIs unavailable'}

        except Exception as e:
            self.logger.error(f"Error checking WOS API status: {e}")
            return {'status': STATUS_ERROR, 'message': f'Check failed: {str(e)[:30]}'}

    async def check_gift_distribution_api(self) -> dict:
        """Check Gift Code Distribution API status"""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                start = datetime.now()
                async with session.get(self.gift_api_url) as response:
                    elapsed = (datetime.now() - start).total_seconds()

                    if response.status == 200:
                        if elapsed > 3:
                            return {'status': STATUS_WARNING, 'message': f'Online (slow: {elapsed:.1f}s)'}
                        return {'status': STATUS_HEALTHY, 'message': 'Online'}
                    else:
                        return {'status': STATUS_ERROR, 'message': f'Error (HTTP {response.status})'}
        except asyncio.TimeoutError:
            return {'status': STATUS_ERROR, 'message': 'Timeout (>5s)'}
        except aiohttp.ClientError as e:
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

    def get_unused_cog_files(self) -> list:
        """
        Detect unused cog files in the cogs folder.
        Compares files in cogs/ against loaded cogs and known helper files.
        Returns list of filenames that appear to be unused.
        """
        cogs_path = "cogs"
        if not os.path.exists(cogs_path):
            return []

        unused_files = []

        # Get module names from loaded extensions
        loaded_module_names = set()
        for ext in self.bot.extensions.keys():
            if ext.startswith("cogs."):
                loaded_module_names.add(ext[5:])  # Remove "cogs." prefix

        # Combine built-in helper files with custom ones
        all_helper_files = set(HELPER_FILES)
        all_helper_files.update(self.get_custom_helper_files())

        for filename in os.listdir(cogs_path):
            if not filename.endswith('.py'):
                continue

            # Skip __pycache__ and similar
            if filename.startswith('__') and filename != '__init__.py':
                continue

            module_name = filename[:-3]  # Remove .py

            # Skip known helper files (built-in and custom)
            if module_name in all_helper_files:
                continue

            # Check if this module is loaded as an extension
            if module_name in loaded_module_names:
                continue

            # This file exists but isn't loaded - it's potentially unused
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
        statuses = [db_health['status'], log_health['status'], system_health['latency_status']]

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
        """Run cleanup operations (WAL checkpoint + log archival)"""
        results = {
            'db_cleaned_mb': 0,
            'logs_archived': 0,
            'errors': []
        }

        # WAL checkpoint
        try:
            checkpoint_results = await self._checkpoint_all_databases()
            for r in checkpoint_results:
                if r.get('success') and not r.get('skipped'):
                    results['db_cleaned_mb'] += (r['wal_size_before'] - r['wal_size_after']) / (1024 * 1024)
        except Exception as e:
            results['errors'].append(f"DB cleanup: {e}")
            self.logger.error(f"Cleanup DB error: {e}")

        # Archive orphaned logs
        try:
            archived = await self._archive_orphaned_logs()
            results['logs_archived'] = archived
        except Exception as e:
            results['errors'].append(f"Log archival: {e}")
            self.logger.error(f"Cleanup log error: {e}")

        # Update last cleanup date
        self.update_config(last_cleanup_date=datetime.now(timezone.utc).date().isoformat())

        return results

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

    async def perform_restart(self, interaction: discord.Interaction):
        """
        Perform a bot restart.
        - Docker/Windows: Clean exit (process manager restarts)
        - Linux/Mac: Self-restart via os.execl()
        """
        self.logger.info("Bot restart initiated by user")
        print("Bot restart initiated...")

        # Send confirmation message before restart
        try:
            embed = discord.Embed(
                title=f"{theme.refreshIcon} Restarting...",
                description="The bot is restarting. Please wait a moment.",
                color=theme.emColor1
            )
            await interaction.followup.edit_message(
                message_id=interaction.message.id,
                embed=embed,
                view=None
            )
        except Exception:
            pass

        # Give Discord a moment to send the message
        await asyncio.sleep(1)

        # Determine restart method
        in_container = is_container()
        is_windows = sys.platform == 'win32'

        if in_container or is_windows:
            # Clean exit - let container/process manager handle restart
            self.logger.info("Exiting for external restart...")
            sys.exit(0)
        else:
            # Linux/Mac: Self-restart
            self.logger.info("Self-restarting via os.execl...")
            try:
                os.execl(sys.executable, sys.executable, *sys.argv)
            except Exception as e:
                self.logger.error(f"Self-restart failed: {e}")
                print(f"Self-restart failed: {e}")
                sys.exit(1)

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
        """Build the health dashboard embed"""

        overall_text = "Healthy" if overall == STATUS_HEALTHY else "Attention Needed" if overall == STATUS_WARNING else "Issues Detected"

        embed = discord.Embed(
            title=f"{theme.heartIcon} Bot Health",
            description=f"**Overall:** {self._status_icon(overall)} {overall_text}",
            color=self._status_color(overall)
        )

        # Services section
        services_text = (
            f"{theme.upperDivider}\n"
            f"{self._status_icon(wos_api['status'])} **WOS Player API:** {wos_api['message']}\n"
            f"{self._status_icon(gift_api['status'])} **Gift Distribution API:** {gift_api['message']}\n"
            f"{theme.lowerDivider}"
        )
        embed.add_field(name="Services", value=services_text, inline=False)

        # System section
        system_text = (
            f"{theme.timeIcon} **Uptime:** {system_health['uptime']}\n"
            f"{self._status_icon(system_health['latency_status'])} **Latency:** {system_health['latency_ms']}ms\n"
            f"{theme.settingsIcon} **Cogs:** {system_health['loaded_cogs']} loaded\n"
            f"{theme.infoIcon} **Python:** {system_health['python_version']} on {system_health['platform']}"
        )
        embed.add_field(name="System", value=system_text, inline=False)

        # Storage section
        cleanup_info = f" (cleaned {db_health['last_cleanup']})" if db_health['last_cleanup'] and db_health['last_cleanup'] != 'Never' else ""
        storage_text = (
            f"{self._status_icon(db_health['status'])} **Databases:** {db_health['message']}{cleanup_info}\n"
            f"{self._status_icon(log_health['status'])} **Logs:** {log_health['message']}"
        )
        embed.add_field(name="Storage", value=storage_text, inline=False)

        # Requirements section
        if requirements:
            if requirements.get('error'):
                req_text = f"{self._status_icon(STATUS_ERROR)} {requirements['error']}"
            else:
                issues = len(requirements['missing']) + len(requirements['outdated'])
                if issues == 0:
                    req_text = f"{self._status_icon(STATUS_HEALTHY)} All {requirements['total']} packages OK"
                else:
                    parts = []
                    if requirements['missing']:
                        parts.append(f"{len(requirements['missing'])} missing")
                    if requirements['outdated']:
                        parts.append(f"{len(requirements['outdated'])} outdated")
                    req_text = f"{self._status_icon(requirements['status'])} {requirements['ok_count']}/{requirements['total']} OK ({', '.join(parts)})"
            embed.add_field(name="Dependencies", value=req_text, inline=False)

        return embed


class HealthMenuView(discord.ui.View):
    """Main health menu view"""
    def __init__(self, cog: BotHealth):
        super().__init__(timeout=300)
        self.cog = cog

    @discord.ui.button(
        label="Refresh",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="health_refresh",
        row=0
    )
    async def refresh_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Regather all health data
        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()
        requirements = self.cog.get_requirements_health()

        overall = self.cog.get_overall_status(db_health, log_health, system_health, wos_api, gift_api, requirements)
        embed = self.cog._build_health_embed(overall, wos_api, gift_api, db_health, log_health, system_health, requirements)

        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

    @discord.ui.button(
        label="Run Cleanup",
        emoji=theme.cleanIcon,
        style=discord.ButtonStyle.primary,
        custom_id="health_cleanup",
        row=0
    )
    async def cleanup_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()

        # Show progress
        progress_embed = discord.Embed(
            title=f"{theme.refreshIcon} Running Cleanup...",
            description="Optimizing databases and archiving old logs. This may take a moment.",
            color=theme.emColor1
        )
        await interaction.followup.edit_message(message_id=interaction.message.id, embed=progress_embed, view=None)

        # Run cleanup
        results = await self.cog.run_cleanup()

        # Build result message
        result_parts = []
        if results['db_cleaned_mb'] > 0:
            result_parts.append(f"Recovered {results['db_cleaned_mb']:.1f} MB from databases")
        else:
            result_parts.append("Databases already optimized")

        if results['logs_archived'] > 0:
            result_parts.append(f"Archived {results['logs_archived']} old log files")
        else:
            result_parts.append("No old logs to archive")

        if results['errors']:
            result_parts.append(f"\n{theme.warnIcon} Some issues: {', '.join(results['errors'][:2])}")

        # Show results then refresh health view
        wos_api = await self.cog.check_wos_api_status()
        gift_api = await self.cog.check_gift_distribution_api()
        db_health = self.cog.get_database_health()
        log_health = self.cog.get_log_health()
        system_health = self.cog.get_system_health()

        overall = self.cog.get_overall_status(db_health, log_health, system_health, wos_api, gift_api)
        embed = self.cog._build_health_embed(overall, wos_api, gift_api, db_health, log_health, system_health)

        # Add cleanup result to embed
        embed.add_field(
            name=f"{theme.verifiedIcon} Cleanup Complete",
            value="\n".join(result_parts),
            inline=False
        )

        await interaction.followup.edit_message(message_id=interaction.message.id, embed=embed, view=self)

    @discord.ui.button(
        label="Settings",
        emoji=theme.settingsIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="health_settings",
        row=0
    )
    async def settings_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        config = self.cog.get_config()
        modal = HealthSettingsModal(self.cog, config)
        await interaction.response.send_modal(modal)

    @discord.ui.button(
        label="Clean Old Files",
        emoji=theme.trashIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="health_clean_cogs",
        row=0
    )
    async def clean_cogs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show unused cog files that can be cleaned up"""
        unused_cogs = self.cog.get_unused_cog_files()

        if not unused_cogs:
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} No Unused Files Found",
                description="All files in the cogs folder are in use. Nothing to clean up.",
                color=theme.emColor3
            )
            await interaction.response.edit_message(embed=embed, view=self)
            return

        # Show confirmation view with list of files
        embed = discord.Embed(
            title=f"{theme.trashIcon} Unused Cog Files Found",
            description=(
                f"Found **{len(unused_cogs)}** file(s) in the cogs folder that are not loaded by the bot.\n\n"
                f"These may be leftover files from a previous version. "
                f"Would you like to archive them?\n\n"
                f"**Files to archive:**\n"
                + "\n".join([f"• `{f}`" for f in unused_cogs[:15]])
                + (f"\n*...and {len(unused_cogs) - 15} more*" if len(unused_cogs) > 15 else "")
                + f"\n\n{theme.infoIcon} *Running custom cogs? Use **Manage Exceptions** to exclude them.*"
            ),
            color=theme.emColor2
        )
        embed.set_footer(text="Files will be moved to cogs/old_cogs_archive/ (not deleted)")

        view = CleanCogsConfirmView(self.cog, unused_cogs, self)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(
        label="Reload Cogs",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.primary,
        custom_id="health_reload_cogs",
        row=1
    )
    async def reload_cogs_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show cog reload menu"""
        loaded_cogs = self.cog.get_loaded_cogs()

        embed = discord.Embed(
            title=f"{theme.refreshIcon} Reload Cogs",
            description=(
                f"Select cogs to reload from the dropdown below.\n\n"
                f"**{len(loaded_cogs)}** cogs currently loaded.\n\n"
                f"{theme.warnIcon} *Reloading a cog will reset any in-memory state "
                f"(cached data, running tasks). Database data is preserved.*"
            ),
            color=theme.emColor1
        )

        view = ReloadCogsView(self.cog, loaded_cogs, self)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(
        label="Restart Bot",
        emoji=theme.warnIcon,
        style=discord.ButtonStyle.danger,
        custom_id="health_restart_bot",
        row=1
    )
    async def restart_bot_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Show restart confirmation"""
        embed = discord.Embed(
            title=f"{theme.warnIcon} Restart Bot",
            description=(
                f"Are you sure you want to restart the bot?\n\n"
                f"**What will happen:**\n"
                f"• Active sessions and menus will stop working\n"
                f"• Running tasks will be cancelled\n"
                f"• Bot will be offline briefly during restart\n"
                f"• All data is saved - nothing will be lost\n\n"
                f"The bot will automatically reconnect after restart."
            ),
            color=0xFF0000
        )

        view = RestartConfirmView(self.cog, self)
        await interaction.response.edit_message(embed=embed, view=view)

    @discord.ui.button(
        label="Main Menu",
        emoji=theme.homeIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="health_back",
        row=1
    )
    async def back_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            main_menu_cog = self.cog.bot.get_cog("MainMenu")
            if main_menu_cog:
                await main_menu_cog.show_maintenance(interaction)
        except Exception as e:
            self.cog.logger.error(f"Error returning to main menu: {e}")
            print(f"Error returning to main menu: {e}")


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
    """View for selecting and reloading cogs"""
    def __init__(self, cog: BotHealth, loaded_cogs: list, parent_view: HealthMenuView):
        super().__init__(timeout=120)
        self.cog = cog
        self.loaded_cogs = loaded_cogs
        self.parent_view = parent_view
        self.selected_cogs = []

        # Add the select menu
        self._add_cog_select()

    def _add_cog_select(self):
        """Add the cog selection dropdown"""
        options = []
        # Discord limits to 25 options, so we show the most important ones
        for cog_name in self.loaded_cogs[:25]:
            # Mark bot_health specially since it can't be reloaded
            if cog_name == "bot_health":
                options.append(discord.SelectOption(
                    label=cog_name,
                    value=cog_name,
                    description="Cannot reload (in use)",
                    emoji=theme.deniedIcon
                ))
            else:
                options.append(discord.SelectOption(
                    label=cog_name,
                    value=cog_name,
                    emoji=theme.settingsIcon
                ))

        if options:
            select = discord.ui.Select(
                placeholder="Select cogs to reload...",
                min_values=1,
                max_values=min(len(options), 25),
                options=options,
                custom_id="cog_select"
            )
            select.callback = self.on_cog_select
            self.add_item(select)

    async def on_cog_select(self, interaction: discord.Interaction):
        """Handle cog selection"""
        self.selected_cogs = interaction.data.get('values', [])
        # Just acknowledge - wait for button press
        await interaction.response.defer()

    @discord.ui.button(
        label="Reload Selected",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.primary,
        custom_id="reload_selected",
        row=1
    )
    async def reload_selected_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.selected_cogs:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select at least one cog to reload.",
                ephemeral=True
            )
            return

        await self._perform_reload(interaction, self.selected_cogs)

    @discord.ui.button(
        label="Reload All",
        emoji=theme.refreshIcon,
        style=discord.ButtonStyle.danger,
        custom_id="reload_all",
        row=1
    )
    async def reload_all_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._perform_reload(interaction, self.loaded_cogs)

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

    @discord.ui.button(
        label="Cancel",
        emoji=theme.backIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="cancel_reload",
        row=1
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

        await interaction.followup.edit_message(
            message_id=interaction.message.id,
            embed=embed,
            view=self.parent_view
        )


class RestartConfirmView(discord.ui.View):
    """Confirmation view for bot restart"""
    def __init__(self, cog: BotHealth, parent_view: HealthMenuView):
        super().__init__(timeout=30)
        self.cog = cog
        self.parent_view = parent_view

    @discord.ui.button(
        label="Confirm Restart",
        emoji=theme.warnIcon,
        style=discord.ButtonStyle.danger,
        custom_id="confirm_restart"
    )
    async def confirm_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer()
        await self.cog.perform_restart(interaction)

    @discord.ui.button(
        label="Cancel",
        emoji=theme.backIcon,
        style=discord.ButtonStyle.secondary,
        custom_id="cancel_restart"
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