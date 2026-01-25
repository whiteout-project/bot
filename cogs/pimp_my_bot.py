"""
Theme management system for bot customization.
Provides icons, colors, and dividers that can be customized via themes.
"""
import discord
from discord.ext import commands
import sqlite3
import os
import re
import io
import json
import base64
import unicodedata
import aiohttp
import logging
from typing import Tuple

logger = logging.getLogger(__name__)
from .permission_handler import PermissionManager

# Database path constant
THEME_DB_PATH = 'db/pimpmybot.sqlite'

# Theme Gallery API configuration (temporarily disabled)
THEME_GALLERY_URL = ""
THEME_GALLERY_API_KEY = ""

# Default values
DEFAULT_EMOJI = "👻"

# Icon categories for theme editor - organized to easily find the right ones
# General: Universal elements used across the bot
# Feature categories: Match main menu structure for intuitive navigation (with some consolidation)
ICON_CATEGORIES = {
    # === GENERAL (Universal elements) ===
    "Status": [
        "verifiedIcon", "deniedIcon", "warnIcon", "infoIcon", "questionIcon", "checkIcon",
        "processingIcon", "blankListIcon", "circleIcon"
    ],
    "Navigation": [
        "homeIcon", "backIcon", "prevIcon", "nextIcon", "forwardIcon",
        "upIcon", "downIcon", "num1Icon", "num2Icon", "num3Icon",
        "num4Icon", "num5Icon", "num10Icon"
    ],
    "Actions": [
        "addIcon", "minusIcon", "trashIcon", "editListIcon", "settingsIcon",
        "saveIcon", "refreshIcon", "eyeIcon", "eyesIcon", "searchIcon", "listIcon",
        "lockIcon", "linkIcon", "copyIcon", "retryIcon", "deleteIcon", "redeemIcon",
        "multiplyIcon", "divideIcon", "magnifyingIcon"
    ],
    "Display": [
        "userIcon", "fidIcon", "timeIcon", "calendarIcon", "levelIcon",
        "globeIcon", "membersIcon", "crownIcon", "totalIcon", "pinIcon",
        "averageIcon", "chartIcon", "documentIcon", "newIcon", "locationIcon",
        "fireIcon", "messageNoIcon"
    ],
    # === FEATURE CATEGORIES ===
    "Operations": [
        "shieldIcon", "crossIcon",
        "importIcon", "exportIcon", "transferIcon",
        "giftIcon", "giftsIcon", "ticketIcon", "packageIcon", "targetIcon",
        "giftAddIcon", "giftAlarmIcon", "gifAlertIcon", "giftCheckIcon",
        "giftTotalIcon", "giftDeleteIcon", "giftHashtagIcon", "giftSettingsIcon"
    ],
    "Alliance History": [
        "allianceOldIcon", "allianceIcon", "avatarOldIcon", "avatarIcon",
        "stoveOldIcon", "stoveIcon", "stateOldIcon", "stateIcon"
    ],
    "Notifications": [
        "announceIcon", "wizardIcon", "alarmClockIcon", "hourglassIcon", "bellIcon", "muteIcon"
    ],
    "Events": [
        "bearTrapIcon", "crazyJoeIcon", "foundryIcon", "canyonClashIcon",
        "fortressBattleIcon", "frostfireMineIcon", "castleBattleIcon",
        "svsIcon", "mercenaryIcon", "dailyResetIcon", "frostdragonIcon"
    ],
    "Minister": [
        "ministerIcon", "constructionIcon", "researchIcon", "trainingIcon",
        "archiveIcon", "medalIcon"
    ],
    "Bot Management": [
        "robotIcon", "supportIcon", "chatIcon", "boltIcon", "testIcon",
        "cleanIcon", "paletteIcon", "starIcon", "heartIcon", "messageIcon",
        "shutdownZzzIcon", "shutdownDoorIcon", "shutdownHandIcon", "shutdownMoonIcon",
        "shutdownPlugIcon", "shutdownStopIcon", "shutdownClapperIcon", "shutdownSparkleIcon"
    ]
}

# Flat list of all icon names derived from categories
ICON_NAMES = [icon for icons in ICON_CATEGORIES.values() for icon in icons]

# Default icon values for reset functionality (matches INSERT statement defaults)
DEFAULT_ICON_VALUES = {
    # Old icons for change tracking
    'allianceOldIcon': '⚔️', 'avatarOldIcon': '👤', 'stoveOldIcon': '🔥', 'stateOldIcon': '🌏',
    # Current icons
    'allianceIcon': '⚔️', 'avatarIcon': '👤', 'stoveIcon': '🔥', 'stateIcon': '🌏',
    'listIcon': '📜', 'fidIcon': '🆔', 'timeIcon': '🕰️', 'homeIcon': '🏠',
    'num1Icon': '1️⃣', 'num2Icon': '2️⃣', 'num3Icon': '3️⃣', 'num4Icon': '4️⃣',
    'num5Icon': '5️⃣', 'num10Icon': '🔟', 'newIcon': '🆕', 'pinIcon': '📍',
    'saveIcon': '💾', 'robotIcon': '🤖', 'crossIcon': '⚔️', 'heartIcon': '💗',
    'shieldIcon': '🛡️', 'targetIcon': '🎯', 'redeemIcon': '🔄', 'membersIcon': '👥',
    'averageIcon': '📈', 'messageIcon': '🔊', 'supportIcon': '🆘', 'foundryIcon': '🏭',
    'announceIcon': '📢', 'ministerIcon': '🏛️', 'researchIcon': '🔬', 'trainingIcon': '⚔️',
    'crazyJoeIcon': '🤪', 'bearTrapIcon': '🐻', 'calendarIcon': '📅', 'editListIcon': '📝',
    'settingsIcon': '⚙️', 'hourglassIcon': '⏳', 'messageNoIcon': '🔇', 'blankListIcon': '⚪',
    'alarmClockIcon': '⏰', 'magnifyingIcon': '🔍', 'frostdragonIcon': '🐉', 'canyonClashIcon': '⚔️',
    'constructionIcon': '🔨', 'castleBattleIcon': '☀️',
    # Gift icons
    'giftIcon': '🎁', 'giftsIcon': '🛍️', 'giftAddIcon': '➕', 'giftAlarmIcon': '⏰',
    'gifAlertIcon': '⚠️', 'giftCheckIcon': '✅', 'giftTotalIcon': '🟰', 'giftDeleteIcon': '🗑️',
    'giftHashtagIcon': '🔢', 'giftSettingsIcon': '⚙️',
    # Status icons
    'processingIcon': '🔄', 'verifiedIcon': '✅', 'questionIcon': '❓', 'transferIcon': '↔️',
    'multiplyIcon': '✖️', 'divideIcon': '➗', 'deniedIcon': '❌', 'deleteIcon': '➖',
    'exportIcon': '📤', 'importIcon': '📥', 'retryIcon': '🔁', 'totalIcon': '🟰',
    'infoIcon': 'ℹ️', 'warnIcon': '⚠️', 'addIcon': '➕',
    # Navigation icons
    'prevIcon': '◀️', 'nextIcon': '▶️', 'backIcon': '⬅️', 'forwardIcon': '➡️',
    'minusIcon': '➖', 'chartIcon': '📊', 'documentIcon': '📄', 'eyeIcon': '👁️',
    'globeIcon': '🌍', 'wizardIcon': '🧙', 'muteIcon': '🔕',
    # Shutdown icons
    'shutdownZzzIcon': '💤', 'shutdownDoorIcon': '🚪', 'shutdownHandIcon': '👋',
    'shutdownMoonIcon': '🌙', 'shutdownPlugIcon': '🔌', 'shutdownStopIcon': '🛑',
    'shutdownClapperIcon': '🎬', 'shutdownSparkleIcon': '✨',
    # Misc icons
    'medalIcon': '🎖️', 'checkIcon': '☑️', 'circleIcon': '⚪',
    'userIcon': '👤', 'trashIcon': '🗑️', 'refreshIcon': '🔄', 'levelIcon': '🔢',
    'lockIcon': '🔐', 'cleanIcon': '🧹', 'archiveIcon': '🗃️', 'upIcon': '⬆️', 'downIcon': '⬇️',
    'crownIcon': '👑', 'linkIcon': '🔗', 'chatIcon': '💬', 'bellIcon': '🔔', 'boltIcon': '⚡',
    'locationIcon': '📍', 'testIcon': '🧪', 'packageIcon': '📦', 'ticketIcon': '🎫',
    'fireIcon': '🔥', 'searchIcon': '🔍', 'paletteIcon': '🎨',
    'eyesIcon': '👀', 'copyIcon': '📋', 'starIcon': '⭐',
    # Event icons
    'fortressBattleIcon': '🏰', 'frostfireMineIcon': '⛏️', 'svsIcon': '⚡',
    'mercenaryIcon': '🗡️', 'dailyResetIcon': '🔄',
}

async def check_interaction_user(interaction: discord.Interaction, expected_user_id: int) -> bool:
    """
    Check if the interaction user matches expected user. Sends denial message if not.
    Returns True if user matches, False otherwise.
    """
    if interaction.user.id != expected_user_id:
        # Import theme here to avoid circular import at module level
        await interaction.response.send_message(
            f"{theme.deniedIcon} Only the user who initiated this command can use this.",
            ephemeral=True
        )
        return False
    return True

def build_divider(start, pattern, end, length, max_length=99):
    """Build a divider string with exact character length.

    Args:
        start: Start character(s)
        pattern: Repeating pattern string (repeats as-is)
        end: End character(s)
        length: Total character count of the output
        max_length: Maximum allowed length (default 99)

    Returns:
        A divider string of exactly `length` characters
    """
    if length > max_length:
        length = max_length

    start_str = str(start) if start else ""
    end_str = str(end) if end else ""
    pattern_str = str(pattern) if pattern else "━"

    # Calculate space available for pattern (reserve room for end)
    pattern_space = length - len(start_str) - len(end_str)

    if pattern_space <= 0:
        return (start_str + end_str)[:length]

    # Repeat pattern to fill the middle space
    if pattern_str:
        repeats_needed = (pattern_space // len(pattern_str)) + 1
        middle = (pattern_str * repeats_needed)[:pattern_space]
    else:
        middle = ""

    return start_str + middle + end_str

class ThemeManager:
    """
    Singleton class that manages theme configuration.
    Loads theme values from database and provides them as attributes.
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._bot = None
        self._set_defaults()
        self.load()

    async def set_bot(self, bot):
        """Set bot reference for emoji validation. Call this after bot is ready."""
        self._bot = bot
        # Fetch application emojis and validate
        app_emojis = await self._fetch_application_emojis()
        self._validate_emojis(app_emojis)

    async def _fetch_application_emojis(self):
        """Fetch list of application emoji IDs the bot has access to."""
        if not self._bot:
            return set()

        try:
            # Use discord.py's built-in method
            app_emojis = await self._bot.fetch_application_emojis()
            return {e.id for e in app_emojis}
        except Exception as e:
            logger.warning(f"Could not fetch application emojis: {e}")
            print(f"Could not fetch application emojis: {e}")
            return set()

    def _validate_emojis(self, accessible_emoji_ids: set = None):
        """Check all icon attributes and hide inaccessible custom emojis."""
        if not self._bot:
            return

        # Build set of accessible emoji IDs (guild emojis + application emojis)
        if accessible_emoji_ids is None:
            accessible_emoji_ids = set()

        # Add guild emojis the bot can see
        for emoji in self._bot.emojis:
            accessible_emoji_ids.add(emoji.id)

        for icon_name in ICON_NAMES:
            value = getattr(self, icon_name, None)
            if not value:
                continue

            # Check if it's a custom Discord emoji
            match = re.match(r'<a?:(\w+):(\d+)>', str(value))
            if match:
                emoji_name = match.group(1)
                emoji_id = int(match.group(2))
                # If bot can't access this emoji, set to empty string (hidden)
                if emoji_id not in accessible_emoji_ids:
                    logger.warning(f"Theme emoji '{icon_name}' (:{emoji_name}:{emoji_id}) is inaccessible - hiding it")
                    print(f"[WARNING] Theme emoji '{icon_name}' (:{emoji_name}:{emoji_id}) is inaccessible - hiding it")
                    setattr(self, icon_name, "")

    def _set_defaults(self):
        """Set all theme values to defaults."""
        # Set all icon defaults using ICON_NAMES constant
        for icon_name in ICON_NAMES:
            setattr(self, icon_name, DEFAULT_EMOJI)

        # Color defaults (as integers for discord.Color)
        self.emColor1 = 0x0000FF  # blue
        self.emColor2 = 0xFF0000  # red
        self.emColor3 = 0x00FF00  # green
        self.emColor4 = 0xFFFF00  # yellow

        # Color strings (for display/editing)
        self.emColorString1 = "#0000FF"
        self.emColorString2 = "#FF0000"
        self.emColorString3 = "#00FF00"
        self.emColorString4 = "#FFFF00"
        self.headerColor1 = "#1F77B4"
        self.headerColor2 = "#28A745"

        # Divider defaults (20 chars to fit mobile with thumbnails)
        self.upperDivider = "━━━━━━━━━━━━━━━━━━━━"
        self.lowerDivider = "━━━━━━━━━━━━━━━━━━━━"
        self.middleDivider = "━━━━━━━━━━━━━━━━━━━━"

    def _ensure_db(self):
        """Create database and default theme if they don't exist."""
        # Ensure db directory exists
        os.makedirs(os.path.dirname(THEME_DB_PATH), exist_ok=True)

        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()

            # Create table if not exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pimpsettings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    themeName TEXT UNIQUE NOT NULL,
                    themeCreator INTEGER,
                    allianceOldIcon TEXT, avatarOldIcon TEXT, stoveOldIcon TEXT, stateOldIcon TEXT,
                    allianceIcon TEXT, avatarIcon TEXT, stoveIcon TEXT, stateIcon TEXT,
                    listIcon TEXT, fidIcon TEXT, timeIcon TEXT, homeIcon TEXT,
                    num1Icon TEXT, num2Icon TEXT, num3Icon TEXT, num4Icon TEXT,
                    num5Icon TEXT, num10Icon TEXT, newIcon TEXT, pinIcon TEXT,
                    saveIcon TEXT, robotIcon TEXT, crossIcon TEXT, heartIcon TEXT,
                    shieldIcon TEXT, targetIcon TEXT, redeemIcon TEXT, membersIcon TEXT,
                    averageIcon TEXT, messageIcon TEXT, supportIcon TEXT, foundryIcon TEXT,
                    announceIcon TEXT, ministerIcon TEXT, researchIcon TEXT, trainingIcon TEXT,
                    crazyJoeIcon TEXT, bearTrapIcon TEXT, calendarIcon TEXT, editListIcon TEXT,
                    settingsIcon TEXT, hourglassIcon TEXT, messageNoIcon TEXT, blankListIcon TEXT,
                    alarmClockIcon TEXT, magnifyingIcon TEXT, frostdragonIcon TEXT, canyonClashIcon TEXT,
                    constructionIcon TEXT, castleBattleIcon TEXT,
                    giftIcon TEXT, giftsIcon TEXT, giftAddIcon TEXT, giftAlarmIcon TEXT,
                    gifAlertIcon TEXT, giftCheckIcon TEXT, giftTotalIcon TEXT, giftDeleteIcon TEXT,
                    giftHashtagIcon TEXT, giftSettingsIcon TEXT,
                    processingIcon TEXT, verifiedIcon TEXT, questionIcon TEXT, transferIcon TEXT,
                    multiplyIcon TEXT, divideIcon TEXT, deniedIcon TEXT, deleteIcon TEXT,
                    exportIcon TEXT, importIcon TEXT, retryIcon TEXT, totalIcon TEXT,
                    infoIcon TEXT, warnIcon TEXT, addIcon TEXT,
                    prevIcon TEXT, nextIcon TEXT, backIcon TEXT, forwardIcon TEXT,
                    minusIcon TEXT, chartIcon TEXT, documentIcon TEXT, eyeIcon TEXT,
                    globeIcon TEXT, wizardIcon TEXT, muteIcon TEXT,
                    shutdownZzzIcon TEXT, shutdownDoorIcon TEXT, shutdownHandIcon TEXT,
                    shutdownMoonIcon TEXT, shutdownPlugIcon TEXT, shutdownStopIcon TEXT,
                    shutdownClapperIcon TEXT, shutdownSparkleIcon TEXT,
                    medalIcon TEXT, checkIcon TEXT, circleIcon TEXT,
                    userIcon TEXT, trashIcon TEXT, refreshIcon TEXT, levelIcon TEXT,
                    lockIcon TEXT, cleanIcon TEXT, archiveIcon TEXT, upIcon TEXT, downIcon TEXT,
                    crownIcon TEXT, linkIcon TEXT, chatIcon TEXT, bellIcon TEXT, boltIcon TEXT,
                    locationIcon TEXT, testIcon TEXT, packageIcon TEXT, ticketIcon TEXT, fireIcon TEXT, searchIcon TEXT, paletteIcon TEXT,
                    eyesIcon TEXT, copyIcon TEXT, starIcon TEXT,
                    fortressBattleIcon TEXT, frostfireMineIcon TEXT, svsIcon TEXT, mercenaryIcon TEXT, dailyResetIcon TEXT,
                    dividerStart1 TEXT, dividerPattern1 TEXT, dividerEnd1 TEXT, dividerLength1 INTEGER, dividerCodeBlock1 INTEGER DEFAULT 0,
                    dividerStart2 TEXT, dividerPattern2 TEXT, dividerEnd2 TEXT, dividerLength2 INTEGER, dividerCodeBlock2 INTEGER DEFAULT 0,
                    dividerStart3 TEXT, dividerPattern3 TEXT, dividerEnd3 TEXT, dividerLength3 INTEGER, dividerCodeBlock3 INTEGER DEFAULT 0,
                    emColorString1 TEXT, emColorString2 TEXT, emColorString3 TEXT, emColorString4 TEXT,
                    headerColor1 TEXT, headerColor2 TEXT,
                    furnaceLevel0 TEXT, furnaceLevel1 TEXT, furnaceLevel2 TEXT, furnaceLevel3 TEXT,
                    furnaceLevel4 TEXT, furnaceLevel5 TEXT, furnaceLevel6 TEXT, furnaceLevel7 TEXT,
                    furnaceLevel8 TEXT, furnaceLevel9 TEXT, furnaceLevel10 TEXT, furnaceLevel11 TEXT,
                    furnaceLevel12 TEXT, furnaceLevel13 TEXT, furnaceLevel14 TEXT, furnaceLevel15 TEXT,
                    furnaceLevel16 TEXT, furnaceLevel17 TEXT, furnaceLevel18 TEXT, furnaceLevel19 TEXT,
                    furnaceLevel20 TEXT, furnaceLevel21 TEXT, furnaceLevel22 TEXT, furnaceLevel23 TEXT,
                    furnaceLevel24 TEXT, furnaceLevel25 TEXT, furnaceLevel26 TEXT, furnaceLevel27 TEXT,
                    furnaceLevel28 TEXT, furnaceLevel29 TEXT, furnaceLevel30 TEXT,
                    is_active INTEGER DEFAULT 0,
                    themeDescription TEXT DEFAULT '',
                    createdAt TEXT DEFAULT '',
                    created_guild_id INTEGER
                )
            """)

            # Create server_themes table for per-server theme selection
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS server_themes (
                    guild_id INTEGER PRIMARY KEY,
                    theme_name TEXT NOT NULL
                )
            """)

            # Migration: Add dividerCodeBlock columns if they don't exist
            cursor.execute("PRAGMA table_info(pimpsettings)")
            existing_columns = {col[1] for col in cursor.fetchall()}
            for col_name in ['dividerCodeBlock1', 'dividerCodeBlock2', 'dividerCodeBlock3']:
                if col_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE pimpsettings ADD COLUMN {col_name} INTEGER DEFAULT 0")

            # Check if default theme exists
            cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName='default'")
            if cursor.fetchone()[0] == 0:
                # Insert default theme with proper emojis
                cursor.execute("""
                    INSERT INTO pimpsettings (
                        themeName, themeCreator, is_active,
                        allianceOldIcon, avatarOldIcon, stoveOldIcon, stateOldIcon,
                        allianceIcon, avatarIcon, stoveIcon, stateIcon,
                        listIcon, fidIcon, timeIcon, homeIcon,
                        num1Icon, num2Icon, num3Icon, num4Icon,
                        num5Icon, num10Icon, newIcon, pinIcon,
                        saveIcon, robotIcon, crossIcon, heartIcon,
                        shieldIcon, targetIcon, redeemIcon, membersIcon,
                        averageIcon, messageIcon, supportIcon, foundryIcon,
                        announceIcon, ministerIcon, researchIcon, trainingIcon,
                        crazyJoeIcon, bearTrapIcon, calendarIcon, editListIcon,
                        settingsIcon, hourglassIcon, messageNoIcon, blankListIcon,
                        alarmClockIcon, magnifyingIcon, frostdragonIcon, canyonClashIcon,
                        constructionIcon, castleBattleIcon,
                        giftIcon, giftsIcon, giftAddIcon, giftAlarmIcon,
                        gifAlertIcon, giftCheckIcon, giftTotalIcon, giftDeleteIcon,
                        giftHashtagIcon, giftSettingsIcon,
                        processingIcon, verifiedIcon, questionIcon, transferIcon,
                        multiplyIcon, divideIcon, deniedIcon, deleteIcon,
                        exportIcon, importIcon, retryIcon, totalIcon,
                        infoIcon, warnIcon, addIcon,
                        prevIcon, nextIcon, backIcon, forwardIcon,
                        minusIcon, chartIcon, documentIcon, eyeIcon,
                        globeIcon, wizardIcon, muteIcon,
                        shutdownZzzIcon, shutdownDoorIcon, shutdownHandIcon,
                        shutdownMoonIcon, shutdownPlugIcon, shutdownStopIcon,
                        shutdownClapperIcon, shutdownSparkleIcon,
                        medalIcon, checkIcon, circleIcon,
                        userIcon, trashIcon, refreshIcon, levelIcon,
                        lockIcon, cleanIcon, archiveIcon, upIcon, downIcon,
                        crownIcon, linkIcon, chatIcon, bellIcon, boltIcon,
                        locationIcon, testIcon, packageIcon, ticketIcon, fireIcon, searchIcon, paletteIcon,
                        eyesIcon, copyIcon, starIcon,
                        fortressBattleIcon, frostfireMineIcon, svsIcon, mercenaryIcon, dailyResetIcon,
                        dividerStart1, dividerPattern1, dividerEnd1, dividerLength1,
                        dividerStart2, dividerPattern2, dividerEnd2, dividerLength2,
                        dividerStart3, dividerPattern3, dividerEnd3, dividerLength3,
                        emColorString1, emColorString2, emColorString3, emColorString4,
                        headerColor1, headerColor2
                    ) VALUES (
                        'default', '@WOSLand', 1,
                        '⚔️', '👤', '🔥', '🌏',
                        '⚔️', '👤', '🔥', '🌏',
                        '📜', '🆔', '🕰️', '🏠',
                        '1️⃣', '2️⃣', '3️⃣', '4️⃣',
                        '5️⃣', '🔟', '🆕', '📍',
                        '💾', '🤖', '⚔️', '💗',
                        '🛡️', '🎯', '🔄', '👥',
                        '📈', '🔊', '🆘', '🏭',
                        '📢', '🏛️', '🔬', '⚔️',
                        '🤪', '🐻', '📅', '📝',
                        '⚙️', '⏳', '🔇', '⚪',
                        '⏰', '🔍', '🐉', '⚔️',
                        '🏗️', '☀️',
                        '🎁', '🛍️', '➕', '⏰',
                        '⚠️', '✅', '🟰', '🗑️',
                        '🔢', '⚙️',
                        '🔄', '✅', '❓', '↔️',
                        '✖️', '➗', '❌', '➖',
                        '📤', '📥', '🔁', '🟰',
                        'ℹ️', '⚠️', '➕',
                        '◀️', '▶️', '⬅️', '➡️',
                        '➖', '📊', '📄', '👁️',
                        '🌍', '🧙', '🔕',
                        '💤', '🚪', '👋',
                        '🌙', '🔌', '🛑',
                        '🎬', '✨',
                        '🎖️', '☑️', '⚪',
                        '👤', '🗑️', '🔄', '🔢',
                        '🔐', '🧹', '🗃️', '⬆️', '⬇️',
                        '👑', '🔗', '💬', '🔔', '⚡',
                        '📍', '🧪', '📦', '🎫', '🔥', '🔍', '🎨',
                        '👀', '📋', '⭐',
                        '🏰', '⛏️', '⚡', '🗡️', '🔄',
                        '━', '━', '━', 20,
                        '━', '━', '━', 20,
                        '━', '━', '━', 20,
                        '#3498DB', '#E74C3C', '#2ECC71', '#F1C40F',
                        '#1F77B4', '#28A745'
                    )
                """)
                conn.commit()
                logger.info("Theme database created with default theme.")
                print("Theme database created with default theme.")

    def load(self):
        """Load theme from database. Safe to call multiple times."""
        # Ensure database exists
        self._ensure_db()

        if not os.path.exists(THEME_DB_PATH):
            return

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                # Use Row factory to get dict-like access by column name
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                # Check if table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pimpsettings'")
                if not cursor.fetchone():
                    return

                # Get active theme
                cursor.execute("SELECT * FROM pimpsettings WHERE is_active=1 LIMIT 1")
                theme_row = cursor.fetchone()

                if not theme_row:
                    cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", ("default",))
                    theme_row = cursor.fetchone()

                if theme_row:
                    # Convert Row to dict for easier access
                    theme_dict = dict(theme_row)
                    self._apply_theme(theme_dict)
                    self._validate_emojis()

        except Exception as e:
            logger.warning(f"Could not load theme settings: {e}")
            print(f"Warning: Could not load theme settings: {e}")

    def _apply_theme(self, theme_dict):
        """Apply theme data from database row dictionary."""
        # Apply icons using the ICON_NAMES constant
        for icon_name in ICON_NAMES:
            value = theme_dict.get(icon_name) or DEFAULT_EMOJI
            setattr(self, icon_name, value)

        # Apply divider settings using the shared build_divider function
        # Code block wrapping is configurable per divider (default: no code block)
        divider1 = build_divider(
            theme_dict.get('dividerStart1') or "━",
            theme_dict.get('dividerPattern1') or "━",
            theme_dict.get('dividerEnd1') or "━",
            int(theme_dict.get('dividerLength1') or 20)
        )
        divider2 = build_divider(
            theme_dict.get('dividerStart2') or "━",
            theme_dict.get('dividerPattern2') or "━",
            theme_dict.get('dividerEnd2') or "━",
            int(theme_dict.get('dividerLength2') or 20)
        )
        divider3 = build_divider(
            theme_dict.get('dividerStart3') or "━",
            theme_dict.get('dividerPattern3') or "━",
            theme_dict.get('dividerEnd3') or "━",
            int(theme_dict.get('dividerLength3') or 20)
        )

        # Apply code block wrapping if enabled
        self.upperDivider = f"`{divider1}`" if theme_dict.get('dividerCodeBlock1') else divider1
        self.lowerDivider = f"`{divider2}`" if theme_dict.get('dividerCodeBlock2') else divider2
        self.middleDivider = f"`{divider3}`" if theme_dict.get('dividerCodeBlock3') else divider3

        # Apply colors by name
        self.emColorString1 = theme_dict.get('emColorString1') or "#0000FF"
        self.emColorString2 = theme_dict.get('emColorString2') or "#FF0000"
        self.emColorString3 = theme_dict.get('emColorString3') or "#00FF00"
        self.emColorString4 = theme_dict.get('emColorString4') or "#FFFF00"
        self.headerColor1 = theme_dict.get('headerColor1') or "#1F77B4"
        self.headerColor2 = theme_dict.get('headerColor2') or "#28A745"

        # Convert color strings to integers (with fallback for malformed values)
        def parse_color(color_str: str, default: int) -> int:
            try:
                return int(color_str.lstrip('#'), 16)
            except ValueError:
                return default

        self.emColor1 = parse_color(self.emColorString1, 0x0000FF)
        self.emColor2 = parse_color(self.emColorString2, 0xFF0000)
        self.emColor3 = parse_color(self.emColorString3, 0x00FF00)
        self.emColor4 = parse_color(self.emColorString4, 0xFFFF00)

    def load_for_guild(self, guild_id: int = None):
        """Load theme for specific guild, or global if no override."""
        self._ensure_db()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()

                theme_name = None

                # Check for server-specific override
                if guild_id:
                    cursor.execute(
                        "SELECT theme_name FROM server_themes WHERE guild_id = ?",
                        (guild_id,)
                    )
                    result = cursor.fetchone()
                    if result:
                        theme_name = result['theme_name']

                # Fall back to global active theme
                if not theme_name:
                    cursor.execute("SELECT themeName FROM pimpsettings WHERE is_active = 1")
                    result = cursor.fetchone()
                    theme_name = result['themeName'] if result else 'default'

                # Load the theme
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName = ?", (theme_name,))
                row = cursor.fetchone()

                if row:
                    self._apply_theme(dict(row))
                    self._validate_emojis()

        except Exception as e:
            logger.warning(f"Could not load theme for guild {guild_id}: {e}")
            print(f"Warning: Could not load theme for guild {guild_id}: {e}")

    def get_server_theme_name(self, guild_id: int) -> str:
        """Get the theme name for a specific server (or global if no override)."""
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()

                # Check for server-specific override
                if guild_id:
                    cursor.execute(
                        "SELECT theme_name FROM server_themes WHERE guild_id = ?",
                        (guild_id,)
                    )
                    result = cursor.fetchone()
                    if result:
                        return result[0]

                # Fall back to global active theme
                cursor.execute("SELECT themeName FROM pimpsettings WHERE is_active = 1")
                result = cursor.fetchone()
                return result[0] if result else 'default'

        except Exception:
            return 'default'

# Singleton instance - this is what other modules import
theme = ThemeManager()

def get_theme_for_guild(guild_id: int = None) -> ThemeManager:
    """Get theme configured for a specific guild. Reloads the singleton with guild's theme."""
    theme.load_for_guild(guild_id)
    return theme

class ThemeMenuView(discord.ui.View):
    """Main theme management menu - entry point from settings."""

    def __init__(self, cog, original_user_id, guild_id: int = None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.guild_id = guild_id
        # Check if user is global admin
        _, self.is_global_admin = PermissionManager.is_admin(original_user_id)
        # Default to server's active theme on init
        self.selected_theme = theme.get_server_theme_name(guild_id)
        self._build_components()

    def _get_theme_list(self):
        """Get list of all theme names from database with metadata."""
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT themeName, is_active, created_guild_id FROM pimpsettings ORDER BY themeName")
                return cursor.fetchall()
        except Exception:
            return []

    def _get_global_active_theme(self):
        """Get the name of the globally active theme."""
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT themeName FROM pimpsettings WHERE is_active = 1")
                result = cursor.fetchone()
                return result[0] if result else 'default'
        except Exception:
            return 'default'

    def _get_server_theme(self):
        """Get the theme name for this server (or None if using global)."""
        if not self.guild_id:
            return None
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT theme_name FROM server_themes WHERE guild_id = ?", (self.guild_id,))
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception:
            return None

    def _theme_owned_by_guild(self, theme_name: str, guild_id: int) -> bool:
        """Check if theme was created on the given guild."""
        if not guild_id:
            return False
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT created_guild_id FROM pimpsettings WHERE themeName=?", (theme_name,))
                result = cursor.fetchone()
                return result and result[0] == guild_id
        except Exception:
            return False

    def build_embed(self) -> discord.Embed:
        """Build the theme menu embed."""
        themes = self._get_theme_list()
        global_active = self._get_global_active_theme()
        server_theme = self._get_server_theme()

        embed = discord.Embed(
            title=f"{theme.paletteIcon} Theme Settings",
            description=(
                f"{theme.upperDivider}\n"
                f"Customize your bot's appearance with themes!\n"
                f"- Custom themes can be created, edited, and shared.\n"
                f"- Each theme consists of icons, dividers, and colors.\n"
                f"- Each server can use a different theme, or the global default.\n"
                f"- Any theme can be set as the global default.\n"
                f"- The original default theme can be edited but not deleted.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor1
        )

        # Status info
        embed.add_field(name="Global Active", value=global_active, inline=True)

        if server_theme:
            embed.add_field(name="This Server", value=f"{server_theme} {theme.checkIcon}", inline=True)
        else:
            embed.add_field(name="This Server", value=f"(using global)", inline=True)

        embed.add_field(name="Total Themes", value=str(len(themes)), inline=True)

        # Help info
        help_text = (
            f"{theme.addIcon} **Create** - Make a new theme\n"
            f"{theme.editListIcon} **Edit** - Customize the currently selected theme\n"
            f"{theme.importIcon} **Import** / {theme.exportIcon} **Export** - JSON import/export\n"
            f"{theme.starIcon} **Set Default** - Global default (global admin only)\n"
            f"{theme.checkIcon} **Apply to Server** - Apply selected theme to this server\n"
            f"{theme.globeIcon} **Revert to Global** - Revert to using the global default theme\n"
            f"{theme.trashIcon} **Delete** - Remove the selected theme (cannot be undone)\n"
            f"{theme.heartIcon} **Share Online** - Share your theme with others online (custom themes only)\n"
        )
        embed.add_field(name="Quick Guide", value=help_text, inline=False)
        embed.set_footer(text="Seeing ghosts? The bot must have access to your theme's custom emojis. Try re-importing or check server emoji access.")

        return embed

    def _build_components(self):
        """Build the view's components."""
        self.clear_items()

        # Theme dropdown
        themes = self._get_theme_list()
        global_active = self._get_global_active_theme()
        server_theme = self._get_server_theme()
        is_default = self.selected_theme == "default"
        is_global_active = self.selected_theme == global_active
        is_server_active = self.selected_theme == server_theme

        # Check if selected theme is owned by this guild (for permission checks)
        is_owned_theme = self._theme_owned_by_guild(self.selected_theme, self.guild_id)
        can_modify = self.is_global_admin or is_owned_theme

        if themes:
            options = []
            for name, is_active_flag, created_guild_id in themes:
                # Build label with status indicators
                label = name
                if is_active_flag:
                    label = f"{name} (Global)"
                if server_theme == name:
                    label = f"{name} (Server)"
                # Show ownership indicator for non-global admins
                if not self.is_global_admin:
                    if created_guild_id == self.guild_id:
                        label = f"{label} ✎"  # Editable - owned by this server
                options.append(discord.SelectOption(
                    label=label,
                    value=name,
                    default=(name == self.selected_theme)
                ))

            select = discord.ui.Select(
                placeholder="Select a theme...",
                options=options,
                custom_id="theme_select",
                row=0
            )
            select.callback = self.theme_selected
            self.add_item(select)

        # Row 1: Create - Edit - Import - Export
        create_btn = discord.ui.Button(
            label="Create",
            emoji=theme.addIcon or None,
            style=discord.ButtonStyle.success,
            custom_id="create_theme",
            row=1
        )
        create_btn.callback = self.create_theme
        self.add_item(create_btn)

        edit_btn = discord.ui.Button(
            label="Edit",
            emoji=theme.editListIcon or None,
            style=discord.ButtonStyle.primary,
            custom_id="edit_theme",
            row=1,
            disabled=not can_modify
        )
        edit_btn.callback = self.edit_theme
        self.add_item(edit_btn)

        import_btn = discord.ui.Button(
            label="Import",
            emoji=theme.importIcon or None,
            style=discord.ButtonStyle.secondary,
            custom_id="import_theme",
            row=1
        )
        import_btn.callback = self.import_theme
        self.add_item(import_btn)

        export_btn = discord.ui.Button(
            label="Export",
            emoji=theme.exportIcon or None,
            style=discord.ButtonStyle.secondary,
            custom_id="export_theme",
            row=1,
            disabled=not self.selected_theme
        )
        export_btn.callback = self.export_theme
        self.add_item(export_btn)

        # Row 2: Set Default - Set for Server - Delete
        if self.is_global_admin:
            set_default_btn = discord.ui.Button(
                label="Set Default",
                emoji=theme.starIcon or None,
                style=discord.ButtonStyle.primary,
                custom_id="activate_theme",
                row=2,
                disabled=is_global_active
            )
            set_default_btn.callback = self.activate_theme
            self.add_item(set_default_btn)

        # Toggle button: if server has override for selected theme, show "Revert to Global" to clear it
        if is_server_active:
            revert_global_btn = discord.ui.Button(
                label="Revert to Global",
                emoji=theme.globeIcon or None,
                style=discord.ButtonStyle.secondary,
                custom_id="clear_server_theme",
                row=2
            )
            revert_global_btn.callback = self.clear_server_theme
            self.add_item(revert_global_btn)
        else:
            set_server_btn = discord.ui.Button(
                label="Apply to Server",
                emoji=theme.checkIcon or None,
                style=discord.ButtonStyle.primary,
                custom_id="set_server_theme",
                row=2
            )
            set_server_btn.callback = self.set_server_theme
            self.add_item(set_server_btn)

        delete_btn = discord.ui.Button(
            label="Delete",
            emoji=theme.trashIcon or None,
            style=discord.ButtonStyle.danger,
            custom_id="delete_theme",
            row=2,
            disabled=is_default or not can_modify
        )
        delete_btn.callback = self.delete_theme
        self.add_item(delete_btn)

        # Row 3: Share Online (Coming Soon) - Main Menu
        share_btn = discord.ui.Button(
            label="Share Online (soon)",
            emoji=theme.heartIcon or None,
            style=discord.ButtonStyle.secondary,
            custom_id="share_theme",
            row=3,
            disabled=True  # Always disabled until website ready
        )
        share_btn.callback = self.share_theme
        self.add_item(share_btn)

        back_btn = discord.ui.Button(
            label="Main Menu",
            emoji=theme.homeIcon or None,
            style=discord.ButtonStyle.secondary,
            custom_id="back_to_settings",
            row=3
        )
        back_btn.callback = self.back_to_settings
        self.add_item(back_btn)

    async def theme_selected(self, interaction: discord.Interaction):
        """Handle theme selection from dropdown."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        self.selected_theme = interaction.data['values'][0]
        self._build_components()
        await interaction.response.edit_message(view=self)

    async def edit_theme(self, interaction: discord.Interaction):
        """Open the theme editor wizard for the selected theme."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme from the dropdown first.",
                ephemeral=True
            )
            return

        # Permission check: only global admins or theme owners can edit
        if not self.is_global_admin and not self._theme_owned_by_guild(self.selected_theme, self.guild_id):
            await interaction.response.send_message(
                f"{theme.deniedIcon} You can only edit themes created on your server.",
                ephemeral=True
            )
            return

        try:
            # Import and use the new editor
            from .pimp_my_bot_editor import ThemeWizardSession, ThemeEditorHub

            session = ThemeWizardSession(self.cog, self.selected_theme, interaction.user.id, is_new=False, guild_id=self.guild_id)
            session.load_theme_data()

            hub_view = ThemeEditorHub(self.cog, session)
            embed = hub_view.build_hub_embed()

            await interaction.response.edit_message(embed=embed, view=hub_view)
        except Exception as e:
            logger.error(f"Edit theme error: {e}")
            print(f"Edit theme error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error opening theme editor: {e}",
                ephemeral=True
            )

    async def activate_theme(self, interaction: discord.Interaction):
        """Activate the selected theme globally (global admin only)."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.is_global_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only global administrators can set the global theme.",
                ephemeral=True
            )
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme first.",
                ephemeral=True
            )
            return

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                # Deactivate all themes
                cursor.execute("UPDATE pimpsettings SET is_active=0")
                # Activate selected theme
                cursor.execute("UPDATE pimpsettings SET is_active=1 WHERE themeName=?", (self.selected_theme,))
                conn.commit()

            # Reload theme - use guild theme if server has override, otherwise global
            theme.load_for_guild(self.guild_id)

            # Rebuild components and embed to reflect new active state
            self._build_components()
            embed = self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)

        except Exception as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error activating theme: {e}",
                ephemeral=True
            )

    async def set_server_theme(self, interaction: discord.Interaction):
        """Set the selected theme for this server only."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme first.",
                ephemeral=True
            )
            return

        if not self.guild_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This command must be used in a server.",
                ephemeral=True
            )
            return

        try:
            self.cog.activate_theme_for_server(self.guild_id, self.selected_theme)

            # Reload theme for this guild
            theme.load_for_guild(self.guild_id)

            # Rebuild components and embed
            self._build_components()
            embed = self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)

        except Exception as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error setting server theme: {e}",
                ephemeral=True
            )

    async def clear_server_theme(self, interaction: discord.Interaction):
        """Clear server theme override and use global theme instead."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.guild_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This command must be used in a server.",
                ephemeral=True
            )
            return

        try:
            self.cog.clear_server_theme(self.guild_id)

            # Reload theme for this guild (now falls back to global since override cleared)
            theme.load_for_guild(self.guild_id)

            # Rebuild components and embed
            self._build_components()
            embed = self.build_embed()
            await interaction.response.edit_message(embed=embed, view=self)

        except Exception as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error clearing server theme: {e}",
                ephemeral=True
            )

    async def delete_theme(self, interaction: discord.Interaction):
        """Delete the selected theme with confirmation."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme first.",
                ephemeral=True
            )
            return

        if self.selected_theme == "default":
            await interaction.response.send_message(
                f"{theme.deniedIcon} Cannot delete the default theme.",
                ephemeral=True
            )
            return

        # Check all deletion rules (global active, in-use by servers, ownership)
        can_delete, error_msg = self.cog.can_delete_theme(
            self.selected_theme,
            self.original_user_id,
            self.guild_id
        )
        if not can_delete:
            await interaction.response.send_message(
                f"{theme.deniedIcon} {error_msg}",
                ephemeral=True
            )
            return

        # Show confirmation view
        confirm_view = DeleteThemeConfirmView(self.cog, self.original_user_id, self.selected_theme, self)
        embed = discord.Embed(
            title=f"{theme.warnIcon} Confirm Delete",
            description=f"Are you sure you want to delete **{self.selected_theme}**?\n\nThis action cannot be undone.",
            color=discord.Color.red()
        )
        await interaction.response.edit_message(embed=embed, view=confirm_view)

    async def share_theme(self, interaction: discord.Interaction):
        """Share the selected theme to the online gallery."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme first.",
                ephemeral=True
            )
            return

        # Check ownership
        if not self.is_global_admin and not self._theme_owned_by_guild(self.selected_theme, self.guild_id):
            await interaction.response.send_message(
                f"{theme.deniedIcon} You can only share themes created on your server.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            result = await self.cog.share_theme_to_gallery(
                self.selected_theme,
                interaction.user.id,
                interaction.user.display_name
            )

            if result.get("success"):
                embed = discord.Embed(
                    title=f"{theme.verifiedIcon} Theme Shared!",
                    description=(
                        f"**{self.selected_theme}** has been shared to the theme gallery.\n\n"
                        f"{theme.linkIcon} [View on Gallery]({result.get('url', '')})"
                    ),
                    color=theme.emColor3
                )
                await interaction.followup.send(embed=embed, ephemeral=True)
            else:
                await interaction.followup.send(
                    f"{theme.deniedIcon} {result.get('error', 'Failed to share theme.')}",
                    ephemeral=True
                )
        except Exception as e:
            await interaction.followup.send(
                f"{theme.deniedIcon} Error sharing theme: {e}",
                ephemeral=True
            )

    async def create_theme(self, interaction: discord.Interaction):
        """Open the create theme modal with the new wizard."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        # Use the new theme basics modal from the editor
        from .pimp_my_bot_editor import ThemeBasicsModal

        modal = ThemeBasicsModal(self.cog, self.original_user_id)
        await interaction.response.send_modal(modal)

    async def export_theme(self, interaction: discord.Interaction):
        """Export the selected theme as a JSON file."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.warnIcon} Please select a theme first.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            # Get theme data
            with sqlite3.connect(THEME_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (self.selected_theme,))
                row = cursor.fetchone()

                if not row:
                    await interaction.followup.send(
                        f"{theme.deniedIcon} Theme not found.",
                        ephemeral=True
                    )
                    return

                theme_dict = dict(row)

            # Build export data
            export_data = {
                "themeName": self.selected_theme,
                "themeDescription": theme_dict.get('themeDescription', ''),
                "icons": {},
                "dividers": {},
                "colors": {}
            }

            # Export icons - preserve actual values including None/NULL from database
            for icon_name in ICON_NAMES:
                export_data["icons"][icon_name] = theme_dict.get(icon_name)

            # Export dividers
            for i in [1, 2, 3]:
                export_data["dividers"][f"divider{i}"] = {
                    "start": theme_dict.get(f'dividerStart{i}', '━'),
                    "pattern": theme_dict.get(f'dividerPattern{i}', '━'),
                    "end": theme_dict.get(f'dividerEnd{i}', '━'),
                    "length": theme_dict.get(f'dividerLength{i}', 20)
                }

            # Export colors
            export_data["colors"] = {
                "emColorString1": theme_dict.get('emColorString1', '#0000FF'),
                "emColorString2": theme_dict.get('emColorString2', '#FF0000'),
                "emColorString3": theme_dict.get('emColorString3', '#00FF00'),
                "emColorString4": theme_dict.get('emColorString4', '#FFFF00'),
                "headerColor1": theme_dict.get('headerColor1', '#1F77B4'),
                "headerColor2": theme_dict.get('headerColor2', '#28A745')
            }

            # Create JSON file
            json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
            file = discord.File(
                io.BytesIO(json_str.encode('utf-8')),
                filename=f"{self.selected_theme}_theme.json"
            )

            await interaction.followup.send(
                f"{theme.exportIcon} Theme **{self.selected_theme}** exported:",
                file=file,
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Export theme error: {e}")
            print(f"Export theme error: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error exporting theme: {e}",
                ephemeral=True
            )

    async def import_theme(self, interaction: discord.Interaction):
        """Show instructions for importing a theme."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        embed = discord.Embed(
            title=f"{theme.importIcon} Import Theme",
            description=(
                "To import a theme, send a message in this channel with:\n\n"
                "1. A `.json` theme file attached\n"
                "2. The bot will process it within 2 minutes\n\n"
                f"{theme.infoIcon} **Tip:** Use `/theme import_file:` for direct import."
            ),
            color=theme.emColor1
        )

        # Register import session
        session_key = f"import_{interaction.user.id}_{interaction.channel.id}"
        self.cog.emoji_edit_sessions[session_key] = {
            'type': 'theme_import',
            'user_id': interaction.user.id,
            'menu_view': self,
            'original_message': interaction.message
        }

        await interaction.response.send_message(embed=embed, ephemeral=True)

    async def back_to_settings(self, interaction: discord.Interaction):
        """Return to main settings menu."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        alliance_cog = interaction.client.get_cog("Alliance")
        if alliance_cog:
            await alliance_cog.show_main_menu(interaction)
        else:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Could not return to settings menu.",
                ephemeral=True
            )

class PageModal(discord.ui.Modal):
    def __init__(self, view, original_user_id):
        super().__init__(title="Go to Page")
        self.view = view
        self.original_user_id = original_user_id
        # Safely get page count with fallback
        page_count = len(getattr(self.view, 'pages', [])) or 1
        self.page_input = discord.ui.TextInput(
            label="Enter page number",
            placeholder=f"1 to {page_count}",
            required=True,
            min_length=1,
            max_length=3
        )
        self.add_item(self.page_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        # Verify view has pages attribute
        if not hasattr(self.view, 'pages') or not self.view.pages:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This view does not support page navigation.",
                ephemeral=True
            )
            return

        try:
            page_num = int(self.page_input.value) - 1
            if 0 <= page_num < len(self.view.pages):
                self.view.current_page = page_num
                self.view.update_buttons()
                await interaction.response.edit_message(embeds=self.view.pages[self.view.current_page], view=self.view)
            else:
                await interaction.response.send_message("Invalid page number.", ephemeral=True)
        except ValueError:
            await interaction.response.send_message("Please enter a valid number.", ephemeral=True)

class DeleteThemeConfirmView(discord.ui.View):
    """Confirmation view for deleting a theme."""

    def __init__(self, cog, original_user_id, theme_name, menu_view):
        super().__init__(timeout=60)
        self.cog = cog
        self.original_user_id = original_user_id
        self.theme_name = theme_name
        self.menu_view = menu_view

    @discord.ui.button(label="Delete", style=discord.ButtonStyle.danger)
    async def confirm_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM pimpsettings WHERE themeName=?", (self.theme_name,))
                conn.commit()

            # Return to menu with default selected
            self.menu_view.selected_theme = "default"
            self.menu_view._build_components()
            embed = self.menu_view.build_embed()

            await interaction.response.edit_message(embed=embed, view=self.menu_view)

        except Exception as e:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error deleting theme: {e}",
                ephemeral=True
            )

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel_delete(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        # Return to menu
        self.menu_view._build_components()
        embed = self.menu_view.build_embed()
        await interaction.response.edit_message(embed=embed, view=self.menu_view)

class CreateThemeModal(discord.ui.Modal):
    def __init__(self, cog, original_user_id, guild_id: int = None, from_menu=False):
        super().__init__(title="Create New Theme")
        self.cog = cog
        self.original_user_id = original_user_id
        self.guild_id = guild_id
        self.from_menu = from_menu

        self.theme_name_input = discord.ui.TextInput(
            label="Theme Name",
            placeholder="Enter a unique theme name",
            required=True,
            min_length=1,
            max_length=50
        )
        self.add_item(self.theme_name_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        await interaction.response.defer()
        new_theme_name = self.theme_name_input.value.strip()
        result = await self.cog.create_theme(new_theme_name, interaction.user.id, self.guild_id)

        if result["success"]:
            await self.cog.fetch_theme_info(interaction, new_theme_name, is_new_theme=True, from_menu=self.from_menu)
        else:
            await interaction.followup.send(result["error"], ephemeral=True)

class CreateThemeView(discord.ui.View):
    def __init__(self, cog, original_user_id, guild_id: int = None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.guild_id = guild_id

        create_button = discord.ui.Button(
            label="Create New Theme",
            style=discord.ButtonStyle.primary,
            emoji=theme.addIcon
        )
        create_button.callback = self.create_callback
        self.add_item(create_button)

    async def create_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        modal = CreateThemeModal(self.cog, self.original_user_id, self.guild_id)
        await interaction.response.send_modal(modal)

class DeleteThemeView(discord.ui.View):
    def __init__(self, cog, original_user_id, themename=None):
        super().__init__(timeout=7200)
        self.cog = cog
        self.original_user_id = original_user_id
        self.selected_theme = themename

        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT themeName FROM pimpsettings WHERE themeName != 'default'")
            theme_names = [row[0] for row in cursor.fetchall()]

        if theme_names:
            select_options = [
                discord.SelectOption(label=name, value=name, default=(name == themename))
                for name in theme_names
            ]
            delete_select = discord.ui.Select(
                placeholder="Select a theme to delete",
                options=select_options,
                custom_id="delete_select"
            )
            delete_select.callback = self.select_callback
            self.add_item(delete_select)

            delete_button = discord.ui.Button(
                label="Delete Theme",
                style=discord.ButtonStyle.secondary,
                emoji=theme.trashIcon
            )
            delete_button.callback = self.delete_callback
            self.add_item(delete_button)

    async def select_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        selected = interaction.data.get('values', [None])[0]
        if selected:
            self.selected_theme = selected
            await interaction.response.defer()

    async def delete_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        if not self.selected_theme:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Please select a theme from the dropdown first.",
                ephemeral=True
            )
            return

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()

                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (self.selected_theme,))
                result = cursor.fetchone()

                if result and result[0] == 1:
                    await interaction.response.send_message(
                        f"{theme.upperDivider}\n"
                        f"### {theme.warnIcon} Warning: {theme.warnIcon}\n"
                        "You cannot delete the active theme.\n\n"
                        "Make sure to activate a different theme before deleting.\n\n"
                        f"{theme.lowerDivider}\n\n",
                        ephemeral=True
                    )
                    return

                cursor.execute("DELETE FROM pimpsettings WHERE themeName=?", (self.selected_theme,))
                conn.commit()

            deleted_name = self.selected_theme
            self.selected_theme = None

            await interaction.response.send_message(
                f"{theme.verifiedIcon} Theme **{deleted_name}** has been deleted successfully!",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"Delete theme error: {e}")
            print(f"Delete theme error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error deleting theme: {e}",
                ephemeral=True
            )

class MultiFieldEditModal(discord.ui.Modal):
    """Modal for editing divider patterns or other multi-value fields."""
    def __init__(self, view, field_name, themename, original_user_id):
        super().__init__(title=f"Edit {field_name}")
        self.view = view
        self.field_name = field_name
        self.themename = themename
        self.cog = view.cog
        self.original_user_id = original_user_id
        self.inputs = []

        icons = self.cog._get_theme_data(themename)
        current_value = str(icons.get(field_name, ""))

        if field_name.startswith('emColor') or field_name.startswith('attendanceReport'):
            input_field = discord.ui.TextInput(
                label="Color Value (hex color code)",
                placeholder="#FFFFFF",
                default=current_value,
                required=True,
                max_length=7
            )
            self.inputs.append(input_field)
            self.add_item(input_field)
        elif ',' in current_value:
            values = [v.strip() for v in current_value.split(',')]
            for i, val in enumerate(values[:5]):
                input_field = discord.ui.TextInput(
                    label=f"Value {i+1}",
                    placeholder="Enter text or emoji",
                    default=val,
                    required=False,
                    max_length=200
                )
                self.inputs.append(input_field)
                self.add_item(input_field)
        else:
            input_field = discord.ui.TextInput(
                label="Value",
                placeholder="Enter value",
                default=current_value,
                required=True,
                style=discord.TextStyle.long
            )
            self.inputs.append(input_field)
            self.add_item(input_field)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        try:
            await interaction.response.defer()

            if len(self.inputs) == 1:
                new_value = self.inputs[0].value.strip()
            else:
                values = [inp.value.strip() for inp in self.inputs if inp.value.strip()]
                new_value = ','.join(values)

            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()

                column_name = None
                for col in columns_info:
                    if col[1] == self.field_name:
                        column_name = col[1]
                        break

                if column_name:
                    cursor.execute(f"UPDATE pimpsettings SET {column_name}=? WHERE themeName=?", (new_value, self.themename))
                    conn.commit()

                # Reload if active theme (use global load since this is legacy code without guild context)
                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (self.themename,))
                result = cursor.fetchone()
                if result and result[0] == 1:
                    theme.load_for_guild(None)

            # Rebuild embeds
            new_icons = self.cog._get_theme_data(self.themename)
            if self.themename == "default":
                new_lines = self.cog._build_default_theme_lines()
            else:
                new_lines = [f"{name} = {value} = \\{value}" for name, value in new_icons.items()]

            new_embeds = self.cog._build_embeds_from_lines(new_lines, self.themename)
            self.view.pages = [new_embeds[i:i+10] for i in range(0, len(new_embeds), 10)]
            self.view.all_emoji_names = [line.split(" = ")[0] for line in new_lines]
            self.view.update_buttons()

            await interaction.edit_original_response(embeds=self.view.pages[self.view.current_page], view=self.view)
            await interaction.followup.send(f"{theme.verifiedIcon} Field **{self.field_name}** updated successfully!", ephemeral=True)

        except Exception as e:
            logger.error(f"Multi-field edit error: {e}")
            print(f"Multi-field edit error: {e}")
            await interaction.followup.send(f"{theme.deniedIcon} Error: {e}", ephemeral=True)

class EditEmojiChoiceView(discord.ui.View):
    def __init__(self, pagination_view, emoji_name, current_url, themename, original_user_id):
        super().__init__(timeout=7200)
        self.pagination_view = pagination_view
        self.emoji_name = emoji_name
        self.current_url = current_url
        self.themename = themename
        self.cog = pagination_view.cog
        self.original_user_id = original_user_id

        url_button = discord.ui.Button(
            label="Enter URL",
            style=discord.ButtonStyle.primary,
            emoji=theme.exportIcon
        )
        url_button.callback = self.url_callback
        self.add_item(url_button)

    async def url_callback(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        modal = EditEmojiModal(self.pagination_view, self.emoji_name, self.current_url, self.themename, self.original_user_id)
        await interaction.response.send_modal(modal)

class EditEmojiModal(discord.ui.Modal):
    def __init__(self, view, emoji_name, current_value, themename, original_user_id):
        super().__init__(title=f"Edit {emoji_name}")
        self.view = view
        self.emoji_name = emoji_name
        self.themename = themename
        self.cog = view.cog
        self.original_user_id = original_user_id

        self.url_input = discord.ui.TextInput(
            label="Image URL",
            placeholder="Enter direct image URL (png, jpg, gif)",
            default=current_value if current_value.startswith("http") else "",
            required=True,
            style=discord.TextStyle.long
        )
        self.add_item(self.url_input)

    async def on_submit(self, interaction: discord.Interaction):
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        try:
            await interaction.response.defer()

            new_url = self.url_input.value.strip()

            if not new_url.lower().endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp')):
                await interaction.followup.send(
                    f"{theme.deniedIcon} URL must be a direct image link (.png, .jpg, .gif, .webp)",
                    ephemeral=True
                )
                return

            async def update_pagination_view():
                new_icons = self.cog._get_theme_data(self.themename)
                if self.themename == "default":
                    new_lines = self.cog._build_default_theme_lines()
                else:
                    new_lines = [f"{name} = {value} = \\{value}" for name, value in new_icons.items()]
                new_embeds = self.cog._build_embeds_from_lines(new_lines, self.themename)
                self.view.pages = [new_embeds[i:i+10] for i in range(0, len(new_embeds), 10)]
                self.view.all_emoji_names = [line.split(" = ")[0] for line in new_lines]
                self.view.update_buttons()
                await interaction.edit_original_response(embeds=self.view.pages[self.view.current_page], view=self.view)

            success = await self.cog._process_emoji_update(
                self.emoji_name,
                new_url,
                self.themename,
                update_pagination_view
            )

            if success:
                await interaction.followup.send(f"{theme.verifiedIcon} Emoji **{self.emoji_name}** updated successfully!", ephemeral=True)
            else:
                await interaction.followup.send(f"{theme.deniedIcon} Failed to update emoji. Check if the URL is accessible.", ephemeral=True)

        except Exception as e:
            logger.error(f"Edit emoji modal error: {e}")
            print(f"Edit emoji modal error: {e}")
            await interaction.followup.send(f"{theme.deniedIcon} Error: {e}", ephemeral=True)

class PaginationView(discord.ui.View):
    def __init__(self, pages, current_page, all_emoji_names, themename, cog, original_user_id, from_menu=False):
        super().__init__(timeout=7200)
        self.pages = pages
        self.current_page = current_page
        self.all_emoji_names = all_emoji_names
        self.themename = themename
        self.cog = cog
        self.original_user_id = original_user_id
        self.from_menu = from_menu
        self.update_buttons()

    def update_buttons(self):
        self.clear_items()

        prev_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="prev", emoji=theme.importIcon, row=0)
        prev_button.callback = self.prev_callback
        prev_button.disabled = self.current_page == 0
        self.add_item(prev_button)

        page_button = discord.ui.Button(label=f"{self.current_page + 1} of {len(self.pages)}", style=discord.ButtonStyle.secondary, custom_id="pages", emoji=theme.listIcon, row=0)
        page_button.callback = self.page_callback
        self.add_item(page_button)

        next_button = discord.ui.Button(label="", style=discord.ButtonStyle.secondary, custom_id="next", emoji=theme.exportIcon, row=0)
        next_button.callback = self.next_callback
        next_button.disabled = self.current_page == len(self.pages) - 1
        self.add_item(next_button)

        if self.themename != "default":
            start_idx = self.current_page * 10
            end_idx = start_idx + 10
            page_emojis = self.all_emoji_names[start_idx:end_idx]

            if page_emojis:
                icons = self.cog._get_theme_data(self.themename)
                select_options = []

                for emoji_name in page_emojis:
                    emoji_value = str(icons.get(emoji_name, ""))

                    display_emoji = None
                    emoji_pattern = r'<(a)?:([\w]+):(\d+)>'
                    emoji_match = re.search(emoji_pattern, emoji_value)

                    if emoji_match:
                        # Custom emoji - create PartialEmoji for select option
                        animated = emoji_match.group(1) == 'a'
                        name = emoji_match.group(2)
                        emoji_id = int(emoji_match.group(3))
                        display_emoji = discord.PartialEmoji(name=name, id=emoji_id, animated=animated)

                    if not display_emoji and emoji_value:
                        # Try unicode emoji
                        for char in emoji_value[:5]:
                            try:
                                cat = unicodedata.category(char)
                                if (cat in ['So', 'Sm'] or ord(char) > 0x1F000) and char not in [',', '.', '•', '━', '-', '=', '#']:
                                    display_emoji = char
                                    break
                            except (ValueError, TypeError):
                                continue

                    try:
                        if display_emoji:
                            select_options.append(discord.SelectOption(label=emoji_name, value=emoji_name, emoji=display_emoji))
                        else:
                            select_options.append(discord.SelectOption(label=emoji_name, value=emoji_name))
                    except (ValueError, TypeError):
                        # If emoji fails (e.g., inaccessible custom emoji), add without emoji
                        select_options.append(discord.SelectOption(label=emoji_name, value=emoji_name))

                emoji_select = discord.ui.Select(
                    placeholder="Select an emoji to edit",
                    options=select_options,
                    custom_id="emoji_select",
                    row=1
                )
                emoji_select.callback = self.emoji_select_callback
                self.add_item(emoji_select)

        # Add action buttons when accessed from settings menu
        if self.from_menu:
            # Check if this theme is active
            is_active = False
            try:
                with sqlite3.connect(THEME_DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (self.themename,))
                    result = cursor.fetchone()
                    is_active = result and result[0] == 1
            except Exception:
                pass

            # Activate button (disabled if already active)
            activate_btn = discord.ui.Button(
                label="Activate" if not is_active else "Active",
                emoji=theme.verifiedIcon,
                style=discord.ButtonStyle.success if not is_active else discord.ButtonStyle.secondary,
                custom_id="activate_theme",
                disabled=is_active,
                row=2
            )
            activate_btn.callback = self.activate_callback
            self.add_item(activate_btn)

            # Export button
            export_btn = discord.ui.Button(
                label="Export",
                emoji=theme.exportIcon,
                style=discord.ButtonStyle.primary,
                custom_id="export_theme",
                row=2
            )
            export_btn.callback = self.export_callback
            self.add_item(export_btn)

            # Delete button (disabled for default theme or active theme)
            can_delete = self.themename != "default" and not is_active
            delete_btn = discord.ui.Button(
                label="Delete",
                emoji=theme.trashIcon,
                style=discord.ButtonStyle.danger if can_delete else discord.ButtonStyle.secondary,
                custom_id="delete_theme",
                disabled=not can_delete,
                row=2
            )
            delete_btn.callback = self.delete_callback
            self.add_item(delete_btn)

            # Back button
            back_btn = discord.ui.Button(
                label="Back",
                emoji=f"{theme.backIcon}",
                style=discord.ButtonStyle.secondary,
                custom_id="back_to_menu",
                row=2
            )
            back_btn.callback = self.back_to_menu_callback
            self.add_item(back_btn)

    async def prev_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} Only the user who initiated this command can use this.", ephemeral=True)
            return
        self.current_page -= 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)

    async def page_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} Only the user who initiated this command can use this.", ephemeral=True)
            return
        modal = PageModal(self, self.original_user_id)
        await interaction.response.send_modal(modal)

    async def next_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} Only the user who initiated this command can use this.", ephemeral=True)
            return
        self.current_page += 1
        self.update_buttons()
        await interaction.response.edit_message(embeds=self.pages[self.current_page], view=self)

    async def emoji_select_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.original_user_id:
            await interaction.response.send_message(f"{theme.deniedIcon} Only the user who initiated this command can use this.", ephemeral=True)
            return

        selected_emoji = interaction.data['values'][0]

        if selected_emoji.startswith(('divider', 'emColor', 'attendanceReport')):
            modal = MultiFieldEditModal(self, selected_emoji, self.themename, self.original_user_id)
            await interaction.response.send_modal(modal)
            return

        icons = self.cog._get_theme_data(self.themename)
        current_value = str(icons.get(selected_emoji, ""))

        emoji_pattern = r'<a?:[\w]+:(\d+)>'
        emoji_match = re.search(emoji_pattern, current_value)

        if emoji_match:
            emoji_id = emoji_match.group(1)
            is_animated = current_value.startswith("<a:")
            emoji_ext = "gif" if is_animated else "png"
            current_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
        else:
            current_url = current_value if current_value.startswith("http") else ""

        edit_view = EditEmojiChoiceView(self, selected_emoji, current_url, self.themename, self.original_user_id)

        session_key = f"{interaction.user.id}_{interaction.channel.id}"
        self.cog.emoji_edit_sessions[session_key] = {
            'emoji_name': selected_emoji,
            'themename': self.themename,
            'pagination_view': self,
            'timeout': 300,
            'original_message': interaction.message
        }

        embed = discord.Embed(
            title=f"{theme.settingsIcon} Edit {selected_emoji}",
            description=(
                f"**Current Value:** {current_value}\n\n"
                f"**Choose how to update this emoji:**\n"
                f"{theme.exportIcon} Click the button below to enter a URL\n"
                f"{theme.importIcon} Or send in chat within 5 minutes:\n"
                f"  • An emoji (custom or unicode)\n"
                f"  • An image attachment\n"
                f"  • A direct image URL"
            ),
            color=theme.emColor2
        )

        await interaction.response.send_message(embed=embed, view=edit_view, ephemeral=True)

    async def activate_callback(self, interaction: discord.Interaction):
        """Activate this theme."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                # Deactivate all themes
                cursor.execute("UPDATE pimpsettings SET is_active=0")
                # Activate selected theme
                cursor.execute("UPDATE pimpsettings SET is_active=1 WHERE themeName=?", (self.themename,))
                conn.commit()

            # Reload theme values
            theme.load()

            # Rebuild buttons to reflect new state
            self.update_buttons()

            await interaction.response.edit_message(view=self)
            await interaction.followup.send(
                f"{theme.verifiedIcon} Theme **{self.themename}** is now active!",
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Activate theme error: {e}")
            print(f"Activate theme error: {e}")
            await interaction.response.send_message(
                f"{theme.deniedIcon} Error activating theme: {e}",
                ephemeral=True
            )

    async def export_callback(self, interaction: discord.Interaction):
        """Export this theme as JSON."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        try:
            await interaction.response.defer(ephemeral=True)

            icons = self.cog._get_theme_data(self.themename)
            if not icons:
                await interaction.followup.send(
                    f"{theme.deniedIcon} Could not export theme data.",
                    ephemeral=True
                )
                return

            json_data = json.dumps(icons, indent=2, ensure_ascii=False)
            file = discord.File(
                io.BytesIO(json_data.encode('utf-8')),
                filename=f"{self.themename}_theme.json"
            )

            await interaction.followup.send(
                f"{theme.exportIcon} Theme **{self.themename}** exported:",
                file=file,
                ephemeral=True
            )
        except Exception as e:
            logger.error(f"Export theme error: {e}")
            print(f"Export theme error: {e}")
            await interaction.followup.send(
                f"{theme.deniedIcon} Error exporting theme: {e}",
                ephemeral=True
            )

    async def delete_callback(self, interaction: discord.Interaction):
        """Delete this theme (with confirmation)."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        # Show confirmation
        embed = discord.Embed(
            title=f"{theme.warnIcon} Delete Theme",
            description=f"Are you sure you want to delete **{self.themename}**?\n\nThis action cannot be undone.",
            color=theme.emColor2
        )

        view = discord.ui.View(timeout=7200)

        confirm_btn = discord.ui.Button(
            label="Delete",
            emoji=theme.trashIcon,
            style=discord.ButtonStyle.danger
        )

        cancel_btn = discord.ui.Button(
            label="Cancel",
            emoji=f"{theme.backIcon}",
            style=discord.ButtonStyle.secondary
        )

        async def confirm_delete(btn_interaction: discord.Interaction):
            if not await check_interaction_user(btn_interaction, self.original_user_id):
                return

            try:
                with sqlite3.connect(THEME_DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM pimpsettings WHERE themeName=?", (self.themename,))
                    conn.commit()

                # Go back to theme menu
                await self.cog.show_theme_menu(btn_interaction)
                await btn_interaction.followup.send(
                    f"{theme.verifiedIcon} Theme **{self.themename}** has been deleted.",
                    ephemeral=True
                )
            except Exception as e:
                logger.error(f"Delete theme error: {e}")
                print(f"Delete theme error: {e}")
                await btn_interaction.response.send_message(
                    f"{theme.deniedIcon} Error deleting theme: {e}",
                    ephemeral=True
                )

        async def cancel_delete(btn_interaction: discord.Interaction):
            if not await check_interaction_user(btn_interaction, self.original_user_id):
                return
            # Go back to the pagination view
            await btn_interaction.response.edit_message(
                embeds=self.pages[self.current_page],
                view=self
            )

        confirm_btn.callback = confirm_delete
        cancel_btn.callback = cancel_delete

        view.add_item(confirm_btn)
        view.add_item(cancel_btn)

        await interaction.response.edit_message(embed=embed, view=view)

    async def back_to_menu_callback(self, interaction: discord.Interaction):
        """Return to theme menu."""
        if not await check_interaction_user(interaction, self.original_user_id):
            return

        await self.cog.show_theme_menu(interaction)

class Theme(commands.Cog):
    """Cog for managing bot themes via Discord commands."""

    # Command group for theme management - defined at class level
    pimp_group = discord.app_commands.Group(name='pimp', description='Manage bot themes')

    def __init__(self, bot):
        self.bot = bot
        self.emoji_edit_sessions = {}

    @commands.Cog.listener()
    async def on_ready(self):
        """Set bot reference for emoji validation once bot is fully ready."""
        await theme.set_bot(self.bot)

    def can_delete_theme(self, theme_name: str, user_id: int, guild_id: int) -> Tuple[bool, str]:
        """
        Check if theme can be deleted. Returns (can_delete, error_message).

        Rules:
        - Cannot delete if ANY server is using this theme
        - Cannot delete if it's the global active theme
        - Non-global admins can only delete themes created on their server
        """
        is_admin, is_global = PermissionManager.is_admin(user_id)

        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()

            # Check if theme is the global active theme
            cursor.execute(
                "SELECT is_active FROM pimpsettings WHERE themeName = ?",
                (theme_name,)
            )
            result = cursor.fetchone()
            if result and result[0] == 1:
                return False, "Cannot delete the global active theme. Activate a different theme globally first."

            # Check if ANY server is using this theme
            cursor.execute(
                "SELECT guild_id FROM server_themes WHERE theme_name = ?",
                (theme_name,)
            )
            servers_using = cursor.fetchall()
            if servers_using:
                server_count = len(servers_using)
                return False, f"Cannot delete - {server_count} server(s) are using this theme. They must switch to a different theme first."

            # Check ownership (skip for global admins)
            if not is_global:
                cursor.execute(
                    "SELECT created_guild_id FROM pimpsettings WHERE themeName = ?",
                    (theme_name,)
                )
                result = cursor.fetchone()
                if result and result[0] and result[0] != guild_id:
                    return False, "You can only delete themes created on your server."

        return True, ""

    def activate_theme_for_server(self, guild_id: int, theme_name: str):
        """Set theme override for a specific server."""
        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO server_themes (guild_id, theme_name)
                VALUES (?, ?)
            """, (guild_id, theme_name))
            conn.commit()

    def clear_server_theme(self, guild_id: int):
        """Remove server-specific theme override (use global theme instead)."""
        with sqlite3.connect(THEME_DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM server_themes WHERE guild_id = ?", (guild_id,))
            conn.commit()

    async def share_theme_to_gallery(self, theme_name: str, author_discord_id: int, author_username: str) -> dict:
        """Share a theme to the online gallery. Returns dict with success/error/url."""
        if not THEME_GALLERY_URL or not THEME_GALLERY_API_KEY:
            return {"success": False, "error": "Theme gallery not configured."}

        try:
            # Get theme data
            with sqlite3.connect(THEME_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (theme_name,))
                row = cursor.fetchone()

                if not row:
                    return {"success": False, "error": f"Theme '{theme_name}' not found."}

                theme_dict = dict(row)

            # Build theme data for upload
            icons = {}
            for icon_name in ICON_NAMES:
                icons[icon_name] = theme_dict.get(icon_name)

            dividers = {}
            for i in [1, 2, 3]:
                dividers[f"divider{i}"] = {
                    "start": theme_dict.get(f'dividerStart{i}', ''),
                    "pattern": theme_dict.get(f'dividerPattern{i}', ''),
                    "end": theme_dict.get(f'dividerEnd{i}', ''),
                    "length": theme_dict.get(f'dividerLength{i}', 20)
                }

            colors = {
                "emColorString1": theme_dict.get('emColorString1', '#0000FF'),
                "emColorString2": theme_dict.get('emColorString2', '#FF0000'),
                "emColorString3": theme_dict.get('emColorString3', '#00FF00'),
                "emColorString4": theme_dict.get('emColorString4', '#FFFF00'),
                "headerColor1": theme_dict.get('headerColor1', '#1F77B4'),
                "headerColor2": theme_dict.get('headerColor2', '#28A745')
            }

            upload_data = {
                "themeName": theme_name,
                "themeDescription": theme_dict.get('themeDescription', ''),
                "icons": icons,
                "dividers": dividers,
                "colors": colors,
                "author_discord_id": author_discord_id,
                "author_username": author_username
            }

            # Upload to gallery
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{THEME_GALLERY_URL.rstrip('/')}/api/bot/themes",
                    json=upload_data,
                    headers={
                        "X-API-Key": THEME_GALLERY_API_KEY,
                        "Content-Type": "application/json"
                    }
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        theme_uuid = data.get("uuid", "")
                        return {
                            "success": True,
                            "url": f"{THEME_GALLERY_URL.rstrip('/')}/theme/{theme_uuid}"
                        }
                    else:
                        error_text = await resp.text()
                        return {"success": False, "error": f"Gallery API error: {error_text}"}

        except aiohttp.ClientError as e:
            return {"success": False, "error": f"Connection error: {e}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _process_emoji_update(self, emoji_name, new_url, themename, view_update_callback=None):
        """
        Process emoji update from URL, uploaded image, or unicode emoji.

        Args:
            emoji_name: Name of the icon to update
            new_url: URL of new image, or emoji string for unicode emoji
            themename: Name of theme to update
            view_update_callback: Optional async callback to update the view after save
        """
        try:
            from PIL import Image
            PIL_AVAILABLE = True
        except ImportError:
            PIL_AVAILABLE = False

        try:
            is_unicode_emoji = new_url and not new_url.startswith('http')

            if is_unicode_emoji:
                new_emoji_str = new_url
            else:
                app_id = self.bot.application_id
                bot_token = self.bot.http.token

                async with aiohttp.ClientSession() as session:
                    async with session.get(new_url) as resp:
                        if resp.status != 200:
                            return False
                        image_data = await resp.read()

                        max_size = 2048 * 1024
                        if len(image_data) > max_size:
                            if not PIL_AVAILABLE:
                                logger.error(f"Image too large ({len(image_data)} bytes) and PIL not available for resizing")
                                print(f"[ERROR] Image too large ({len(image_data)} bytes) and PIL not available for resizing")
                                return False

                            try:
                                img = Image.open(io.BytesIO(image_data))
                                max_dimension = 256
                                ratio = min(max_dimension / img.width, max_dimension / img.height)
                                new_size = (int(img.width * ratio), int(img.height * ratio))
                                img = img.resize(new_size, Image.Resampling.LANCZOS)

                                img_byte_arr = io.BytesIO()
                                img_format = 'PNG' if not new_url.lower().endswith('.gif') else 'GIF'
                                img.save(img_byte_arr, format=img_format, optimize=True)
                                image_data = img_byte_arr.getvalue()
                            except Exception as e:
                                logger.error(f"Image resizing failed: {e}")
                                print(f"[ERROR] Image resizing failed: {e}")
                                return False

                    icons = self._get_theme_data(themename)
                    old_value = icons.get(emoji_name, "")

                    emoji_pattern = r'<a?:(\w+):(\d+)>'
                    old_emoji_match = re.search(emoji_pattern, str(old_value))

                    # Discord emoji names: 2-32 chars, lowercase alphanumeric + underscores only
                    base_name = re.sub(r'[^a-z0-9]', '_', emoji_name.lower())
                    safe_theme = re.sub(r'[^a-z0-9]', '_', themename.lower())
                    # Remove consecutive underscores and trim
                    new_emoji_name = re.sub(r'_+', '_', f"{safe_theme}_{base_name}").strip('_')[:32]
                    # Ensure at least 2 chars
                    if len(new_emoji_name) < 2:
                        new_emoji_name = f"em_{new_emoji_name}"[:32]

                    if old_emoji_match:
                        old_emoji_id = old_emoji_match.group(2)
                        delete_url = f"https://discord.com/api/v10/applications/{app_id}/emojis/{old_emoji_id}"
                        headers = {"Authorization": f"Bot {bot_token}"}
                        try:
                            async with session.delete(delete_url, headers=headers):
                                pass
                        except Exception:
                            pass

                    if new_url.lower().endswith('.gif'):
                        mime_type = "image/gif"
                    elif new_url.lower().endswith(('.jpg', '.jpeg')):
                        mime_type = "image/jpeg"
                    else:
                        mime_type = "image/png"

                    image_base64 = base64.b64encode(image_data).decode('utf-8')
                    data_uri = f"data:{mime_type};base64,{image_base64}"

                    upload_url = f"https://discord.com/api/v10/applications/{app_id}/emojis"
                    headers = {"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"}
                    payload = {"name": new_emoji_name, "image": data_uri}

                    async with session.post(upload_url, headers=headers, json=payload) as resp:
                        if resp.status == 201:
                            result = await resp.json()
                            new_emoji_id = result['id']
                            is_animated = result.get('animated', False)
                            emoji_prefix = "a" if is_animated else ""
                            new_emoji_str = f"<{emoji_prefix}:{new_emoji_name}:{new_emoji_id}>"
                        else:
                            error_text = await resp.text()
                            logger.error(f"Emoji upload failed ({resp.status}): {error_text}")
                            print(f"Emoji upload failed ({resp.status}): {error_text}")
                            return False

            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                if emoji_name in ICON_NAMES:
                    cursor.execute(f"UPDATE pimpsettings SET {emoji_name}=? WHERE themeName=?", (new_emoji_str, themename))
                    conn.commit()

                # Check if this is the active theme and reload if so
                cursor.execute("SELECT is_active FROM pimpsettings WHERE themeName=?", (themename,))
                result = cursor.fetchone()
                if result and result[0] == 1:
                    theme.load()

            # Call view update callback if provided
            if view_update_callback:
                try:
                    await view_update_callback()
                except Exception:
                    pass

            return True

        except Exception as e:
            logger.error(f"Edit emoji error: {e}")
            print(f"Edit emoji error: {e}")
            return False

    async def _check_admin(self, interaction: discord.Interaction) -> bool:
        """Check if user is admin. Returns True if allowed, False if not."""
        is_admin, _ = PermissionManager.is_admin(interaction.user.id)
        if not is_admin:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only administrators can use this command.",
                ephemeral=True
            )
            return False
        return True

    @pimp_group.command(name='menu', description='Open the interactive theme menu')
    async def pimp_menu(self, interaction: discord.Interaction):
        """Open the interactive theme management menu."""
        if not await self._check_admin(interaction):
            return
        guild_id = interaction.guild_id if interaction.guild else None
        view = ThemeMenuView(self, interaction.user.id, guild_id)
        embed = view.build_embed()
        await interaction.response.send_message(embed=embed, view=view)

    @pimp_group.command(name='view', description='View a theme\'s icons and settings')
    @discord.app_commands.describe(theme='The theme to view')
    async def pimp_view(self, interaction: discord.Interaction, theme: str):
        """View detailed theme information."""
        if not await self._check_admin(interaction):
            return
        await self.fetch_theme_info(interaction, theme)

    @pimp_view.autocomplete('theme')
    async def pimp_view_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='create', description='Create a new theme from default template')
    @discord.app_commands.describe(name='Name for the new theme')
    async def pimp_create(self, interaction: discord.Interaction, name: str):
        """Create a new theme with the given name."""
        if not await self._check_admin(interaction):
            return
        await self.show_create_theme(interaction, name)

    @pimp_group.command(name='edit', description='Open the theme editor')
    @discord.app_commands.describe(theme='The theme to edit')
    async def pimp_edit(self, interaction: discord.Interaction, theme: str):
        """Open the theme editor for a specific theme."""
        if not await self._check_admin(interaction):
            return
        await self.fetch_theme_info(interaction, theme, from_menu=True)

    @pimp_edit.autocomplete('theme')
    async def pimp_edit_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='set', description='Set a theme for this server only')
    @discord.app_commands.describe(theme_name='The theme to use for this server')
    async def pimp_set(self, interaction: discord.Interaction, theme_name: str):
        """Set a theme override for this server."""
        if not await self._check_admin(interaction):
            return
        guild_id = interaction.guild_id if interaction.guild else None
        if not guild_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This command can only be used in a server.",
                ephemeral=True
            )
            return

        # Verify theme exists
        icons = self._get_theme_data(theme_name)
        if not icons:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Theme '{theme_name}' not found.",
                ephemeral=True
            )
            return

        self.activate_theme_for_server(guild_id, theme_name)
        await interaction.response.send_message(
            f"{theme.verifiedIcon} Theme **{theme_name}** is now active for this server.",
            ephemeral=True
        )

    @pimp_set.autocomplete('theme_name')
    async def pimp_set_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='global', description='Set the global active theme (Global Admin only)')
    @discord.app_commands.describe(theme_name='The theme to activate globally')
    async def pimp_global(self, interaction: discord.Interaction, theme_name: str):
        """Set the global active theme. Requires Global Admin."""
        is_admin, is_global = PermissionManager.is_admin(interaction.user.id)
        if not is_admin or not is_global:
            await interaction.response.send_message(
                f"{theme.deniedIcon} Only global administrators can set the global theme.",
                ephemeral=True
            )
            return
        await self.activate_theme(interaction, theme_name)

    @pimp_global.autocomplete('theme_name')
    async def pimp_global_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='export', description='Export a theme as JSON file')
    @discord.app_commands.describe(theme='The theme to export')
    async def pimp_export(self, interaction: discord.Interaction, theme: str):
        """Export a theme to a JSON file."""
        if not await self._check_admin(interaction):
            return
        await self.export_theme(interaction, theme)

    @pimp_export.autocomplete('theme')
    async def pimp_export_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='import', description='Import a theme from JSON file')
    @discord.app_commands.describe(file='JSON file containing the theme data')
    async def pimp_import(self, interaction: discord.Interaction, file: discord.Attachment):
        """Import a theme from a JSON file."""
        if not await self._check_admin(interaction):
            return
        await self.import_theme(interaction, file)

    @pimp_group.command(name='delete', description='Delete a theme')
    @discord.app_commands.describe(theme='The theme to delete')
    async def pimp_delete(self, interaction: discord.Interaction, theme: str):
        """Delete a theme (with restrictions)."""
        if not await self._check_admin(interaction):
            return
        await self.show_delete_theme(interaction, theme)

    @pimp_delete.autocomplete('theme')
    async def pimp_delete_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    @pimp_group.command(name='clear', description='Clear server theme override and use global theme')
    async def pimp_clear(self, interaction: discord.Interaction):
        """Clear server-specific theme and use global theme instead."""
        if not await self._check_admin(interaction):
            return
        guild_id = interaction.guild_id if interaction.guild else None
        if not guild_id:
            await interaction.response.send_message(
                f"{theme.deniedIcon} This command can only be used in a server.",
                ephemeral=True
            )
            return

        self.clear_server_theme(guild_id)
        await interaction.response.send_message(
            f"{theme.verifiedIcon} Server theme cleared. Now using global theme.",
            ephemeral=True
        )

    @pimp_group.command(name='share', description='Share a theme to the online gallery')
    @discord.app_commands.describe(theme_name='The theme to share')
    async def pimp_share(self, interaction: discord.Interaction, theme_name: str):
        """Share a theme to the community gallery website."""
        if not await self._check_admin(interaction):
            return

        guild_id = interaction.guild_id if interaction.guild else None
        _, is_global = PermissionManager.is_admin(interaction.user.id)

        # Check if user owns this theme (or is global admin)
        if not is_global:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT created_guild_id FROM pimpsettings WHERE themeName=?", (theme_name,))
                result = cursor.fetchone()

                if not result:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} Theme '{theme_name}' not found.",
                        ephemeral=True
                    )
                    return

                if result[0] != guild_id:
                    await interaction.response.send_message(
                        f"{theme.deniedIcon} You can only share themes created on your server.",
                        ephemeral=True
                    )
                    return

        await interaction.response.defer(ephemeral=True)

        result = await self.share_theme_to_gallery(
            theme_name,
            interaction.user.id,
            interaction.user.display_name
        )

        if result.get("success"):
            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Theme Shared!",
                description=(
                    f"**{theme_name}** has been shared to the theme gallery.\n\n"
                    f"{theme.linkIcon} [View on Gallery]({result.get('url', '')})"
                ),
                color=theme.emColor3
            )
            await interaction.followup.send(embed=embed, ephemeral=True)
        else:
            await interaction.followup.send(
                f"{theme.deniedIcon} {result.get('error', 'Failed to share theme.')}",
                ephemeral=True
            )

    @pimp_share.autocomplete('theme_name')
    async def pimp_share_autocomplete(self, interaction: discord.Interaction, current: str):
        return await self._theme_autocomplete(current)

    async def _theme_autocomplete(self, current: str):
        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT themename FROM pimpsettings")
                themes = [row[0] for row in cursor.fetchall()]

            choices = [discord.app_commands.Choice(name=t, value=t) for t in themes]
            if current:
                choices = [c for c in choices if current.lower() in c.name.lower()][:25]
            else:
                choices = choices[:25]
            return choices
        except Exception:
            return []

    def _get_theme_data(self, themename: str):
        """Fetch theme data from database."""
        with sqlite3.connect(THEME_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (themename,))
            theme_row = cursor.fetchone()

            if not theme_row:
                return None

            theme_dict = dict(theme_row)

            # Get all icons by name
            icons = {name: theme_dict.get(name) or DEFAULT_EMOJI for name in ICON_NAMES}

            # Add divider settings
            icons.update({
                'dividerEmojiStart1': theme_dict.get('dividerStart1') or "━",
                'dividerEmojiPattern1': theme_dict.get('dividerPattern1') or "━",
                'dividerEmojiEnd1': theme_dict.get('dividerEnd1') or "━",
                'dividerLength1': theme_dict.get('dividerLength1') or 20,
                'dividerEmojiStart2': theme_dict.get('dividerStart2') or "━",
                'dividerEmojiPattern2': theme_dict.get('dividerPattern2') or "━",
                'dividerEmojiEnd2': theme_dict.get('dividerEnd2') or "━",
                'dividerLength2': theme_dict.get('dividerLength2') or 20,
                'emColorString1': theme_dict.get('emColorString1') or "#0000FF",
                'emColorString2': theme_dict.get('emColorString2') or "#FF0000",
                'emColorString3': theme_dict.get('emColorString3') or "#00FF00",
                'emColorString4': theme_dict.get('emColorString4') or "#FFFF00",
                'headerColor1': theme_dict.get('headerColor1') or "#1F77B4",
                'headerColor2': theme_dict.get('headerColor2') or "#28A745",
            })

            return icons

    def _build_default_theme_lines(self):
        """Build default theme lines with Twemoji CDN URLs."""
        default_icons = {
            'allianceOldIcon': '2694', 'avatarOldIcon': '1f464', 'stoveOldIcon': '1f525', 'stateOldIcon': '1f30f',
            'allianceIcon': '2694', 'avatarIcon': '1f464', 'stoveIcon': '1f525', 'stateIcon': '1f30f',
            'listIcon': '1f4dc', 'fidIcon': '1f194', 'timeIcon': '1f570', 'homeIcon': '1f3e0',
            'num1Icon': '31-20e3', 'num2Icon': '32-20e3', 'num3Icon': '33-20e3', 'num4Icon': '34-20e3',
            'num5Icon': '35-20e3', 'num10Icon': '1f51f', 'newIcon': '1f195', 'pinIcon': '1f4cd',
            'saveIcon': '1f4be', 'robotIcon': '1f916', 'crossIcon': '2694', 'heartIcon': '1f497',
            'shieldIcon': '1f6e1', 'targetIcon': '1f3af', 'redeemIcon': '1f503', 'membersIcon': '1f465',
            'averageIcon': '1f4c8', 'messageIcon': '1f50a', 'supportIcon': '1f198', 'foundryIcon': '1f3ed',
            'announceIcon': '1f4e2', 'ministerIcon': '1f3db', 'researchIcon': '1f52c', 'trainingIcon': '2694',
            'crazyJoeIcon': '1f92a', 'bearTrapIcon': '1f43b', 'calendarIcon': '1f4c5', 'editListIcon': '1f4dd',
            'settingsIcon': '2699', 'hourglassIcon': '23f3', 'messageNoIcon': '1f507', 'blankListIcon': '26AA',
            'alarmClockIcon': '23f0', 'magnifyingIcon': '1f50d', 'frostdragonIcon': '1f409', 'canyonClashIcon': '1f3de',
            'constructionIcon': '1f528', 'castleBattleIcon': '1f3f0',
            'giftIcon': '1f381', 'giftsIcon': '1f6cd', 'giftAddIcon': '2795', 'giftAlarmIcon': '2795',
            'gifAlertIcon': '26a0', 'giftCheckIcon': '2705', 'giftTotalIcon': '1f7f0', 'giftDeleteIcon': '1f5d1',
            'giftHashtagIcon': '0023-fe0f-20e3', 'giftSettingsIcon': '2699',
            'processingIcon': '1f504', 'verifiedIcon': '2705', 'questionIcon': '2753', 'transferIcon': '2194',
            'multiplyIcon': '2716', 'divideIcon': '2797', 'deniedIcon': '274c', 'deleteIcon': '2796',
            'exportIcon': '27a1', 'importIcon': '2b05', 'retryIcon': '1f501', 'totalIcon': '1f7f0',
            'infoIcon': '2139', 'warnIcon': '26a0', 'addIcon': '2795',
            'prevIcon': '25c0', 'nextIcon': '25b6', 'backIcon': '2b05', 'forwardIcon': '27a1',
            'minusIcon': '2796', 'chartIcon': '1f4ca', 'documentIcon': '1f4c4', 'eyeIcon': '1f441',
            'globeIcon': '1f30d', 'wizardIcon': '1f9d9', 'muteIcon': '1f515',
            'shutdownZzzIcon': '1f4a4', 'shutdownDoorIcon': '1f6aa', 'shutdownHandIcon': '1f44b',
            'shutdownMoonIcon': '1f319', 'shutdownPlugIcon': '1f50c', 'shutdownStopIcon': '1f6d1',
            'shutdownClapperIcon': '1f3ac', 'shutdownSparkleIcon': '2728'
        }

        cdn_base = "https://cdn.jsdelivr.net/gh/twitter/twemoji@latest/assets/72x72/"
        lines = [f"{name} = {cdn_base}{code}.png" for name, code in default_icons.items()]

        lines.extend([
            "dividerEmojiStart1 = ━", "dividerEmojiPattern1 = ━", "dividerEmojiEnd1 = ━",
            "dividerLength1 = 9", "dividerEmojiStart2 = ━", "dividerEmojiPattern2 = ━",
            "dividerEmojiEnd2 = ━", "dividerLength2 = 9",
            "emColorString1 = #0000FF", "emColorString2 = #FF0000", "emColorString3 = #00FF00", "emColorString4 = #FFFF00",
            "headerColor1 = #FFFFFF", "headerColor2 = #FFFFFF",
        ])

        return lines

    def _build_embeds_from_lines(self, lines, themename):
        """Build embeds from theme lines."""
        embeds = []
        for line in lines:
            parts = line.split(" = ")
            name = parts[0]

            if len(parts) >= 2:
                if themename == "default" and len(parts) == 2:
                    value = parts[1]
                    if value.startswith("http"):
                        embed = discord.Embed(title=name, description=f"[{name} - Link]({value})", color=theme.emColor3)
                        embed.set_thumbnail(url=value)
                    elif value.startswith('#'):
                        color_url = f"https://www.colorhexa.com/{value.strip('#')}.png"
                        embed = discord.Embed(title=name, description=f"[{value}]({color_url})", color=theme.emColor3)
                        embed.set_thumbnail(url=color_url)
                    else:
                        embed = discord.Embed(title=name, description=f"`{value}`", color=theme.emColor3)
                elif len(parts) == 3:
                    emoji_display = parts[2].strip('\\')
                    emoji_pattern = r'<a?:(\w+):(\d+)>'
                    emoji_matches = re.findall(emoji_pattern, emoji_display)

                    if emoji_matches:
                        description = f"{emoji_display}\n\n**Emoji Link(s):**\n"
                        for emoji_name, emoji_id in emoji_matches:
                            is_animated = f"<a:{emoji_name}:{emoji_id}>" in emoji_display
                            emoji_ext = "gif" if is_animated else "png"
                            emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
                            description += f"• [{emoji_name}]({emoji_url})\n"
                        embed = discord.Embed(title=name, description=description, color=theme.emColor3)
                        if emoji_matches:
                            is_animated = f"<a:{emoji_matches[0][0]}:{emoji_matches[0][1]}>" in emoji_display
                            emoji_ext = "gif" if is_animated else "png"
                            embed.set_thumbnail(url=f"https://cdn.discordapp.com/emojis/{emoji_matches[0][1]}.{emoji_ext}")
                    elif emoji_display.startswith("http"):
                        embed = discord.Embed(title=name, description=f"[{name} - Link]({emoji_display})", color=theme.emColor3)
                        embed.set_thumbnail(url=emoji_display)
                    elif emoji_display.startswith('#'):
                        color_url = f"https://www.colorhexa.com/{emoji_display.strip('#')}.png"
                        embed = discord.Embed(title=name, description=f"[{emoji_display}]({color_url})", color=theme.emColor3)
                        embed.set_thumbnail(url=color_url)
                    else:
                        embed = discord.Embed(title=name, description=f"{emoji_display}", color=theme.emColor3)
                else:
                    if len(parts) > 1 and parts[1].startswith('http'):
                        embed = discord.Embed(title=name, description=f"[{name} - Link]({parts[1]})", color=theme.emColor3)
                        embed.set_thumbnail(url=parts[1])
                    elif len(parts) > 1 and parts[1].startswith('#'):
                        color_url = f"https://www.colorhexa.com/{parts[1].strip('#')}.png"
                        embed = discord.Embed(title=name, description=f"[{parts[1]}]({color_url})", color=theme.emColor3)
                        embed.set_thumbnail(url=color_url)
                    elif len(parts) > 1:
                        embed = discord.Embed(title=name, description=f"{parts[1]}", color=theme.emColor3)
                    else:
                        continue

                embeds.append(embed)

        return embeds

    async def create_theme(self, new_theme_name: str, creator_id: int, guild_id: int = None) -> dict:
        """Create a new theme directly with the given name. Returns dict with success/error."""
        new_theme_name = new_theme_name.strip()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (new_theme_name,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    return {"success": False, "error": f"{theme.deniedIcon} A theme with the name **{new_theme_name}** already exists!"}

                # Get column structure - exclude metadata columns we handle separately
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                exclude_cols = ['id', 'themeName', 'themeCreator', 'is_active', 'themeDescription', 'createdAt', 'created_guild_id']
                data_columns = [col[1] for col in columns_info if col[1] not in exclude_cols]

                # Copy values from default theme to ensure all columns are covered
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName='default'")
                default_row = cursor.fetchone()
                if not default_row:
                    return {"success": False, "error": f"{theme.deniedIcon} Default theme not found!"}

                # Get column names to map values
                cursor.execute("PRAGMA table_info(pimpsettings)")
                all_columns = [col[1] for col in cursor.fetchall()]
                default_dict = dict(zip(all_columns, default_row))

                # Extract values for data columns from default theme
                all_values = [default_dict.get(col) for col in data_columns]

                placeholders = ', '.join(['?' for _ in range(len(data_columns))])
                columns_str = ', '.join(data_columns)
                query = f"INSERT INTO pimpsettings (themeName, themeCreator, {columns_str}, is_active, created_guild_id) VALUES (?, ?, {placeholders}, 0, ?)"
                cursor.execute(query, [new_theme_name, creator_id] + all_values + [guild_id])
                conn.commit()

            return {"success": True}
        except Exception as e:
            logger.error(f"Create theme error: {e}")
            print(f"Create theme error: {e}")
            return {"success": False, "error": f"{theme.deniedIcon} Error creating theme: {e}"}

    async def create_theme_with_metadata(self, new_theme_name: str, creator_id: int, description: str = "", guild_id: int = None) -> dict:
        """Create a new theme with description and timestamp. Returns dict with success/error."""
        from datetime import datetime

        new_theme_name = new_theme_name.strip()

        try:
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (new_theme_name,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    return {"success": False, "error": f"{theme.deniedIcon} A theme with the name **{new_theme_name}** already exists!"}

                # Get column structure - exclude metadata columns we handle separately
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                exclude_cols = ['id', 'themeName', 'themeCreator', 'is_active', 'themeDescription', 'createdAt', 'created_guild_id']
                data_columns = [col[1] for col in columns_info if col[1] not in exclude_cols]

                # Copy values from default theme to ensure all columns are covered
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName='default'")
                default_row = cursor.fetchone()
                if not default_row:
                    return {"success": False, "error": f"{theme.deniedIcon} Default theme not found!"}

                # Get column names to map values
                cursor.execute("PRAGMA table_info(pimpsettings)")
                all_columns = [col[1] for col in cursor.fetchall()]
                default_dict = dict(zip(all_columns, default_row))

                # Extract values for data columns from default theme
                all_values = [default_dict.get(col) for col in data_columns]
                created_at = datetime.now().strftime("%Y-%m-%d %H:%M")

                placeholders = ', '.join(['?' for _ in range(len(data_columns))])
                columns_str = ', '.join(data_columns)
                query = f"""INSERT INTO pimpsettings
                    (themeName, themeCreator, {columns_str}, is_active, themeDescription, createdAt, created_guild_id)
                    VALUES (?, ?, {placeholders}, 0, ?, ?, ?)"""
                cursor.execute(query, [new_theme_name, creator_id] + all_values + [description, created_at, guild_id])
                conn.commit()

            return {"success": True}
        except Exception as e:
            logger.error(f"Create theme with metadata error: {e}")
            print(f"Create theme with metadata error: {e}")
            return {"success": False, "error": f"{theme.deniedIcon} Error creating theme: {e}"}

    async def show_create_theme(self, interaction: discord.Interaction, themename: str = None):
        # If themename provided via slash command, create directly without modal
        guild_id = interaction.guild_id if interaction.guild else None
        if themename:
            await interaction.response.defer()
            result = await self.create_theme(themename, interaction.user.id, guild_id)
            if result["success"]:
                await self.fetch_theme_info(interaction, themename, is_new_theme=True)
            else:
                await interaction.followup.send(result["error"], ephemeral=True)
            return

        # Otherwise show the create button which opens modal
        view = CreateThemeView(self, interaction.user.id, guild_id)
        embed = discord.Embed(
            title=f"{theme.addIcon} Create New Theme",
            description=(
                "Click the button below to create a new theme.\n\n"
                f"{theme.infoIcon} You'll be able to:\n"
                "- Name your theme\n"
                "- Start with default emojis\n"
                "- Edit each emoji individually"
            ),
            color=theme.emColor1
        )
        await interaction.response.send_message(embed=embed, view=view)

    async def show_delete_theme(self, interaction: discord.Interaction, themename: str = None):
        view = DeleteThemeView(self, interaction.user.id, themename)
        embed = discord.Embed(
            title=f"{theme.trashIcon} Delete Theme",
            description=(
                f"{theme.upperDivider}\n"
                f"### {theme.warnIcon} Warning: {theme.warnIcon}\n"
                "You cannot delete the active theme.\n"
                "Make sure to activate a different theme before deleting.\n"
                f"{theme.lowerDivider}"
            ),
            color=theme.emColor2
        )
        await interaction.response.send_message(embed=embed, view=view)

    async def activate_theme(self, interaction: discord.Interaction, themename: str):
        try:
            await interaction.response.defer(thinking=True)

            icons = self._get_theme_data(themename)
            if not icons:
                await interaction.followup.send(f"{theme.deniedIcon} Theme '{themename}' not found.")
                return

            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("UPDATE pimpsettings SET is_active=0")
                cursor.execute("UPDATE pimpsettings SET is_active=1 WHERE themeName=?", (themename,))
                conn.commit()

            # Reload theme - use guild theme if server has override, otherwise global
            guild_id = interaction.guild.id if interaction.guild else None
            theme.load_for_guild(guild_id)

            embed = discord.Embed(
                title=f"{theme.verifiedIcon} Theme Activated",
                description=f"**Theme:** {themename}\n**Status:** Active and loaded\n\nThe bot is now using this theme.",
                color=theme.emColor3
            )
            await interaction.followup.send(embed=embed)

        except Exception as e:
            await interaction.followup.send(f"{theme.deniedIcon} Error activating theme: {e}")

    async def export_theme(self, interaction: discord.Interaction, themename: str):
        """Export a theme as JSON file."""
        try:
            await interaction.response.defer(thinking=True)

            with sqlite3.connect(THEME_DB_PATH) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM pimpsettings WHERE themeName=?", (themename,))
                row = cursor.fetchone()

                if not row:
                    await interaction.followup.send(f"{theme.deniedIcon} Theme '{themename}' not found.")
                    return

                theme_dict = dict(row)

            export_data = {
                "themeName": themename,
                "themeDescription": theme_dict.get('themeDescription', ''),
                "icons": {},
                "dividers": {},
                "colors": {}
            }

            for icon_name in ICON_NAMES:
                export_data["icons"][icon_name] = theme_dict.get(icon_name)

            for i in [1, 2, 3]:
                export_data["dividers"][f"divider{i}"] = {
                    "start": theme_dict.get(f'dividerStart{i}', '━'),
                    "pattern": theme_dict.get(f'dividerPattern{i}', '━'),
                    "end": theme_dict.get(f'dividerEnd{i}', '━'),
                    "length": theme_dict.get(f'dividerLength{i}', 20)
                }

            export_data["colors"] = {
                "emColorString1": theme_dict.get('emColorString1', '#0000FF'),
                "emColorString2": theme_dict.get('emColorString2', '#FF0000'),
                "emColorString3": theme_dict.get('emColorString3', '#00FF00'),
                "emColorString4": theme_dict.get('emColorString4', '#FFFF00'),
                "headerColor1": theme_dict.get('headerColor1', '#1F77B4'),
                "headerColor2": theme_dict.get('headerColor2', '#28A745')
            }

            json_str = json.dumps(export_data, indent=2, ensure_ascii=False)
            file = discord.File(
                io.BytesIO(json_str.encode('utf-8')),
                filename=f"{themename}_theme.json"
            )

            embed = discord.Embed(
                title=f"{theme.exportIcon} Theme Exported",
                description=f"**Theme:** {themename}\n**Icons:** {len(export_data['icons'])}",
                color=theme.emColor1
            )
            await interaction.followup.send(embed=embed, file=file)

        except Exception as e:
            await interaction.followup.send(f"{theme.deniedIcon} Error exporting theme: {e}")

    async def import_theme(self, interaction: discord.Interaction, import_file: discord.Attachment):
        """Import a theme from a JSON file attachment."""
        try:
            await interaction.response.defer(thinking=True)

            if not import_file.filename.endswith('.json'):
                await interaction.followup.send(f"{theme.deniedIcon} Please provide a valid JSON file.")
                return

            json_data = await import_file.read()
            theme_data = json.loads(json_data.decode('utf-8'))

            theme_name = theme_data.get("themeName", "imported_theme")

            # Don't allow overwriting default
            if theme_name.lower() == "default":
                theme_name = "imported_default"

            # Save to database
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()

                # Check if theme exists and make unique name if needed
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (theme_name,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    base_name = theme_name
                    counter = 1
                    while exists:
                        theme_name = f"{base_name}_{counter}"
                        cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (theme_name,))
                        exists = cursor.fetchone()[0] > 0
                        counter += 1

                # Build insert values from import data
                icons = theme_data.get("icons", {})
                dividers = theme_data.get("dividers", {})
                colors = theme_data.get("colors", {})

                # Get all column names
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                column_names = [col[1] for col in columns_info]

                # Build values dict
                values_dict = {
                    'themeName': theme_name,
                    'themeCreator': interaction.user.id,
                    'is_active': 0,
                    'themeDescription': theme_data.get('themeDescription', '')
                }

                # Add icons (handle both flat strings and nested dict formats)
                for icon_name, icon_value in icons.items():
                    if icon_name in column_names:
                        # Handle nested format: {"emoji": "<:...>", "name": "...", "id": "..."}
                        if isinstance(icon_value, dict):
                            icon_value = icon_value.get('emoji') or icon_value.get('value') or icon_value.get('url')
                        values_dict[icon_name] = icon_value

                # Add dividers (handle both nested and flat formats)
                for div_key, div_data in dividers.items():
                    if isinstance(div_data, dict):
                        # Nested format: {"start": "━", "pattern": "━", "end": "━", "length": 20, "codeBlock": false}
                        # or old format: {"raw": "━"}
                        num = div_key[-1]  # divider1 -> 1
                        if f'dividerStart{num}' in column_names:
                            if 'raw' in div_data:
                                # Old export format - just has raw value
                                values_dict[f'dividerStart{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerPattern{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerEnd{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerLength{num}'] = 20
                                values_dict[f'dividerCodeBlock{num}'] = 0
                            else:
                                # New export format (from gallery)
                                values_dict[f'dividerStart{num}'] = div_data.get('start', '━')
                                values_dict[f'dividerPattern{num}'] = div_data.get('pattern', '━')
                                values_dict[f'dividerEnd{num}'] = div_data.get('end', '━')
                                values_dict[f'dividerLength{num}'] = div_data.get('length', 20)
                                # Handle codeBlock (bool from gallery -> int for db)
                                values_dict[f'dividerCodeBlock{num}'] = 1 if div_data.get('codeBlock') else 0
                    elif div_key.startswith('divider') and div_key in column_names:
                        # Flat format from old export: dividerLength1, dividerStart1, etc.
                        values_dict[div_key] = div_data

                # Add colors
                for color_key, color_value in colors.items():
                    if color_key in column_names:
                        values_dict[color_key] = color_value

                # Insert new theme
                insert_columns = [c for c in column_names if c != 'id' and c in values_dict]
                insert_values = [values_dict.get(c) for c in insert_columns]

                placeholders = ', '.join(['?' for _ in insert_columns])
                columns_str = ', '.join(insert_columns)

                cursor.execute(
                    f"INSERT INTO pimpsettings ({columns_str}) VALUES ({placeholders})",
                    insert_values
                )
                conn.commit()

            await interaction.followup.send(
                f"{theme.verifiedIcon} Theme **{theme_name}** imported successfully! "
                f"Use the Theme Manager to activate or edit it."
            )

        except json.JSONDecodeError:
            await interaction.followup.send(f"{theme.deniedIcon} Invalid JSON file format.")
        except Exception as e:
            logger.error(f"Theme import error: {e}")
            print(f"Theme import error: {e}")
            await interaction.followup.send(f"{theme.deniedIcon} Error importing theme: {e}")

    async def fetch_theme_info(self, interaction: discord.Interaction, themename: str, is_new_theme: bool = False, from_menu: bool = False):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(thinking=True)

            icons = self._get_theme_data(themename)
            if not icons:
                await interaction.followup.send(f"Theme '{themename}' not found.")
                return

            if themename == "default":
                lines = self._build_default_theme_lines()
            else:
                lines = [f"{name} = {value} = \\{value}" for name, value in icons.items()]

            embeds = self._build_embeds_from_lines(lines, themename)
            pages = [embeds[i:i+10] for i in range(0, len(embeds), 10)]
            all_emoji_names = [line.split(" = ")[0] for line in lines]

            view = PaginationView(pages, 0, all_emoji_names, themename, self, interaction.user.id, from_menu=from_menu)

            if is_new_theme:
                await interaction.followup.send(
                    f"{theme.verifiedIcon} **Theme '{themename}' created successfully!**\n\nHere's your new theme preview:",
                    embeds=pages[0],
                    view=view
                )
            else:
                await interaction.followup.send(embeds=pages[0], view=view)

        except Exception as e:
            logger.error(f"Fetch theme info error: {e}")
            print(f"Fetch theme info error: {e}")
            await interaction.followup.send("An error occurred while fetching theme info.")

    async def show_theme_menu(self, interaction: discord.Interaction):
        """Show the theme menu from settings - called by alliance.py."""
        guild_id = interaction.guild_id if interaction.guild else None
        view = ThemeMenuView(self, interaction.user.id, guild_id)
        embed = view.build_embed()
        await interaction.response.edit_message(embed=embed, view=view)

    async def _handle_theme_import(self, message, session, session_key):
        """Handle theme import from JSON file attachment."""
        # Check for JSON attachment
        json_attachment = None
        for attachment in message.attachments:
            if attachment.filename.endswith('.json'):
                json_attachment = attachment
                break

        if not json_attachment:
            return  # Not a JSON file, ignore

        del self.emoji_edit_sessions[session_key]

        try:
            # Read and parse JSON
            json_data = await json_attachment.read()
            theme_data = json.loads(json_data.decode('utf-8'))

            theme_name = theme_data.get("themeName", "imported_theme")

            # Don't allow overwriting default
            if theme_name.lower() == "default":
                theme_name = "imported_default"

            # Check if theme exists
            with sqlite3.connect(THEME_DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (theme_name,))
                exists = cursor.fetchone()[0] > 0

                if exists:
                    # Append number to make unique
                    base_name = theme_name
                    counter = 1
                    while exists:
                        theme_name = f"{base_name}_{counter}"
                        cursor.execute("SELECT COUNT(*) FROM pimpsettings WHERE themeName=?", (theme_name,))
                        exists = cursor.fetchone()[0] > 0
                        counter += 1

                # Build insert values from import data
                icons = theme_data.get("icons", {})
                dividers = theme_data.get("dividers", {})
                colors = theme_data.get("colors", {})

                # Get all column names
                cursor.execute("PRAGMA table_info(pimpsettings)")
                columns_info = cursor.fetchall()
                column_names = [col[1] for col in columns_info]

                # Build values dict
                values_dict = {
                    'themeName': theme_name,
                    'themeCreator': message.author.id,
                    'is_active': 0,
                    'themeDescription': theme_data.get('themeDescription', '')
                }

                # Add icons (handle both flat strings and nested dict formats)
                for icon_name, icon_value in icons.items():
                    if icon_name in column_names:
                        # Handle nested format: {"emoji": "<:...>", "name": "...", "id": "..."}
                        if isinstance(icon_value, dict):
                            icon_value = icon_value.get('emoji') or icon_value.get('value') or icon_value.get('url')
                        values_dict[icon_name] = icon_value

                # Add dividers (handle both nested and flat formats)
                for div_key, div_data in dividers.items():
                    if isinstance(div_data, dict):
                        # Nested format: {"start": "━", "pattern": "━", "end": "━", "length": 20, "codeBlock": false}
                        # or old format: {"raw": "━"}
                        num = div_key[-1]  # divider1 -> 1
                        if f'dividerStart{num}' in column_names:
                            if 'raw' in div_data:
                                # Old export format - just has raw value
                                values_dict[f'dividerStart{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerPattern{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerEnd{num}'] = div_data.get('raw', '━')
                                values_dict[f'dividerLength{num}'] = 20
                                values_dict[f'dividerCodeBlock{num}'] = 0
                            else:
                                # New export format (from gallery)
                                values_dict[f'dividerStart{num}'] = div_data.get('start', '━')
                                values_dict[f'dividerPattern{num}'] = div_data.get('pattern', '━')
                                values_dict[f'dividerEnd{num}'] = div_data.get('end', '━')
                                values_dict[f'dividerLength{num}'] = div_data.get('length', 20)
                                # Handle codeBlock (bool from gallery -> int for db)
                                values_dict[f'dividerCodeBlock{num}'] = 1 if div_data.get('codeBlock') else 0
                    elif div_key.startswith('divider') and div_key in column_names:
                        # Flat format from old export: dividerLength1, dividerStart1, etc.
                        values_dict[div_key] = div_data

                # Add colors
                for color_key, color_value in colors.items():
                    if color_key in column_names:
                        values_dict[color_key] = color_value

                # Insert new theme
                insert_columns = [c for c in column_names if c != 'id' and c in values_dict]
                insert_values = [values_dict.get(c) for c in insert_columns]

                placeholders = ', '.join(['?' for _ in insert_columns])
                columns_str = ', '.join(insert_columns)

                cursor.execute(
                    f"INSERT INTO pimpsettings ({columns_str}) VALUES ({placeholders})",
                    insert_values
                )
                conn.commit()

            # Send success message
            await message.channel.send(
                f"{theme.verifiedIcon} Theme **{theme_name}** imported successfully! "
                f"Use the Theme Manager to activate or edit it.",
                delete_after=15
            )

            # Delete the user's message with the JSON file
            try:
                await message.delete()
            except Exception:
                pass  # May fail due to permissions

            # Update menu view if available
            menu_view = session.get('menu_view')
            original_message = session.get('original_message')
            if menu_view and original_message:
                menu_view.selected_theme = theme_name
                menu_view._build_components()
                try:
                    await original_message.edit(embed=menu_view.build_embed(), view=menu_view)
                except Exception:
                    pass

        except json.JSONDecodeError:
            await message.add_reaction(theme.deniedIcon)
            await message.channel.send(
                f"{theme.deniedIcon} Invalid JSON file format.",
                delete_after=10
            )
        except Exception as e:
            logger.error(f"Theme import error: {e}")
            print(f"Theme import error: {e}")
            await message.add_reaction(theme.deniedIcon)
            await message.channel.send(
                f"{theme.deniedIcon} Error importing theme: {e}",
                delete_after=10
            )

    @commands.Cog.listener()
    async def on_message(self, message):
        """Listen for emoji messages when user is editing, or theme imports."""
        if message.author.bot:
            return

        # Check for theme import session
        import_key = f"import_{message.author.id}_{message.channel.id}"
        if import_key in self.emoji_edit_sessions:
            session = self.emoji_edit_sessions[import_key]
            if session.get('type') == 'theme_import':
                await self._handle_theme_import(message, session, import_key)
                return

        session_key = f"{message.author.id}_{message.channel.id}"
        if session_key not in self.emoji_edit_sessions:
            return

        session = self.emoji_edit_sessions[session_key]

        emoji_pattern = r'<a?:(\w+):(\d+)>'
        emoji_match = re.search(emoji_pattern, message.content)

        emoji_url = None
        is_valid = False

        if message.attachments:
            for attachment in message.attachments:
                if attachment.content_type and attachment.content_type.startswith('image/'):
                    emoji_url = attachment.url
                    is_valid = True
                    break
                elif any(attachment.filename.lower().endswith(ext) for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                    emoji_url = attachment.url
                    is_valid = True
                    break

        if not is_valid and emoji_match:
            emoji_id = emoji_match.group(2)
            is_animated = "<a:" in message.content
            emoji_ext = "gif" if is_animated else "png"
            emoji_url = f"https://cdn.discordapp.com/emojis/{emoji_id}.{emoji_ext}"
            is_valid = True
        elif not is_valid:
            content = message.content.strip()
            if content.startswith('http') and any(ext in content.lower() for ext in ['.png', '.jpg', '.jpeg', '.gif', '.webp']):
                emoji_url = content
                is_valid = True
            elif content and len(content) <= 10:
                for char in content:
                    try:
                        if unicodedata.category(char) in ['So', 'Sm'] or ord(char) > 0x1F000:
                            emoji_url = content
                            is_valid = True
                            break
                    except (ValueError, TypeError):
                        pass

        if not is_valid:
            return

        del self.emoji_edit_sessions[session_key]

        try:
            # Build appropriate view update callback based on flow type
            if 'category_view' in session:
                # New editor flow
                category_view = session['category_view']
                wizard_session = session.get('session')
                original_message = session.get('original_message')

                async def update_category_view():
                    if wizard_session:
                        wizard_session.load_theme_data()
                    if original_message and category_view:
                        # Rebuild dropdown options to show updated emoji values
                        category_view._build_components()
                        embed = category_view.build_category_embed()
                        await original_message.edit(embed=embed, view=category_view)

                view_callback = update_category_view
            else:
                # Old pagination flow
                pagination_view = session['pagination_view']
                original_message = session.get('original_message')
                themename = session['themename']

                async def update_pagination_view():
                    new_icons = self._get_theme_data(themename)
                    if themename == "default":
                        new_lines = self._build_default_theme_lines()
                    else:
                        new_lines = [f"{name} = {value} = \\{value}" for name, value in new_icons.items()]
                    new_embeds = self._build_embeds_from_lines(new_lines, themename)
                    pagination_view.pages = [new_embeds[i:i+10] for i in range(0, len(new_embeds), 10)]
                    pagination_view.all_emoji_names = [line.split(" = ")[0] for line in new_lines]
                    pagination_view.update_buttons()
                    if original_message:
                        await original_message.edit(embeds=pagination_view.pages[pagination_view.current_page], view=pagination_view)

                view_callback = update_pagination_view

            success = await self._process_emoji_update(
                session['emoji_name'],
                emoji_url or message.content.strip(),
                session['themename'],
                view_callback
            )

            # On success, delete the message immediately. On failure, react with X.
            if success:
                try:
                    await message.delete()
                except discord.HTTPException:
                    pass  # Message may already be deleted or we lack permissions
            else:
                await message.add_reaction(theme.deniedIcon)

        except Exception as e:
            logger.error(f"Error processing emoji from message: {e}")
            print(f"Error processing emoji from message: {e}")
            try:
                await message.add_reaction(theme.deniedIcon)
            except discord.HTTPException:
                pass

async def setup(bot):
    await bot.add_cog(Theme(bot))