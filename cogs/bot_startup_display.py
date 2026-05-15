"""
Startup display module.
Provides clean, formatted console output during bot startup.
No external dependencies — pure stdlib.
"""

import os
import sys
import random

# Detect if we can use Unicode box-drawing characters
_can_unicode = True
try:
    if sys.stdout and hasattr(sys.stdout, 'encoding') and sys.stdout.encoding:
        '╔═╗║╚╝✓✗◦└·'.encode(sys.stdout.encoding)
    else:
        _can_unicode = False
except (UnicodeEncodeError, LookupError):
    _can_unicode = False

# Character sets
if _can_unicode:
    _TL, _TR, _BL, _BR = '╔', '╗', '╚', '╝'
    _H, _V = '═', '║'
    _OK, _FAIL, _PROGRESS = '✅', '❌', '⏳'
    _SUB = '      └ '
    _DOT = '·'
else:
    _TL, _TR, _BL, _BR = '+', '+', '+', '+'
    _H, _V = '=', '|'
    _OK, _FAIL, _PROGRESS = '[+]', '[x]', '[.]'
    _SUB = '      - '
    _DOT = '-'

_IS_TTY = hasattr(sys.stdout, 'isatty') and sys.stdout.isatty()
_BOX_WIDTH = 46

DISCORD_LINK = "discord.gg/apYByj6K2m"

def _get_ready_messages():
    """Build ready messages using theme icons at call time."""
    try:
        from cogs.pimp_my_bot import theme
        return [
            f"{theme.startupGiftIcon} Ready to redeem those codes faster than all the other bots!",
            f"{theme.startupBoxingIcon} Ready to rumble like Discord isn't rate limiting us again!",
            f"{theme.startupRocketIcon} Systems nominal. Ethics optional. Proceeding anyway.",
            f"{theme.startupLockIcon} Locked and loaded. Confidence high, accuracy pending.",
            f"{theme.startupSwordsIcon} Standing by for duty and looking busy, Chief. That's half the job.",
            f"{theme.startupIceIcon} Warmed up and ready to suppress any errors like they never happened.",
            f"{theme.startupCashIcon} Morally bankrupt, but rich in redeemable codes. Let's do this!",
        ]
    except Exception:
        return [
            "🎁 Ready to redeem those codes faster than all the other bots!",
            "🥊 Ready to rumble like Discord isn't rate limiting us again!",
            "🚀 Systems nominal. Ethics optional. Proceeding anyway.",
            "🔒 Locked and loaded. Confidence high, accuracy pending.",
            "⚔️ Standing by for duty and looking busy, Chief. That's half the job.",
            "🧊 Warmed up and ready to suppress any errors like they never happened.",
            "💸 Morally bankrupt, but rich in redeemable codes. Let's do this!",
        ]


def _get_shutdown_messages():
    """Build shutdown messages using theme icons at call time."""
    try:
        from cogs.pimp_my_bot import theme
        return [
            f"{theme.shutdownStopIcon} Ctrl+C detected! The bot is powering down... beep boop!",
            f"{theme.shutdownHandIcon} Caught Ctrl+C! Time for the bot to take a nap. Sweet dreams!",
            f"{theme.shutdownPlugIcon} Ctrl+C pressed! Unplugging the bot. See you next time!",
            f"{theme.shutdownDoorIcon} Exit signal received! The bot has left the building...",
            f"{theme.shutdownZzzIcon} Ctrl+C! The bot is going to sleep. Wake me up when you need me!",
            f"{theme.shutdownClapperIcon} And that's a wrap! Bot shutting down gracefully.",
            f"{theme.shutdownMoonIcon} Trying to turn the bot off and not on again. Ctrl+C ya later!",
            f"{theme.shutdownSparkleIcon} Ctrl+C and poof! The bot vanishes into thin air...",
        ]
    except Exception:
        return [
            "🛑 Ctrl+C detected! The bot is powering down... beep boop!",
            "👋 Caught Ctrl+C! Time for the bot to take a nap. Sweet dreams!",
        ]


def _center(text, width):
    """Center text within a given width."""
    padding = width - len(text)
    left = padding // 2
    right = padding - left
    return ' ' * left + text + ' ' * right


def header(version, python_version, flags=None):
    """Display the startup header box. flags is an optional list of active CLI flags."""
    inner = _BOX_WIDTH - 2  # inside the borders
    title = "Whiteout Survival Bot (Python)"
    ver_line = f"{version} {_DOT} Python {python_version}"
    help_line = f"Need help? {DISCORD_LINK}"

    print()
    print(f"  {_TL}{_H * inner}{_TR}")
    print(f"  {_V}{_center(title, inner)}{_V}")
    print(f"  {_V}{_center(ver_line, inner)}{_V}")
    if flags:
        flags_line = f"Flags: {', '.join(flags)}"
        print(f"  {_V}{_center(flags_line, inner)}{_V}")
    print(f"  {_V}{_center(help_line, inner)}{_V}")
    print(f"  {_BL}{_H * inner}{_BR}")
    print()


def phase_ok(message):
    """Display a successful phase completion."""
    if _IS_TTY:
        # Overwrite any in-progress line
        sys.stdout.write(f"\r  {_OK} {message}          \n")
        sys.stdout.flush()
    else:
        print(f"  {_OK} {message}")


def phase_start(message):
    """Display an in-progress phase (TTY only, overwritten by phase_ok)."""
    if _IS_TTY:
        sys.stdout.write(f"\r  {_PROGRESS} {message}...")
        sys.stdout.flush()


def phase_fail(message, details=None, fix=None):
    """Display a failed phase with optional details and fix suggestion."""
    print(f"  {_FAIL} {message}")
    if details:
        for detail in details:
            print(f"{_SUB}{detail}")
    if fix:
        print(f"      Run: {fix}")


def error_box(title, message, fix=None):
    """Display a fatal error."""
    print(f"\n  {_FAIL} {title}")
    for line in message.split('\n'):
        print(f"      {line}")
    if fix:
        print(f"      {fix}")
    print()


def connection_retry(attempt, reason, wait):
    """Display a connection retry status (single-line update on TTY)."""
    msg = f"  {_PROGRESS} Connecting to Discord... (attempt {attempt}, {reason}, retrying in {wait}s)"
    if _IS_TTY:
        sys.stdout.write(f"\r{msg}   ")
        sys.stdout.flush()
    else:
        print(msg)


def venv_instructions(venv_python, platform):
    """Display clear venv re-run instructions."""
    print(f"\n  {_OK} Virtual environment created")
    print()
    if platform == "win32":
        print(f"  To continue, run the bot with the venv Python:")
        print(f"    {venv_python} {sys.argv[0] if sys.argv else 'main.py'}")
    else:
        print(f"  Restarting in virtual environment...")
    print()


def venv_exists_instructions(venv_python, platform):
    """Display instructions when venv exists but user isn't in it."""
    print(f"\n  A virtual environment exists but you're not using it.")
    print(f"  Run the bot with:")
    print(f"    {venv_python} {sys.argv[0] if sys.argv else 'main.py'}")
    print()


def summary(servers, alliances, members, alliance_details=None):
    """Display the summary line with optional per-alliance breakdown."""
    try:
        from cogs.pimp_my_bot import theme
        icon = theme.chartIcon
    except Exception:
        icon = '📊'
    print(f"\n  {icon} Servers: {servers} {_DOT} Alliances: {alliances} {_DOT} Members: {members}")
    if alliance_details:
        for name, count in alliance_details:
            print(f"      {name}: {count} members")


def api_status(name, status, detail=None):
    """Display API connection status."""
    is_ok = status in ('ok', 'healthy')
    symbol = _OK if is_ok else _FAIL
    verb = "Connected to" if is_ok else "Could not reach"
    detail_str = f" ({detail})" if detail else ""
    print(f"  {symbol} {verb} {name}{detail_str}")


def info(message):
    """Display an informational line (no status symbol)."""
    try:
        from cogs.pimp_my_bot import theme
        icon = theme.chatIcon
    except Exception:
        icon = '💬'
    print(f"\n  {icon} {message}")


def warn(message):
    """Display a warning line."""
    try:
        from cogs.pimp_my_bot import theme
        icon = theme.warnIcon
    except Exception:
        icon = '⚠️'
    print(f"  {icon} {message}")


def ready():
    """Display the final ready message and usage hint."""
    message = random.choice(_get_ready_messages())
    print(f"\n  {message}")
    print(f"   ↳ Run /settings in Discord to configure the bot.")


def shutdown():
    """Return a random shutdown message."""
    return random.choice(_get_shutdown_messages())


def python_too_old(required, current):
    """Display Python version error."""
    print(f"\n  {_FAIL} Python {required}+ is required (running {current})")
    print()
    if sys.platform == "win32":
        print("  To upgrade:")
        print("    1. Download Python 3.13+ from python.org/downloads")
        print("    2. Run installer (check 'Add Python to PATH')")
        print("    3. Delete the 'bot_venv' folder")
        print("    4. Run the bot again")
    elif os.path.exists("/.dockerenv"):
        print("  Update your Dockerfile base image:")
        print("    FROM python:3.13-slim-bookworm")
    else:
        print("  To upgrade:")
        print("    pyenv install 3.13 && pyenv local 3.13")
        print("    rm -rf bot_venv && python main.py")
    print()


def update_available(new_version, source_name):
    """Display that an update is being installed."""
    phase_ok(f"Updating to {new_version} (via {source_name})")


def up_to_date(version, source_name):
    """Display that the bot is up to date."""
    phase_ok(f"Up to date ({version} via {source_name})")
