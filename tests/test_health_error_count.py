"""Bot Health error count: ERROR/CRITICAL lines of the last 24h in the category logs."""
import os
from datetime import datetime

from cogs import bot_health

NOW = datetime(2026, 10, 4, 12, 0, 0)


def _write(path, lines, mtime=None):
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if mtime is not None:
        os.utime(path, (mtime, mtime))


def test_counts_only_recent_error_and_critical_lines(tmp_path):
    _write(tmp_path / "gift.txt", [
        "2026-10-04 11:00:00,001 - gift - ERROR - [SIGN ERROR] ID 1",
        "2026-10-04 11:05:00,001 - gift - CRITICAL - database gone",
        "2026-10-04 11:06:00,001 - gift - WARNING - retrying after 503",
        "2026-10-04 11:07:00,001 - bot - INFO - [ERROR] tagged print routed here",
        "2026-10-03 11:59:59,999 - gift - ERROR - older than 24h",
        "  File \"x.py\", line 3, in f",
    ])
    _write(tmp_path / "bot.txt", ["2026-10-04 09:00:00,000 - bot - ERROR - restart failed"])

    count, by_file = bot_health.count_recent_errors(str(tmp_path), NOW)

    assert count == 3
    assert by_file == {"gift.txt": 2, "bot.txt": 1}


def test_rotated_file_is_included(tmp_path):
    _write(tmp_path / "alliance.txt", ["2026-10-04 10:00:00,000 - alliance - ERROR - a"])
    _write(tmp_path / "alliance.txt.1", ["2026-10-04 01:00:00,000 - alliance - ERROR - b"])

    count, by_file = bot_health.count_recent_errors(str(tmp_path), NOW)

    assert count == 2
    assert by_file == {"alliance.txt": 2}


def test_missing_folder_and_unrelated_logs_count_nothing(tmp_path):
    _write(tmp_path / "redemption.txt", ["2026-10-04 10:00:00,000 - ERROR - no level field here"])
    _write(tmp_path / "backuplog.txt", ["2026-10-04 10:00:00,000 - backup - ERROR - not a category log"])

    assert bot_health.count_recent_errors(str(tmp_path), NOW) == (0, {})
    assert bot_health.count_recent_errors(str(tmp_path / "missing"), NOW) == (0, {})


def test_status_turns_warning_only_at_the_threshold():
    assert bot_health.error_count_status(bot_health.ERROR_COUNT_WARNING - 1) == bot_health.STATUS_HEALTHY
    assert bot_health.error_count_status(bot_health.ERROR_COUNT_WARNING) == bot_health.STATUS_WARNING


def test_message_names_the_busiest_log():
    assert bot_health.error_count_message(0, {}) == "None in the last 24h"
    assert bot_health.error_count_message(84, {"gift.txt": 80, "bot.txt": 4}) == "84 in the last 24h, mostly gift.txt"
