"""pytest bootstrap for the in-repo test suite.

Ensures the repo root (so `import cogs.*` resolves) and this tests dir (so
`from harness import ...` resolves) are importable no matter where pytest is
invoked from. Also skips suites whose target cog isn't present in the current
checkout, so the run never hard-errors on a version that lacks a feature.
"""
import sqlite3
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent
for _p in (str(_REPO), str(_HERE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# Skip suites whose target module isn't in this checkout (e.g. the attendance
# OCR parsers may live on a different branch/version). Keeps `pytest tests`
# green here while the suites auto-run wherever the feature exists.
collect_ignore = []
_REQUIRES = {
    "test_attendance_ocr_layer1.py": "cogs/attendance_ocr_parsers.py",
    "test_attendance_ocr_layer2.py": "cogs/attendance_ocr_parsers.py",
    "test_attendance_ocr_alias.py": "cogs/attendance_ocr_parsers.py",
    "test_attendance_ocr_fallback.py": "cogs/attendance_ocr_parsers.py",
    "test_attendance_history.py": "cogs/attendance_history.py",
    "test_layer1_parser.py": "cogs/bear_track.py",
    "test_layer2_ocr.py": "cogs/bear_track.py",
    "test_bear_name_matching.py": "cogs/bear_track.py",
    "test_bear_persist_no_deadlock.py": "cogs/bear_track.py",
    "test_ocr_auto_manage.py": "cogs/bear_track.py",
}
for _test_file, _needed in _REQUIRES.items():
    if not (_REPO / _needed).exists():
        collect_ignore.append(_test_file)

SIM_INSTALL_HINT = 'python -m pip install "simcord[pytest]>=2.2.1,<3" tzdata'


def pytest_addoption(parser):
    parser.addoption("--sim", action="store_true", help="run the SimCord UI suite in tests/sim")


def pytest_ignore_collect(collection_path, config):
    if collection_path.name != "sim" or collection_path.parent != _HERE:
        return None
    if not config.getoption("--sim"):
        return True
    try:
        import simcord  # noqa: F401
    except ImportError:
        raise pytest.UsageError(f"--sim needs SimCord: {SIM_INSTALL_HINT}")
    return None


def pytest_collection_modifyitems(config, items):
    if config.getoption("--sim"):
        return
    skip = pytest.mark.skip(reason="SimCord UI suite: add --sim to run it")
    for item in items:
        if _HERE / "sim" in item.path.parents:
            item.add_marker(skip)


@pytest.fixture
def make_templates_cog():
    """Builds a NotificationTemplates cog on a fresh in-memory database."""
    import importlib
    templates = importlib.import_module("cogs.notification_templates")

    def _build():
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE bear_notifications (id INTEGER PRIMARY KEY, event_type TEXT)")
        conn.commit()

        cog = templates.NotificationTemplates.__new__(templates.NotificationTemplates)
        cog.conn = conn
        cog.cursor = conn.cursor()
        cog._setup_database()
        return cog

    return _build
