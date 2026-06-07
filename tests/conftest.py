"""pytest bootstrap for the in-repo test suite.

Ensures the repo root (so `import cogs.*` resolves) and this tests dir (so
`from harness import ...` resolves) are importable no matter where pytest is
invoked from. Also skips suites whose target cog isn't present in the current
checkout, so the run never hard-errors on a version that lacks a feature.
"""
import sys
from pathlib import Path

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
    "test_layer1_parser.py": "cogs/bear_track.py",
    "test_layer2_ocr.py": "cogs/bear_track.py",
    "test_bear_name_matching.py": "cogs/bear_track.py",
    "test_bear_persist_no_deadlock.py": "cogs/bear_track.py",
    "test_ocr_auto_manage.py": "cogs/bear_track.py",
}
for _test_file, _needed in _REQUIRES.items():
    if not (_REPO / _needed).exists():
        collect_ignore.append(_test_file)
