"""Cheap static guardrails that catch whole bug classes at import/parse time.

No Discord, no DB — these scan the source tree and the theme/cog contracts.
Each one corresponds to a real past bug class (see commit refs in comments).
"""
from __future__ import annotations

import ast
import importlib.util
import re
import sys
from pathlib import Path

import pytest

from cogs.pimp_my_bot import ICON_NAMES

REPO = Path(__file__).resolve().parent.parent
COGS = REPO / "cogs"
COG_FILES = sorted(COGS.glob("*.py"))  # includes cogs/bot_restart.py
PY_FILES = COG_FILES + [REPO / "main.py"]


def _read(p: Path) -> str:
    return p.read_text(encoding="utf-8")


# ── 5a. Theme-icon contract (catches editIcon-style typos, commit 4b6a1b4) ──
def test_all_theme_icon_references_exist():
    valid = set(ICON_NAMES)
    icon_ref = re.compile(r"theme\.(\w+Icon)\b")
    bad = []
    for f in COG_FILES:
        for m in icon_ref.finditer(_read(f)):
            if m.group(1) not in valid:
                bad.append(f"{f.name}: theme.{m.group(1)}")
    assert not bad, "theme.<x>Icon references not in ICON_NAMES:\n  " + "\n  ".join(sorted(set(bad)))


# ── 5b. Missing f-prefix lint (catches leaked "{theme.x}" text, commit 763ba07) ──
def test_no_unprefixed_placeholder_string_literals():
    offenders = []
    for f in PY_FILES:
        try:
            tree = ast.parse(_read(f))
        except SyntaxError:
            continue  # covered by the 3.11 compat test
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                if "{theme." in node.value or "{self." in node.value:
                    offenders.append(f"{f.name}:{node.lineno}  {node.value[:60]!r}")
    assert not offenders, "string literals with a placeholder but no f-prefix:\n  " + "\n  ".join(offenders)


# ── 5c. LEVEL_MAPPING single source of truth ──
# `cogs/bot_level_mapping.py` is the only place that should define the FC level
# table; everything else must import from it. Attendance had a no-spaces copy
# that drifted from the canonical "FC 1 - 1" format and went unnoticed until
# manually spotted — this guards against that recurrence.
def test_level_mapping_defined_only_in_bot_level_mapping():
    pattern = re.compile(r"^\s*\w*LEVEL_MAPPING\s*=\s*\{", re.MULTILINE)
    offenders = [
        f.name for f in COG_FILES
        if f.name != "bot_level_mapping.py" and pattern.search(_read(f))
    ]
    assert not offenders, (
        "These files define their own *LEVEL_MAPPING dict — import from "
        "cogs.bot_level_mapping instead:\n  " + "\n  ".join(offenders)
    )


# ── 5d. matplotlib backend safety ──
# Any cog importing matplotlib.pyplot must first call matplotlib.use('Agg').
# Without this, matplotlib lazy-picks the interactive TkAgg backend on Windows
# hosts (Tk is bundled with CPython), and figures GC'd off the main thread —
# which is exactly what the bot does inside asyncio executors for OCR + chart
# rendering — crash the interpreter with "main thread is not in main loop" and
# "Tcl_AsyncDelete: async handler deleted by the wrong thread". CI doesn't
# notice because slim-bookworm images don't ship Tk.
def test_matplotlib_pyplot_imports_set_agg_backend_first():
    pyplot_re = re.compile(r"^\s*import\s+matplotlib\.pyplot\b", re.M)
    use_re = re.compile(r"matplotlib\.use\(\s*['\"]Agg['\"]")
    bad = []
    for f in COG_FILES:
        text = _read(f)
        py_match = pyplot_re.search(text)
        if not py_match:
            continue
        use_match = use_re.search(text)
        if not use_match or use_match.start() > py_match.start():
            bad.append(f.name)
    assert not bad, (
        "These cogs import matplotlib.pyplot without calling "
        "matplotlib.use('Agg') first:\n  " + "\n  ".join(bad)
    )


# ── 6. Cog registration integrity (CLAUDE.md: get_cog must match a real cog) ──
KNOWN_NON_COG = {"LoginHandler"}  # helper looked up via get_cog with a fallback (bot_health)


def _cog_load_list() -> list[str]:
    m = re.search(r"cogs\s*=\s*(\[[^\]]*\])", _read(REPO / "main.py"))
    assert m, "could not find the cog load list in main.py"
    return ast.literal_eval(m.group(1))


def _registered_cog_classes() -> set[str]:
    """Every class that subclasses commands.Cog (some cogs register via a
    variable, so detect the class definition rather than the add_cog call)."""
    classes = set()
    cls_re = re.compile(r"class\s+(\w+)\s*\([^)]*commands\.Cog")
    for f in COG_FILES:
        for m in cls_re.finditer(_read(f)):
            classes.add(m.group(1))
    return classes


def test_every_load_list_cog_defines_setup():
    missing = [c for c in _cog_load_list()
               if not re.search(r"async\s+def\s+setup\s*\(", _read(COGS / f"{c}.py"))]
    assert not missing, f"cogs in the load list without async def setup(): {missing}"


def test_get_cog_strings_match_registered_cogs():
    registered = _registered_cog_classes()
    bad = []
    targets = re.compile(r'get_cog\(\s*["\']([^"\']+)["\']\s*\)')
    for f in COG_FILES + [REPO / "main.py"]:
        for m in targets.finditer(_read(f)):
            name = m.group(1)
            if name not in registered and name not in KNOWN_NON_COG:
                bad.append(f"{f.name}: get_cog({name!r})")
    assert not bad, "get_cog() targets that are not registered cogs:\n  " + "\n  ".join(sorted(set(bad)))


# ── 7a. Duplicate slash-command names (would raise at tree.sync) ──
def test_no_duplicate_command_names():
    name_re = re.compile(r'@(?:app_commands|commands)\.command\([^)]*name\s*=\s*["\']([^"\']+)["\']', re.S)
    seen, dupes = {}, []
    for f in COG_FILES:
        for m in name_re.finditer(_read(f)):
            n = m.group(1)
            if n in seen:
                dupes.append(f"{n} ({seen[n]} & {f.name})")
            seen[n] = f.name
    assert not dupes, f"duplicate command names: {dupes}"


# ── 7b. Requirements ↔ imports resolve (deployment-break guard) ──
# requirements.txt package name -> import name where they differ.
PKG_TO_IMPORT = {
    "discord.py": "discord", "pillow": "PIL", "python-bidi": "bidi",
    "python-dotenv": "dotenv", "aiohttp-socks": "aiohttp_socks",
    "arabic-reshaper": "arabic_reshaper",
}
# Third-party imports that are guaranteed-present transitively (not direct deps).
ALLOWED_TRANSITIVE = {"cv2", "packaging", "pkg_resources", "yaml"}


def _requirement_packages() -> list[str]:
    out = []
    for line in _read(REPO / "requirements.txt").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(re.split(r"[<>=!~]", line, maxsplit=1)[0].strip().lower())
    return out


def test_declared_requirements_are_importable():
    missing = []
    for pkg in _requirement_packages():
        mod = PKG_TO_IMPORT.get(pkg, pkg.replace("-", "_"))
        if importlib.util.find_spec(mod) is None:
            missing.append(f"{pkg} (import {mod})")
    assert not missing, f"requirements.txt entries not importable in the venv: {missing}"


def _third_party_imports() -> set[str]:
    local = {f.stem for f in COG_FILES} | {"cogs", "main", "bot_restart", "harness",
                                           "harness_attendance", "build_release"}
    stdlib = sys.stdlib_module_names
    found = set()
    for f in PY_FILES:
        try:
            tree = ast.parse(_read(f))
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for a in node.names:
                    found.add(a.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                if node.level == 0 and node.module:  # absolute import only
                    found.add(node.module.split(".")[0])
    return {m for m in found if m not in stdlib and m not in local}


def test_code_imports_are_satisfied():
    unresolved = []
    for mod in sorted(_third_party_imports()):
        if mod in ALLOWED_TRANSITIVE:
            continue
        if importlib.util.find_spec(mod) is None:
            unresolved.append(mod)
    assert not unresolved, f"third-party imports not installed (add to requirements.txt?): {unresolved}"
