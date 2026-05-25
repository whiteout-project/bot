"""Python 3.11 f-string compatibility check.

Two f-string features in 3.12+ will raise SyntaxError on 3.11 and break
cog loading silently for production users. This test scans both bots' cogs
for those patterns so we catch them in CI before shipping.

The two forbidden-on-3.11 patterns:

  1. Backslash inside an f-string {} expression:
        f"foo {'\\u00B7'.join(x)} bar"
     Lifted in 3.12. Workaround: bind the literal outside, or use the
     actual Unicode character in the source.

  2. Same quote reused inside an f-string {} expression:
        f"{d["key"]}"
     Lifted in 3.12. Workaround: switch the inner quote, or bind to a
     variable.

This test runs on whatever Python interpreter pytest uses, but the
detection is interpreter-version-independent — we parse the AST and
inspect the source span of each FormattedValue manually.
"""
from __future__ import annotations

import ast
import os
import sys
from pathlib import Path

import pytest


HERE = Path(__file__).resolve().parent       # <repo>/tests
REPO_ROOT = HERE.parent                       # the bot repo this test lives in
ROOTS_TO_SCAN = [
    REPO_ROOT,                                       # this bot
    REPO_ROOT.parent / "Kingshot-Discord-Bot",       # sibling repo, scanned only if present
]

SKIP_DIR_NAMES = {
    "bot_venv", ".venv", ".git", "__pycache__", "site-packages",
    "backups", "log", "db", "fonts", "models", "docker", "install", "tests",
}


def _iter_python_files(root: Path):
    if not root.exists():
        return
    for dirpath, dirs, files in os.walk(root):
        dirs[:] = [d for d in dirs if d not in SKIP_DIR_NAMES]
        for fn in files:
            if fn.endswith(".py"):
                yield Path(dirpath) / fn


def _scan_fstrings(path: Path):
    """Yield (lineno, kind, snippet) for each 3.11-incompatible f-string."""
    try:
        src = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return
    try:
        tree = ast.parse(src, filename=str(path))
    except SyntaxError as e:
        yield (e.lineno or 1, "syntax_error", f"{e.msg}")
        return

    src_lines = src.splitlines(keepends=True)

    for node in ast.walk(tree):
        if not isinstance(node, ast.JoinedStr):
            continue
        # Determine outer quote of this f-string by inspecting the original source.
        # ast doesn't expose the prefix/quote directly; pull it from the source span.
        try:
            fstring_src = ast.get_source_segment(src, node) or ""
        except Exception:
            fstring_src = ""
        outer_quote = _outer_quote_of(fstring_src)

        for value in node.values:
            if not isinstance(value, ast.FormattedValue):
                continue
            # Get the source text of the {expression} including the braces.
            try:
                expr_src = ast.get_source_segment(src, value) or ""
            except Exception:
                expr_src = ""
            if not expr_src:
                continue
            # ast.get_source_segment for a FormattedValue returns "{expr}" (with braces).
            # Strip them for analysis.
            stripped = expr_src
            if stripped.startswith("{"):
                stripped = stripped[1:]
            if stripped.endswith("}"):
                stripped = stripped[:-1]

            lineno = value.lineno
            if "\\" in stripped:
                yield (lineno, "backslash_in_fstring",
                       f"{{{stripped.strip()[:80]}}}")
            if outer_quote and outer_quote in stripped:
                yield (lineno, "nested_same_quote",
                       f"{outer_quote!r} reused in {{{stripped.strip()[:80]}}}")


def _outer_quote_of(fstring_src: str) -> str | None:
    """Given the raw source of an f-string (e.g. `f"hi"` or `f'''hi'''`),
    return the single-char outer quote (`"` or `'`), or None."""
    if not fstring_src:
        return None
    # Skip prefix
    i = 0
    while i < len(fstring_src) and fstring_src[i] in "rRfFbB":
        i += 1
    rest = fstring_src[i:]
    for q in ('"""', "'''", '"', "'"):
        if rest.startswith(q):
            return q[0]
    return None


@pytest.mark.parametrize("root", ROOTS_TO_SCAN, ids=lambda p: p.name)
def test_no_python311_incompatible_fstrings(root: Path):
    """Fail with a list of files+lines that use 3.12-only f-string syntax."""
    if not root.exists():
        pytest.skip(f"{root} does not exist on this machine")

    issues = []
    for path in _iter_python_files(root):
        for lineno, kind, snippet in _scan_fstrings(path):
            rel = path.relative_to(root.parent)
            issues.append(f"{rel}:{lineno}  [{kind}]  {snippet}")

    if issues:
        message = (
            f"Found {len(issues)} Python 3.11-incompatible f-string(s) in "
            f"{root.name}. These will SyntaxError on production:\n  "
            + "\n  ".join(issues)
        )
        pytest.fail(message)


if __name__ == "__main__":
    rc = 0
    for root in ROOTS_TO_SCAN:
        if not root.exists():
            continue
        issues = []
        for path in _iter_python_files(root):
            for lineno, kind, snippet in _scan_fstrings(path):
                rel = path.relative_to(root.parent)
                issues.append(f"  {rel}:{lineno}  [{kind}]  {snippet}")
        if issues:
            rc = 1
            print(f"FAIL: {root.name}")
            print("\n".join(issues))
        else:
            print(f"OK: {root.name} clean")
    sys.exit(rc)
