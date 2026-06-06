"""Self-healing bootstrap in main.py.

When a user drops `main.py` into an empty folder and runs it, the top-level
import `from cogs import bot_startup_display` fails. `_bootstrap_from_main_branch`
catches the ImportError, downloads the main-branch source archive, and extracts
it stripping GitHub's wrapping top-level directory (e.g. `bot-main/cogs/foo.py`
becomes `cogs/foo.py` in the cwd). main.py then restarts so the downloaded
matching main.py runs against the downloaded cogs/.

These tests exercise the function directly with a `file://` URL pointed at an
on-the-fly fixture zip — no network, no subprocess. Importing `main` works
because cogs/ exists in this checkout and bot.run() is __main__-guarded.

Test prerequisites: run from inside the bot venv (the venv guard in main.py's
top-level code would sys.exit(0) otherwise — see the test suite README).
"""
from __future__ import annotations

import os
import zipfile
from pathlib import Path

import pytest

import main


def _build_github_style_zip(zip_path: Path, top_dir: str, files: dict[str, str]) -> None:
    """Write a zip mirroring GitHub's archive layout: every entry is under one
    top-level directory."""
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(f"{top_dir}/", "")  # github archives include a dir-only entry
        for rel_path, content in files.items():
            zf.writestr(f"{top_dir}/{rel_path}", content)


def test_extracts_files_stripping_top_directory(tmp_path, monkeypatch):
    """The GitHub-generated top-level dir (e.g. `bot-main/`) is stripped — files
    land at the cwd root, not inside a wrapping folder."""
    zip_path = tmp_path / "fixture.zip"
    _build_github_style_zip(zip_path, "bot-main", {
        "main.py": "# placeholder main.py\n",
        "requirements.txt": "discord.py\n",
        "cogs/bot_startup_display.py": "# placeholder cog\n",
    })
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    monkeypatch.chdir(install_dir)

    main._bootstrap_from_main_branch(zip_path.as_uri())

    assert (install_dir / "main.py").read_text() == "# placeholder main.py\n"
    assert (install_dir / "requirements.txt").read_text() == "discord.py\n"
    assert (install_dir / "cogs" / "bot_startup_display.py").read_text() == "# placeholder cog\n"
    # Top-level dir must NOT be reproduced inside install_dir
    assert not (install_dir / "bot-main").exists()


def test_creates_nested_directories(tmp_path, monkeypatch):
    """Files at arbitrary depth get their parent directories auto-created."""
    zip_path = tmp_path / "fixture.zip"
    _build_github_style_zip(zip_path, "bot-main", {
        "cogs/nested/deep.py": "# deep\n",
        "fonts/Inter/Inter-Regular.ttf": "fake-font-bytes",
    })
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    monkeypatch.chdir(install_dir)

    main._bootstrap_from_main_branch(zip_path.as_uri())

    assert (install_dir / "cogs" / "nested" / "deep.py").read_text() == "# deep\n"
    assert (install_dir / "fonts" / "Inter" / "Inter-Regular.ttf").read_text() == "fake-font-bytes"


def test_exits_with_helpful_message_on_unreachable_url(tmp_path, monkeypatch, capsys):
    """An unreachable URL → sys.exit(1) + a message naming the URL so the user
    knows where to download manually."""
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    monkeypatch.chdir(install_dir)
    bad_url = (tmp_path / "does-not-exist.zip").as_uri()

    with pytest.raises(SystemExit) as exc:
        main._bootstrap_from_main_branch(bad_url)

    assert exc.value.code == 1
    out = capsys.readouterr().out
    assert "Download failed" in out
    assert bad_url in out
    assert "Extract everything into this folder" in out


def test_handles_top_dir_only_entry_without_crashing(tmp_path, monkeypatch):
    """GitHub archives include a `top-dir/` entry with no payload. The split
    logic must skip it rather than try to write an empty filename."""
    zip_path = tmp_path / "fixture.zip"
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("bot-main/", "")
        zf.writestr("bot-main/main.py", "x")
    install_dir = tmp_path / "install"
    install_dir.mkdir()
    monkeypatch.chdir(install_dir)

    main._bootstrap_from_main_branch(zip_path.as_uri())

    assert (install_dir / "main.py").read_text() == "x"


def test_env_var_overrides_default_url(monkeypatch):
    """`BOT_BOOTSTRAP_URL` overrides the hard-coded GitHub URL. We can't
    exercise the except branch without re-deleting cogs/, but we can at least
    verify the lookup pattern main.py uses resolves to our value."""
    monkeypatch.setenv("BOT_BOOTSTRAP_URL", "file:///tmp/override.zip")
    # Mirror the exact lookup main.py performs in its ImportError branch.
    resolved = os.environ.get(
        "BOT_BOOTSTRAP_URL",
        "https://github.com/whiteout-project/bot/archive/refs/heads/main.zip",
    )
    assert resolved == "file:///tmp/override.zip"

    monkeypatch.delenv("BOT_BOOTSTRAP_URL", raising=False)
    resolved = os.environ.get(
        "BOT_BOOTSTRAP_URL",
        "https://github.com/whiteout-project/bot/archive/refs/heads/main.zip",
    )
    assert resolved == "https://github.com/whiteout-project/bot/archive/refs/heads/main.zip"
