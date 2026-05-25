"""Package the current working tree as a local 'release' for upgrade testing.

The bot's updater downloads a GitHub *source archive* (a zip with a single
top-level directory) and a release-info JSON. This builds local equivalents so
the upgrade test can serve an unreleased version without publishing to GitHub.

Outputs into the build dir (default tests/upgrade/_build):
  release.zip       - zip with a single top-level dir 'bot-<tag>/' holding the
                      tree (exactly the shape the updater's extractor expects)
  release.json      - GitHub-API 'releases/latest'-shaped object
  requirements.txt  - the new requirements (served to the raw-file intercept)
"""
import argparse
import json
import os
import shutil
import zipfile
from pathlib import Path

# Directories/files that never belong in a release package (vcs, venvs, caches,
# runtime data, the test harness itself).
EXCLUDE_DIRS = {
    ".git", "bot_venv", "venv3.11", "venv3.14", "linux-venv-314",
    "__pycache__", "db", "db.bak", "log", "backups", "captcha_images",
    "update", "cogs.bak", ".vscode", ".github", "PLANS", "tests", ".pytest_cache",
}
EXCLUDE_SUFFIXES = {".bak", ".pyc"}
EXCLUDE_NAMES = {"bot_token.txt", "package.zip", "main.py.bak", "requirements.old"}


def _ignored(path: Path, repo: Path) -> bool:
    rel = path.relative_to(repo)
    if set(rel.parts) & EXCLUDE_DIRS:
        return True
    if path.name in EXCLUDE_NAMES or path.suffix in EXCLUDE_SUFFIXES:
        return True
    return False


def build(repo: Path, out: Path, tag: str) -> dict:
    repo, out = repo.resolve(), out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    top = f"bot-{tag}"

    zip_path = out / "release.zip"    # current contract: GitHub source archive (single top dir)
    patch_path = out / "patch.zip"    # legacy contract (<= v1.2.0): flat patch.zip, files at root
    for p in (zip_path, patch_path):
        if p.exists():
            p.unlink()

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as src_zf, \
            zipfile.ZipFile(patch_path, "w", zipfile.ZIP_DEFLATED) as patch_zf:
        for root, dirs, files in os.walk(repo):
            rootp = Path(root)
            dirs[:] = [d for d in dirs if not _ignored(rootp / d, repo)]
            for name in files:
                p = rootp / name
                if _ignored(p, repo):
                    continue
                rel = p.relative_to(repo)
                src_zf.write(p, arcname=str(Path(top) / rel))  # source archive: under bot-<tag>/
                patch_zf.write(p, arcname=str(rel))            # patch: flat at the root

    reqs_src = repo / "requirements.txt"
    if reqs_src.exists():
        shutil.copy2(reqs_src, out / "requirements.txt")

    info = {
        "tag_name": tag,
        "name": f"Local test build {tag}",
        "body": "Local upgrade-test build (not a real release).",
        "draft": False,
        "prerelease": False,
        # Legacy versions (<= v1.2.0) read assets[0].browser_download_url (a
        # patch.zip); current versions ignore this and use the source archive.
        "assets": [{
            "name": "patch.zip",
            "browser_download_url": f"https://github.com/whiteout-project/bot/releases/download/{tag}/patch.zip",
        }],
    }
    (out / "release.json").write_text(json.dumps(info, indent=2), encoding="utf-8")

    return {
        "tag": tag,
        "zip": str(zip_path),
        "patch": str(patch_path),
        "json": str(out / "release.json"),
        "reqs": str(out / "requirements.txt"),
    }


def main() -> None:
    repo_default = Path(__file__).resolve().parents[2]  # tests/upgrade/ -> repo root
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default=str(repo_default), help="working tree to package")
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent / "_build"))
    ap.add_argument("--tag", default="v999.0.0-test", help="synthetic tag, newer than every clone")
    args = ap.parse_args()
    print(json.dumps(build(Path(args.source), Path(args.out), args.tag), indent=2))


if __name__ == "__main__":
    main()
