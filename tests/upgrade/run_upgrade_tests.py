"""Upgrade test driver.

For each old version (a git tag of this repo) it:
  1. materialises a clean clone of that tag into <work-dir>/<tag>
  2. creates a venv and installs the bootstrap dep (requests) so the intercept
     can patch it; the clone's own startup installs the rest
  3. runs `main.py --autoupdate` with the requests/relaunch intercept active,
     serving the LOCAL build (build_release.py) as the 'latest release'
  4. checks the clone upgraded: its `version` file and `main.py` now match the
     build
  5. smoke-runs `main.py --no-update` (dummy token) to confirm the upgraded
     tree initialises: dependencies OK, DB ready, and every cog loads
  6. prints a PASS/FAIL matrix

Runs on Windows and under WSL (Linux). Requires `git` and a base Python.

Examples:
  python tests/upgrade/run_upgrade_tests.py                 # all tags
  python tests/upgrade/run_upgrade_tests.py --versions v1.4.3
  python tests/upgrade/run_upgrade_tests.py --keep --work-dir C:\\bot-tests
"""
import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import build_release

REPO = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
DUMMY_TOKEN = "INVALID_TOKEN_FOR_UPGRADE_TESTING"
UPGRADE_TIMEOUT = 1800   # full dep install can be slow on first download
SMOKE_TIMEOUT = 240


def sh(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def git_tags(repo: Path) -> list:
    out = sh(["git", "-C", str(repo), "tag"]).stdout.split()
    return sorted(out, key=lambda t: [int(x) for x in re.findall(r"\d+", t)] or [0])


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def venv_python(clone: Path) -> Path:
    if sys.platform == "win32":
        return clone / "bot_venv" / "Scripts" / "python.exe"
    return clone / "bot_venv" / "bin" / "python"


def site_packages(py: Path) -> Path:
    out = sh([str(py), "-c", "import sysconfig;print(sysconfig.get_paths()['purelib'])"]).stdout.strip()
    return Path(out)


def materialize(repo: Path, tag: str, clone: Path) -> None:
    if clone.exists():
        shutil.rmtree(clone, onerror=_force_rm)
    clone.mkdir(parents=True)
    tmp_zip = clone.parent / f"_{tag}.zip"
    res = sh(["git", "-C", str(repo), "archive", "--format=zip", "--output", str(tmp_zip), tag])
    if res.returncode != 0:
        raise RuntimeError(f"git archive {tag} failed: {res.stderr.strip()}")
    with zipfile.ZipFile(tmp_zip) as zf:
        zf.extractall(clone)
    tmp_zip.unlink()


def _force_rm(func, path, _):
    import stat
    try:
        os.chmod(path, stat.S_IWRITE)
        func(path)
    except Exception:
        pass


def prepare_clone(clone: Path, base_python: str) -> Path:
    sh([base_python, "-m", "venv", str(clone / "bot_venv")])
    py = venv_python(clone)
    # Bootstrap deps the oldest versions import at module top *before* their
    # update code runs (the first releases have no dependency setup of their own
    # and assume the original installer prepared the environment): the update
    # code needs requests; colorama is imported on line 1; and v1.0.x imports
    # discord before it ever calls check_and_update_files().
    sh([str(py), "-m", "pip", "install", "-q", "--disable-pip-version-check",
        "requests", "colorama", "discord.py"])
    shutil.copy2(HERE / "_sitecustomize.py", site_packages(py) / "sitecustomize.py")
    (clone / "bot_token.txt").write_text(DUMMY_TOKEN, encoding="utf-8")
    return py


def env_for(artifacts: dict, token: str | None) -> dict:
    env = dict(os.environ)
    env["UPGRADE_TEST_RELEASE_JSON"] = artifacts["json"]
    env["UPGRADE_TEST_RELEASE_ZIP"] = artifacts["zip"]
    env["UPGRADE_TEST_PATCH_ZIP"] = artifacts["patch"]
    env["UPGRADE_TEST_REQS"] = artifacts["reqs"]
    env["PYTHONUNBUFFERED"] = "1"
    # Force UTF-8 so a captured (piped) stdout on a legacy Windows codepage
    # doesn't crash the bot's own print()s — matches a real terminal/Docker.
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    if token:
        env["UPGRADE_TEST_BOT_TOKEN"] = token
    return env


def run_capture(cmd, cwd, env, timeout, input_text=None):
    try:
        cp = subprocess.run(cmd, cwd=str(cwd), env=env, capture_output=True,
                            encoding="utf-8", errors="replace", timeout=timeout,
                            input=input_text)
        return cp.returncode, (cp.stdout or "") + (cp.stderr or ""), False
    except subprocess.TimeoutExpired as e:
        return None, (e.stdout or "") + (e.stderr or ""), True


def check_upgrade(clone: Path, artifacts: dict, out: str) -> tuple:
    version_file = clone / "version"
    got = version_file.read_text(encoding="utf-8").strip() if version_file.exists() else "(none)"
    if got != artifacts["tag"]:
        return False, f"version file is {got!r}, expected {artifacts['tag']!r}"
    # main.py in the build should now be on disk in the clone
    with zipfile.ZipFile(artifacts["zip"]) as zf:
        top = zf.namelist()[0].split("/")[0]
        new_main = zf.read(f"{top}/main.py")
    if (clone / "main.py").read_bytes() != new_main:
        return False, "main.py was not replaced with the build version"
    return True, "version + main.py updated"


def check_smoke(out: str) -> tuple:
    if "Database ready" not in out:
        return False, "no 'Database ready' (DB init failed before cog load)"
    m = re.search(r"(\d+)/(\d+) modules loaded", out)
    if not m:
        return False, "never reached cog loading"
    loaded, total = int(m.group(1)), int(m.group(2))
    if loaded != total:
        fails = re.findall(r"^\s+([\w.]+): (.+)$", out, re.M)
        detail = "; ".join(f"{c}: {e}" for c, e in fails[:4])
        return False, f"{loaded}/{total} cogs loaded — {detail or 'see output'}"
    return True, f"{loaded}/{total} cogs, DB ready"


def test_version(tag: str, artifacts: dict, work: Path, base_python: str, token: str | None) -> dict:
    clone = work / tag
    row = {"version": tag, "stage": "", "ok": False, "detail": ""}
    try:
        print(f"  [{tag}] materialise…", flush=True)
        materialize(REPO, tag, clone)

        print(f"  [{tag}] venv + bootstrap…", flush=True)
        py = prepare_clone(clone, base_python)

        print(f"  [{tag}] run upgrade (installs deps; may take minutes)…", flush=True)
        rc, out, timed = run_capture([str(py), "main.py", "--autoupdate"],
                                     clone, env_for(artifacts, token), UPGRADE_TIMEOUT,
                                     input_text="y\n" * 5)  # auto-answer any prompt in old versions
        if timed:
            row.update(stage="upgrade", detail="timed out during upgrade")
            return row
        ok, why = check_upgrade(clone, artifacts, out)
        if not ok:
            row.update(stage="upgrade", detail=why)
            return row

        print(f"  [{tag}] smoke (init + load all cogs)…", flush=True)
        rc, out, timed = run_capture([str(py), "main.py", "--no-update"],
                                     clone, env_for(artifacts, token), SMOKE_TIMEOUT)
        ok, why = check_smoke(out)
        row.update(stage="smoke", ok=ok, detail=why + (" [timeout]" if timed and not ok else ""))
        return row
    except Exception as exc:
        row.update(stage=row["stage"] or "setup", detail=f"{type(exc).__name__}: {exc}")
        return row


def main() -> int:
    default_work = os.environ.get("BOT_TESTS_DIR") or (
        r"C:\bot-tests" if sys.platform == "win32" else str(Path.home() / "bot-tests"))
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--work-dir", default=default_work, help="where clones are created")
    ap.add_argument("--python", default=sys.executable, help="base Python for clone venvs")
    ap.add_argument("--tag", default="v999.0.0-test", help="synthetic build tag")
    ap.add_argument("--versions", default="auto", help="'auto' (all git tags) or comma list e.g. v1.4.3,v1.3.0")
    ap.add_argument("--token", default=None, help="real bot token for a full connect smoke (optional)")
    ap.add_argument("--keep", action="store_true", help="keep clones after the run")
    args = ap.parse_args()

    work = Path(args.work_dir)
    work.mkdir(parents=True, exist_ok=True)

    print(f"Building local release from {REPO} …")
    artifacts = build_release.build(REPO, HERE / "_build", args.tag)
    print(f"  build: {artifacts['tag']}  ({artifacts['zip']})")

    if args.versions == "auto":
        versions = git_tags(REPO)
    else:
        versions = [v.strip() for v in args.versions.split(",") if v.strip()]
    print(f"Testing upgrade -> {args.tag} from: {', '.join(versions)}\n")

    rows = [test_version(v, artifacts, work, args.python, args.token) for v in versions]

    print("\n" + "=" * 72)
    print(f"{'VERSION':<14}{'RESULT':<8}{'STAGE':<10}DETAIL")
    print("-" * 72)
    for r in rows:
        print(f"{r['version']:<14}{'PASS' if r['ok'] else 'FAIL':<8}{r['stage']:<10}{r['detail']}")
    print("=" * 72)
    passed = sum(1 for r in rows if r["ok"])
    print(f"{passed}/{len(rows)} versions upgraded and booted cleanly")

    if not args.keep:
        for v in versions:
            shutil.rmtree(work / v, ignore_errors=True)

    return 0 if passed == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
