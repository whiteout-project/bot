# Upgrade tests

Verifies that **old versions of the bot can upgrade to an unreleased new version**
and boot cleanly afterwards — without publishing anything to GitHub.

## How it works

The bot's updater fetches a GitHub release JSON + source-archive zip over
`requests`. These tests:

1. **`build_release.py`** packages the current working tree into a local
   `release.zip` (GitHub-archive-shaped) + `release.json` with a synthetic tag
   newer than every clone.
2. **`_sitecustomize.py`** is copied into each clone's venv. Python auto-imports
   it, so it transparently makes `requests.get` serve the local build for the
   GitHub/GitLab update URLs, and neutralises the real relaunch so the run ends
   after the update. This is **version-agnostic** — it works against every
   historical version of the update code, no per-clone patching.
3. **`run_upgrade_tests.py`** (the driver) does, per git tag:
   - materialise a clean clone (`git archive <tag>`),
   - create a venv + install `requests`,
   - run `main.py --autoupdate` → it pulls the local build and upgrades,
   - check the clone's `version` file and `main.py` now match the build,
   - smoke-run `main.py --no-update` (dummy token) and confirm it initialises:
     dependencies OK, **Database ready**, and **all cogs load**,
   - print a PASS/FAIL matrix.

The upgrade run exits **before** the bot connects to Discord, so no token is
needed there. The smoke run uses a dummy token and is checked at the
"all cogs loaded" stage, so it doesn't require Discord connectivity either.

## Run it

```bash
# All tags (full real installs — slow first time, then pip-cached):
python tests/upgrade/run_upgrade_tests.py

# One version, keep the clone for inspection:
python tests/upgrade/run_upgrade_tests.py --versions v1.4.3 --keep

# Pick where clones go (default: C:\bot-tests on Windows, ~/bot-tests on Linux):
python tests/upgrade/run_upgrade_tests.py --work-dir C:\bot-tests
```

Run it from the repo root with whatever Python you want the clone venvs built
from (`--python` overrides). On **WSL** run the same command to cover the Linux
upgrade path (the intercept stops the real `os.execl` relaunch).

### Options
- `--versions auto|<list>`  `auto` = every git tag; or `v1.4.3,v1.3.0`
- `--work-dir <dir>`        where clones are created
- `--python <path>`         base Python for clone venvs (default: current)
- `--tag <tag>`             synthetic build tag (default `v999.0.0-test`)
- `--token <token>`         real bot token for a full connect smoke (optional)
- `--keep`                  keep clones after the run

## Requirements
- `git` on PATH and a base Python (3.11+).
- Network access to PyPI for dependency installs.
- Disk/CPU for one venv per version (full mode installs onnxruntime, rapidocr,
  matplotlib, etc. — the pip wheel cache is shared, so only the first install of
  each wheel downloads).

## Notes
- Very old, pre-ONNX versions whose requirements have no wheels for a modern
  Python will fail at the dependency stage — that's a real "can't upgrade from
  that version on this Python" finding, not a harness bug. Use `--python` to
  point at an older interpreter for those, or limit `--versions`.
- `_build/` (generated) and clones under the work dir are disposable.
