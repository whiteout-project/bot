"""Upgrade-test intercept, auto-imported by Python at interpreter startup.

The driver copies this into a clone venv's site-packages as `sitecustomize.py`.
It makes the bot's updater fetch the LOCAL test build instead of GitHub, and
neutralises the real relaunch so the upgrade run ends cleanly (on every OS and
for every historical version of the update code).

Controlled by env vars (paths produced by build_release.py):
  UPGRADE_TEST_RELEASE_JSON, UPGRADE_TEST_RELEASE_ZIP, UPGRADE_TEST_REQS

Everything is best-effort; a failure here must never break the interpreter.
"""
import json
import os
import sys


def _install() -> None:
    rel_json = os.environ.get("UPGRADE_TEST_RELEASE_JSON")
    rel_zip = os.environ.get("UPGRADE_TEST_RELEASE_ZIP")
    rel_patch = os.environ.get("UPGRADE_TEST_PATCH_ZIP")
    rel_reqs = os.environ.get("UPGRADE_TEST_REQS")
    if not (rel_json and rel_zip and os.path.exists(rel_json) and os.path.exists(rel_zip)):
        return

    with open(rel_json, encoding="utf-8") as fh:
        release = json.load(fh)
    with open(rel_zip, "rb") as fh:
        zip_bytes = fh.read()
    if rel_patch and os.path.exists(rel_patch):
        with open(rel_patch, "rb") as fh:
            patch_bytes = fh.read()
    else:
        patch_bytes = zip_bytes
    reqs_text = ""
    if rel_reqs and os.path.exists(rel_reqs):
        with open(rel_reqs, encoding="utf-8") as fh:
            reqs_text = fh.read()

    try:
        import requests
    except Exception:
        return  # driver pre-installs requests, so this normally succeeds

    class _Resp:
        def __init__(self, *, status=200, content=b"", text="", js=None):
            self.status_code = status
            self.content = content
            self.text = text
            self._js = js
            self.headers = {}

        def json(self):
            return self._js

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.exceptions.HTTPError(str(self.status_code))

        def iter_content(self, chunk_size=8192):
            for i in range(0, len(self.content), chunk_size):
                yield self.content[i:i + chunk_size]

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def _fake(url):
        low = str(url).lower()
        # Legacy patch.zip (GitHub release asset or GitLab generic package) —
        # must be checked before the generic .zip rule below.
        if "patch.zip" in low:
            return _Resp(content=patch_bytes)
        if "api.github.com" in low and "releases/latest" in low:
            return _Resp(js=release, text=json.dumps(release))
        if "gitlab" in low and "releases" in low:
            return _Resp(js=[release], text=json.dumps([release]))  # GitLab: list, newest first
        if ("raw.githubusercontent" in low or "/raw/" in low) and "requirements.txt" in low:
            return _Resp(text=reqs_text, content=reqs_text.encode())
        if low.endswith(".zip") or "/archive/" in low:  # current source archive
            return _Resp(content=zip_bytes)
        return None

    _orig_get = requests.get

    def _get(url, *a, **k):
        r = _fake(url)
        return r if r is not None else _orig_get(url, *a, **k)

    requests.get = _get

    try:
        from requests import sessions
        _orig_sget = sessions.Session.get

        def _sget(self, url, *a, **k):
            r = _fake(url)
            return r if r is not None else _orig_sget(self, url, *a, **k)

        sessions.Session.get = _sget
    except Exception:
        pass

    # Prevent a real relaunch so the upgrade run exits after writing the new
    # version (covers os.execl/os.execv on Linux and old subprocess.Popen paths).
    def _stop(*a, **k):
        print("  [upgrade-test] relaunch intercepted; exiting after update")
        raise SystemExit(0)

    os.execl = _stop
    os.execv = _stop
    os.execlp = _stop
    os.execvp = _stop
    os._exit = lambda code=0: _stop()

    import subprocess
    _RealPopen = subprocess.Popen

    def _is_relaunch(args):
        try:
            flat = " ".join(args) if isinstance(args, (list, tuple)) else str(args)
        except Exception:
            return False
        return "main.py" in flat

    # Must stay a real subclass of subprocess.Popen: asyncio.windows_utils does
    # `class Popen(subprocess.Popen)` at import time, so a plain function here
    # would break every `import asyncio`.
    class _Popen(_RealPopen):
        def __init__(self, args, *a, **k):
            if _is_relaunch(args):
                print("  [upgrade-test] subprocess relaunch intercepted")
                raise SystemExit(0)
            super().__init__(args, *a, **k)

    subprocess.Popen = _Popen


try:
    _install()
except Exception as exc:  # never break the host interpreter
    print(f"  [upgrade-test] intercept install failed: {exc!r}")
