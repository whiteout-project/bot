"""Crash-resume snapshots must outlive a failed finalize, and never be
re-created after one.

Both OCR pipelines deleted the snapshot before the finalize work ran: a DB or
render error during finalize permanently lost the parsed results. And because
in-flight batches kept calling save_snapshot after Done/Cancel, a stale
snapshot could be re-created - restored on every restart as a phantom
"Recovered your upload" with dead buttons (the attendance payload serializes
the finalized flag).
"""
import asyncio
import importlib
from types import SimpleNamespace

import pytest

bt = importlib.import_module("cogs.bear_track")
parsers = importlib.import_module("cogs.attendance_ocr_parsers")
ao = importlib.import_module("cogs.attendance_ocr")
ocr_resume = importlib.import_module("cogs.ocr_resume")


# ---------- attendance ----------

def _att_session(render_fails=False):
    s = parsers.OcrUploadSession.__new__(parsers.OcrUploadSession)
    s.finalized = False
    s.cancelled = False
    s._timer_task = None
    s._lock = asyncio.Lock()
    s.channel = SimpleNamespace(id=1)
    calls = SimpleNamespace(deleted=0, rendered=0)

    async def render_review(timed_out=False):
        calls.rendered += 1
        if render_fails:
            raise RuntimeError("render blew up")

    s.render_review = render_review
    s.delete_snapshot = lambda: setattr(calls, "deleted", calls.deleted + 1)
    return s, calls


def test_attendance_failed_finalize_keeps_snapshot():
    s, calls = _att_session(render_fails=True)
    asyncio.run(s.finalize())
    assert calls.rendered == 1
    assert calls.deleted == 0, "snapshot must survive a failed finalize for crash-resume"


def test_attendance_successful_finalize_deletes_snapshot():
    s, calls = _att_session()
    asyncio.run(s.finalize())
    assert calls.deleted == 1


def test_attendance_save_snapshot_noop_after_finalize(monkeypatch):
    s, _ = _att_session()
    s.finalized = True
    saved = []
    monkeypatch.setattr(ocr_resume, "save", lambda *a, **k: saved.append(a))
    s._snapshot_key = lambda: "att:1:2"
    s.save_snapshot()
    assert saved == [], "a finalized session must not re-create its snapshot"


def test_attendance_resume_skips_finalized_payload(monkeypatch):
    cog = ao.AttendanceOCR(SimpleNamespace(get_channel=lambda cid: SimpleNamespace(id=9)))
    deleted = []
    built = []

    monkeypatch.setattr(
        ocr_resume, "load_all",
        lambda kind: [("att:9:2", {"channel_id": 9, "uploader_id": 2, "finalized": True})],
    )
    monkeypatch.setattr(ocr_resume, "delete", lambda key: deleted.append(key))
    monkeypatch.setattr(
        parsers, "build_session_from_snapshot",
        lambda cog_, channel, payload: built.append(payload) or SimpleNamespace(
            finalized=True, cancelled=False, resume=None),
    )

    asyncio.run(cog.on_ready())

    assert deleted == ["att:9:2"], "stale finalized snapshot must be pruned"
    assert cog.active_sessions == {}, "finalized snapshot must not become a live session"


# ---------- bear ----------

def _bear_session(finalize_fails=False):
    s = bt.BearSession.__new__(bt.BearSession)
    s.finalized = False
    s.lock = asyncio.Lock()
    s.channel_id = 1
    s.user_id = 2
    s.timer_task = None
    calls = SimpleNamespace(deleted=0)

    async def _finalize_session(session, timed_out=False):
        if finalize_fails:
            raise RuntimeError("finalize blew up")

    async def _release_all_engines():
        pass

    s.cog = SimpleNamespace(_finalize_session=_finalize_session)
    s._release_all_engines = _release_all_engines
    s.delete_snapshot = lambda: setattr(calls, "deleted", calls.deleted + 1)
    return s, calls


def test_bear_failed_finalize_keeps_snapshot():
    s, calls = _bear_session(finalize_fails=True)
    with pytest.raises(RuntimeError):
        asyncio.run(s.finalize())
    assert calls.deleted == 0, "snapshot must survive a failed finalize for crash-resume"


def test_bear_successful_finalize_deletes_snapshot():
    s, calls = _bear_session()
    asyncio.run(s.finalize())
    assert calls.deleted == 1


def test_bear_save_snapshot_noop_after_finalize(monkeypatch):
    s, _ = _bear_session()
    s.finalized = True
    saved = []
    monkeypatch.setattr(ocr_resume, "save", lambda *a, **k: saved.append(a))
    s._snapshot_key = lambda: "bear:1:2"
    s.save_snapshot()
    assert saved == [], "a finalized session must not re-create its snapshot"
