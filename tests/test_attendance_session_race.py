"""Concurrent uploads from one uploader must share one OCR session.

Discord caps 10 attachments per message, so an 11+ screenshot upload arrives
as two near-simultaneous on_message events. The session was only registered
after the slow classification OCR, so both events passed the no-session check
and forked duplicate sessions (double OCR load, interleaved resume snapshots,
possible double event writes).
"""
import asyncio
import importlib
from types import SimpleNamespace

ao = importlib.import_module("cogs.attendance_ocr")


class _FakeSession:
    def __init__(self):
        self.finalized = False
        self.cancelled = False
        self.started_with = []
        self.added = []

    async def start(self, images, status_message=None):
        self.started_with.append(images)

    async def add_attachments(self, images):
        self.added.append(images)


def _msg(channel_id=1, user_id=2, content=""):
    return SimpleNamespace(
        author=SimpleNamespace(bot=False, id=user_id),
        guild=object(),
        channel=SimpleNamespace(id=channel_id),
        content=content,
        attachments=[SimpleNamespace(filename="shot.png")],
    )


def test_concurrent_uploads_share_one_session(monkeypatch):
    cog = ao.AttendanceOCR(SimpleNamespace())
    built = []

    def fake_build_session(event_type, *, cog, channel, uploader, alliance_id):
        s = _FakeSession()
        built.append(s)
        return s

    async def slow_classify(self, channel_id, images):
        await asyncio.sleep(0.05)  # the classification OCR window
        return ("foundry",), "ocr text"

    async def _anoop(self, *a, **k):
        return None

    monkeypatch.setattr(ao, "get_channel_settings", lambda cid: {"alliance_id": 5})
    monkeypatch.setattr(ao, "get_channel_keywords", lambda cid: {})
    monkeypatch.setattr(ao, "get_ocr_upload_admin_only", lambda aid: False)
    monkeypatch.setattr(ao, "build_session", fake_build_session)
    monkeypatch.setattr(ao.AttendanceOCR, "_classify_images", slow_classify)
    monkeypatch.setattr(ao.AttendanceOCR, "_send_reading_ack", _anoop)
    monkeypatch.setattr(ao.AttendanceOCR, "_maybe_delete_source", _anoop)

    async def run():
        await asyncio.gather(cog.on_message(_msg()), cog.on_message(_msg()))

    asyncio.run(run())

    assert len(built) == 1, "second concurrent upload must not fork a new session"
    session = built[0]
    assert len(session.started_with) == 1
    assert len(session.added) == 1, "second upload must merge into the first session"


def test_different_uploaders_get_separate_sessions(monkeypatch):
    cog = ao.AttendanceOCR(SimpleNamespace())
    built = []

    def fake_build_session(event_type, *, cog, channel, uploader, alliance_id):
        s = _FakeSession()
        built.append(s)
        return s

    async def slow_classify(self, channel_id, images):
        await asyncio.sleep(0.02)
        return ("foundry",), "ocr text"

    async def _anoop(self, *a, **k):
        return None

    monkeypatch.setattr(ao, "get_channel_settings", lambda cid: {"alliance_id": 5})
    monkeypatch.setattr(ao, "get_channel_keywords", lambda cid: {})
    monkeypatch.setattr(ao, "get_ocr_upload_admin_only", lambda aid: False)
    monkeypatch.setattr(ao, "build_session", fake_build_session)
    monkeypatch.setattr(ao.AttendanceOCR, "_classify_images", slow_classify)
    monkeypatch.setattr(ao.AttendanceOCR, "_send_reading_ack", _anoop)
    monkeypatch.setattr(ao.AttendanceOCR, "_maybe_delete_source", _anoop)

    async def run():
        await asyncio.gather(
            cog.on_message(_msg(user_id=2)), cog.on_message(_msg(user_id=3))
        )

    asyncio.run(run())

    assert len(built) == 2, "different uploaders must keep separate sessions"


def _patch_keyword_channel(monkeypatch, built):
    def fake_build_session(event_type, *, cog, channel, uploader, alliance_id):
        s = _FakeSession()
        built.append(s)
        return s

    async def slow_classify(self, channel_id, images):
        await asyncio.sleep(0.05)
        return ("foundry",), "ocr text"

    async def _anoop(self, *a, **k):
        return None

    monkeypatch.setattr(ao, "get_channel_settings", lambda cid: {"alliance_id": 5})
    monkeypatch.setattr(ao, "get_channel_keywords", lambda cid: {"foundry": ["foundry"]})
    monkeypatch.setattr(ao, "get_ocr_upload_admin_only", lambda aid: False)
    monkeypatch.setattr(ao, "build_session", fake_build_session)
    monkeypatch.setattr(ao.AttendanceOCR, "_classify_images", slow_classify)
    monkeypatch.setattr(ao.AttendanceOCR, "_send_reading_ack", _anoop)
    monkeypatch.setattr(ao.AttendanceOCR, "_maybe_delete_source", _anoop)


def test_split_upload_without_keyword_joins_starting_session(monkeypatch):
    cog = ao.AttendanceOCR(SimpleNamespace())
    built = []
    _patch_keyword_channel(monkeypatch, built)

    async def run():
        await asyncio.gather(
            cog.on_message(_msg(content="foundry")), cog.on_message(_msg())
        )
        await cog.on_message(_msg())

    asyncio.run(run())

    assert len(built) == 1
    assert len(built[0].added) == 2, "keyword-less follow-ups must join the open session"


def test_keyword_gate_still_blocks_without_session(monkeypatch):
    cog = ao.AttendanceOCR(SimpleNamespace())
    built = []
    _patch_keyword_channel(monkeypatch, built)

    asyncio.run(cog.on_message(_msg()))

    assert built == []
