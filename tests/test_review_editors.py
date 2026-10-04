"""Who can edit an uploaded screenshot review (Bear Track hunts and attendance): uploader only by
default; optionally the alliance's admins too, or anyone in the channel."""
import asyncio
import sqlite3
import types

import pytest

from cogs import permission_handler as ph


def _admins(monkeypatch, alliance_ids=(), is_global=False):
    monkeypatch.setattr(ph.PermissionManager, "get_admin_alliance_ids",
                        staticmethod(lambda uid, gid: (list(alliance_ids), is_global)))


@pytest.mark.parametrize("mode", ["uploader", "admins", "anyone"])
def test_uploader_can_always_edit(monkeypatch, mode):
    _admins(monkeypatch)
    assert ph.can_edit_upload(1, 1, mode, 5, 99) is True


def test_default_mode_keeps_everyone_else_out(monkeypatch):
    _admins(monkeypatch, alliance_ids=[5])
    assert ph.can_edit_upload(2, 1, "uploader", 5, 99) is False


def test_admin_mode_lets_that_alliances_admins_in(monkeypatch):
    _admins(monkeypatch, alliance_ids=[5])
    assert ph.can_edit_upload(2, 1, "admins", 5, 99) is True
    assert ph.can_edit_upload(2, 1, "admins", 6, 99) is False


def test_admin_mode_lets_global_admins_in(monkeypatch):
    _admins(monkeypatch, is_global=True)
    assert ph.can_edit_upload(2, 1, "admins", 6, 99) is True


def test_anyone_mode_lets_anyone_in(monkeypatch):
    _admins(monkeypatch)
    assert ph.can_edit_upload(2, 1, "anyone", 5, 99) is True


def test_unknown_mode_falls_back_to_uploader_only(monkeypatch):
    _admins(monkeypatch, is_global=True)
    assert ph.can_edit_upload(2, 1, "junk", 5, 99) is False


def test_modes_cycle_and_have_plain_labels():
    assert [ph.next_editor_mode(m) for m in ph.EDITOR_MODES] == ["admins", "anyone", "uploader"]
    assert ph.editor_mode_label("uploader") == "Uploader only"
    assert ph.editor_mode_label("admins") == "Uploader + admins"
    assert ph.editor_mode_label("anyone") == "Anyone"


# --- storage ------------------------------------------------------------------------------


@pytest.fixture
def alliance_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with sqlite3.connect("db/alliance.sqlite") as c:
        c.execute("CREATE TABLE alliancesettings (alliance_id INTEGER PRIMARY KEY, "
                  "ocr_upload_admin_only INTEGER DEFAULT 0, ocr_review_editors TEXT DEFAULT 'uploader')")
    return tmp_path


def test_attendance_review_editors_default_and_persist(alliance_db):
    from cogs import attendance_ocr_setup as setup
    assert setup.get_ocr_review_editors(5) == "uploader"
    setup.set_ocr_review_editors(5, "anyone")
    assert setup.get_ocr_review_editors(5) == "anyone"


def test_attendance_review_editors_rejects_unknown_mode(alliance_db):
    from cogs import attendance_ocr_setup as setup
    with pytest.raises(ValueError):
        setup.set_ocr_review_editors(5, "everyone")


def test_bear_setting_column_is_allowed():
    from cogs import bear_track
    src = __import__("inspect").getsource(bear_track.BearTrack.update_bear_setting)
    assert "bear_review_editors" in src


# --- the checks on the review screens --------------------------------------------------------


def _interaction(user_id):
    sent = []

    async def send_message(*a, **k):
        sent.append(k)

    return types.SimpleNamespace(user=types.SimpleNamespace(id=user_id), guild_id=99,
                                 response=types.SimpleNamespace(send_message=send_message)), sent


def test_bear_review_follows_the_alliance_setting(monkeypatch):
    from cogs import bear_track
    _admins(monkeypatch)
    inter, sent = _interaction(2)
    review = types.SimpleNamespace(
        original_user_id=1, alliance_id=5,
        cog=types.SimpleNamespace(get_bear_settings=lambda aid: {"review_editors": "anyone"}))
    assert asyncio.run(bear_track.BearHuntReviewView._can_edit(review, inter)) is True
    review.cog = types.SimpleNamespace(get_bear_settings=lambda aid: {"review_editors": "uploader"})
    assert asyncio.run(bear_track.BearHuntReviewView._can_edit(review, inter)) is False
    assert sent and sent[0]["ephemeral"] is True


def test_attendance_review_follows_the_alliance_setting(monkeypatch):
    from cogs import attendance_ocr_parsers as parsers
    _admins(monkeypatch, alliance_ids=[5])
    monkeypatch.setattr(parsers, "_review_editor_mode", lambda channel_id: (5, "admins"))
    session = types.SimpleNamespace(uploader_id=1, channel=types.SimpleNamespace(id=42))
    inter, sent = _interaction(2)
    assert asyncio.run(parsers.can_edit_session(session, inter)) is True
    monkeypatch.setattr(parsers, "_review_editor_mode", lambda channel_id: (5, "uploader"))
    assert asyncio.run(parsers.can_edit_session(session, inter)) is False
    assert sent and sent[0]["ephemeral"] is True
