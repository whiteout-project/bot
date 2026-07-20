"""Theme emoji-edit / import chat sessions must actually expire.

Sessions stored 'timeout': 300 but nothing read it - days later any emoji or
image posted in that channel silently overwrote the theme icon (or imported
any attached JSON) and the user's message was deleted.
"""
import importlib
from types import SimpleNamespace

pmb = importlib.import_module("cogs.pimp_my_bot")


def _cog(sessions):
    cog = pmb.Theme.__new__(pmb.Theme)
    cog.emoji_edit_sessions = sessions
    return cog


def test_expired_session_is_dropped(monkeypatch):
    cog = _cog({"1_2": {"timeout": 300, "created_at": 1000.0}})
    monkeypatch.setattr(pmb.time, "time", lambda: 1000.0 + 301)
    assert cog.claim_emoji_session("1_2") is None
    assert cog.emoji_edit_sessions == {}, "expired session must be removed"


def test_fresh_session_is_claimed(monkeypatch):
    session = {"timeout": 300, "created_at": 1000.0}
    cog = _cog({"1_2": session})
    monkeypatch.setattr(pmb.time, "time", lambda: 1000.0 + 299)
    assert cog.claim_emoji_session("1_2") is session


def test_stampless_legacy_session_expires(monkeypatch):
    cog = _cog({"1_2": {"timeout": 300}})
    monkeypatch.setattr(pmb.time, "time", lambda: 1000.0)
    assert cog.claim_emoji_session("1_2") is None
