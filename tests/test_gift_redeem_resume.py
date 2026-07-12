"""On a resumed redemption (bot restart re-runs the queued process), the
per-alliance progress message must be reused, not re-posted. We persist its id
into the process details on first post and re-fetch it on resume — only posting
a fresh message when none is saved or it can't be fetched.
"""
import asyncio
import logging
import sqlite3
import types

import cogs.gift_redemption as gr


class FakeMsg:
    def __init__(self, mid):
        self.id = mid
        self.edits = []

    async def edit(self, **kw):
        self.edits.append(kw)


class FakeChannel:
    def __init__(self, fetchable=None):
        self.sends = []
        self.fetched = []
        self._fetchable = fetchable or {}

    async def send(self, **kw):
        m = FakeMsg(999)
        self.sends.append(m)
        return m

    async def fetch_message(self, mid):
        self.fetched.append(mid)
        if mid in self._fetchable:
            return self._fetchable[mid]
        raise Exception("message not found")


class FakePQ:
    def __init__(self):
        self.updated = {}

    def should_preempt(self):
        return False

    def update_details(self, pid, details):
        self.updated[pid] = details


def _run(monkeypatch, channel, process):
    ac = sqlite3.connect(":memory:")
    ac.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, channel_id INTEGER, redemption_channel_id INTEGER)")
    ac.execute("INSERT INTO alliancesettings VALUES (5, NULL, 777)")
    ac.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    ac.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    ac.commit()
    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT, validation_status TEXT)")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    mc.commit()

    pq = FakePQ()
    cog = types.SimpleNamespace(
        alliance_cursor=ac.cursor(),
        cursor=mc.cursor(),
        get_test_fid=lambda: 999,
        captcha_solver=object(),
        bot=types.SimpleNamespace(get_channel=lambda cid: channel,
                                  get_cog=lambda name: pq),
        logger=logging.getLogger("test"),
    )

    users_mem = sqlite3.connect(":memory:")
    users_mem.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT)")
    for fid, nick in [(1, "a"), (2, "b")]:
        users_mem.execute("INSERT INTO users VALUES (?,?,?)", (fid, nick, "5"))
    users_mem.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("users.sqlite"):
            return users_mem
        return real_connect(path, *a, **k)

    async def fake_claim(cog, fid, giftcode, **kw):
        return "SUCCESS"

    async def _anoop(*a, **k):
        pass

    monkeypatch.setattr(gr.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(gr.random, "uniform", lambda a, b: 0)
    monkeypatch.setattr(gr, "claim_giftcode_rewards_wos", fake_claim)
    monkeypatch.setattr(gr, "batch_get_user_giftcode_status", lambda cog, code, ids: {})
    monkeypatch.setattr(gr, "batch_process_alliance_results", lambda cog, results: None)
    monkeypatch.setattr(gr, "post_redemption_summary", _anoop)

    asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE", process=process))
    return pq


def test_first_run_posts_and_persists_message_id(monkeypatch):
    ch = FakeChannel()
    process = {"id": 42, "details": {"giftcode": "CODE"}}
    pq = _run(monkeypatch, ch, process)
    assert len(ch.sends) == 1                                   # posted once
    assert pq.updated.get(42, {}).get("progress_message_id") == 999
    assert process["details"].get("progress_message_id") == 999  # in-memory synced


def test_resume_reuses_saved_message(monkeypatch):
    existing = FakeMsg(555)
    ch = FakeChannel(fetchable={555: existing})
    process = {"id": 42, "details": {"giftcode": "CODE", "progress_message_id": 555}}
    _run(monkeypatch, ch, process)
    assert ch.sends == []             # never re-posted
    assert 555 in ch.fetched         # fetched the saved message
    assert existing.edits            # and edited it in place


def test_resume_reposts_when_saved_message_gone(monkeypatch):
    ch = FakeChannel(fetchable={})    # 555 not fetchable -> deleted
    process = {"id": 42, "details": {"giftcode": "CODE", "progress_message_id": 555}}
    pq = _run(monkeypatch, ch, process)
    assert 555 in ch.fetched
    assert len(ch.sends) == 1                                   # fresh post
    assert pq.updated.get(42, {}).get("progress_message_id") == 999  # re-persisted
