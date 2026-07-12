"""A missing/unreachable alliance progress channel must NOT stop redemption.
Before the fix, use_giftcode_for_alliance returned False at the channel check
and never touched a member (the cause of "nobody got the rewards" in the
support logs). It should redeem every member and just skip the live posts.
"""
import asyncio
import logging
import sqlite3
import types

import cogs.gift_redemption as gr


def test_null_channel_still_redeems_all_members(monkeypatch):
    # alliance settings with a NULL channel_id (the real-world broken state)
    ac = sqlite3.connect(":memory:")
    ac.execute("CREATE TABLE alliancesettings (alliance_id INTEGER, channel_id INTEGER, redemption_channel_id INTEGER)")
    ac.execute("INSERT INTO alliancesettings VALUES (5, NULL, NULL)")
    ac.execute("CREATE TABLE alliance_list (alliance_id INTEGER, name TEXT)")
    ac.execute("INSERT INTO alliance_list VALUES (5, 'TestAlli')")
    ac.commit()

    mc = sqlite3.connect(":memory:")
    mc.execute("CREATE TABLE gift_codes (giftcode TEXT, validation_status TEXT)")
    mc.execute("CREATE TABLE user_giftcodes (fid INTEGER, giftcode TEXT, status TEXT)")
    mc.commit()

    cog = types.SimpleNamespace(
        alliance_cursor=ac.cursor(),
        cursor=mc.cursor(),
        get_test_fid=lambda: 999,
        captcha_solver=object(),          # truthy = solver ready
        bot=types.SimpleNamespace(get_channel=lambda cid: None, get_cog=lambda name: None),
        logger=logging.getLogger("test"),
    )

    # in-memory users.sqlite (the function opens this path directly)
    users_mem = sqlite3.connect(":memory:")
    users_mem.execute("CREATE TABLE users (fid INTEGER, nickname TEXT, alliance TEXT)")
    for fid, nick in [(1, "a"), (2, "b"), (3, "c")]:
        users_mem.execute("INSERT INTO users VALUES (?,?,?)", (fid, nick, "5"))
    users_mem.commit()
    real_connect = sqlite3.connect

    def fake_connect(path, *a, **k):
        if str(path).endswith("users.sqlite"):
            return users_mem
        return real_connect(path, *a, **k)

    claimed = []

    async def fake_claim(cog, fid, giftcode, **kw):
        claimed.append(fid)
        return "SUCCESS"

    async def _anoop(*a, **k):
        pass

    monkeypatch.setattr(gr.sqlite3, "connect", fake_connect)
    monkeypatch.setattr(gr.random, "uniform", lambda a, b: 0)  # no per-member delay
    monkeypatch.setattr(gr, "claim_giftcode_rewards_wos", fake_claim)
    monkeypatch.setattr(gr, "batch_get_user_giftcode_status", lambda cog, code, ids: {})
    monkeypatch.setattr(gr, "batch_process_alliance_results", lambda cog, results: None)
    monkeypatch.setattr(gr, "post_redemption_summary", _anoop)

    result = asyncio.run(gr.use_giftcode_for_alliance(cog, 5, "CODE"))

    assert result is True                 # did not bail on the missing channel
    assert sorted(claimed) == [1, 2, 3]   # every member was processed
