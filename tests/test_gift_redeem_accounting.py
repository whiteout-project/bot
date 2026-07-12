"""A gift_redeem handler must report the batch outcome that actually happened.
When use_giftcode_for_alliance bails (e.g. no channel, invalid code) it returns
False without raising; the handler used to record success=True regardless, so a
redemption that reached zero members still showed green. The batch result must
follow the return value.
"""
import asyncio
import logging
import types

import cogs.gift_redemption as gr


def _run_handler(monkeypatch, use_return):
    recorded = {}

    async def fake_use(cog, alliance_id, giftcode, process=None):
        return use_return

    async def fake_start(cog, batch_id, alliance_id):
        pass

    async def fake_result(cog, batch_id, alliance_id, success):
        recorded['success'] = success

    monkeypatch.setattr(gr, 'use_giftcode_for_alliance', fake_use)
    monkeypatch.setattr(gr, '_record_batch_start', fake_start)
    monkeypatch.setattr(gr, '_record_batch_result', fake_result)

    cog = types.SimpleNamespace(logger=logging.getLogger('test'), captcha_solver=None)
    process = {'id': 1, 'alliance_id': 5, 'details': {'giftcode': 'CODE', 'batch_id': 'b1'}}
    asyncio.run(gr.handle_gift_redeem_process(cog, process))
    return recorded


def test_bailed_redemption_records_failure(monkeypatch):
    recorded = _run_handler(monkeypatch, use_return=False)
    assert recorded.get('success') is False


def test_successful_redemption_records_success(monkeypatch):
    recorded = _run_handler(monkeypatch, use_return=True)
    assert recorded.get('success') is True
