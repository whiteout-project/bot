"""post_redemption_summary is a module-level function (no `self`). Its failed
bucket had leftover `self.message` code that raised NameError whenever a code
had failures and the summary was enabled — surfacing as "An unexpected error
occurred processing <code>" after the summary posted.
"""
import asyncio
import logging
import types

import cogs.gift_redemption as gr


def test_failed_bucket_posts_without_self_error(monkeypatch):
    sent = []

    class Chan:
        async def send(self, **kw):
            sent.append(kw.get("embed"))

    monkeypatch.setattr(
        gr, "get_summary_settings",
        lambda cog, aid: {"enabled": 1, "success": 0, "already": 0, "failed": 1},
    )
    cog = types.SimpleNamespace(logger=logging.getLogger("test"))
    failed = {123: ("Bob", "Furnace level too low", 1)}

    # Must not raise NameError('self') — and must actually post the Failed embed.
    asyncio.run(gr.post_redemption_summary(cog, Chan(), 2, "SIR", "CODE", [], [], failed))

    assert any(e is not None and "Failed" in (e.title or "") for e in sent)
