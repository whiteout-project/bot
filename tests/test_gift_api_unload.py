"""Reloading the Gift Code cog must shut down its GiftCodeAPI client.

GiftCodeAPI is not a cog - nothing called its cog_unload, so every reload
leaked the 5-10min API sync task (and its DB connections): duplicate "New
Gift Code Found" notifications and doubled validation traffic per reload.
"""
import asyncio
import importlib
from types import SimpleNamespace

go = importlib.import_module("cogs.gift_operations")


def test_cog_unload_shuts_down_api_client():
    cog = go.GiftOperations.__new__(go.GiftOperations)
    calls = []

    async def api_unload():
        calls.append("api")

    cog.api = SimpleNamespace(cog_unload=api_unload)

    asyncio.run(cog.cog_unload())

    assert calls == ["api"], "cog_unload must shut down the GiftCodeAPI client"


def test_cog_unload_survives_missing_api():
    cog = go.GiftOperations.__new__(go.GiftOperations)
    cog.api = None

    asyncio.run(cog.cog_unload())  # must not raise
