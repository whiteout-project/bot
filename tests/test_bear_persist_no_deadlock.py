"""Regression: bear hunt submit must not deadlock on the SQLite write lock.

The bug: `_persist_hunt_and_render` learned OCR aliases *inside* its open
`bear_conn` write transaction. `learn_alias` opens its own connection to the
same `bear_data.sqlite`, so it blocked on the busy-timeout (~30s per matched
row), freezing the event loop. Fix: learn aliases after the commit.

This drives the real persist path with both the persistent connection and
`BEAR_DB_PATH` pointed at one temp DB (as in production), so a reintroduced
in-transaction learn would actually deadlock — caught by the time bound.
"""
from __future__ import annotations

import asyncio
import sqlite3
import time
from unittest.mock import AsyncMock, MagicMock

from harness import bt


def test_bear_submit_no_write_lock_deadlock(tmp_path, monkeypatch):
    bear_db = tmp_path / "bear_data.sqlite"
    alliance_db = tmp_path / "alliance.sqlite"
    monkeypatch.setattr(bt, "BEAR_DB_PATH", str(bear_db))
    bt.init_bear_database()  # bear_hunts / bear_player_damage / ocr_name_alias

    bear_conn = sqlite3.connect(str(bear_db), timeout=30.0, check_same_thread=False)
    bear_conn.execute("PRAGMA journal_mode=WAL")
    bear_conn.commit()
    alliance_conn = sqlite3.connect(str(alliance_db), check_same_thread=False)
    alliance_conn.execute(
        "CREATE TABLE alliancesettings (alliance_id INTEGER PRIMARY KEY, bear_damage_range INTEGER)")
    alliance_conn.commit()

    ds = bt.DataSubmit(alliance_conn, bear_conn)

    interaction = MagicMock()
    interaction.response.is_done = MagicMock(return_value=False)
    interaction.response.defer = AsyncMock()
    interaction.followup.send = AsyncMock()
    interaction.edit_original_response = AsyncMock()

    rows = [
        {"fid": 1000 + i, "name": f"P{i}", "nickname": f"Nick{i}",
         "damage": 5000 - i, "rank": i + 1, "candidates": []}
        for i in range(3)
    ]

    t0 = time.monotonic()
    asyncio.run(ds.process_full_submission(
        interaction,
        hunt_meta={"date": "2026-06-05", "hunting_trap": 2,
                   "rallies": 40, "total_damage": 123456},
        player_rows=rows, alliance_id=7, alliance_name="SIR",
    ))
    elapsed = time.monotonic() - t0

    # Buggy version blocks ~30s PER matched row on the busy-timeout; fixed is
    # near-instant (plus chart render). Generous bound, still well under one
    # 30s lock wait.
    assert elapsed < 20, f"bear submit took {elapsed:.1f}s — write-lock deadlock?"

    # Hunt persisted, and aliases learned (after commit).
    assert bear_conn.execute("SELECT COUNT(*) FROM bear_hunts").fetchone()[0] == 1
    learned = bear_conn.execute("SELECT COUNT(*) FROM bear_name_alias").fetchone()[0]
    assert learned == 3
    bear_conn.close()
    alliance_conn.close()
