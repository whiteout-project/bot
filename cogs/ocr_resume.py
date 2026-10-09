"""Persist in-progress OCR upload sessions so a crash/restart doesn't lose parsed work."""
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone

import discord

os.makedirs("db", exist_ok=True)
_DB = "db/ocr_resume.sqlite"

# Prune snapshots older than this so a stuck row can't re-post recovery forever.
STALE_AFTER_HOURS = 6


def _conn():
    return sqlite3.connect(_DB, timeout=30.0, check_same_thread=False)


def _init():
    try:
        with _conn() as c:
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("CREATE TABLE IF NOT EXISTS ocr_snapshots (key TEXT PRIMARY KEY, kind TEXT, payload TEXT, updated_at TEXT)")
    except Exception:
        pass


_init()


def save(key, kind, payload):
    """Best-effort upsert of a session snapshot; never break OCR on a snapshot failure."""
    try:
        with _conn() as c:
            c.execute("INSERT OR REPLACE INTO ocr_snapshots (key, kind, payload, updated_at) VALUES (?, ?, ?, ?)",
                      (key, kind, json.dumps(payload), datetime.now(timezone.utc).isoformat()))
    except Exception:
        pass


def load_all(kind):
    """Return [(key, payload), ...] for kind, dropping rows older than STALE_AFTER_HOURS."""
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=STALE_AFTER_HOURS)).isoformat()
        with _conn() as c:
            c.execute("DELETE FROM ocr_snapshots WHERE kind = ? AND (updated_at IS NULL OR updated_at < ?)",
                      (kind, cutoff))
            rows = c.execute("SELECT key, payload FROM ocr_snapshots WHERE kind = ?", (kind,)).fetchall()
        return [(k, json.loads(p)) for k, p in rows]
    except Exception:
        return []


def delete(key):
    try:
        with _conn() as c:
            c.execute("DELETE FROM ocr_snapshots WHERE key = ?", (key,))
    except Exception:
        pass


async def resolve_channel(bot, channel_id):
    """(channel, gone) for a snapshot's channel. Only a NotFound means gone; an outage keeps the snapshot."""
    channel = bot.get_channel(channel_id) if channel_id else None
    if channel is not None or not channel_id:
        return channel, channel_id is None
    try:
        return await bot.fetch_channel(channel_id), False
    except discord.NotFound:
        return None, True
    except discord.HTTPException:
        return None, False
