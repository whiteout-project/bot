"""Power / Combat Power change history: record, read, and format deltas."""
import logging
import sqlite3

from .pimp_my_bot import theme

logger = logging.getLogger(__name__)

_CHANGES_DB = "db/changes.sqlite"

METRICS = {
    "power": {
        "table": "power_changes",
        "old_col": "old_power",
        "new_col": "new_power",
    },
    "combat_power": {
        "table": "combat_power_changes",
        "old_col": "old_combat_power",
        "new_col": "new_combat_power",
    },
}


def ensure_tables() -> None:
    with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
        for m in METRICS.values():
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {m['table']} ("
                f"id INTEGER PRIMARY KEY AUTOINCREMENT, "
                f"fid INTEGER NOT NULL, "
                f"{m['old_col']} INTEGER NOT NULL, "
                f"{m['new_col']} INTEGER NOT NULL, "
                f"change_date TEXT NOT NULL)"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{m['table']}_fid "
                f"ON {m['table']}(fid)"
            )
        conn.commit()


def _pct(old, new):
    return ((new - old) / old * 100.0) if old else None


def record_change(fid, metric, old_value, new_value, change_date) -> None:
    if metric not in METRICS:
        return
    if old_value is None or new_value is None:
        return
    if int(new_value) == int(old_value) or not int(new_value):
        return
    m = METRICS[metric]
    try:
        ensure_tables()
        with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
            conn.execute(
                f"INSERT INTO {m['table']} "
                f"(fid, {m['old_col']}, {m['new_col']}, change_date) "
                f"VALUES (?, ?, ?, ?)",
                (fid, int(old_value), int(new_value), change_date),
            )
            conn.commit()
    except Exception as e:
        logger.error(f"record_change failed for fid={fid} metric={metric}: {e}")
        print(f"record_change failed for fid={fid} metric={metric}: {e}")


def _row_to_delta(old, new, change_date):
    return {"old": old, "new": new, "pct": _pct(old, new), "change_date": change_date}


def latest_delta(fid, metric):
    if metric not in METRICS:
        return None
    ensure_tables()
    m = METRICS[metric]
    with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
        row = conn.execute(
            f"SELECT {m['old_col']}, {m['new_col']}, change_date FROM {m['table']} "
            f"WHERE fid = ? ORDER BY change_date DESC, id DESC LIMIT 1",
            (fid,),
        ).fetchone()
    return _row_to_delta(*row) if row else None


def latest_deltas(fids, metric):
    if metric not in METRICS or not fids:
        return {}
    ensure_tables()
    m = METRICS[metric]
    placeholders = ",".join("?" for _ in fids)
    with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
        rows = conn.execute(
            f"SELECT fid, {m['old_col']}, {m['new_col']}, change_date FROM {m['table']} "
            f"WHERE fid IN ({placeholders}) ORDER BY change_date ASC, id ASC",
            tuple(fids),
        ).fetchall()
    out = {}
    for r in rows:
        out[r[0]] = _row_to_delta(r[1], r[2], r[3])  # ascending so last seen = most recent
    return out


def deltas_at(fids, metric, change_date):
    if metric not in METRICS or not fids:
        return {}
    ensure_tables()
    m = METRICS[metric]
    placeholders = ",".join("?" for _ in fids)
    with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
        rows = conn.execute(
            f"SELECT fid, {m['old_col']}, {m['new_col']}, change_date "
            f"FROM {m['table']} WHERE change_date = ? AND fid IN ({placeholders})",
            (change_date, *fids),
        ).fetchall()
    return {r[0]: _row_to_delta(r[1], r[2], r[3]) for r in rows}


def history(fid, metric):
    if metric not in METRICS:
        return []
    ensure_tables()
    m = METRICS[metric]
    with sqlite3.connect(_CHANGES_DB, timeout=30.0) as conn:
        rows = conn.execute(
            f"SELECT {m['old_col']}, {m['new_col']}, change_date FROM {m['table']} "
            f"WHERE fid = ? ORDER BY change_date DESC, id DESC",
            (fid,),
        ).fetchall()
    return [_row_to_delta(r[0], r[1], r[2]) for r in rows]


def format_delta(pct) -> str:
    if pct is None:
        return theme.newIcon
    if pct > 0:
        return f"{theme.upIcon} +{pct:.0f}%"
    if pct < 0:
        return f"{theme.downIcon} {pct:.0f}%"
    return f"{theme.forwardIcon} 0%"
