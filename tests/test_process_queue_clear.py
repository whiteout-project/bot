import importlib
import sqlite3

pqmod = importlib.import_module("cogs.process_queue")


def _mk_pq(tmp_path):
    pq = pqmod.ProcessQueue.__new__(pqmod.ProcessQueue)  # bypass __init__ (no bot/DB file)
    conn = sqlite3.connect(tmp_path / "settings.sqlite")
    conn.execute(
        "CREATE TABLE process_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued', priority INTEGER NOT NULL, "
        "alliance_id INTEGER, details TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, "
        "completed_at TEXT)"
    )
    conn.commit()
    pq.conn = conn
    pq.cursor = conn.cursor()
    return pq


def _add(pq, status, priority=100):
    pq.cursor.execute(
        "INSERT INTO process_queue (action, status, priority, created_at) VALUES ('gift_redeem', ?, ?, 't')",
        (status, priority),
    )
    pq.conn.commit()


def test_queue_counts(tmp_path):
    pq = _mk_pq(tmp_path)
    for s in ("queued", "queued", "active", "failed", "completed"):
        _add(pq, s)
    assert pq.queue_counts() == {"queued": 2, "active": 1, "completed": 1, "failed": 1}


def test_queue_counts_empty(tmp_path):
    pq = _mk_pq(tmp_path)
    assert pq.queue_counts() == {"queued": 0, "active": 0, "completed": 0, "failed": 0}


def test_clear_processes_default_removes_queued_and_failed_only(tmp_path):
    pq = _mk_pq(tmp_path)
    for s in ("queued", "failed", "active", "completed"):
        _add(pq, s)
    removed = pq.clear_processes()   # default: queued + failed
    assert removed == 2
    counts = pq.queue_counts()
    assert counts["queued"] == 0 and counts["failed"] == 0
    assert counts["active"] == 1 and counts["completed"] == 1


def test_clear_processes_custom_statuses(tmp_path):
    pq = _mk_pq(tmp_path)
    for s in ("completed", "queued"):
        _add(pq, s)
    removed = pq.clear_processes(("completed",))
    assert removed == 1
    counts = pq.queue_counts()
    assert counts["completed"] == 0 and counts["queued"] == 1
