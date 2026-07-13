"""Backups must capture committed data that still lives in a WAL file.

The old flow ran PRAGMA wal_checkpoint(TRUNCATE) (result ignored - busy
checkpoints silently no-op) and then raw-copied only the .sqlite main files
into the zip while the bot kept writing. Transactions living only in -wal
were absent from the backup, and a mid-copy auto-checkpoint could tear the
main file. The snapshot must come from SQLite's online backup API instead.
"""
import importlib
import os
import sqlite3
import zipfile

bb = importlib.import_module("cogs.bot_backup")


def test_backup_zip_contains_wal_resident_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("db")

    writer = sqlite3.connect("db/live.sqlite")
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE t (x INTEGER)")
    writer.execute("INSERT INTO t VALUES (42)")
    writer.commit()  # committed, but resident in live.sqlite-wal
    assert os.path.getsize("db/live.sqlite-wal") > 0, "precondition: data in WAL"

    cog = bb.BackupOperations.__new__(bb.BackupOperations)
    out = tmp_path / "backup.zip"
    try:
        cog._write_db_zip(str(out), None, "readme")
    finally:
        writer.close()

    restored = tmp_path / "restored"
    with zipfile.ZipFile(out) as zf:
        zf.extract("live.sqlite", restored)

    conn = sqlite3.connect(restored / "live.sqlite")
    try:
        rows = conn.execute("SELECT x FROM t").fetchall()
    finally:
        conn.close()
    assert rows == [(42,)], "backup must include committed WAL-resident rows"
