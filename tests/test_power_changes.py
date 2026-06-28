import pytest
from cogs import power_changes as pc


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "changes.sqlite"
    monkeypatch.setattr(pc, "_CHANGES_DB", str(path))
    pc.ensure_tables()
    return str(path)


def test_record_then_latest_delta(db):
    pc.record_change(1, "power", 100, 150, "2026-06-27T10:00:00")
    d = pc.latest_delta(1, "power")
    assert d["old"] == 100 and d["new"] == 150
    assert round(d["pct"], 1) == 50.0
    assert d["change_date"] == "2026-06-27T10:00:00"


def test_no_baseline_returns_none(db):
    assert pc.latest_delta(99, "power") is None


def test_record_skips_when_old_none_or_equal_or_zero(db):
    pc.record_change(2, "power", None, 100, "t")     # first-ever: no baseline
    pc.record_change(2, "power", 100, 100, "t")      # unchanged
    pc.record_change(2, "power", 100, 0, "t")        # overwrite-to-zero ignored
    assert pc.latest_delta(2, "power") is None


def test_latest_delta_picks_most_recent(db):
    pc.record_change(3, "power", 100, 200, "2026-01-01T00:00:00")
    pc.record_change(3, "power", 200, 180, "2026-02-01T00:00:00")
    d = pc.latest_delta(3, "power")
    assert d["old"] == 200 and d["new"] == 180
    assert d["pct"] < 0


def test_deltas_at_filters_by_change_date(db):
    pc.record_change(4, "power", 100, 200, "2026-01-01T00:00:00")
    pc.record_change(4, "power", 200, 300, "2026-03-03T09:00:00")
    at = pc.deltas_at([4, 5], "power", "2026-03-03T09:00:00")
    assert set(at.keys()) == {4}
    assert at[4]["new"] == 300


def test_latest_deltas_batch(db):
    pc.record_change(6, "power", 10, 20, "t1")
    pc.record_change(7, "power", 50, 40, "t2")
    out = pc.latest_deltas([6, 7, 8], "power")
    assert out[6]["new"] == 20 and out[7]["new"] == 40
    assert 8 not in out


def test_history_newest_first(db):
    pc.record_change(9, "combat_power", 100, 200, "2026-01-01T00:00:00")
    pc.record_change(9, "combat_power", 200, 250, "2026-02-01T00:00:00")
    h = pc.history(9, "combat_power")
    assert [r["new"] for r in h] == [250, 200]


def test_format_delta():
    assert pc.format_delta(None) == pc.theme.newIcon
    assert pc.format_delta(12.0) == f"{pc.theme.upIcon} +12%"
    assert pc.format_delta(-7.0) == f"{pc.theme.downIcon} -7%"
    assert pc.format_delta(0.0) == f"{pc.theme.forwardIcon} 0%"


def test_reads_safe_on_cold_db(tmp_path, monkeypatch):
    monkeypatch.setattr(pc, "_CHANGES_DB", str(tmp_path / "cold.sqlite"))
    # No ensure_tables() call here - reads must not raise on a missing table.
    assert pc.latest_delta(1, "power") is None
    assert pc.latest_deltas([1, 2], "power") == {}
    assert pc.deltas_at([1], "power", "t") == {}
    assert pc.history(1, "power") == []
