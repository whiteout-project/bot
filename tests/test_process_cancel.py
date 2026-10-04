"""Stopping queue jobs from Bot Health: queued jobs are removed, the running one stops at its next check."""
import asyncio
import sqlite3
import types

import pytest

from cogs import process_queue as pqm


@pytest.fixture
def queue(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "db").mkdir()
    with sqlite3.connect("db/settings.sqlite") as c:
        c.execute("""CREATE TABLE process_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, action TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'queued', priority INTEGER NOT NULL, alliance_id INTEGER,
            details TEXT NOT NULL DEFAULT '{}', created_at TEXT NOT NULL, completed_at TEXT)""")
    q = pqm.ProcessQueue(types.SimpleNamespace())
    yield q
    q.conn.close()


def _status(q, pid):
    row = q.conn.execute("SELECT status FROM process_queue WHERE id = ?", (pid,)).fetchone()
    return row[0] if row else None


def test_cancel_queued_job_removes_it(queue):
    pid = queue.enqueue('gift_redeem', pqm.GIFT_REDEEM, details={'giftcode': 'ABC'})
    assert queue.cancel_process(pid) == 'removed'
    assert _status(queue, pid) is None


def test_cancel_unknown_job_is_gone(queue):
    assert queue.cancel_process(999) == 'gone'


def test_cancel_running_job_flags_it_and_processor_marks_it_cancelled(queue):
    pid = queue.enqueue('state_resolve', pqm.STATE_RESOLVE, details={'mode': 'auto'})
    seen = {}

    async def handler(process):
        seen['running'] = queue.running_info()
        assert queue.should_preempt() is False
        assert queue.cancel_process(process['id']) == 'stopping'
        assert queue.should_preempt() is True
        raise pqm.PreemptedException()

    queue.register_handler('state_resolve', handler)

    async def run_once():
        task = asyncio.create_task(queue._processor_loop())
        for _ in range(50):
            await asyncio.sleep(0.01)
            if _status(queue, pid) != 'active' and 'running' in seen:
                break
        queue._shutting_down = True
        queue._wake_event.set()
        await asyncio.wait_for(task, 2)

    asyncio.run(run_once())
    assert seen['running']['id'] == pid and seen['running']['action'] == 'state_resolve'
    assert _status(queue, pid) == 'cancelled'
    assert queue.running_info() is None


def test_stoppable_actions_are_the_ones_that_check_in():
    assert set(pqm.STOPPABLE_ACTIONS) == {'gift_redeem', 'member_add', 'state_resolve'}


def test_queued_processes_lists_waiting_jobs_in_run_order(queue):
    low = queue.enqueue('state_resolve', pqm.STATE_RESOLVE)
    high = queue.enqueue('gift_redeem', pqm.GIFT_REDEEM)
    assert [p['id'] for p in queue.queued_processes()] == [high, low]


def test_stop_requested_only_for_the_flagged_running_job(queue):
    queue._current_process = {'id': 5, 'action': 'gift_redeem', 'priority': 200}
    assert queue.stop_requested() is False
    assert queue.cancel_process(5) == 'stopping'
    assert queue.stop_requested() is True


def test_running_job_that_cannot_stop_is_reported(queue):
    queue._current_process = {'id': 6, 'action': 'gift_validate', 'priority': 100}
    assert queue.cancel_process(6) == 'unstoppable'
    assert queue.stop_requested() is False


def test_removing_a_queued_job_drops_its_runtime_context(queue):
    pid = queue.enqueue('member_add', pqm.MEMBER_ADD)
    queue.attach_runtime_context(pid, {'interaction': object()})
    queue.cancel_process(pid)
    assert queue.get_runtime_context(pid) == {}
