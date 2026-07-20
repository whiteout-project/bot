import asyncio

from cogs.attendance_ocr_parsers import OcrUploadSession


def _bare_session(timeout=0.0):
    """An OcrUploadSession with only the timer machinery wired up."""
    s = OcrUploadSession.__new__(OcrUploadSession)
    s._timer_task = None
    s.timeout_seconds = timeout
    s.finalized = False
    s.cancelled = False
    return s


def test_timeout_finalize_is_not_self_cancelled():
    """The timeout path calls stop_timer from inside _timer_run. Cancelling that
    running task would kill finalize before it renders, leaving a stuck message
    with a dead Done button."""
    s = _bare_session()
    rendered = []

    async def fake_finalize(*, timed_out=False):
        if s.finalized or s.cancelled:
            return
        s.finalized = True
        OcrUploadSession.stop_timer(s)
        await asyncio.sleep(0)          # a self-cancel would surface here
        rendered.append(timed_out)

    s.finalize = fake_finalize

    async def run():
        OcrUploadSession.restart_timer(s)
        await asyncio.sleep(0.05)

    asyncio.run(run())
    assert rendered == [True]           # finalize ran to completion
    assert s._timer_task is None


def test_stop_timer_still_cancels_a_foreign_task():
    """Clicking Done (or a new upload) must still cancel the pending timer."""
    s = _bare_session(timeout=30)
    ticked = []

    async def run():
        OcrUploadSession.restart_timer(s)
        task = s._timer_task
        await asyncio.sleep(0)          # let the timer reach its sleep
        OcrUploadSession.stop_timer(s)  # called from outside the timer task
        await asyncio.sleep(0)
        ticked.append(task.cancelled() or task.done())

    asyncio.run(run())
    assert ticked == [True]
    assert s._timer_task is None
