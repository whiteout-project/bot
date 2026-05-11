"""
Persistent process queue with priority and cooperative preemption. Coordinates
all bulk API operations (gift code redemption, alliance sync, member adds) so
high-priority work interrupts lower-priority work and queued operations
survive bot restarts.
"""
import asyncio
import json
import logging
import sqlite3
from datetime import datetime
from typing import Callable, Dict, Optional

from discord.ext import commands

logger = logging.getLogger('bot')

# Priority constants (lower = higher priority)
GIFT_VALIDATE = 100
GIFT_REDEEM = 200
MEMBER_ADD = 300
ALLIANCE_CONTROL = 400
ALLIANCE_SYNC = 500


class PreemptedException(Exception):
    """Raised by a handler to signal it was preempted by higher-priority work.

    The processor catches this and re-queues the process so it runs again
    after the higher-priority work completes.
    """


class ProcessQueue(commands.Cog):
    """
    SQLite-backed priority queue for all bulk API operations.

    Operations are stored as rows in `process_queue` (in db/settings.sqlite)
    with serializable params, so they survive crashes. The processor loop
    always picks the highest-priority queued process. Long-running operations
    (gift redemption, alliance sync, member add) call `should_preempt()`
    between players to yield cooperatively to higher-priority work.
    """

    def __init__(self, bot):
        self.bot = bot
        self.conn = sqlite3.connect('db/settings.sqlite', timeout=30.0, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.commit()
        self.cursor = self.conn.cursor()

        self._handlers: Dict[str, Callable] = {}
        self._processor_task: Optional[asyncio.Task] = None
        self._wake_event = asyncio.Event()
        self._current_process: Optional[Dict] = None
        # Runtime context for non-serializable references (Discord interactions, messages).
        # Lost on restart by design — handlers should fall back gracefully when missing.
        self._runtime_contexts: Dict[int, Dict] = {}

        logger.info("ProcessQueue cog initialized")

    def cog_unload(self):
        if self._processor_task and not self._processor_task.done():
            self._processor_task.cancel()
        try:
            self.conn.close()
        except Exception:
            pass

    # ── Handler registration ──────────────────────────────────────────

    def register_handler(self, action: str, handler: Callable):
        """Register an async handler for an action type.

        Handler signature: async def handler(process: dict) -> None
        The process dict contains: id, action, status, priority, alliance_id,
        details, created_at. Handlers should read from `details` (JSON payload)
        and may call `should_preempt()` to yield to higher-priority work.
        """
        self._handlers[action] = handler
        logger.info(f"ProcessQueue: Registered handler for action '{action}'")

    # ── Queue operations ─────────────────────────────────────────────

    def enqueue(self, action: str, priority: int, alliance_id: Optional[int] = None,
                details: Optional[Dict] = None) -> int:
        """Insert a new process into the queue.

        Returns the new process ID.
        """
        details_json = json.dumps(details or {})
        created_at = datetime.now().isoformat()

        self.cursor.execute("""
            INSERT INTO process_queue (action, status, priority, alliance_id, details, created_at)
            VALUES (?, 'queued', ?, ?, ?, ?)
        """, (action, priority, alliance_id, details_json, created_at))
        self.conn.commit()
        process_id = self.cursor.lastrowid

        logger.info(f"ProcessQueue: Enqueued {action} (id={process_id}, priority={priority}, alliance={alliance_id})")

        # Wake the processor
        self._wake_event.set()

        return process_id

    def get_next_queued(self) -> Optional[Dict]:
        """Get the highest-priority queued process, or None if queue is empty."""
        self.cursor.execute("""
            SELECT id, action, status, priority, alliance_id, details, created_at
            FROM process_queue
            WHERE status = 'queued'
            ORDER BY priority ASC, id ASC
            LIMIT 1
        """)
        row = self.cursor.fetchone()
        if not row:
            return None
        return self._row_to_dict(row)

    def _row_to_dict(self, row) -> Dict:
        return {
            'id': row[0],
            'action': row[1],
            'status': row[2],
            'priority': row[3],
            'alliance_id': row[4],
            'details': json.loads(row[5]) if row[5] else {},
            'created_at': row[6],
        }

    def mark_active(self, process_id: int):
        self.cursor.execute(
            "UPDATE process_queue SET status = 'active' WHERE id = ?",
            (process_id,)
        )
        self.conn.commit()

    def mark_completed(self, process_id: int):
        self.cursor.execute(
            "UPDATE process_queue SET status = 'completed', completed_at = ? WHERE id = ?",
            (datetime.now().isoformat(), process_id)
        )
        self.conn.commit()

    def mark_failed(self, process_id: int):
        self.cursor.execute(
            "UPDATE process_queue SET status = 'failed', completed_at = ? WHERE id = ?",
            (datetime.now().isoformat(), process_id)
        )
        self.conn.commit()

    def requeue(self, process_id: int):
        """Set a process back to 'queued' (used when preempted by higher priority work)."""
        self.cursor.execute(
            "UPDATE process_queue SET status = 'queued' WHERE id = ?",
            (process_id,)
        )
        self.conn.commit()

    def update_details(self, process_id: int, details: Dict):
        # Overwrites the full details blob — callers checkpointing before a
        # PreemptedException must include every original field.
        self.cursor.execute(
            "UPDATE process_queue SET details = ? WHERE id = ?",
            (json.dumps(details or {}), process_id),
        )
        self.conn.commit()

    def has_higher_priority_waiting(self, current_priority: int) -> bool:
        """Check if a higher-priority process is queued (for cooperative preemption)."""
        self.cursor.execute("""
            SELECT 1 FROM process_queue
            WHERE status = 'queued' AND priority < ?
            LIMIT 1
        """, (current_priority,))
        return self.cursor.fetchone() is not None

    def should_preempt(self) -> bool:
        """Check if the currently-running process should yield to higher-priority work.

        Returns True if there's a queued process with strictly higher priority
        than the currently active one.
        """
        if not self._current_process:
            return False
        return self.has_higher_priority_waiting(self._current_process['priority'])

    def get_queue_info(self) -> Dict:
        """Get queue size and processing state for UI display."""
        self.cursor.execute("SELECT COUNT(*) FROM process_queue WHERE status = 'queued'")
        queue_size = self.cursor.fetchone()[0]
        return {
            'queue_size': queue_size,
            'is_processing': self._current_process is not None,
        }

    def get_queued_processes_by_action(self, action: str) -> list:
        """Get all queued processes for a given action type."""
        self.cursor.execute("""
            SELECT id, action, status, priority, alliance_id, details, created_at
            FROM process_queue
            WHERE status = 'queued' AND action = ?
            ORDER BY priority ASC, id ASC
        """, (action,))
        return [self._row_to_dict(row) for row in self.cursor.fetchall()]

    def get_position(self, process_id: int) -> Optional[int]:
        """Return the 1-based queue position of `process_id` (1 = next to run,
        counting any active jobs as position 1). Returns None if the process
        is no longer in the queue (already completed/cancelled)."""
        self.cursor.execute(
            "SELECT priority, status FROM process_queue WHERE id = ?",
            (process_id,),
        )
        row = self.cursor.fetchone()
        if not row:
            return None
        priority, status = row
        if status not in ('queued', 'active'):
            return None
        self.cursor.execute("""
            SELECT COUNT(*) FROM process_queue
            WHERE status IN ('queued','active')
              AND (priority < ? OR (priority = ? AND id < ?))
        """, (priority, priority, process_id))
        ahead = self.cursor.fetchone()[0] or 0
        return ahead + 1

    def has_queued_or_active(self, action: str, alliance_id: Optional[int] = None) -> bool:
        if alliance_id is None:
            self.cursor.execute(
                "SELECT 1 FROM process_queue WHERE status IN ('queued','active') AND action = ? LIMIT 1",
                (action,),
            )
        else:
            self.cursor.execute(
                "SELECT 1 FROM process_queue WHERE status IN ('queued','active') AND action = ? AND alliance_id = ? LIMIT 1",
                (action, alliance_id),
            )
        return self.cursor.fetchone() is not None

    # ── Runtime context (non-serializable references) ────────────────

    def attach_runtime_context(self, process_id: int, context: Dict):
        """Attach Discord interactions/messages to a process for live UI updates.

        Lost on restart by design — handlers must fall back gracefully when missing.
        """
        self._runtime_contexts[process_id] = context

    def get_runtime_context(self, process_id: int) -> Dict:
        """Get runtime context for a process, or empty dict if not present."""
        return self._runtime_contexts.get(process_id, {})

    def clear_runtime_context(self, process_id: int):
        """Remove runtime context once a process is done."""
        self._runtime_contexts.pop(process_id, None)

    # ── Processor loop ───────────────────────────────────────────────

    async def start_processor(self):
        """Start the background processor task if not already running."""
        if self._processor_task is None or self._processor_task.done():
            self._processor_task = asyncio.create_task(self._processor_loop())
            logger.info("ProcessQueue: Processor task started")

    async def _processor_loop(self):
        """Main processor loop. Picks highest-priority queued process and executes it."""
        while True:
            try:
                process = self.get_next_queued()

                if not process:
                    # No work — wait for wake signal
                    self._wake_event.clear()
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=30)
                    except asyncio.TimeoutError:
                        pass
                    continue

                action = process['action']
                handler = self._handlers.get(action)

                if not handler:
                    logger.warning(
                        f"ProcessQueue: No handler registered for action '{action}' "
                        f"(id={process['id']}); waiting 5s and retrying"
                    )
                    await asyncio.sleep(5)
                    continue

                self._current_process = process
                self.mark_active(process['id'])

                preempted = False
                try:
                    logger.info(f"ProcessQueue: Executing {action} (id={process['id']}, alliance={process['alliance_id']})")
                    await handler(process)
                    self.mark_completed(process['id'])
                    logger.info(f"ProcessQueue: Completed {action} (id={process['id']})")
                except PreemptedException:
                    preempted = True
                    self.requeue(process['id'])
                    logger.info(f"ProcessQueue: Preempted {action} (id={process['id']}); re-queued for later")
                except Exception as e:
                    logger.exception(f"ProcessQueue: Handler for {action} failed (id={process['id']}): {e}")
                    self.mark_failed(process['id'])
                finally:
                    if not preempted:
                        self.clear_runtime_context(process['id'])
                    self._current_process = None

            except asyncio.CancelledError:
                logger.info("ProcessQueue: Processor cancelled")
                break
            except Exception as e:
                logger.exception(f"ProcessQueue: Processor loop error: {e}")
                await asyncio.sleep(1)

    # ── Crash recovery ───────────────────────────────────────────────

    def recover_interrupted(self):
        """Reset any 'active' processes back to 'queued' for crash recovery.

        Called from on_ready. The processor will pick them up automatically.
        """
        self.cursor.execute("""
            UPDATE process_queue
            SET status = 'queued'
            WHERE status = 'active'
        """)
        recovered = self.cursor.rowcount
        self.conn.commit()

        if recovered > 0:
            logger.info(f"ProcessQueue: Recovered {recovered} interrupted process(es)")

        return recovered

    @commands.Cog.listener()
    async def on_ready(self):
        """Reset interrupted processes and start the processor.

        Waits briefly before starting so that other cogs have time to register
        their handlers (gift_operations, alliance_sync, alliance_member_operations).
        """
        if self._processor_task and not self._processor_task.done():
            return  # Already started

        self.recover_interrupted()

        # Give other cogs a few seconds to register handlers before processing
        await asyncio.sleep(3)
        await self.start_processor()


async def setup(bot):
    await bot.add_cog(ProcessQueue(bot))
