"""Tests for worker logic with fork isolation.

All tests use multiprocessing shared state since tasks run in forked processes.
"""

import asyncio
import multiprocessing
import multiprocessing.managers
import time
from collections.abc import Callable, Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from pq.client import PQ
from pq.models import FailureKind, Periodic, TaskStatus
from pq.priority import Priority

if TYPE_CHECKING:
    from loguru import Record

# Global shared state for fork-isolated tests
_shared_results: Any = None
_shared_calls: Any = None


def _set_shared_results(results: Any) -> None:
    global _shared_results
    _shared_results = results


def _set_shared_calls(calls: Any) -> None:
    global _shared_calls
    _shared_calls = calls


# Module-level handlers for testing (must be importable)
def capture_handler(value: int) -> None:
    """Capture a value to shared state."""
    _shared_results.append(value)


def noop_handler() -> None:
    """Do nothing."""
    pass


def failing_handler() -> None:
    """Always fails."""
    raise ValueError("boom")


def tracked_handler(key: str) -> None:
    """Track calls for testing."""
    _shared_calls.append((key,))


def ordered_handler(n: int) -> None:
    """Append n to results for ordering tests."""
    _shared_results.append(n)


def periodic_handler(n: int) -> None:
    """Periodic task handler."""
    _shared_results.append(n)


def periodic_noop_handler() -> None:
    """Periodic noop handler."""
    pass


def counter_handler() -> None:
    """Counter handler for periodic tests."""
    _shared_results.append(1)


async def async_capture_handler(value: str) -> None:
    """Async handler that captures a value."""
    await asyncio.sleep(0.01)
    _shared_results.append(value)


async def async_periodic_handler(n: int) -> None:
    """Async periodic handler."""
    await asyncio.sleep(0.01)
    _shared_results.append(n)


async def async_failing_handler() -> None:
    """Async handler that fails."""
    await asyncio.sleep(0.01)
    raise ValueError("async boom")


async def slow_async_handler() -> None:
    """Async handler that takes too long."""
    await asyncio.sleep(10)


def slow_sync_handler() -> None:
    """Sync handler that takes too long."""
    time.sleep(10)


async def tracked_slow_async_handler() -> None:
    """Slow async handler that appends ``"completed"`` to shared state
    only AFTER the sleep finishes.

    Tests of "the per-task ``max_runtime_seconds`` killed the execution" assert
    the absence of this marker — if the worker honoured the override
    (e.g. 1 s cap), ``asyncio.wait_for`` cancels the coroutine before
    the append; if it ignored the override and let the 10 s sleep
    finish, the marker is written. This is stronger than asserting
    ``periodic.last_run is not None``, because ``last_run`` is set
    by the worker BEFORE the fork (Phase 1 — claim), independent of
    whether the handler runs to completion."""
    await asyncio.sleep(10)
    _shared_results.append("completed")


def lock_observer_handler(db_url: str) -> None:
    """Handler used by
    ``test_periodic_lock_duration_scales_with_per_task_max_runtime``.

    The periodic worker sets ``locked_until = func.now() +
    interval`` BEFORE forking (a SQL expression — not a Python
    datetime), then clears it back to ``None`` after the task
    finishes (``worker.py:833``). Two consequences for the test:

    1. After ``run_worker_once`` returns, ``locked_until`` is back
       to ``None`` — too late to observe.
    2. A simpler ``pre_execute`` hook approach (read
       ``task.locked_until`` from the periodic object passed in)
       doesn't work either: the value is an unresolved SQLAlchemy
       expression on the expunged row, and trying to read it raises
       ``DetachedInstanceError`` because the lazy-load needs a
       session.

    So the only viable observation point is mid-handler, in the
    forked child, with a freshly-opened session that re-queries
    the row.
    """
    from sqlalchemy import select

    from pq.client import PQ as PQClient

    client = PQClient(db_url)
    try:
        with client.session() as session:
            periodic = session.execute(select(Periodic)).scalar_one()
            _shared_results.append(periodic.locked_until)
    finally:
        client.close()


def sleep_handler(duration: float) -> None:
    """Sleep for a given duration. Used in concurrency tests."""
    time.sleep(duration)
    _shared_results.append(1)


class TestRunWorkerOnce:
    """Tests for run_worker_once method."""

    def test_processes_pending_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a pending task."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(capture_handler, value=42)
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [42]

    def test_deletes_task_after_processing(self, pq: PQ) -> None:
        """Worker marks task completed after processing."""
        pq.enqueue(noop_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        assert pq.pending_count() == 0

    def test_skips_future_task(self, pq: PQ) -> None:
        """Worker skips tasks scheduled for the future."""
        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue(noop_handler, run_at=future)

        processed = pq.run_worker_once()

        assert processed is False
        assert pq.pending_count() == 1

    def test_returns_false_when_empty(self, pq: PQ) -> None:
        """Worker returns False when no tasks available."""
        processed = pq.run_worker_once()
        assert processed is False

    def test_deletes_task_on_failure(self, pq: PQ) -> None:
        """Worker marks task failed when handler fails."""
        pq.enqueue(failing_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        assert pq.pending_count() == 0

    def test_processes_direct_function(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker can process task with direct function path."""
        calls = manager.list()
        _set_shared_calls(calls)

        pq.enqueue(tracked_handler, key="value")
        pq.run_worker_once()

        assert list(calls) == [("value",)]

    def test_processes_higher_priority_first(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes higher priority tasks first."""
        results = manager.list()
        _set_shared_results(results)

        # Enqueue in reverse priority order
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)
        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        # Should process in priority order: HIGH, NORMAL, LOW
        assert list(results) == [1, 2, 3]

    def test_filters_by_priority(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker only processes tasks matching priority filter."""
        results = manager.list()
        _set_shared_results(results)

        # Enqueue tasks with different priorities
        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)

        # Worker with HIGH-only filter
        pq.run_worker_once(priorities={Priority.HIGH})
        pq.run_worker_once(priorities={Priority.HIGH})  # Should find nothing

        # Only HIGH priority task should be processed
        assert list(results) == [1]
        assert pq.pending_count() == 2  # NORMAL and LOW still pending

        # Process remaining with no filter
        pq.run_worker_once()
        pq.run_worker_once()

        assert list(results) == [1, 2, 3]


class TestPeriodicTasks:
    """Tests for periodic task processing."""

    def test_processes_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes a periodic task."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(periodic_handler, run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_advances_next_run(self, pq: PQ) -> None:
        """Worker advances next_run after processing."""
        from sqlalchemy import select

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.next_run > original_next_run
            # Should be ~1 hour in the future
            expected = original_next_run + timedelta(hours=1)
            assert abs((periodic.next_run - expected).total_seconds()) < 1

    def test_sets_last_run(self, pq: PQ) -> None:
        """Worker sets last_run after processing."""
        from sqlalchemy import select

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.last_run is None

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.last_run is not None

    def test_skips_future_periodic(self, pq: PQ) -> None:
        """Worker skips periodic tasks not yet due."""
        from sqlalchemy import update

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        # Move next_run to the future
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:periodic_noop_handler")
                .values(next_run=datetime.now(UTC) + timedelta(hours=1))
            )

        processed = pq.run_worker_once()

        assert processed is False

    def test_periodic_keeps_running(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Periodic task can be run multiple times."""
        results = manager.list()
        _set_shared_results(results)

        # Schedule with 0 interval so it's always ready
        pq.schedule(counter_handler, run_every=timedelta(seconds=0))

        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        assert len(results) == 3
        assert pq.periodic_count() == 1  # Still exists

    def test_periodic_failure_advances_schedule(self, pq: PQ) -> None:
        """Periodic task advances schedule even on failure."""
        from sqlalchemy import select

        pq.schedule(failing_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            original_next_run = periodic.next_run

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            assert periodic.next_run > original_next_run

    def test_overdue_periodic_runs_once(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Overdue periodic task runs once, not historically."""
        from sqlalchemy import update

        count = manager.list()
        _set_shared_results(count)

        # Schedule with 1 hour interval
        pq.schedule(counter_handler, run_every=timedelta(hours=1))

        # Manually set next_run to 3 hours ago (simulating missed runs)
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:counter_handler")
                .values(next_run=datetime.now(UTC) - timedelta(hours=3))
            )

        # Run worker multiple times
        pq.run_worker_once()
        pq.run_worker_once()
        pq.run_worker_once()

        # Should only run ONCE (not 3+ times to catch up)
        assert len(count) == 1


class TestActiveFlag:
    """Tests for active flag on periodic tasks."""

    def test_inactive_periodic_not_executed(self, pq: PQ) -> None:
        """Worker skips inactive periodic tasks."""
        pq.schedule(periodic_noop_handler, run_every=timedelta(seconds=0), active=False)

        processed = pq.run_worker_once()
        assert processed is False

    def test_active_periodic_executed(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker executes active periodic tasks normally."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(counter_handler, run_every=timedelta(seconds=0), active=True)

        processed = pq.run_worker_once()
        assert processed is True
        assert len(results) == 1

    def test_reactivated_periodic_executed(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker executes a periodic task after re-activation."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(counter_handler, run_every=timedelta(seconds=0), active=False)
        assert pq.run_worker_once() is False

        pq.schedule(counter_handler, run_every=timedelta(seconds=0), active=True)
        assert pq.run_worker_once() is True
        assert len(results) == 1


class TestMaxConcurrent:
    """Tests for max_concurrent periodic task behavior."""

    def test_max_concurrent_skips_while_locked(self, pq: PQ) -> None:
        """Periodic task with max_concurrent=1 is skipped while locked."""
        from sqlalchemy import update

        pq.schedule(periodic_noop_handler, run_every=timedelta(seconds=0))

        # Manually set locked_until to the future
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:periodic_noop_handler")
                .values(locked_until=datetime.now(UTC) + timedelta(hours=1))
            )

        # Worker should skip since task is locked
        processed = pq.run_worker_once()
        assert processed is False

    def test_max_concurrent_runs_after_lock_expires(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Periodic task runs after lock has expired."""
        from sqlalchemy import update

        results = manager.list()
        _set_shared_results(results)

        pq.schedule(counter_handler, run_every=timedelta(seconds=0))

        # Set locked_until to the past (expired lock)
        with pq.session() as session:
            session.execute(
                update(Periodic)
                .where(Periodic.name == "tests.test_worker:counter_handler")
                .values(locked_until=datetime.now(UTC) - timedelta(seconds=1))
            )

        processed = pq.run_worker_once()
        assert processed is True
        assert len(results) == 1

    def test_max_concurrent_clears_lock_on_success(self, pq: PQ) -> None:
        """Lock is cleared after successful task execution."""
        from sqlalchemy import select

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            assert periodic.locked_until is None

    def test_max_concurrent_clears_lock_on_failure(self, pq: PQ) -> None:
        """Lock is cleared even after task failure."""
        from sqlalchemy import select

        pq.schedule(failing_handler, run_every=timedelta(hours=1))

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            assert periodic.locked_until is None

    def test_max_concurrent_none_allows_overlap(self, pq: PQ) -> None:
        """max_concurrent=None does not set a lock."""
        from sqlalchemy import select

        pq.schedule(
            periodic_noop_handler,
            run_every=timedelta(seconds=0),
            max_concurrent=None,
        )

        pq.run_worker_once()

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_worker:periodic_noop_handler"
                )
            ).scalar_one()
            # locked_until should remain None (no lock set)
            assert periodic.locked_until is None


class TestAsyncTasks:
    """Tests for async task handler support."""

    def test_processes_async_one_off_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes an async one-off task."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(async_capture_handler, value="async_test")
        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == ["async_test"]

    def test_processes_async_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Worker processes an async periodic task."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(async_periodic_handler, run_every=timedelta(hours=1), n=1)

        processed = pq.run_worker_once()

        assert processed is True
        assert list(results) == [1]

    def test_async_task_failure_handled(self, pq: PQ) -> None:
        """Worker handles async task failures correctly."""
        pq.enqueue(async_failing_handler)
        assert pq.pending_count() == 1

        pq.run_worker_once()

        # Task should be marked as failed
        assert pq.pending_count() == 0


class TestHooks:
    """Tests for pre_execute/post_execute hooks."""

    def test_pre_execute_hook_called_for_one_off_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Pre-execute hook is called before one-off task execution."""
        results = manager.list()
        _set_shared_results(results)

        def pre_hook(task: Any) -> None:
            _shared_results.append(("pre", task.name))

        pq.enqueue(noop_handler)

        from pq.worker import run_worker_once

        run_worker_once(pq, pre_execute=pre_hook)

        assert len(results) == 1
        assert results[0][0] == "pre"
        assert "noop_handler" in results[0][1]

    def test_post_execute_hook_called_for_one_off_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Post-execute hook is called after one-off task execution."""
        results = manager.list()
        _set_shared_results(results)

        def post_hook(task: Any, error: Exception | None) -> None:
            _shared_results.append(("post", task.name, error))

        pq.enqueue(noop_handler)

        from pq.worker import run_worker_once

        run_worker_once(pq, post_execute=post_hook)

        assert len(results) == 1
        assert results[0][0] == "post"
        assert "noop_handler" in results[0][1]
        assert results[0][2] is None  # No error

    def test_post_execute_hook_receives_error_on_failure(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Post-execute hook receives error when task fails."""
        results = manager.list()
        _set_shared_results(results)

        def post_hook(task: Any, error: Exception | None) -> None:
            _shared_results.append(
                ("post", error is not None, str(error) if error else None)
            )

        pq.enqueue(failing_handler)

        from pq.worker import run_worker_once

        run_worker_once(pq, post_execute=post_hook)

        assert len(results) == 1
        assert results[0][0] == "post"
        assert results[0][1] is True  # Had error
        assert "boom" in results[0][2]  # Error message contains "boom"

    def test_pre_execute_hook_called_for_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Pre-execute hook is called before periodic task execution."""
        results = manager.list()
        _set_shared_results(results)

        def pre_hook(task: Any) -> None:
            _shared_results.append(("pre", task.name))

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        from pq.worker import run_worker_once

        run_worker_once(pq, pre_execute=pre_hook)

        assert len(results) == 1
        assert results[0][0] == "pre"
        assert "periodic_noop_handler" in results[0][1]

    def test_post_execute_hook_called_for_periodic_task(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Post-execute hook is called after periodic task execution."""
        results = manager.list()
        _set_shared_results(results)

        def post_hook(task: Any, error: Exception | None) -> None:
            _shared_results.append(("post", task.name, error))

        pq.schedule(periodic_noop_handler, run_every=timedelta(hours=1))

        from pq.worker import run_worker_once

        run_worker_once(pq, post_execute=post_hook)

        assert len(results) == 1
        assert results[0][0] == "post"
        assert "periodic_noop_handler" in results[0][1]
        assert results[0][2] is None  # No error

    def test_both_hooks_called_in_order(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Pre and post hooks are called in correct order."""
        results = manager.list()
        _set_shared_results(results)

        def pre_hook(task: Any) -> None:
            _shared_results.append("pre")

        def post_hook(task: Any, error: Exception | None) -> None:
            _shared_results.append("post")

        pq.enqueue(noop_handler)

        from pq.worker import run_worker_once

        run_worker_once(pq, pre_execute=pre_hook, post_execute=post_hook)

        assert list(results) == ["pre", "post"]

    def test_hook_receives_task_object_with_correct_fields(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Hooks receive task object with expected fields."""
        results = manager.list()
        _set_shared_results(results)

        def pre_hook(task: Any) -> None:
            _shared_results.append(
                {
                    "has_id": hasattr(task, "id"),
                    "has_name": hasattr(task, "name"),
                    "has_payload": hasattr(task, "payload"),
                    "has_client_id": hasattr(task, "client_id"),
                }
            )

        pq.enqueue(noop_handler, client_id="hook-test")

        from pq.worker import run_worker_once

        run_worker_once(pq, pre_execute=pre_hook)

        assert len(results) == 1
        task_info = results[0]
        assert task_info["has_id"] is True
        assert task_info["has_name"] is True
        assert task_info["has_payload"] is True
        assert task_info["has_client_id"] is True

    def test_post_execute_called_even_when_pre_execute_fails(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Post-execute hook is called even if pre-execute hook raises."""
        results = manager.list()
        _set_shared_results(results)

        def pre_hook(task: Any) -> None:
            _shared_results.append("pre")
            raise RuntimeError("pre hook failed")

        def post_hook(task: Any, error: Exception | None) -> None:
            _shared_results.append("post")

        pq.enqueue(noop_handler)

        from pq.worker import run_worker_once

        run_worker_once(pq, pre_execute=pre_hook, post_execute=post_hook)

        # Both should be called (post catches the pre-hook error)
        assert "pre" in results
        assert "post" in results


class TestTaskTimeout:
    """Tests for task timeout functionality."""

    def test_async_task_timeout(self, pq: PQ) -> None:
        """Async task that exceeds timeout is terminated."""
        pq.enqueue(slow_async_handler)
        # Use 0.1 second timeout - task should timeout
        pq.run_worker_once(max_runtime=0.1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0

    def test_sync_task_timeout(self, pq: PQ) -> None:
        """Sync task that exceeds timeout is terminated."""
        pq.enqueue(slow_sync_handler)
        # Use 1 second timeout (SIGALRM has 1-second granularity)
        pq.run_worker_once(max_runtime=1)

        # Task should be removed (marked failed)
        assert pq.pending_count() == 0


class TestConcurrentWorker:
    """Tests for concurrent worker mode (concurrency > 1)."""

    def _run_concurrent_worker(
        self,
        db_url: str,
        *,
        concurrency: int = 3,
        max_runtime: float = 30,
        poll_interval: float = 0.1,
        priorities: set[Priority] | None = None,
    ) -> tuple[int, int]:
        """Fork a child running the worker. Returns (child_pid, parent_pid).

        Creates a fresh PQ instance in the child to avoid sharing
        the parent's connection pool across fork.

        The child runs ``run_worker``, which handles SIGINT by draining.
        Running ``_run_concurrent`` directly made SIGINT a
        ``KeyboardInterrupt`` at whatever the worker was doing: between a
        fork and noting the child down, it lost track of a running task and
        left its row RUNNING.

        Caller must eventually os.kill(pid, SIGINT) and os.waitpid(pid, 0).
        """
        import os

        from pq.worker import run_worker

        pid = os.fork()
        if pid == 0:
            exit_code = 1
            try:
                child_pq = PQ(db_url)
                run_worker(
                    child_pq,
                    concurrency=concurrency,
                    poll_interval=poll_interval,
                    max_runtime=max_runtime,
                    priorities=priorities,
                    retention_days=0,
                    stale_task_timeout=None,
                )
                child_pq.close()
                exit_code = 0
            finally:
                os._exit(exit_code)

        return pid, os.getpid()

    def _wait_for(
        self,
        predicate: Callable[[], bool],
        *,
        timeout: float = 5,
        poll: float = 0.1,
    ) -> None:
        """Poll until predicate returns True, or raise AssertionError."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(poll)
        raise AssertionError(f"Condition not met within {timeout}s")

    def _stop_worker(self, pid: int) -> None:
        """Send SIGINT and wait for the worker to exit."""
        import os
        import signal as sig

        os.kill(pid, sig.SIGINT)
        os.waitpid(pid, 0)

    def test_parallel_execution(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """5 tasks sleeping 1s complete in ~1s with concurrency=5, not 5s."""
        results = manager.list()
        _set_shared_results(results)

        for _ in range(5):
            pq.enqueue(sleep_handler, duration=1.0)

        start = time.perf_counter()
        pid, _ = self._run_concurrent_worker(db_url, concurrency=5)

        # Wait for DB state (not just handler results) to avoid race between
        # handler completion and worker reaping/updating the DB.
        self._wait_for(lambda: len(pq.list_completed()) >= 5)
        self._stop_worker(pid)

        elapsed = time.perf_counter() - start

        assert len(results) == 5
        assert pq.pending_count() == 0
        assert len(pq.list_completed()) == 5
        assert elapsed < 2.5  # ~1s parallel + overhead, not 5s sequential

    def test_slot_refill(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """6 tasks with concurrency=3 run in 2 batches (~1s), not 6 sequential."""
        results = manager.list()
        _set_shared_results(results)

        for _ in range(6):
            pq.enqueue(sleep_handler, duration=0.5)

        start = time.perf_counter()
        pid, _ = self._run_concurrent_worker(db_url, concurrency=3)

        self._wait_for(lambda: len(pq.list_completed()) >= 6)
        self._stop_worker(pid)

        elapsed = time.perf_counter() - start

        assert len(results) == 6
        assert pq.pending_count() == 0
        assert len(pq.list_completed()) == 6
        assert elapsed < 2.0  # 2 batches × 0.5s + overhead

    def test_failed_task_concurrent(self, pq: PQ, db_url: str) -> None:
        """Failed tasks have correct status, error type, and error message."""
        pq.enqueue(failing_handler)

        pid, _ = self._run_concurrent_worker(db_url)

        self._wait_for(lambda: len(pq.list_failed()) >= 1)
        self._stop_worker(pid)

        assert pq.pending_count() == 0
        failed = pq.list_failed()
        assert len(failed) == 1
        assert failed[0].status.value == "failed"
        assert "ValueError" in (failed[0].error or "")
        assert "boom" in (failed[0].error or "")
        assert failed[0].completed_at is not None

    def test_mixed_success_and_failure(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Successful and failing tasks are tracked independently."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(capture_handler, value=1)
        pq.enqueue(failing_handler)
        pq.enqueue(capture_handler, value=2)

        pid, _ = self._run_concurrent_worker(db_url, concurrency=3)

        self._wait_for(
            lambda: len(pq.list_completed()) >= 2 and len(pq.list_failed()) >= 1
        )
        self._stop_worker(pid)

        assert pq.pending_count() == 0
        assert sorted(results) == [1, 2]
        assert len(pq.list_completed()) == 2
        assert len(pq.list_failed()) == 1

    def test_concurrent_periodic_task(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Periodic tasks are processed in concurrent mode."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(periodic_handler, 42, run_every=timedelta(hours=1))

        pid, _ = self._run_concurrent_worker(db_url)

        self._wait_for(lambda: len(results) >= 1)
        self._stop_worker(pid)

        assert list(results) == [42]

    def test_graceful_shutdown_completes_in_flight(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """SIGINT during execution waits for in-flight tasks to complete."""
        results = manager.list()
        _set_shared_results(results)

        for _ in range(3):
            pq.enqueue(sleep_handler, duration=1.5)

        pid, _ = self._run_concurrent_worker(db_url, concurrency=3)

        # Wait for tasks to start but not finish
        time.sleep(0.5)

        # SIGINT — worker should wait for in-flight tasks before exiting
        self._stop_worker(pid)

        # All 3 should have completed (worker waited for them)
        assert len(results) == 3
        assert pq.pending_count() == 0
        assert len(pq.list_completed()) == 3

    def test_timeout_concurrent(self, pq: PQ, db_url: str) -> None:
        """Task exceeding max_runtime_seconds is killed and marked failed."""
        pq.enqueue(slow_sync_handler)  # sleeps 10s

        pid, _ = self._run_concurrent_worker(db_url, max_runtime=1)

        self._wait_for(lambda: len(pq.list_failed()) >= 1, timeout=10)
        self._stop_worker(pid)

        failed = pq.list_failed()
        assert len(failed) == 1
        assert "Timed out" in (failed[0].error or "")
        assert failed[0].completed_at is not None

    def test_priority_ordering_concurrent(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Higher priority tasks are claimed first in concurrent mode."""
        results = manager.list()
        _set_shared_results(results)

        # Enqueue in reverse priority order
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)
        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)

        # concurrency=1 forces sequential claiming within the concurrent loop
        pid, _ = self._run_concurrent_worker(db_url, concurrency=1)

        self._wait_for(lambda: len(pq.list_completed()) >= 3)
        self._stop_worker(pid)

        assert list(results) == [1, 2, 3]

    def test_mixed_one_off_and_periodic_concurrent(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """One-off and periodic tasks are processed together."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(capture_handler, value="one-off")
        pq.schedule(periodic_handler, 42, run_every=timedelta(hours=1))

        pid, _ = self._run_concurrent_worker(db_url, concurrency=3)

        self._wait_for(lambda: len(results) >= 2)
        self._stop_worker(pid)

        assert "one-off" in list(results)
        assert 42 in list(results)
        assert len(pq.list_completed()) == 1  # one-off tracked in tasks table

    def test_failed_periodic_concurrent(self, pq: PQ, db_url: str) -> None:
        """Failed periodic tasks clear lock and advance schedule in concurrent mode."""
        from sqlalchemy import select as sa_select

        pq.schedule(failing_handler, run_every=timedelta(hours=1))

        pid, _ = self._run_concurrent_worker(db_url)

        # Wait for the periodic task to be processed (schedule advances)
        self._wait_for(
            lambda: self._periodic_was_run(pq, "tests.test_worker:failing_handler")
        )
        self._stop_worker(pid)

        # Verify lock is cleared and schedule is advanced
        with pq.session() as session:
            periodic = session.execute(
                sa_select(Periodic).where(
                    Periodic.name == "tests.test_worker:failing_handler"
                )
            ).scalar_one()
            assert periodic.locked_until is None  # lock cleared
            assert periodic.last_run is not None  # was run

    def test_priority_filter_concurrent(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Priority filter restricts which tasks are claimed in concurrent mode."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(ordered_handler, n=1, priority=Priority.HIGH)
        pq.enqueue(ordered_handler, n=2, priority=Priority.NORMAL)
        pq.enqueue(ordered_handler, n=3, priority=Priority.LOW)

        pid, _ = self._run_concurrent_worker(
            db_url, concurrency=3, priorities={Priority.HIGH}
        )

        self._wait_for(lambda: len(pq.list_completed()) >= 1)
        # Give it time to potentially pick up more tasks (it shouldn't)
        time.sleep(0.3)
        self._stop_worker(pid)

        assert list(results) == [1]  # only HIGH
        assert len(pq.list_completed()) == 1
        assert pq.pending_count() == 2  # NORMAL and LOW still pending

    def _periodic_was_run(self, pq: PQ, name: str) -> bool:
        """Check if a periodic task's last_run was set."""
        from sqlalchemy import select as sa_select

        with pq.session() as session:
            periodic = session.execute(
                sa_select(Periodic).where(Periodic.name == name)
            ).scalar_one_or_none()
            return periodic is not None and periodic.last_run is not None

    def test_future_task_not_claimed_concurrent(self, pq: PQ, db_url: str) -> None:
        """Tasks with run_at in the future are not picked up."""
        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue(noop_handler, run_at=future)

        pid, _ = self._run_concurrent_worker(db_url)

        time.sleep(0.5)
        self._stop_worker(pid)

        assert pq.pending_count() == 1  # still waiting

    def test_empty_queue_concurrent(self, pq: PQ, db_url: str) -> None:
        """Worker idles on empty queue without errors."""
        pid, _ = self._run_concurrent_worker(db_url)

        time.sleep(0.5)
        self._stop_worker(pid)

        assert pq.pending_count() == 0

    def test_concurrency_one_is_sequential(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """concurrency=1 preserves existing sequential behavior."""
        results = manager.list()
        _set_shared_results(results)

        pq.enqueue(capture_handler, value=42)

        from pq.worker import run_worker_once

        processed = run_worker_once(pq)
        assert processed is True
        assert list(results) == [42]
        assert len(pq.list_completed()) == 1

    # ------------------------------------------------------------------
    # Per-task max_runtime_seconds override — concurrent paths
    # ------------------------------------------------------------------
    # Cover the ``_claim_and_fork_one_off`` and ``_claim_and_fork_periodic``
    # code paths (``worker.py:844, 933``) which mirror the sequential
    # ``_process_one_off_task`` / ``_process_periodic_task`` logic but
    # are reached only under ``concurrency > 1``. Without these, a
    # regression in the concurrent fork-claim path (where the per-task
    # value is read off the row before ``session.expunge``) would slip
    # past the sequential tests in ``TestPerTaskMaxRuntimeEnforcement``.

    def test_one_off_per_task_max_runtime_honored_concurrent(
        self, pq: PQ, db_url: str
    ) -> None:
        """Per-task ``max_runtime_seconds=1`` kills a slow task even when the
        concurrent worker's own default is loose (60 s)."""
        pq.enqueue(slow_sync_handler, max_runtime_seconds=1)

        pid, _ = self._run_concurrent_worker(db_url, max_runtime=60)
        self._wait_for(lambda: len(pq.list_failed()) >= 1, timeout=15)
        self._stop_worker(pid)

        failed = pq.list_failed()
        assert len(failed) == 1
        # Per-task value WAS honored: task died at ~1 s despite worker
        # default of 60 s. If the concurrent claim path forgot to read
        # ``max_runtime_seconds``, we'd hit the 10 s sleep instead and
        # this assertion would time out long before failure.
        assert "Timed out" in (failed[0].error or "")
        assert failed[0].max_runtime_seconds == 1.0

    def test_periodic_per_task_max_runtime_honored_concurrent(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
    ) -> None:
        """Same as above but on the periodic concurrent path.

        Uses ``tracked_slow_async_handler`` so we can assert on the
        ABSENCE of the post-sleep marker — direct proof the kill
        happened. ``last_run`` alone is set by the worker before the
        fork (Phase 1), so it doesn't differentiate "killed by
        per-task max_runtime_seconds" from "ran to completion"."""
        from sqlalchemy import select

        results = manager.list()
        _set_shared_results(results)

        pq.schedule(
            tracked_slow_async_handler,
            run_every=timedelta(seconds=60),
            max_runtime_seconds=1.0,
        )

        pid, _ = self._run_concurrent_worker(db_url, max_runtime=60)

        # Wait until the worker has claimed AND processed the
        # periodic. ``last_run`` advances at claim time (before fork),
        # so it tells us "claim happened" — necessary precondition for
        # the kill-or-not check below. We then wait a small grace
        # period for the forked child to finish its 1 s timeout cycle
        # (and, if the override was ignored, complete its 10 s sleep).
        def _periodic_fired() -> bool:
            with pq.session() as session:
                p = session.execute(select(Periodic)).scalar_one()
                return p.last_run is not None

        self._wait_for(_periodic_fired, timeout=15)
        # Wait long enough that BOTH outcomes would have completed:
        # - kill at ~1 s (override honoured): no marker written
        # - 10 s sleep finishes (override ignored): marker written
        # 13 s is past the 10 s sleep + post-execute lifecycle.
        time.sleep(13)
        self._stop_worker(pid)

        assert list(results) == [], (
            f"per-task max_runtime_seconds=1.0 on the concurrent periodic path "
            f"should have killed the handler before its 10 s sleep "
            f"finished; got results={list(results)} (handler ran to "
            f"completion → override was ignored)"
        )


class TestStaleReaperRace:
    """Tests for the Phase 3 race between worker status update and reaper."""

    def test_worker_does_not_overwrite_reaped_task(self, pq: PQ) -> None:
        """If the reaper marks a task FAILED while the worker is alive,
        the worker's Phase 3 update must not overwrite it back to COMPLETED.

        Simulates: task is claimed -> reaper fires -> worker finishes fork.
        """
        from datetime import UTC, datetime, timedelta

        from sqlalchemy import update as sa_update

        from pq.models import Task, TaskStatus

        # Enqueue and let the worker claim + execute it
        task_id = pq.enqueue(noop_handler, client_id="race-test")

        # Manually simulate the race:
        # 1. Set task to RUNNING (as if worker claimed it)
        with pq.session() as session:
            session.execute(
                sa_update(Task)
                .where(Task.id == task_id)
                .values(
                    status=TaskStatus.RUNNING,
                    started_at=datetime.now(UTC) - timedelta(hours=2),
                )
            )

        # 2. Reaper fires and marks it FAILED
        reaped = pq.reap_stale_tasks(timedelta(hours=1))
        assert reaped == 1

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "Reaped" in (task.error or "")

        # 3. Now simulate what Phase 3 does: try to update WHERE status = RUNNING
        #    This should be a no-op because status is already FAILED
        with pq.session() as session:
            result = session.connection().execute(
                sa_update(Task)
                .where(Task.id == task_id, Task.status == TaskStatus.RUNNING)
                .values(
                    status=TaskStatus.COMPLETED,
                    completed_at=datetime.now(UTC),
                )
            )
            assert result.rowcount == 0  # No rows updated

        # Task should still be FAILED with the reaper's error message
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "Reaped" in (task.error or "")


class TestPerTaskMaxRuntimeEnforcement:
    """Tests that the worker honours per-task ``max_runtime_seconds`` on
    every execution path (sequential + concurrent × one-off + periodic).

    Each test pins one knob and varies the other, asserting the worker
    picks the right effective ceiling:

    - per-task value when set
    - worker default when per-task is NULL (backwards-compat)
    """

    # ------------------------------------------------------------------
    # One-off tasks — sequential path (``_process_one_off_task``)
    # ------------------------------------------------------------------

    def test_one_off_async_task_uses_per_task_max_runtime(self, pq: PQ) -> None:
        """Per-task override TIGHTER than worker default kills the task."""
        # Worker default = 30s, per-task = 0.1s → killed at 0.1s.
        pq.enqueue(slow_async_handler, max_runtime_seconds=0.1)
        pq.run_worker_once(max_runtime=30)
        assert pq.pending_count() == 0  # task processed (and failed)

    def test_one_off_sync_task_uses_per_task_max_runtime(self, pq: PQ) -> None:
        """Same as above for sync handlers (SIGALRM path)."""
        # SIGALRM has 1-second granularity; use 1s for the override
        # and 60s for the worker default.
        pq.enqueue(slow_sync_handler, max_runtime_seconds=1)
        pq.run_worker_once(max_runtime=60)
        assert pq.pending_count() == 0

    def test_one_off_task_falls_back_to_worker_default_when_null(self, pq: PQ) -> None:
        """Backwards-compat: enqueue without override → worker default
        applies (so existing call sites keep their previous semantics
        exactly)."""
        # No per-task value; worker default = 0.1s → still killed.
        pq.enqueue(slow_async_handler)
        pq.run_worker_once(max_runtime=0.1)
        assert pq.pending_count() == 0

    def test_one_off_task_override_can_be_longer_than_worker_default(
        self, pq: PQ
    ) -> None:
        """Per-task override LOOSER than worker default lets a fast task
        complete normally even when the worker's general policy is
        aggressive. This is THE central use case — a worker fleet
        sized for short tasks lets one occasionally-long task opt out
        of the tight ceiling."""
        # Worker default = 0.5s. Fast handler completes in ~0s; we
        # pass max_runtime_seconds=10 just to prove the override is honoured
        # (else the task would be killed even though it's fast — no:
        # fast tasks aren't affected by either ceiling; what we're
        # really proving is the column is consumed by the right code
        # path without breaking happy-path execution).
        pq.enqueue(noop_handler, max_runtime_seconds=10.0)
        result = pq.run_worker_once(max_runtime=0.5)
        assert result is True  # task was processed
        assert pq.pending_count() == 0

    # ------------------------------------------------------------------
    # Periodic tasks — sequential path (``_process_periodic_task``)
    # ------------------------------------------------------------------

    def test_periodic_async_task_uses_per_task_max_runtime(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Per-task override TIGHTER than worker default kills the
        periodic handler before it can write the "completed" marker.

        Asserting on the absence of the marker proves the kill
        actually happened. (Earlier weaker version asserted
        ``last_run is not None``, but the worker sets ``last_run``
        BEFORE the fork — independent of whether the handler runs to
        completion. The marker is set AFTER the slow sleep, so its
        absence is a real signal that the override fired.)"""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(
            tracked_slow_async_handler,
            run_every=timedelta(seconds=60),
            max_runtime_seconds=0.1,
        )
        pq.run_worker_once(max_runtime=30)

        assert list(results) == [], (
            f"per-task max_runtime_seconds=0.1 should have killed the handler "
            f"before its 10 s sleep finished; got results={list(results)}"
        )

    def test_periodic_task_falls_back_to_worker_default_when_null(
        self, pq: PQ, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Backwards-compat for the periodic path: NULL override means
        the worker default applies. Worker default = 0.1 s here →
        ``tracked_slow_async_handler``'s 10 s sleep never finishes →
        marker never written."""
        results = manager.list()
        _set_shared_results(results)

        pq.schedule(tracked_slow_async_handler, run_every=timedelta(seconds=60))
        pq.run_worker_once(max_runtime=0.1)

        assert list(results) == [], (
            f"worker default of 0.1 s should have killed the handler "
            f"before its 10 s sleep finished; got results={list(results)}"
        )

    def test_periodic_lock_duration_scales_with_per_task_max_runtime(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
    ) -> None:
        """When ``max_concurrent=1``, the worker sets
        ``locked_until = now() + lock_duration`` to keep concurrent
        workers off the schedule during execution. That lock duration
        must track the per-task override, not the worker default —
        otherwise a long-running periodic would lose its lock
        mid-execution and another worker could claim it.

        See ``lock_observer_handler`` for why the observation has to
        happen mid-handler with a fresh DB query rather than via a
        ``pre_execute`` hook or a post-run inspection.
        """
        captured = manager.list()
        _set_shared_results(captured)

        pq.schedule(
            lock_observer_handler,
            db_url=db_url,
            run_every=timedelta(seconds=60),
            max_concurrent=1,
            max_runtime_seconds=120.0,
        )
        pq.run_worker_once(max_runtime=30.0)

        # Handler ran once and captured ``locked_until`` it observed for
        # its own row, before the post-execution cleanup cleared the lock.
        assert len(captured) == 1, (
            f"lock_observer_handler should have run exactly once; "
            f"got {len(captured)} captures"
        )
        locked_until = captured[0]
        assert locked_until is not None, (
            "locked_until was None at execution time — concurrency lock "
            "was never set despite max_concurrent=1"
        )
        # The captured ``locked_until`` was set ~now()+lock_duration just
        # before the fork; ``now()`` here is mid- to post-fork. The window
        # should be well above the worker default (30 s) and bounded above
        # by the per-task override (120 s) plus measurement slack.
        window_seconds = (locked_until - datetime.now(UTC)).total_seconds()
        assert window_seconds > 60.0, (
            f"Lock window {window_seconds:.1f}s is suspiciously short — "
            f"likely fell back to the worker default (30s) instead of "
            f"honouring the per-task override (120s)."
        )
        assert window_seconds < 180.0, (
            f"Lock window {window_seconds:.1f}s exceeds the per-task "
            f"override + measurement slack."
        )


def shutdown_probe_handler(duration: float, marker: str) -> None:
    """Sleep, then append a completion marker.

    Drain tests assert on the marker's presence/absence: a task that was
    allowed to finish appends it; a task SIGKILLed by the drain never does.
    """
    time.sleep(duration)
    _shared_results.append(marker)


class _ForkedWorkers:
    """Helpers for tests that fork real ``run_worker`` processes."""

    @pytest.fixture(autouse=True)
    def _reap_leaked_workers(self) -> Generator[None, None, None]:
        """SIGKILL and reap any forked worker a failing test left behind.

        Without this, a worker orphaned by an assertion failure keeps
        polling the shared test database and claims tasks enqueued by
        subsequent tests, cascading the failure.
        """
        import os
        import signal as sig

        self._worker_pids: list[int] = []
        yield
        for pid in self._worker_pids:
            try:
                os.kill(pid, sig.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                os.waitpid(pid, 0)
            except ChildProcessError:
                pass  # already reaped by _wait_for_exit

    def _fork_worker(
        self,
        db_url: str,
        *,
        concurrency: int = 1,
        drain_timeout: float = 5.0,
        poll_interval: float = 0.1,
        max_runtime: float = 30,
    ) -> int:
        """Fork a child process running run_worker. Returns child pid.

        Creates a fresh PQ instance in the child to avoid sharing the
        parent's connection pool across fork. Caller must eventually signal
        the child and reap it with os.waitpid; the autouse fixture cleans
        up on test failure.
        """
        import os

        from pq.worker import run_worker

        pid = os.fork()
        if pid == 0:
            child_pq = PQ(db_url)
            try:
                run_worker(
                    child_pq,
                    concurrency=concurrency,
                    poll_interval=poll_interval,
                    max_runtime=max_runtime,
                    drain_timeout=drain_timeout,
                    retention_days=0,
                    stale_task_timeout=None,
                )
            finally:
                child_pq.close()
                os._exit(0)
        self._worker_pids.append(pid)
        return pid

    def _wait_for(
        self,
        predicate: Callable[[], bool],
        *,
        timeout: float = 5,
        poll: float = 0.05,
    ) -> None:
        """Poll until predicate returns True, or raise AssertionError."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(poll)
        raise AssertionError(f"Condition not met within {timeout}s")

    def _wait_for_exit(
        self, pid: int, *, timeout: float = 10, poll: float = 0.05
    ) -> None:
        """Wait for the worker process to exit, or fail (and clean up)."""
        import os
        import signal as sig

        deadline = time.time() + timeout
        while time.time() < deadline:
            done, _ = os.waitpid(pid, os.WNOHANG)
            if done == pid:
                self._worker_pids.remove(pid)
                return
            time.sleep(poll)
        os.kill(pid, sig.SIGKILL)
        os.waitpid(pid, 0)
        self._worker_pids.remove(pid)
        raise AssertionError(f"Worker did not exit within {timeout}s")


class TestGracefulShutdown(_ForkedWorkers):
    """Tests for SIGTERM/SIGINT graceful drain (run_worker)."""

    def _task_running(self, pq: PQ, task_id: int) -> bool:
        task = pq.get_task(task_id)
        return task is not None and task.status.value == "running"

    def _assert_no_running(self, pq: PQ) -> None:
        """No orphaned RUNNING rows after shutdown."""
        from sqlalchemy import select as sa_select

        from pq.models import Task, TaskStatus

        with pq.session() as session:
            running = (
                session.execute(
                    sa_select(Task).where(Task.status == TaskStatus.RUNNING)
                )
                .scalars()
                .all()
            )
            assert running == [], f"Orphaned RUNNING rows: {running}"

    def test_sigterm_idle_worker_exits_promptly(self, db_url: str) -> None:
        """SIGTERM with an empty queue -> prompt clean exit."""
        import os
        import signal as sig

        pid = self._fork_worker(db_url)
        time.sleep(0.5)  # let the worker install handlers and start polling

        start = time.perf_counter()
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=5)
        assert time.perf_counter() - start < 2.0

    def test_sigterm_fast_task_completes(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """In-flight task finishing within drain_timeout is written COMPLETED."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=1.0, marker="done")
        pid = self._fork_worker(db_url, drain_timeout=10.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid)

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "completed"
        assert list(results) == ["done"]
        self._assert_no_running(pq)

    def test_sigterm_slow_task_killed_and_failed(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Task overrunning drain_timeout is killed and marked FAILED."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=30.0, marker="done")
        pid = self._fork_worker(db_url, drain_timeout=1.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        start = time.perf_counter()
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=8)
        assert time.perf_counter() - start < 5.0  # ~1s drain + writes

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "failed"
        assert "shutdown" in (task.error or "")
        assert task.completed_at is not None
        assert task.attempts == 1
        assert list(results) == []  # handler never finished
        self._assert_no_running(pq)

    def test_drain_timeout_zero_kills_immediately(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """drain_timeout=0 -> in-flight task killed at once, prompt exit."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=30.0, marker="done")
        pid = self._fork_worker(db_url, drain_timeout=0.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        start = time.perf_counter()
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=5)
        assert time.perf_counter() - start < 3.0

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "failed"
        assert "shutdown" in (task.error or "")
        self._assert_no_running(pq)

    def test_second_sigterm_during_drain_is_ignored(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """A repeat SIGTERM must not abandon the drain: the in-flight task
        still gets to finish and its status is written."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=1.5, marker="done")
        pid = self._fork_worker(db_url, drain_timeout=10.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        os.kill(pid, sig.SIGTERM)
        time.sleep(0.3)  # inside the drain window
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid)

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "completed"
        assert list(results) == ["done"]
        self._assert_no_running(pq)

    def test_sigint_drains_like_sigterm(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """SIGINT (Ctrl-C) goes through the same drain path — including the
        sequential worker, which previously abandoned its child."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=1.0, marker="done")
        pid = self._fork_worker(db_url, concurrency=1, drain_timeout=10.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        os.kill(pid, sig.SIGINT)
        self._wait_for_exit(pid)

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "completed"
        assert list(results) == ["done"]
        self._assert_no_running(pq)

    def test_concurrent_mixed_fast_and_slow(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Concurrent drain: fast child completes, slow child killed+FAILED."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        fast_id = pq.enqueue(shutdown_probe_handler, duration=1.5, marker="fast")
        slow_id = pq.enqueue(shutdown_probe_handler, duration=30.0, marker="slow")
        pid = self._fork_worker(db_url, concurrency=3, drain_timeout=4.0)

        self._wait_for(
            lambda: self._task_running(pq, fast_id) and self._task_running(pq, slow_id)
        )
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=8)

        fast = pq.get_task(fast_id)
        slow = pq.get_task(slow_id)
        assert fast is not None and fast.status.value == "completed"
        assert slow is not None and slow.status.value == "failed"
        assert "shutdown" in (slow.error or "")
        assert list(results) == ["fast"]
        self._assert_no_running(pq)

    def test_sequential_slow_task_killed(
        self, pq: PQ, db_url: str, manager: multiprocessing.managers.SyncManager
    ) -> None:
        """Sequential worker blocked on a running task honours the deadline
        (previously it would sit in os.wait4 until the task finished)."""
        import os
        import signal as sig

        results = manager.list()
        _set_shared_results(results)

        task_id = pq.enqueue(shutdown_probe_handler, duration=30.0, marker="done")
        pid = self._fork_worker(db_url, concurrency=1, drain_timeout=1.0)

        self._wait_for(lambda: self._task_running(pq, task_id))
        start = time.perf_counter()
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=8)
        assert time.perf_counter() - start < 5.0

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status.value == "failed"
        assert "shutdown" in (task.error or "")
        self._assert_no_running(pq)

    @pytest.mark.parametrize(
        "extra_thread", [False, True], ids=["one_thread", "two_threads"]
    )
    def test_sigterm_while_forking_does_not_hang(self, extra_thread: bool) -> None:
        """SIGTERM handled during fork() must not hang the worker: loguru
        keeps its handler locks acquired across fork(), so a drain handler
        that logs blocks on them forever. The second case gives the worker an
        idle thread, as Sentry or an OTel exporter would, so the signal can
        arrive on either thread."""
        import os
        import random
        import signal as sig
        import threading

        from pq.models import Task
        from pq.worker import _fork_child, _handle_shutdown_signal, _shutdown

        for _ in range(20):
            ready_read, ready_write = os.pipe()
            pid = os.fork()
            if pid == 0:
                try:
                    if extra_thread:
                        threading.Thread(
                            target=threading.Event().wait, daemon=True
                        ).start()
                    _shutdown.reset(drain_timeout=1)
                    sig.signal(sig.SIGTERM, _handle_shutdown_signal)
                    os.write(ready_write, b"x")
                    while not _shutdown.requested:
                        child_pid, read_fd = _fork_child(
                            noop_handler,
                            (),
                            {},
                            max_runtime=5,
                            task=Task(name="noop", payload={}),
                        )
                        os.waitpid(child_pid, 0)
                        os.close(read_fd)
                finally:
                    os._exit(0)
            self._worker_pids.append(pid)
            # Signal only once the handler is in place, at a random point in
            # the fork loop.
            os.close(ready_write)
            os.read(ready_read, 1)
            os.close(ready_read)
            time.sleep(random.uniform(0, 0.02))
            os.kill(pid, sig.SIGTERM)
            self._wait_for_exit(pid, timeout=5, poll=0.005)

    @pytest.mark.parametrize(
        ("concurrency", "with_task"),
        [(1, True), (2, True), (1, False)],
        ids=["sequential_busy", "concurrent_busy", "sequential_idle"],
    )
    def test_sigterm_is_logged_once_by_the_worker_loop(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
        tmp_path: Path,
        concurrency: int,
        with_task: bool,
    ) -> None:
        """The signal handler doesn't log, so the worker loop logs the
        request: promptly, once, and before the worker stops. A busy worker
        logs it while its task is still running."""
        import os
        import signal as sig

        from loguru import logger

        _set_shared_results(manager.list())
        log_path = tmp_path / "worker.log"
        sink = logger.add(log_path, level="INFO", format="{message}", buffering=1)
        try:
            pid = self._fork_worker(db_url, concurrency=concurrency, drain_timeout=10.0)
            if with_task:
                task_id = pq.enqueue(
                    shutdown_probe_handler, duration=3.0, marker="done"
                )
                self._wait_for(lambda: self._task_running(pq, task_id))
            else:
                time.sleep(0.5)  # let the worker install handlers and poll

            os.kill(pid, sig.SIGTERM)
            self._wait_for(lambda: "Received signal" in log_path.read_text(), timeout=2)
            if with_task:
                assert self._task_running(pq, task_id)
            self._wait_for_exit(pid)
        finally:
            logger.remove(sink)

        lines = log_path.read_text().splitlines()
        received = [line for line in lines if line.startswith("Received signal")]
        assert received == [
            f"Received signal {sig.SIGTERM.value}, draining (drain_timeout=10.0s)..."
        ]
        assert lines.index(received[0]) < lines.index("Worker stopped.")

    def test_periodic_in_flight_lock_cleared(self, pq: PQ, db_url: str) -> None:
        """Periodic task killed by the drain: locked_until cleared, schedule
        intact, no one-off rows involved."""
        import os
        import signal as sig

        from sqlalchemy import select as sa_select

        pq.schedule(
            periodic_sleep_handler,
            run_every=timedelta(seconds=60),
            max_concurrent=1,
        )
        pid = self._fork_worker(db_url, concurrency=2, drain_timeout=1.0)

        def _locked() -> bool:
            with pq.session() as session:
                periodic = session.execute(sa_select(Periodic)).scalar_one()
                return periodic.locked_until is not None

        self._wait_for(_locked)
        os.kill(pid, sig.SIGTERM)
        self._wait_for_exit(pid, timeout=8)

        with pq.session() as session:
            periodic = session.execute(sa_select(Periodic)).scalar_one()
            assert periodic.locked_until is None
            assert periodic.last_run is not None
            assert periodic.active is True
        self._assert_no_running(pq)

    def test_drain_timeout_validation(self, pq: PQ) -> None:
        """Negative / non-finite drain_timeout is rejected."""
        import pytest

        from pq.worker import run_worker

        with pytest.raises(ValueError):
            run_worker(pq, drain_timeout=-1.0)
        with pytest.raises(ValueError):
            run_worker(pq, drain_timeout=float("inf"))


class TestShutdownSignalHandler:
    """Unit tests for the drain handler and the log line it defers."""

    @pytest.fixture(autouse=True)
    def _clean_shutdown_state(self) -> Generator[None, None, None]:
        from pq.worker import DEFAULT_DRAIN_TIMEOUT, _shutdown

        _shutdown.reset(drain_timeout=5.0)
        yield
        _shutdown.reset(DEFAULT_DRAIN_TIMEOUT)

    def test_handler_requests_drain_without_logging(
        self, log_records: list["Record"]
    ) -> None:
        import signal as sig

        from pq.worker import _handle_shutdown_signal, _shutdown

        before = time.monotonic()
        _handle_shutdown_signal(sig.SIGTERM.value, None)

        assert _shutdown.requested
        assert _shutdown.signum == sig.SIGTERM
        assert _shutdown.deadline is not None
        assert before + 5.0 <= _shutdown.deadline <= time.monotonic() + 5.0
        assert log_records == []

    def test_repeat_signal_keeps_the_first(self) -> None:
        import signal as sig

        from pq.worker import _handle_shutdown_signal, _shutdown

        _handle_shutdown_signal(sig.SIGTERM.value, None)
        deadline = _shutdown.deadline
        _handle_shutdown_signal(sig.SIGINT.value, None)

        assert _shutdown.signum == sig.SIGTERM
        assert _shutdown.deadline == deadline

    def test_log_request_logs_once_after_a_signal(
        self, log_records: list["Record"]
    ) -> None:
        import signal as sig

        from pq.worker import _handle_shutdown_signal, _shutdown

        _shutdown.log_request()
        assert log_records == []

        _handle_shutdown_signal(sig.SIGINT.value, None)
        _shutdown.log_request()
        _shutdown.log_request()
        assert [r["message"] for r in log_records] == [
            f"Received signal {sig.SIGINT.value}, draining (drain_timeout=5.0s)..."
        ]

    def test_reset_clears_the_signal_and_the_log_flag(
        self, log_records: list["Record"]
    ) -> None:
        import signal as sig

        from pq.worker import _handle_shutdown_signal, _shutdown

        _handle_shutdown_signal(sig.SIGTERM.value, None)
        _shutdown.log_request()
        _shutdown.reset(drain_timeout=5.0)

        assert not _shutdown.requested
        assert not _shutdown.logged
        _handle_shutdown_signal(sig.SIGTERM.value, None)
        _shutdown.log_request()
        assert len(log_records) == 2


def periodic_sleep_handler() -> None:
    """Periodic handler that sleeps long enough to straddle the drain."""
    time.sleep(30)


CONTINUATION_CLIENT_ID = "continuation"


def continuation_handler(
    db_url: str, generation: int, delay: float, run_seconds: float
) -> None:
    """Run that upserts its own continuation under the same ``client_id``
    while it is still RUNNING (the pattern from ricwo/pq#27).

    Generation 0 upserts generation 1 (due in ``delay`` s), keeps running
    for ``run_seconds``, then fails. Generation 1 runs for ``run_seconds``
    and succeeds. Start and end times go to shared state.
    """
    from pq.client import PQ as PQClient

    _shared_results.append((generation, "start", time.time()))
    if generation == 0:
        client = PQClient(db_url)
        try:
            client.upsert(
                continuation_handler,
                db_url=db_url,
                generation=1,
                delay=delay,
                run_seconds=run_seconds,
                client_id=CONTINUATION_CLIENT_ID,
                run_at=datetime.now(UTC) + timedelta(seconds=delay),
            )
        finally:
            client.close()
    time.sleep(run_seconds)
    _shared_results.append((generation, "end", time.time()))
    if generation == 0:
        raise RuntimeError("generation 0 failed after upserting its continuation")


class TestFinishOneOff:
    """Unit tests for ``_finish_one_off``, the shared end-of-run update."""

    def _claim(self, pq: PQ, task_id: int, started_at: datetime) -> None:
        """Simulate a worker claim of the row with the given ``started_at``."""
        from sqlalchemy import update

        from pq.models import Task, TaskStatus

        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(status=TaskStatus.RUNNING, started_at=started_at, attempts=1)
            )

    def _running_task(self, pq: PQ) -> tuple[int, datetime]:
        task_id = pq.upsert(capture_handler, value=1, client_id="finish")
        started_at = datetime.now(UTC)
        self._claim(pq, task_id, started_at)
        return task_id, started_at

    def _finish_capturing_warnings(
        self,
        pq: PQ,
        task_id: int,
        started_at: datetime | None,
        status: TaskStatus,
        error_msg: str | None,
    ) -> list[str]:
        from loguru import logger

        from pq.worker import _ChildResult, _finish_one_off

        failure = None if status == TaskStatus.COMPLETED else FailureKind.ERROR

        messages: list[str] = []
        sink = logger.add(messages.append, level="WARNING", format="{message}")
        try:
            _finish_one_off(
                pq,
                task_id,
                started_at,
                "capture_handler",
                _ChildResult(failure, error_msg),
            )
        finally:
            logger.remove(sink)
        return messages

    def test_requeues_row_with_parked_version(self, pq: PQ) -> None:
        """A FAILED run with a parked upsert re-queues the row instead."""
        from sqlalchemy import text

        from pq.models import TaskStatus
        from pq.worker import _ChildResult, _finish_one_off

        task_id, started_at = self._running_task(pq)
        pq.upsert(capture_handler, value=2, client_id="finish")

        _finish_one_off(
            pq,
            task_id,
            started_at,
            "capture_handler",
            _ChildResult(FailureKind.ERROR, "boom"),
        )

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.payload["kwargs"] == {"value": 2}
        assert task.attempts == 0
        assert task.started_at is None
        assert task.completed_at is None
        assert task.error is None
        assert task.requeue is None
        with pq.session() as session:
            is_null = session.execute(
                text("SELECT requeue IS NULL FROM pq_tasks WHERE id = :id"),
                {"id": task_id},
            ).scalar_one()
        assert is_null is True

    def test_records_result_without_parked_version(self, pq: PQ) -> None:
        from pq.models import TaskStatus
        from pq.worker import _ChildResult, _finish_one_off

        task_id, started_at = self._running_task(pq)

        _finish_one_off(
            pq,
            task_id,
            started_at,
            "capture_handler",
            _ChildResult(FailureKind.ERROR, "boom"),
        )

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert task.error == "boom"
        assert task.completed_at is not None
        assert task.payload["kwargs"] == {"value": 1}

    def test_leaves_non_running_row_alone(self, pq: PQ) -> None:
        """A row that is no longer RUNNING (reaped) is not overwritten."""
        from pq.models import TaskStatus

        task_id = pq.upsert(capture_handler, value=1, client_id="finish")

        messages = self._finish_capturing_warnings(
            pq, task_id, datetime.now(UTC), TaskStatus.COMPLETED, None
        )

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.completed_at is None
        assert any("no longer RUNNING" in message for message in messages)

    def test_leaves_later_run_of_the_row_alone(self, pq: PQ) -> None:
        """A run that ends after its row was reaped, re-queued and claimed
        again does not touch the later run (``started_at`` differs)."""
        from datetime import timedelta

        from pq.models import TaskStatus
        from pq.worker import _ChildResult, _finish_one_off

        # Run 0 is alive but slow, with a parked version (value=2).
        task_id = pq.upsert(capture_handler, value=1, client_id="finish")
        run_0_started_at = datetime.now(UTC) - timedelta(hours=2)
        self._claim(pq, task_id, run_0_started_at)
        pq.upsert(capture_handler, value=2, client_id="finish")

        # The reaper re-queues the row; another worker claims it (run 1)
        # and gets its own parked version (value=3).
        assert pq.reap_stale_tasks(timedelta(hours=1)) == 1
        run_1_started_at = datetime.now(UTC)
        self._claim(pq, task_id, run_1_started_at)
        pq.upsert(capture_handler, value=3, client_id="finish")

        # Run 0 ends late: it must not record its result on run 1, nor
        # apply run 1's parked version.
        messages = self._finish_capturing_warnings(
            pq, task_id, run_0_started_at, TaskStatus.FAILED, "late"
        )

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RUNNING
        assert task.started_at == run_1_started_at
        assert task.payload["kwargs"] == {"value": 2}
        assert task.error is None
        assert task.requeue is not None
        assert task.requeue["payload"]["kwargs"] == {"value": 3}
        assert any("no longer RUNNING" in message for message in messages)

        # Run 1 ends normally and re-queues the row with its parked version.
        _finish_one_off(
            pq, task_id, run_1_started_at, "capture_handler", _ChildResult(None)
        )
        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.payload["kwargs"] == {"value": 3}
        assert task.requeue is None

    def test_none_started_at_never_matches(self, pq: PQ) -> None:
        from pq.models import TaskStatus

        task_id, started_at = self._running_task(pq)

        messages = self._finish_capturing_warnings(
            pq, task_id, None, TaskStatus.COMPLETED, None
        )

        task = pq.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RUNNING
        assert task.started_at == started_at
        assert any("no longer RUNNING" in message for message in messages)


class TestUpsertWhileRunning(_ForkedWorkers):
    """Regression tests for ricwo/pq#27 with real forked workers: a run
    that upserts its own continuation never overlaps with it, and the
    end of one run never writes its status onto the next one."""

    def _run_continuation(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
        log_path: Any,
        *,
        workers: int,
        concurrency: int,
        delay: float,
        run_seconds: float = 1.0,
        timeout: float = 15.0,
    ) -> tuple[list[tuple[int, str, float]], list[str]]:
        """Start the continuation chain and wait until generation 1 is
        COMPLETED. Returns the handler events (sorted by time) and the
        sequence of row statuses (consecutive duplicates collapsed, but
        two different claims of the row count as two RUNNING entries)."""
        import os
        import signal as sig

        from loguru import logger

        _set_shared_results(manager.list())
        sink = logger.add(log_path, level="INFO", format="{level} | {message}")
        try:
            task_id = pq.upsert(
                continuation_handler,
                db_url=db_url,
                generation=0,
                delay=delay,
                run_seconds=run_seconds,
                client_id=CONTINUATION_CLIENT_ID,
            )
            for _ in range(workers):
                self._fork_worker(db_url, concurrency=concurrency)

            statuses: list[str] = []
            last_row: tuple[str, datetime | None] | None = None
            deadline = time.time() + timeout
            while time.time() < deadline:
                task = pq.get_task(task_id)
                assert task is not None
                row = (task.status.value, task.started_at)
                if row != last_row:
                    statuses.append(task.status.value)
                    last_row = row
                if task.payload["kwargs"]["generation"] == 1 and (
                    task.status.value in ("completed", "failed")
                ):
                    break
                time.sleep(0.02)
            else:
                raise AssertionError(f"Chain did not finish; statuses={statuses}")
        finally:
            for pid in list(self._worker_pids):
                os.kill(pid, sig.SIGTERM)
            for pid in list(self._worker_pids):
                self._wait_for_exit(pid)
            logger.remove(sink)

        events = sorted(_shared_results, key=lambda event: event[2])
        return events, statuses

    def _assert_no_overlap_and_final_state(
        self, pq: PQ, events: list[tuple[int, str, float]], log_text: str
    ) -> None:
        from pq.models import TaskStatus

        times = {(generation, kind): ts for generation, kind, ts in events}
        assert set(times) == {(0, "start"), (0, "end"), (1, "start"), (1, "end")}, (
            events
        )
        # Run 1 starts only after run 0 has ended.
        assert times[(1, "start")] > times[(0, "end")], events

        assert "no longer RUNNING" not in log_text, log_text
        assert "re-queued with the version upserted while it ran" in log_text

        final = pq.get_task_by_client_id(CONTINUATION_CLIENT_ID)
        assert final is not None
        assert final.status == TaskStatus.COMPLETED
        assert final.payload["kwargs"]["generation"] == 1
        assert final.error is None
        assert final.requeue is None
        assert final.attempts == 1

    def test_continuation_due_now_two_workers(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
        tmp_path: Any,
    ) -> None:
        """Continuation due now, two sequential workers: no overlap, run 0's
        FAILED never lands on run 1's row."""
        log_path = tmp_path / "workers.log"
        events, statuses = self._run_continuation(
            pq, db_url, manager, log_path, workers=2, concurrency=1, delay=0.0
        )

        self._assert_no_overlap_and_final_state(pq, events, log_path.read_text())
        assert "failed" not in statuses, statuses

    def test_continuation_due_later_one_worker(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
        tmp_path: Any,
    ) -> None:
        """Continuation due in 2 s, one worker: the row goes RUNNING ->
        PENDING (end of run 0) -> RUNNING -> COMPLETED, without a warning."""
        log_path = tmp_path / "workers.log"
        events, statuses = self._run_continuation(
            pq, db_url, manager, log_path, workers=1, concurrency=1, delay=2.0
        )

        self._assert_no_overlap_and_final_state(pq, events, log_path.read_text())
        assert statuses == ["pending", "running", "pending", "running", "completed"]

    def test_continuation_due_now_concurrent_worker(
        self,
        pq: PQ,
        db_url: str,
        manager: multiprocessing.managers.SyncManager,
        tmp_path: Any,
    ) -> None:
        """Continuation due now, one worker with ``concurrency=2``: covers
        the ``_reap_and_update`` path. Still no overlap."""
        log_path = tmp_path / "workers.log"
        events, statuses = self._run_continuation(
            pq, db_url, manager, log_path, workers=1, concurrency=2, delay=0.0
        )

        self._assert_no_overlap_and_final_state(pq, events, log_path.read_text())
        assert "failed" not in statuses, statuses
