"""Tests for worker logic with fork isolation.

All tests use multiprocessing shared state since tasks run in forked processes.
"""

import asyncio
import multiprocessing
import multiprocessing.managers
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from pq.client import PQ
from pq.models import Periodic
from pq.priority import Priority

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
    ) -> tuple[int, int]:
        """Fork a child running _run_concurrent. Returns (child_pid, parent_pid).

        Creates a fresh PQ instance in the child to avoid sharing
        the parent's connection pool across fork.

        Caller must eventually os.kill(pid, SIGINT) and os.waitpid(pid, 0).
        """
        import os

        from pq.worker import _run_concurrent

        pid = os.fork()
        if pid == 0:
            child_pq = PQ(db_url)
            try:
                _run_concurrent(
                    child_pq,
                    concurrency=concurrency,
                    poll_interval=poll_interval,
                    max_runtime=max_runtime,
                    priorities=None,
                    pre_execute=None,
                    post_execute=None,
                    retention_days=0,
                    cleanup_interval=3600,
                )
            except KeyboardInterrupt:
                pass
            child_pq.close()
            os._exit(0)

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
        """Task exceeding max_runtime is killed and marked failed."""
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

        from pq.worker import _run_concurrent

        import os
        import signal as sig

        pid = os.fork()
        if pid == 0:
            child_pq = PQ(db_url)
            try:
                _run_concurrent(
                    child_pq,
                    concurrency=3,
                    poll_interval=0.1,
                    max_runtime=30,
                    priorities={Priority.HIGH},
                    pre_execute=None,
                    post_execute=None,
                    retention_days=0,
                    cleanup_interval=3600,
                )
            except KeyboardInterrupt:
                pass
            child_pq.close()
            os._exit(0)

        self._wait_for(lambda: len(pq.list_completed()) >= 1)
        # Give it time to potentially pick up more tasks (it shouldn't)
        time.sleep(0.3)
        os.kill(pid, sig.SIGINT)
        os.waitpid(pid, 0)

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
            result = session.execute(
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
