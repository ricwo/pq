"""Regression tests from the bug and performance audit.

Each test pins one defect that was reproduced before it was fixed. Tests that
need a whole process of their own (a child that escapes the worker, output
buffers at ``os._exit``) run a script with ``_run_script``.
"""

import asyncio
import functools
import gc
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import BaseModel
from sqlalchemy import select, text, update

from pq import worker
from pq.client import PQ
from pq.models import Periodic, Task, TaskStatus
from pq.registry import get_function_path, resolve_function_path
from pq.serialization import deserialize, serialize

REPO_ROOT = Path(__file__).resolve().parent.parent


# --- Handlers (module level: the worker imports them by path) ---


def noop() -> None:
    pass


def exit_zero() -> None:
    sys.exit(0)


def exit_one() -> None:
    sys.exit(1)


def raise_keyboard_interrupt() -> None:
    raise KeyboardInterrupt


def raise_timeout_error() -> None:
    raise TimeoutError("upstream timed out")


async def inner_wait_for_times_out() -> None:
    await asyncio.wait_for(asyncio.sleep(5), timeout=0.05)


def ignore_alarm_and_sleep(seconds: float) -> None:
    """A task the child's own SIGALRM cannot stop (as in a blocking C call)."""
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    time.sleep(seconds)


def leave_grandchild_holding_pipe(seconds: float) -> None:
    """Fork a process that outlives the task and keeps its inherited fds."""
    if os.fork() == 0:
        time.sleep(seconds)
        os._exit(0)


def append_line(path: str, line: str) -> None:
    with open(path, "a") as f:
        f.write(line + "\n")


def print_without_flush() -> None:
    print("task-output")


def sleep_then_touch(path: str, seconds: float) -> None:
    time.sleep(seconds)
    Path(path).write_text("done")


def relock_own_schedule(db_url: str) -> None:
    """Stand in for another worker that took the periodic lock meanwhile."""
    client = PQ(db_url)
    with client.session() as session:
        session.execute(
            update(Periodic)
            .where(Periodic.name == f"{__name__}:relock_own_schedule")
            .values(locked_until=datetime.now(UTC) + timedelta(hours=1))
        )
    client.close()


def write_freeze_count(path: str) -> None:
    Path(path).write_text(str(gc.get_freeze_count()))


def raise_error_with_nul() -> None:
    raise ValueError("bad byte \x00 in response")


def shared_name() -> None:
    pass


def raise_huge_error() -> None:
    raise ValueError("x" * 500_000)


def close_inherited_fds_then_sleep(seconds: float) -> None:
    """Close the error pipe early, as a task that daemonizes would."""
    os.closerange(3, 1024)
    time.sleep(seconds)


def spawn_then_outlive_max_runtime(pid_file: str) -> None:
    """Start a process in the task's process group, then run too long."""
    pid = os.fork()
    if pid == 0:
        time.sleep(120)
        os._exit(0)
    Path(pid_file).write_text(str(pid))
    time.sleep(120)


class Event(BaseModel):
    at: datetime
    id: UUID
    amount: Decimal


def take_event(event: dict[str, str]) -> None:
    pass


def make_handler(greeting: str = "hello"):  # noqa: ANN201
    def handler(greeting: str = greeting) -> None:
        pass

    return handler


handler = make_handler()


class Cycle:
    """Garbage that only the cycle collector can free."""

    def __init__(self) -> None:
        self.me = self


class Holder:
    @staticmethod
    def method() -> None:
        pass

    @staticmethod
    def shared_name() -> None:
        pass

    @classmethod
    def class_method(cls) -> None:
        pass

    def instance_method(self) -> None:
        pass


# --- Helpers ---


def _run_script(body: str, db_url: str, timeout: float = 60) -> str:
    """Run ``body`` in a fresh interpreter with stdout on a pipe."""
    result = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(body)],
        cwd=REPO_ROOT,
        env={**os.environ, "PQ_TEST_URL": db_url, "PYTHONUNBUFFERED": ""},
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


@contextmanager
def _sigterm_self_after(seconds: float) -> Iterator[None]:
    """Send this process SIGTERM after ``seconds``, to stop a ``run_worker``
    in the block. Cancelled on the way out: outside ``run_worker`` the
    signal would end the test run."""
    timer = threading.Timer(seconds, os.kill, (os.getpid(), signal.SIGTERM))
    timer.daemon = True
    timer.start()
    try:
        yield
    finally:
        timer.cancel()


def _task(pq: PQ, task_id: int) -> Task:
    task = pq.get_task(task_id)
    assert task is not None
    return task


def _next_run(pq: PQ) -> datetime:
    """The ``next_run`` of the only schedule."""
    with pq.session() as session:
        return session.execute(select(Periodic.next_run)).scalar_one()


def _is_gone(pid: int) -> bool:
    """True if the process has exited (a zombie counts: it only awaits its
    parent's ``wait``)."""
    try:
        status = Path(f"/proc/{pid}/status").read_text()
    except FileNotFoundError:
        return True
    return "State:\tZ" in status


# --- The task child must never run the parent's code ---


class TestChildNeverEscapes:
    @pytest.mark.parametrize(
        ("handler", "status"),
        [
            ("exit_zero", "completed"),
            ("exit_one", "failed"),
            ("raise_keyboard_interrupt", "failed"),
        ],
    )
    def test_base_exception_in_handler_stays_in_the_child(
        self, pq: PQ, db_url: str, handler: str, status: str
    ) -> None:
        """``SystemExit`` and ``KeyboardInterrupt`` are not ``Exception``. They
        used to unwind out of the child's task code into the worker code that
        the child inherited from its parent: the worker's ``finally`` blocks
        and exit handlers ran a second time, in the child."""
        out = _run_script(
            f"""
            import os
            from pq import PQ
            from tests import test_audit

            pq = PQ(os.environ["PQ_TEST_URL"])
            task_id = pq.enqueue(test_audit.{handler})
            try:
                pq.run_worker_once()
            finally:
                print("worker-returned", flush=True)
            print(pq.get_task(task_id).status.value, flush=True)
            """,
            db_url,
        )
        assert out.split() == ["worker-returned", status]


class TestChildOutput:
    def test_buffered_stdout_is_written_once(self, pq: PQ, db_url: str) -> None:
        """``os._exit`` skips the flush of ``sys.stdout``, so a task's
        ``print`` to a pipe was lost. And what the worker had buffered at the
        fork is in the child's buffer too: it must not be written twice."""
        out = _run_script(
            """
            import os
            from pq import PQ
            from tests import test_audit

            pq = PQ(os.environ["PQ_TEST_URL"])
            pq.enqueue(test_audit.print_without_flush)
            print("parent-line")
            pq.run_worker_once()
            """,
            db_url,
        )
        assert out.split() == ["parent-line", "task-output"]


# --- Timeouts ---


class TestTimeouts:
    def test_timeout_error_from_sync_handler_is_a_normal_failure(self, pq: PQ) -> None:
        task_id = pq.enqueue(raise_timeout_error)
        pq.run_worker_once(max_runtime=30)
        error = _task(pq, task_id).error or ""
        assert "TimeoutError: upstream timed out" in error
        assert "Traceback" in error

    def test_timeout_error_from_async_handler_is_a_normal_failure(self, pq: PQ) -> None:
        """A ``TimeoutError`` raised inside the task (its own ``wait_for``, a
        socket timeout) was reported as the task exceeding ``max_runtime``,
        without a traceback."""
        task_id = pq.enqueue(inner_wait_for_times_out)
        pq.run_worker_once(max_runtime=30)
        error = _task(pq, task_id).error or ""
        assert "TimeoutError" in error
        assert "Traceback" in error
        assert "Timed out after" not in error

    def test_worker_kills_a_child_that_ignores_its_alarm(self, pq: PQ) -> None:
        """Only the child enforced ``max_runtime``, with SIGALRM. A child that
        cannot run the handler (blocked in C code, signal masked) ran on
        forever and held its worker slot."""
        task_id = pq.enqueue(ignore_alarm_and_sleep, 60.0)
        start = time.monotonic()
        pq.run_worker_once(max_runtime=1)
        elapsed = time.monotonic() - start
        task = _task(pq, task_id)
        assert task.status == TaskStatus.FAILED
        assert (task.error or "").startswith("Timed out after")
        assert elapsed < 1 + worker.KILL_GRACE_SECONDS + 5

    def test_timeout_kills_what_the_task_left_running(
        self, pq: PQ, tmp_path: Path
    ) -> None:
        """A child that stops itself at its max runtime used to leave the
        processes it had started running."""
        pid_file = tmp_path / "pid"
        task_id = pq.enqueue(spawn_then_outlive_max_runtime, str(pid_file))
        pq.run_worker_once(max_runtime=1)
        assert (_task(pq, task_id).error or "").startswith("Timed out after")
        left_behind = int(pid_file.read_text())
        deadline = time.monotonic() + 5
        while not _is_gone(left_behind) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _is_gone(left_behind)

    def test_child_that_closes_its_pipe_early_is_awaited_without_spinning(
        self, pq: PQ
    ) -> None:
        """End of file on the error pipe is not the end of the child. The
        worker must keep waiting for the exit, and not in a busy loop: a
        pipe at end of file reports as readable for ever."""
        task_id = pq.enqueue(close_inherited_fds_then_sleep, 1.5)
        cpu_before = time.process_time()
        start = time.monotonic()
        pq.run_worker_once()
        assert time.monotonic() - start >= 1.5
        assert time.process_time() - cpu_before < 0.3
        assert _task(pq, task_id).status == TaskStatus.COMPLETED

    def test_worker_does_not_wait_for_a_grandchild(self, pq: PQ) -> None:
        """A process the task forked keeps the error pipe open. The worker
        read that pipe to end of file after the child exited, so it waited
        for the grandchild."""
        task_id = pq.enqueue(leave_grandchild_holding_pipe, 8.0)
        start = time.monotonic()
        pq.run_worker_once()
        assert time.monotonic() - start < 5
        assert _task(pq, task_id).status == TaskStatus.COMPLETED


class TestChildProcess:
    def test_child_gc_skips_inherited_objects_and_worker_gc_is_unchanged(
        self, pq: PQ, tmp_path: Path
    ) -> None:
        """A garbage collection in the child writes to every object it
        visits, which copies the pages the child shares with the worker.
        Objects frozen before the fork are skipped."""
        out = tmp_path / "frozen"
        pq.enqueue(write_freeze_count, str(out))
        pq.run_worker_once()
        assert int(out.read_text()) > 0
        assert gc.get_freeze_count() == 0

    def test_worker_still_collects_its_own_garbage(self, pq: PQ) -> None:
        """Freezing in the worker around each fork (instead of in the child)
        moved everything the worker had allocated out of the collector's
        reach, so its cyclic garbage was never freed."""
        gc.collect()
        for _ in range(20):
            garbage = [Cycle() for _ in range(500)]
            del garbage
            pq.enqueue(noop)
            pq.run_worker_once()
        left = sum(1 for o in gc.get_objects() if isinstance(o, Cycle))
        assert left < 5000  # of 10,000; all of them stayed before the fix

    def test_huge_error_is_truncated(self, pq: PQ) -> None:
        task_id = pq.enqueue(raise_huge_error)
        pq.run_worker_once()
        error = _task(pq, task_id).error or ""
        assert len(error) < 1.2 * worker._MAX_ERROR_CHARS
        assert error.startswith("ValueError: xxx")
        assert "[... truncated ...]" in error
        assert "raise_huge_error" in error  # the traceback's frames

    def test_hook_reads_every_column_of_a_periodic_task(
        self, pq: PQ, tmp_path: Path
    ) -> None:
        """The row passed to the hooks used to be a detached ORM object with
        expired attributes: reading ``locked_until`` or ``next_run`` raised."""
        out = tmp_path / "hook"

        def pre_execute(task: Task | Periodic) -> None:
            assert isinstance(task, Periodic)
            out.write_text(f"{task.locked_until}|{task.next_run}|{task.last_run}")

        pq.schedule(noop, run_every=timedelta(hours=1))
        worker.run_worker_once(pq, pre_execute=pre_execute)
        locked_until, next_run, last_run = out.read_text().split("|")
        assert "None" not in (locked_until, next_run, last_run)


# --- Worker loop ---


class TestWorkerLoop:
    def test_sequential_worker_reaps_while_the_queue_is_busy(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With ``concurrency=1`` cleanup and the stale reaper only ran when
        the queue was empty, so a worker that always had work never ran them."""
        pending_at_reap: list[int] = []

        def reap(self: PQ, threshold: timedelta) -> int:
            pending_at_reap.append(self.pending_count())
            return 0

        monkeypatch.setattr(PQ, "reap_stale_tasks", reap)
        for _ in range(3):
            pq.enqueue(noop)
        with _sigterm_self_after(1.5):
            worker.run_worker(pq, poll_interval=0.05)
        assert pending_at_reap and pending_at_reap[0] > 0

    @pytest.mark.parametrize("concurrency", [1, 2])
    def test_cleanup_error_does_not_stop_the_worker(
        self,
        pq: PQ,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
        concurrency: int,
    ) -> None:
        """An error in the hourly cleanup propagated out of ``run_worker``.
        With ``concurrency > 1`` that skipped the drain: children in flight
        were abandoned and their rows stayed RUNNING."""

        def boom(self: PQ, before: datetime | None = None) -> int:
            raise RuntimeError("database went away")

        monkeypatch.setattr(PQ, "clear_completed", boom)
        marker = tmp_path / "done"
        task_id = pq.enqueue(sleep_then_touch, str(marker), 0.5)
        with _sigterm_self_after(2.0):
            worker.run_worker(pq, concurrency=concurrency, poll_interval=0.05)
        assert marker.exists()
        assert _task(pq, task_id).status == TaskStatus.COMPLETED

    def test_finish_is_retried_after_a_database_error(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One failed status write left a finished task RUNNING until the
        stale reaper failed it an hour later."""
        task_id = pq.enqueue(noop)
        write = worker._write_one_off_result
        attempts = 0

        def flaky_write(*args: object) -> TaskStatus | None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise ConnectionError("connection reset")
            return write(*args)

        monkeypatch.setattr(worker, "_write_one_off_result", flaky_write)
        monkeypatch.setattr(worker, "_FINISH_RETRY_DELAYS", (0.01, 0.01))
        pq.run_worker_once()
        assert attempts == 2
        assert _task(pq, task_id).status == TaskStatus.COMPLETED

    def test_rejected_status_write_is_not_retried(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from sqlalchemy.exc import DataError

        attempts = 0

        def rejected(*args: object) -> TaskStatus | None:
            nonlocal attempts
            attempts += 1
            raise DataError("UPDATE", {}, ValueError("rejected"))

        pq.enqueue(noop)
        monkeypatch.setattr(worker, "_write_one_off_result", rejected)
        pq.run_worker_once()
        assert attempts == 1

    def test_error_text_with_nul_character_is_stored(self, pq: PQ) -> None:
        """Postgres rejects the NUL character in text. The status write
        failed, and the task stayed RUNNING until the stale reaper."""
        task_id = pq.enqueue(raise_error_with_nul)
        pq.run_worker_once()
        task = _task(pq, task_id)
        assert task.status == TaskStatus.FAILED
        assert "bad byte \\x00 in response" in (task.error or "")

    def test_failed_fork_closes_its_pipe(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def no_fork() -> int:
            raise OSError("fork: resource temporarily unavailable")

        task_id = pq.enqueue(noop)
        open_before = set(os.listdir("/proc/self/fd"))
        monkeypatch.setattr(os, "fork", no_fork)
        pq.run_worker_once()
        monkeypatch.undo()
        assert set(os.listdir("/proc/self/fd")) == open_before
        task = _task(pq, task_id)
        assert task.status == TaskStatus.FAILED
        assert "fork" in (task.error or "")

    def test_run_worker_closes_its_wake_pipe(self, pq: PQ) -> None:
        open_before = set(os.listdir("/proc/self/fd"))
        start = time.monotonic()
        with _sigterm_self_after(0.3):
            worker.run_worker(pq, poll_interval=30)
        # The signal ends the 30 s sleep at once.
        assert time.monotonic() - start < 5
        assert set(os.listdir("/proc/self/fd")) == open_before

    def test_stale_timeout_is_raised_above_max_runtime(self) -> None:
        """A reaper window shorter than ``max_runtime`` fails live tasks."""
        hour = timedelta(hours=1)
        assert worker._effective_stale_timeout(hour, max_runtime=7200) == timedelta(
            seconds=14400
        )
        assert worker._effective_stale_timeout(hour, max_runtime=1800) == hour
        assert worker._effective_stale_timeout(None, max_runtime=7200) is None
        # A short max runtime: the floor is as long as a child can live.
        assert worker._effective_stale_timeout(
            timedelta(seconds=1), max_runtime=2
        ) == timedelta(seconds=2 + worker.KILL_GRACE_SECONDS + 5)


# --- Periodic schedules ---


class TestPeriodic:
    def test_reschedule_with_same_timing_keeps_next_run(self, pq: PQ) -> None:
        """Workers call ``schedule()`` on every start. That reset ``next_run``
        to now, so each interval schedule (a daily one included) fired after
        every deploy, once per restarted worker."""
        pq.schedule(noop, run_every=timedelta(days=1))
        pq.run_worker_once()
        advanced = _next_run(pq)
        assert advanced > datetime.now(UTC) + timedelta(hours=23)

        pq.schedule(noop, run_every=timedelta(days=1))
        assert _next_run(pq) == advanced

    def test_reschedule_with_new_timing_resets_next_run(self, pq: PQ) -> None:
        pq.schedule(noop, run_every=timedelta(days=1))
        pq.run_worker_once()
        pq.schedule(noop, run_every=timedelta(hours=1))
        next_run = _next_run(pq)
        assert next_run <= datetime.now(UTC)

        pq.schedule(noop, cron="0 0 1 1 *")
        next_run = _next_run(pq)
        assert next_run > datetime.now(UTC) + timedelta(days=1)

    def test_cron_does_not_fire_twice_when_worker_clock_is_behind(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The next cron time was computed from the worker's clock, but a
        schedule is due by the database's clock. A worker a little behind got
        the tick that had just fired as the "next" one, and ran it again."""
        pq.schedule(noop, cron="* * * * *")
        fired = datetime.now(UTC).replace(second=0, microsecond=0)
        with pq.session() as session:
            session.execute(update(Periodic).values(next_run=fired))

        class BehindClock(datetime):
            @classmethod
            def now(cls, tz=None):  # noqa: ANN001, ANN206
                return datetime.now(tz) - timedelta(minutes=2)

        monkeypatch.setattr(worker, "datetime", BehindClock)
        pq.run_worker_once()
        monkeypatch.undo()
        next_run = _next_run(pq)
        assert next_run > fired

    def test_cron_is_read_in_utc_whatever_the_session_time_zone(
        self, pq: PQ, db_url: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The database returns ``next_run`` in the session's time zone, and
        croniter reads the expression in the time zone of the time it starts
        from: a daily 09:00 schedule got 08:00 as its next run."""
        pq.schedule(noop, cron="0 9 * * *")
        fired = datetime.now(UTC).replace(hour=9, minute=0, second=0, microsecond=0)
        if fired > datetime.now(UTC):
            fired -= timedelta(days=1)
        with pq.session() as session:
            session.execute(update(Periodic).values(next_run=fired))

        class JustBehindClock(datetime):
            @classmethod
            def now(cls, tz=None):  # noqa: ANN001, ANN206
                return fired - timedelta(seconds=1)

        monkeypatch.setattr(worker, "datetime", JustBehindClock)
        with PQ(db_url + "&options=-c%20timezone%3DEurope/Berlin") as berlin:
            berlin.run_worker_once()
        monkeypatch.undo()
        next_run = _next_run(pq)
        assert next_run == fired + timedelta(days=1)

    def test_reactivated_schedule_gets_a_fresh_next_run(self, pq: PQ) -> None:
        pq.schedule(noop, cron="0 0 1 1 *", active=False)
        with pq.session() as session:
            session.execute(
                update(Periodic).values(next_run=datetime.now(UTC) - timedelta(days=30))
            )
        pq.schedule(noop, cron="0 0 1 1 *", active=True)
        next_run = _next_run(pq)
        assert next_run > datetime.now(UTC)

    def test_tasks_and_schedules_run_in_one_order(self, pq: PQ, tmp_path: Path) -> None:
        """One-off tasks used to be claimed first whatever their priority:
        while any was due, no periodic schedule ran. Now both form one queue,
        by priority and then by the time they came due."""
        from pq import Priority

        order = tmp_path / "order"
        long_due = datetime.now(UTC) - timedelta(hours=1)
        pq.enqueue(append_line, str(order), "earlier-task", run_at=long_due)
        pq.schedule(append_line, str(order), "schedule", run_every=timedelta(hours=1))
        with pq.session() as session:
            session.execute(
                update(Periodic).values(next_run=long_due + timedelta(minutes=1))
            )
        pq.enqueue(append_line, str(order), "later-task")
        pq.enqueue(append_line, str(order), "urgent-task", priority=Priority.HIGH)
        pq.enqueue(append_line, str(order), "batch-task", priority=Priority.BATCH)

        while pq.run_worker_once():
            pass
        assert order.read_text().split() == [
            "urgent-task",
            "earlier-task",
            "schedule",
            "later-task",
            "batch-task",
        ]

    def test_schedule_that_is_always_due_does_not_starve_tasks(
        self, pq: PQ, tmp_path: Path
    ) -> None:
        order = tmp_path / "order"
        pq.schedule(append_line, str(order), "schedule", run_every=timedelta(0))
        pq.run_worker_once()
        pq.enqueue(append_line, str(order), "task")
        for _ in range(3):
            pq.run_worker_once()
        assert "task" in order.read_text().split()[:3]

    @pytest.mark.parametrize("failure", ["raises", "taken"])
    def test_schedule_that_cannot_be_claimed_does_not_block_tasks(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str
    ) -> None:
        """A due schedule goes before the tasks that came due after it. If
        its claim fails, or another worker took it meanwhile, those tasks
        must run all the same."""

        def no_claim(*args: object, **kwargs: object) -> None:
            if failure == "raises":
                raise OverflowError("date value out of range")

        order = tmp_path / "order"
        pq.schedule(append_line, str(order), "schedule", run_every=timedelta(hours=1))
        pq.enqueue(append_line, str(order), "task")
        monkeypatch.setattr(worker, "_claim_periodic", no_claim)
        assert pq.run_worker_once() is True
        assert order.read_text().split() == ["task"]

    def test_task_does_not_yield_to_a_schedule_behind_it(
        self, pq: PQ, db_url: str, tmp_path: Path
    ) -> None:
        """While another worker is claiming the schedule that is next, a
        schedule further back must not overtake a task that is ahead of it."""
        from pq import Priority

        order = tmp_path / "order"
        pq.schedule(
            append_line,
            str(order),
            "high-schedule",
            run_every=timedelta(hours=1),
            priority=Priority.HIGH,
            key="high",
        )
        pq.schedule(
            append_line,
            str(order),
            "low-schedule",
            run_every=timedelta(hours=1),
            priority=Priority.LOW,
            key="low",
        )
        pq.enqueue(append_line, str(order), "normal-task")
        with PQ(db_url) as other_worker, other_worker.session() as claiming:
            claiming.execute(
                select(Periodic).where(Periodic.key == "high").with_for_update()
            )
            pq.run_worker_once()
        assert order.read_text().split() == ["normal-task"]

    @pytest.mark.parametrize("head", ["locked", "broken"])
    def test_schedule_behind_one_that_cannot_be_claimed_still_runs(
        self, pq: PQ, db_url: str, tmp_path: Path, head: str
    ) -> None:
        """The first due schedule is being claimed by another worker, or its
        claim fails on every poll. The schedule behind it runs."""
        from pq import Priority

        order = tmp_path / "order"
        for key, priority in (("head", Priority.HIGH), ("behind", Priority.LOW)):
            pq.schedule(
                append_line,
                str(order),
                key,
                run_every=timedelta(hours=1),
                priority=priority,
                key=key,
            )
        if head == "broken":
            # A row from before such values were rejected: its lock time
            # overflows when the schedule is claimed.
            with pq.session() as session:
                session.execute(
                    update(Periodic)
                    .where(Periodic.key == "head")
                    .values(max_runtime_seconds=1e14)
                )
            assert pq.run_worker_once() is True
        else:
            with PQ(db_url) as other_worker, other_worker.session() as claiming:
                claiming.execute(
                    select(Periodic).where(Periodic.key == "head").with_for_update()
                )
                assert pq.run_worker_once() is True
        assert order.read_text().split() == ["behind"]

    def test_schedule_values_that_would_overflow_are_rejected(self, pq: PQ) -> None:
        """They made the claim of the schedule fail on every poll."""
        with pytest.raises(ValueError, match="run_every"):
            pq.schedule(noop, run_every=timedelta(days=10**8))
        with pytest.raises(ValueError, match="max_runtime_seconds"):
            pq.schedule(noop, run_every=timedelta(hours=1), max_runtime_seconds=1e14)
        with pytest.raises(ValueError, match="max_runtime_seconds"):
            pq.enqueue(noop, max_runtime_seconds=1e14)
        with pytest.raises(ValueError, match="max_runtime_seconds"):
            pq.upsert(noop, client_id="x", max_runtime_seconds=1e14)

    def test_finished_run_leaves_a_newer_lock_alone(self, pq: PQ, db_url: str) -> None:
        """The end of a run cleared ``locked_until`` whatever it held, so a
        run that outlived its lock released the lock of the run after it."""
        pq.schedule(relock_own_schedule, db_url, run_every=timedelta(hours=1))
        pq.run_worker_once()
        with pq.session() as session:
            locked_until = session.execute(select(Periodic.locked_until)).scalar_one()
        assert locked_until is not None

    def test_lock_outlives_the_hard_kill(self, pq: PQ) -> None:
        """The lock has to cover the whole time a child can be alive."""
        pq.schedule(noop, run_every=timedelta(hours=1))
        claim = worker._claim_periodic(pq, max_runtime=100, priorities=())
        assert claim is not None and claim.locked_until is not None
        with pq.session() as session:
            now = session.execute(text("SELECT now()")).scalar_one()
        assert claim.locked_until - now > timedelta(
            seconds=100 + worker.KILL_GRACE_SECONDS
        )


# --- Serialization and registry ---


class SubHolder(Holder):
    pass


class TestSerialization:
    def test_pydantic_model_with_non_json_fields(self, pq: PQ) -> None:
        """``model_dump()`` keeps ``datetime``, ``UUID`` and ``Decimal`` as
        Python objects, which the JSONB column cannot store: the enqueue
        raised ``TypeError``."""
        event = Event(
            at=datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            id=UUID("12345678-1234-5678-1234-567812345678"),
            amount=Decimal("1.50"),
        )
        task_id = pq.enqueue(take_event, event)
        args, _ = deserialize(_task(pq, task_id).payload)
        assert args == (
            {
                "at": "2026-01-02T03:04:05Z",
                "id": "12345678-1234-5678-1234-567812345678",
                "amount": "1.50",
            },
        )

    def test_nul_character_is_pickled(self, pq: PQ) -> None:
        """Postgres rejects the NUL character in JSONB: the enqueue raised."""
        task_id = pq.enqueue(noop, "a\x00b", key={"nested": ["\x00"]})
        args, kwargs = deserialize(_task(pq, task_id).payload)
        assert args == ("a\x00b",)
        assert kwargs == {"key": {"nested": ["\x00"]}}

    def test_nan_is_pickled_not_stored_as_invalid_json(self) -> None:
        """``json.dumps`` writes ``NaN``, which Postgres rejects in JSONB."""
        payload = serialize((float("nan"),), {})
        assert payload["pickled_args"] == [0]
        (restored,), _ = deserialize(payload)
        assert restored != restored


class TestRegistry:
    def test_partial_is_rejected_with_value_error(self) -> None:
        with pytest.raises(ValueError, match="import path"):
            get_function_path(functools.partial(noop))

    def test_nested_function_is_rejected(self) -> None:
        def nested() -> None:
            pass

        with pytest.raises(ValueError, match="nested"):
            get_function_path(nested)

    def test_static_method_round_trips(self) -> None:
        """A method was stored as ``module:method`` and could never be
        imported by the worker."""
        path = get_function_path(Holder.method)
        assert path == f"{__name__}:Holder.method"
        assert resolve_function_path(path) is Holder.method

    def test_method_is_not_mistaken_for_a_module_function_of_its_name(self) -> None:
        """A name the module also has must not stand in for the method: the
        worker would run the other function."""
        path = get_function_path(Holder.shared_name)
        assert path == f"{__name__}:Holder.shared_name"
        assert resolve_function_path(path) is Holder.shared_name

        def noop() -> None:  # same name as the module-level handler
            pass

        with pytest.raises(ValueError, match="nested"):
            get_function_path(noop)

    def test_function_from_a_factory_resolves_to_its_exported_twin(self) -> None:
        """A second function from the same factory runs the same code as the
        one the module exports under that name."""
        assert make_handler() is not handler
        assert get_function_path(make_handler()) == f"{__name__}:handler"

    def test_inherited_class_method_is_rejected(self) -> None:
        """Its path leads to the base class: the worker would call the
        method on the wrong class."""
        with pytest.raises(ValueError, match="cannot import"):
            get_function_path(SubHolder.class_method)

    def test_factory_function_with_other_defaults_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="cannot import"):
            get_function_path(make_handler(greeting="other"))

    def test_class_method_round_trips(self) -> None:
        path = get_function_path(Holder.class_method)
        assert resolve_function_path(path) == Holder.class_method

    def test_method_bound_to_an_instance_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="bound to an instance"):
            get_function_path(Holder().instance_method)


class TestClient:
    def test_session_is_transactional_after_an_autocommit_call(self, pq: PQ) -> None:
        """``_autocommit`` switches the pooled connection to autocommit and
        back. A connection left in autocommit would make ``session()`` lose
        its rollback."""
        pq.enqueue(noop)  # uses, and returns, the pooled connection
        with pytest.raises(RuntimeError, match="undo"):
            with pq.session() as session:
                session.execute(update(Task).values(name="changed"))
                raise RuntimeError("undo")
        with pq.session() as session:
            assert session.execute(select(Task.name)).scalar_one() != "changed"

    def test_upsert_many_sends_one_statement_for_the_batch(self, pq: PQ) -> None:
        from sqlalchemy import event

        from pq import Upsert

        statements: list[str] = []

        def record(conn, cursor, statement, *args: object) -> None:  # noqa: ANN001
            statements.append(statement)

        event.listen(pq._engine, "before_cursor_execute", record)
        try:
            ids = pq.upsert_many(
                [Upsert(task=noop, client_id=c) for c in ["b", "a", "b", "c"]]
            )
        finally:
            event.remove(pq._engine, "before_cursor_execute", record)
        assert len([s for s in statements if s.startswith("INSERT")]) == 1
        assert ids[0] == ids[2]
        assert len(set(ids)) == 3


class TestPackaging:
    def test_console_scripts_resolve(self) -> None:
        """``pyproject.toml`` declared ``pq = pq.cli:main`` with no
        ``pq.cli`` module, so the installed ``pq`` command crashed."""
        from importlib.metadata import entry_points

        for script in entry_points(group="console_scripts"):
            if script.dist is not None and script.dist.name == "python-pq":
                script.load()
