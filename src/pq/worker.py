"""Worker loop for processing tasks with fork isolation.

Each task runs in a forked child process for memory isolation.
If a task OOMs or crashes, only the child is affected - the worker continues.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import os
import select as select_module
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import FrameType
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

from croniter import croniter
from loguru import logger
from sqlalchemy import func, or_, select, update

from pq.models import Periodic, Task, TaskStatus
from pq.registry import resolve_function_path
from pq.serialization import deserialize

if TYPE_CHECKING:
    from collections.abc import Set

    from pq.client import PQ
    from pq.priority import Priority

# What ``signal.getsignal`` returns / ``signal.signal`` accepts.
_SignalHandler = (
    Callable[[int, FrameType | None], object] | int | signal.Handlers | None
)


class PreExecuteHook(Protocol):
    """Protocol for pre-execution hooks called before task handler runs.

    Called in the forked child process before task execution.
    Use for initializing fork-unsafe resources (OTel, DB connections).
    """

    def __call__(self, task: Task | Periodic) -> None:
        """Called before task execution.

        Args:
            task: The task about to be executed.
        """
        ...


class PostExecuteHook(Protocol):
    """Protocol for post-execution hooks called after task handler completes.

    Called in the forked child process after task execution (success or failure).
    Use for cleanup/flushing (OTel traces, etc.).
    """

    def __call__(self, task: Task | Periodic, error: Exception | None) -> None:
        """Called after task execution.

        Args:
            task: The task that was executed.
            error: The exception if task failed, None if successful.
        """
        ...


# Default max runtime: 30 minutes
DEFAULT_MAX_RUNTIME: float = 30 * 60

# Default retention: 7 days
DEFAULT_RETENTION_DAYS: int = 7

# Default cleanup interval: 1 hour
DEFAULT_CLEANUP_INTERVAL: float = 3600

# Default stale task timeout: 1 hour
# Tasks RUNNING longer than this are assumed orphaned (worker died) and reaped.
DEFAULT_STALE_TASK_TIMEOUT: timedelta = timedelta(hours=1)

# Default reaper check interval: 5 minutes
# How often the worker checks for stale RUNNING tasks.
DEFAULT_REAPER_INTERVAL: float = 300

# Default drain timeout: 20 seconds
# On SIGTERM/SIGINT the worker stops claiming and waits up to this long for
# in-flight tasks to finish before SIGKILLing them and marking them FAILED.
# Sits under the common 30 s orchestrator termination grace period, leaving
# headroom for the final status writes.
DEFAULT_DRAIN_TIMEOUT: float = 20.0

# Exit codes for child process
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_TIMEOUT = 124  # Like GNU timeout


class _ChildResult(NamedTuple):
    """Result from waiting for a forked child process."""

    task_status: TaskStatus
    error_msg: str | None
    exit_kind: str  # "success", "timeout", "oom", "killed", "error"


@dataclass
class _ChildSlot:
    """Tracks a running forked child in concurrent mode."""

    pid: int
    read_fd: int
    task_id: int
    name: str
    start_time: float
    is_periodic: bool
    periodic_max_concurrent: int | None = None


class WorkerError(Exception):
    """Base class for worker errors."""

    pass


class TaskTimeoutError(WorkerError):
    """Raised when a task exceeds its max runtime."""

    pass


class TaskOOMError(WorkerError):
    """Raised when a task is killed by OOM killer."""

    pass


class TaskKilledError(WorkerError):
    """Raised when a task is killed by a signal."""

    pass


class TaskInterruptedError(WorkerError):
    """Raised when a task is killed because the worker is shutting down."""

    pass


@dataclass
class _ShutdownState:
    """Graceful-shutdown coordination for the worker loops.

    Written by the signal handler (which runs in the main thread between
    bytecodes, so no locking is needed) and read by the worker loops and
    ``_wait_for_child``. Module-level because a worker owns its process;
    ``run_worker`` resets it on entry.
    """

    requested: bool = False
    deadline: float | None = None  # time.monotonic() deadline, set on signal
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT

    def reset(self, drain_timeout: float) -> None:
        self.requested = False
        self.deadline = None
        self.drain_timeout = drain_timeout

    def deadline_passed(self) -> bool:
        """True once shutdown was requested and the drain budget is spent."""
        if not self.requested:
            return False
        return self.deadline is None or time.monotonic() >= self.deadline


_shutdown = _ShutdownState()


def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    """SIGTERM/SIGINT handler: request a drain, never raise.

    Raising from here would land at arbitrary bytecode and could orphan a
    task (e.g. between claim-commit and child registration). Repeat signals
    during the drain are ignored so they can't abandon remaining children.
    """
    if _shutdown.requested:
        return
    _shutdown.deadline = time.monotonic() + _shutdown.drain_timeout
    _shutdown.requested = True
    logger.info(
        f"Received signal {signum}, draining"
        f" (drain_timeout={_shutdown.drain_timeout}s)..."
    )


def _install_shutdown_handlers() -> dict[int, _SignalHandler] | None:
    """Register the drain handler for SIGTERM/SIGINT.

    Returns the previous handlers (for restoration), or ``None`` when not
    running in the main thread — ``signal.signal`` is only allowed there, so
    in that case the worker keeps the legacy KeyboardInterrupt-only behavior.
    """
    if threading.current_thread() is not threading.main_thread():
        logger.warning(
            "Worker is not running in the main thread; SIGTERM graceful"
            " shutdown is disabled (only KeyboardInterrupt is handled)."
        )
        return None
    previous: dict[int, _SignalHandler] = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, _handle_shutdown_signal)
    return previous


def _restore_shutdown_handlers(previous: dict[int, _SignalHandler] | None) -> None:
    """Restore the signal handlers saved by ``_install_shutdown_handlers``."""
    if previous is None:
        return
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _kill_child_group(child_pid: int) -> None:
    """SIGKILL a task child's process group (best effort).

    The child calls ``os.setpgrp()`` first thing, so its pgid equals its
    pid. If the signal lands before ``setpgrp()`` the process group doesn't
    exist yet — fall back to killing the pid directly. ESRCH everywhere
    means the child already exited; the caller reaps it normally.
    """
    try:
        os.killpg(child_pid, signal.SIGKILL)
    except ProcessLookupError:
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def _child_timeout_handler(signum: int, frame: Any) -> None:
    """Signal handler for timeout in child process."""
    os._exit(EXIT_TIMEOUT)


def _run_in_child(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    max_runtime: float,
    error_write_fd: int,
    task: Task | Periodic,
    pre_execute: PreExecuteHook | None,
    post_execute: PostExecuteHook | None,
) -> None:
    """Execute handler in child process.

    This function never returns - it always calls os._exit().
    """
    # Create new process group so we don't get parent's signals
    os.setpgrp()

    # Restore default signal disposition: the fork inherits the parent's
    # drain handler, which in the child would silently swallow a direct
    # SIGTERM/SIGINT (e.g. from systemd's cgroup-wide kill).
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    # Set up timeout
    signal.signal(signal.SIGALRM, _child_timeout_handler)
    signal.alarm(int(max_runtime) + 1)  # +1 buffer for async timeout

    error: Exception | None = None
    error_tb: str = ""

    try:
        if pre_execute:
            pre_execute(task)

        if inspect.iscoroutinefunction(handler):
            asyncio.run(asyncio.wait_for(handler(*args, **kwargs), timeout=max_runtime))
        else:
            handler(*args, **kwargs)

    except asyncio.TimeoutError as e:
        error = e

    except Exception as e:
        error = e
        error_tb = traceback.format_exc()

    finally:
        try:
            if post_execute:
                post_execute(task, error)
        except Exception:
            pass  # Don't let hook errors mask task errors

    if error is None:
        os._exit(EXIT_SUCCESS)
    elif isinstance(error, asyncio.TimeoutError):
        os._exit(EXIT_TIMEOUT)
    else:
        # Send error message to parent via pipe
        try:
            error_msg = f"{type(error).__name__}: {error}\n{error_tb}"
            os.write(error_write_fd, error_msg.encode("utf-8", errors="replace"))
        except Exception:
            pass  # Best effort
        os._exit(EXIT_FAILURE)


def _fork_child(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    max_runtime: float,
    task: Task | Periodic,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> tuple[int, int]:
    """Fork a child process to execute the handler.

    Returns:
        Tuple of (child_pid, read_fd) for monitoring the child.
    """
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()

    if child_pid == 0:
        # === CHILD PROCESS ===
        os.close(read_fd)
        _run_in_child(
            handler,
            args,
            kwargs,
            max_runtime,
            write_fd,
            task,
            pre_execute,
            post_execute,
        )
        os._exit(EXIT_FAILURE)  # _run_in_child never returns, but just in case

    # === PARENT PROCESS ===
    os.close(write_fd)
    return child_pid, read_fd


def _wait_for_child(child_pid: int, read_fd: int) -> _ChildResult:
    """Wait for a forked child process and return the result.

    The wait is drain-aware: once shutdown has been requested and the drain
    deadline has passed, the child's process group is SIGKILLed and the
    result reports the ``"shutdown"`` exit kind. A blocking ``os.wait4``
    would never notice the shutdown flag (PEP 475 retries the syscall after
    the non-raising signal handler), so this polls with ``WNOHANG`` and
    sleeps in ``select`` on the child's error pipe — the pipe closes when
    the child exits, so child completion wakes the wait immediately, while
    the bounded timeout keeps flag/deadline checks prompt.

    Args:
        child_pid: PID of the child process.
        read_fd: Read end of the error pipe.

    Returns:
        _ChildResult with task status, error message, and exit kind.
    """
    killed_by_drain = False
    error_bytes = b""
    while True:
        pid, raw_status, rusage = os.wait4(child_pid, os.WNOHANG)
        if pid != 0:
            break
        if not killed_by_drain and _shutdown.deadline_passed():
            _kill_child_group(child_pid)
            killed_by_drain = True
        timeout = 1.0
        if _shutdown.requested and _shutdown.deadline is not None:
            timeout = min(timeout, max(0.0, _shutdown.deadline - time.monotonic()))
            timeout = max(timeout, 0.01)  # killed child needs a moment to exit
        ready, _, _ = select_module.select([read_fd], [], [], timeout)
        if ready:
            # Drain incrementally: an error message larger than the pipe
            # buffer would otherwise block the child in write() while we
            # spin on an always-readable fd.
            try:
                error_bytes += os.read(read_fd, 65536)
            except OSError:
                pass

    # Read any remaining error output from the pipe
    try:
        while True:
            chunk = os.read(read_fd, 4096)
            if not chunk:
                break
            error_bytes += chunk
    except Exception:
        pass
    finally:
        os.close(read_fd)

    error_msg = error_bytes.decode("utf-8", errors="replace") if error_bytes else ""

    if os.WIFSIGNALED(raw_status):
        signal_num = os.WTERMSIG(raw_status)
        if killed_by_drain and signal_num == signal.SIGKILL:
            return _ChildResult(
                TaskStatus.FAILED,
                "Task interrupted by worker shutdown"
                f" (exceeded drain_timeout={_shutdown.drain_timeout}s)",
                "shutdown",
            )
        if signal_num == signal.SIGKILL:
            max_rss_kb = rusage.ru_maxrss
            if sys.platform == "darwin":
                max_rss_kb = max_rss_kb // 1024
            return _ChildResult(
                TaskStatus.FAILED,
                f"Task killed (likely OOM). Max RSS: {max_rss_kb} KB",
                "oom",
            )
        return _ChildResult(
            TaskStatus.FAILED,
            f"Task killed by signal {signal_num}",
            "killed",
        )

    if os.WIFEXITED(raw_status):
        exit_code = os.WEXITSTATUS(raw_status)
        if exit_code == EXIT_SUCCESS:
            return _ChildResult(TaskStatus.COMPLETED, None, "success")
        if exit_code == EXIT_TIMEOUT:
            return _ChildResult(
                TaskStatus.FAILED, "Task exceeded max runtime", "timeout"
            )
        if error_msg:
            return _ChildResult(TaskStatus.FAILED, error_msg.rstrip(), "error")
        return _ChildResult(
            TaskStatus.FAILED,
            f"Task failed with exit code {exit_code}",
            "error",
        )

    return _ChildResult(TaskStatus.FAILED, "Unknown child exit status", "error")


def _execute_in_fork(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    *,
    max_runtime: float,
    task: Task | Periodic,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> None:
    """Execute handler in a forked child process for isolation.

    The child process has isolated memory, so OOM or crashes only affect
    the child. Parent monitors via os.wait4() and handles various exit
    scenarios.

    Args:
        handler: Task handler function.
        args: Positional arguments for handler.
        kwargs: Keyword arguments for handler.
        max_runtime: Maximum execution time in seconds.
        task: The task being executed (passed to hooks).
        pre_execute: Called in forked child BEFORE task execution.
        post_execute: Called in forked child AFTER task execution.

    Raises:
        TaskTimeoutError: If task exceeds max runtime.
        TaskOOMError: If task is killed by OOM killer (SIGKILL).
        TaskKilledError: If task is killed by another signal.
        Exception: If task raises an exception.
    """
    child_pid, read_fd = _fork_child(
        handler,
        args,
        kwargs,
        max_runtime=max_runtime,
        task=task,
        pre_execute=pre_execute,
        post_execute=post_execute,
    )
    result = _wait_for_child(child_pid, read_fd)

    if result.exit_kind == "success":
        return
    if result.exit_kind == "timeout":
        raise TaskTimeoutError(result.error_msg or "Task exceeded max runtime")
    if result.exit_kind == "oom":
        raise TaskOOMError(result.error_msg or "Task killed (likely OOM)")
    if result.exit_kind == "killed":
        raise TaskKilledError(result.error_msg or "Task killed by signal")
    if result.exit_kind == "shutdown":
        raise TaskInterruptedError(
            result.error_msg or "Task interrupted by worker shutdown"
        )
    raise Exception(result.error_msg or "Task failed")


def _interruptible_sleep(seconds: float) -> None:
    """Sleep up to ``seconds``, returning early once shutdown is requested.

    A plain ``time.sleep`` would run to completion despite SIGTERM (PEP 475
    retries it after the non-raising handler), delaying an idle worker's
    shutdown by up to ``poll_interval``.
    """
    deadline = time.monotonic() + seconds
    while not _shutdown.requested:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        time.sleep(min(0.1, remaining))


def _maybe_run_cleanup(
    pq: PQ,
    retention_days: int,
    cleanup_interval: float,
    last_cleanup: list[float],
) -> None:
    """Run cleanup if retention is enabled and interval has passed.

    Args:
        pq: PQ client instance.
        retention_days: Days to keep completed/failed tasks. 0 to disable.
        cleanup_interval: Seconds between cleanup runs.
        last_cleanup: Mutable list containing last cleanup timestamp.
    """
    if retention_days <= 0:
        return

    now = time.time()
    if now - last_cleanup[0] < cleanup_interval:
        return

    cutoff = datetime.now(UTC) - timedelta(days=retention_days)
    completed = pq.clear_completed(before=cutoff)
    failed = pq.clear_failed(before=cutoff)

    if completed or failed:
        logger.info(f"Cleanup: removed {completed} completed, {failed} failed tasks")

    last_cleanup[0] = now


def _maybe_reap_stale(
    pq: PQ,
    stale_task_timeout: timedelta | None,
    reaper_interval: float,
    last_reap: list[float],
) -> None:
    """Reap stale RUNNING tasks if enabled and interval has passed.

    Args:
        pq: PQ client instance.
        stale_task_timeout: RUNNING tasks older than this are marked FAILED.
            ``None`` disables reaping.
        reaper_interval: Seconds between reaper checks.
        last_reap: Mutable list containing last reap timestamp.
    """
    if stale_task_timeout is None:
        return

    now = time.time()
    if now - last_reap[0] < reaper_interval:
        return

    reaped = pq.reap_stale_tasks(stale_task_timeout)
    if reaped:
        logger.info(f"Reaped {reaped} stale RUNNING task(s)")

    last_reap[0] = now


def run_worker(
    pq: PQ,
    *,
    concurrency: int = 1,
    poll_interval: float = 1.0,
    max_runtime: float = DEFAULT_MAX_RUNTIME,
    priorities: Set[Priority] | None = None,
    retention_days: int = DEFAULT_RETENTION_DAYS,
    cleanup_interval: float = DEFAULT_CLEANUP_INTERVAL,
    stale_task_timeout: timedelta | None = DEFAULT_STALE_TASK_TIMEOUT,
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> None:
    """Run the worker loop indefinitely.

    Each task executes in a forked child process for memory isolation.

    On SIGTERM or SIGINT the worker shuts down gracefully: it stops
    claiming new tasks, waits up to ``drain_timeout`` seconds for in-flight
    tasks to finish (writing their statuses as usual), then SIGKILLs any
    still-running task children and marks their rows FAILED with an explicit
    shutdown error. Interrupted tasks are NOT re-queued — pq's at-most-once
    semantics are preserved; applications that need redelivery must
    re-enqueue such tasks themselves.

    Args:
        pq: PQ client instance.
        concurrency: Maximum number of tasks to process simultaneously.
            Default: 1 (sequential processing).
        poll_interval: Seconds to sleep between polls when idle.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        priorities: If set, only process tasks with these priority levels.
            Use this to dedicate workers to specific priority tiers.
        retention_days: Days to keep completed/failed tasks. Default: 7.
            Set to 0 to disable automatic cleanup.
        cleanup_interval: Seconds between cleanup runs. Default: 3600 (1 hour).
        stale_task_timeout: Mark RUNNING tasks older than this as FAILED.
            Catches orphaned tasks whose worker died mid-execution.
            Default: 1 hour. Set to ``None`` to disable.
        drain_timeout: Seconds to wait for in-flight tasks when shutting
            down on SIGTERM/SIGINT. Default: 20. Must be set below the
            orchestrator's termination grace period, with headroom for the
            final status writes. ``0`` skips the wait entirely: in-flight
            tasks are killed as soon as the shutdown is noticed (within
            about a second).
        pre_execute: Called in forked child BEFORE task execution.
            Use for initializing fork-unsafe resources (OTel, DB connections).
        post_execute: Called in forked child AFTER task execution (success or failure).
            Use for cleanup/flushing (OTel traces, etc.).

    Raises:
        ValueError: If ``drain_timeout`` is negative or not finite.
    """
    if math.isnan(drain_timeout) or math.isinf(drain_timeout) or drain_timeout < 0:
        raise ValueError(
            f"drain_timeout must be a finite number >= 0 (got {drain_timeout!r})"
        )
    if priorities:
        priority_names = ", ".join(p.name for p in sorted(priorities, reverse=True))
        logger.info(
            f"Starting PQ worker (priorities: {priority_names},"
            f" concurrency: {concurrency})..."
        )
    else:
        logger.info(
            f"Starting PQ worker (concurrency: {concurrency},"
            " fork isolation enabled)..."
        )

    _shutdown.reset(drain_timeout)
    previous_handlers = _install_shutdown_handlers()
    try:
        if concurrency > 1:
            _run_concurrent(
                pq,
                concurrency=concurrency,
                poll_interval=poll_interval,
                max_runtime=max_runtime,
                priorities=priorities,
                pre_execute=pre_execute,
                post_execute=post_execute,
                retention_days=retention_days,
                cleanup_interval=cleanup_interval,
                stale_task_timeout=stale_task_timeout,
            )
            return

        last_cleanup: list[float] = [0.0]
        last_reap: list[float] = [0.0]

        try:
            while not _shutdown.requested:
                if not run_worker_once(
                    pq,
                    max_runtime=max_runtime,
                    priorities=priorities,
                    pre_execute=pre_execute,
                    post_execute=post_execute,
                ):
                    if _shutdown.requested:
                        break
                    _maybe_run_cleanup(
                        pq, retention_days, cleanup_interval, last_cleanup
                    )
                    _maybe_reap_stale(
                        pq, stale_task_timeout, DEFAULT_REAPER_INTERVAL, last_reap
                    )
                    _interruptible_sleep(poll_interval)
        except KeyboardInterrupt:
            # Fallback for non-main-thread workers (no handlers installed).
            pass
        logger.info("Worker stopped.")
    finally:
        _restore_shutdown_handlers(previous_handlers)
        # Clear the drain state: a lingering past-deadline flag would make
        # later run_worker_once calls in this process kill their children
        # on the first _wait_for_child iteration.
        _shutdown.reset(drain_timeout)


def run_worker_once(
    pq: PQ,
    *,
    max_runtime: float = DEFAULT_MAX_RUNTIME,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> bool:
    """Process a single task if available.

    Checks one-off tasks first, then periodic tasks.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
        priorities: If set, only process tasks with these priority levels.
        pre_execute: Called in forked child BEFORE task execution.
            Use for initializing fork-unsafe resources (OTel, DB connections).
        post_execute: Called in forked child AFTER task execution (success or failure).
            Use for cleanup/flushing (OTel traces, etc.).

    Returns:
        True if a task was processed, False if queue was empty.
    """
    # Try one-off task first
    if _process_one_off_task(
        pq,
        max_runtime=max_runtime,
        priorities=priorities,
        pre_execute=pre_execute,
        post_execute=post_execute,
    ):
        return True

    # Try periodic task
    if _process_periodic_task(
        pq,
        max_runtime=max_runtime,
        priorities=priorities,
        pre_execute=pre_execute,
        post_execute=post_execute,
    ):
        return True

    return False


def _process_one_off_task(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> bool:
    """Claim and process a one-off task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.
        priorities: If set, only process tasks with these priority levels.
        pre_execute: Called in forked child BEFORE task execution.
        post_execute: Called in forked child AFTER task execution.

    Returns:
        True if a task was processed.
    """
    # Phase 1: Claim task
    task: Task | None = None
    try:
        with pq.session() as session:
            # Claim highest priority pending task with FOR UPDATE SKIP LOCKED
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.PENDING)
                .where(Task.run_at <= func.now())
            )
            if priorities:
                stmt = stmt.where(Task.priority.in_([p.value for p in priorities]))
            stmt = (
                stmt.order_by(Task.priority.desc(), Task.run_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            task = session.execute(stmt).scalar_one_or_none()

            if task is None:
                return False

            # Mark as running
            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(UTC)
            task.attempts += 1

            # Get task data for execution (before session closes)
            name = task.name
            payload = task.payload
            task_id = task.id
            # Per-task ``max_runtime_seconds`` (NULL → use worker default).
            # Read while still in the session because the row is expunged
            # below for the forked-child handoff.
            task_max_runtime = task.max_runtime_seconds

            # Flush to commit status changes, then expunge for forked process
            session.flush()
            session.expunge(task)

    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return False

    # Effective per-task wall-clock ceiling: per-task value when set,
    # otherwise the worker's configured default. The reaper applies the
    # same rule on its own (see ``Client.reap_stale_tasks``), so the
    # two stay in sync without the worker having to coordinate.
    effective_max_runtime = (
        task_max_runtime if task_max_runtime is not None else max_runtime
    )

    # Phase 2: Execute handler in forked process
    start = time.perf_counter()
    status = TaskStatus.COMPLETED
    error_msg: str | None = None

    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        _execute_in_fork(
            handler,
            args,
            kwargs,
            max_runtime=effective_max_runtime,
            task=task,
            pre_execute=pre_execute,
            post_execute=post_execute,
        )

    except TaskTimeoutError:
        status = TaskStatus.FAILED
        error_msg = f"Timed out after {time.perf_counter() - start:.3f} s"

    except TaskOOMError as e:
        status = TaskStatus.FAILED
        error_msg = str(e)

    except TaskKilledError as e:
        status = TaskStatus.FAILED
        error_msg = str(e)

    except TaskInterruptedError as e:
        status = TaskStatus.FAILED
        error_msg = str(e)

    except Exception as e:
        status = TaskStatus.FAILED
        error_msg = str(e)

    elapsed = time.perf_counter() - start

    # Phase 3: Update task status (guarded — only if still RUNNING, so we
    # don't overwrite a reaper's FAILED verdict for an orphaned task)
    try:
        with pq.session() as session:
            values: dict[str, object] = {
                "status": status,
                "completed_at": datetime.now(UTC),
            }
            if error_msg:
                values["error"] = error_msg
            result = session.execute(
                update(Task)
                .where(Task.id == task_id, Task.status == TaskStatus.RUNNING)
                .values(**values)
            )
            if result.rowcount == 0:
                logger.warning(
                    f"Task '{name}' (id={task_id}) was no longer RUNNING"
                    " when Phase 3 tried to update — likely reaped"
                )
    except Exception as e:
        logger.error(f"Error updating task status: {e}")

    # Log result
    if status == TaskStatus.COMPLETED:
        logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")
    else:
        logger.error(f"Task '{name}' failed after {elapsed:.3f} s: {error_msg}")

    return True


def _calculate_next_run_cron(cron_expr: str) -> datetime:
    """Calculate the next run time using a cron expression.

    Args:
        cron_expr: Cron expression string.

    Returns:
        The next run datetime.
    """
    now = datetime.now(UTC)
    cron = croniter(cron_expr, now)
    return cron.get_next(datetime)


def _process_periodic_task(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> bool:
    """Claim and process a periodic task.

    Args:
        pq: PQ client instance.
        max_runtime: Maximum execution time in seconds.
        priorities: If set, only process tasks with these priority levels.
        pre_execute: Called in forked child BEFORE task execution.
        post_execute: Called in forked child AFTER task execution.

    Returns:
        True if a task was processed.
    """
    # Phase 1: Claim and advance schedule
    periodic: Periodic | None = None
    try:
        with pq.session() as session:
            # Claim highest priority due periodic task with FOR UPDATE SKIP LOCKED
            # Filter out tasks that are locked (max_concurrent=1 and locked_until in future)
            stmt = select(Periodic).where(
                Periodic.active.is_(True),
                Periodic.next_run <= func.now(),
                or_(
                    Periodic.max_concurrent.is_(None),
                    Periodic.locked_until.is_(None),
                    Periodic.locked_until <= func.now(),
                ),
            )
            if priorities:
                stmt = stmt.where(Periodic.priority.in_([p.value for p in priorities]))
            stmt = (
                stmt.order_by(Periodic.priority.desc(), Periodic.next_run)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            periodic = session.execute(stmt).scalar_one_or_none()

            if periodic is None:
                return False

            # Get task data before expunge
            name = periodic.name
            payload = periodic.payload
            periodic_id = periodic.id
            periodic_max_concurrent = periodic.max_concurrent
            # Per-schedule ``max_runtime_seconds`` (NULL → use worker default).
            # Read while still in the session because the row is expunged
            # below for the forked-child handoff.
            periodic_max_runtime = periodic.max_runtime_seconds

            # Effective per-execution wall-clock ceiling: per-schedule value
            # when set, otherwise the worker's configured default. Drives
            # both the in-child timeout enforcement AND the
            # ``locked_until`` window so the lock doesn't expire while a
            # legitimately-long execution is still running.
            effective_max_runtime = (
                periodic_max_runtime
                if periodic_max_runtime is not None
                else max_runtime
            )

            # Set lock before execution if concurrency is limited
            if periodic.max_concurrent is not None:
                lock_duration = (
                    effective_max_runtime if effective_max_runtime > 0 else 3600
                )
                periodic.locked_until = func.now() + timedelta(seconds=lock_duration)

            # Advance schedule BEFORE execution
            periodic.last_run = func.now()
            if periodic.cron:
                periodic.next_run = _calculate_next_run_cron(periodic.cron)
            else:
                periodic.next_run = func.now() + periodic.run_every

            # Flush to commit schedule changes, then expunge for forked process
            session.flush()
            session.expunge(periodic)

    except Exception as e:
        logger.error(f"Error claiming periodic task: {e}")
        return False

    # Phase 2: Execute handler in forked process
    start = time.perf_counter()
    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        _execute_in_fork(
            handler,
            args,
            kwargs,
            max_runtime=effective_max_runtime,
            task=periodic,
            pre_execute=pre_execute,
            post_execute=post_execute,
        )
        elapsed = time.perf_counter() - start
        logger.debug(f"Periodic task '{name}' completed in {elapsed:.3f} s")

    except TaskTimeoutError:
        elapsed = time.perf_counter() - start
        logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")

    except TaskOOMError as e:
        elapsed = time.perf_counter() - start
        logger.error(f"Periodic task '{name}' OOM after {elapsed:.3f} s: {e}")

    except TaskKilledError as e:
        elapsed = time.perf_counter() - start
        logger.error(f"Periodic task '{name}' killed after {elapsed:.3f} s: {e}")

    except TaskInterruptedError as e:
        elapsed = time.perf_counter() - start
        logger.warning(f"Periodic task '{name}' interrupted after {elapsed:.3f} s: {e}")

    except Exception as e:
        elapsed = time.perf_counter() - start
        logger.error(f"Periodic task '{name}' failed after {elapsed:.3f} s: {e}")

    finally:
        # Clear lock after execution (success or failure)
        if periodic_max_concurrent is not None:
            try:
                with pq.session() as session:
                    session.execute(
                        update(Periodic)
                        .where(Periodic.id == periodic_id)
                        .values(locked_until=None)
                    )
            except Exception as e:
                logger.error(f"Failed to clear lock for periodic task '{name}': {e}")

    return True


# --- Concurrent worker functions ---


def _claim_and_fork_one_off(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> _ChildSlot | None:
    """Claim a one-off task and fork a child to execute it.

    Returns:
        _ChildSlot if a task was claimed and forked, None if no tasks available.
    """
    try:
        with pq.session() as session:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.PENDING)
                .where(Task.run_at <= func.now())
            )
            if priorities:
                stmt = stmt.where(Task.priority.in_([p.value for p in priorities]))
            stmt = (
                stmt.order_by(Task.priority.desc(), Task.run_at)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            task = session.execute(stmt).scalar_one_or_none()

            if task is None:
                return None

            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now(UTC)
            task.attempts += 1

            name = task.name
            payload = task.payload
            task_id = task.id
            # Per-task ``max_runtime_seconds`` (NULL → use worker default).
            # Same handling as the sequential ``_process_one_off_task`` path.
            task_max_runtime = task.max_runtime_seconds

            session.flush()
            session.expunge(task)

    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return None

    effective_max_runtime = (
        task_max_runtime if task_max_runtime is not None else max_runtime
    )

    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        child_pid, read_fd = _fork_child(
            handler,
            args,
            kwargs,
            max_runtime=effective_max_runtime,
            task=task,
            pre_execute=pre_execute,
            post_execute=post_execute,
        )
    except Exception as e:
        logger.error(f"Error starting task '{name}': {e}")
        try:
            with pq.session() as session:
                t = session.get(Task, task_id)
                if t:
                    t.status = TaskStatus.FAILED
                    t.completed_at = datetime.now(UTC)
                    t.error = str(e)
        except Exception as update_err:
            logger.error(f"Error updating task status: {update_err}")
        return None

    return _ChildSlot(
        pid=child_pid,
        read_fd=read_fd,
        task_id=task_id,
        name=name,
        start_time=time.perf_counter(),
        is_periodic=False,
    )


def _claim_and_fork_periodic(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> _ChildSlot | None:
    """Claim a periodic task and fork a child to execute it.

    Returns:
        _ChildSlot if a task was claimed and forked, None if no tasks available.
    """
    try:
        with pq.session() as session:
            stmt = select(Periodic).where(
                Periodic.active.is_(True),
                Periodic.next_run <= func.now(),
                or_(
                    Periodic.max_concurrent.is_(None),
                    Periodic.locked_until.is_(None),
                    Periodic.locked_until <= func.now(),
                ),
            )
            if priorities:
                stmt = stmt.where(Periodic.priority.in_([p.value for p in priorities]))
            stmt = (
                stmt.order_by(Periodic.priority.desc(), Periodic.next_run)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            periodic = session.execute(stmt).scalar_one_or_none()

            if periodic is None:
                return None

            name = periodic.name
            payload = periodic.payload
            periodic_id = periodic.id
            periodic_max_concurrent = periodic.max_concurrent
            # Per-schedule ``max_runtime_seconds`` (NULL → use worker default).
            # Same handling as the sequential ``_process_periodic_task`` path.
            periodic_max_runtime = periodic.max_runtime_seconds

            effective_max_runtime = (
                periodic_max_runtime
                if periodic_max_runtime is not None
                else max_runtime
            )

            if periodic.max_concurrent is not None:
                lock_duration = (
                    effective_max_runtime if effective_max_runtime > 0 else 3600
                )
                periodic.locked_until = func.now() + timedelta(seconds=lock_duration)

            periodic.last_run = func.now()
            if periodic.cron:
                periodic.next_run = _calculate_next_run_cron(periodic.cron)
            else:
                periodic.next_run = func.now() + periodic.run_every

            session.flush()
            session.expunge(periodic)

    except Exception as e:
        logger.error(f"Error claiming periodic task: {e}")
        return None

    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        child_pid, read_fd = _fork_child(
            handler,
            args,
            kwargs,
            max_runtime=effective_max_runtime,
            task=periodic,
            pre_execute=pre_execute,
            post_execute=post_execute,
        )
    except Exception as e:
        logger.error(f"Error starting periodic task '{name}': {e}")
        if periodic_max_concurrent is not None:
            try:
                with pq.session() as session:
                    session.execute(
                        update(Periodic)
                        .where(Periodic.id == periodic_id)
                        .values(locked_until=None)
                    )
            except Exception as lock_err:
                logger.error(
                    f"Failed to clear lock for periodic task '{name}': {lock_err}"
                )
        return None

    return _ChildSlot(
        pid=child_pid,
        read_fd=read_fd,
        task_id=periodic_id,
        name=name,
        start_time=time.perf_counter(),
        is_periodic=True,
        periodic_max_concurrent=periodic_max_concurrent,
    )


def _try_claim_and_fork(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: Set[Priority] | None = None,
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> _ChildSlot | None:
    """Try to claim any available task (one-off first, then periodic) and fork.

    Returns:
        _ChildSlot if a task was claimed and forked, None if no tasks available.
    """
    slot = _claim_and_fork_one_off(
        pq,
        max_runtime=max_runtime,
        priorities=priorities,
        pre_execute=pre_execute,
        post_execute=post_execute,
    )
    if slot is not None:
        return slot

    return _claim_and_fork_periodic(
        pq,
        max_runtime=max_runtime,
        priorities=priorities,
        pre_execute=pre_execute,
        post_execute=post_execute,
    )


def _reap_and_update(pq: PQ, slot: _ChildSlot) -> None:
    """Wait for a forked child to finish and update task status.

    Args:
        pq: PQ client instance.
        slot: The child slot to reap.
    """
    result = _wait_for_child(slot.pid, slot.read_fd)
    elapsed = time.perf_counter() - slot.start_time

    if slot.is_periodic:
        if result.exit_kind == "success":
            logger.debug(f"Periodic task '{slot.name}' completed in {elapsed:.3f} s")
        elif result.exit_kind == "timeout":
            logger.error(f"Periodic task '{slot.name}' timed out after {elapsed:.3f} s")
        elif result.exit_kind == "oom":
            logger.error(
                f"Periodic task '{slot.name}' OOM after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        elif result.exit_kind == "killed":
            logger.error(
                f"Periodic task '{slot.name}' killed after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        elif result.exit_kind == "shutdown":
            logger.warning(
                f"Periodic task '{slot.name}' interrupted after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        else:
            logger.error(
                f"Periodic task '{slot.name}' failed after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )

        if slot.periodic_max_concurrent is not None:
            try:
                with pq.session() as session:
                    session.execute(
                        update(Periodic)
                        .where(Periodic.id == slot.task_id)
                        .values(locked_until=None)
                    )
            except Exception as e:
                logger.error(
                    f"Failed to clear lock for periodic task '{slot.name}': {e}"
                )
    else:
        error_msg = result.error_msg
        if result.exit_kind == "timeout":
            error_msg = f"Timed out after {elapsed:.3f} s"

        try:
            with pq.session() as session:
                values: dict[str, object] = {
                    "status": result.task_status,
                    "completed_at": datetime.now(UTC),
                }
                if error_msg:
                    values["error"] = error_msg
                row_result = session.execute(
                    update(Task)
                    .where(
                        Task.id == slot.task_id,
                        Task.status == TaskStatus.RUNNING,
                    )
                    .values(**values)
                )
                if row_result.rowcount == 0:
                    logger.warning(
                        f"Task '{slot.name}' (id={slot.task_id}) was no longer"
                        " RUNNING when reap tried to update — likely reaped"
                    )
        except Exception as e:
            logger.error(f"Error updating task status: {e}")

        if result.task_status == TaskStatus.COMPLETED:
            logger.debug(f"Task '{slot.name}' completed in {elapsed:.3f} s")
        else:
            logger.error(
                f"Task '{slot.name}' failed after {elapsed:.3f} s: {error_msg}"
            )


def _run_concurrent(
    pq: PQ,
    *,
    concurrency: int,
    poll_interval: float,
    max_runtime: float,
    priorities: Set[Priority] | None,
    pre_execute: PreExecuteHook | None,
    post_execute: PostExecuteHook | None,
    retention_days: int,
    cleanup_interval: float,
    stale_task_timeout: timedelta | None = None,
) -> None:
    """Run the concurrent worker loop.

    Manages up to ``concurrency`` forked children simultaneously using
    select() on error pipes to detect child completion.

    Args:
        pq: PQ client instance.
        concurrency: Maximum number of concurrent tasks.
        poll_interval: Seconds between polls when idle.
        max_runtime: Maximum execution time per task in seconds.
        priorities: If set, only process tasks with these priority levels.
        pre_execute: Called in forked child BEFORE task execution.
        post_execute: Called in forked child AFTER task execution.
        retention_days: Days to keep completed/failed tasks.
        cleanup_interval: Seconds between cleanup runs.
        stale_task_timeout: If set, reap RUNNING tasks older than this.
    """
    children: dict[int, _ChildSlot] = {}  # pid -> slot
    fd_to_pid: dict[int, int] = {}  # read_fd -> pid
    last_cleanup: list[float] = [0.0]
    last_reap: list[float] = [0.0]

    try:
        while not _shutdown.requested:
            # Step 1: Fill empty slots with new tasks
            while len(children) < concurrency and not _shutdown.requested:
                slot = _try_claim_and_fork(
                    pq,
                    max_runtime=max_runtime,
                    priorities=priorities,
                    pre_execute=pre_execute,
                    post_execute=post_execute,
                )
                if slot is None:
                    break
                children[slot.pid] = slot
                fd_to_pid[slot.read_fd] = slot.pid

            # Step 2: Wait for events
            if children:
                read_fds = list(fd_to_pid.keys())
                ready, _, _ = select_module.select(read_fds, [], [], poll_interval)
                for fd in ready:
                    pid = fd_to_pid.pop(fd)
                    slot = children.pop(pid)
                    _reap_and_update(pq, slot)
            else:
                _interruptible_sleep(poll_interval)

            if _shutdown.requested:
                break

            # Maintenance (each rate-limited independently)
            _maybe_run_cleanup(pq, retention_days, cleanup_interval, last_cleanup)
            _maybe_reap_stale(
                pq, stale_task_timeout, DEFAULT_REAPER_INTERVAL, last_reap
            )

    except KeyboardInterrupt:
        # Fallback for non-main-thread workers (no handlers installed); the
        # drain below still runs, just without a deadline.
        pass

    if children:
        logger.info(f"Shutting down, draining {len(children)} in-flight task(s)...")
        # _wait_for_child inside _reap_and_update enforces the drain
        # deadline (absolute, shared by all children): each child either
        # finishes in time and gets its real status, or is SIGKILLed and
        # marked FAILED with the shutdown error.
        for slot in list(children.values()):
            _reap_and_update(pq, slot)
    logger.info("Worker stopped.")
