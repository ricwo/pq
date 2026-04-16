"""Worker loop for processing tasks with fork isolation.

Each task runs in a forked child process for memory isolation.
If a task OOMs or crashes, only the child is affected - the worker continues.
"""

from __future__ import annotations

import asyncio
import inspect
import os
import select as select_module
import signal
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, NamedTuple, Protocol

from croniter import croniter
from loguru import logger
from sqlalchemy import func, or_, select, update

from pq.models import Periodic, Task, TaskStatus
from pq.registry import resolve_function_path
from pq.serialization import deserialize

if TYPE_CHECKING:
    from collections.abc import Callable, Set

    from pq.client import PQ
    from pq.priority import Priority


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

    Args:
        child_pid: PID of the child process.
        read_fd: Read end of the error pipe.

    Returns:
        _ChildResult with task status, error message, and exit kind.
    """
    _, raw_status, rusage = os.wait4(child_pid, 0)

    # Read error message from pipe
    error_bytes = b""
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
    raise Exception(result.error_msg or "Task failed")


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
    pre_execute: PreExecuteHook | None = None,
    post_execute: PostExecuteHook | None = None,
) -> None:
    """Run the worker loop indefinitely.

    Each task executes in a forked child process for memory isolation.

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
        pre_execute: Called in forked child BEFORE task execution.
            Use for initializing fork-unsafe resources (OTel, DB connections).
        post_execute: Called in forked child AFTER task execution (success or failure).
            Use for cleanup/flushing (OTel traces, etc.).
    """
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
        while True:
            if not run_worker_once(
                pq,
                max_runtime=max_runtime,
                priorities=priorities,
                pre_execute=pre_execute,
                post_execute=post_execute,
            ):
                _maybe_run_cleanup(pq, retention_days, cleanup_interval, last_cleanup)
                _maybe_reap_stale(
                    pq, stale_task_timeout, DEFAULT_REAPER_INTERVAL, last_reap
                )
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Worker stopped.")


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

            # Flush to commit status changes, then expunge for forked process
            session.flush()
            session.expunge(task)

    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return False

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
            max_runtime=max_runtime,
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

            # Set lock before execution if concurrency is limited
            if periodic.max_concurrent is not None:
                lock_duration = max_runtime if max_runtime > 0 else 3600
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
            max_runtime=max_runtime,
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

            session.flush()
            session.expunge(task)

    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return None

    try:
        handler = resolve_function_path(name)
        args, kwargs = deserialize(payload)
        child_pid, read_fd = _fork_child(
            handler,
            args,
            kwargs,
            max_runtime=max_runtime,
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

            if periodic.max_concurrent is not None:
                lock_duration = max_runtime if max_runtime > 0 else 3600
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
            max_runtime=max_runtime,
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
        while True:
            # Step 1: Fill empty slots with new tasks
            while len(children) < concurrency:
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
                time.sleep(poll_interval)

            # Maintenance (each rate-limited independently)
            _maybe_run_cleanup(pq, retention_days, cleanup_interval, last_cleanup)
            _maybe_reap_stale(
                pq, stale_task_timeout, DEFAULT_REAPER_INTERVAL, last_reap
            )

    except KeyboardInterrupt:
        if children:
            logger.info(
                f"Shutting down, waiting for {len(children)} task(s) to finish..."
            )
            for slot in list(children.values()):
                _reap_and_update(pq, slot)
        logger.info("Worker stopped.")
