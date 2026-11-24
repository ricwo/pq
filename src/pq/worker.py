"""Worker loop for processing tasks with fork isolation.

Each task runs in a forked child process for memory isolation.
If a task OOMs or crashes, only the child is affected - the worker continues.
"""

from __future__ import annotations

import asyncio
import gc
import inspect
import math
import os
import select as select_module
import signal
import sys
import threading
import time
import traceback
from collections.abc import Callable, Coroutine, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from types import FrameType
from typing import TYPE_CHECKING, Any, NamedTuple, NoReturn, Protocol
from typing import cast as typing_cast

from croniter import croniter
from loguru import logger
from sqlalchemy import (
    ColumnElement,
    Row,
    Select,
    Table,
    Update,
    and_,
    bindparam,
    case,
    cast,
    func,
    or_,
    select,
    update,
)

from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError

from pq.client import _close_pools_before_exit
from pq.models import Periodic, Task, TaskStatus, requeue_values
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

# How long past its max runtime a task child may live before the worker
# SIGKILLs its process group. The child stops itself at its max runtime
# (SIGALRM, or the deadline of an async task) and may then still run the
# post-execute hook. The worker's kill is for a child that cannot do that: one
# blocked in C code that never returns to the interpreter, or one whose task
# replaced or masked SIGALRM.
KILL_GRACE_SECONDS: float = 10.0

# A periodic lock is held this much longer than the child can live, so that
# the worker that set it is the one to release it.
_LOCK_MARGIN_SECONDS: float = 5.0

# Upper bound on how long the loop sleeps while it has children. A child that
# exits wakes the loop at once (its pipe closes); this bounds the wait for one
# whose pipe is held open by a process it left behind.
_CHILD_POLL_SECONDS: float = 1.0

# Pauses before the retries of a failed status write at the end of a run.
# The loop waits through them, so they are short: long enough for a
# connection that was reset to be replaced.
_FINISH_RETRY_DELAYS: tuple[float, ...] = (0.2, 1.0)
_NOT_RETRIED = (DataError, IntegrityError, ProgrammingError)

# Cap on the traceback a child reports, in characters. The exception line
# that precedes it is capped at an eighth of this.
_MAX_ERROR_CHARS: int = 64_000

# Exit codes for child process
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_TIMEOUT = 124  # Like GNU timeout


class _ChildResult(NamedTuple):
    """How a forked child ended."""

    task_status: TaskStatus
    error_msg: str | None
    exit_kind: str  # "success", "timeout", "oom", "killed", "shutdown", "error"


@dataclass(frozen=True)
class _Claim:
    """A task this worker has claimed and must run and record."""

    row: Task | Periodic
    max_runtime: float  # the row's own cap, or the worker default
    # One-off: ``started_at`` written by the claim. Identifies this run on
    # the row (see ``_finish_one_off``).
    started_at: datetime | None = None
    # Periodic: the ``locked_until`` this claim set, if the schedule limits
    # concurrency. The lock is released only while it still holds this value.
    locked_until: datetime | None = None

    @property
    def is_periodic(self) -> bool:
        return isinstance(self.row, Periodic)


@dataclass
class _Child:
    """A running forked child."""

    pid: int
    claim: _Claim
    started: float  # time.monotonic() at the fork
    kill_at: float  # time.monotonic() after which the worker kills it
    read_fd: int | None  # read end of the error pipe; None once closed
    output: bytearray = field(default_factory=bytearray)
    killed: str | None = None  # why the worker killed it: "timeout", "shutdown"
    # Once the pipe is gone, how long to sleep before the next look for the
    # child's exit. Doubles each time. A child closes its pipe as it exits,
    # so the first look, moments later, usually finds it gone.
    exit_poll: float = 0.00005


class WorkerError(Exception):
    """Base class for worker errors."""


class TaskTimeoutError(WorkerError):
    """A task exceeded its max runtime."""


class TaskOOMError(WorkerError):
    """A task was killed, most likely by the OOM killer."""


class TaskKilledError(WorkerError):
    """A task was killed by a signal."""


class TaskInterruptedError(WorkerError):
    """A task was killed because the worker was shutting down."""


class _MaxRuntimeExceeded(Exception):
    """The worker's deadline for an async task passed."""


# The tables, for statements built without the ORM.
_TASKS = typing_cast(Table, Task.__table__)
_PERIODIC = typing_cast(Table, Periodic.__table__)


@dataclass
class _ShutdownState:
    """Graceful-shutdown coordination for the worker loops.

    Written by the signal handler (which runs in the main thread between
    bytecodes, so no locking is needed) and read by the worker loop and
    ``_poll_child``. Module-level because a worker owns its process;
    ``run_worker`` resets it on entry.
    """

    signum: int | None = None  # signal that requested the shutdown
    deadline: float | None = None  # time.monotonic() deadline, set on signal
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    logged: bool = False  # log_request() already ran for this shutdown
    # A pipe the signal handler writes to, so that a worker loop asleep in
    # ``_wait`` wakes up at once. Set while handlers are installed.
    wake_r: int | None = None
    wake_w: int | None = None

    @property
    def requested(self) -> bool:
        return self.signum is not None

    def reset(self, drain_timeout: float) -> None:
        self.signum = None
        self.deadline = None
        self.drain_timeout = drain_timeout
        self.logged = False

    def deadline_passed(self) -> bool:
        """True once shutdown was requested and the drain budget is spent."""
        if not self.requested:
            return False
        return self.deadline is None or time.monotonic() >= self.deadline

    def log_request(self) -> None:
        """Log the shutdown request once, from the worker loop.

        The signal handler can't log it itself (see
        ``_handle_shutdown_signal``), so the loops call this when they see
        ``requested``.
        """
        if not self.requested or self.logged:
            return
        self.logged = True
        logger.info(
            f"Received signal {self.signum}, draining"
            f" (drain_timeout={self.drain_timeout}s)..."
        )


_shutdown = _ShutdownState()


def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    """SIGTERM/SIGINT handler: request a drain, never raise, never log.

    Raising from here would land at arbitrary bytecode and could orphan a
    task (e.g. between claim-commit and child registration). Logging from
    here can deadlock: the handler runs on the main thread wherever it was
    interrupted, and loguru holds its handler locks for the whole of
    ``os.fork()``. The worker loops log the request instead
    (``_ShutdownState.log_request``). Repeat signals during the drain are
    ignored so they can't abandon remaining children.
    """
    if _shutdown.requested:
        return
    _shutdown.deadline = time.monotonic() + _shutdown.drain_timeout
    _shutdown.signum = signum
    if _shutdown.wake_w is not None:
        try:
            os.write(_shutdown.wake_w, b"\0")
        except OSError:
            pass  # the loop still notices at its next wake-up


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
    _shutdown.wake_r, _shutdown.wake_w = os.pipe()
    os.set_blocking(_shutdown.wake_r, False)
    os.set_blocking(_shutdown.wake_w, False)
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
    for fd in (_shutdown.wake_r, _shutdown.wake_w):
        if fd is not None:
            os.close(fd)
    _shutdown.wake_r = _shutdown.wake_w = None


def _kill_child_group(child_pid: int, *, reaped: bool = False) -> None:
    """SIGKILL a task child's process group (best effort).

    The child calls ``os.setpgrp()`` first thing, so its pgid equals its
    pid. If the signal lands before ``setpgrp()`` the process group doesn't
    exist yet — fall back to killing the pid directly. ESRCH everywhere
    means the child already exited; the caller reaps it normally.

    With ``reaped``, the child itself is gone and only what it left in its
    group is killed. No fallback to the pid then: the number may belong to
    another process by now.
    """
    try:
        os.killpg(child_pid, signal.SIGKILL)
    except ProcessLookupError:
        if reaped:
            return
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    except PermissionError:
        pass  # what is left in the group runs as another user


# --- In the child ---


def _child_timeout_handler(signum: int, frame: FrameType | None) -> None:
    """Signal handler for timeout in child process."""
    os._exit(EXIT_TIMEOUT)


def _flush_stdio() -> None:
    """Write out what ``print`` and friends have buffered."""
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass  # closed, or None when the process has no such stream


async def _run_with_deadline(
    coro: Coroutine[Any, Any, object], max_runtime: float
) -> None:
    """Await ``coro``, raising ``_MaxRuntimeExceeded`` if it takes too long.

    A ``TimeoutError`` that the task raises itself (its own ``wait_for``, a
    socket timeout) passes through as an ordinary failure.
    """
    try:
        async with asyncio.timeout(max_runtime) as deadline:
            await coro
    except TimeoutError:
        if deadline.expired():
            raise _MaxRuntimeExceeded from None
        raise


def _clip(text: str, limit: int) -> str:
    """Cut the middle out of a text longer than ``limit`` characters."""
    if len(text) <= limit:
        return text
    half = limit // 2
    return f"{text[:half]}\n[... truncated ...]\n{text[-half:]}"


def _error_report(summary: str) -> str:
    """The error text the worker stores for a failed task: the exception
    being handled as one line, then its traceback.

    Both are capped: an exception can carry megabytes (a response body, a
    rendered document), and the text is stored on the task and logged.
    """
    return (
        f"{_clip(summary, _MAX_ERROR_CHARS // 8)}\n"
        f"{_clip(traceback.format_exc(), _MAX_ERROR_CHARS)}"
    )


def _write_error(fd: int, report: str) -> None:
    """Send the error text to the worker through the pipe (best effort)."""
    data = report.encode("utf-8", errors="replace")
    try:
        while data:
            data = data[os.write(fd, data) :]
    except OSError:
        pass


def _execute_task(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    max_runtime: float,
    error_write_fd: int,
    task: Task | Periodic,
    pre_execute: PreExecuteHook | None,
    post_execute: PostExecuteHook | None,
) -> int:
    """Run the hooks and the handler in the child. Returns its exit code."""
    # Create new process group so we don't get parent's signals
    os.setpgrp()

    # Restore default signal disposition: the fork inherits the parent's
    # drain handler, which in the child would silently swallow a direct
    # SIGTERM/SIGINT (e.g. from systemd's cgroup-wide kill).
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    # Set up timeout. +1 gives an async task's own deadline the first go.
    signal.signal(signal.SIGALRM, _child_timeout_handler)
    signal.alarm(min(int(max_runtime) + 1, 2**31 - 1))

    error: Exception | None = None
    report = ""
    exit_code = EXIT_SUCCESS

    try:
        if pre_execute:
            pre_execute(task)

        if inspect.iscoroutinefunction(handler):
            asyncio.run(_run_with_deadline(handler(*args, **kwargs), max_runtime))
        else:
            handler(*args, **kwargs)

    except _MaxRuntimeExceeded:
        error = TimeoutError(f"Task exceeded max runtime of {max_runtime} s")
        exit_code = EXIT_TIMEOUT

    except SystemExit as e:
        # ``sys.exit()`` ends the task the way it would end a script: a
        # success for 0 or None, a failure otherwise.
        if e.code not in (0, None):
            error = RuntimeError(f"Task called sys.exit({e.code!r})")
            report = _error_report(f"SystemExit: {e.code}")
            exit_code = EXIT_FAILURE

    except Exception as e:
        error = e
        report = _error_report(f"{type(e).__name__}: {e}")
        exit_code = EXIT_FAILURE

    except BaseException as e:  # KeyboardInterrupt, asyncio.CancelledError
        error = RuntimeError(f"Task raised {type(e).__name__}: {e}")
        report = _error_report(f"{type(e).__name__}: {e}")
        exit_code = EXIT_FAILURE

    try:
        if post_execute:
            post_execute(task, error)
    except BaseException:
        pass  # Don't let hook errors mask task errors

    if report:
        _write_error(error_write_fd, report)

    # Hang up cleanly on any connection the task opened through pq. Not done
    # in _child_timeout_handler: a signal handler can interrupt pool code
    # that holds the pool's lock.
    _close_pools_before_exit()
    return exit_code


def _run_in_child(
    handler: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    max_runtime: float,
    error_write_fd: int,
    task: Task | Periodic,
    pre_execute: PreExecuteHook | None,
    post_execute: PostExecuteHook | None,
) -> NoReturn:
    """Execute handler in child process.

    Never returns and never raises: the child always ends in ``os._exit()``.
    An exception that left this function would unwind into the worker loop
    that the child inherited from its parent, and the child would go on to
    run the worker's own code (its ``finally`` blocks, the code after
    ``run_worker``, the interpreter's exit handlers).
    """
    try:
        exit_code = _execute_task(
            handler,
            args,
            kwargs,
            max_runtime,
            error_write_fd,
            task,
            pre_execute,
            post_execute,
        )
    except BaseException:
        exit_code = EXIT_FAILURE
    # os._exit() skips the flush that a normal exit does.
    _flush_stdio()
    os._exit(exit_code)


# --- In the worker: one child ---


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
    # The child gets a copy of what the worker has buffered, and would
    # write it a second time.
    _flush_stdio()
    try:
        child_pid = os.fork()
    except BaseException:
        os.close(read_fd)
        os.close(write_fd)
        raise

    if child_pid == 0:
        # === CHILD PROCESS ===
        # A garbage collection writes to the header of every object it
        # looks at, which copies the memory page the object is on: one full
        # collection in a child copies about half of the heap it shares
        # with the worker. Frozen objects are skipped, so the pages stay
        # shared. Done here and not before the fork: objects that the
        # worker freezes and unfreezes around each fork are never collected
        # in the worker again.
        gc.freeze()
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

    # === PARENT PROCESS ===
    os.close(write_fd)
    return child_pid, read_fd


def _close_output(child: _Child) -> None:
    if child.read_fd is not None:
        os.close(child.read_fd)
        child.read_fd = None


def _read_output(child: _Child) -> None:
    """Take what the child has written to its error pipe, without blocking."""
    if child.read_fd is None:
        return
    try:
        while True:
            chunk = os.read(child.read_fd, 65536)
            if not chunk:
                # End of file: every writer is gone. Stop watching the pipe,
                # which would otherwise report as readable forever.
                _close_output(child)
                return
            if len(child.output) < 8 * _MAX_ERROR_CHARS:
                child.output += chunk
    except BlockingIOError:
        pass
    except OSError:
        _close_output(child)


def _poll_child(child: _Child) -> _ChildResult | None:
    """Check on a child without blocking. Returns its result once it exited.

    Also enforces the two deadlines a child has: its max runtime plus
    ``KILL_GRACE_SECONDS``, and the worker's drain deadline. Past either,
    the child's process group is SIGKILLed.
    """
    _read_output(child)
    try:
        pid, raw_status, rusage = os.wait4(child.pid, os.WNOHANG)
    except ChildProcessError:
        # Something else in this process waited for the child (a SIGCHLD
        # handler, or SIGCHLD set to be ignored).
        _close_output(child)
        return _ChildResult(
            TaskStatus.FAILED,
            "Task result unknown: its process was reaped outside pq",
            "error",
        )

    if pid == 0:
        if child.killed is None:
            if _shutdown.deadline_passed():
                child.killed = "shutdown"
            elif time.monotonic() >= child.kill_at:
                child.killed = "timeout"
            if child.killed is not None:
                _kill_child_group(child.pid)
        if child.read_fd is None:
            child.exit_poll = min(child.exit_poll * 2, _CHILD_POLL_SECONDS)
        return None

    # The child wrote its error before it exited, so all of it is in the
    # pipe now. No wait for end of file: a process the task left behind can
    # hold the pipe open for as long as it lives.
    _read_output(child)
    _close_output(child)

    if os.WIFSIGNALED(raw_status):
        signal_num = os.WTERMSIG(raw_status)
        if signal_num == signal.SIGKILL and child.killed == "shutdown":
            return _ChildResult(
                TaskStatus.FAILED,
                "Task interrupted by worker shutdown"
                f" (exceeded drain_timeout={_shutdown.drain_timeout}s)",
                "shutdown",
            )
        if signal_num == signal.SIGKILL and child.killed == "timeout":
            return _ChildResult(
                TaskStatus.FAILED, "Task exceeded max runtime", "timeout"
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
            # The child stopped itself. What it started and left running is
            # past the deadline too.
            _kill_child_group(child.pid, reaped=True)
            return _ChildResult(
                TaskStatus.FAILED, "Task exceeded max runtime", "timeout"
            )
        error_msg = child.output.decode("utf-8", errors="replace").rstrip()
        if error_msg:
            return _ChildResult(TaskStatus.FAILED, error_msg, "error")
        return _ChildResult(
            TaskStatus.FAILED,
            f"Task failed with exit code {exit_code}",
            "error",
        )

    return _ChildResult(TaskStatus.FAILED, "Unknown child exit status", "error")


def _wait(children: Iterable[_Child], timeout: float) -> None:
    """Sleep until a child needs attention, a shutdown signal arrives, or
    ``timeout`` seconds pass.

    A child that exits closes its error pipe, which ends the sleep at once.
    The sleep is also cut to the next deadline the worker has to enforce.
    """
    now = time.monotonic()
    poller = select_module.poll()
    for child in children:
        if child.read_fd is not None:
            poller.register(child.read_fd, select_module.POLLIN)
        else:
            timeout = min(timeout, child.exit_poll)
        if child.killed is not None:
            timeout = min(timeout, 0.05)  # a killed child exits in a moment
        else:
            timeout = min(timeout, child.kill_at - now)
            if _shutdown.deadline is not None:
                timeout = min(timeout, _shutdown.deadline - now)
    if _shutdown.wake_r is not None:
        poller.register(_shutdown.wake_r, select_module.POLLIN)
    poller.poll(max(timeout, 0.0) * 1000)
    if _shutdown.wake_r is not None:
        try:
            os.read(_shutdown.wake_r, 64)
        except OSError:
            pass  # nothing written: the sleep ended for another reason


# --- In the worker: claiming ---


def _priority_values(priorities: Set[Priority] | None) -> tuple[int, ...]:
    """The priorities filter as a hashable key. Empty means no filter."""
    return tuple(sorted(int(p) for p in priorities)) if priorities else ()


def _due_periodic(priorities: tuple[int, ...]) -> list[ColumnElement[bool]]:
    """Conditions for a periodic schedule that is due and not locked."""
    periodic = _PERIODIC
    conditions: list[ColumnElement[bool]] = [
        periodic.c.active.is_(True),
        periodic.c.next_run <= func.now(),
        or_(
            periodic.c.max_concurrent.is_(None),
            periodic.c.locked_until.is_(None),
            periodic.c.locked_until <= func.now(),
        ),
    ]
    if priorities:
        conditions.append(periodic.c.priority.in_(priorities))
    return conditions


@lru_cache(maxsize=64)
def _claim_task_statement(
    priorities: tuple[int, ...], *, before_schedule: bool
) -> Update:
    """Claim the due pending task with the highest priority, in one statement.

    ``FOR UPDATE SKIP LOCKED`` makes concurrent workers pick different rows.
    ``started_at`` also identifies this run when its result is recorded (see
    ``_finish_one_off``).

    With ``before_schedule`` the task is claimed only if it goes before a
    due periodic schedule, given by the parameters ``schedule_priority`` and
    ``schedule_next_run``: it has a higher priority, or the same priority
    and came due no later. The check is on the claimed row and not in the
    subquery, where it would make the scan read on through the whole
    backlog when the first task does not pass. Every task after the first
    fails the check as well.
    """
    tasks = _TASKS
    due = select(tasks.c.id).where(
        tasks.c.status == TaskStatus.PENDING, tasks.c.run_at <= func.now()
    )
    if priorities:
        due = due.where(tasks.c.priority.in_(priorities))
    due = (
        due.order_by(tasks.c.priority.desc(), tasks.c.run_at)
        .limit(1)
        .with_for_update(skip_locked=True)
    )
    claim = update(tasks).where(tasks.c.id == due.scalar_subquery())
    if before_schedule:
        priority = bindparam("schedule_priority", type_=tasks.c.priority.type)
        next_run = bindparam("schedule_next_run", type_=tasks.c.run_at.type)
        claim = claim.where(
            or_(
                tasks.c.priority > priority,
                and_(tasks.c.priority == priority, tasks.c.run_at <= next_run),
            )
        )
    return claim.values(
        status=TaskStatus.RUNNING,
        started_at=func.now(),
        attempts=tasks.c.attempts + 1,
    ).returning(*tasks.c)


@lru_cache(maxsize=32)
def _due_periodic_statement(priorities: tuple[int, ...]) -> Select[Any]:
    """The due periodic schedules that are not locked, best first."""
    periodic = _PERIODIC
    return (
        select(periodic)
        .where(*_due_periodic(priorities))
        .order_by(periodic.c.priority.desc(), periodic.c.next_run)
        .limit(1)
    )


def _next_cron_run(cron_expr: str, database_now: datetime) -> datetime:
    """The first cron time after ``database_now``.

    By the database's clock, not the worker's: a schedule is due by the
    database's clock. On a worker whose clock is behind it, the first cron
    time after the worker's "now" can be one that is already due, which
    would then run again.
    """
    # In UTC: croniter reads the expression in the time zone of its base,
    # and the database returns times in the session's time zone.
    return croniter(cron_expr, database_now.astimezone(UTC)).get_next(datetime)


def _lock_seconds(max_runtime: float) -> float:
    """How long a periodic lock is held: as long as the child can be alive."""
    if max_runtime <= 0:
        return 3600.0
    return max_runtime + KILL_GRACE_SECONDS + _LOCK_MARGIN_SECONDS


def _claim_periodic(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: tuple[int, ...],
    only: int | None = None,
    skip: int | None = None,
) -> _Claim | None:
    """Claim a due periodic schedule: lock it and advance it to its next run.

    With ``only``, the schedule with that id or none: the caller has decided
    that it is the next thing to run. Otherwise the first due schedule that
    no other worker is claiming, leaving out the one with the id ``skip``.

    The schedule is advanced before the task runs, so a run that fails or is
    killed is not repeated: the next run is the next scheduled one.
    """
    periodic = _PERIODIC
    stmt = _due_periodic_statement(priorities).add_columns(
        func.now().label("database_now")
    )
    if only is not None:
        stmt = stmt.where(periodic.c.id == only)
    if skip is not None:
        stmt = stmt.where(periodic.c.id != skip)
    with pq.session() as session:
        due = session.execute(stmt.with_for_update(skip_locked=True)).one_or_none()
        if due is None:
            return None

        # The schedule's own cap when set, otherwise the worker's default.
        # It sets the child's timeout and the lock window, so the lock does
        # not expire while a long run is still going.
        effective_max_runtime = (
            due.max_runtime_seconds
            if due.max_runtime_seconds is not None
            else max_runtime
        )
        values: dict[str, object] = {"last_run": func.now()}
        if due.max_concurrent is not None:
            values["locked_until"] = func.now() + timedelta(
                seconds=_lock_seconds(effective_max_runtime)
            )
        if due.cron:
            values["next_run"] = _next_cron_run(due.cron, due.database_now)
        else:
            values["next_run"] = func.now() + due.run_every
        claimed = session.execute(
            update(periodic)
            .where(periodic.c.id == due.id)
            .values(values)
            .returning(*periodic.c)
        ).one()

    return _Claim(
        row=Periodic(**claimed._mapping),
        max_runtime=effective_max_runtime,
        locked_until=claimed.locked_until if due.max_concurrent is not None else None,
    )


def _claimed_task(claimed: Row[Any], max_runtime: float) -> _Claim:
    task = Task(**claimed._mapping)
    return _Claim(
        row=task,
        max_runtime=(
            task.max_runtime_seconds
            if task.max_runtime_seconds is not None
            else max_runtime
        ),
        started_at=task.started_at,
    )


def _try_claim(
    pq: PQ, *, max_runtime: float, priorities: tuple[int, ...]
) -> _Claim | None:
    """Claim the next thing to run. One-off tasks and periodic schedules
    form one queue: highest priority first, and within a priority whichever
    came due first.

    One-off tasks used to go first whatever their priority, so while any
    was due no schedule ran: a backlog held every schedule up until it was
    worked off. In one queue a schedule waits no longer than a task of its
    priority that came due at the same time, and a schedule that is due
    again as soon as it has run does not hold up the tasks either.

    A poll costs two statements on one connection when no schedule is due:
    the read of the schedules and the claim of a task.
    """
    try:
        with pq._autocommit() as session:
            schedule = session.execute(_due_periodic_statement(priorities)).first()
            if schedule is None:
                claimed = session.execute(
                    _claim_task_statement(priorities, before_schedule=False)
                ).one_or_none()
                return None if claimed is None else _claimed_task(claimed, max_runtime)

            claimed = session.execute(
                _claim_task_statement(priorities, before_schedule=True),
                {
                    "schedule_priority": schedule.priority,
                    "schedule_next_run": schedule.next_run,
                },
            ).one_or_none()
            if claimed is not None:
                return _claimed_task(claimed, max_runtime)
    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return None

    # The schedule is next. If it cannot be claimed (another worker is
    # claiming it, or its claim fails), what is behind it does not wait for
    # it: a task first, else another schedule.
    try:
        claim = _claim_periodic(
            pq, max_runtime=max_runtime, priorities=priorities, only=schedule.id
        )
        if claim is not None:
            return claim
    except Exception as e:
        logger.error(f"Error claiming periodic task '{schedule.name}': {e}")
    try:
        with pq._autocommit() as session:
            claimed = session.execute(
                _claim_task_statement(priorities, before_schedule=False)
            ).one_or_none()
        if claimed is not None:
            return _claimed_task(claimed, max_runtime)
        return _claim_periodic(
            pq, max_runtime=max_runtime, priorities=priorities, skip=schedule.id
        )
    except Exception as e:
        logger.error(f"Error claiming task: {e}")
        return None


def _try_start(
    pq: PQ,
    claim: _Claim,
    pre_execute: PreExecuteHook | None,
    post_execute: PostExecuteHook | None,
) -> _Child | None:
    """Fork the child for a claimed task.

    A task that cannot be started (its function cannot be imported, the fork
    fails) is recorded as failed, and None is returned.
    """
    try:
        handler = resolve_function_path(claim.row.name)
        args, kwargs = deserialize(claim.row.payload)
        child_pid, read_fd = _fork_child(
            handler,
            args,
            kwargs,
            max_runtime=claim.max_runtime,
            task=claim.row,
            pre_execute=pre_execute,
            post_execute=post_execute,
        )
    except Exception as e:
        _record(pq, claim, _ChildResult(TaskStatus.FAILED, str(e), "error"), 0.0)
        return None

    os.set_blocking(read_fd, False)
    now = time.monotonic()
    return _Child(
        pid=child_pid,
        claim=claim,
        started=now,
        kill_at=now + claim.max_runtime + KILL_GRACE_SECONDS,
        read_fd=read_fd,
    )


# --- In the worker: recording results ---


@lru_cache(maxsize=1)
def _finish_statement() -> Update:
    """The statement that ends a one-off run, with the run and its result as
    parameters (``run_id``, ``run_started_at``, ``result_status``,
    ``result_error``). Built once: it is the same for every run.

    Each column gets one of two values, picked under the row lock that the
    ``UPDATE`` takes: the result of the run, or, if an ``upsert()`` parked a
    new version on the row meanwhile, that version as a fresh PENDING task.
    """
    tasks = _TASKS
    closed = {
        "status": cast(
            bindparam("result_status", type_=tasks.c.status.type),
            tasks.c.status.type,
        ),
        "completed_at": func.now(),
        "error": bindparam("result_error", type_=tasks.c.error.type),
    }
    return (
        update(tasks)
        .where(
            tasks.c.id == bindparam("run_id"),
            tasks.c.status == TaskStatus.RUNNING,
            tasks.c.started_at
            == bindparam("run_started_at", type_=tasks.c.started_at.type),
        )
        .values(
            {
                column: case(
                    (tasks.c.requeue.is_(None), closed.get(column, tasks.c[column])),
                    else_=requeued,
                )
                for column, requeued in requeue_values().items()
            }
        )
        .returning(tasks.c.status)
    )


def _write_one_off_result(
    pq: PQ,
    task_id: int,
    started_at: datetime | None,
    status: TaskStatus,
    error_msg: str | None,
) -> TaskStatus | None:
    """End a one-off run in one statement. Returns the row's new status, or
    None if the row was not RUNNING for this run."""
    with pq._autocommit() as session:
        return session.execute(
            _finish_statement(),
            {
                "run_id": task_id,
                "run_started_at": started_at,
                "result_status": status,
                "result_error": error_msg,
            },
        ).scalar_one_or_none()


def _finish_one_off(
    pq: PQ,
    task_id: int,
    started_at: datetime | None,
    name: str,
    status: TaskStatus,
    error_msg: str | None,
) -> None:
    """Record the end of a one-off run.

    Only touches the row if it is still RUNNING *with this run's*
    ``started_at`` (the value written by its claim). So a reaper's FAILED
    verdict (or a cancel) is not overwritten, and neither is a later run
    of the same row: if the reaper re-queued the row while this run was
    still alive and another worker claimed it, ``started_at`` no longer
    matches. ``None`` never matches (a RUNNING row always has
    ``started_at``), so the row is left alone. If an ``upsert()`` parked a
    new version on the row while it ran (``Task.requeue``), the row is
    re-queued as PENDING with that version instead of being closed. The
    outcome of this run is then only in the worker log, the same as when
    an upsert overwrites a COMPLETED or FAILED row.

    The write is one ``UPDATE``, so it serializes with a concurrent
    ``upsert()``, whose ``ON CONFLICT DO UPDATE`` locks the same row: a
    parked version is either seen here, or the upsert runs after this
    statement and hits a non-RUNNING row, which it overwrites directly.

    A failed write is retried: without its result the row stays RUNNING
    until the stale reaper fails it, although the task ran to its end.
    """
    if error_msg is not None:
        # Postgres does not store the NUL character in text.
        error_msg = error_msg.replace("\x00", "\\x00")
    retry_delays = iter(_FINISH_RETRY_DELAYS)
    while True:
        try:
            new_status = _write_one_off_result(
                pq, task_id, started_at, status, error_msg
            )
            break
        except Exception as e:
            # No retry for a statement the database rejected, which it would
            # reject again, nor once the drain deadline has passed.
            delay = (
                None
                if isinstance(e, _NOT_RETRIED) or _shutdown.deadline_passed()
                else next(retry_delays, None)
            )
            if delay is None:
                logger.error(f"Error updating task status: {e}")
                return
            logger.warning(f"Error updating task status, retrying in {delay} s: {e}")
            time.sleep(delay)

    if new_status is None:
        logger.warning(
            f"Task '{name}' (id={task_id}) was no longer RUNNING"
            " for this run when the worker tried to record its result"
            " — likely reaped or canceled"
        )
    elif new_status == TaskStatus.PENDING:
        logger.info(
            f"Task '{name}' (id={task_id}) ended as {status.value};"
            " re-queued with the version upserted while it ran"
        )


def _release_periodic_lock(pq: PQ, claim: _Claim) -> None:
    """Clear the lock of a finished periodic run, if it is still this run's.

    A lock that another worker has set since (this run outlived its own) is
    left in place.
    """
    if claim.locked_until is None:
        return
    periodic = _PERIODIC
    try:
        with pq._autocommit() as session:
            session.execute(
                update(periodic)
                .where(
                    periodic.c.id == claim.row.id,
                    periodic.c.locked_until == claim.locked_until,
                )
                .values(locked_until=None)
            )
    except Exception as e:
        logger.error(f"Failed to clear lock for periodic task '{claim.row.name}': {e}")


def _record(pq: PQ, claim: _Claim, result: _ChildResult, elapsed: float) -> None:
    """Log how a run ended and write it to the database."""
    name = claim.row.name

    if claim.is_periodic:
        if result.exit_kind == "success":
            logger.debug(f"Periodic task '{name}' completed in {elapsed:.3f} s")
        elif result.exit_kind == "timeout":
            logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")
        elif result.exit_kind == "oom":
            logger.error(
                f"Periodic task '{name}' OOM after {elapsed:.3f} s: {result.error_msg}"
            )
        elif result.exit_kind == "killed":
            logger.error(
                f"Periodic task '{name}' killed after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        elif result.exit_kind == "shutdown":
            logger.warning(
                f"Periodic task '{name}' interrupted after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        else:
            logger.error(
                f"Periodic task '{name}' failed after {elapsed:.3f} s:"
                f" {result.error_msg}"
            )
        _release_periodic_lock(pq, claim)
        return

    error_msg = result.error_msg
    if result.exit_kind == "timeout":
        error_msg = f"Timed out after {elapsed:.3f} s"

    _finish_one_off(
        pq, claim.row.id, claim.started_at, name, result.task_status, error_msg
    )

    if result.task_status == TaskStatus.COMPLETED:
        logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")
    else:
        logger.error(f"Task '{name}' failed after {elapsed:.3f} s: {error_msg}")


def _collect(pq: PQ, children: dict[int, _Child]) -> None:
    """Record every child that has exited and drop it from ``children``."""
    for child in list(children.values()):
        result = _poll_child(child)
        if result is not None:
            del children[child.pid]
            _record(pq, child.claim, result, time.monotonic() - child.started)


# --- In the worker: maintenance ---


def _effective_stale_timeout(
    stale_task_timeout: timedelta | None, *, max_runtime: float
) -> timedelta | None:
    """The stale-reaper window, raised to twice the worker's max runtime.

    A shorter window fails tasks that are still running. Twice is the rule
    the reaper applies to a task with its own ``max_runtime_seconds``. For a
    very short max runtime the floor is the time a child can be alive.
    """
    if stale_task_timeout is None:
        return None
    floor = timedelta(
        seconds=max(2 * max_runtime, _lock_seconds(max_runtime))
        if max_runtime > 0
        else 0
    )
    if stale_task_timeout < floor:
        logger.warning(
            f"stale_task_timeout={stale_task_timeout} is too short for"
            f" max_runtime={max_runtime} s and would fail tasks that are"
            f" still running; using {floor}."
        )
        return floor
    return stale_task_timeout


@dataclass
class _Maintenance:
    """Cleanup of old finished tasks and the stale reaper, each on its own
    interval. An error in either is logged and retried at the next interval:
    it must not stop the worker loop."""

    pq: PQ
    retention_days: int  # 0 disables cleanup
    cleanup_interval: float
    stale_task_timeout: timedelta | None  # None disables the reaper
    reaper_interval: float = DEFAULT_REAPER_INTERVAL
    next_cleanup: float = 0.0  # time.monotonic()
    next_reap: float = 0.0

    def run_due(self) -> None:
        now = time.monotonic()
        if self.retention_days > 0 and now >= self.next_cleanup:
            self.next_cleanup = now + self.cleanup_interval
            self._clean_up()
        if self.stale_task_timeout is not None and now >= self.next_reap:
            self.next_reap = now + self.reaper_interval
            self._reap(self.stale_task_timeout)

    def _clean_up(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(days=self.retention_days)
        try:
            completed = self.pq.clear_completed(before=cutoff)
            failed = self.pq.clear_failed(before=cutoff)
        except Exception as e:
            logger.error(f"Error cleaning up finished tasks: {e}")
            return
        if completed or failed:
            logger.info(
                f"Cleanup: removed {completed} completed, {failed} failed tasks"
            )

    def _reap(self, stale_task_timeout: timedelta) -> None:
        try:
            reaped = self.pq.reap_stale_tasks(stale_task_timeout)
        except Exception as e:
            logger.error(f"Error reaping stale tasks: {e}")
            return
        if reaped:
            logger.info(f"Reaped {reaped} stale RUNNING task(s)")


# --- Entry points ---


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
    re-enqueue such tasks themselves. The one exception: if an ``upsert()``
    stored a new version on the row while it ran, the row is re-queued with
    that new version (the interrupted run itself is not retried).

    Args:
        pq: PQ client instance.
        concurrency: Maximum number of tasks to process simultaneously.
            Default: 1 (sequential processing).
        poll_interval: Seconds to sleep between polls when idle.
        max_runtime: Maximum execution time per task in seconds. Default: 30 min.
            The task's child stops itself at this time. A child that cannot
            (it is blocked outside the interpreter) is killed by the worker
            ``KILL_GRACE_SECONDS`` later.
        priorities: If set, only process tasks with these priority levels.
            Use this to dedicate workers to specific priority tiers.
        retention_days: Days to keep completed/failed tasks. Default: 7.
            Set to 0 to disable automatic cleanup.
        cleanup_interval: Seconds between cleanup runs. Default: 3600 (1 hour).
        stale_task_timeout: Mark RUNNING tasks older than this as FAILED
            (or re-queue them, if an upsert parked a new version on them).
            Catches orphaned tasks whose worker died mid-execution.
            Default: 1 hour. Set to ``None`` to disable. Must be longer
            than the longest run: a live run that is reaped can overlap
            with the version re-queued after it. A value below twice
            ``max_runtime`` is raised to that.
        drain_timeout: Seconds to wait for in-flight tasks when shutting
            down on SIGTERM/SIGINT. Default: 20. Must be set below the
            orchestrator's termination grace period, with headroom for the
            final status writes. ``0`` skips the wait entirely: in-flight
            tasks are killed as soon as the shutdown is noticed.
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
    stale_task_timeout = _effective_stale_timeout(
        stale_task_timeout, max_runtime=max_runtime
    )

    _shutdown.reset(drain_timeout)
    previous_handlers = _install_shutdown_handlers()
    try:
        _run_concurrent(
            pq,
            concurrency=max(concurrency, 1),
            poll_interval=poll_interval,
            max_runtime=max_runtime,
            priorities=priorities,
            pre_execute=pre_execute,
            post_execute=post_execute,
            retention_days=retention_days,
            cleanup_interval=cleanup_interval,
            stale_task_timeout=stale_task_timeout,
        )
    finally:
        _restore_shutdown_handlers(previous_handlers)
        # Clear the drain state: a lingering past-deadline flag would make
        # later run_worker_once calls in this process kill their children
        # as soon as they look at them.
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

    One-off tasks and periodic schedules form one queue: highest priority
    first, and within a priority whichever came due first.

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
    claim = _try_claim(
        pq, max_runtime=max_runtime, priorities=_priority_values(priorities)
    )
    if claim is None:
        return False
    child = _try_start(pq, claim, pre_execute, post_execute)
    if child is not None:
        children = {child.pid: child}
        while children:
            _wait(children.values(), _CHILD_POLL_SECONDS)
            _collect(pq, children)
    return True


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
    """Run the worker loop until a shutdown is requested, then drain.

    Keeps up to ``concurrency`` forked children running. It sleeps in
    ``poll()`` on the children's error pipes, so a child that finishes is
    recorded, and its slot refilled, at once.

    Args:
        pq: PQ client instance.
        concurrency: Maximum number of concurrent tasks.
        poll_interval: Seconds between polls for new tasks while a slot is free.
        max_runtime: Maximum execution time per task in seconds.
        priorities: If set, only process tasks with these priority levels.
        pre_execute: Called in forked child BEFORE task execution.
        post_execute: Called in forked child AFTER task execution.
        retention_days: Days to keep completed/failed tasks.
        cleanup_interval: Seconds between cleanup runs.
        stale_task_timeout: If set, reap RUNNING tasks older than this.
    """
    children: dict[int, _Child] = {}  # pid -> child
    priority_values = _priority_values(priorities)
    maintenance = _Maintenance(pq, retention_days, cleanup_interval, stale_task_timeout)

    try:
        try:
            while not _shutdown.requested:
                # Fill empty slots with new tasks
                while len(children) < concurrency and not _shutdown.requested:
                    claim = _try_claim(
                        pq, max_runtime=max_runtime, priorities=priority_values
                    )
                    if claim is None:
                        break
                    child = _try_start(pq, claim, pre_execute, post_execute)
                    if child is not None:
                        children[child.pid] = child
                if _shutdown.requested:
                    break

                # Each task is rate-limited on its own. Run on every pass,
                # so a worker that always has work still gets to them.
                maintenance.run_due()

                has_free_slot = len(children) < concurrency
                _wait(
                    children.values(),
                    poll_interval if has_free_slot else _CHILD_POLL_SECONDS,
                )
                _collect(pq, children)
        except KeyboardInterrupt:
            # Fallback for non-main-thread workers (no handlers installed);
            # the drain below still runs, just without a deadline.
            pass
        _shutdown.log_request()
    finally:
        # Also on an unexpected error: children in flight must not be left
        # behind with their rows RUNNING. Each child either finishes and
        # gets its real status, or is SIGKILLed at the drain deadline (one
        # deadline, shared by all children) and marked FAILED with the
        # shutdown error.
        if children:
            logger.info(f"Shutting down, draining {len(children)} in-flight task(s)...")
            while children:
                _wait(children.values(), _CHILD_POLL_SECONDS)
                _collect(pq, children)
    logger.info("Worker stopped.")
