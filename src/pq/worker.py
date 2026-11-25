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
import socket
import sys
import threading
import time
import traceback
import warnings
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
    Row,
    Select,
    Table,
    Update,
    and_,
    bindparam,
    cast,
    delete,
    exists,
    func,
    or_,
    select,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DataError, IntegrityError, ProgrammingError

from pq.client import _MAX_SECONDS, _close_pools_before_exit
from pq.models import (
    FailureKind,
    Periodic,
    Task,
    TaskStatus,
    Worker,
    end_run_values,
)
from pq.registry import resolve_function_path
from pq.serialization import deserialize

if TYPE_CHECKING:
    from collections.abc import Set

    from sqlalchemy.orm import Session

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

# A worker's heartbeat: every ``HEARTBEAT_INTERVAL_SECONDS`` it extends its
# lease in ``pq_workers`` to ``WORKER_TIMEOUT_SECONDS`` from then. A worker
# whose lease has run out counts as dead, and the tasks it was running as
# orphaned. Each worker writes its own lease, so workers with different
# values here can share a database.
WORKER_TIMEOUT_SECONDS: float = 60.0
HEARTBEAT_INTERVAL_SECONDS: float = 15.0

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

# What the child writes to its pipe when the handler has returned, before
# the post-execute hook runs. An error report starts with the name of an
# exception, so the two cannot be confused.
_HANDLER_DONE = b"\x06"


class _ChildResult(NamedTuple):
    """How a run ended."""

    failure: FailureKind | None  # None: the task completed
    error_msg: str | None = None

    @property
    def task_status(self) -> TaskStatus:
        return TaskStatus.COMPLETED if self.failure is None else TaskStatus.FAILED


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
    # Why the worker killed it: TIMEOUT or SHUTDOWN. None if it did not.
    killed: FailureKind | None = None
    # Once the pipe is gone, how long to sleep before the next look for the
    # child's exit. Doubles each time. A child closes its pipe as it exits,
    # so the first look, moments later, usually finds it gone.
    exit_poll: float = 0.00005


class _WorkerError(Exception):
    """Base class for worker errors."""


class _TaskTimeoutError(_WorkerError):
    """A task exceeded its max runtime."""


class _TaskOOMError(_WorkerError):
    """A task was killed, most likely by the OOM killer."""


class _TaskKilledError(_WorkerError):
    """A task was killed by a signal."""


class _TaskInterruptedError(_WorkerError):
    """A task was killed because the worker was shutting down."""


# pq raises none of these: how a run ended is in ``Task.failure_kind``. They
# stay importable under their old names, with a warning, until a later
# release removes them.
_DEPRECATED_EXCEPTIONS: dict[str, type[Exception]] = {
    "WorkerError": _WorkerError,
    "TaskTimeoutError": _TaskTimeoutError,
    "TaskOOMError": _TaskOOMError,
    "TaskKilledError": _TaskKilledError,
    "TaskInterruptedError": _TaskInterruptedError,
}
for _name, _cls in _DEPRECATED_EXCEPTIONS.items():
    _cls.__name__ = _cls.__qualname__ = _name


def __getattr__(name: str) -> type[Exception]:
    if name in _DEPRECATED_EXCEPTIONS:
        warnings.warn(
            f"pq.worker.{name} is deprecated and will be removed: pq never"
            " raises it. How a task failed is in Task.failure_kind.",
            DeprecationWarning,
            stacklevel=2,
        )
        return _DEPRECATED_EXCEPTIONS[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class _MaxRuntimeExceeded(Exception):
    """The worker's deadline for an async task passed."""


# The tables, for statements built without the ORM.
_TASKS = typing_cast(Table, Task.__table__)
_PERIODIC = typing_cast(Table, Periodic.__table__)
_WORKERS = typing_cast(Table, Worker.__table__)


@dataclass
class _ShutdownState:
    """Graceful-shutdown coordination for the worker loops.

    Written by ``request`` (from the signal handler, which runs in the main
    thread between bytecodes, or from ``stop_worker`` in any thread; each
    write is one attribute, so no locking is needed) and read by the worker
    loop and ``_poll_child``. Module-level because a worker owns its
    process; ``run_worker`` resets it on entry.
    """

    signum: int | None = None  # the signal that asked, None for stop_worker()
    deadline: float | None = None  # time.monotonic() deadline, set by request
    drain_timeout: float = DEFAULT_DRAIN_TIMEOUT
    logged: bool = False  # log_request() already ran for this shutdown
    # A pipe ``request`` writes to, so that a worker loop asleep in ``_wait``
    # wakes up at once. Set while ``run_worker`` runs.
    wake_r: int | None = None
    wake_w: int | None = None

    @property
    def requested(self) -> bool:
        return self.deadline is not None

    def reset(self, drain_timeout: float) -> None:
        self.signum = None
        self.deadline = None
        self.drain_timeout = drain_timeout
        self.logged = False

    def request(self, signum: int | None) -> None:
        """Ask the worker loop to drain and stop. Never raises, never logs
        (see ``_handle_shutdown_signal``). Later requests during the drain
        change nothing, so they cannot abandon the remaining children."""
        if self.requested:
            return
        self.signum = signum
        # Last: setting the deadline is what makes ``requested`` true.
        self.deadline = time.monotonic() + self.drain_timeout
        if self.wake_w is not None:
            try:
                os.write(self.wake_w, b"\0")
            except OSError:
                pass  # the loop still notices at its next wake-up

    def deadline_passed(self) -> bool:
        """True once shutdown was requested and the drain budget is spent."""
        return self.deadline is not None and time.monotonic() >= self.deadline

    def log_request(self) -> None:
        """Log the shutdown request once, from the worker loop.

        The signal handler can't log it itself (see
        ``_handle_shutdown_signal``), so the loops call this when they see
        ``requested``.
        """
        if not self.requested or self.logged:
            return
        self.logged = True
        source = "stop_worker()" if self.signum is None else f"signal {self.signum}"
        logger.info(
            f"Received {source}, draining (drain_timeout={self.drain_timeout}s)..."
        )


_shutdown = _ShutdownState()


def _handle_shutdown_signal(signum: int, frame: FrameType | None) -> None:
    """SIGTERM/SIGINT handler: request a drain, never raise, never log.

    Raising from here would land at arbitrary bytecode and could orphan a
    task (e.g. between claim-commit and child registration). Logging from
    here can deadlock: the handler runs on the main thread wherever it was
    interrupted, and loguru holds its handler locks for the whole of
    ``os.fork()``. The worker loops log the request instead
    (``_ShutdownState.log_request``).
    """
    _shutdown.request(signum)


def stop_worker() -> None:
    """Ask the worker running in this process to drain and stop, the way
    SIGTERM does: it claims nothing more, waits up to its ``drain_timeout``
    for the tasks in flight, and ``run_worker`` returns.

    Safe to call from any thread and from a signal handler. This is the
    only way to stop a worker that runs outside the main thread, where
    Python delivers no signals. Without a running ``run_worker`` it does
    nothing.
    """
    # The pipe is open exactly while ``run_worker`` runs. A request left
    # behind with no worker would make a later ``run_worker_once`` kill its
    # child as soon as the drain time is up.
    if _shutdown.wake_w is not None:
        _shutdown.request(None)


def _install_shutdown_handlers() -> dict[int, _SignalHandler]:
    """Open the wake-up pipe and register the drain handler for
    SIGTERM/SIGINT. Returns the previous handlers, for restoration.

    ``signal.signal`` is only allowed in the main thread, and only there do
    signals reach Python code. In any other thread no handler is installed
    and the worker is stopped with ``stop_worker()``.
    """
    _shutdown.wake_r, _shutdown.wake_w = os.pipe()
    os.set_blocking(_shutdown.wake_r, False)
    os.set_blocking(_shutdown.wake_w, False)
    previous: dict[int, _SignalHandler] = {}
    if threading.current_thread() is not threading.main_thread():
        logger.warning(
            "Worker is not running in the main thread: SIGTERM and SIGINT do"
            " not reach it. Call stop_worker() to shut it down gracefully."
        )
        return previous
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.getsignal(signum)
        signal.signal(signum, _handle_shutdown_signal)
    return previous


def _restore_shutdown_handlers(previous: dict[int, _SignalHandler]) -> None:
    """Undo ``_install_shutdown_handlers``."""
    for signum, handler in previous.items():
        signal.signal(signum, handler)
    wake_r, wake_w = _shutdown.wake_r, _shutdown.wake_w
    # Unset before closing: ``stop_worker()`` in another thread must not
    # write to a descriptor that is closed, or reused for something else.
    _shutdown.wake_r = _shutdown.wake_w = None
    for fd in (wake_r, wake_w):
        if fd is not None:
            os.close(fd)


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


@lru_cache(maxsize=1)
def _prctl() -> Callable[..., int] | None:
    """libc's ``prctl`` on Linux, else None. Looked up in the worker, once,
    so that the child only has to call it."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        import ctypes

        return ctypes.CDLL(None, use_errno=True).prctl
    except Exception:
        return None


_PR_SET_PDEATHSIG = 1


def _die_with_worker(worker_pid: int) -> None:
    """Have the kernel SIGKILL this child when its worker dies (Linux).

    The child runs in its own process group, so nothing else takes it down
    with the worker. Left running, it would finish its work while its row
    is failed, or re-queued and run a second time, as orphaned.

    The signal is sent when the thread that forked the child ends, which
    for a worker is the thread it runs in. Processes the task itself starts
    are not covered.
    """
    prctl = _prctl()
    if prctl is None:
        return
    try:
        prctl(_PR_SET_PDEATHSIG, int(signal.SIGKILL), 0, 0, 0)
    except Exception:
        return
    if os.getppid() != worker_pid:
        os._exit(EXIT_FAILURE)  # the worker died before the request took effect


def _write_pipe(fd: int, data: bytes) -> None:
    """Send bytes to the worker through the pipe (best effort)."""
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
    worker_pid: int,
) -> int:
    """Run the hooks and the handler in the child. Returns its exit code."""
    # Create new process group so we don't get parent's signals
    os.setpgrp()
    _die_with_worker(worker_pid)

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

    # The handler is done and its outcome goes to the worker now, before the
    # hook: a hook that overruns or dies does not change what the task did.
    # The alarm is for the handler. The hook runs until the worker's own
    # deadline, ``KILL_GRACE_SECONDS`` after the max runtime.
    signal.alarm(0)
    if exit_code == EXIT_SUCCESS:
        _write_pipe(error_write_fd, _HANDLER_DONE)
    elif report:
        _write_pipe(error_write_fd, report.encode("utf-8", errors="replace"))

    try:
        if post_execute:
            post_execute(task, error)
    except BaseException:
        pass  # Don't let hook errors mask task errors

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
    worker_pid: int,
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
            worker_pid,
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
    worker_pid = os.getpid()
    _prctl()  # loaded here, so the child does not have to
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
            worker_pid,
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
            FailureKind.ERROR,
            "Task result unknown: its process was reaped outside pq",
        )

    if pid == 0:
        if child.killed is None:
            if _shutdown.deadline_passed():
                child.killed = FailureKind.SHUTDOWN
            elif time.monotonic() >= child.kill_at:
                child.killed = FailureKind.TIMEOUT
            if child.killed is not None:
                _kill_child_group(child.pid)
        if child.read_fd is None:
            child.exit_poll = min(child.exit_poll * 2, _CHILD_POLL_SECONDS)
        return None

    # The child wrote its outcome before it exited, so all of it is in the
    # pipe now. No wait for end of file: a process the task left behind can
    # hold the pipe open for as long as it lives.
    _read_output(child)
    _close_output(child)

    exit_code = os.WEXITSTATUS(raw_status) if os.WIFEXITED(raw_status) else None
    if exit_code == EXIT_TIMEOUT:
        # The child stopped itself. What it started and left running is
        # past the deadline too.
        _kill_child_group(child.pid, reaped=True)

    if exit_code == EXIT_SUCCESS:
        return _ChildResult(None)

    # What the child wrote is how its handler ended, and that is the result
    # of the task. How the process ended after that, in the post-execute
    # hook or on its way out, does not change it.
    if child.output and exit_code is None:
        logger.warning(
            f"Task '{child.claim.row.name}' had finished, but its process was"
            " killed afterwards (post_execute overran, or a signal from"
            f" outside; wait status {raw_status})"
        )
    if child.output.startswith(_HANDLER_DONE):
        return _ChildResult(None)
    error_msg = child.output.decode("utf-8", errors="replace").rstrip()
    if error_msg:
        return _ChildResult(FailureKind.ERROR, error_msg)

    # Nothing written: the handler did not get to its end.
    if os.WIFSIGNALED(raw_status):
        signal_num = os.WTERMSIG(raw_status)
        if signal_num == signal.SIGKILL and child.killed == FailureKind.SHUTDOWN:
            return _ChildResult(
                FailureKind.SHUTDOWN,
                "Task interrupted by worker shutdown"
                f" (exceeded drain_timeout={_shutdown.drain_timeout}s)",
            )
        if signal_num == signal.SIGKILL and child.killed == FailureKind.TIMEOUT:
            return _ChildResult(FailureKind.TIMEOUT, "Task exceeded max runtime")
        if signal_num == signal.SIGKILL:
            max_rss_kb = rusage.ru_maxrss
            if sys.platform == "darwin":
                max_rss_kb = max_rss_kb // 1024
            return _ChildResult(
                FailureKind.OOM,
                f"Task killed (likely OOM). Max RSS: {max_rss_kb} KB",
            )
        return _ChildResult(FailureKind.KILLED, f"Task killed by signal {signal_num}")

    if exit_code is not None:
        if exit_code == EXIT_TIMEOUT:
            return _ChildResult(FailureKind.TIMEOUT, "Task exceeded max runtime")
        return _ChildResult(
            FailureKind.ERROR, f"Task failed with exit code {exit_code}"
        )

    return _ChildResult(FailureKind.ERROR, "Unknown child exit status")


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

    The claim also writes what the row has to say about the run while it is
    RUNNING: who runs it (``worker_id``) and how long it may take
    (``worker_max_runtime``, unless the task has its own cap).
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
        failure_kind=None,
        worker_id=bindparam("worker_id", type_=tasks.c.worker_id.type),
        claimed_max_runtime_seconds=func.coalesce(
            tasks.c.max_runtime_seconds,
            bindparam(
                "worker_max_runtime", type_=tasks.c.claimed_max_runtime_seconds.type
            ),
        ),
    ).returning(*tasks.c)


@lru_cache(maxsize=32)
def _due_periodic_statement(priorities: tuple[int, ...]) -> Select[Any]:
    """The due periodic schedules that are not locked, best first."""
    periodic = _PERIODIC
    due = select(periodic).where(
        periodic.c.active.is_(True),
        periodic.c.next_run <= func.now(),
        or_(
            periodic.c.max_concurrent.is_(None),
            periodic.c.locked_until.is_(None),
            periodic.c.locked_until <= func.now(),
        ),
    )
    if priorities:
        due = due.where(periodic.c.priority.in_(priorities))
    return due.order_by(periodic.c.priority.desc(), periodic.c.next_run).limit(1)


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


def _effective_max_runtime(own: float | None, default: float) -> float:
    """A row's own ``max_runtime_seconds`` when set, otherwise the worker's."""
    return own if own is not None else default


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

        # Sets the child's timeout and the lock window, so the lock does not
        # expire while a long run is still going.
        effective_max_runtime = _effective_max_runtime(
            due.max_runtime_seconds, max_runtime
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


def _claim_task(
    session: Session,
    priorities: tuple[int, ...],
    max_runtime: float,
    worker_id: int | None,
    schedule: Row[Any] | None = None,
) -> _Claim | None:
    """Claim the next due one-off task. With ``schedule``, only one that
    goes before that due schedule (see ``_claim_task_statement``)."""
    run: dict[str, object] = {
        "worker_id": worker_id,
        # NULL for a value the reaper could not turn into an interval. It
        # then falls back to its own window.
        "worker_max_runtime": max_runtime if 0 < max_runtime <= _MAX_SECONDS else None,
    }
    if schedule is None:
        claimed = session.execute(
            _claim_task_statement(priorities, before_schedule=False), run
        ).one_or_none()
    else:
        claimed = session.execute(
            _claim_task_statement(priorities, before_schedule=True),
            {
                **run,
                "schedule_priority": schedule.priority,
                "schedule_next_run": schedule.next_run,
            },
        ).one_or_none()
    if claimed is None:
        return None
    task = Task(**claimed._mapping)
    return _Claim(
        row=task,
        max_runtime=_effective_max_runtime(task.max_runtime_seconds, max_runtime),
        started_at=task.started_at,
    )


def _try_claim(
    pq: PQ,
    *,
    max_runtime: float,
    priorities: tuple[int, ...],
    worker_id: int | None = None,
) -> _Claim | None:
    """Claim the next thing to run. One-off tasks and periodic schedules
    form one queue: highest priority first, and within a priority whichever
    came due first.

    A poll costs two statements on one connection when no schedule is due:
    the read of the schedules and the claim of a task.
    """
    try:
        with pq._autocommit() as session:
            schedule = session.execute(_due_periodic_statement(priorities)).first()
            claim = _claim_task(session, priorities, max_runtime, worker_id, schedule)
            if claim is not None or schedule is None:
                return claim

        # The schedule is next. If it cannot be claimed (another worker is
        # claiming it, or its claim fails), what is behind it does not wait
        # for it: a task first, else another schedule.
        try:
            claim = _claim_periodic(
                pq, max_runtime=max_runtime, priorities=priorities, only=schedule.id
            )
            if claim is not None:
                return claim
        except Exception as e:
            logger.error(f"Error claiming periodic task '{schedule.name}': {e}")
        with pq._autocommit() as session:
            claim = _claim_task(session, priorities, max_runtime, worker_id)
        if claim is not None:
            return claim
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
        _record(pq, claim, _ChildResult(FailureKind.ERROR, str(e)), 0.0)
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
    ``result_error``, ``result_failure_kind``). Built once: it is the same
    for every run.

    Each column gets one of two values, picked under the row lock that the
    ``UPDATE`` takes: the result of the run, or, if an ``upsert()`` parked a
    new version on the row meanwhile, that version as a fresh PENDING task.

    A row that the orphan check failed is still this run's to close, as long
    as nothing has re-queued it: the worker was taken for dead, and here is
    its run reporting after all.
    """
    tasks = _TASKS
    closed = {
        "status": cast(
            bindparam("result_status", type_=tasks.c.status.type),
            tasks.c.status.type,
        ),
        "completed_at": func.now(),
        "error": bindparam("result_error", type_=tasks.c.error.type),
        "failure_kind": bindparam(
            "result_failure_kind", type_=tasks.c.failure_kind.type
        ),
    }
    return (
        update(tasks)
        .where(
            tasks.c.id == bindparam("run_id"),
            tasks.c.started_at
            == bindparam("run_started_at", type_=tasks.c.started_at.type),
            or_(
                tasks.c.status == TaskStatus.RUNNING,
                and_(
                    tasks.c.status == TaskStatus.FAILED,
                    tasks.c.failure_kind == FailureKind.WORKER_LOST,
                ),
            ),
        )
        .values(end_run_values(closed))
        .returning(tasks.c.status)
    )


def _write_one_off_result(
    pq: PQ, task_id: int, started_at: datetime | None, result: _ChildResult
) -> TaskStatus | None:
    """End a one-off run in one statement. Returns the row's new status, or
    None if the row was not this run's to close any more."""
    with pq._autocommit() as session:
        return session.execute(
            _finish_statement(),
            {
                "run_id": task_id,
                "run_started_at": started_at,
                "result_status": result.task_status,
                "result_error": result.error_msg,
                "result_failure_kind": result.failure,
            },
        ).scalar_one_or_none()


def _finish_one_off(
    pq: PQ,
    task_id: int,
    started_at: datetime | None,
    name: str,
    result: _ChildResult,
) -> None:
    """Record the end of a one-off run.

    Only touches the row if it is still RUNNING *with this run's*
    ``started_at`` (the value written by its claim). So the stale reaper's
    FAILED verdict is not overwritten, and neither is a later run of the
    same row: if a reaper re-queued the row while this run was still alive
    and another worker claimed it, ``started_at`` no longer matches.
    ``None`` never matches (a RUNNING row always has ``started_at``), so
    the row is left alone. A version that an ``upsert()`` parked on the row
    is applied instead of closing it (see ``_finish_statement``); the
    outcome of this run is then only in the worker log.

    A failed write is retried: without its result the row stays RUNNING
    until the stale reaper fails it, although the task ran to its end.
    """
    if result.error_msg is not None:
        # Postgres does not store the NUL character in text.
        result = result._replace(error_msg=result.error_msg.replace("\x00", "\\x00"))
    retry_delays = iter(_FINISH_RETRY_DELAYS)
    while True:
        try:
            new_status = _write_one_off_result(pq, task_id, started_at, result)
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
            " (likely reaped)"
        )
    elif new_status == TaskStatus.PENDING:
        logger.info(
            f"Task '{name}' (id={task_id}) ended as {result.task_status.value};"
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


# How ``_record`` logs a periodic run that failed: level and verb.
_PERIODIC_FAILURE_LOG: dict[FailureKind, tuple[str, str]] = {
    FailureKind.OOM: ("ERROR", "OOM"),
    FailureKind.KILLED: ("ERROR", "killed"),
    FailureKind.SHUTDOWN: ("WARNING", "interrupted"),
}


def _record(pq: PQ, claim: _Claim, result: _ChildResult, elapsed: float) -> None:
    """Log how a run ended and write it to the database."""
    name = claim.row.name

    if claim.is_periodic:
        if result.failure is None:
            logger.debug(f"Periodic task '{name}' completed in {elapsed:.3f} s")
        elif result.failure == FailureKind.TIMEOUT:
            logger.error(f"Periodic task '{name}' timed out after {elapsed:.3f} s")
        else:
            level, verb = _PERIODIC_FAILURE_LOG.get(result.failure, ("ERROR", "failed"))
            logger.log(
                level,
                f"Periodic task '{name}' {verb} after {elapsed:.3f} s:"
                f" {result.error_msg}",
            )
        _release_periodic_lock(pq, claim)
        return

    if result.failure == FailureKind.TIMEOUT:
        result = result._replace(error_msg=f"Timed out after {elapsed:.3f} s")

    _finish_one_off(pq, claim.row.id, claim.started_at, name, result)

    if result.failure is None:
        logger.debug(f"Task '{name}' completed in {elapsed:.3f} s")
    else:
        logger.error(f"Task '{name}' failed after {elapsed:.3f} s: {result.error_msg}")


def _collect(pq: PQ, children: dict[int, _Child]) -> None:
    """Record every child that has exited and drop it from ``children``."""
    for child in list(children.values()):
        result = _poll_child(child)
        if result is not None:
            del children[child.pid]
            _record(pq, child.claim, result, time.monotonic() - child.started)


def _drain(
    pq: PQ, children: dict[int, _Child], heartbeat: _Heartbeat | None = None
) -> None:
    """Wait for every child to exit and record it. The worker's lease is
    kept up meanwhile: its tasks are still running."""
    while children:
        if heartbeat is not None:
            heartbeat.run_due(check_orphans=False)
        _wait(children.values(), _CHILD_POLL_SECONDS)
        _collect(pq, children)


# --- In the worker: heartbeat ---


@lru_cache(maxsize=2)
def _heartbeat_statement(*, registered: bool) -> Any:
    """Extend this worker's lease in ``pq_workers`` (parameters ``hostname``,
    ``pid``, ``lease_seconds``, and ``worker_id`` once registered). Returns
    the worker's id.

    The first beat inserts the row. Later ones insert it again under the
    same id if it is gone, so the tasks that carry the id stay valid.
    """
    workers = _WORKERS
    expires_at = func.now() + func.make_interval(
        0, 0, 0, 0, 0, 0, bindparam("lease_seconds")
    )
    values: dict[str, object] = {
        "hostname": bindparam("hostname"),
        "pid": bindparam("pid"),
        "expires_at": expires_at,
    }
    if not registered:
        return insert(workers).values(values).returning(workers.c.id)
    values["id"] = bindparam("worker_id")
    stmt = insert(workers).values(values)
    return stmt.on_conflict_do_update(
        index_elements=[workers.c.id],
        set_={"expires_at": stmt.excluded.expires_at},
    ).returning(workers.c.id)


@lru_cache(maxsize=1)
def _orphan_statement() -> Update:
    """Fail, or re-queue from a parked version, every RUNNING task whose
    worker is dead: its lease has run out, or its row is gone.

    The error keeps the stale reaper's prefix: both mean that the task was
    still RUNNING when nothing was running it any more.
    """
    tasks, workers = _TASKS, _WORKERS
    alive = exists().where(
        workers.c.id == tasks.c.worker_id, workers.c.expires_at > func.now()
    )
    closed = {
        "status": cast(TaskStatus.FAILED.name, tasks.c.status.type),
        "completed_at": func.now(),
        "error": func.format(
            "Reaped: task still RUNNING, but its worker (id=%s) stopped"
            " sending heartbeats. Worker likely died.",
            tasks.c.worker_id,
        ),
        "failure_kind": cast(FailureKind.WORKER_LOST.value, tasks.c.failure_kind.type),
    }
    return (
        update(tasks)
        .where(
            tasks.c.status == TaskStatus.RUNNING,
            tasks.c.worker_id.is_not(None),
            ~alive,
        )
        .values(end_run_values(closed))
        .returning(tasks.c.id, tasks.c.name, tasks.c.status)
    )


# Rows of dead workers are kept this long after their lease ran out, then
# deleted. Looked for this often.
_DEAD_WORKER_RETENTION = timedelta(hours=1)
_PRUNE_WORKERS_INTERVAL: float = 600.0

# A heartbeat that failed is tried again after this long. Waiting a whole
# interval would let the lease run out.
_HEARTBEAT_RETRY_SECONDS: float = 1.0


@dataclass
class _Heartbeat:
    """This worker's lease in ``pq_workers``, and the check for tasks of
    workers whose lease has run out.

    An error in either is logged and tried again at the next interval: it
    must not stop the worker loop.
    """

    pq: PQ
    worker_id: int | None = None  # None until the first beat got through
    next_beat: float = 0.0  # time.monotonic()
    last_beat: float | None = None  # when the last beat got through
    # Since when every beat has got through, on time. The database was
    # reachable for all of that time.
    connected_since: float | None = None
    next_prune: float = 0.0

    def seconds_until_due(self) -> float:
        return max(self.next_beat - time.monotonic(), 0.0)

    def claim_id(self) -> int | None:
        """The worker id to claim under: this worker's, while its lease is
        good for at least one more heartbeat interval. Otherwise None.

        A task claimed under a lease that has run out, or is about to, is
        failed as orphaned by the other workers while it runs. Without a
        worker id only the stale reaper applies to it.
        """
        if self.last_beat is None:
            return None
        lease_left = WORKER_TIMEOUT_SECONDS - (time.monotonic() - self.last_beat)
        return self.worker_id if lease_left > HEARTBEAT_INTERVAL_SECONDS else None

    def run_due(self, *, check_orphans: bool = True) -> None:
        now = time.monotonic()
        if now < self.next_beat:
            return
        if not self._beat():
            self.next_beat = time.monotonic() + min(
                _HEARTBEAT_RETRY_SECONDS, HEARTBEAT_INTERVAL_SECONDS
            )
            return
        self.next_beat = now + HEARTBEAT_INTERVAL_SECONDS
        # Another worker is only called dead by a worker that has itself
        # got every beat through for a whole lease. After an outage of the
        # database every lease has run out, and the live workers must get
        # to renew theirs before anyone looks.
        connected_for = time.monotonic() - (self.connected_since or now)
        if check_orphans and connected_for >= WORKER_TIMEOUT_SECONDS:
            self._reap_orphans()

    def _beat(self) -> bool:
        parameters: dict[str, object] = {
            "hostname": socket.gethostname()[:255],
            "pid": os.getpid(),
            "lease_seconds": WORKER_TIMEOUT_SECONDS,
        }
        if self.worker_id is not None:
            parameters["worker_id"] = self.worker_id
        try:
            statement = _heartbeat_statement(registered=self.worker_id is not None)
            with self.pq._autocommit() as session:
                self.worker_id = session.execute(statement, parameters).scalar_one()
        except Exception as e:
            logger.error(f"Worker heartbeat failed: {e}")
            self.connected_since = None
            return False
        now = time.monotonic()
        gap = None if self.last_beat is None else now - self.last_beat
        if gap is not None and gap > WORKER_TIMEOUT_SECONDS:
            logger.warning(
                f"Worker heartbeat was late: {gap:.0f} s since the last one,"
                f" past the {WORKER_TIMEOUT_SECONDS:.0f} s after which a worker"
                " counts as dead. Tasks it was running may have been failed"
                " as orphaned."
            )
        if self.connected_since is None or (
            gap is not None and gap > 2 * HEARTBEAT_INTERVAL_SECONDS
        ):
            self.connected_since = now
        self.last_beat = now
        return True

    def _reap_orphans(self) -> None:
        workers = _WORKERS
        try:
            with self.pq._autocommit() as session:
                orphans = session.execute(_orphan_statement()).all()
                if time.monotonic() >= self.next_prune:
                    self.next_prune = time.monotonic() + _PRUNE_WORKERS_INTERVAL
                    session.execute(
                        delete(workers).where(
                            workers.c.expires_at < func.now() - _DEAD_WORKER_RETENTION
                        )
                    )
        except Exception as e:
            logger.error(f"Error reaping tasks of dead workers: {e}")
            return
        for task_id, name, status in orphans:
            if status == TaskStatus.PENDING:
                logger.warning(
                    f"Worker of task (id={task_id}) is gone; re-queued as"
                    f" '{name}' by a parked upsert"
                )
            else:
                logger.warning(
                    f"Worker of task '{name}' (id={task_id}) is gone; marked FAILED"
                )

    def close(self) -> None:
        """Delete the worker's row: it has stopped, and what is still
        RUNNING under its id is orphaned from now on."""
        if self.worker_id is None:
            return
        workers = _WORKERS
        try:
            with self.pq._autocommit() as session:
                session.execute(delete(workers).where(workers.c.id == self.worker_id))
        except Exception as e:
            logger.error(f"Failed to remove the worker's heartbeat row: {e}")


# --- In the worker: maintenance ---


def _effective_stale_timeout(
    stale_task_timeout: timedelta | None, *, max_runtime: float
) -> timedelta | None:
    """The stale-reaper window, raised to twice the worker's max runtime.

    Twice the max runtime of its run is what the reaper gives every row
    whose claim recorded one. The floor here covers the rows that have
    none, claimed by a version of pq that did not record it. For a very
    short max runtime the floor is the time a child can be alive.
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
    re-enqueue such tasks themselves, and find them by
    ``Task.failure_kind == FailureKind.SHUTDOWN``. The one exception: if an
    ``upsert()`` stored a new version on the row while it ran, the row is
    re-queued with that new version (the interrupted run itself is not
    retried). Outside the main thread no signal reaches the worker:
    ``stop_worker()`` starts the same shutdown from code.

    While it runs, the worker keeps a row in ``pq_workers`` and extends its
    lease there every ``HEARTBEAT_INTERVAL_SECONDS``. Tasks of a worker
    whose lease has run out (``WORKER_TIMEOUT_SECONDS`` without a heartbeat)
    are failed with ``FailureKind.WORKER_LOST`` by any other worker, or
    re-queued if an ``upsert()`` parked a version on them. On Linux the
    kernel kills a task's process when its worker dies, so such a task is
    not running any more. A worker that is alive but cannot reach the
    database for that long is taken for dead as well: its tasks are failed
    while they run, and a result they still report replaces that verdict.

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
            Catches what the heartbeat does not: a row left RUNNING by
            ``run_worker_once``, or by a run whose result could not be
            written. Default: 1 hour. Set to ``None`` to disable. A row
            gets at least twice the max runtime of its run (see
            ``PQ.reap_stale_tasks``), and a value below twice this
            worker's ``max_runtime`` is raised to that.
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

    This is one claim and one run, without what ``run_worker`` does around
    them:

    - No heartbeat, no cleanup and no stale reaper. A row left RUNNING (the
      process died during the run) stays RUNNING, and a version parked on
      it does not run, until something calls ``PQ.reap_stale_tasks``. A
      program that loops over this function calls it itself.
    - No signal handling. A SIGTERM during the call ends the process by
      default and leaves the row RUNNING.

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
        _drain(pq, {child.pid: child})
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
    heartbeat = _Heartbeat(pq)

    try:
        try:
            while not _shutdown.requested:
                # Before the claims: they are made under the lease only
                # while it is good (``claim_id``).
                heartbeat.run_due()

                # Fill empty slots with new tasks
                while len(children) < concurrency and not _shutdown.requested:
                    claim = _try_claim(
                        pq,
                        max_runtime=max_runtime,
                        priorities=priority_values,
                        worker_id=heartbeat.claim_id(),
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
                    min(
                        poll_interval if has_free_slot else _CHILD_POLL_SECONDS,
                        heartbeat.seconds_until_due(),
                    ),
                )
                _collect(pq, children)
        except KeyboardInterrupt:
            # Raised by something other than SIGINT, which the worker's
            # handler takes. The drain below still runs, without a deadline.
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
            _drain(pq, children, heartbeat)
        heartbeat.close()
    logger.info("Worker stopped.")
