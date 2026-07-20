"""PQ client - main interface for task queue."""

import importlib.resources
import math
from collections.abc import Callable, Set
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Self

from croniter import croniter
from croniter.croniter import CroniterBadCronError
from loguru import logger
import sqlalchemy as sa
from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from pq.models import Base, Periodic, Task, TaskStatus
from pq.priority import Priority
from pq.registry import get_function_path
from pq.serialization import serialize


def _validate_max_runtime(max_runtime_seconds: float | None) -> None:
    """Reject obviously-degenerate per-task ``max_runtime_seconds`` values.

    ``None`` means "use the worker's configured default" and is the
    common case — nothing to validate. ``0``, negative, NaN, and
    infinity are all rejected because the downstream enforcement is
    undefined for those:

    - ``signal.alarm(int(max_runtime_seconds) + 1)`` would fire at 1 s for
      ``0``, at 0 s (which disables the alarm entirely) for any
      negative integer ≥ -1, and raise ``ValueError`` from inside
      the worker for NaN / infinity.
    - ``max_runtime_seconds * 2`` in the stale-reaper SQL would be ≤ 0 or
      NaN, making the per-row threshold a no-op or NULL (the global
      default would apply) — silent surprise vs. the call site's
      intent.
    - Periodic ``lock_duration`` already guards with a 3600 s
      fallback for ``≤ 0`` (``worker.py``), but that fallback exists
      for the worker-level default, not as a contract for callers.

    None of these are useful behaviours to expose. Failing fast at
    enqueue/upsert/schedule time keeps the bug at the call site,
    where it's trivially fixable, instead of producing a confusing
    runtime surprise hours later.
    """
    if max_runtime_seconds is None:
        return
    if math.isnan(max_runtime_seconds) or math.isinf(max_runtime_seconds):
        raise ValueError(
            f"max_runtime_seconds must be a finite positive number (got {max_runtime_seconds!r}); "
            f"pass None to use the worker's configured default."
        )
    if max_runtime_seconds <= 0:
        raise ValueError(
            f"max_runtime_seconds must be > 0 (got {max_runtime_seconds!r}); pass None to "
            f"use the worker's configured default."
        )


class PQ:
    """Postgres-backed task queue client."""

    def __init__(self, database_url: str) -> None:
        """Initialize PQ with database connection.

        Args:
            database_url: PostgreSQL connection string.
        """
        self._engine: Engine = create_engine(
            database_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
        )
        self._session_factory = sessionmaker(bind=self._engine)

    @contextmanager
    def session(self) -> Any:
        """Get a database session context manager."""
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        """Close all database connections and dispose the engine.

        This closes all connections in the connection pool. After calling
        this method, the PQ instance should not be used.
        """
        self._engine.dispose()

    def __enter__(self) -> Self:
        """Enter context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context manager and close connections."""
        self.close()

    def run_db_migrations(self) -> None:
        """Run database migrations to latest version.

        Call this once at application startup before using the queue.
        Uses Alembic to apply any pending migrations. Safe to call
        multiple times - only pending migrations are applied.

        Example:
            pq = PQ("postgresql://localhost/mydb")
            pq.run_db_migrations()
        """
        # Lazy import to avoid fork issues on macOS
        from alembic import command
        from alembic.config import Config

        # Get migrations directory from within the installed package
        migrations_pkg = importlib.resources.files("pq.migrations")
        migrations_dir = str(migrations_pkg)

        alembic_cfg = Config()
        alembic_cfg.set_main_option("script_location", migrations_dir)
        alembic_cfg.set_main_option(
            "sqlalchemy.url", self._engine.url.render_as_string(hide_password=False)
        )
        command.upgrade(alembic_cfg, "head")

    def create_tables(self) -> None:
        """Create all tables directly (for testing only).

        For production, use run_db_migrations() instead. This method
        bypasses Alembic and creates tables directly via SQLAlchemy,
        which doesn't track schema versions.
        """
        Base.metadata.create_all(self._engine)

    def drop_tables(self) -> None:
        """Drop all tables (for testing only)."""
        Base.metadata.drop_all(self._engine)

    def clear_all(self) -> None:
        """Clear all tasks and periodic schedules."""
        with self.session() as session:
            session.execute(delete(Task))
            session.execute(delete(Periodic))

    def enqueue(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_at: datetime | None = None,
        priority: Priority = Priority.NORMAL,
        client_id: str | None = None,
        max_runtime_seconds: float | None = None,
        **kwargs: Any,
    ) -> int:
        """Enqueue a one-off task.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_at: When to run the task. Defaults to now.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            client_id: Optional client-provided identifier. Must be unique if provided.
            max_runtime_seconds: Per-task wall-clock cap in seconds. When ``None`` (the
                common case), the worker that picks up this task uses its
                configured default. When set, this specific task gets the
                supplied ceiling AND the stale-reaper threshold scales with
                it (``max_runtime_seconds * 2`` if larger than the reaper's global
                default). Use this for occasionally-long tasks in a fleet
                whose worker default is sized for typical short work.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Task ID.

        Raises:
            ValueError: If task is a lambda, closure, or cannot be imported,
                or if ``max_runtime_seconds`` is not None and ``<= 0``.
            IntegrityError: If client_id already exists.
        """
        _validate_max_runtime(max_runtime_seconds)
        name = get_function_path(task)
        payload = serialize(args, kwargs)

        if run_at is None:
            run_at = datetime.now(UTC)

        task_obj = Task(
            name=name,
            payload=payload,
            run_at=run_at,
            priority=priority,
            client_id=client_id,
            max_runtime_seconds=max_runtime_seconds,
        )

        with self.session() as session:
            session.add(task_obj)
            session.flush()
            return task_obj.id

    def upsert(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_at: datetime | None = None,
        priority: Priority = Priority.NORMAL,
        client_id: str,
        max_runtime_seconds: float | None = None,
        **kwargs: Any,
    ) -> int:
        """Enqueue a task, updating if client_id already exists.

        Behaves like enqueue(), but on conflict for client_id, updates all fields.
        Status resets to PENDING, attempts to 0, and timestamps are cleared.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_at: When to run the task. Defaults to now.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            client_id: Client-provided identifier. Required for conflict resolution.
            max_runtime_seconds: Per-task wall-clock cap in seconds. See ``enqueue`` for
                details. ``None`` (the default) leaves the worker's own
                configured ``max_runtime_seconds`` in effect.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Task ID.

        Raises:
            ValueError: If task is a lambda, closure, or cannot be imported,
                or if ``max_runtime_seconds`` is not None and ``<= 0``.
        """
        _validate_max_runtime(max_runtime_seconds)
        name = get_function_path(task)
        payload = serialize(args, kwargs)

        if run_at is None:
            run_at = datetime.now(UTC)

        stmt = (
            insert(Task)
            .values(
                client_id=client_id,
                name=name,
                payload=payload,
                priority=priority,
                status=TaskStatus.PENDING,
                run_at=run_at,
                max_runtime_seconds=max_runtime_seconds,
            )
            .on_conflict_do_update(
                index_elements=["client_id"],
                set_={
                    "name": name,
                    "payload": payload,
                    "priority": priority,
                    "status": TaskStatus.PENDING,
                    "run_at": run_at,
                    "max_runtime_seconds": max_runtime_seconds,
                    "attempts": 0,
                    "started_at": None,
                    "completed_at": None,
                    "error": None,
                },
            )
            .returning(Task.id)
        )

        with self.session() as session:
            result = session.execute(stmt)
            return result.scalar_one()

    def schedule(
        self,
        task: Callable[..., Any],
        *args: Any,
        run_every: timedelta | None = None,
        cron: str | croniter | None = None,
        priority: Priority = Priority.NORMAL,
        client_id: str | None = None,
        max_concurrent: int | None = 1,
        key: str = "",
        active: bool = True,
        max_runtime_seconds: float | None = None,
        **kwargs: Any,
    ) -> int:
        """Schedule a periodic task.

        If a periodic task with this function already exists, it will be updated.
        Either run_every or cron must be provided, but not both.

        Args:
            task: Callable function to execute.
            *args: Positional arguments to pass to the handler.
            run_every: Interval between executions (e.g., timedelta(hours=1)).
            cron: Cron expression string (e.g., "0 9 * * 1") or croniter object.
            priority: Task priority. Higher = higher priority. Defaults to NORMAL.
            client_id: Optional client-provided identifier. Must be unique if provided.
            max_concurrent: Maximum concurrent executions. Default 1 (no overlap).
                Set to None for unlimited concurrency. Values > 1 are reserved
                for future use and raise ValueError.
            key: Discriminator for multiple schedules of the same function.
                Defaults to "" (empty string).
            active: Whether the task is active. Inactive tasks are not executed.
                Defaults to True.
            max_runtime_seconds: Per-execution wall-clock cap in seconds. When ``None``
                (the common case), each execution uses the worker's configured
                default. When set, this schedule's executions get the
                supplied ceiling, and the ``locked_until`` window (used when
                ``max_concurrent`` is in effect) is extended to match so the
                lock doesn't expire while the execution is legitimately still
                running. Periodic tasks are not subject to the stale-task
                reaper, so this knob is purely about the wall-clock cap.
            **kwargs: Keyword arguments to pass to the handler.

        Returns:
            Periodic task ID.

        Raises:
            ValueError: If neither run_every nor cron is provided, or if both are.
            ValueError: If cron expression is invalid.
            ValueError: If max_concurrent is greater than 1.
            ValueError: If task is a lambda, closure, or cannot be imported.
            ValueError: If ``max_runtime_seconds`` is not None and ``<= 0``.
            IntegrityError: If client_id already exists.
        """
        if run_every is None and cron is None:
            raise ValueError("Either run_every or cron must be provided")
        if run_every is not None and cron is not None:
            raise ValueError("Only one of run_every or cron can be provided")
        if max_concurrent is not None and max_concurrent > 1:
            raise ValueError(
                f"max_concurrent must be 1 or None, got {max_concurrent} "
                "(values > 1 reserved for future use)"
            )
        _validate_max_runtime(max_runtime_seconds)

        # Validate and normalize cron expression
        cron_expr: str | None = None
        if cron is not None:
            if isinstance(cron, croniter):
                # Extract expression from croniter object
                cron_expr = " ".join(str(f) for f in cron.expressions)
            else:
                # Validate string expression
                try:
                    croniter(cron)
                except (KeyError, ValueError, CroniterBadCronError) as e:
                    raise ValueError(f"Invalid cron expression '{cron}': {e}") from e
                cron_expr = cron

        name = get_function_path(task)
        payload = serialize(args, kwargs)

        # Calculate next_run based on cron or interval
        now = datetime.now(UTC)
        if cron_expr:
            cron_iter = croniter(cron_expr, now)
            next_run = cron_iter.get_next(datetime)
        else:
            next_run = now

        with self.session() as session:
            stmt = (
                insert(Periodic)
                .values(
                    name=name,
                    key=key,
                    payload=payload,
                    priority=priority,
                    run_every=run_every,
                    cron=cron_expr,
                    next_run=next_run,
                    client_id=client_id,
                    max_concurrent=max_concurrent,
                    active=active,
                    max_runtime_seconds=max_runtime_seconds,
                )
                .on_conflict_do_update(
                    index_elements=["name", "key"],
                    set_={
                        "payload": payload,
                        "priority": priority,
                        "run_every": run_every,
                        "cron": cron_expr,
                        "next_run": next_run,
                        "max_concurrent": max_concurrent,
                        "active": active,
                        "max_runtime_seconds": max_runtime_seconds,
                    },
                )
                .returning(Periodic.id)
            )
            result = session.execute(stmt)
            return result.scalar_one()

    def cancel(self, task_id: int) -> bool:
        """Cancel a one-off task by ID.

        Args:
            task_id: Task ID.

        Returns:
            True if task was found and deleted, False otherwise.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.id == task_id)
            result = session.execute(stmt)
            return result.rowcount > 0

    def unschedule(self, task: Callable[..., Any] | str, *, key: str = "") -> bool:
        """Remove a periodic task.

        Args:
            task: The scheduled function to remove, or its import path
                as a 'module:name' string (useful when the module has
                been removed and the callable is no longer importable).
            key: Discriminator key. Defaults to "" (the default schedule).

        Returns:
            True if task was found and deleted, False otherwise.
        """
        name = task if isinstance(task, str) else get_function_path(task)
        with self.session() as session:
            stmt = delete(Periodic).where(Periodic.name == name, Periodic.key == key)
            result = session.execute(stmt)
            return result.rowcount > 0

    def pending_count(self) -> int:
        """Count pending one-off tasks."""
        with self.session() as session:
            result = session.execute(
                select(func.count())
                .select_from(Task)
                .where(Task.status == TaskStatus.PENDING)
            )
            return result.scalar_one()

    def periodic_count(self) -> int:
        """Count periodic task schedules."""
        with self.session() as session:
            result = session.execute(select(func.count()).select_from(Periodic))
            return result.scalar_one()

    def get_task(self, task_id: int) -> Task | None:
        """Get a task by ID.

        Args:
            task_id: Task ID.

        Returns:
            Task object or None if not found.
        """
        with self.session() as session:
            task = session.get(Task, task_id)
            if task:
                session.expunge(task)
            return task

    def get_task_by_client_id(self, client_id: str) -> Task | None:
        """Get a task by client_id.

        Args:
            client_id: Client-provided identifier.

        Returns:
            Task object or None if not found.
        """
        with self.session() as session:
            stmt = select(Task).where(Task.client_id == client_id)
            task = session.execute(stmt).scalar_one_or_none()
            if task:
                session.expunge(task)
            return task

    def get_periodic_by_client_id(self, client_id: str) -> Periodic | None:
        """Get a periodic task by client_id.

        Args:
            client_id: Client-provided identifier.

        Returns:
            Periodic object or None if not found.
        """
        with self.session() as session:
            stmt = select(Periodic).where(Periodic.client_id == client_id)
            periodic = session.execute(stmt).scalar_one_or_none()
            if periodic:
                session.expunge(periodic)
            return periodic

    def list_failed(self, limit: int = 100) -> list[Task]:
        """List failed tasks.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of failed tasks, most recent first.
        """
        with self.session() as session:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.FAILED)
                .order_by(Task.completed_at.desc())
                .limit(limit)
            )
            tasks = list(session.execute(stmt).scalars().all())
            for task in tasks:
                session.expunge(task)
            return tasks

    def list_completed(self, limit: int = 100) -> list[Task]:
        """List completed tasks.

        Args:
            limit: Maximum number of tasks to return.

        Returns:
            List of completed tasks, most recent first.
        """
        with self.session() as session:
            stmt = (
                select(Task)
                .where(Task.status == TaskStatus.COMPLETED)
                .order_by(Task.completed_at.desc())
                .limit(limit)
            )
            tasks = list(session.execute(stmt).scalars().all())
            for task in tasks:
                session.expunge(task)
            return tasks

    def clear_completed(self, before: datetime | None = None) -> int:
        """Clear completed tasks.

        Args:
            before: Only clear tasks completed before this time. If None, clears all.

        Returns:
            Number of tasks deleted.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.COMPLETED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            result = session.execute(stmt)
            return result.rowcount

    def clear_failed(self, before: datetime | None = None) -> int:
        """Clear failed tasks.

        Args:
            before: Only clear tasks failed before this time. If None, clears all.

        Returns:
            Number of tasks deleted.
        """
        with self.session() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.FAILED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            result = session.execute(stmt)
            return result.rowcount

    def reap_stale_tasks(self, threshold: timedelta) -> int:
        """Mark stale RUNNING tasks as FAILED.

        When a worker dies mid-execution (e.g. pod restart, OOM on the worker
        process), in-flight tasks stay RUNNING forever because no parent
        process remains to update their status. This method detects those
        orphaned rows and transitions them to FAILED.

        Tasks enqueued with a per-task ``max_runtime_seconds`` override get a
        proportionally larger reaper window: the effective threshold for
        a row is ``max(threshold, max_runtime_seconds * 2)``. This avoids
        reaping a legitimately long-running task whose declared budget
        exceeds the global default. NULL ``max_runtime_seconds`` (the
        common case) falls back to the supplied ``threshold`` unchanged.

        Args:
            threshold: Default per-task stale window for tasks that don't
                have a ``max_runtime_seconds`` override (or whose override
                is smaller than this default). A task is stale when
                ``started_at + effective_threshold < now()``. Should be
                at least 2x the worker's own ``max_runtime_seconds``, e.g.
                ``timedelta(seconds=max_runtime_seconds * 2)``.

        Returns:
            Number of tasks reaped.
        """
        now = datetime.now(UTC)
        threshold_seconds = threshold.total_seconds()
        # Per-row effective stale window: the larger of the supplied
        # default and 2x the row's per-task override (NULL → just the
        # default). Computed in SQL with ``GREATEST`` + ``COALESCE``
        # so the reaper stays a single round-trip even when the row
        # set is heterogeneous in declared budget.
        effective_threshold_seconds = sa.func.greatest(
            threshold_seconds,
            sa.func.coalesce(Task.max_runtime_seconds * 2, threshold_seconds),
        )
        stale_cutoff = sa.func.now() - sa.func.make_interval(
            sa.literal(0),  # years
            sa.literal(0),  # months
            sa.literal(0),  # weeks
            sa.literal(0),  # days
            sa.literal(0),  # hours
            sa.literal(0),  # mins
            effective_threshold_seconds,  # secs
        )
        # Per-row ``error`` message includes the values that actually
        # decided this row's fate: the row's own
        # ``max_runtime_seconds`` (or ``NULL`` if unset) and the
        # effective threshold the reaper applied. This avoids the
        # operator needing a separate query to figure out "why did MY
        # long-budget task get reaped?" — the answer is in the row.
        # Built with Postgres ``format()`` so the message is computed
        # per row in the same UPDATE round-trip.
        per_row_error = sa.func.format(
            "Reaped: task still RUNNING past its stale window "
            "(default threshold=%s s, per-task max_runtime_seconds=%s, "
            "effective threshold=%s s). Worker likely died.",
            threshold_seconds,
            sa.func.coalesce(sa.cast(Task.max_runtime_seconds, sa.String), "NULL"),
            effective_threshold_seconds,
        )
        with self.session() as session:
            stmt = (
                update(Task)
                .where(
                    Task.status == TaskStatus.RUNNING,
                    Task.started_at < stale_cutoff,
                )
                .values(
                    status=TaskStatus.FAILED,
                    completed_at=now,
                    error=per_row_error,
                )
                .returning(
                    Task.id,
                    Task.name,
                    Task.started_at,
                    Task.max_runtime_seconds,
                )
            )
            reaped = list(session.execute(stmt).all())
            for task_id, name, started_at, max_runtime_seconds in reaped:
                logger.warning(
                    f"Reaped stale task '{name}' (id={task_id},"
                    f" started_at={started_at},"
                    f" max_runtime_seconds={max_runtime_seconds})"
                )
            return len(reaped)

    def run_worker(
        self,
        *,
        concurrency: int = 1,
        poll_interval: float = 1.0,
        max_runtime: float = 30 * 60,
        priorities: Set[Priority] | None = None,
        drain_timeout: float = 20.0,
    ) -> None:
        """Run the worker loop (blocking).

        Each task executes in a forked child process for memory isolation.

        On SIGTERM or SIGINT the worker stops claiming new tasks, waits up
        to ``drain_timeout`` seconds for in-flight tasks to finish, then
        kills any still-running task and marks its row FAILED with an
        explicit shutdown error. Interrupted tasks are not re-queued.

        Args:
            concurrency: Maximum number of tasks to process simultaneously.
                Default: 1 (sequential processing).
            poll_interval: Seconds to sleep between polls when idle.
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
            priorities: If set, only process tasks with these priority levels.
                Use this to dedicate workers to specific priority tiers.
            drain_timeout: Seconds to wait for in-flight tasks on
                SIGTERM/SIGINT. Default: 20. Set below the orchestrator's
                termination grace period. ``0`` skips the wait: in-flight
                tasks are killed as soon as the shutdown is noticed.
        """
        from pq.worker import run_worker

        run_worker(
            self,
            concurrency=concurrency,
            poll_interval=poll_interval,
            max_runtime=max_runtime,
            priorities=priorities,
            drain_timeout=drain_timeout,
        )

    def run_worker_once(
        self,
        *,
        max_runtime: float = 30 * 60,
        priorities: Set[Priority] | None = None,
    ) -> bool:
        """Process a single task if available.

        Each task executes in a forked child process for memory isolation.

        Args:
            max_runtime: Maximum execution time per task in seconds. Default: 30 min.
            priorities: If set, only process tasks with these priority levels.

        Returns:
            True if a task was processed, False if queue was empty.
        """
        from pq.worker import run_worker_once

        return run_worker_once(self, max_runtime=max_runtime, priorities=priorities)
