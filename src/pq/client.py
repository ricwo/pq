"""PQ client - main interface for task queue."""

import importlib.resources
import math
import os
import weakref
from functools import lru_cache
from collections.abc import Callable, Iterator, Sequence, Set
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Any, Self

from croniter import croniter
from croniter.croniter import CroniterBadCronError
from loguru import logger
import sqlalchemy as sa
from sqlalchemy import create_engine, delete, func, select, update
from sqlalchemy.dialects.postgresql import Insert, insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import InstrumentedAttribute, Session, sessionmaker

from pq.models import Base, Periodic, Task, TaskStatus, requeue_values
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
    if max_runtime_seconds > _MAX_SECONDS:
        raise ValueError(
            f"max_runtime_seconds must be at most {_MAX_SECONDS} (got"
            f" {max_runtime_seconds!r}); pass None to use the worker's"
            f" configured default."
        )


# Upper bound for ``max_runtime_seconds`` and ``run_every``: about 30 years.
# A time that far ahead still fits a timestamp; much larger values overflow
# when a schedule is claimed, which would fail on every poll.
_MAX_SECONDS: int = 10**9


_live_instances: "weakref.WeakSet[PQ]" = weakref.WeakSet()
"""Every ``PQ`` instance, so a forked child can reset their pools. Weak
references: the set never keeps an instance alive. ``close()`` does not remove
an instance: its engine reconnects if it is used again."""


def _reset_pools_after_fork() -> None:
    """Give each ``PQ`` instance an empty pool in a forked child.

    A child inherits the parent's pooled connections, sockets and TLS state
    included. Querying on one makes two processes speak on one Postgres
    session: over TLS the parent's next record fails with ``bad record mac``,
    over a plain socket the two processes read each other's results.

    ``close=False`` drops the inherited pool without closing its connections.
    Closing them would send a terminate message on the parent's sockets and
    break the parent's sessions.

    Connections checked out at fork time are not covered: if the child returns
    one (for example by leaving a ``with pq.session()`` block it was forked
    in), the commit and rollback still go out on the parent's socket. Do not
    fork inside an open ``pq.session()``.
    """
    _dispose_pools(
        close=False, failure="Failed to reset a PQ connection pool after fork"
    )


def _close_pools_before_exit() -> None:
    """Close the connections a forked child opened, before it calls
    ``os._exit()``.

    ``os._exit()`` skips interpreter cleanup, so without this Postgres sees
    each connection drop without a terminate message: it counts the session
    as abandoned, and over TLS logs ``unexpected eof while reading``. The
    connections inherited from the parent are not in these pools any more
    (``_reset_pools_after_fork`` dropped them), so this closes only the
    child's own. Connections still checked out are left alone.
    """
    _dispose_pools(close=True, failure="Failed to close a PQ connection pool")


def _dispose_pools(*, close: bool, failure: str) -> None:
    for instance in list(_live_instances):
        try:
            instance._engine.dispose(close=close)
        except Exception:
            # One failing instance must not stop the others.
            logger.exception(failure)


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_pools_after_fork)


@dataclass(kw_only=True, slots=True)
class Upsert:
    """One task for ``PQ.upsert_many``; the fields are ``upsert()``'s arguments.

    ``args`` and ``kwargs`` are fields here, where ``upsert()`` takes
    ``*args`` and ``**kwargs``. The item is checked when it is upserted,
    by the same code as ``upsert()``, so both accept the same values.
    """

    task: Callable[..., object]
    client_id: str
    # Kept out of ``repr`` so task arguments (tokens, PII) don't reach logs.
    args: tuple[object, ...] = field(default=(), repr=False)
    kwargs: dict[str, object] = field(default_factory=dict, repr=False)
    run_at: datetime | None = None
    priority: Priority = Priority.NORMAL
    max_runtime_seconds: float | None = None


def _upsert_row(u: Upsert) -> dict[str, object]:
    """Check one item and turn it into the row ``_upsert_statement`` inserts."""
    _validate_max_runtime(u.max_runtime_seconds)
    return {
        "client_id": u.client_id,
        "name": get_function_path(u.task),
        "payload": serialize(u.args, u.kwargs),
        "priority": int(u.priority),
        "status": TaskStatus.PENDING,
        "run_at": datetime.now(UTC) if u.run_at is None else u.run_at,
        "max_runtime_seconds": u.max_runtime_seconds,
    }


@lru_cache(maxsize=1)
def _upsert_statement() -> Insert:
    """The ``INSERT ... ON CONFLICT DO UPDATE`` behind ``PQ.upsert``, for
    rows made by ``_upsert_row``.

    In ``ON CONFLICT DO UPDATE``, ``Task.<column>`` is the existing row and
    ``excluded.<column>`` the row that was to be inserted. A RUNNING row
    keeps every column as it is (the run in progress keeps its payload,
    ``started_at``, ``attempts`` and ``max_runtime_seconds``); the new
    version is parked in ``requeue`` and applied when the run ends. Any other
    status gets the plain overwrite. All in one statement, so the decision is
    made under the row lock that ``ON CONFLICT`` takes.
    """
    stmt = insert(Task)
    new = stmt.excluded
    running = Task.status == TaskStatus.RUNNING

    def unless_running[T](column: InstrumentedAttribute[T]) -> sa.ColumnElement[T]:
        return sa.case((running, column), else_=new[column.key])

    def reset_unless_running[T](
        column: InstrumentedAttribute[T], value: T
    ) -> sa.ColumnElement[T]:
        return sa.case((running, column), else_=value)

    # ``run_at`` goes into the JSON as ``timestamptz``, so Postgres writes it
    # as ISO 8601 with an offset: the instant the direct path stores, whatever
    # the time zone of the session that applies the version later.
    requeue = sa.func.jsonb_build_object(
        "name",
        new.name,
        "payload",
        new.payload,
        "priority",
        new.priority,
        "run_at",
        new.run_at,
        "max_runtime_seconds",
        new.max_runtime_seconds,
    )
    return stmt.on_conflict_do_update(
        index_elements=["client_id"],
        set_={
            "name": unless_running(Task.name),
            "payload": unless_running(Task.payload),
            "priority": unless_running(Task.priority),
            "status": unless_running(Task.status),
            "run_at": unless_running(Task.run_at),
            "max_runtime_seconds": unless_running(Task.max_runtime_seconds),
            "attempts": reset_unless_running(Task.attempts, 0),
            "started_at": reset_unless_running(Task.started_at, None),
            "completed_at": reset_unless_running(Task.completed_at, None),
            "error": reset_unless_running(Task.error, None),
            "requeue": sa.case((running, requeue), else_=sa.null()),
        },
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
        _live_instances.add(self)

    @contextmanager
    def session(self) -> Iterator[Session]:
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

    @contextmanager
    def _autocommit(self) -> Iterator[Session]:
        """A session in which each statement is its own transaction.

        For work that is one statement (an enqueue, a claim, the end of a
        run): it saves the ``BEGIN`` and ``COMMIT`` round trips that
        ``session()`` adds, which are half of what such a call sends.

        Autocommit is switched on the driver's connection, the way
        SQLAlchemy's own pre-ping does it. SQLAlchemy's ``AUTOCOMMIT``
        isolation level would cost what it saves: with psycopg2 it sends a
        ``SET default_transaction_isolation`` each time a pooled connection
        enters or leaves it.
        """
        with self._engine.connect() as connection:
            driver_connection = connection.connection.dbapi_connection
            assert driver_connection is not None
            driver_connection.autocommit = True
            try:
                with Session(bind=connection, expire_on_commit=False) as session:
                    yield session
                    # Sends nothing in autocommit. Commits for real on a
                    # driver that ignored the attribute.
                    session.commit()
            finally:
                try:
                    driver_connection.autocommit = False
                except Exception:
                    # Never return a connection to the pool in autocommit:
                    # ``session()`` would lose its transactions on it.
                    connection.invalidate()

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

        stmt = (
            insert(Task)
            .values(
                name=name,
                payload=payload,
                run_at=run_at,
                priority=int(priority),
                client_id=client_id,
                max_runtime_seconds=max_runtime_seconds,
            )
            .returning(Task.id)
        )
        with self._autocommit() as session:
            return session.execute(stmt).scalar_one()

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

        If the existing task is RUNNING, it is not changed. The new version
        is stored on the row and runs after the current run ends (the worker
        or the stale reaper re-queues the row with it). A second upsert while
        the row is still RUNNING replaces the stored version, so only the
        latest one runs. One ``client_id`` does not run twice at the same
        time, as long as the stale reaper's timeout is longer than the
        longest run. If the worker running the task died, the stored version
        runs only after the stale reaper re-queues the row: with the reaper
        disabled (``stale_task_timeout=None``) it never runs.

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
        row = _upsert_row(
            Upsert(
                task=task,
                client_id=client_id,
                args=args,
                kwargs=kwargs,
                run_at=run_at,
                priority=priority,
                max_runtime_seconds=max_runtime_seconds,
            )
        )
        with self._autocommit() as session:
            stmt = _upsert_statement().returning(Task.id)
            return session.execute(stmt, row).scalar_one()

    def upsert_many(self, upserts: Sequence[Upsert]) -> list[int]:
        """Upsert several tasks in one transaction.

        Each item behaves like ``upsert()`` with the same arguments, so a
        RUNNING row gets the new version parked. A repeated ``client_id``
        ends with its last version. The batch is sent as one multi-row
        statement per 1000 tasks, against one statement and one commit per
        ``upsert()`` call. Nothing is written if any item fails.

        The rows are written in ``client_id`` order, so that two batches
        with overlapping ids lock rows in the same order and cannot
        deadlock. Each row stays locked until the batch commits: a worker
        finishing one of these tasks waits for the whole batch. Split very
        large batches into chunks to keep that wait short.

        Returns:
            Task IDs, in the order of ``upserts``.

        Raises:
            ValueError: As ``upsert()``, for the first invalid item.
        """
        rows = [_upsert_row(u) for u in upserts]
        if not rows:
            return []
        # One statement cannot update a row twice: keep the last version of
        # a repeated ``client_id``, which is what applying them in turn
        # leaves behind.
        latest = {u.client_id: row for u, row in zip(upserts, rows, strict=True)}
        ordered = [latest[client_id] for client_id in sorted(latest)]
        stmt = _upsert_statement().returning(Task.client_id, Task.id)
        with self.session() as session:
            ids = {
                client_id: task_id
                for client_id, task_id in session.execute(stmt, ordered)
            }
        return [ids[u.client_id] for u in upserts]

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

        If a periodic task with this function and ``key`` already exists, it
        is updated. Its next run stays where it was unless ``run_every`` or
        ``cron`` changed, so calling this on every application start does
        not run the task each time. A new schedule, one whose timing
        changed, and one that was inactive first run now (``run_every``) or
        at the next cron time.
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
        if run_every is not None and not (
            timedelta(0) <= run_every <= timedelta(seconds=_MAX_SECONDS)
        ):
            raise ValueError(
                f"run_every must be between 0 and {_MAX_SECONDS} seconds"
                f" (got {run_every!r})"
            )
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

        stmt = insert(Periodic).values(
            name=name,
            key=key,
            payload=payload,
            priority=int(priority),
            run_every=run_every,
            cron=cron_expr,
            next_run=next_run,
            client_id=client_id,
            max_concurrent=max_concurrent,
            active=active,
            max_runtime_seconds=max_runtime_seconds,
        )
        new = stmt.excluded
        # Workers register their schedules on every start. A schedule whose
        # timing did not change keeps its ``next_run``: resetting it would
        # run every interval schedule once per worker start, and would skip
        # a cron time that came due while the workers restarted. A schedule
        # that was switched off starts afresh: its ``next_run`` is from
        # before the pause.
        same_timing = sa.and_(
            Periodic.active.is_(True),
            Periodic.run_every.is_not_distinct_from(new.run_every),
            Periodic.cron.is_not_distinct_from(new.cron),
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["name", "key"],
            set_={
                "payload": new.payload,
                "priority": new.priority,
                "run_every": new.run_every,
                "cron": new.cron,
                "next_run": sa.case(
                    (same_timing, Periodic.next_run), else_=new.next_run
                ),
                "max_concurrent": new.max_concurrent,
                "active": new.active,
                "max_runtime_seconds": new.max_runtime_seconds,
            },
        ).returning(Periodic.id)
        with self._autocommit() as session:
            return session.execute(stmt).scalar_one()

    def cancel(self, task_id: int) -> bool:
        """Cancel a one-off task by ID.

        Args:
            task_id: Task ID.

        Returns:
            True if task was found and deleted, False otherwise.
        """
        with self._autocommit() as session:
            stmt = delete(Task).where(Task.id == task_id)
            return session.connection().execute(stmt).rowcount > 0

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
        with self._autocommit() as session:
            stmt = delete(Periodic).where(Periodic.name == name, Periodic.key == key)
            return session.connection().execute(stmt).rowcount > 0

    def pending_count(self) -> int:
        """Count pending one-off tasks."""
        with self._autocommit() as session:
            result = session.execute(
                select(func.count())
                .select_from(Task)
                .where(Task.status == TaskStatus.PENDING)
            )
            return result.scalar_one()

    def periodic_count(self) -> int:
        """Count periodic task schedules."""
        with self._autocommit() as session:
            result = session.execute(select(func.count()).select_from(Periodic))
            return result.scalar_one()

    def get_task(self, task_id: int) -> Task | None:
        """Get a task by ID.

        Args:
            task_id: Task ID.

        Returns:
            Task object or None if not found.
        """
        with self._autocommit() as session:
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
        with self._autocommit() as session:
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
        with self._autocommit() as session:
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
        with self._autocommit() as session:
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
        with self._autocommit() as session:
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
        with self._autocommit() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.COMPLETED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            return session.connection().execute(stmt).rowcount

    def clear_failed(self, before: datetime | None = None) -> int:
        """Clear failed tasks.

        Args:
            before: Only clear tasks failed before this time. If None, clears all.

        Returns:
            Number of tasks deleted.
        """
        with self._autocommit() as session:
            stmt = delete(Task).where(Task.status == TaskStatus.FAILED)
            if before is not None:
                stmt = stmt.where(Task.completed_at < before)
            return session.connection().execute(stmt).rowcount

    def reap_stale_tasks(self, threshold: timedelta) -> int:
        """Mark stale RUNNING tasks as FAILED, or re-queue them.

        When a worker dies mid-execution (e.g. pod restart, OOM on the worker
        process), in-flight tasks stay RUNNING forever because no parent
        process remains to update their status. This method detects those
        orphaned rows and transitions them to FAILED.

        A stale row that holds a version parked by ``upsert()`` (see
        ``Task.requeue``) is re-queued as PENDING with that version instead
        of being failed, so the parked version is not lost.

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
            Number of tasks reaped (failed plus re-queued).
        """
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
        stale = (Task.status == TaskStatus.RUNNING, Task.started_at < stale_cutoff)
        fail = (
            update(Task)
            .where(*stale, Task.requeue.is_(None))
            .values(
                status=TaskStatus.FAILED,
                completed_at=func.now(),
                error=per_row_error,
            )
            .returning(Task.id, Task.name, Task.started_at, Task.max_runtime_seconds)
        )
        # Stale rows with a parked upsert are re-queued with the parked
        # version. A row is matched by one of the two statements under its
        # row lock, so an ``upsert()`` that parks a version in between
        # decides which one.
        requeue = (
            update(Task)
            .where(*stale, Task.requeue.is_not(None))
            .values(requeue_values())
            .returning(Task.id, Task.name)
        )
        with self._autocommit() as session:
            failed = session.execute(fail).all()
            requeued = session.execute(requeue).all()
        for task_id, name, started_at, max_runtime_seconds in failed:
            logger.warning(
                f"Reaped stale task '{name}' (id={task_id},"
                f" started_at={started_at},"
                f" max_runtime_seconds={max_runtime_seconds})"
            )
        for task_id, name in requeued:
            logger.warning(
                f"Reaped stale task (id={task_id}); re-queued as '{name}'"
                " by a parked upsert"
            )
        return len(failed) + len(requeued)

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
        explicit shutdown error. Interrupted tasks are not re-queued,
        unless an ``upsert()`` stored a new version while they ran: then the
        row is re-queued with that version.

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
