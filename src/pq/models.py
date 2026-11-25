"""SQLAlchemy 2.0 models for PQ task queue."""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    ColumnElement,
    DateTime,
    Enum,
    Float,
    Identity,
    Index,
    Integer,
    Interval,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    case,
    cast,
    func,
    literal_column,
    null,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class TaskStatus(StrEnum):
    """Task execution status.

    The database stores the member names (``'PENDING'``), which is what
    hand-written SQL, the partial indexes and the check constraints use. The
    lowercase values are what ``.value`` gives, in log lines for example.
    """

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class FailureKind(StrEnum):
    """Why a task is FAILED, stored in ``Task.failure_kind``.

    ``Task.error`` holds the text for people. This is the same cause for
    code: ``task.failure_kind == FailureKind.SHUTDOWN``.
    """

    ERROR = "error"  # the task raised, or its process exited with an error
    TIMEOUT = "timeout"  # it ran past its max runtime
    OOM = "oom"  # its process was SIGKILLed from outside, most likely for memory
    KILLED = "killed"  # its process was ended by another signal
    SHUTDOWN = "shutdown"  # its worker's drain ran out and killed it
    STALE = "stale"  # still RUNNING past its stale window; reaped
    WORKER_LOST = "worker_lost"  # its worker stopped sending heartbeats; reaped


class Base(DeclarativeBase):
    """Base class for all models."""

    pass


class Task(Base):
    """One-off task with status tracking."""

    __tablename__ = "pq_tasks"
    __table_args__ = (
        # The claim: the first entry is the pending task to run next, so a
        # claim reads one entry however long the backlog is.
        Index(
            "ix_pq_tasks_pending",
            text("priority DESC"),
            "run_at",
            postgresql_where=text("status = 'PENDING'"),
        ),
        # Everything else that looks rows up by status: the stale reaper
        # (RUNNING), and cleanup and listings (COMPLETED or FAILED, by
        # ``completed_at``).
        Index(
            "ix_pq_tasks_status_completed_at",
            "status",
            "completed_at",
            postgresql_where=text("status <> 'PENDING'"),
        ),
        # What the statements in pq keep true and its code relies on. A
        # statement written outside pq has to keep them true as well.
        # ``started_at`` identifies a run and is the stale reaper's cutoff.
        CheckConstraint(
            "status <> 'RUNNING' OR started_at IS NOT NULL",
            name="ck_pq_tasks_running_has_started_at",
        ),
        # A parked version belongs to the run in progress.
        CheckConstraint(
            "requeue IS NULL OR status = 'RUNNING'",
            name="ck_pq_tasks_requeue_only_while_running",
        ),
        # Cleanup and the listings filter and sort on ``completed_at``.
        CheckConstraint(
            "status NOT IN ('COMPLETED', 'FAILED') OR completed_at IS NOT NULL",
            name="ck_pq_tasks_finished_has_completed_at",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    client_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, unique=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status", create_constraint=True),
        nullable=False,
        default=TaskStatus.PENDING,
    )
    run_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Why a FAILED task failed: one of ``FailureKind``'s values. NULL in
    # every other status, and on rows that failed before the column existed.
    failure_kind: Mapped[str | None] = mapped_column(String(16), nullable=True)
    # How often the row was claimed since it was last queued. pq does not
    # retry, so it writes 0 or 1 here: the claim adds one, and every way back
    # to PENDING that pq offers resets it.
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Per-task override of the worker-level ``max_runtime``. When NULL
    # (the common case), the worker uses its configured default. When
    # set, it caps this specific task's wall-clock at the given number
    # of seconds AND extends the stale-reaper threshold proportionally
    # (the reaper picks the larger of its global default and
    # ``max_runtime_seconds * 2``). Useful for occasionally-long tasks
    # in a fleet whose default is sized for typical short work.
    max_runtime_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    # Next version of this task, parked by an ``upsert()`` that arrived while
    # the row was RUNNING: name, payload, priority, run_at (ISO 8601) and
    # max_runtime_seconds. Whoever ends the current run (worker or reaper)
    # re-queues the row with it instead of closing the row. NULL when no
    # upsert is parked (the common case). ``none_as_null`` makes the ORM
    # write SQL NULL for ``None`` (plain ``JSONB`` writes JSON ``null``,
    # which ``IS NULL`` does not match).
    requeue: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB(none_as_null=True), nullable=True
    )
    # The run in progress, written by the claim and cleared when the run
    # ends. ``worker_id`` is the ``pq_workers`` row of the worker running it
    # (NULL for ``run_worker_once``, which has none). No foreign key: a
    # worker that is gone is exactly what the orphan check looks for.
    worker_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    # The max runtime this run has: the task's own ``max_runtime_seconds``,
    # or the default of the worker that claimed it. The stale reaper of every
    # worker goes by this value.
    claimed_max_runtime_seconds: Mapped[float | None] = mapped_column(
        Float, nullable=True
    )


def requeue_values() -> dict[str, ColumnElement[Any]]:
    """``UPDATE`` values that turn a RUNNING row into a fresh PENDING task
    from the version parked in ``Task.requeue``.

    For a statement whose ``WHERE`` keeps rows with ``requeue IS NOT NULL``,
    or that picks these values per row with ``CASE``. Applied in SQL so that
    ending a run is one statement: the row lock it takes serializes it with a
    concurrent ``upsert()``.
    """
    spec = Task.requeue
    return {
        "name": spec["name"].astext,
        "payload": spec["payload"],
        "priority": spec["priority"].astext.cast(SmallInteger),
        "run_at": spec["run_at"].astext.cast(DateTime(timezone=True)),
        "max_runtime_seconds": spec["max_runtime_seconds"].astext.cast(Float),
        # Cast: in a ``CASE`` next to another literal, Postgres would type a
        # bare ``'PENDING'`` as text, which the enum column does not accept.
        "status": cast(literal_column("'PENDING'"), Task.status.type),
        "requeue": null(),
        **fresh_run_values(),
    }


def fresh_run_values() -> dict[str, ColumnElement[Any]]:
    """``UPDATE`` values that clear every trace of earlier runs, for a row
    that goes back to PENDING (``requeue_values``, ``upsert()``)."""
    return {
        "attempts": literal_column("0"),
        "started_at": null(),
        "completed_at": null(),
        "error": null(),
        "failure_kind": cast(null(), Task.failure_kind.type),
        **run_ended_values(),
    }


def run_ended_values() -> dict[str, ColumnElement[Any]]:
    """``UPDATE`` values that clear what a claim wrote about its run. Part
    of every statement that takes a row out of RUNNING.

    Cast: a bare ``NULL`` on both sides of a ``CASE`` is typed as text.
    """
    return {
        "worker_id": cast(null(), Task.worker_id.type),
        "claimed_max_runtime_seconds": cast(
            null(), Task.claimed_max_runtime_seconds.type
        ),
    }


def end_run_values(closed: dict[str, ColumnElement[Any]]) -> dict[str, Any]:
    """``UPDATE`` values that end a run, picked per row under the row lock
    the statement takes: ``closed`` (the columns of a finished row), or, if
    an ``upsert()`` parked a new version on the row, that version as a fresh
    PENDING task (``requeue_values``).
    """
    closed = {**run_ended_values(), **closed}
    return {
        column: case(
            (Task.requeue.is_(None), closed.get(column, Task.__table__.c[column])),
            else_=requeued,
        )
        for column, requeued in requeue_values().items()
    }


class Periodic(Base):
    """Recurring task with interval or cron scheduling."""

    __tablename__ = "pq_periodic"
    __table_args__ = (
        Index("ix_pq_periodic_priority_next_run", "priority", "next_run"),
        UniqueConstraint("name", "key"),
        # A schedule is an interval or a cron expression. With neither, its
        # claim could not compute the next run.
        CheckConstraint(
            "(run_every IS NULL) <> (cron IS NULL)",
            name="ck_pq_periodic_interval_or_cron",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    client_id: Mapped[str | None] = mapped_column(
        String(255), nullable=True, unique=True, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    key: Mapped[str] = mapped_column(String(255), nullable=False, server_default="")
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    run_every: Mapped[timedelta | None] = mapped_column(Interval, nullable=True)
    cron: Mapped[str | None] = mapped_column(String(100), nullable=True)
    next_run: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    max_concurrent: Mapped[int | None] = mapped_column(SmallInteger, nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    # Per-task override of the worker-level ``max_runtime``. Same semantics
    # as ``Task.max_runtime_seconds`` — NULL means "use the worker's
    # configured default", a set value caps this specific schedule's
    # wall-clock at the given number of seconds and also extends the
    # ``locked_until`` window (when ``max_concurrent`` is in effect) so
    # the lock doesn't expire while the task is legitimately still
    # running. Periodic tasks are not subject to the stale-task reaper
    # (they're guarded by ``locked_until`` and the natural re-fire of
    # the schedule), so this knob is purely about the per-execution
    # wall-clock cap.
    max_runtime_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_run: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Worker(Base):
    """A running worker (``run_worker``) and its heartbeat.

    The worker extends ``expires_at`` every few seconds. Once it has passed,
    or the row is gone, the worker counts as dead and the tasks that carry
    its id as orphaned. A worker deletes its row when it stops.
    """

    __tablename__ = "pq_workers"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
