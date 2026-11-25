"""worker heartbeats, run state on the task row, and check constraints

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-11-25 12:00:00 Z

``pq_workers`` holds one row per running worker. The worker extends
``expires_at`` every few seconds; once it has passed, or the row is gone, the
tasks that carry the worker's id are orphaned and are failed or re-queued
without waiting for the stale window.

Four columns on ``pq_tasks``:

- ``worker_id``: the worker running the task, written by the claim.
- ``worker_claimed_at``: the ``started_at`` of the claim that wrote
  ``worker_id``. A worker of the previous release claims without writing
  either, so a ``worker_id`` it finds on the row is not taken for its own.
- ``claimed_max_runtime_seconds``: the max runtime this run has, written by
  the claim. The stale reaper of any worker goes by it, where it used to go
  by its own worker's default.
- ``failure_kind``: why a FAILED task failed (``pq.FailureKind``), next to
  the text in ``error``.

All four are nullable without a default, so adding them is a catalog-only
change. Rows from before the migration have NULL: no worker to check, the
reaper's own window, cause unknown.

The check constraints state what pq's statements already keep true. They are
added ``NOT VALID``: new and updated rows are checked, existing rows are not
read, so the migration does not scan the tables and does not fail on an old
row. ``ALTER TABLE ... VALIDATE CONSTRAINT`` checks the existing rows later
if wanted.

``IF [NOT] EXISTS`` throughout: a table made by ``create_tables()`` has the
columns and constraints of the version that made it, whatever revision it is
stamped with.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "f6a7b8c9d0e1"
down_revision: Union[str, Sequence[str], None] = "e5f6a7b8c9d0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_CHECKS: tuple[tuple[str, str, str], ...] = (
    (
        "pq_tasks",
        "ck_pq_tasks_running_has_started_at",
        "status <> 'RUNNING' OR started_at IS NOT NULL",
    ),
    (
        "pq_tasks",
        "ck_pq_tasks_requeue_only_while_running",
        "requeue IS NULL OR status = 'RUNNING'",
    ),
    (
        "pq_tasks",
        "ck_pq_tasks_finished_has_completed_at",
        "status NOT IN ('COMPLETED', 'FAILED') OR completed_at IS NOT NULL",
    ),
    (
        "pq_periodic",
        "ck_pq_periodic_interval_or_cron",
        "(run_every IS NULL) <> (cron IS NULL)",
    ),
)

_TASK_COLUMNS: tuple[tuple[str, str], ...] = (
    ("failure_kind", "VARCHAR(16)"),
    ("worker_id", "BIGINT"),
    ("worker_claimed_at", "TIMESTAMP WITH TIME ZONE"),
    ("claimed_max_runtime_seconds", "DOUBLE PRECISION"),
)


def upgrade() -> None:
    """Create ``pq_workers``, add the run columns and the constraints."""
    op.create_table(
        "pq_workers",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        if_not_exists=True,
    )
    for column, column_type in _TASK_COLUMNS:
        op.execute(
            f"ALTER TABLE pq_tasks ADD COLUMN IF NOT EXISTS {column} {column_type}"
        )
    for table, name, condition in _CHECKS:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
        op.execute(
            f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({condition}) NOT VALID"
        )


def downgrade() -> None:
    """Drop the constraints, the run columns and ``pq_workers``."""
    for table, name, _ in _CHECKS:
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {name}")
    for column, _ in _TASK_COLUMNS:
        op.execute(f"ALTER TABLE pq_tasks DROP COLUMN IF EXISTS {column}")
    op.drop_table("pq_workers", if_exists=True)
