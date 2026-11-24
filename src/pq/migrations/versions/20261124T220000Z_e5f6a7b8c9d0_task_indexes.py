"""replace the pq_tasks status index with two partial indexes

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-11-24 22:00:00 Z

``ix_pq_tasks_status_priority_run_at`` (status, priority, run_at) could not
return the claim's order (``priority DESC, run_at``), so each claim read and
sorted every due task of the highest pending priority: 3.9 ms with 8,000 of
them, against 0.2 ms with the index below, on PostgreSQL 16 and 18.

``ix_pq_tasks_pending`` holds pending rows only, in claim order: a claim
reads its first entry. ``ix_pq_tasks_status_completed_at`` serves the other
lookups by status: the stale reaper (RUNNING), and cleanup and listings
(COMPLETED or FAILED, by ``completed_at``), which no longer read every
finished row to find the old ones.

Each index is built with a plain ``CREATE INDEX``, which blocks writes to
``pq_tasks`` while it runs: well under a second for a million rows.

``IF [NOT] EXISTS`` throughout: a table made by ``create_tables()`` has the
indexes of the version that made it, whatever revision it is stamped with.

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "e5f6a7b8c9d0"
down_revision: Union[str, Sequence[str], None] = "d4e5f6a7b8c9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the two partial indexes and drop the index they replace."""
    op.create_index(
        "ix_pq_tasks_pending",
        "pq_tasks",
        [sa.text("priority DESC"), "run_at"],
        postgresql_where=sa.text("status = 'PENDING'"),
        if_not_exists=True,
    )
    op.create_index(
        "ix_pq_tasks_status_completed_at",
        "pq_tasks",
        ["status", "completed_at"],
        postgresql_where=sa.text("status <> 'PENDING'"),
        if_not_exists=True,
    )
    op.drop_index(
        "ix_pq_tasks_status_priority_run_at", table_name="pq_tasks", if_exists=True
    )


def downgrade() -> None:
    """Restore the single (status, priority, run_at) index."""
    op.create_index(
        "ix_pq_tasks_status_priority_run_at",
        "pq_tasks",
        ["status", "priority", "run_at"],
        if_not_exists=True,
    )
    op.drop_index(
        "ix_pq_tasks_status_completed_at", table_name="pq_tasks", if_exists=True
    )
    op.drop_index("ix_pq_tasks_pending", table_name="pq_tasks", if_exists=True)
