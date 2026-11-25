# Database

## Setup

Run migrations once at application startup:

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.run_db_migrations()
```

This is safe to call multiple times - only pending migrations are applied.

## Tables

pq creates four tables with the `pq_` prefix:

| Table | Description |
|-------|-------------|
| `pq_tasks` | One-off tasks |
| `pq_periodic` | Periodic task schedules |
| `pq_workers` | Running workers and their heartbeat leases |
| `pq_schema_version` | Alembic migration tracking |

Migrate before you start workers of a new version. Workers of the previous version keep working on the migrated tables, so a rolling deploy is fine.

## Writing to the tables yourself

Reading the tables is safe. If your application also updates `pq_tasks` with its own statements, these rules hold for every row, and the database enforces them with check constraints:

| Rule | Constraint |
|------|------------|
| A `RUNNING` task has `started_at` | `ck_pq_tasks_running_has_started_at` |
| `requeue` is set only while the task is `RUNNING` | `ck_pq_tasks_requeue_only_while_running` |
| A `COMPLETED` or `FAILED` task has `completed_at` | `ck_pq_tasks_finished_has_completed_at` |
| A schedule has exactly one of `run_every` and `cron` | `ck_pq_periodic_interval_or_cron` |

The migration adds the constraints as `NOT VALID`: they apply to rows that are inserted or updated from then on, and the existing rows are not read. To have the old rows checked as well, run `ALTER TABLE pq_tasks VALIDATE CONSTRAINT <name>` for each.

These columns describe the run in progress and are cleared when it ends: `worker_id`, `worker_claimed_at`, `claimed_max_runtime_seconds` and, for the next version of the task, `requeue`. `failure_kind` is set on `FAILED` rows only. A statement of your own that moves a row to another status should clear them the way pq does.

Workers and clients of the release before these columns do not write or clear them. While some are still running, a row they finish or re-queue keeps the `failure_kind` it had. The orphan check is safe with them: it only acts on a `worker_id` written by the row's current claim.

## Alembic Integration

If your application uses Alembic for migrations, exclude pq tables from autogenerate to avoid conflicts:

```python title="migrations/env.py"
EXCLUDED_TABLE_PREFIXES = ("pq_",)


def include_name(name: str | None, type_: str, parent_names: dict[str, str]) -> bool:
    """Filter out tables managed by external libraries."""
    if type_ == "table" and name is not None:
        return not name.startswith(EXCLUDED_TABLE_PREFIXES)
    return True


# In run_migrations_online():
context.configure(
    connection=connection,
    target_metadata=target_metadata,
    include_name=include_name,  # Add this
)
```

This prevents Alembic from generating migrations for pq tables, which are managed by `pq.run_db_migrations()`.
