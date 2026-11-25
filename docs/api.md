# API Reference

## PQ

::: pq.PQ
    options:
      show_source: false
      members:
        - __init__
        - run_db_migrations
        - enqueue
        - upsert
        - upsert_many
        - schedule
        - unschedule
        - cancel
        - run_worker
        - run_worker_once
        - stop_worker
        - reap_stale_tasks
        - pending_count
        - periodic_count
        - get_task
        - get_task_by_client_id
        - get_periodic_by_client_id
        - list_failed
        - list_completed
        - clear_failed
        - clear_completed
        - clear_all

## Upsert

::: pq.Upsert
    options:
      show_source: false

## Priority

::: pq.Priority
    options:
      show_source: false

## TaskStatus

::: pq.TaskStatus
    options:
      show_source: false

## FailureKind

::: pq.FailureKind
    options:
      show_source: false

## Models

::: pq.Task
    options:
      show_source: false

::: pq.Periodic
    options:
      show_source: false

## Worker Hooks

::: pq.PreExecuteHook
    options:
      show_source: false

::: pq.PostExecuteHook
    options:
      show_source: false

## Exceptions

pq raises no exceptions of its own for a task that fails: the outcome is on the task (`status`, `error`, `failure_kind`). `pq.TaskTimeoutError` and the other exception classes in `pq.worker` were never raised. They are deprecated and will be removed.
