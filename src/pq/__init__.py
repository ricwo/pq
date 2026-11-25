"""PQ - Postgres-backed task queue."""

import warnings

from pq.client import PQ, Upsert
from pq.models import FailureKind, Periodic, Task, TaskStatus
from pq.priority import Priority
from pq.worker import PostExecuteHook, PreExecuteHook

__version__ = "0.9.2"

__all__ = [
    "PQ",
    "FailureKind",
    "Periodic",
    "PostExecuteHook",
    "PreExecuteHook",
    "Priority",
    "Task",
    "TaskStatus",
    "Upsert",
]


def __getattr__(name: str) -> type[Exception]:
    if name == "TaskTimeoutError":
        from pq import worker

        warnings.warn(
            "pq.TaskTimeoutError is deprecated and will be removed: pq never"
            " raises it. A task that timed out has"
            " Task.failure_kind == FailureKind.TIMEOUT.",
            DeprecationWarning,
            stacklevel=2,
        )
        return worker._TaskTimeoutError
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
