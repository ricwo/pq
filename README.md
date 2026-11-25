# pq

[![PyPI](https://img.shields.io/pypi/v/python-pq)](https://pypi.org/project/python-pq/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

Postgres-backed job queue for Python with fork-based worker isolation.

If you already run Postgres, you don't need Redis or RabbitMQ to process background jobs. pq uses `SELECT ... FOR UPDATE SKIP LOCKED` to turn your existing database into a reliable task queue. Enqueue in the same transaction as your writes, and process tasks in isolated child processes that can't crash your worker.

```python
from pq import PQ

pq = PQ("postgresql://localhost/mydb")
pq.run_db_migrations()

def send_email(to: str, subject: str) -> None:
    ...

pq.enqueue(send_email, to="user@example.com", subject="Hello")
pq.run_worker()
```

## Install

```bash
uv add python-pq
```

Or with pip:

```bash
pip install python-pq
```

Requires PostgreSQL and Python 3.13+.

## Features

- **Fork isolation** -- Each task runs in a forked child process. If it OOMs, segfaults, or crashes, the worker keeps running. Each child gets a fresh connection pool and opens its own database connections.
- **No extra infrastructure** -- Uses your existing Postgres. No broker to deploy, monitor, or lose data.
- **Transactional enqueueing** -- Enqueue tasks in the same database transaction as your writes. If the transaction rolls back, the task is never created.
- **Periodic tasks** -- Schedule with intervals (`timedelta`) or cron expressions. Control overlap, pause/resume without deleting.
- **Priority queues** -- Five levels from `BATCH` (0) to `CRITICAL` (100). Dedicate workers to specific priority tiers.
- **Lifecycle hooks** -- Run `pre_execute` / `post_execute` code in the forked child, safe for fork-unsafe libraries like OpenTelemetry.

## Tasks

### Enqueueing

Pass any importable function with its arguments:

```python
def greet(name: str) -> None:
    print(f"Hello, {name}!")

pq.enqueue(greet, name="World")
pq.enqueue(greet, "World")  # Positional args work too
```

### Delayed execution

```python
from datetime import datetime, timedelta, UTC

pq.enqueue(greet, "World", run_at=datetime.now(UTC) + timedelta(hours=1))
```

### Priority

```python
from pq import Priority

pq.enqueue(task, priority=Priority.CRITICAL)  # 100 - runs first
pq.enqueue(task, priority=Priority.HIGH)      # 75
pq.enqueue(task, priority=Priority.NORMAL)    # 50 (default)
pq.enqueue(task, priority=Priority.LOW)       # 25
pq.enqueue(task, priority=Priority.BATCH)     # 0 - runs last
```

### Cancellation

```python
task_id = pq.enqueue(my_task)
pq.cancel(task_id)  # True if the task was deleted
```

`cancel()` deletes a task that is waiting or finished. A task that is running is left alone and `cancel()` returns `False`: pq does not stop a run in progress.

### Client IDs

Use `client_id` for idempotency and lookups:

```python
pq.enqueue(process_order, order_id=123, client_id="order-123")

task = pq.get_task_by_client_id("order-123")
# Duplicate client_id raises IntegrityError
```

### Upsert

Insert or update a task by `client_id`. Useful for debouncing -- only the latest version runs:

```python
pq.upsert(send_email, to="a@b.com", client_id="welcome-email")

# Second call updates the existing task (resets to PENDING)
pq.upsert(send_email, to="new@b.com", client_id="welcome-email")
```

If the task is running, the new version is queued and runs after the current run ends, so one `client_id` does not run twice at the same time. This lets a task schedule its own continuation:

```python
def backfill(offset: int) -> None:
    more = process_rows(offset, limit=1000)
    if more:
        with PQ(DATABASE_URL) as client:
            # Runs after this run ends, never next to it
            client.upsert(backfill, offset=offset + 1000, client_id="backfill")
```

If the worker running the task dies, the queued version runs once another worker notices, about a minute later (see [Dead workers](#dead-workers)).

For a batch, `upsert_many()` takes a list of `Upsert` items and writes them in one transaction:

```python
from pq import Upsert

pq.upsert_many(
    [Upsert(task=send_email, kwargs={"to": to}, client_id=f"welcome-{to}") for to in emails]
)
```

## Periodic Tasks

### Intervals

```python
from datetime import timedelta

def heartbeat() -> None:
    print("alive")

pq.schedule(heartbeat, run_every=timedelta(minutes=5))
```

Call `schedule()` on every application start: it creates the schedule or updates it. A schedule whose `run_every` or `cron` did not change keeps its next run time, so a restart does not run the task again, and a cron time that came due during the restart still runs. A new schedule, one whose timing changed, and one that was inactive first run right away (`run_every`) or at the next cron time.

### Cron expressions

```python
pq.schedule(weekly_report, cron="0 9 * * 1")  # Monday 9am
```

### With arguments

```python
pq.schedule(report, run_every=timedelta(hours=1), report_type="hourly")
```

### Overlap control

By default, periodic tasks don't overlap -- if an instance is still running when the next tick arrives, the tick is skipped:

```python
# Default: max_concurrent=1, no overlap
pq.schedule(sync_inventory, run_every=timedelta(minutes=5))

# Allow unlimited concurrency
pq.schedule(fast_task, run_every=timedelta(seconds=30), max_concurrent=None)
```

### Pausing and resuming

```python
# Pause -- task stays in the database but won't run
pq.schedule(sync_inventory, run_every=timedelta(minutes=5), active=False)

# Resume
pq.schedule(sync_inventory, run_every=timedelta(minutes=5), active=True)
```

### Multiple schedules

Use `key` to register the same function with different configurations:

```python
pq.schedule(sync_data, run_every=timedelta(hours=1), key="us", region="us")
pq.schedule(sync_data, run_every=timedelta(hours=2), key="eu", region="eu")

pq.unschedule(sync_data, key="us")
```

## Workers

### Running

```python
pq.run_worker(poll_interval=1.0)           # Run forever
processed = pq.run_worker_once()            # Process single task (for testing)
```

`run_worker_once()` is one claim and one run. It has no heartbeat, cleanup, stale reaper or signal handling; a program that loops over it calls `pq.reap_stale_tasks()` itself.

### Stopping

On `SIGTERM` or `SIGINT` the worker stops claiming, waits up to `drain_timeout` seconds (default 20) for the tasks in flight, then kills what is still running and marks it failed. `pq.stop_worker()` starts the same shutdown from code. A worker that runs outside the main thread gets no signals, so there it is the only way:

```python
thread = threading.Thread(target=pq.run_worker)
thread.start()
...
pq.stop_worker()
thread.join()
```

`stop_worker()` also works before the worker has got going: the next `run_worker` in the process then stops as soon as it starts.

### Failed tasks

A failed task has `status == TaskStatus.FAILED`, the text of the failure in `error`, and its cause in `failure_kind`:

| `FailureKind` | Cause |
|---|---|
| `ERROR` | The task raised, or its process exited with an error |
| `TIMEOUT` | It ran past its max runtime |
| `OOM` | Its process was killed from outside with `SIGKILL`, most likely for memory |
| `KILLED` | Its process was ended by another signal |
| `SHUTDOWN` | The worker's drain ran out and killed it |
| `WORKER_LOST` | Its worker stopped sending heartbeats |
| `STALE` | It was still marked running past its stale window |

pq does not retry. To run tasks again that a deploy interrupted, select them by cause:

```python
from pq import FailureKind

for task in pq.list_failed():
    if task.failure_kind == FailureKind.SHUTDOWN:
        ...
```

### Dead workers

Each worker keeps a row in `pq_workers` and extends a lease there every 15 seconds. When a worker dies without shutting down (the machine is lost, the process is killed), its lease runs out after 60 seconds. Any other worker then fails the tasks it was running with `FailureKind.WORKER_LOST`, or re-queues them if an `upsert()` had queued a new version.

On Linux the kernel kills a task's process when its worker dies, so a task marked this way is not running any more. On other systems it can run on to its end.

A worker that is alive but cut off from the database for more than 60 seconds is taken for dead as well, by the workers that can reach it. Its tasks keep running. If one of them finishes and its row is still failed as `WORKER_LOST`, its real result replaces that. If your application re-queued the row in between, the task has run twice.

The stale reaper (`stale_task_timeout`, default 1 hour) remains for what the heartbeat cannot see: a task run by `run_worker_once()`, or a run whose result could not be written. A task counts as stale once it has been running for longer than `stale_task_timeout` and twice the max runtime of its run.

### Timeout

Kill tasks that run too long:

```python
pq.run_worker(max_runtime=300)  # 5 minute timeout per task
```

The task's process stops itself at `max_runtime`. If it cannot (it is blocked in native code, or the task changed its own `SIGALRM` handling), the worker kills it 10 seconds later, with anything it started. A `TimeoutError` raised by the task itself is an ordinary failure, with its traceback.

### Priority-dedicated workers

Reserve workers for high-priority tasks:

```python
from pq import Priority

# This worker only processes CRITICAL and HIGH
pq.run_worker(priorities={Priority.CRITICAL, Priority.HIGH})
```

### Lifecycle hooks

Run code before/after each task in the forked child process:

```python
from pq import PQ, Task, Periodic

def setup_tracing(task: Task | Periodic) -> None:
    print(f"Starting: {task.name}")

def flush_tracing(task: Task | Periodic, error: Exception | None) -> None:
    if error:
        print(f"Failed: {error}")

pq.run_worker(pre_execute=setup_tracing, post_execute=flush_tracing)
```

Hooks run in the forked child, making them safe for fork-unsafe resources like OpenTelemetry.

The task's result is settled when its function returns. `post_execute` runs after that and does not change it: if the hook raises, hangs or is killed, a task that finished stays `COMPLETED`. The hook has until 10 seconds after `max_runtime`, then the worker kills the process.

### Calling pq from a task

A task can use your application's `PQ` instance, for example to enqueue a follow-up task. Each forked child gets a fresh connection pool, opens its own connections, and closes them when the task ends. A child killed by its timeout or by a signal exits without closing them.

If your own code forks (with `os.fork()` or `multiprocessing`), fork outside an open `pq.session()`. A connection checked out at fork time stays shared between the two processes.

## Logging

pq uses [loguru](https://github.com/Delgan/loguru) internally and does **not** configure the logger on import. This means it inherits whatever logging setup the host application has already established.

If you are running pq as a standalone script and want pq's default log format, call `configure_logging()` explicitly during startup:

```python
from pq.logging import configure_logging

configure_logging()
```

## Serialization

Arguments are serialized automatically:

| Type | Method |
|------|--------|
| JSON-serializable (str, int, list, dict) | JSON |
| Pydantic models | `model_dump(mode="json")` → JSON (the task receives a dict) |
| Custom objects, lambdas, `NaN`, strings with a NUL character | dill (pickle) |

## Async tasks

Async handlers work without any changes:

```python
import httpx

async def fetch(url: str) -> None:
    async with httpx.AsyncClient() as client:
        response = await client.get(url)
        print(response.status_code)

pq.enqueue(fetch, "https://example.com")
```

## Error handling

Failed tasks are marked with status `FAILED` and the error is stored:

```python
for task in pq.list_failed():
    print(f"{task.name}: {task.error}")

pq.clear_failed(before=datetime.now(UTC) - timedelta(days=7))
pq.clear_completed(before=datetime.now(UTC) - timedelta(days=1))
```

## How it works

Every task runs in a forked child process:

```
Worker (parent)
    |
    +-- fork() -> Child executes task -> exits
    |             (OOM/crash only affects child)
    |
    +-- Continues processing next task
```

The parent monitors via `os.wait4()` and detects timeout, OOM (SIGKILL), and signal-based crashes. The child process exits after every task, giving you true memory isolation.

A child shares the worker's memory until it writes to it. The child freezes what it inherited for the garbage collector (`gc.freeze()`), so a collection in the child does not copy those pages: in a measurement with a 730 MB worker, one collection copied 390 MB without the freeze and 24 MB with it.

One-off tasks and periodic schedules form one queue: the highest priority runs first, and within a priority whichever came due first. A backlog of one-off tasks delays a schedule of the same priority no more than it delays a task that came due at the same time. Give a schedule a higher priority to keep it on time under load.

Multiple workers can run in parallel. Tasks are claimed atomically with PostgreSQL's `FOR UPDATE SKIP LOCKED`, so each task runs exactly once.

## Alternatives

There are good options in this space. pq makes different tradeoffs:

| | Broker | Isolation | Use case |
|---|---|---|---|
| **pq** | Postgres | Fork (process-per-task) | Teams already on Postgres who want fewer moving parts |
| **Celery** | Redis/RabbitMQ | Per-worker process | Large-scale, multi-language, established teams |
| **RQ** | Redis | Per-worker process | Simple Redis-based queues |
| **Dramatiq** | Redis/RabbitMQ | Per-worker process/thread | Celery alternative with better defaults |
| **ARQ** | Redis | Async (single process) | Async-first applications |
| **Procrastinate** | Postgres | Async (single process) | Async-first, Postgres-backed, Django integration |

pq is a good fit when:
- You already run Postgres and don't want to add Redis or RabbitMQ
- You want transactional enqueueing (enqueue atomically with your writes)
- You need true process isolation per task (OOM/crash safety)
- You want periodic tasks with overlap control, pause/resume, and cron

pq is not the right choice when:
- You need very high throughput (10,000+ jobs/second) -- use a dedicated broker
- You need cross-language workers -- Celery or a dedicated queue service is better
- You need complex workflows (DAGs, chaining, fan-out) -- look at Temporal or Prefect

## Documentation

Full docs at [ricwo.github.io/pq](https://ricwo.github.io/pq/).

## Development

```bash
make dev       # Start Postgres
uv run pytest  # Run tests
```

## License

MIT
