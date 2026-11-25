"""Tests for the gaps in pq's implicit state machines (ricwo/pq#33).

One class per gap. Each test fails on the code before the fix.
"""

import inspect
import os
import signal
import subprocess
import sys
import textwrap
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.exc import IntegrityError

import pq as pq_package
from pq import worker
from pq.client import PQ
from pq.models import FailureKind, Periodic, Task, TaskStatus, Worker
from pq.worker import _ChildResult, _Heartbeat

from tests.test_audit import REPO_ROOT, _is_gone, _sigterm_self_after, _task


# --- Handlers (module level: the worker imports them by path) ---


def noop() -> None:
    pass


def fail() -> None:
    raise ValueError("boom")


def sleep_for(seconds: float) -> None:
    time.sleep(seconds)


def sleep_then_touch(path: str, seconds: float) -> None:
    time.sleep(seconds)
    Path(path).write_text("done")


def signal_self(signum: int) -> None:
    os.kill(os.getpid(), signum)
    time.sleep(30)


def write_own_row(db_url: str, client_id: str, path: str) -> None:
    """Write what the task's own row and its worker's row say while it runs."""
    with PQ(db_url) as client, client.session() as session:
        worker_id, budget = session.execute(
            select(Task.worker_id, Task.claimed_max_runtime_seconds).where(
                Task.client_id == client_id
            )
        ).one()
        lease = session.execute(
            select(Worker.expires_at > text("now()")).where(Worker.id == worker_id)
        ).scalar_one_or_none()
    Path(path).write_text(f"{worker_id is not None} {budget} {lease}")


def write_pid_then_sleep(path: str, seconds: float) -> None:
    Path(path).write_text(str(os.getpid()))
    time.sleep(seconds)


# --- Helpers ---


def _claim(pq: PQ, *, max_runtime: float = 1800, worker_id: int | None = None) -> Task:
    """Claim the next task the way a worker does, without running it."""
    claim = worker._try_claim(
        pq, max_runtime=max_runtime, priorities=(), worker_id=worker_id
    )
    assert claim is not None and isinstance(claim.row, Task)
    return claim.row


def _age_run(pq: PQ, task_id: int, age: timedelta) -> None:
    with pq.session() as session:
        session.execute(
            text("UPDATE pq_tasks SET started_at = now() - :age WHERE id = :id"),
            {"age": age, "id": task_id},
        )


def _add_worker(pq: PQ, *, lease: timedelta) -> int:
    """A ``pq_workers`` row whose lease ends ``lease`` from now (negative:
    it has run out)."""
    with pq.session() as session:
        return session.execute(
            text(
                "INSERT INTO pq_workers (hostname, pid, expires_at)"
                " VALUES ('test', 1, now() + :lease) RETURNING id"
            ),
            {"lease": lease},
        ).scalar_one()


def _worker_rows(pq: PQ) -> int:
    with pq.session() as session:
        return session.execute(text("SELECT count(*) FROM pq_workers")).scalar_one()


@pytest.fixture(autouse=True)
def _no_workers_left(pq: PQ) -> None:
    with pq.session() as session:
        session.execute(text("DELETE FROM pq_workers"))


# --- G1: the stale reaper and runs with a longer max runtime ---


class TestStaleReaperUsesTheRunsMaxRuntime:
    def test_claim_records_the_max_runtime_of_the_run(self, pq: PQ) -> None:
        pq.enqueue(noop, client_id="default")
        assert _claim(pq, max_runtime=14400).claimed_max_runtime_seconds == 14400

        pq.enqueue(noop, client_id="own", max_runtime_seconds=5)
        assert _claim(pq, max_runtime=14400).claimed_max_runtime_seconds == 5

    def test_short_window_spares_the_run_of_a_worker_with_a_longer_max_runtime(
        self, pq: PQ
    ) -> None:
        """A worker with a 1 h stale window failed every run older than
        that, including the two-hour-old run of a worker whose max runtime
        is 4 h."""
        task_id = pq.enqueue(noop)
        _claim(pq, max_runtime=4 * 3600)
        _age_run(pq, task_id, timedelta(hours=2))

        assert pq.reap_stale_tasks(timedelta(hours=1)) == 0
        assert _task(pq, task_id).status == TaskStatus.RUNNING

        # Past twice its own max runtime it is stale for every reaper.
        _age_run(pq, task_id, timedelta(hours=9))
        assert pq.reap_stale_tasks(timedelta(hours=1)) == 1
        task = _task(pq, task_id)
        assert task.status == TaskStatus.FAILED
        assert task.failure_kind == FailureKind.STALE
        assert "max runtime of the run=14400," in (task.error or "")

    def test_end_of_run_clears_what_the_claim_wrote(self, pq: PQ) -> None:
        task_id = pq.enqueue(noop)
        pq.run_worker_once(max_runtime=77)
        task = _task(pq, task_id)
        assert task.status == TaskStatus.COMPLETED
        assert task.claimed_max_runtime_seconds is None
        assert task.worker_id is None

    def test_unusable_worker_max_runtime_is_not_recorded(self, pq: PQ) -> None:
        """An infinite max runtime on the row would break the reaper's
        statement for every row."""
        task_id = pq.enqueue(noop)
        _claim(pq, max_runtime=float("inf"))
        assert _task(pq, task_id).claimed_max_runtime_seconds is None
        assert pq.reap_stale_tasks(timedelta(hours=1)) == 0


# --- G2: cancel() and a running task ---


class TestCancel:
    def test_cancel_leaves_a_running_task_alone(self, pq: PQ) -> None:
        """``cancel()`` deleted the row of a RUNNING task and returned True.
        The run went on, and the version parked on the row was lost."""
        task_id = pq.upsert(noop, client_id="job")
        _claim(pq)
        pq.upsert(noop, client_id="job", priority=pq_package.Priority.HIGH)

        assert pq.cancel(task_id) is False

        task = _task(pq, task_id)
        assert task.status == TaskStatus.RUNNING
        assert task.requeue is not None

    def test_cancel_deletes_a_task_that_is_not_running(self, pq: PQ) -> None:
        pending = pq.enqueue(noop)
        failed = pq.enqueue(fail)
        pq.run_worker_once()
        pq.run_worker_once()
        states = {_task(pq, pending).status, _task(pq, failed).status}
        assert states == {TaskStatus.COMPLETED, TaskStatus.FAILED}

        waiting = pq.enqueue(noop, run_at=datetime.now(UTC) + timedelta(hours=1))
        for task_id in (pending, failed, waiting):
            assert pq.cancel(task_id) is True
            assert pq.get_task(task_id) is None
        assert pq.cancel(waiting) is False


# --- G3: orphaned RUNNING rows ---


class TestHeartbeat:
    def test_worker_holds_a_lease_and_stamps_its_tasks(
        self, pq: PQ, db_url: str, tmp_path: Path
    ) -> None:
        seen = tmp_path / "seen"
        task_id = pq.enqueue(
            write_own_row, db_url, "stamped", str(seen), client_id="stamped"
        )
        with _sigterm_self_after(1.5):
            worker.run_worker(pq, poll_interval=0.05, max_runtime=123)

        # While it ran: a worker id, the worker's max runtime, a live lease.
        assert seen.read_text() == "True 123.0 True"
        # Afterwards the run's state is cleared and the worker's row is gone.
        task = _task(pq, task_id)
        assert task.status == TaskStatus.COMPLETED
        assert task.worker_id is None
        assert _worker_rows(pq) == 0

    def test_run_worker_once_has_no_worker_id(self, pq: PQ) -> None:
        pq.enqueue(noop)
        assert _claim(pq).worker_id is None
        assert _worker_rows(pq) == 0

    def test_tasks_of_dead_workers_are_failed_and_others_left_alone(
        self, pq: PQ
    ) -> None:
        dead = _add_worker(pq, lease=timedelta(seconds=-1))
        alive = _add_worker(pq, lease=timedelta(minutes=1))
        ids: dict[str, int] = {}
        for name, worker_id in (
            ("dead", dead),
            ("gone", dead + alive + 1000),  # no such row
            ("alive", alive),
            ("none", None),
        ):
            ids[name] = pq.enqueue(noop, client_id=name)
            assert _claim(pq, worker_id=worker_id).client_id == name

        _Heartbeat(pq)._reap_orphans()

        for name in ("dead", "gone"):
            task = _task(pq, ids[name])
            assert task.status == TaskStatus.FAILED, name
            assert task.failure_kind == FailureKind.WORKER_LOST
            assert (task.error or "").startswith("Reaped: task still RUNNING")
            assert task.completed_at is not None
            assert task.worker_id is None
        for name in ("alive", "none"):
            assert _task(pq, ids[name]).status == TaskStatus.RUNNING, name

    def test_orphan_with_a_parked_version_is_requeued(self, pq: PQ) -> None:
        dead = _add_worker(pq, lease=timedelta(seconds=-1))
        task_id = pq.upsert(noop, client_id="job")
        _claim(pq, worker_id=dead)
        pq.upsert(sleep_for, 0.0, client_id="job")

        _Heartbeat(pq)._reap_orphans()

        task = _task(pq, task_id)
        assert task.status == TaskStatus.PENDING
        assert task.name.endswith(":sleep_for")
        assert task.requeue is None
        assert task.worker_id is None and task.failure_kind is None

    def test_no_worker_is_called_dead_before_this_one_was_connected_for_a_lease(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After an outage of the database every lease has run out. The
        first worker back must not fail the tasks of the others, which are
        alive and about to renew."""
        monkeypatch.setattr(worker, "WORKER_TIMEOUT_SECONDS", 0.6)
        monkeypatch.setattr(worker, "HEARTBEAT_INTERVAL_SECONDS", 0.1)
        dead = _add_worker(pq, lease=timedelta(seconds=-1))
        task_id = pq.enqueue(noop)
        _claim(pq, worker_id=dead)

        heartbeat = _Heartbeat(pq)
        heartbeat.run_due()  # first beat: connected since now
        assert _task(pq, task_id).status == TaskStatus.RUNNING

        deadline = time.monotonic() + 0.4
        while time.monotonic() < deadline:
            heartbeat.run_due()
            time.sleep(0.02)
        assert _task(pq, task_id).status == TaskStatus.RUNNING  # 0.4 s < lease

        # A gap in its own beats (it was cut off as well) starts the count again.
        time.sleep(0.3)
        heartbeat.run_due()
        time.sleep(0.4)
        heartbeat.run_due()
        assert _task(pq, task_id).status == TaskStatus.RUNNING

        # Connected for a whole lease from here on.
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline:
            heartbeat.run_due()
            time.sleep(0.02)
        assert _task(pq, task_id).failure_kind == FailureKind.WORKER_LOST

    def test_heartbeat_restores_a_row_that_was_deleted(self, pq: PQ) -> None:
        heartbeat = _Heartbeat(pq)
        heartbeat.run_due()
        worker_id = heartbeat.worker_id
        assert worker_id is not None and _worker_rows(pq) == 1

        with pq.session() as session:
            session.execute(text("DELETE FROM pq_workers"))
        heartbeat.next_beat = 0.0
        heartbeat.run_due()
        assert heartbeat.worker_id == worker_id
        with pq.session() as session:
            assert session.execute(select(Worker.id)).scalar_one() == worker_id

        heartbeat.close()
        assert _worker_rows(pq) == 0

    def test_failed_heartbeat_does_not_stop_the_worker(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        def broken(*, registered: bool) -> object:
            raise RuntimeError("database went away")

        monkeypatch.setattr(worker, "_heartbeat_statement", broken)
        marker = tmp_path / "done"
        task_id = pq.enqueue(sleep_then_touch, str(marker), 0.2)
        with _sigterm_self_after(1.5):
            worker.run_worker(pq, poll_interval=0.05)
        assert marker.exists()
        task = _task(pq, task_id)
        assert task.status == TaskStatus.COMPLETED

    def test_lease_is_kept_up_during_the_drain(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The tasks in flight are still running during the drain. A worker
        that stopped its heartbeat there would have them failed as orphaned
        by the others."""
        monkeypatch.setattr(worker, "HEARTBEAT_INTERVAL_SECONDS", 0.1)
        beats: list[float] = []
        original = _Heartbeat._beat

        def counting(self: _Heartbeat) -> bool:
            beats.append(time.monotonic())
            return original(self)

        monkeypatch.setattr(_Heartbeat, "_beat", counting)
        pq.enqueue(sleep_for, 1.5)
        started = time.monotonic()
        with _sigterm_self_after(0.4):
            worker.run_worker(pq, poll_interval=0.05, drain_timeout=10)
        assert [b for b in beats if b - started > 1.0], "no beat late in the drain"

    def test_late_result_replaces_the_verdict_of_the_orphan_check(self, pq: PQ) -> None:
        """A worker that could not reach the database for a while is taken
        for dead. Its run was alive all along and reports in the end."""
        dead = _add_worker(pq, lease=timedelta(seconds=-1))
        task_id = pq.enqueue(noop)
        claimed = _claim(pq, worker_id=dead)
        _Heartbeat(pq)._reap_orphans()
        assert _task(pq, task_id).failure_kind == FailureKind.WORKER_LOST

        worker._finish_one_off(
            pq, task_id, claimed.started_at, claimed.name, _ChildResult(None)
        )

        task = _task(pq, task_id)
        assert task.status == TaskStatus.COMPLETED
        assert task.failure_kind is None and task.error is None

    def test_late_result_does_not_touch_a_row_that_was_requeued_or_is_stale(
        self, pq: PQ
    ) -> None:
        # Re-queued after the verdict: the row belongs to the next run.
        dead = _add_worker(pq, lease=timedelta(seconds=-1))
        requeued = pq.upsert(noop, client_id="requeued")
        first = _claim(pq, worker_id=dead)
        _Heartbeat(pq)._reap_orphans()
        pq.upsert(noop, client_id="requeued")
        worker._finish_one_off(
            pq, requeued, first.started_at, first.name, _ChildResult(None)
        )
        assert _task(pq, requeued).status == TaskStatus.PENDING
        pq.cancel(requeued)

        # The stale reaper's verdict stands: past twice its max runtime no
        # run is alive.
        stale = pq.enqueue(noop)
        claimed = _claim(pq)
        with pq.session() as session:
            started_at = session.execute(
                update(Task)
                .where(Task.id == stale)
                .values(started_at=datetime.now(UTC) - timedelta(hours=3))
                .returning(Task.started_at)
            ).scalar_one()
        assert pq.reap_stale_tasks(timedelta(hours=1)) == 1
        worker._finish_one_off(pq, stale, started_at, claimed.name, _ChildResult(None))
        assert _task(pq, stale).failure_kind == FailureKind.STALE

    @pytest.mark.skipif(not sys.platform.startswith("linux"), reason="Linux only")
    def test_task_dies_with_its_worker_and_is_failed_by_another(
        self, pq: PQ, db_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A worker killed with SIGKILL left its task's process running,
        in its own process group, and the row RUNNING for an hour."""
        pid_file = tmp_path / "pid"
        task_id = pq.enqueue(write_pid_then_sleep, str(pid_file), 30.0)
        script = textwrap.dedent(
            """
            import os
            from pq import PQ, worker
            worker.WORKER_TIMEOUT_SECONDS = 1.0
            worker.HEARTBEAT_INTERVAL_SECONDS = 0.2
            PQ(os.environ["PQ_TEST_URL"]).run_worker(poll_interval=0.05)
            """
        )
        doomed = subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=REPO_ROOT,
            env={**os.environ, "PQ_TEST_URL": db_url},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            deadline = time.monotonic() + 20
            while not pid_file.exists() or not pid_file.read_text():
                assert time.monotonic() < deadline, "task never started"
                time.sleep(0.05)
            task_pid = int(pid_file.read_text())
            assert _task(pq, task_id).worker_id is not None
        finally:
            doomed.kill()
            doomed.wait()

        deadline = time.monotonic() + 5
        while not _is_gone(task_pid) and time.monotonic() < deadline:
            time.sleep(0.05)
        assert _is_gone(task_pid), "the task outlived its worker"

        monkeypatch.setattr(worker, "WORKER_TIMEOUT_SECONDS", 1.0)
        monkeypatch.setattr(worker, "HEARTBEAT_INTERVAL_SECONDS", 0.2)
        with _sigterm_self_after(4.0):
            worker.run_worker(pq, poll_interval=0.05)
        task = _task(pq, task_id)
        assert task.status == TaskStatus.FAILED
        assert task.failure_kind == FailureKind.WORKER_LOST


# --- G4: the cause of a failure ---


class TestFailureKind:
    def test_completed_task_has_none(self, pq: PQ) -> None:
        task_id = pq.enqueue(noop)
        pq.run_worker_once()
        assert _task(pq, task_id).failure_kind is None

    def test_error(self, pq: PQ) -> None:
        task_id = pq.enqueue(fail)
        pq.run_worker_once()
        task = _task(pq, task_id)
        assert task.failure_kind == FailureKind.ERROR
        assert "ValueError: boom" in (task.error or "")

    def test_timeout(self, pq: PQ) -> None:
        task_id = pq.enqueue(sleep_for, 30.0)
        pq.run_worker_once(max_runtime=1)
        assert _task(pq, task_id).failure_kind == FailureKind.TIMEOUT

    def test_killed_from_outside(self, pq: PQ) -> None:
        oom = pq.enqueue(signal_self, int(signal.SIGKILL))
        pq.run_worker_once()
        assert _task(pq, oom).failure_kind == FailureKind.OOM

        killed = pq.enqueue(signal_self, int(signal.SIGTERM))
        pq.run_worker_once()
        assert _task(pq, killed).failure_kind == FailureKind.KILLED

    def test_shutdown(self, pq: PQ) -> None:
        task_id = pq.enqueue(sleep_for, 30.0)
        with _sigterm_self_after(0.5):
            worker.run_worker(pq, poll_interval=0.05, drain_timeout=0.2)
        task = _task(pq, task_id)
        assert task.failure_kind == FailureKind.SHUTDOWN
        assert (task.error or "").startswith("Task interrupted by worker shutdown")

    def test_queueing_the_row_again_clears_it(self, pq: PQ) -> None:
        task_id = pq.upsert(fail, client_id="job")
        pq.run_worker_once()
        assert _task(pq, task_id).failure_kind == FailureKind.ERROR

        pq.upsert(noop, client_id="job")
        assert _task(pq, task_id).failure_kind is None

    def test_claim_clears_what_another_writer_left(self, pq: PQ) -> None:
        """An application that re-queues a FAILED row with its own UPDATE
        does not know the column."""
        task_id = pq.enqueue(fail)
        pq.run_worker_once()
        with pq.session() as session:
            session.execute(
                update(Task)
                .where(Task.id == task_id)
                .values(status=TaskStatus.PENDING, completed_at=None, error=None)
            )
        assert _claim(pq).failure_kind is None


# --- G5: the post-execute hook and the task's result ---


def _slow_hook(task: Task | Periodic, error: Exception | None) -> None:
    time.sleep(3)


def _hook_that_never_returns(task: Task | Periodic, error: Exception | None) -> None:
    time.sleep(60)


class TestPostExecuteDoesNotChangeTheResult:
    def test_slow_hook_after_a_finished_task(self, pq: PQ) -> None:
        """The child's alarm stayed armed through ``post_execute``: a task
        that returned in time was recorded as timed out when the hook ran
        past the max runtime."""
        task_id = pq.enqueue(noop)
        pq.run_worker_once(max_runtime=1, post_execute=_slow_hook)
        task = _task(pq, task_id)
        assert task.status == TaskStatus.COMPLETED
        assert task.error is None

    def test_hook_killed_at_the_workers_deadline(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(worker, "KILL_GRACE_SECONDS", 0.5)
        done = pq.enqueue(noop)
        start = time.monotonic()
        pq.run_worker_once(max_runtime=1, post_execute=_hook_that_never_returns)
        assert time.monotonic() - start < 10
        assert _task(pq, done).status == TaskStatus.COMPLETED

        failed = pq.enqueue(fail)
        pq.run_worker_once(max_runtime=1, post_execute=_hook_that_never_returns)
        task = _task(pq, failed)
        assert task.failure_kind == FailureKind.ERROR
        assert "ValueError: boom" in (task.error or "")

    def test_task_that_runs_too_long_still_times_out(self, pq: PQ) -> None:
        task_id = pq.enqueue(sleep_for, 30.0)
        pq.run_worker_once(max_runtime=1, post_execute=_slow_hook)
        task = _task(pq, task_id)
        assert task.failure_kind == FailureKind.TIMEOUT
        assert (task.error or "").startswith("Timed out after")


# --- G6: stopping a worker from code ---


class TestStopWorker:
    def test_worker_in_a_thread_is_stopped_and_drains(
        self, pq: PQ, tmp_path: Path, log_records: list
    ) -> None:
        """Outside the main thread no signal reaches the worker, and
        nothing else could ask it to stop."""
        marker = tmp_path / "done"
        task_id = pq.enqueue(sleep_then_touch, str(marker), 0.8)
        thread = threading.Thread(
            target=lambda: pq.run_worker(poll_interval=0.05), daemon=True
        )
        thread.start()
        deadline = time.monotonic() + 10
        while _task(pq, task_id).status != TaskStatus.RUNNING:
            assert time.monotonic() < deadline
            time.sleep(0.02)

        pq.stop_worker()
        thread.join(timeout=10)

        assert not thread.is_alive()
        assert marker.exists()
        assert _task(pq, task_id).status == TaskStatus.COMPLETED
        assert any("stop_worker()" in r["message"] for r in log_records)

    def test_stop_worker_without_a_worker_does_nothing(
        self, pq: PQ, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A request with no worker to take it must not linger: it would
        make the next ``run_worker_once`` kill its task as a shutdown."""
        monkeypatch.setattr(worker._shutdown, "drain_timeout", 0.0)
        pq.stop_worker()
        time.sleep(0.05)
        once = pq.enqueue(sleep_for, 0.3)
        pq.run_worker_once()
        assert _task(pq, once).status == TaskStatus.COMPLETED

        task_id = pq.enqueue(noop)
        with _sigterm_self_after(1.0):
            worker.run_worker(pq, poll_interval=0.05)
        assert _task(pq, task_id).status == TaskStatus.COMPLETED


# --- G7: exception classes that are never raised ---


class TestDeprecatedExceptions:
    @pytest.mark.parametrize(
        "name",
        [
            "WorkerError",
            "TaskTimeoutError",
            "TaskOOMError",
            "TaskKilledError",
            "TaskInterruptedError",
        ],
    )
    def test_worker_names_warn(self, name: str) -> None:
        with pytest.warns(DeprecationWarning, match=name):
            cls = getattr(worker, name)
        assert issubclass(cls, Exception) and cls.__name__ == name

    def test_package_name_warns(self) -> None:
        with pytest.warns(DeprecationWarning, match="TaskTimeoutError"):
            cls = pq_package.TaskTimeoutError
        assert issubclass(cls, Exception)
        assert "TaskTimeoutError" not in pq_package.__all__

    def test_importing_pq_does_not_warn(self) -> None:
        result = subprocess.run(
            [sys.executable, "-W", "error", "-c", "import pq, pq.worker"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr


# --- G8: PQ.run_worker and the arguments of pq.worker.run_worker ---


def _touch_before(task: Task | Periodic) -> None:
    Path(os.environ["PQ_TEST_HOOK_DIR"], "pre").write_text(task.name)


def _touch_after(task: Task | Periodic, error: Exception | None) -> None:
    Path(os.environ["PQ_TEST_HOOK_DIR"], "post").write_text(repr(error))


class TestClientPassesWorkerArguments:
    @pytest.mark.parametrize("name", ["run_worker", "run_worker_once"])
    def test_same_arguments_and_defaults(self, name: str) -> None:
        """The README passed ``pre_execute`` to ``pq.run_worker``, which did
        not take it."""
        on_client = dict(inspect.signature(getattr(PQ, name)).parameters)
        in_worker = dict(inspect.signature(getattr(worker, name)).parameters)
        del on_client["self"], in_worker["pq"]
        assert list(on_client) == list(in_worker)
        for parameter, client_side in on_client.items():
            assert client_side.default == in_worker[parameter].default, parameter

    def test_hooks_run(
        self, pq: PQ, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PQ_TEST_HOOK_DIR", str(tmp_path))
        pq.enqueue(noop)
        assert pq.run_worker_once(pre_execute=_touch_before, post_execute=_touch_after)
        assert (tmp_path / "pre").read_text().endswith(":noop")
        assert (tmp_path / "post").read_text() == "None"


# --- G10: invariants the schema now states ---


class TestCheckConstraints:
    @pytest.mark.parametrize(
        ("columns", "values"),
        [
            ("status", "'RUNNING'"),  # RUNNING without started_at
            ("status", "'COMPLETED'"),  # finished without completed_at
            ("status", "'FAILED'"),
            ("status, requeue", "'PENDING', '{}'"),  # parked, but not running
        ],
    )
    def test_task_rows_pq_would_never_write(
        self, pq: PQ, columns: str, values: str
    ) -> None:
        with (
            pytest.raises(IntegrityError, match="ck_pq_tasks_"),
            pq.session() as session,
        ):
            session.execute(
                text(
                    f"INSERT INTO pq_tasks (name, payload, priority, run_at, attempts,"
                    f" {columns}) VALUES ('t', '{{}}', 0, now(), 0, {values})"
                )
            )

    @pytest.mark.parametrize("timing", ["NULL, NULL", "interval '1 hour', '* * * * *'"])
    def test_schedule_needs_an_interval_or_a_cron_expression(
        self, pq: PQ, timing: str
    ) -> None:
        with (
            pytest.raises(IntegrityError, match="ck_pq_periodic_interval_or_cron"),
            pq.session() as session,
        ):
            session.execute(
                text(
                    "INSERT INTO pq_periodic (name, payload, priority, next_run,"
                    f" run_every, cron) VALUES ('t', '{{}}', 0, now(), {timing})"
                )
            )


class TestMigration:
    def test_upgrade_on_populated_tables_and_downgrade(
        self, pq: PQ, db_url: str
    ) -> None:
        """The migration applies to tables that hold rows, including a row
        that breaks one of the new constraints, and downgrades cleanly."""
        import importlib.resources

        from alembic import command
        from alembic.config import Config
        from sqlalchemy import inspect as sa_inspect

        cfg = Config()
        cfg.set_main_option(
            "script_location", str(importlib.resources.files("pq.migrations"))
        )
        cfg.set_main_option("sqlalchemy.url", db_url)
        command.stamp(cfg, "head")

        def schema() -> tuple[set[str], set[str], set[str]]:
            with pq.session() as session:
                inspector = sa_inspect(session.connection())
                return (
                    set(inspector.get_table_names()),
                    {c["name"] for c in inspector.get_columns("pq_tasks")},
                    {
                        c["name"] or ""
                        for c in inspector.get_check_constraints("pq_tasks")
                    },
                )

        new_columns = {"failure_kind", "worker_id", "claimed_max_runtime_seconds"}
        try:
            command.downgrade(cfg, "e5f6a7b8c9d0")
            tables, columns, checks = schema()
            assert "pq_workers" not in tables
            assert not new_columns & columns
            assert not {c for c in checks if c.startswith("ck_pq_tasks_")}

            # Rows from before, one of them not what pq writes today.
            with pq.session() as session:
                session.execute(
                    text(
                        "INSERT INTO pq_tasks (name, payload, priority, status,"
                        " run_at, client_id, attempts) VALUES"
                        " ('t', '{}', 50, 'PENDING', now(), 'old-pending', 0),"
                        " ('t', '{}', 50, 'FAILED', now(), 'old-odd', 1)"
                    )
                )

            command.upgrade(cfg, "head")
            tables, columns, checks = schema()
            assert "pq_workers" in tables
            assert new_columns <= columns
            assert len({c for c in checks if c.startswith("ck_pq_tasks_")}) == 3

            # The old rows are there and usable.
            old = pq.get_task_by_client_id("old-pending")
            assert old is not None and old.failure_kind is None
            pq.upsert(noop, client_id="old-odd")
            assert _task(pq, old.id).worker_id is None
            requeued = pq.get_task_by_client_id("old-odd")
            assert requeued is not None and requeued.status == TaskStatus.PENDING
        finally:
            command.upgrade(cfg, "head")
