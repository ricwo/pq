"""Tests for PQ client."""

import pytest
from croniter import croniter
from datetime import UTC, datetime, timedelta
from sqlalchemy.exc import IntegrityError

from pq.client import PQ
from pq.models import Periodic, Task


def dummy_handler(key: str = "") -> None:
    """Dummy handler for testing."""
    pass


def cleanup_handler(full: bool = False) -> None:
    """Cleanup handler for testing periodic tasks."""
    pass


def cron_handler() -> None:
    """Handler for cron tests."""
    pass


class TestEnqueue:
    """Tests for enqueue method."""

    def test_enqueue_creates_task(self, pq: PQ) -> None:
        """Enqueue creates a task in the database."""
        task_id = pq.enqueue(dummy_handler, key="value")

        assert task_id is not None
        assert pq.pending_count() == 1

    def test_enqueue_stores_correct_data(self, pq: PQ) -> None:
        """Enqueue stores name, payload, and run_at correctly."""
        task_id = pq.enqueue(dummy_handler, key="value")

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "tests.test_client:dummy_handler"
            assert task.payload["args"] == []
            assert task.payload["kwargs"] == {"key": "value"}
            assert task.run_at <= datetime.now(UTC)

    def test_enqueue_with_run_at(self, pq: PQ) -> None:
        """Enqueue respects custom run_at time."""
        future = datetime.now(UTC) + timedelta(hours=1)
        task_id = pq.enqueue(dummy_handler, run_at=future)

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            # Allow small time drift
            assert abs((task.run_at - future).total_seconds()) < 1

    def test_enqueue_stores_function_path(self, pq: PQ) -> None:
        """Enqueue stores function path as name."""
        task_id = pq.enqueue(dummy_handler, key="value")

        with pq.session() as session:
            from sqlalchemy import select

            task = session.execute(select(Task).where(Task.id == task_id)).scalar_one()
            assert task.name == "tests.test_client:dummy_handler"

    def test_enqueue_returns_int_id(self, pq: PQ) -> None:
        """Enqueue returns an integer ID."""
        task_id = pq.enqueue(dummy_handler)
        assert isinstance(task_id, int)
        assert task_id > 0


class TestSchedule:
    """Tests for schedule method."""

    def test_schedule_creates_periodic(self, pq: PQ) -> None:
        """Schedule creates a periodic task."""
        periodic_id = pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        assert periodic_id is not None
        assert pq.periodic_count() == 1

    def test_schedule_stores_correct_data(self, pq: PQ) -> None:
        """Schedule stores name, payload, and run_every correctly."""
        interval = timedelta(hours=2)
        pq.schedule(cleanup_handler, run_every=interval, full=True)

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.name == "tests.test_client:cleanup_handler"
            assert periodic.payload["kwargs"] == {"full": True}
            assert periodic.run_every == interval
            assert periodic.next_run <= datetime.now(UTC)
            assert periodic.last_run is None

    def test_schedule_upserts_existing(self, pq: PQ) -> None:
        """Scheduling same function updates existing record."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), full=True)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.run_every == timedelta(hours=2)
            assert periodic.payload["kwargs"] == {"full": True}

    def test_schedule_with_cron_string(self, pq: PQ) -> None:
        """Schedule with valid cron string works."""
        pq.schedule(cron_handler, cron="0 9 * * 1")  # Monday 9am

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cron_handler"
                )
            ).scalar_one()
            assert periodic.cron == "0 9 * * 1"
            assert periodic.run_every is None

    def test_schedule_with_invalid_cron_raises(self, pq: PQ) -> None:
        """Schedule with invalid cron string raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            pq.schedule(cron_handler, cron="invalid cron")

        assert "Invalid cron expression" in str(exc_info.value)
        assert "invalid cron" in str(exc_info.value)

    def test_schedule_with_croniter_object(self, pq: PQ) -> None:
        """Schedule with croniter object works."""
        cron_obj = croniter("30 14 * * 5")  # Friday 2:30pm
        pq.schedule(cron_handler, cron=cron_obj)

        with pq.session() as session:
            from sqlalchemy import select

            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cron_handler"
                )
            ).scalar_one()
            # Expression should be extracted and stored
            assert periodic.cron == "30 14 * * 5"
            assert periodic.run_every is None


class TestScheduleMaxConcurrent:
    """Tests for max_concurrent parameter in schedule."""

    def test_schedule_with_max_concurrent(self, pq: PQ) -> None:
        """Schedule stores max_concurrent in DB."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=1)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent == 1

    def test_schedule_with_max_concurrent_none(self, pq: PQ) -> None:
        """Schedule stores max_concurrent=None for unlimited concurrency."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=None)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent is None

    def test_schedule_upserts_max_concurrent(self, pq: PQ) -> None:
        """Schedule upsert updates max_concurrent value."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=1)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=None)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent is None

    def test_schedule_max_concurrent_invalid_raises(self, pq: PQ) -> None:
        """Schedule with max_concurrent > 1 raises ValueError."""
        with pytest.raises(ValueError, match="max_concurrent must be 1 or None"):
            pq.schedule(cleanup_handler, run_every=timedelta(hours=1), max_concurrent=2)

    def test_schedule_max_concurrent_default(self, pq: PQ) -> None:
        """Schedule without max_concurrent defaults to 1."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.max_concurrent == 1


class TestScheduleActive:
    """Tests for active parameter in schedule."""

    def test_schedule_active_defaults_true(self, pq: PQ) -> None:
        """Schedule stores active=True by default."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is True

    def test_schedule_active_false(self, pq: PQ) -> None:
        """Schedule stores active=False when explicitly set."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is False

    def test_schedule_upserts_active(self, pq: PQ) -> None:
        """Schedule upsert updates active flag."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=True)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is False

    def test_schedule_upserts_active_reactivate(self, pq: PQ) -> None:
        """Schedule upsert can re-enable an inactive task."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=True)

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.active is True


class TestPeriodicKey:
    """Tests for periodic task key discriminator."""

    def test_different_keys_create_separate_entries(self, pq: PQ) -> None:
        """Same function with different keys creates separate periodic entries."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="us")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")

        assert pq.periodic_count() == 2

    def test_same_key_upserts(self, pq: PQ) -> None:
        """Same function + same key upserts (updates) the existing entry."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="region")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="region")

        assert pq.periodic_count() == 1

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.run_every == timedelta(hours=2)

    def test_unschedule_with_key_removes_only_that_entry(self, pq: PQ) -> None:
        """Unschedule with key only removes the matching entry."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="us")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")
        assert pq.periodic_count() == 2

        result = pq.unschedule(cleanup_handler, key="us")

        assert result is True
        assert pq.periodic_count() == 1

    def test_unschedule_without_key_removes_default_entry(self, pq: PQ) -> None:
        """Unschedule without key only removes the default-key entry."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2), key="eu")
        assert pq.periodic_count() == 2

        result = pq.unschedule(cleanup_handler)

        assert result is True
        assert pq.periodic_count() == 1

    def test_default_key_is_empty_string(self, pq: PQ) -> None:
        """Omitting key stores empty string in DB."""
        from sqlalchemy import select

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(
                select(Periodic).where(
                    Periodic.name == "tests.test_client:cleanup_handler"
                )
            ).scalar_one()
            assert periodic.key == ""


class TestCancel:
    """Tests for cancel method."""

    def test_cancel_removes_task(self, pq: PQ) -> None:
        """Cancel removes task from database."""
        task_id = pq.enqueue(dummy_handler)
        assert pq.pending_count() == 1

        result = pq.cancel(task_id)

        assert result is True
        assert pq.pending_count() == 0

    def test_cancel_nonexistent_returns_false(self, pq: PQ) -> None:
        """Cancel returns False for nonexistent task."""
        result = pq.cancel(999999)
        assert result is False


class TestUnschedule:
    """Tests for unschedule method."""

    def test_unschedule_removes_periodic(self, pq: PQ) -> None:
        """Unschedule removes periodic task."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))
        assert pq.periodic_count() == 1

        result = pq.unschedule(cleanup_handler)

        assert result is True
        assert pq.periodic_count() == 0

    def test_unschedule_nonexistent_returns_false(self, pq: PQ) -> None:
        """Unschedule returns False for nonexistent function."""
        result = pq.unschedule(dummy_handler)
        assert result is False


class TestClientId:
    """Tests for client_id functionality."""

    def test_enqueue_with_client_id(self, pq: PQ) -> None:
        """Enqueue stores client_id correctly."""
        task_id = pq.enqueue(dummy_handler, client_id="my-task-1")

        task = pq.get_task(task_id)
        assert task is not None
        assert task.client_id == "my-task-1"

    def test_enqueue_duplicate_client_id_raises(self, pq: PQ) -> None:
        """Enqueue with duplicate client_id raises IntegrityError."""
        pq.enqueue(dummy_handler, client_id="unique-id")

        with pytest.raises(IntegrityError):
            pq.enqueue(dummy_handler, client_id="unique-id")

    def test_enqueue_without_client_id(self, pq: PQ) -> None:
        """Enqueue without client_id sets it to None."""
        task_id = pq.enqueue(dummy_handler)

        task = pq.get_task(task_id)
        assert task is not None
        assert task.client_id is None

    def test_schedule_with_client_id(self, pq: PQ) -> None:
        """Schedule stores client_id correctly."""
        pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="periodic-1"
        )

        periodic = pq.get_periodic_by_client_id("periodic-1")
        assert periodic is not None
        assert periodic.client_id == "periodic-1"

    def test_schedule_upsert_preserves_client_id(self, pq: PQ) -> None:
        """Schedule upsert does not overwrite client_id."""
        pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="original-id"
        )
        pq.schedule(cleanup_handler, run_every=timedelta(hours=2))

        periodic = pq.get_periodic_by_client_id("original-id")
        assert periodic is not None
        assert periodic.run_every == timedelta(hours=2)

    def test_get_task_by_client_id(self, pq: PQ) -> None:
        """get_task_by_client_id returns correct task."""
        task_id = pq.enqueue(dummy_handler, client_id="lookup-test")

        task = pq.get_task_by_client_id("lookup-test")
        assert task is not None
        assert task.id == task_id

    def test_get_task_by_client_id_not_found(self, pq: PQ) -> None:
        """get_task_by_client_id returns None for non-existent client_id."""
        task = pq.get_task_by_client_id("does-not-exist")
        assert task is None

    def test_get_periodic_by_client_id(self, pq: PQ) -> None:
        """get_periodic_by_client_id returns correct periodic."""
        periodic_id = pq.schedule(
            cleanup_handler, run_every=timedelta(hours=1), client_id="periodic-lookup"
        )

        periodic = pq.get_periodic_by_client_id("periodic-lookup")
        assert periodic is not None
        assert periodic.id == periodic_id

    def test_get_periodic_by_client_id_not_found(self, pq: PQ) -> None:
        """get_periodic_by_client_id returns None for non-existent client_id."""
        periodic = pq.get_periodic_by_client_id("does-not-exist")
        assert periodic is None

    def test_multiple_tasks_null_client_id(self, pq: PQ) -> None:
        """Multiple tasks with null client_id are allowed."""
        task_id_1 = pq.enqueue(dummy_handler)
        task_id_2 = pq.enqueue(dummy_handler)

        assert task_id_1 != task_id_2
        assert pq.pending_count() == 2


def upsert_handler(value: int = 0) -> None:
    """Handler for upsert tests."""
    pass


def failing_upsert_handler() -> None:
    """Failing handler for upsert tests."""
    raise ValueError("boom")


class TestUpsert:
    """Tests for upsert method."""

    def test_upsert_creates_new_task(self, pq: PQ) -> None:
        """Upsert creates a new task when client_id doesn't exist."""
        task_id = pq.upsert(upsert_handler, value=42, client_id="new-task")

        assert task_id is not None
        assert pq.pending_count() == 1

        task = pq.get_task_by_client_id("new-task")
        assert task is not None
        assert task.id == task_id
        assert task.payload["kwargs"] == {"value": 42}

    def test_upsert_updates_existing_task(self, pq: PQ) -> None:
        """Upsert updates task when client_id already exists."""
        # Create initial task
        task_id_1 = pq.upsert(upsert_handler, value=1, client_id="my-task")

        # Upsert with same client_id
        task_id_2 = pq.upsert(upsert_handler, value=2, client_id="my-task")

        # Should still have only 1 task
        assert pq.pending_count() == 1
        # Should return the same task ID
        assert task_id_1 == task_id_2

        task = pq.get_task_by_client_id("my-task")
        assert task is not None
        # Should have updated payload
        assert task.payload["kwargs"] == {"value": 2}

    def test_upsert_resets_status_to_pending(self, pq: PQ) -> None:
        """Upsert resets status to PENDING on conflict."""
        from pq.models import TaskStatus

        # Create and process task
        pq.upsert(dummy_handler, client_id="reset-test")
        pq.run_worker_once()

        # Task should be completed
        task = pq.get_task_by_client_id("reset-test")
        assert task is not None
        assert task.status == TaskStatus.COMPLETED

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="reset-test")

        # Status should be reset to PENDING
        task = pq.get_task_by_client_id("reset-test")
        assert task is not None
        assert task.status == TaskStatus.PENDING

    def test_upsert_resets_attempts_to_zero(self, pq: PQ) -> None:
        """Upsert resets attempts to 0 on conflict."""
        # Create and process task
        pq.upsert(dummy_handler, client_id="attempts-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("attempts-test")
        assert task is not None
        assert task.attempts == 1

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="attempts-test")

        task = pq.get_task_by_client_id("attempts-test")
        assert task is not None
        assert task.attempts == 0

    def test_upsert_clears_timestamps(self, pq: PQ) -> None:
        """Upsert clears started_at and completed_at on conflict."""
        # Create and process task
        pq.upsert(dummy_handler, client_id="timestamps-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("timestamps-test")
        assert task is not None
        assert task.started_at is not None
        assert task.completed_at is not None

        # Upsert same client_id
        pq.upsert(dummy_handler, client_id="timestamps-test")

        task = pq.get_task_by_client_id("timestamps-test")
        assert task is not None
        assert task.started_at is None
        assert task.completed_at is None

    def test_upsert_clears_error(self, pq: PQ) -> None:
        """Upsert clears error field on conflict."""
        from pq.models import TaskStatus

        pq.upsert(failing_upsert_handler, client_id="error-test")
        pq.run_worker_once()

        task = pq.get_task_by_client_id("error-test")
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert task.error is not None

        # Upsert same client_id with different handler
        pq.upsert(dummy_handler, client_id="error-test")

        task = pq.get_task_by_client_id("error-test")
        assert task is not None
        assert task.error is None

    def test_upsert_updates_priority(self, pq: PQ) -> None:
        """Upsert updates priority on conflict."""
        from pq.priority import Priority

        pq.upsert(dummy_handler, client_id="priority-test", priority=Priority.LOW)

        task = pq.get_task_by_client_id("priority-test")
        assert task is not None
        assert task.priority == Priority.LOW.value

        pq.upsert(dummy_handler, client_id="priority-test", priority=Priority.HIGH)

        task = pq.get_task_by_client_id("priority-test")
        assert task is not None
        assert task.priority == Priority.HIGH.value

    def test_upsert_updates_run_at(self, pq: PQ) -> None:
        """Upsert updates run_at on conflict."""
        now = datetime.now(UTC)
        future = now + timedelta(hours=2)

        pq.upsert(dummy_handler, client_id="run-at-test", run_at=now)

        task = pq.get_task_by_client_id("run-at-test")
        assert task is not None
        assert abs((task.run_at - now).total_seconds()) < 1

        pq.upsert(dummy_handler, client_id="run-at-test", run_at=future)

        task = pq.get_task_by_client_id("run-at-test")
        assert task is not None
        assert abs((task.run_at - future).total_seconds()) < 1

    def test_upsert_returns_int_id(self, pq: PQ) -> None:
        """Upsert returns an integer ID."""
        task_id = pq.upsert(dummy_handler, client_id="int-test")
        assert isinstance(task_id, int)
        assert task_id > 0


class TestPendingAge:
    """Tests for pending_age method."""

    def test_no_pending_tasks(self, pq: PQ) -> None:
        """Returns None when no pending tasks exist."""
        assert pq.pending_age() is None

    def test_returns_timedelta(self, pq: PQ) -> None:
        """Returns a timedelta for a pending task that is ready to run."""
        pq.enqueue(dummy_handler)

        age = pq.pending_age()
        assert isinstance(age, timedelta)
        assert age.total_seconds() >= 0

    def test_ignores_future_tasks(self, pq: PQ) -> None:
        """Ignores pending tasks with run_at in the future."""
        future = datetime.now(UTC) + timedelta(hours=1)
        pq.enqueue(dummy_handler, run_at=future)

        assert pq.pending_age() is None

    def test_returns_oldest(self, pq: PQ) -> None:
        """Returns age of the oldest ready task when multiple exist."""
        from sqlalchemy import select, update

        # Create two tasks, then backdate one
        pq.enqueue(dummy_handler, client_id="old")
        pq.enqueue(dummy_handler, client_id="new")

        with pq.session() as session:
            old_task = session.execute(
                select(Task).where(Task.client_id == "old")
            ).scalar_one()
            session.execute(
                update(Task)
                .where(Task.id == old_task.id)
                .values(run_at=datetime.now(UTC) - timedelta(minutes=10))
            )

        age = pq.pending_age()
        assert age is not None
        assert age.total_seconds() >= 600  # at least 10 minutes

    def test_ignores_non_pending_tasks(self, pq: PQ) -> None:
        """Ignores completed/running tasks."""
        pq.enqueue(dummy_handler)
        pq.run_worker_once()

        assert pq.pending_age() is None


class TestOverduePeriodicCount:
    """Tests for overdue_periodic_count method."""

    def test_no_periodic_tasks(self, pq: PQ) -> None:
        """Returns 0 when no periodic tasks exist."""
        assert pq.overdue_periodic_count() == 0

    def test_counts_overdue(self, pq: PQ) -> None:
        """Counts periodic tasks with next_run in the past."""
        # schedule with run_every sets next_run = now, making it immediately overdue
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        assert pq.overdue_periodic_count() == 1

    def test_ignores_inactive(self, pq: PQ) -> None:
        """Ignores inactive periodic tasks even if overdue."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        assert pq.overdue_periodic_count() == 0

    def test_ignores_future(self, pq: PQ) -> None:
        """Ignores periodic tasks with next_run in the future."""
        from sqlalchemy import select, update

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(select(Periodic)).scalar_one()
            session.execute(
                update(Periodic)
                .where(Periodic.id == periodic.id)
                .values(next_run=datetime.now(UTC) + timedelta(hours=1))
            )

        assert pq.overdue_periodic_count() == 0


class TestOverduePeriodicAge:
    """Tests for overdue_periodic_age method."""

    def test_no_periodic_tasks(self, pq: PQ) -> None:
        """Returns None when no periodic tasks exist."""
        assert pq.overdue_periodic_age() is None

    def test_returns_timedelta(self, pq: PQ) -> None:
        """Returns a timedelta for an overdue periodic task."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        age = pq.overdue_periodic_age()
        assert isinstance(age, timedelta)
        assert age.total_seconds() >= 0

    def test_returns_oldest(self, pq: PQ) -> None:
        """Returns age of the most overdue periodic task."""
        from sqlalchemy import select, update

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="old")
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), key="new")

        with pq.session() as session:
            old_periodic = session.execute(
                select(Periodic).where(Periodic.key == "old")
            ).scalar_one()
            session.execute(
                update(Periodic)
                .where(Periodic.id == old_periodic.id)
                .values(next_run=datetime.now(UTC) - timedelta(minutes=30))
            )

        age = pq.overdue_periodic_age()
        assert age is not None
        assert age.total_seconds() >= 1800  # at least 30 minutes

    def test_ignores_inactive(self, pq: PQ) -> None:
        """Returns None when only inactive periodic tasks are overdue."""
        pq.schedule(cleanup_handler, run_every=timedelta(hours=1), active=False)

        assert pq.overdue_periodic_age() is None

    def test_ignores_future(self, pq: PQ) -> None:
        """Returns None when all periodic tasks have next_run in the future."""
        from sqlalchemy import select, update

        pq.schedule(cleanup_handler, run_every=timedelta(hours=1))

        with pq.session() as session:
            periodic = session.execute(select(Periodic)).scalar_one()
            session.execute(
                update(Periodic)
                .where(Periodic.id == periodic.id)
                .values(next_run=datetime.now(UTC) + timedelta(hours=1))
            )

        assert pq.overdue_periodic_age() is None
