"""Tests for the locks a migration takes before it changes the schema.

Each test has a database of its own, so it can stop at an old revision and
count the backends that wait for a lock without seeing another test's.
"""

import importlib.resources
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Generator
from dataclasses import dataclass

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import make_url, text

from pq.client import PQ
from pq.migrations import MIGRATION_LOCK_KEY

# The revision before the first migration that locks ``pq_tasks`` twice:
# ``SHARE`` for its indexes, then ``ACCESS EXCLUSIVE``.
BEFORE_INDEXES = "d4e5f6a7b8c9"

_MIGRATE = "import sys; from pq import PQ; PQ(sys.argv[1]).run_db_migrations()"

_INSERT_TASK = (
    "INSERT INTO pq_tasks (name, payload, priority, status, run_at, client_id,"
    " attempts) VALUES ('t', '{}', 50, 'PENDING', now(), :client_id, 0)"
)


@dataclass
class _Scratch:
    pq: PQ
    url: str
    cfg: Config

    def migrator(self) -> subprocess.Popen[str]:
        """Start a process that runs ``run_db_migrations()``."""
        return subprocess.Popen(
            [sys.executable, "-c", _MIGRATE, self.url],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def waiting(self) -> int:
        """Backends of this database that wait for a lock."""
        with self.pq.session() as session:
            return session.execute(
                text(
                    "SELECT count(*) FROM pg_stat_activity"
                    " WHERE datname = current_database()"
                    " AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'"
                )
            ).scalar_one()

    def version(self) -> str | None:
        with self.pq.session() as session:
            return session.execute(
                text("SELECT version_num FROM pq_schema_version")
            ).scalar_one_or_none()


@pytest.fixture
def scratch(pq: PQ, db_url: str) -> Generator[_Scratch, None, None]:
    database = f"pq_migration_locks_{uuid.uuid4().hex[:12]}"
    url = make_url(db_url).set(database=database).render_as_string(False)
    with pq._engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as connection:
        connection.execute(text(f"CREATE DATABASE {database}"))
    cfg = Config()
    cfg.set_main_option(
        "script_location", str(importlib.resources.files("pq.migrations"))
    )
    cfg.set_main_option("sqlalchemy.url", url)
    client = PQ(url)
    try:
        yield _Scratch(client, url, cfg)
    finally:
        client.close()
        with pq._engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            connection.execute(text(f"DROP DATABASE IF EXISTS {database} (FORCE)"))


def _wait_until(condition: Callable[[], bool], timeout: float = 30) -> None:
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        time.sleep(0.02)


def _finish(migrators: list[subprocess.Popen[str]]) -> list[tuple[int, str]]:
    """Exit code and output of each migrator."""
    results = []
    for migrator in migrators:
        try:
            output, _ = migrator.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            migrator.kill()
            output, _ = migrator.communicate()
        results.append((migrator.returncode, output))
    return results


def _head(cfg: Config) -> str | None:
    from alembic.script import ScriptDirectory

    return ScriptDirectory.from_config(cfg).get_current_head()


class TestMigratingAtOnce:
    def test_on_an_empty_database(self, scratch: _Scratch) -> None:
        """The first deploy: every worker finds no tables and creates them."""
        migrators: list[subprocess.Popen[str]] = []
        try:
            # Held here until every migrator has seen the empty database and
            # waits for its turn.
            with scratch.pq.session() as holder:
                holder.execute(
                    text("SELECT pg_advisory_xact_lock(:key)"),
                    {"key": MIGRATION_LOCK_KEY},
                )
                migrators = [scratch.migrator() for _ in range(6)]
                _wait_until(
                    lambda: (
                        scratch.waiting() == len(migrators)
                        or any(m.poll() is not None for m in migrators)
                    )
                )
                assert scratch.waiting() == len(migrators)
            results = _finish(migrators)
        finally:
            for migrator in migrators:
                migrator.kill()

        assert [code for code, _ in results] == [0] * 6, results
        assert scratch.version() == _head(scratch.cfg)
        with scratch.pq.session() as session:
            session.execute(text(_INSERT_TASK), {"client_id": "after"})

    def test_on_an_old_schema(self, scratch: _Scratch) -> None:
        """A release with a migration: every worker pod starts with the old
        schema in place and runs the migration."""
        command.upgrade(scratch.cfg, BEFORE_INDEXES)
        migrators: list[subprocess.Popen[str]] = []
        try:
            # A reader keeps every migrator from its table lock until all of
            # them are inside the migration.
            with scratch.pq.session() as reader:
                reader.execute(text("SELECT count(*) FROM pq_tasks"))
                migrators = [scratch.migrator() for _ in range(6)]
                _wait_until(
                    lambda: (
                        scratch.waiting() == len(migrators)
                        or any(m.poll() is not None for m in migrators)
                    )
                )
            results = _finish(migrators)
        finally:
            for migrator in migrators:
                migrator.kill()

        assert [code for code, _ in results] == [0] * 6, results
        assert scratch.version() == _head(scratch.cfg)


class TestMigratingNextToATransaction:
    def test_that_read_the_table_and_then_writes_to_it(self, scratch: _Scratch) -> None:
        """A transaction that read ``pq_tasks`` before the migration started
        and writes to it while the migration waits. Both finish."""
        command.upgrade(scratch.cfg, BEFORE_INDEXES)
        migrator: subprocess.Popen[str] | None = None
        try:
            with scratch.pq.session() as session:
                session.execute(text("SET LOCAL lock_timeout = '20s'"))
                session.execute(text("SELECT count(*) FROM pq_tasks"))
                migrator = scratch.migrator()
                started = migrator
                _wait_until(
                    lambda: scratch.waiting() == 1 or started.poll() is not None
                )
                assert scratch.waiting() == 1
                # The migration waits for this transaction. The transaction
                # does not wait for the migration.
                session.execute(text(_INSERT_TASK), {"client_id": "during"})
                assert migrator.poll() is None
            ((code, output),) = _finish([migrator])
        finally:
            if migrator is not None:
                migrator.kill()

        assert code == 0, output
        assert scratch.version() == _head(scratch.cfg)
        assert scratch.pq.get_task_by_client_id("during") is not None

    def test_takes_no_lock_when_nothing_is_pending(self, scratch: _Scratch) -> None:
        """A worker start on a current schema does not wait for a transaction
        that uses ``pq_tasks``, and so does not hold up the queue behind it."""
        command.upgrade(scratch.cfg, "head")
        migrator: subprocess.Popen[str] | None = None
        try:
            with scratch.pq.session() as session:
                session.execute(text(_INSERT_TASK), {"client_id": "open"})
                migrator = scratch.migrator()
                output, _ = migrator.communicate(timeout=30)
        finally:
            if migrator is not None:
                migrator.kill()

        assert migrator.returncode == 0, output
