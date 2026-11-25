"""Pytest fixtures for PQ tests."""

import importlib.resources
import multiprocessing
import multiprocessing.managers
import os
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest
from alembic import command
from alembic.config import Config
from loguru import logger
from sqlalchemy import text

from pq.client import PQ

if TYPE_CHECKING:
    from loguru import Record


def _without_gssapi(url: str) -> str:
    # Disable GSSAPI to prevent segfaults when forking (psycopg2-binary bundles
    # libkrb5/libgssapi whose internal state corrupts across os.fork).
    if "gssencmode" not in url:
        url += "&gssencmode=disable" if "?" in url else "?gssencmode=disable"
    return url


@pytest.fixture
def db_url() -> str:
    """Database URL for tests."""
    return _without_gssapi(
        os.environ.get(
            "PQ_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres"
        )
    )


@pytest.fixture
def tls_db_url() -> str:
    """URL of the Postgres that only speaks TLS (``postgres-tls`` in
    docker-compose.yml), for tests whose failure only shows over TLS."""
    return _without_gssapi(
        os.environ.get(
            "PQ_TLS_DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5434/postgres?sslmode=require",
        )
    )


def _clean_pq(url: str) -> Generator[PQ, None, None]:
    client = PQ(url)
    client.create_tables()
    client.clear_all()
    yield client
    client.clear_all()
    client.close()


@pytest.fixture
def pq(db_url: str) -> Generator[PQ, None, None]:
    """PQ client with clean database for each test."""
    yield from _clean_pq(db_url)


@pytest.fixture
def alembic_cfg(pq: PQ, db_url: str) -> Config:
    """Alembic config for the test database, stamped at head.

    The ``pq`` fixture uses ``create_tables()``, which bypasses Alembic, so
    the history is stamped to match the schema before a test migrates it.
    """
    cfg = Config()
    cfg.set_main_option(
        "script_location", str(importlib.resources.files("pq.migrations"))
    )
    cfg.set_main_option("sqlalchemy.url", db_url)
    command.stamp(cfg, "head")
    return cfg


@pytest.fixture
def tls_pq(tls_db_url: str) -> Generator[PQ, None, None]:
    """Like ``pq``, on the TLS-only Postgres. Fails if the session isn't TLS."""
    for client in _clean_pq(tls_db_url):
        with client.session() as session:
            uses_tls = session.execute(
                text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
            ).scalar_one()
        assert uses_tls, f"{tls_db_url} is not a TLS connection"
        yield client


@pytest.fixture
def log_records() -> Generator[list["Record"], None, None]:
    """Every loguru record emitted during the test, DEBUG and up."""
    records: list[Record] = []
    sink = logger.add(lambda message: records.append(message.record), level="DEBUG")
    yield records
    logger.remove(sink)


@pytest.fixture
def manager() -> Generator[multiprocessing.managers.SyncManager, None, None]:
    """Multiprocessing manager for shared state in fork-isolated tests."""
    mgr = multiprocessing.Manager()
    yield mgr
    mgr.shutdown()
