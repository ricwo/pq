"""Pytest fixtures for PQ tests."""

import multiprocessing
import multiprocessing.managers
import os
from collections.abc import Generator

import pytest

from pq.client import PQ


@pytest.fixture
def db_url() -> str:
    """Database URL for tests."""
    url = os.environ.get(
        "PQ_DATABASE_URL", "postgresql://postgres:postgres@localhost:5433/postgres"
    )
    # Disable GSSAPI to prevent segfaults when forking (psycopg2-binary bundles
    # libkrb5/libgssapi whose internal state corrupts across os.fork).
    if "gssencmode" not in url:
        url += "&gssencmode=disable" if "?" in url else "?gssencmode=disable"
    return url


@pytest.fixture
def pq(db_url: str) -> Generator[PQ, None, None]:
    """PQ client with clean database for each test."""
    client = PQ(db_url)
    client.create_tables()
    client.clear_all()
    yield client
    client.clear_all()
    client.close()


@pytest.fixture
def manager() -> Generator[multiprocessing.managers.SyncManager, None, None]:
    """Multiprocessing manager for shared state in fork-isolated tests."""
    mgr = multiprocessing.Manager()
    yield mgr
    mgr.shutdown()
