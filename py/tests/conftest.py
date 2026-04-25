from __future__ import annotations

import os
import uuid
from collections.abc import Generator

# macOS test runs import both Intel OpenMP and LLVM OpenMP through modeling
# dependencies. Keeping the pools single-threaded prevents interpreter shutdown
# from hanging while OpenMP runtimes reap worker threads.
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")
os.environ.setdefault("POLARS_MAX_THREADS", "1")

import pytest
from f1_polymarket_lab.storage.db import build_engine
from f1_polymarket_lab.storage.migrations import upgrade_database
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import OperationalError


@pytest.fixture
def postgres_db_url() -> Generator[str, None, None]:
    base_url = os.getenv("TEST_POSTGRES_URL")
    if not base_url:
        pytest.skip("TEST_POSTGRES_URL is not set")

    parsed_url = make_url(base_url)
    database_name = f"{parsed_url.database or 'f1_polymarket_lab_test'}_{uuid.uuid4().hex[:8]}"
    admin_url = parsed_url.set(database="postgres")
    admin_engine = create_engine(
        admin_url.render_as_string(hide_password=False),
        future=True,
        isolation_level="AUTOCOMMIT",
    )

    try:
        with admin_engine.connect() as connection:
            connection.execute(text(f'CREATE DATABASE "{database_name}"'))
    except OperationalError as exc:
        admin_engine.dispose()
        pytest.skip(f"TEST_POSTGRES_URL is unreachable: {exc}")

    test_url = parsed_url.set(database=database_name).render_as_string(hide_password=False)
    upgrade_database(test_url)

    try:
        yield test_url
    finally:
        test_engine = build_engine(test_url)
        test_engine.dispose()
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) "
                    "FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.execute(text(f'DROP DATABASE IF EXISTS "{database_name}"'))
        admin_engine.dispose()


@pytest.fixture
def postgres_engine(postgres_db_url: str) -> Generator[Engine, None, None]:
    engine = build_engine(postgres_db_url)
    try:
        yield engine
    finally:
        engine.dispose()
