"""Pytest configuration and fixtures."""

import subprocess
import time

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from drift.config import DriftConfig, StorageSettings, CollectorSettings, RetentionSettings
from drift.storage.models import Base


# Connection strings for Docker containers
# Tests use a separate database (drift_test) to avoid conflicts with development
DRIFT_DB_DSN = "postgresql+psycopg://drift:drift@localhost:5433/drift_test"
TARGET_DB_DSN = "postgresql://postgres:postgres@localhost:5434/postgres"  # psycopg uses plain postgresql://


def is_postgres_ready(dsn: str, timeout: int = 30) -> bool:
    """Check if PostgreSQL is accepting connections."""
    import psycopg

    # Strip SQLAlchemy dialect prefix for raw psycopg connection
    clean_dsn = dsn.replace("postgresql+psycopg://", "postgresql://")

    start = time.time()
    while time.time() - start < timeout:
        try:
            with psycopg.connect(clean_dsn, connect_timeout=2) as conn:
                conn.execute("SELECT 1")
                return True
        except Exception:
            time.sleep(1)
    return False


@pytest.fixture(scope="session")
def docker_services():
    """Ensure Docker containers are running.

    This fixture checks if the containers are up. If not, it gives a helpful error.
    We don't auto-start containers because that should be an explicit user action.
    """
    # Check if drift-db is ready
    if not is_postgres_ready(DRIFT_DB_DSN, timeout=5):
        pytest.skip(
            "Docker containers not running. Start them with: docker compose up -d"
        )

    # Check if target-db is ready
    if not is_postgres_ready(TARGET_DB_DSN, timeout=5):
        pytest.skip(
            "Docker containers not running. Start them with: docker compose up -d"
        )

    yield


@pytest.fixture(scope="session")
def drift_db_engine(docker_services):
    """SQLAlchemy engine for the Drift storage database."""
    engine = create_engine(DRIFT_DB_DSN)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def init_db(drift_db_engine):
    """Create all tables in the test database.

    This runs once per test session.
    """
    Base.metadata.create_all(drift_db_engine)
    yield
    # Don't drop tables - useful for debugging failed tests


@pytest.fixture
def db_session(drift_db_engine, init_db):
    """Provide a transactional database session for tests.

    Each test runs in a transaction that's rolled back afterward,
    so tests don't affect each other.
    """
    connection = drift_db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()
    connection.close()


@pytest.fixture
def test_config() -> DriftConfig:
    """Provide a test configuration."""
    return DriftConfig(
        storage=StorageSettings(dsn=DRIFT_DB_DSN),
        collector=CollectorSettings(interval_seconds=60, snapshot_timeout_seconds=10),
        retention=RetentionSettings(raw_days=7, hourly_days=90),
    )


@pytest.fixture
def target_db_dsn() -> str:
    """Connection string for the target (monitored) database."""
    return TARGET_DB_DSN
