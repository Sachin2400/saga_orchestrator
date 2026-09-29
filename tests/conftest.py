"""Shared test fixtures for the Saga Orchestrator test suite."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture(autouse=True)
def _isolate_services():
    """Clear in-memory service ledgers between tests to avoid cross-test contamination."""
    import services

    services._ledger.clear()
    services._failure_overrides.clear()
    yield
    services._ledger.clear()
    services._failure_overrides.clear()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "test_saga_wal.db")
