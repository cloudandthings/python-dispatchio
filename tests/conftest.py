"""Shared fixtures for all tests."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio.models import Job, SubprocessJob


REFERENCE_TIME = datetime(2025, 1, 15, 2, 30, tzinfo=timezone.utc)


@pytest.fixture
def ref_time():
    return REFERENCE_TIME


@pytest.fixture
def mem_store():
    return SQLAlchemyStateStore("sqlite:///:memory:")


@pytest.fixture
def simple_job():
    return Job(
        name="test_job",
        executor=SubprocessJob(command=["echo", "{run_id}"]),
    )
