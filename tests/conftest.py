"""Shared fixtures for all tests."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio.models import Job, SubprocessJob, AttemptRecord, Status


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
        executor=SubprocessJob(command=["echo", "{logical_run_id}"]),
    )


def make_attempt(
    job_name="job", logical_run_id="20250115", attempt=0, status=Status.DONE, **kw
):
    """Helper to create AttemptRecord for tests."""
    return AttemptRecord(
        job_name=job_name,
        logical_run_id=logical_run_id,
        attempt=attempt,
        dispatchio_attempt_id=uuid4(),
        status=status,
        **kw,
    )
