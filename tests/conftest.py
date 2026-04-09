"""Shared fixtures for all tests."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone

from dispatchio.state.memory import MemoryStateStore
from dispatchio.models import Job, SubprocessJob


REFERENCE_TIME = datetime(2025, 1, 15, 2, 30, tzinfo=timezone.utc)


@pytest.fixture
def ref_time():
    return REFERENCE_TIME


@pytest.fixture
def mem_store():
    return MemoryStateStore()


@pytest.fixture
def simple_job():
    return Job(
        name="test_job",
        executor=SubprocessJob(command=["echo", "{run_id}"]),
    )
