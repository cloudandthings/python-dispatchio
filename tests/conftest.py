"""Shared fixtures for all tests."""

from __future__ import annotations

import pytest
from datetime import datetime, timezone
from unittest.mock import patch
from uuid import uuid4

from rich.console import Console

from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio.models import Job, SubprocessJob, Attempt, Status


REFERENCE_TIME = datetime(2025, 1, 15, 2, 30, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def aws_default_region(monkeypatch):
    """Ensure a default AWS region is set for every test.

    boto3 raises NoRegionError when no region is configured and none is passed
    explicitly. CI environments have no ~/.aws/config, so we set a fallback
    here. Tests that need a specific region should still pass it explicitly to
    boto3.client(); this fixture only fills the gap when they don't.
    """
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def ref_time():
    return REFERENCE_TIME


@pytest.fixture
def state_store():
    return SQLAlchemyStateStore(
        connection_string="sqlite:///:memory:", namespace="test"
    )


@pytest.fixture
def simple_job():
    return Job(
        name="test_job",
        executor=SubprocessJob(command=["echo", "{run_key}"]),
    )


def make_attempt(
    job_name="job", run_key="20250115", attempt=0, status=Status.DONE, **kw
):
    """Helper to create Attempt for tests."""
    return Attempt(
        job_name=job_name,
        run_key=run_key,
        attempt=attempt,
        correlation_id=uuid4(),
        status=status,
        **kw,
    )


@pytest.fixture
def rich_output():
    """Patch dispatchio.cli.output.console with a recording console."""
    captured = Console(record=True, no_color=True, width=200)
    with patch("dispatchio.cli.output.console", captured):
        yield captured


@pytest.fixture
def rich_error_output():
    """Patch dispatchio.cli.output.error_console with a recording console."""
    captured = Console(record=True, no_color=True, width=200)
    with patch("dispatchio.cli.output.error_console", captured):
        yield captured
