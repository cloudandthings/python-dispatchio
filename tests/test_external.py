"""Tests for dispatchio.external helpers."""

from __future__ import annotations

import pytest

from dispatchio import (
    DAILY,
    EventDependencySpec,
    Job,
    SubprocessJob,
    event_dependency,
    external_dependency,
    validate_event_dependencies,
)


def test_external_dependency_builds_dependency() -> None:
    dep = event_dependency("event.user_registered", cadence=DAILY)
    assert dep.job_name == "event.user_registered"
    assert dep.cadence == DAILY


def test_external_dependency_requires_external_prefix() -> None:
    with pytest.raises(ValueError, match="must start with 'event.' or 'external.'"):
        event_dependency("user_registered")


def test_external_dependency_alias_accepts_external_prefix() -> None:
    dep = external_dependency("external.user_registered", cadence=DAILY)
    assert dep.job_name == "external.user_registered"


def test_validate_external_dependencies_accepts_registered_names() -> None:
    job = Job.create(
        name="welcome",
        executor=SubprocessJob(command=["echo", "ok"]),
        depends_on=[event_dependency("event.user_registered", cadence=DAILY)],
    )

    validate_event_dependencies(
        [job],
        (
            EventDependencySpec(
                name="event.user_registered",
                cadence="daily (YYYYMMDD)",
                description="User registration event",
            ),
        ),
    )


def test_validate_external_dependencies_raises_on_unknown() -> None:
    job = Job.create(
        name="welcome",
        executor=SubprocessJob(command=["echo", "ok"]),
        depends_on=[event_dependency("event.user_registered", cadence=DAILY)],
    )

    with pytest.raises(ValueError, match="Unknown event dependencies"):
        validate_event_dependencies(
            [job],
            (
                EventDependencySpec(
                    name="event.kyc_passed",
                    cadence="daily (YYYYMMDD)",
                    description="KYC event",
                ),
            ),
        )


def test_validate_external_dependencies_raises_on_duplicate_registry_names() -> None:
    job = Job.create(
        name="welcome",
        executor=SubprocessJob(command=["echo", "ok"]),
        depends_on=[event_dependency("event.user_registered", cadence=DAILY)],
    )

    with pytest.raises(ValueError, match="Duplicate event dependency names"):
        validate_event_dependencies(
            [job],
            (
                EventDependencySpec(
                    name="event.user_registered",
                    cadence="daily (YYYYMMDD)",
                    description="User registration event",
                ),
                EventDependencySpec(
                    name="event.user_registered",
                    cadence="daily (YYYYMMDD)",
                    description="Duplicate entry",
                ),
            ),
        )


def test_validate_event_dependencies_accepts_external_prefix_for_compatibility() -> (
    None
):
    job = Job.create(
        name="welcome",
        executor=SubprocessJob(command=["echo", "ok"]),
        depends_on=[event_dependency("external.user_registered", cadence=DAILY)],
    )

    validate_event_dependencies(
        [job],
        (
            EventDependencySpec(
                name="external.user_registered",
                cadence="daily (YYYYMMDD)",
                description="Compatibility event key",
            ),
        ),
    )
