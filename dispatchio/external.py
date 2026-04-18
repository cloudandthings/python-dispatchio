"""Helpers for event-driven dependencies.

Event dependencies are non-executable dependency keys such as
"event.user_registered" that are satisfied when matching completion-style
events are received by the orchestrator receiver backend.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Iterable

from beartype import beartype

from dispatchio.cadence import Cadence
from dispatchio.models import Dependency, Job, Status


@dataclass(frozen=True)
class EventDependencySpec:
    """Registry entry for validating and documenting event dependency keys."""

    name: str
    cadence: str
    description: str


@beartype
def event_dependency(
    name: str,
    *,
    cadence: Cadence | None = None,
    required_status: Status = Status.DONE,
) -> Dependency:
    """Create a Dependency for an event key.

    Args:
        name: Event dependency key (must start with "event." or "external.").
        cadence: Cadence used to resolve the dependency run_id.
        required_status: Required status for dependency satisfaction.
    """
    if not (name.startswith("event.") or name.startswith("external.")):
        raise ValueError(
            f"Event dependency {name!r} must start with 'event.' or 'external.'."
        )
    return Dependency(
        job_name=name,
        cadence=cadence,
        required_status=required_status,
    )


@beartype
def validate_event_dependencies(
    jobs: list[Job],
    registry: Iterable[EventDependencySpec],
) -> None:
    """Validate that all event dependencies referenced by jobs are registered.

    Raises:
        ValueError: If registry names are duplicated or a referenced external
            dependency is missing from the registry.
    """
    entries = list(registry)
    allowed_names: set[str] = set()
    duplicate_names: set[str] = set()

    for entry in entries:
        if entry.name in allowed_names:
            duplicate_names.add(entry.name)
        allowed_names.add(entry.name)

    if duplicate_names:
        names = ", ".join(sorted(duplicate_names))
        raise ValueError(f"Duplicate event dependency names in registry: {names}")

    unknown: list[tuple[str, str]] = []
    for job in jobs:
        for dep in job.depends_on:
            is_event_dep = dep.job_name.startswith("event.") or dep.job_name.startswith(
                "external."
            )
            if is_event_dep and dep.job_name not in allowed_names:
                unknown.append((job.name, dep.job_name))

    if unknown:
        detail = ", ".join(f"{job}->{dep}" for job, dep in unknown)
        raise ValueError(
            "Unknown event dependencies referenced by jobs: "
            f"{detail}. Register them with EventDependencySpec."
        )


# Backward-compatible aliases.
ExternalDependencySpec = EventDependencySpec
external_dependency = event_dependency
validate_external_dependencies = validate_event_dependencies
