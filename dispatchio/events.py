"""Helpers for event-driven dependencies.

Event dependencies are non-executable dependency keys such as
"user_registered" that are satisfied when matching completion-style
events are received by the orchestrator receiver backend.
"""

from __future__ import annotations

from beartype import beartype

from dispatchio.cadence import Cadence
from dispatchio.models import Event, EventDependency, Status


@beartype
def event_dependency(
    name: str,
    *,
    cadence: Cadence | None = None,
    required_status: Status = Status.DONE,
) -> EventDependency:
    return EventDependency(
        event_name=name,
        cadence=cadence,
        required_status=required_status,
    )


def emit_event(
    name: str,
    run_key: str,
    *,
    namespace: str = "default",
    status: Status = Status.DONE,
) -> None:
    """Signal that an external event has occurred.

    `namespace` identifies the target orchestrator. Any orchestrator whose
    state store is scoped to that namespace will pick up the event when
    evaluating EventDependency conditions. If no orchestrator with that
    namespace exists the event is stored but never consumed — this is safe
    and documented behaviour.
    """
    from dispatchio.config.factory import _build_state
    from dispatchio.config.settings import DispatchioSettings

    settings = DispatchioSettings()
    state = _build_state(settings.state, namespace=namespace)
    state.set_event(
        Event(namespace=namespace, name=name, run_key=run_key, status=status)
    )
