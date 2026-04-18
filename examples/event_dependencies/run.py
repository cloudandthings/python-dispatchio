"""
Event dependency demo runner.

Run with:
    python examples/event_dependencies/run.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import FilesystemReceiver, Status  # noqa: E402
from dispatchio.receiver.base import CompletionEvent  # noqa: E402
from examples.event_dependencies.jobs import orchestrator  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _emit_external_event(event_name: str, run_id: str) -> None:
    receiver = orchestrator.receiver
    if not isinstance(receiver, FilesystemReceiver):
        raise RuntimeError(
            "This demo expects a filesystem receiver so it can emit local events."
        )

    receiver.emit(
        CompletionEvent(
            job_name=event_name,
            run_id=run_id,
            status=Status.DONE,
            metadata={"source": "external_demo"},
        )
    )


def _tick(label: str, reference_time: datetime) -> list[tuple[str, str]]:
    result = orchestrator.tick(reference_time=reference_time)
    rows = [(entry.job_name, entry.action.value) for entry in result.results]

    log.info(label)
    for job_name, action in rows:
        log.info("  %s -> %s", job_name, action)

    return rows


def _submitted(rows: list[tuple[str, str]], job_name: str) -> bool:
    return any(
        row_job == job_name and action in {"submitted", "retrying"}
        for row_job, action in rows
    )


def _log_attempts(run_id: str) -> None:
    attempts = orchestrator.state.list_attempts(logical_run_id=run_id)
    log.info("Attempt history for logical_run_id=%s", run_id)
    for record in attempts:
        log.info(
            "  %s attempt=%d status=%s trigger=%s reason=%s",
            record.job_name,
            record.attempt,
            record.status.value,
            record.trigger_type.value,
            record.trigger_reason,
        )


if __name__ == "__main__":
    reference_time = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
    run_id = reference_time.strftime("%Y%m%d")

    # Tick 1: nothing submitted yet (no events received).
    tick1 = _tick("Tick 1 - no events", reference_time)
    assert not _submitted(tick1, "send_welcome_email")
    assert not _submitted(tick1, "activate_paid_features")

    # Emit one event.
    _emit_external_event("event.user_registered", run_id)

    # Tick 2: single-event job is now unblocked; two-event job is still waiting.
    tick2 = _tick("Tick 2 - event.user_registered received", reference_time)
    assert _submitted(tick2, "send_welcome_email")
    assert not _submitted(tick2, "activate_paid_features")

    # Emit the second event required by the fan-in job.
    _emit_external_event("event.kyc_passed", run_id)

    # Tick 3: two-event dependency fan-in is now satisfied.
    tick3 = _tick("Tick 3 - event.kyc_passed received", reference_time)
    assert _submitted(tick3, "activate_paid_features")

    _log_attempts(run_id)

    log.info("Event dependency demo completed successfully")
