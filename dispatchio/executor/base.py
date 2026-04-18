"""
Executor protocol.

An Executor takes a Job and a resolved logical_run_id and actually
runs the job. It is fire-and-forget: submit() kicks the job off and
returns immediately. The job reports completion separately via the
receiver layer.

Executors may optionally implement the Pokeable protocol to support
active liveness checks. The orchestrator calls poke() during Phase 2
(lost detection) for any active job whose executor is Pokeable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from dispatchio.models import Job, AttemptRecord, Status


@runtime_checkable
class Executor(Protocol):
    def submit(
        self,
        job: Job,
        attempt: AttemptRecord,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        """
        Submit the job for execution.
        Should raise on hard failure (e.g. cannot reach the executor).
        Must NOT block waiting for the job to complete.

        job: Job definition including executor config
        attempt: the AttemptRecord for this execution (contains job_name,
                 logical_run_id, attempt number, dispatchio_attempt_id)
        reference_time: the tick's reference time
        timeout: per-submission deadline in seconds. Not yet enforced by
                 local executors; reserved for cloud executors (e.g. ECS)
                 where the API call itself may be slow or rate-limited.
        """
        ...


@runtime_checkable
class Pokeable(Protocol):
    """
    Optional protocol for executors that can actively check job liveness.

    Implement this alongside Executor to enable active liveness checks
    as an alternative or complement to process polling mechanisms.
    The orchestrator calls poke() during Phase 2 for every active job whose
    executor implements this protocol.

    Return values:
        Status.RUNNING — process is confirmed alive; no state change
        Status.DONE    — process exited cleanly (exit code 0) without posting
                         a completion event; the orchestrator marks the job DONE
        Status.ERROR   — process died unexpectedly; orchestrator marks ERROR
        None           — liveness cannot be determined (e.g. job not tracked
                         by this executor instance); orchestrator falls back to
                         other liveness mechanisms
    """

    def poke(self, record: AttemptRecord) -> Status | None: ...
