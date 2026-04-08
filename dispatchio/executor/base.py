"""
Executor protocol.

An Executor takes a Job and a resolved run_id and actually
runs the job. It is fire-and-forget: submit() kicks the job off and
returns immediately. The job reports completion separately via the
receiver layer.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from dispatchio.models import Job


@runtime_checkable
class Executor(Protocol):
    def submit(
        self,
        job: Job,
        run_id: str,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        """
        Submit the job for execution.
        Should raise on hard failure (e.g. cannot reach the executor).
        Must NOT block waiting for the job to complete.

        timeout: per-submission deadline in seconds. Not yet enforced by
                 local executors; reserved for cloud executors (e.g. ECS)
                 where the API call itself may be slow or rate-limited.
        """
        ...
