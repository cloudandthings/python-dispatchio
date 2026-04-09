"""
StateStore protocol.

Any backend (memory, filesystem, DynamoDB, S3 …) must implement this interface.
The orchestrator and CLI interact exclusively through this protocol, so backends
are fully interchangeable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

from dispatchio.models import RunRecord, Status


@runtime_checkable
class StateStore(Protocol):
    """Read/write interface for job run state."""

    def get(self, job_name: str, run_id: str) -> RunRecord | None:
        """Return the RunRecord for (job_name, run_id), or None if not found."""
        ...

    def put(self, record: RunRecord) -> None:
        """Upsert a RunRecord. Overwrites any existing record for the same key."""
        ...

    def heartbeat(self, job_name: str, run_id: str, at: datetime | None = None) -> None:
        """
        Update last_heartbeat_at for a RUNNING job.
        Creates a RUNNING record if none exists (first heartbeat = implicit start).
        `at` defaults to datetime.utcnow() if not provided.
        """
        ...

    def list_records(
        self,
        job_name: str | None = None,
        status: Status | None = None,
    ) -> list[RunRecord]:
        """
        Return all records, optionally filtered by job_name and/or status.
        Results are sorted by (job_name, run_id) for consistent output.
        """
        ...
