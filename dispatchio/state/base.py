"""
StateStore protocol.

Any backend (memory, filesystem, DynamoDB, S3 …) must implement this interface.
The orchestrator and CLI interact exclusively through this protocol, so backends
are fully interchangeable.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from dispatchio.models import AttemptRecord, DeadLetterRecord, RetryRequest, Status


@runtime_checkable
class StateStore(Protocol):
    """Read/write interface for job attempt state and dead-letter tracking."""

    # ---------------------------------------------------------------------------
    # Attempt queries and mutations
    # ---------------------------------------------------------------------------

    def get_latest_attempt(
        self, job_name: str, logical_run_id: str
    ) -> AttemptRecord | None:
        """
        Return the most recent attempt for (job_name, logical_run_id).
        Returns None if no attempts exist for this key.
        """
        ...

    def get_attempt(self, dispatchio_attempt_id: UUID) -> AttemptRecord | None:
        """Return the attempt matching the given dispatchio_attempt_id, or None."""
        ...

    def append_attempt(self, record: AttemptRecord) -> None:
        """
        Insert a new immutable attempt row.
        The attempt number is auto-allocated as max(attempt)+1 for the given
        (job_name, logical_run_id), or 0 if no prior attempts exist.
        Raises if (job_name, logical_run_id, attempt) would violate unique constraint.
        """
        ...

    def update_attempt(self, record: AttemptRecord) -> None:
        """
        Update an existing attempt row by dispatchio_attempt_id.
        Used when changing status, adding trace metadata, or appending
        completion_event_trace.
        """
        ...

    def list_attempts(
        self,
        job_name: str | None = None,
        logical_run_id: str | None = None,
        attempt: int | None = None,
        status: Status | None = None,
    ) -> list[AttemptRecord]:
        """
        Return all attempts matching the given filters.
        If no filters provided, returns all attempts.
        Results sorted by (job_name, logical_run_id, attempt DESC) for consistent output.
        """
        ...

    def get_attempt_by_executor_trace(
        self, trace_key: str, trace_value: str
    ) -> AttemptRecord | None:
        """Find the first attempt whose trace.executor[trace_key] == trace_value."""
        ...

    # ---------------------------------------------------------------------------
    # Dead letter tracking
    # ---------------------------------------------------------------------------

    def append_dead_letter(self, record: DeadLetterRecord) -> None:
        """Record a rejected completion event in the dead-letter log."""
        ...

    def get_dead_letter(self, dead_letter_id: UUID) -> DeadLetterRecord | None:
        """Retrieve a dead-letter record by ID."""
        ...

    def list_dead_letters(
        self,
        job_name: str | None = None,
        status: str | None = None,
    ) -> list[DeadLetterRecord]:
        """Return dead-letter records, optionally filtered by job_name and status."""
        ...

    # ---------------------------------------------------------------------------
    # Retry request audit
    # ---------------------------------------------------------------------------

    def append_retry_request(self, record: RetryRequest) -> None:
        """Record a manual retry request for audit."""
        ...

    def list_retry_requests(
        self,
        logical_run_id: str | None = None,
        requested_by: str | None = None,
    ) -> list[RetryRequest]:
        """Return retry request audit records, optionally filtered."""
        ...
