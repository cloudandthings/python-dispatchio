"""
StateStore protocol.

Any backend (memory, filesystem, DynamoDB, S3 …) must implement this interface.
The orchestrator and CLI interact exclusively through this protocol, so backends
are fully interchangeable.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable
from uuid import UUID

from dispatchio.models import (
    AttemptRecord,
    DeadLetterRecord,
    Event,
    OrchestratorRunRecord,
    OrchestratorRunStatus,
    OrchestratorRunMode,
    RetryRequest,
    Status,
)


@runtime_checkable
class StateStore(Protocol):
    """Read/write interface for job attempt state and dead-letter tracking."""

    # ---------------------------------------------------------------------------
    # Attempt queries and mutations
    # ---------------------------------------------------------------------------

    def get_latest_attempt(self, job_name: str, run_key: str) -> AttemptRecord | None:
        """
        Return the most recent attempt for (job_name, run_key).
        Returns None if no attempts exist for this key.
        """
        ...

    def get_attempt(self, correlation_id: str) -> AttemptRecord | None:
        """Return the attempt matching the given correlation_id, or None."""
        ...

    def append_attempt(self, record: AttemptRecord) -> None:
        """
        Insert a new immutable attempt row.
        The attempt number is auto-allocated as max(attempt)+1 for the given
        (job_name, run_key), or 0 if no prior attempts exist.
        Raises if (job_name, run_key, attempt) would violate unique constraint.
        """
        ...

    def update_attempt(self, record: AttemptRecord) -> None:
        """
        Update an existing attempt row by correlation_id.
        Used when changing status, adding trace metadata, or appending
        completion_event_trace.
        """
        ...

    def list_attempts(
        self,
        job_name: str | None = None,
        run_key: str | None = None,
        attempt: int | None = None,
        status: Status | None = None,
    ) -> list[AttemptRecord]:
        """
        Return all attempts matching the given filters.
        If no filters provided, returns all attempts.
        Results sorted by (job_name, run_key, attempt DESC) for consistent output.
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
        run_key: str | None = None,
        requested_by: str | None = None,
    ) -> list[RetryRequest]:
        """Return retry request audit records, optionally filtered."""
        ...

    # ---------------------------------------------------------------------------
    # Orchestrator run management
    # ---------------------------------------------------------------------------

    def append_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """
        Insert a new orchestrator run record.
        Raises if (orchestrator_name, run_key) would violate unique constraint.
        """
        ...

    def get_orchestrator_run(
        self, orchestrator_run_id: UUID
    ) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by its UUID."""
        ...

    def get_orchestrator_run_by_key(
        self, orchestrator_name: str, run_key: str
    ) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by orchestrator name and run key."""
        ...

    def list_orchestrator_runs(
        self,
        orchestrator_name: str | None = None,
        status: OrchestratorRunStatus | None = None,
        mode: OrchestratorRunMode | None = None,
    ) -> list[OrchestratorRunRecord]:
        """
        List orchestrator runs, optionally filtered by orchestrator name,
        status, and/or mode. Results sorted by (orchestrator_name, opened_at DESC).
        """
        ...

    def update_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """
        Update an existing orchestrator run record by orchestrator_run_id.
        Used when changing status, updating checkpoint, or recording activation/closure.
        """
        ...

    # ---------------------------------------------------------------------------
    # Event
    # ---------------------------------------------------------------------------

    def set_event(self, event: Event) -> None:
        """Upsert an event by (name, run_key). Last write wins."""
        ...

    def get_event(self, name: str, run_key: str) -> Event | None:
        """Return the event for (name, run_key), or None."""
        ...

    def list_events(self, run_key: str | None = None) -> list[Event]:
        """List all events, optionally filtered by run_key."""
        ...
