"""
StateStore.

Any backend (memory, filesystem, DynamoDB, S3 …) must implement this interface.
The orchestrator and CLI interact exclusively through this protocol, so backends
are fully interchangeable.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
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


class AmbiguousNamespaceError(Exception):
    """Raised when a state store operation is attempted without a namespace in a multi-orchestrator context."""

    def __init__(
        self, message: str = "Namespace is required for this operation but is not set."
    ) -> None:
        super().__init__(message)


class StateStore(ABC):
    """Read/write interface for job attempt state and dead-letter tracking."""

    def __init__(self, namespace: str | None = "default") -> None:
        """Initialize the state store filtered by namespace for multi-orchestrator support.

        If namespace is None, the store operates in a global mode without partitioning.
        """
        super().__init__()
        self._namespace = namespace

    @property
    def namespace(self) -> str | None:
        return self._namespace

    # ---------------------------------------------------------------------------
    # Attempt queries and mutations
    # ---------------------------------------------------------------------------

    @abstractmethod
    def get_latest_attempt(self, job_name: str, run_key: str) -> AttemptRecord | None:
        """
        Return the most recent attempt for (job_name, run_key).
        Returns None if no attempts exist for this key.

        Raises AmbiguousNamespaceError if namespace is not set.
        """
        ...

    @abstractmethod
    def get_attempt(self, correlation_id: str) -> AttemptRecord | None:
        """Return the attempt matching the given correlation_id, or None."""
        ...

    @abstractmethod
    def append_attempt(self, record: AttemptRecord) -> None:
        """
        Insert a new immutable attempt row.
        The attempt number is auto-allocated as max(attempt)+1 for the given
        (job_name, run_key), or 0 if no prior attempts exist.
        Raises if (job_name, run_key, attempt) would violate unique constraint.
        """
        ...

    @abstractmethod
    def update_attempt(self, record: AttemptRecord) -> None:
        """
        Update an existing attempt row by correlation_id.
        Used when changing status, adding trace metadata, or appending
        completion_event_trace.
        """
        ...

    @abstractmethod
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

    @abstractmethod
    def get_attempt_by_executor_trace(
        self, trace_key: str, trace_value: str
    ) -> AttemptRecord | None:
        """Find the first attempt whose trace.executor[trace_key] == trace_value."""
        ...

    # ---------------------------------------------------------------------------
    # Dead letter tracking
    # ---------------------------------------------------------------------------

    @abstractmethod
    def append_dead_letter(self, record: DeadLetterRecord) -> None:
        """Record a rejected completion event in the dead-letter log."""
        ...

    @abstractmethod
    def get_dead_letter(self, dead_letter_id: UUID) -> DeadLetterRecord | None:
        """Retrieve a dead-letter record by ID."""
        ...

    @abstractmethod
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

    @abstractmethod
    def append_retry_request(self, record: RetryRequest) -> None:
        """Record a manual retry request for audit."""
        ...

    @abstractmethod
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

    @abstractmethod
    def append_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """
        Insert a new orchestrator run record.
        Raises if run_key would violate unique constraint.
        """
        ...

    @abstractmethod
    def get_orchestrator_run(
        self, orchestrator_run_id: UUID
    ) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by its UUID."""
        ...

    @abstractmethod
    def get_orchestrator_run_by_key(self, run_key: str) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by run key."""
        ...

    @abstractmethod
    def list_orchestrator_runs(
        self,
        status: OrchestratorRunStatus | None = None,
        mode: OrchestratorRunMode | None = None,
    ) -> list[OrchestratorRunRecord]:
        """
        List orchestrator runs, optionally filtered by status and/or mode.
        Results sorted by opened_at DESC.
        """
        ...

    @abstractmethod
    def update_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """
        Update an existing orchestrator run record by orchestrator_run_id.
        Used when changing status, updating checkpoint, or recording activation/closure.
        """
        ...

    # ---------------------------------------------------------------------------
    # Event
    # ---------------------------------------------------------------------------

    @abstractmethod
    def set_event(self, event: Event) -> None:
        """Upsert an event by (namespace, name, run_key). Last write wins.

        The event's own namespace field determines the target orchestrator.
        The store's namespace is not used — callers can address any namespace.
        """
        ...

    @abstractmethod
    def get_event(self, name: str, run_key: str) -> Event | None:
        """Return the event for (namespace, name, run_key), or None.

        Filters by the store's namespace. Raises AmbiguousNamespaceError if
        the store has no namespace set.
        """
        ...

    @abstractmethod
    def list_events(self, run_key: str | None = None) -> list[Event]:
        """List events for this store's namespace, optionally filtered by run_key.

        If the store has no namespace set, returns events across all namespaces.
        """
        ...
