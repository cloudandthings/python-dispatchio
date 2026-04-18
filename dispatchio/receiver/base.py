"""
CompletionReceiver protocol + CompletionEvent model.

Jobs push their status back to Dispatchio by emitting a CompletionEvent.
The receiver layer abstracts HOW that event arrives (filesystem drop,
HTTP POST, SQS message, etc.) from the orchestrator's perspective.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, Field, field_validator

from dispatchio.models import Status


class CompletionEvent(BaseModel):
    """
    Minimal payload a job must send to signal its status.

    Phase 2 Required fields (strict completion correlation):
        job_name              — must match a Job.name exactly
        logical_run_id        — the logical_run_id this job was started with
        attempt               — the attempt number (0, 1, 2, ...)
        dispatchio_attempt_id — the UUID assigned at submission
        status                — DONE | ERROR | RUNNING | LOST | CANCELLED

    Optional fields:
        reason                — optional reason for terminal states

    Backward compat (deprecated, map to required fields):
        run_id                — alias for logical_run_id
        error_reason          — alias for reason (when status=ERROR)
        metadata              — reserved for future use
    """

    job_name: str
    status: Status
    # Phase 2 required fields (for strict completion correlation)
    logical_run_id: str | None = None
    attempt: int | None = None
    dispatchio_attempt_id: UUID | None = None
    reason: str | None = None
    # Backward compat fields (deprecated; map to primary fields)
    run_id: str | None = None  # alias for logical_run_id
    error_reason: str | None = None  # alias for reason
    metadata: dict[str, Any] = Field(default_factory=dict)
    trace: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime | None = None  # defaults to now() if omitted

    @field_validator("logical_run_id", mode="before")
    @classmethod
    def _populate_logical_run_id(cls, v: Any, info) -> str | None:
        """If logical_run_id not provided, use run_id (backward compat)."""
        if v is not None:
            return v
        return info.data.get("run_id")

    @field_validator("attempt", mode="after")
    @classmethod
    def _validate_attempt(cls, v: int | None) -> int | None:
        """Attempt must be non-negative when provided."""
        if v is not None and v < 0:
            raise ValueError(f"attempt must be >= 0, got {v}")
        return v

    @field_validator("reason", mode="before")
    @classmethod
    def _populate_reason(cls, v: Any, info) -> str | None:
        """If reason not provided, use error_reason (backward compat)."""
        if v is not None:
            return v
        return info.data.get("error_reason")

    def get_logical_run_id(self) -> str:
        """Return the logical_run_id, raising if not set."""
        result = self.logical_run_id or self.run_id
        if not result:
            raise ValueError(
                "logical_run_id (or run_id) is required in CompletionEvent"
            )
        return result

    def get_attempt(self) -> int:
        """Return the attempt, raising if not set."""
        if self.attempt is None:
            raise ValueError("attempt is required in Phase 2 CompletionEvent")
        return self.attempt

    def get_dispatchio_attempt_id(self) -> UUID:
        """Return the dispatchio_attempt_id, raising if not set."""
        if self.dispatchio_attempt_id is None:
            raise ValueError(
                "dispatchio_attempt_id is required in Phase 2 CompletionEvent"
            )
        return self.dispatchio_attempt_id


@runtime_checkable
class CompletionReceiver(Protocol):
    def drain(self) -> list[CompletionEvent]:
        """
        Return all pending completion events and clear them from the source.
        Called once per orchestrator tick (or independently by the consumer).
        Must be non-blocking — return [] if nothing is waiting.
        """
        ...
