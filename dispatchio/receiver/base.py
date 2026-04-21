"""
StatusReceiver protocol + StatusEvent model.

Jobs push their status back to Dispatchio by emitting a StatusEvent.
The receiver layer abstracts HOW that event arrives (filesystem drop,
HTTP POST, SQS message, etc.) from the orchestrator's perspective.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from pydantic import BaseModel, Field

from dispatchio.models import Status


class StatusEvent(BaseModel):
    """
    Minimal payload a job must send to signal its status.

    Required fields (strict completion correlation):
        correlation_id — the string identifier assigned at submission
        status         — DONE | ERROR | RUNNING | LOST | CANCELLED

    Optional fields:
        reason         — optional reason for terminal states

    Backward compat (deprecated, map to required fields):
        metadata              — reserved for future use
    """

    correlation_id: UUID
    status: Status
    reason: str | None = None  # alias for reason

    metadata: dict[str, Any] = Field(default_factory=dict)
    trace: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=datetime.now)


@runtime_checkable
class StatusReceiver(Protocol):
    def drain(self) -> list[StatusEvent]:
        """
        Return all pending status events and clear them from the source.
        Called once per orchestrator tick (or independently by the consumer).
        Must be non-blocking — return [] if nothing is waiting.
        """
        ...
