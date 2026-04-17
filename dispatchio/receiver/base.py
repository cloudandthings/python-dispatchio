"""
CompletionReceiver protocol + CompletionEvent model.

Jobs push their status back to Dispatchio by emitting a CompletionEvent.
The receiver layer abstracts HOW that event arrives (filesystem drop,
HTTP POST, SQS message, etc.) from the orchestrator's perspective.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from dispatchio.models import Status


class CompletionEvent(BaseModel):
    """
    Minimal payload a job must send to signal its status.

    Fields:
        job_name     — must match a Job.name exactly
        run_id       — the run_id this job was started with
        status       — DONE | ERROR | RUNNING
        error_reason — optional message when status=ERROR
        metadata     — arbitrary key/value pairs stored on the RunRecord
    """

    job_name: str
    run_id: str
    status: Status
    error_reason: str | None = None
    metadata: dict[str, Any] = {}
    occurred_at: datetime | None = None  # defaults to now() if omitted


@runtime_checkable
class CompletionReceiver(Protocol):
    def drain(self) -> list[CompletionEvent]:
        """
        Return all pending completion events and clear them from the source.
        Called once per orchestrator tick (or independently by the consumer).
        Must be non-blocking — return [] if nothing is waiting.
        """
        ...
