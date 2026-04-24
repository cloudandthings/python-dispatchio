from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from dispatchio.models.enums import Status, TriggerType


class Attempt(BaseModel):
    """
    A single immutable execution instance of a job for a given run_key.

    One job run can have multiple attempts due to retries. Attempts are
    numbered sequentially starting at 0.
    """

    namespace: str = "default"
    job_name: str
    run_key: str
    attempt: int
    correlation_id: UUID
    status: Status
    reason: str | None = None
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    trigger_type: TriggerType = TriggerType.SCHEDULED
    trigger_reason: str | None = None
    trace: dict[str, Any] = Field(default_factory=dict)
    completion_event_trace: dict[str, Any] | None = None
    requested_by: str | None = None

    def is_finished(self) -> bool:
        return self.status in Status.finished()

    def is_active(self) -> bool:
        return self.status in Status.active()
