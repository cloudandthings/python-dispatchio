from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from dispatchio.models.enums import (
    DeadLetterReasonCode,
    DeadLetterSourceBackend,
    DeadLetterStatus,
)


class DeadLetter(BaseModel):
    """
    A completion event that was rejected (dead-lettered).

    All identity fields are best-effort — a CORRELATION_FAILURE may leave
    job_name, run_key, attempt, and correlation_id unpopulated.
    """

    dead_letter_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime
    source_backend: DeadLetterSourceBackend
    reason_code: DeadLetterReasonCode
    reason_detail: str | None = None
    status: DeadLetterStatus = DeadLetterStatus.OPEN
    namespace: str = "default"
    job_name: str | None = None
    run_key: str | None = None
    attempt: int | None = None
    correlation_id: UUID | None = None
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    resolved_at: datetime | None = None
    resolver_notes: str | None = None
