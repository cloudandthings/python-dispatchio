from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class RetryRequest(BaseModel):
    """
    Audit record for a manual retry request.

    Created once per `dispatchio retry create` invocation so operators can
    answer: who requested a retry, why, and which attempt numbers were assigned.
    """

    id: int | None = None
    requested_at: datetime
    requested_by: str
    run_key: str
    requested_jobs: list[str] = Field(default_factory=list)
    cascade: bool = True
    reason: str | None = None
    selected_jobs: list[str] = Field(default_factory=list)
    assigned_attempt_by_job: dict[str, int] = Field(default_factory=dict)
