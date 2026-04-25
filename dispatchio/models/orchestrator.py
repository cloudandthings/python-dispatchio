from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dispatchio.models.enums import OrchestratorRunMode, OrchestratorRunStatus


class OrchestratorRun(BaseModel):
    """
    A first-class persisted orchestrator run.

    Encapsulates one scheduling unit and tracks its lifecycle from submission
    through completion. Job attempts within the run share the same run_key.
    """

    id: int | None = None
    namespace: str
    run_key: str
    status: OrchestratorRunStatus
    mode: OrchestratorRunMode
    priority: int = 0
    submitted_by: str | None = None
    reason: str | None = None
    force: bool = False
    replay_group_id: int | None = None
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    opened_at: datetime
    activated_at: datetime | None = None
    closed_at: datetime | None = None
    namespace_id: int | None = None
