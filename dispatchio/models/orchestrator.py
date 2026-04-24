from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

from dispatchio.models.enums import OrchestratorRunMode, OrchestratorRunStatus


class OrchestratorRun(BaseModel):
    """
    A first-class persisted orchestrator run.

    Encapsulates one scheduling unit and tracks its lifecycle from submission
    through completion. Job attempts within the run share the same run_key.
    """

    orchestrator_run_id: UUID = Field(default_factory=uuid4)
    namespace: str
    run_key: str
    status: OrchestratorRunStatus
    mode: OrchestratorRunMode
    priority: int = 0
    submitted_by: str | None = None
    reason: str | None = None
    force: bool = False
    replay_group_id: UUID | None = None
    checkpoint: dict[str, Any] = Field(default_factory=dict)
    opened_at: datetime
    activated_at: datetime | None = None
    closed_at: datetime | None = None
