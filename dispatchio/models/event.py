from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dispatchio.models.enums import Status


class Event(BaseModel):
    """Current known state of an external event occurrence for a given run_key."""

    namespace: str = "default"
    name: str
    run_key: str
    status: Status = Status.DONE
    occurred_at: datetime = Field(default_factory=datetime.now)
    trace: dict[str, Any] = Field(default_factory=dict)

    event_id: int | None = None

    def is_finished(self) -> bool:
        return self.status in Status.finished()
