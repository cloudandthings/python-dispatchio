from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field


class NamespaceIdentity(BaseModel):
    """Stable identity record for a namespace. Name is the mutable display label."""

    id: int | None = None
    name: str
    created_at: datetime = Field(default_factory=datetime.now)


class JobIdentity(BaseModel):
    """Stable identity record for a job within a namespace."""

    id: int | None = None
    namespace_id: int
    name: str
    created_at: datetime = Field(default_factory=datetime.now)


class EventIdentity(BaseModel):
    """Stable identity record for an event type within a namespace."""

    id: int | None = None
    namespace_id: int
    name: str
    created_at: datetime = Field(default_factory=datetime.now)
