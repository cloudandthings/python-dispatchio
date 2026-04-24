"""GraphSpec — versioned JSON graph artifact model.

The envelope for a dispatchio graph artifact. Pydantic validation covers
field-level constraints; graph-level invariants (cycles, missing deps) are
checked separately by validate_graph().
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from dispatchio.cadence import Cadence
from dispatchio.models import Job


class ProducerInfo(BaseModel):
    """Metadata about the tool or service that produced this artifact."""

    model_config = ConfigDict(extra="forbid")

    name: str
    version: str


class GraphExternalDependency(BaseModel):
    """Declaration of an external dependency (event.* or external.*) used by jobs in this graph.

    Declaring external dependencies here allows validate_graph() to confirm
    that every external job_name referenced in depends_on is intentional,
    and signals to orchestrator_from_graph() to use strict_dependencies=False.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    cadence: Cadence
    description: str | None = None

    @field_validator("name")
    @classmethod
    def _must_be_external_prefix(cls, v: str) -> str:
        if not (v.startswith("event.") or v.startswith("external.")):
            raise ValueError(
                f"External dependency name {v!r} must start with 'event.' or 'external.'."
            )
        return v


class GraphSpec(BaseModel):
    """Versioned, strongly-typed envelope for a dispatchio graph artifact.

    Unknown fields are rejected (extra="forbid") so that malformed artifacts
    fail loudly on load rather than silently at runtime.

    graph_version is a Literal type: Pydantic rejects unsupported versions
    with a clear error naming the valid values. Extend to Literal["1", "2"]
    when a new version is needed.
    """

    model_config = ConfigDict(extra="forbid")

    graph_version: Literal["1"]
    name: str
    generated_at: datetime
    producer: ProducerInfo | None = None
    jobs: list[Job]
    external_dependencies: list[GraphExternalDependency] = Field(default_factory=list)
