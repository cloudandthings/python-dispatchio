"""
Cadence — the run_id resolution strategy for a job or dependency.

Replaces the legacy ``run_id_expr`` string grammar ("day0", "mon1", …) with
typed models that are easier to validate, document, and extend.

Public API (re-exported from dispatchio):
    Frequency           — HOURLY / DAILY / WEEKLY / MONTHLY
    DateCadence         — one run per calendar period, optional signed offset
    FixedCadence        — static run_id (ad-hoc / event-driven)
    IncrementalCadence  — batch-triggered; deferred to Phase 4
    Cadence             — Annotated union (Pydantic discriminator)

    HOURLY / DAILY / WEEKLY / MONTHLY   — zero-offset DateCadence constants
    YESTERDAY / LAST_WEEK / LAST_MONTH  — lookback shorthands
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class Frequency(str, Enum):
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class DateCadence(BaseModel):
    """Date-based cadence — one run per calendar period."""

    type: Literal["date"] = "date"
    frequency: Frequency = Frequency.DAILY
    offset: int = 0
    # 0 = current period, -1 = previous, -2 = two ago, …
    # Positive values reference future periods (rare).


class FixedCadence(BaseModel):
    """Fixed run_id — for ad-hoc / event-driven jobs."""

    type: Literal["literal"] = "literal"
    value: str


class IncrementalCadence(BaseModel):
    """
    Batch-triggered — run_id sourced from MetadataStore.
    Implementation deferred to Phase 4 (MetadataStore required).
    """

    type: Literal["incremental"] = "incremental"
    key: str  # MetadataStore key written by a discovery job


Cadence = Annotated[
    DateCadence | FixedCadence | IncrementalCadence,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Convenience constants
# ---------------------------------------------------------------------------

HOURLY = DateCadence(frequency=Frequency.HOURLY)
DAILY = DateCadence(frequency=Frequency.DAILY)
WEEKLY = DateCadence(frequency=Frequency.WEEKLY)
MONTHLY = DateCadence(frequency=Frequency.MONTHLY)

YESTERDAY = DateCadence(frequency=Frequency.DAILY, offset=-1)
LAST_WEEK = DateCadence(frequency=Frequency.WEEKLY, offset=-1)
LAST_MONTH = DateCadence(frequency=Frequency.MONTHLY, offset=-1)
