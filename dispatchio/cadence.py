"""
Cadence — the run_key resolution strategy for a job or dependency.

Public API (re-exported from dispatchio):
    Frequency           — HOURLY / DAILY / WEEKLY / MONTHLY
    DateCadence         — one run per calendar period, optional signed offset
    LiteralCadence      — static run_key (event-driven / explicit key)
    IncrementalCadence  — batch-triggered
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


class LiteralCadence(BaseModel):
    """Literal run_key — the value is used as-is, regardless of reference time.

    Use this when the run_key comes from outside the scheduler: an event ID,
    a user-supplied key, or an explicit date string like "D20260423".
    """

    type: Literal["literal"] = "literal"
    value: str


class IncrementalCadence(BaseModel):
    """
    Batch-triggered — run_key sourced from MetadataStore.
    """

    type: Literal["incremental"] = "incremental"
    key: str  # MetadataStore key written by a discovery job


Cadence = Annotated[
    DateCadence | LiteralCadence | IncrementalCadence,
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
