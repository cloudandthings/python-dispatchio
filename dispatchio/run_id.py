"""
Run ID resolution — converts a Cadence to a concrete run_id string.

The run_id is the key used to store and look up a job's state for a given
logical period. Examples: "20250115" (daily), "202501" (monthly),
"20250113" (weekly — Monday of that week), "2025011509" (hourly).
"""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchio.cadence import Cadence, DateCadence, FixedCadence, Frequency


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _subtract_months(dt: datetime, n: int) -> datetime:
    """
    Apply a signed month offset to dt, clamping the day to the last of
    the resulting month (e.g. March 31 - 1 month = Feb 28/29).

    n > 0 → subtract months (look back)
    n < 0 → add months (look forward, rare)
    n = 0 → no change
    """
    month = dt.month - n
    year  = dt.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    max_day = calendar.monthrange(year, month)[1]
    return dt.replace(year=year, month=month, day=min(dt.day, max_day))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_run_id(cadence: "Cadence", reference_time: datetime) -> str:
    """
    Resolve a Cadence to a concrete run_id string.

    >>> from dispatchio.cadence import DAILY, MONTHLY, WEEKLY, HOURLY, YESTERDAY
    >>> from datetime import datetime
    >>> ref = datetime(2025, 1, 15, 2, 30)
    >>> resolve_run_id(DAILY, ref)
    '20250115'
    >>> resolve_run_id(YESTERDAY, ref)
    '20250114'
    >>> resolve_run_id(MONTHLY, ref)
    '202501'
    >>> resolve_run_id(WEEKLY, ref)
    '20250113'
    >>> resolve_run_id(HOURLY, ref)
    '2025011502'
    """
    from dispatchio.cadence import DateCadence, FixedCadence, Frequency, IncrementalCadence

    if isinstance(cadence, FixedCadence):
        return cadence.value

    if isinstance(cadence, IncrementalCadence):
        raise NotImplementedError(
            "IncrementalCadence requires MetadataStore (Phase 4). "
            f"Key: {cadence.key!r}"
        )

    if isinstance(cadence, DateCadence):
        lookback = -cadence.offset   # negative offset → positive lookback
        freq = cadence.frequency

        if freq == Frequency.DAILY:
            return (reference_time - timedelta(days=lookback)).strftime("%Y%m%d")

        if freq == Frequency.MONTHLY:
            return _subtract_months(reference_time, lookback).strftime("%Y%m")

        if freq == Frequency.WEEKLY:
            monday = reference_time - timedelta(days=reference_time.weekday())
            return (monday - timedelta(weeks=lookback)).strftime("%Y%m%d")

        if freq == Frequency.HOURLY:
            return (reference_time - timedelta(hours=lookback)).strftime("%Y%m%d%H")

    raise ValueError(f"Cannot resolve cadence: {cadence!r}")  # pragma: no cover


def describe_cadence(cadence: "Cadence") -> str:
    """Human-readable description of a cadence (used in CLI/log output)."""
    from dispatchio.cadence import DateCadence, FixedCadence, Frequency, IncrementalCadence

    if isinstance(cadence, FixedCadence):
        return f"literal '{cadence.value}'"
    if isinstance(cadence, IncrementalCadence):
        return f"incremental[{cadence.key!r}]"
    if isinstance(cadence, DateCadence):
        freq   = cadence.frequency.value
        offset = cadence.offset
        if offset == 0:
            return {
                "hourly": "current hour", "daily": "today",
                "weekly": "current week", "monthly": "current month",
            }.get(freq, freq)
        if offset == -1:
            return {
                "hourly": "previous hour", "daily": "yesterday",
                "weekly": "last week",     "monthly": "last month",
            }.get(freq, f"{freq} (offset {offset})")
        n    = -offset
        unit = {"hourly": "hour", "daily": "day",
                "weekly": "week", "monthly": "month"}[freq]
        return f"{n} {unit}{'s' if n > 1 else ''} ago"
    return repr(cadence)  # pragma: no cover
