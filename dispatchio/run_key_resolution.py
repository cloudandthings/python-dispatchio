"""
Run key resolution — converts a Cadence to a concrete run_key string.

The run_key is the key used to store and look up a job's state for a given
logical period. Examples: "D20250115" (daily), "M202501" (monthly),
"W20250113" (weekly — Monday of that week), "H2025011509" (hourly).
"""

from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchio.cadence import Cadence


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
    year = dt.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    max_day = calendar.monthrange(year, month)[1]
    return dt.replace(year=year, month=month, day=min(dt.day, max_day))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_run_key(cadence: Cadence, reference_time: datetime) -> str:
    """
    Resolve a Cadence to a concrete run_key string.

    >>> from dispatchio.cadence import DAILY, MONTHLY, WEEKLY, HOURLY, YESTERDAY
    >>> from datetime import datetime
    >>> ref = datetime(2025, 1, 15, 2, 30)
    >>> resolve_run_key(DAILY, ref)
    'D20250115'
    >>> resolve_run_key(YESTERDAY, ref)
    'D20250114'
    >>> resolve_run_key(MONTHLY, ref)
    'M202501'
    >>> resolve_run_key(WEEKLY, ref)
    'W20250113'
    >>> resolve_run_key(HOURLY, ref)
    'H2025011502'
    """
    from dispatchio.cadence import (
        DateCadence,
        FixedCadence,
        Frequency,
        IncrementalCadence,
    )

    if isinstance(cadence, FixedCadence):
        return cadence.value

    if isinstance(cadence, IncrementalCadence):
        raise NotImplementedError(
            f"IncrementalCadence requires MetadataStore. Key: {cadence.key!r}"
        )

    if isinstance(cadence, DateCadence):
        lookback = -cadence.offset  # negative offset → positive lookback
        freq = cadence.frequency

        if freq == Frequency.DAILY:
            return "D" + (reference_time - timedelta(days=lookback)).strftime("%Y%m%d")

        if freq == Frequency.MONTHLY:
            return "M" + _subtract_months(reference_time, lookback).strftime("%Y%m")

        if freq == Frequency.WEEKLY:
            monday = reference_time - timedelta(days=reference_time.weekday())
            return "W" + (monday - timedelta(weeks=lookback)).strftime("%Y%m%d")

        if freq == Frequency.HOURLY:
            return "H" + (reference_time - timedelta(hours=lookback)).strftime(
                "%Y%m%d%H"
            )

    raise ValueError(f"Cannot resolve cadence: {cadence!r}")  # pragma: no cover


def describe_cadence(cadence: Cadence) -> str:
    """Human-readable description of a cadence (used in CLI/log output)."""
    from dispatchio.cadence import DateCadence, FixedCadence, IncrementalCadence

    if isinstance(cadence, FixedCadence):
        return f"literal '{cadence.value}'"
    if isinstance(cadence, IncrementalCadence):
        return f"incremental[{cadence.key!r}]"
    if isinstance(cadence, DateCadence):
        freq = cadence.frequency.value
        offset = cadence.offset
        if offset == 0:
            return {
                "hourly": "current hour",
                "daily": "today",
                "weekly": "current week",
                "monthly": "current month",
            }.get(freq, freq)
        if offset == -1:
            return {
                "hourly": "previous hour",
                "daily": "yesterday",
                "weekly": "last week",
                "monthly": "last month",
            }.get(freq, f"{freq} (offset {offset})")
        n = -offset
        unit = {"hourly": "hour", "daily": "day", "weekly": "week", "monthly": "month"}[
            freq
        ]
        return f"{n} {unit}{'s' if n > 1 else ''} ago"
    return repr(cadence)  # pragma: no cover


def resolve_orchestrator_run_key(
    orchestrator_cadence: Cadence, reference_time: datetime
) -> str:
    """
    Resolve an orchestrator's cadence to a concrete orchestrator run key.

    The orchestrator run key identifies one scheduling unit for the orchestrator.
    It's the outer container within which all jobs run.

    Examples:
        For an orchestrator with DAILY cadence and reference_time=2025-01-15:
            → "20250115"

        For an orchestrator with WEEKLY cadence and reference_time=2025-01-15:
            → "20250113" (Monday of that week)

        For an orchestrator with MONTHLY cadence and reference_time=2025-01-15:
            → "202501"

    Note: This is analogous to resolve_run_key(), but operates at the
    orchestrator level. Unlike run keys (which have a prefix like "D", "W", "M"),
    orchestrator run keys are typically just date strings for date-based orchestrators.

    Args:
        orchestrator_cadence: The cadence defining the orchestrator's scheduling unit.
        reference_time: The wall-clock time from which to compute the run key.

    Returns:
        A string identifying the orchestrator run (e.g., "20250115" for daily).

    Raises:
        NotImplementedError: For IncrementalCadence (requires MetadataStore).
        ValueError: For unsupported cadence types.
    """
    from dispatchio.cadence import (
        DateCadence,
        FixedCadence,
        Frequency,
        IncrementalCadence,
    )

    # Fixed cadences return their literal value
    if isinstance(orchestrator_cadence, FixedCadence):
        return orchestrator_cadence.value

    # Incremental cadences require metadata store (future)
    if isinstance(orchestrator_cadence, IncrementalCadence):
        raise NotImplementedError(
            f"IncrementalCadence requires MetadataStore. Key: {orchestrator_cadence.key!r}"
        )

    # Date-based cadences
    if isinstance(orchestrator_cadence, DateCadence):
        lookback = -orchestrator_cadence.offset
        freq = orchestrator_cadence.frequency

        if freq == Frequency.DAILY:
            return (reference_time - timedelta(days=lookback)).strftime("%Y%m%d")

        if freq == Frequency.MONTHLY:
            return _subtract_months(reference_time, lookback).strftime("%Y%m")

        if freq == Frequency.WEEKLY:
            monday = reference_time - timedelta(days=reference_time.weekday())
            return (monday - timedelta(weeks=lookback)).strftime("%Y%m%d")

        if freq == Frequency.HOURLY:
            return (reference_time - timedelta(hours=lookback)).strftime("%Y%m%d%H")

    raise ValueError(f"Cannot resolve orchestrator cadence: {orchestrator_cadence!r}")


def resolve_run_key_from_orchestrator(
    orchestrator_run_key: str, job_cadence: Cadence
) -> str:
    """
    Given an orchestrator run key and a job's cadence, resolve the job's run key.

    For date-based orchestrators:
        - If the job cadence is DAILY and orchestrator run key is "20250115":
          → Run key is "D20250115"
        - If the job cadence is MONTHLY and orchestrator run key is "20250115":
          → Run key is "M202501"
        - If the job cadence is YESTERDAY and orchestrator run key is "20250115":
          → Run key is "D20250114"

    For non-date orchestrators (e.g., event-based with run key "event:12345"):
        - Non-fixed job cadences will raise an error (you can't derive a date from an event key).
        - Fixed job cadences return their literal value.

    Args:
        orchestrator_run_key: The resolved orchestrator run key (e.g., "20250115").
        job_cadence: The job's cadence (e.g., DAILY, YESTERDAY, MONTHLY).

    Returns:
        A string identifying the job's run (e.g., "D20250115").

    Raises:
        ValueError: If the run key can't be parsed as a date (e.g., event-based
                    orchestrator with non-fixed job cadence).
        NotImplementedError: For IncrementalCadence.
    """
    from dispatchio.cadence import (
        DateCadence,
        FixedCadence,
        Frequency,
        IncrementalCadence,
    )

    # Fixed cadences always return their literal value, regardless of orchestrator context
    if isinstance(job_cadence, FixedCadence):
        return job_cadence.value

    # Incremental cadences require metadata store (future)
    if isinstance(job_cadence, IncrementalCadence):
        raise NotImplementedError(
            f"IncrementalCadence requires MetadataStore. Key: {job_cadence.key!r}"
        )

    # For date-based job cadences, try to parse the orchestrator run key as a date
    if isinstance(job_cadence, DateCadence):
        # Try to parse the orchestrator run key as a date.
        # Valid formats: "YYYYMMDD" (8 chars), "YYYYMM" (6 chars), "YYYYMMDDhh" (10 chars)
        if len(orchestrator_run_key) == 8 and orchestrator_run_key.isdigit():
            # Parse as YYYYMMDD
            try:
                orchestrator_date = datetime.strptime(orchestrator_run_key, "%Y%m%d")
            except ValueError as e:
                raise ValueError(
                    f"Cannot parse orchestrator run key as date: {orchestrator_run_key!r}"
                ) from e
        elif len(orchestrator_run_key) == 6 and orchestrator_run_key.isdigit():
            # Parse as YYYYMM (assume first day of month)
            try:
                orchestrator_date = datetime.strptime(
                    orchestrator_run_key + "01", "%Y%m%d"
                )
            except ValueError as e:
                raise ValueError(
                    f"Cannot parse orchestrator run key as month: {orchestrator_run_key!r}"
                ) from e
        elif len(orchestrator_run_key) == 10 and orchestrator_run_key.isdigit():
            # Parse as YYYYMMDDhh
            try:
                orchestrator_date = datetime.strptime(orchestrator_run_key, "%Y%m%d%H")
            except ValueError as e:
                raise ValueError(
                    f"Cannot parse orchestrator run key as datetime: {orchestrator_run_key!r}"
                ) from e
        else:
            raise ValueError(
                f"Orchestrator run key {orchestrator_run_key!r} does not match date format "
                f"(YYYYMMDD, YYYYMM, or YYYYMMDDhh). Cannot resolve job cadence {job_cadence!r}."
            )

        # Now resolve the run key from the parsed date using the job cadence
        lookback = -job_cadence.offset
        freq = job_cadence.frequency

        if freq == Frequency.DAILY:
            result_date = orchestrator_date - timedelta(days=lookback)
            return "D" + result_date.strftime("%Y%m%d")

        if freq == Frequency.MONTHLY:
            result_date = _subtract_months(orchestrator_date, lookback)
            return "M" + result_date.strftime("%Y%m")

        if freq == Frequency.WEEKLY:
            monday = orchestrator_date - timedelta(days=orchestrator_date.weekday())
            result_date = monday - timedelta(weeks=lookback)
            return "W" + result_date.strftime("%Y%m%d")

        if freq == Frequency.HOURLY:
            result_date = orchestrator_date - timedelta(hours=lookback)
            return "H" + result_date.strftime("%Y%m%d%H")

    raise ValueError(
        f"Cannot resolve run key from orchestrator run key {orchestrator_run_key!r} "
        f"with job cadence {job_cadence!r}"
    )
