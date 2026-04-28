"""
DateContext — lazy date variable resolver for parametrized job runs.

Provides a dict-like interface that resolves date variable strings on demand.
Variable names follow the pattern:

    {period}{N}[_{anchor}]_{format}[{delimiter}]

Periods:
    day  — calendar day
    wk   — week (anchored to week_start_day)
    mon  — calendar month
    qtr  — calendar quarter (anchored to quarter_start_month)
    yr   — calendar year

Offset (N):
    0 = current period, 1 = previous, 2 = two ago, …

Anchor (optional, for multi-day periods):
    first — first calendar day of the period
    last  — last calendar day of the period
    (omitted) — same as first for yyyymmdd; irrelevant for yyyymm, yyyy, etc.

Format:
    yyyymmdd  — full date (e.g. 20260428)
    yyyymm    — year + month (e.g. 202604)
    yyyyww    — year + ISO week number (e.g. 202617)
    yyyyq     — year + quarter number (e.g. 20262)
    yyyy      — 4-digit year
    mm        — 2-digit month
    dd        — 2-digit day
    ww        — 2-digit ISO week number
    q         — quarter digit (1–4)

Delimiter (optional suffix on format):
    D — dash     (YYYY-MM-DD)
    S — space    (YYYY MM DD)
    L — slash    (YYYY/MM/DD)

Examples:
    day0_yyyymmddD   → today as YYYY-MM-DD
    day5_yyyymm      → YYYYMM of the date 5 days ago
    mon0_first_yyyymmddD → first of current month as YYYY-MM-DD
    mon1_last_yyyymmddD  → last of previous month as YYYY-MM-DD
    qtr0_first_yyyymmddD → first of current quarter as YYYY-MM-DD
"""

from __future__ import annotations

import calendar
import re
from datetime import date, datetime, timedelta

_KEY_RE = re.compile(
    r"^(day|wk|mon|qtr|yr)(\d+)"
    r"(?:_(first|last))?"
    r"_(yyyymmdd|yyyymm|yyyyww|yyyyq|yyyy|mm|dd|ww|q)"
    r"([DSL]?)$"
)

_DELIMITERS: dict[str, str] = {"D": "-", "S": " ", "L": "/", "": ""}


def _subtract_months(d: date, n: int) -> date:
    """Subtract n months from date d, clamping day to end of resulting month."""
    month = d.month - n
    year = d.year + (month - 1) // 12
    month = ((month - 1) % 12) + 1
    max_day = calendar.monthrange(year, month)[1]
    return d.replace(year=year, month=month, day=min(d.day, max_day))


def _last_of_month(d: date) -> date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def _current_quarter_start(d: date, qsm: int) -> date:
    """First day of the quarter containing d, given quarter_start_month."""
    months_since_quarter_start_month = (d.month - qsm) % 12
    q_offset = (months_since_quarter_start_month // 3) * 3
    qs_month = (qsm - 1 + q_offset) % 12 + 1
    qs_year = d.year
    if qs_month > d.month:
        qs_year -= 1
    return date(qs_year, qs_month, 1)


def _apply_format(d: date, fmt: str, delim: str, qsm: int) -> str:
    sep = _DELIMITERS[delim]
    if fmt == "yyyymmdd":
        if sep:
            return f"{d.year:04d}{sep}{d.month:02d}{sep}{d.day:02d}"
        return d.strftime("%Y%m%d")
    if fmt == "yyyymm":
        if sep:
            return f"{d.year:04d}{sep}{d.month:02d}"
        return d.strftime("%Y%m")
    if fmt == "yyyyww":
        iso_year, iso_week, _ = d.isocalendar()
        if sep:
            return f"{iso_year:04d}{sep}{iso_week:02d}"
        return f"{iso_year:04d}{iso_week:02d}"
    if fmt == "yyyyq":
        q = (d.month - qsm) % 12 // 3 + 1
        if sep:
            return f"{d.year:04d}{sep}{q}"
        return f"{d.year:04d}{q}"
    if fmt == "yyyy":
        return f"{d.year:04d}"
    if fmt == "mm":
        return f"{d.month:02d}"
    if fmt == "dd":
        return f"{d.day:02d}"
    if fmt == "ww":
        _, iso_week, _ = d.isocalendar()
        return f"{iso_week:02d}"
    if fmt == "q":
        return str((d.month - qsm) % 12 // 3 + 1)
    raise KeyError(fmt)  # pragma: no cover


class DateContext:
    """
    Lazy date variable resolver.

    Use as a read-only dict: ctx["day0_yyyymmddD"], ctx["mon1_first_yyyymmddD"], etc.

    Args:
        reference_time: The logical reference time for variable resolution.
        week_start_day: Day the week starts (0=Monday, 6=Sunday). Default: 0.
        quarter_start_month: First month of Q1 (1=January). Default: 1.
    """

    __slots__ = ("_reference_date", "_week_start_day", "_quarter_start_month", "_cache")

    def __init__(
        self,
        reference_time: datetime,
        *,
        week_start_day: int = 0,
        quarter_start_month: int = 1,
    ) -> None:
        self._reference_date = reference_time.date()
        self._week_start_day = week_start_day
        self._quarter_start_month = quarter_start_month
        self._cache: dict[str, str] = {}

    def __getitem__(self, key: str) -> str:
        if key in self._cache:
            return self._cache[key]

        m = _KEY_RE.match(key)
        if m is None:
            raise KeyError(f"Unknown date context variable: {key!r}")

        period, n_str, anchor, fmt, delim = m.groups()
        n = int(n_str)

        # Compute the base (period-start) date
        ref = self._reference_date
        if period == "day":
            base = ref - timedelta(days=n)
        elif period == "wk":
            # Monday of ref's week, then subtract n weeks
            week_start = ref - timedelta(
                days=(ref.weekday() - self._week_start_day) % 7
            )
            base = week_start - timedelta(weeks=n)
        elif period == "mon":
            base = _subtract_months(ref, n).replace(day=1)
        elif period == "qtr":
            q_start = _current_quarter_start(ref, self._quarter_start_month)
            base = _subtract_months(q_start, n * 3)
        elif period == "yr":
            base = date(ref.year - n, 1, 1)
        else:  # pragma: no cover
            raise KeyError(key)

        # Apply anchor for non-day periods
        if period != "day":
            if anchor == "last":
                if period == "mon":
                    base = _last_of_month(base)
                elif period == "wk":
                    # Last day of week = base + (6 if Mon-start else varies)
                    base = base + timedelta(days=6)
                elif period == "qtr":
                    # Last day of quarter = last of base + 2 months
                    base = _last_of_month(_subtract_months(base, -2))
                elif period == "yr":
                    base = date(base.year, 12, 31)
            # first / no anchor → base is already at period start

        result = _apply_format(base, fmt, delim, self._quarter_start_month)
        self._cache[key] = result
        return result

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        return _KEY_RE.match(key) is not None

    def get(self, key: str, default: str | None = None) -> str | None:
        try:
            return self[key]
        except KeyError:
            return default
