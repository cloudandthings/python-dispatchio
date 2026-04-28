"""Unit tests for DateContext — lazy date variable resolution."""

from __future__ import annotations

from datetime import datetime

import pytest

from dispatchio.date_context import DateContext


def _ctx(year: int, month: int, day: int, **kwargs) -> DateContext:
    return DateContext(reference_time=datetime(year, month, day, 12, 0, 0), **kwargs)


# ---------------------------------------------------------------------------
# day period
# ---------------------------------------------------------------------------


class TestDayPeriod:
    def test_day0_yyyymmdd(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyymmdd"] == "20260428"

    def test_day0_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyymmddD"] == "2026-04-28"

    def test_day0_yyyymmddS(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyymmddS"] == "2026 04 28"

    def test_day0_yyyymmddL(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyymmddL"] == "2026/04/28"

    def test_day1_yyyymmdd(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day1_yyyymmdd"] == "20260427"

    def test_day5_yyyymmdd(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day5_yyyymmdd"] == "20260423"

    def test_day0_yyyymm(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyymm"] == "202604"

    def test_day5_yyyymm_same_month(self):
        # May 8 - 5 days = May 3 → same month
        ctx = _ctx(2026, 5, 8)
        assert ctx["day5_yyyymm"] == "202605"

    def test_day5_yyyymm_crosses_month(self):
        # May 3 - 5 days = April 28 → different month
        ctx = _ctx(2026, 5, 3)
        assert ctx["day5_yyyymm"] == "202604"

    def test_day0_yyyy(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_yyyy"] == "2026"

    def test_day0_mm(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_mm"] == "04"

    def test_day0_dd(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["day0_dd"] == "28"


# ---------------------------------------------------------------------------
# mon period
# ---------------------------------------------------------------------------


class TestMonPeriod:
    def test_mon0_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon0_first_yyyymmddD"] == "2026-04-01"

    def test_mon0_last_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon0_last_yyyymmddD"] == "2026-04-30"

    def test_mon1_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon1_first_yyyymmddD"] == "2026-03-01"

    def test_mon1_last_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon1_last_yyyymmddD"] == "2026-03-31"

    def test_mon0_yyyymm(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon0_yyyymm"] == "202604"

    def test_mon1_yyyymm(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon1_yyyymm"] == "202603"

    def test_feb_leap_year_last(self):
        ctx = _ctx(2024, 2, 15)
        assert ctx["mon0_last_yyyymmdd"] == "20240229"

    def test_feb_non_leap_year_last(self):
        ctx = _ctx(2025, 2, 15)
        assert ctx["mon0_last_yyyymmdd"] == "20250228"

    def test_wrap_year_previous_month(self):
        ctx = _ctx(2026, 1, 15)
        assert ctx["mon1_first_yyyymmddD"] == "2025-12-01"
        assert ctx["mon1_last_yyyymmddD"] == "2025-12-31"

    def test_mon0_yyyymmdd_no_anchor(self):
        # Without anchor, mon{n}_yyyymmdd should give first of month
        ctx = _ctx(2026, 4, 28)
        assert ctx["mon0_yyyymmdd"] == "20260401"


# ---------------------------------------------------------------------------
# wk period
# ---------------------------------------------------------------------------


class TestWkPeriod:
    def test_wk0_first_yyyymmddD(self):
        # 2026-04-28 is a Tuesday; Monday is 2026-04-27
        ctx = _ctx(2026, 4, 28)
        assert ctx["wk0_first_yyyymmddD"] == "2026-04-27"

    def test_wk1_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["wk1_first_yyyymmddD"] == "2026-04-20"

    def test_wk0_last_yyyymmddD(self):
        # Last of week (Monday start): Sunday = Monday + 6
        ctx = _ctx(2026, 4, 28)
        assert ctx["wk0_last_yyyymmddD"] == "2026-05-03"

    def test_wk0_yyyyww(self):
        ctx = _ctx(2026, 4, 28)
        # ISO week 18 of 2026
        assert ctx["wk0_yyyyww"] == "202618"

    def test_wk0_yyyymmdd_no_anchor(self):
        # Without anchor, wk{n}_yyyymmdd should give first of week
        ctx = _ctx(2026, 4, 28)
        assert ctx["wk0_yyyymmdd"] == "20260427"


# ---------------------------------------------------------------------------
# qtr period
# ---------------------------------------------------------------------------


class TestQtrPeriod:
    def test_qtr0_first_yyyymmddD(self):
        # Q2 starts April 1
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr0_first_yyyymmddD"] == "2026-04-01"

    def test_qtr0_last_yyyymmddD(self):
        # Q2 ends June 30
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr0_last_yyyymmddD"] == "2026-06-30"

    def test_qtr1_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr1_first_yyyymmddD"] == "2026-01-01"

    def test_qtr1_last_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr1_last_yyyymmddD"] == "2026-03-31"

    def test_qtr0_yyyyq(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr0_yyyyq"] == "20262"

    def test_qtr_q1(self):
        ctx = _ctx(2026, 1, 15)
        assert ctx["qtr0_first_yyyymmddD"] == "2026-01-01"
        assert ctx["qtr0_last_yyyymmddD"] == "2026-03-31"
        assert ctx["qtr0_yyyyq"] == "20261"

    def test_qtr0_yyyymmdd_no_anchor(self):
        # Without anchor, qtr{n}_yyyymmdd should give first of quarter
        ctx = _ctx(2026, 4, 28)
        assert ctx["qtr0_yyyymmdd"] == "20260401"


# ---------------------------------------------------------------------------
# yr period
# ---------------------------------------------------------------------------


class TestYrPeriod:
    def test_yr0_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["yr0_first_yyyymmddD"] == "2026-01-01"

    def test_yr0_last_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["yr0_last_yyyymmddD"] == "2026-12-31"

    def test_yr1_first_yyyymmddD(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["yr1_first_yyyymmddD"] == "2025-01-01"

    def test_yr0_yyyy(self):
        ctx = _ctx(2026, 4, 28)
        assert ctx["yr0_yyyy"] == "2026"


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


def test_caching():
    ctx = _ctx(2026, 4, 28)
    result1 = ctx["day0_yyyymmdd"]
    result2 = ctx["day0_yyyymmdd"]
    assert result1 is result2  # same object from cache


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_unknown_key_raises():
    ctx = _ctx(2026, 4, 28)
    with pytest.raises(KeyError, match="Unknown date context variable"):
        _ = ctx["invalid_key"]


def test_contains_valid():
    ctx = _ctx(2026, 4, 28)
    assert "day0_yyyymmdd" in ctx
    assert "mon0_first_yyyymmddD" in ctx


def test_contains_invalid():
    ctx = _ctx(2026, 4, 28)
    assert "bad_key" not in ctx


def test_get_returns_default_for_invalid():
    ctx = _ctx(2026, 4, 28)
    assert ctx.get("invalid_key") is None
    assert ctx.get("invalid_key", "fallback") == "fallback"


# ---------------------------------------------------------------------------
# Overlap window detection (the key use case from jobs.py)
# ---------------------------------------------------------------------------


class TestOverlapWindow:
    def test_not_in_overlap_mid_month(self):
        # May 8: 5 days ago = May 3, same month → no overlap
        ctx = _ctx(2026, 5, 8)
        assert ctx["day0_yyyymm"] == ctx["day5_yyyymm"]

    def test_in_overlap_early_month(self):
        # May 3: 5 days ago = April 28, different month → overlap window
        ctx = _ctx(2026, 5, 3)
        assert ctx["day0_yyyymm"] != ctx["day5_yyyymm"]
        assert ctx["day0_yyyymm"] == "202605"
        assert ctx["day5_yyyymm"] == "202604"

    def test_boundary_day5(self):
        # May 5: 5 days ago = April 30, different month → still in overlap
        ctx = _ctx(2026, 5, 5)
        assert ctx["day0_yyyymm"] != ctx["day5_yyyymm"]

    def test_boundary_day6(self):
        # May 6: 5 days ago = May 1, same month → overlap closed
        ctx = _ctx(2026, 5, 6)
        assert ctx["day0_yyyymm"] == ctx["day5_yyyymm"]
