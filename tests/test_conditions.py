"""
Tests for the Condition types in dispatchio/conditions.py.

reference_time is always passed explicitly; no freezegun needed.
cadence is passed as DAILY for all tests unless the condition is
cadence-specific (none of the current types actually use the cadence
argument, but it is forwarded to satisfy the Protocol).
"""

from __future__ import annotations

from datetime import datetime, time, timezone

import pytest

from dispatchio.cadence import DAILY, MONTHLY
from dispatchio.conditions import (
    AllOf,
    AnyOf,
    DayOfMonthCondition,
    DayOfWeekCondition,
    MinuteOfHourCondition,
    TimeOfDayCondition,
)

# Convenience reference times
_MON_09_30 = datetime(2025, 1, 13, 9, 30, tzinfo=timezone.utc)  # Monday, day 13, 09:30
_WED_06_00 = datetime(
    2025, 1, 15, 6, 0, tzinfo=timezone.utc
)  # Wednesday, day 15, 06:00
_SAT_23_59 = datetime(
    2025, 1, 18, 23, 59, tzinfo=timezone.utc
)  # Saturday, day 18, 23:59


# ---------------------------------------------------------------------------
# TimeOfDayCondition
# ---------------------------------------------------------------------------


class TestTimeOfDayCondition:
    def test_after_met_exactly(self):
        cond = TimeOfDayCondition(after=time(9, 30))
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_after_not_met(self):
        cond = TimeOfDayCondition(after=time(10, 0))
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_before_inside_window(self):
        cond = TimeOfDayCondition(before=time(10, 0))
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_before_at_boundary_excluded(self):
        cond = TimeOfDayCondition(before=time(9, 30))
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_window_inside(self):
        cond = TimeOfDayCondition(after=time(9, 0), before=time(10, 0))
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_window_outside_before(self):
        cond = TimeOfDayCondition(after=time(10, 0), before=time(11, 0))
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_window_outside_after(self):
        cond = TimeOfDayCondition(after=time(6, 0), before=time(9, 0))
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_requires_at_least_one_field(self):
        with pytest.raises(Exception):
            TimeOfDayCondition()


# ---------------------------------------------------------------------------
# MinuteOfHourCondition
# ---------------------------------------------------------------------------


class TestMinuteOfHourCondition:
    def test_after_met(self):
        cond = MinuteOfHourCondition(after=30)
        assert cond.is_met(_MON_09_30, DAILY) is True  # minute=30

    def test_after_not_met(self):
        cond = MinuteOfHourCondition(after=31)
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_before_met(self):
        cond = MinuteOfHourCondition(before=31)
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_before_at_boundary_excluded(self):
        cond = MinuteOfHourCondition(before=30)
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_window(self):
        cond = MinuteOfHourCondition(after=15, before=45)
        assert cond.is_met(_MON_09_30, DAILY) is True
        assert cond.is_met(_WED_06_00, DAILY) is False  # minute=0

    def test_requires_at_least_one_field(self):
        with pytest.raises(Exception):
            MinuteOfHourCondition()


# ---------------------------------------------------------------------------
# DayOfMonthCondition
# ---------------------------------------------------------------------------


class TestDayOfMonthCondition:
    def test_after_met(self):
        cond = DayOfMonthCondition(after=13)
        assert cond.is_met(_MON_09_30, DAILY) is True  # day=13

    def test_after_not_met(self):
        cond = DayOfMonthCondition(after=14)
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_before_met(self):
        cond = DayOfMonthCondition(before=14)
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_before_at_boundary_excluded(self):
        cond = DayOfMonthCondition(before=13)
        assert cond.is_met(_MON_09_30, DAILY) is False

    def test_window(self):
        cond = DayOfMonthCondition(after=10, before=20)
        assert cond.is_met(_MON_09_30, DAILY) is True  # day=13
        assert cond.is_met(_SAT_23_59, DAILY) is True  # day=18
        feb_1 = datetime(2025, 2, 1, tzinfo=timezone.utc)
        assert cond.is_met(feb_1, DAILY) is False  # day=1

    def test_cadence_agnostic(self):
        # DayOfMonthCondition doesn't use cadence — same result with MONTHLY
        cond = DayOfMonthCondition(after=3)
        assert cond.is_met(_MON_09_30, MONTHLY) is True

    def test_requires_at_least_one_field(self):
        with pytest.raises(Exception):
            DayOfMonthCondition()


# ---------------------------------------------------------------------------
# DayOfWeekCondition
# ---------------------------------------------------------------------------


class TestDayOfWeekCondition:
    def test_weekday_included(self):
        cond = DayOfWeekCondition(on_days=[0, 1, 2, 3, 4])
        assert cond.is_met(_MON_09_30, DAILY) is True  # Monday = 0
        assert cond.is_met(_WED_06_00, DAILY) is True  # Wednesday = 2

    def test_weekend_excluded(self):
        cond = DayOfWeekCondition(on_days=[0, 1, 2, 3, 4])
        assert cond.is_met(_SAT_23_59, DAILY) is False  # Saturday = 5

    def test_specific_days(self):
        mon_wed_fri = DayOfWeekCondition(on_days=[0, 2, 4])
        assert mon_wed_fri.is_met(_MON_09_30, DAILY) is True
        assert mon_wed_fri.is_met(_WED_06_00, DAILY) is True
        assert mon_wed_fri.is_met(_SAT_23_59, DAILY) is False


# ---------------------------------------------------------------------------
# AllOf (AND composite)
# ---------------------------------------------------------------------------


class TestAllOf:
    def test_all_met(self):
        cond = AllOf(
            conditions=[
                TimeOfDayCondition(after=time(9, 0)),
                DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
            ]
        )
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_one_not_met(self):
        cond = AllOf(
            conditions=[
                TimeOfDayCondition(after=time(9, 0)),
                DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
            ]
        )
        assert cond.is_met(_SAT_23_59, DAILY) is False  # weekend

    def test_none_met(self):
        cond = AllOf(
            conditions=[
                TimeOfDayCondition(after=time(10, 0)),
                DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
            ]
        )
        assert cond.is_met(_MON_09_30, DAILY) is False  # before 10:00

    def test_empty_is_true(self):
        assert AllOf(conditions=[]).is_met(_MON_09_30, DAILY) is True


# ---------------------------------------------------------------------------
# AnyOf (OR composite)
# ---------------------------------------------------------------------------


class TestAnyOf:
    def test_one_met(self):
        cond = AnyOf(
            conditions=[
                TimeOfDayCondition(after=time(10, 0)),  # not met (09:30)
                DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),  # met (Monday)
            ]
        )
        assert cond.is_met(_MON_09_30, DAILY) is True

    def test_none_met(self):
        cond = AnyOf(
            conditions=[
                TimeOfDayCondition(after=time(10, 0)),
                DayOfWeekCondition(on_days=[5, 6]),  # weekend only
            ]
        )
        assert cond.is_met(_MON_09_30, DAILY) is False  # 09:30, Monday

    def test_empty_is_false(self):
        assert AnyOf(conditions=[]).is_met(_MON_09_30, DAILY) is False


# ---------------------------------------------------------------------------
# Nested composites
# ---------------------------------------------------------------------------


class TestNested:
    def test_allof_inside_anyof(self):
        weekday_morning = AllOf(
            conditions=[
                TimeOfDayCondition(after=time(6, 0)),
                DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
            ]
        )
        weekend_any = DayOfWeekCondition(on_days=[5, 6])
        cond = AnyOf(conditions=[weekday_morning, weekend_any])

        # Monday 09:30 — weekday morning branch satisfied
        assert cond.is_met(_MON_09_30, DAILY) is True
        # Saturday 23:59 — weekend branch satisfied
        assert cond.is_met(_SAT_23_59, DAILY) is True
        # Wednesday 06:00 — exactly at boundary, after=time(6,0): 06:00 >= 06:00 → met
        assert cond.is_met(_WED_06_00, DAILY) is True
