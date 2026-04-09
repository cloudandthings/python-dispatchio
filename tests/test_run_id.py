"""Tests for run_id resolution from Cadence objects."""

from datetime import datetime, timezone
import pytest
from dispatchio.cadence import (
    DAILY,
    HOURLY,
    MONTHLY,
    WEEKLY,
    YESTERDAY,
    LAST_MONTH,
    LAST_WEEK,
    DateCadence,
    FixedCadence,
    Frequency,
    IncrementalCadence,
)
from dispatchio.run_id import describe_cadence, resolve_run_id


REF = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)  # Wednesday


class TestDailyResolution:
    def test_daily_today(self):
        assert resolve_run_id(DAILY, REF) == "20250115"

    def test_yesterday(self):
        assert resolve_run_id(YESTERDAY, REF) == "20250114"

    def test_three_days_ago(self):
        three_ago = DateCadence(frequency=Frequency.DAILY, offset=-3)
        assert resolve_run_id(three_ago, REF) == "20250112"

    def test_crosses_month_boundary(self):
        ref = datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc)
        assert resolve_run_id(YESTERDAY, ref) == "20250131"

    def test_crosses_year_boundary(self):
        ref = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert resolve_run_id(YESTERDAY, ref) == "20241231"


class TestMonthlyResolution:
    def test_monthly_current(self):
        assert resolve_run_id(MONTHLY, REF) == "202501"

    def test_last_month(self):
        assert resolve_run_id(LAST_MONTH, REF) == "202412"

    def test_two_months_ago_crosses_year(self):
        ref = datetime(2025, 2, 15, tzinfo=timezone.utc)
        two_ago = DateCadence(frequency=Frequency.MONTHLY, offset=-2)
        assert resolve_run_id(two_ago, ref) == "202412"

    def test_twelve_months_ago(self):
        twelve_ago = DateCadence(frequency=Frequency.MONTHLY, offset=-12)
        assert resolve_run_id(twelve_ago, REF) == "202401"


class TestHourlyResolution:
    def test_hourly_current(self):
        assert resolve_run_id(HOURLY, REF) == "2025011509"

    def test_previous_hour(self):
        prev = DateCadence(frequency=Frequency.HOURLY, offset=-1)
        assert resolve_run_id(prev, REF) == "2025011508"

    def test_crosses_day_boundary(self):
        ref = datetime(2025, 1, 15, 0, 30, tzinfo=timezone.utc)
        prev = DateCadence(frequency=Frequency.HOURLY, offset=-1)
        assert resolve_run_id(prev, ref) == "2025011423"


class TestWeeklyResolution:
    def test_weekly_current_monday(self):
        # Jan 15 2025 is Wednesday; Monday is Jan 13
        assert resolve_run_id(WEEKLY, REF) == "20250113"

    def test_last_week(self):
        assert resolve_run_id(LAST_WEEK, REF) == "20250106"

    def test_already_monday(self):
        ref = datetime(2025, 1, 13, 10, 0, tzinfo=timezone.utc)  # Monday
        assert resolve_run_id(WEEKLY, ref) == "20250113"


class TestFixedCadence:
    def test_value_returned_as_is(self):
        cad = FixedCadence(value="adhoc-123")
        assert resolve_run_id(cad, REF) == "adhoc-123"

    def test_custom_string(self):
        cad = FixedCadence(value="my-special-run")
        assert resolve_run_id(cad, REF) == "my-special-run"


class TestIncrementalCadence:
    def test_raises_not_implemented(self):
        cad = IncrementalCadence(key="changed_entities/today")
        with pytest.raises(NotImplementedError):
            resolve_run_id(cad, REF)


class TestDescribeCadence:
    def test_daily_today(self):
        assert describe_cadence(DAILY) == "today"

    def test_yesterday(self):
        assert describe_cadence(YESTERDAY) == "yesterday"

    def test_last_month(self):
        assert describe_cadence(LAST_MONTH) == "last month"

    def test_three_days_ago(self):
        three_ago = DateCadence(frequency=Frequency.DAILY, offset=-3)
        desc = describe_cadence(three_ago)
        assert "3" in desc and "day" in desc

    def test_fixed(self):
        cad = FixedCadence(value="static-id")
        assert "static-id" in describe_cadence(cad)

    def test_incremental(self):
        cad = IncrementalCadence(key="my_key")
        assert "my_key" in describe_cadence(cad)
