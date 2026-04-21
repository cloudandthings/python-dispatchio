"""Tests for run_key resolution from Cadence objects."""

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
from dispatchio.run_key_resolution import describe_cadence, resolve_run_key
from dispatchio.run_key_resolution import (
    resolve_run_key_from_orchestrator,
    resolve_orchestrator_run_key,
)


REF = datetime(2025, 1, 15, 9, 30, tzinfo=timezone.utc)  # Wednesday


class TestDailyResolution:
    def test_daily_today(self):
        assert resolve_run_key(DAILY, REF) == "D20250115"

    def test_yesterday(self):
        assert resolve_run_key(YESTERDAY, REF) == "D20250114"

    def test_three_days_ago(self):
        three_ago = DateCadence(frequency=Frequency.DAILY, offset=-3)
        assert resolve_run_key(three_ago, REF) == "D20250112"

    def test_crosses_month_boundary(self):
        ref = datetime(2025, 2, 1, 12, 0, tzinfo=timezone.utc)
        assert resolve_run_key(YESTERDAY, ref) == "D20250131"

    def test_crosses_year_boundary(self):
        ref = datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc)
        assert resolve_run_key(YESTERDAY, ref) == "D20241231"


class TestMonthlyResolution:
    def test_monthly_current(self):
        assert resolve_run_key(MONTHLY, REF) == "M202501"

    def test_last_month(self):
        assert resolve_run_key(LAST_MONTH, REF) == "M202412"

    def test_two_months_ago_crosses_year(self):
        ref = datetime(2025, 2, 15, tzinfo=timezone.utc)
        two_ago = DateCadence(frequency=Frequency.MONTHLY, offset=-2)
        assert resolve_run_key(two_ago, ref) == "M202412"

    def test_twelve_months_ago(self):
        twelve_ago = DateCadence(frequency=Frequency.MONTHLY, offset=-12)
        assert resolve_run_key(twelve_ago, REF) == "M202401"


class TestHourlyResolution:
    def test_hourly_current(self):
        assert resolve_run_key(HOURLY, REF) == "H2025011509"

    def test_previous_hour(self):
        prev = DateCadence(frequency=Frequency.HOURLY, offset=-1)
        assert resolve_run_key(prev, REF) == "H2025011508"

    def test_crosses_day_boundary(self):
        ref = datetime(2025, 1, 15, 0, 30, tzinfo=timezone.utc)
        prev = DateCadence(frequency=Frequency.HOURLY, offset=-1)
        assert resolve_run_key(prev, ref) == "H2025011423"


class TestWeeklyResolution:
    def test_weekly_current_monday(self):
        # Jan 15 2025 is Wednesday; Monday is Jan 13
        assert resolve_run_key(WEEKLY, REF) == "W20250113"

    def test_last_week(self):
        assert resolve_run_key(LAST_WEEK, REF) == "W20250106"

    def test_already_monday(self):
        ref = datetime(2025, 1, 13, 10, 0, tzinfo=timezone.utc)  # Monday
        assert resolve_run_key(WEEKLY, ref) == "W20250113"


class TestFixedCadence:
    def test_value_returned_as_is(self):
        cad = FixedCadence(value="adhoc-123")
        assert resolve_run_key(cad, REF) == "adhoc-123"

    def test_custom_string(self):
        cad = FixedCadence(value="my-special-run")
        assert resolve_run_key(cad, REF) == "my-special-run"


class TestIncrementalCadence:
    def test_raises_not_implemented(self):
        cad = IncrementalCadence(key="changed_entities/today")
        with pytest.raises(NotImplementedError):
            resolve_run_key(cad, REF)


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


class TestOrchestratorRunKeyResolution:
    def test_daily_orchestrator_run_key(self):
        assert resolve_orchestrator_run_key(DAILY, REF) == "20250115"

    def test_weekly_orchestrator_run_key(self):
        # Jan 15 2025 is Wednesday; Monday is Jan 13
        assert resolve_orchestrator_run_key(WEEKLY, REF) == "20250113"

    def test_monthly_orchestrator_run_key(self):
        assert resolve_orchestrator_run_key(MONTHLY, REF) == "202501"

    def test_hourly_orchestrator_run_key(self):
        assert resolve_orchestrator_run_key(HOURLY, REF) == "2025011509"

    def test_fixed_orchestrator_run_key(self):
        cad = FixedCadence(value="event:customer-import-9841")
        assert resolve_orchestrator_run_key(cad, REF) == "event:customer-import-9841"


class TestResolveRunKeyFromOrchestrator:
    def test_daily_orchestrator_daily_job(self):
        assert resolve_run_key_from_orchestrator("20250115", DAILY) == "D20250115"

    def test_daily_orchestrator_yesterday_job(self):
        assert resolve_run_key_from_orchestrator("20250115", YESTERDAY) == "D20250114"

    def test_daily_orchestrator_monthly_job(self):
        assert resolve_run_key_from_orchestrator("20250115", MONTHLY) == "M202501"

    def test_daily_orchestrator_weekly_job(self):
        # Jan 15 2025 is Wednesday; Monday is Jan 13
        assert resolve_run_key_from_orchestrator("20250115", WEEKLY) == "W20250113"

    def test_monthly_orchestrator_daily_job(self):
        # For monthly run key, we anchor to first day of month
        assert resolve_run_key_from_orchestrator("202501", DAILY) == "D20250101"

    def test_hourly_orchestrator_daily_job(self):
        assert resolve_run_key_from_orchestrator("2025011509", DAILY) == "D20250115"

    def test_event_orchestrator_fixed_job(self):
        assert (
            resolve_run_key_from_orchestrator(
                "event:customer-import-9841", FixedCadence(value="audit")
            )
            == "audit"
        )

    def test_event_orchestrator_date_job_raises(self):
        with pytest.raises(ValueError):
            resolve_run_key_from_orchestrator("event:customer-import-9841", DAILY)
