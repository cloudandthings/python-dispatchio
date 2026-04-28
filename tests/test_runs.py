"""Unit tests for RunSpec and RunContext."""

from __future__ import annotations

from datetime import datetime

import pytest

from dispatchio.date_context import DateContext
from dispatchio.runs import RunContext, RunSpec


def _rc(
    year: int = 2026, month: int = 4, day: int = 28, *, is_backfill: bool = False
) -> RunContext:
    ref = datetime(year, month, day, 12, 0, 0)
    return RunContext(
        reference_time=ref,
        dates=DateContext(ref),
        is_backfill=is_backfill,
        job_name="test_job",
    )


# ---------------------------------------------------------------------------
# RunSpec validation
# ---------------------------------------------------------------------------


class TestRunSpec:
    def test_default(self):
        spec = RunSpec()
        assert spec.variant is None
        assert spec.run_key is None
        assert spec.params == {}

    def test_variant_only(self):
        spec = RunSpec(variant="current_month")
        assert spec.variant == "current_month"
        assert spec.run_key is None

    def test_run_key_only(self):
        spec = RunSpec(run_key="M202601")
        assert spec.run_key == "M202601"
        assert spec.variant is None

    def test_params(self):
        spec = RunSpec(params={"start": "2026-01-01", "end": "2026-01-31"})
        assert spec.params["start"] == "2026-01-01"

    def test_variant_and_run_key_raises(self):
        with pytest.raises(ValueError, match="either"):
            RunSpec(variant="v1", run_key="M202601")


# ---------------------------------------------------------------------------
# RunContext
# ---------------------------------------------------------------------------


class TestRunContext:
    def test_basic_fields(self):
        rc = _rc()
        assert rc.job_name == "test_job"
        assert rc.is_backfill is False
        assert rc.reference_time.year == 2026

    def test_ctx_accessible(self):
        rc = _rc(2026, 4, 28)
        assert rc.dates["day0_yyyymmddD"] == "2026-04-28"
        assert rc.dates["mon0_first_yyyymmddD"] == "2026-04-01"

    def test_is_backfill_true(self):
        rc = _rc(is_backfill=True)
        assert rc.is_backfill is True


# ---------------------------------------------------------------------------
# Integration: runs callable pattern from jobs.py
# ---------------------------------------------------------------------------


def month_to_date_runs(rc: RunContext) -> list[RunSpec]:
    """Mirrors the athena_runs() pattern from examples/parametrized_runs/jobs.py."""
    if rc.is_backfill:
        from dispatchio import MONTHLY
        from dispatchio.run_key_resolution import resolve_run_key

        return [
            RunSpec(
                run_key=resolve_run_key(MONTHLY, rc.reference_time),
                params={
                    "start_date": rc.dates["mon0_first_yyyymmddD"],
                    "end_date": rc.dates["mon0_last_yyyymmddD"],
                },
            )
        ]

    runs = [
        RunSpec(
            variant="current_month",
            params={
                "start_date": rc.dates["mon0_first_yyyymmddD"],
                "end_date": rc.dates["day0_yyyymmddD"],
            },
        )
    ]
    if rc.dates["day0_yyyymm"] != rc.dates["day5_yyyymm"]:
        runs.append(
            RunSpec(
                variant="previous_month",
                params={
                    "start_date": rc.dates["mon1_first_yyyymmddD"],
                    "end_date": rc.dates["mon1_last_yyyymmddD"],
                },
            )
        )
    return runs


class TestMonthToDatePattern:
    def test_normal_mid_month(self):
        rc = _rc(2026, 4, 28)
        specs = month_to_date_runs(rc)
        assert len(specs) == 1
        assert specs[0].variant == "current_month"
        assert specs[0].params["start_date"] == "2026-04-01"
        assert specs[0].params["end_date"] == "2026-04-28"

    def test_normal_overlap_window(self):
        # May 3: 5 days ago = April 28 → overlap window active
        rc = _rc(2026, 5, 3)
        specs = month_to_date_runs(rc)
        assert len(specs) == 2
        variants = {s.variant for s in specs}
        assert variants == {"current_month", "previous_month"}

        current = next(s for s in specs if s.variant == "current_month")
        assert current.params["start_date"] == "2026-05-01"
        assert current.params["end_date"] == "2026-05-03"

        previous = next(s for s in specs if s.variant == "previous_month")
        assert previous.params["start_date"] == "2026-04-01"
        assert previous.params["end_date"] == "2026-04-30"

    def test_backfill_mode(self):
        rc = _rc(2026, 1, 15, is_backfill=True)
        specs = month_to_date_runs(rc)
        assert len(specs) == 1
        assert specs[0].run_key == "M202601"
        assert specs[0].variant is None
        assert specs[0].params["start_date"] == "2026-01-01"
        assert specs[0].params["end_date"] == "2026-01-31"

    def test_backfill_same_key_any_day_in_month(self):
        for day in [1, 10, 15, 28, 31]:
            ref = datetime(2026, 1, day, 12, 0, 0)
            rc = RunContext(
                reference_time=ref,
                dates=DateContext(ref),
                is_backfill=True,
                job_name="test",
            )
            specs = month_to_date_runs(rc)
            assert specs[0].run_key == "M202601", f"Failed for day={day}"

    def test_overlap_closed_day6(self):
        # May 6: 5 days ago = May 1, same month → no overlap
        rc = _rc(2026, 5, 6)
        specs = month_to_date_runs(rc)
        assert len(specs) == 1
        assert specs[0].variant == "current_month"
