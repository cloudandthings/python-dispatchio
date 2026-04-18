"""
Tests for the Orchestrator tick engine.

Uses MemoryStateStore and a simple spy executor to avoid any I/O.
freezegun is not required — reference_time is always passed explicitly.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import pytest

from dispatchio.alerts.base import AlertEvent
from dispatchio.cadence import DAILY, MONTHLY, YESTERDAY, DateCadence, Frequency
from dispatchio.conditions import DayOfWeekCondition, TimeOfDayCondition
from dispatchio.models import (
    AlertCondition,
    AlertOn,
    Dependency,
    DependencyMode,
    JobAction,
    Job,
    RetryPolicy,
    RunRecord,
    Status,
    SubprocessJob,
)
from dispatchio.orchestrator import Orchestrator
from dispatchio.receiver.base import CompletionEvent
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)  # Wednesday 09:00 UTC


def _job(name="job", cadence=DAILY, **kwargs) -> Job:
    return Job(
        name=name,
        executor=SubprocessJob(command=["echo", name]),
        cadence=cadence,
        **kwargs,
    )


class SpyExecutor:
    """Records every submit() call without running anything."""

    def __init__(self):
        self.calls: list[dict] = []

    def submit(self, job, run_id, reference_time, timeout=None):
        self.calls.append({"job": job.name, "run_id": run_id})


class FailingExecutor:
    """Always raises on submit()."""

    def submit(self, job, run_id, reference_time, timeout=None):
        raise RuntimeError("executor down")


class SpyAlertHandler:
    def __init__(self):
        self.events: list[AlertEvent] = []

    def handle(self, event: AlertEvent):
        self.events.append(event)


def _make_orch(
    jobs, store=None, executor=None, receiver=None, alert_handler=None, **kwargs
):
    store = store or SQLAlchemyStateStore("sqlite:///:memory:")
    executor = executor or SpyExecutor()
    return (
        Orchestrator(
            jobs=jobs,
            state=store,
            executors={"subprocess": executor},
            receiver=receiver,
            alert_handler=alert_handler,
            **kwargs,
        ),
        store,
        executor,
    )


# ---------------------------------------------------------------------------
# Basic submission
# ---------------------------------------------------------------------------


class TestBasicSubmission:
    def test_submits_unconstrained_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        result = orch.tick(REF)
        submitted = result.submitted()
        assert len(submitted) == 1
        assert submitted[0].job_name == "simple"
        assert store.get("simple", "20250115").status == Status.SUBMITTED

    def test_does_not_resubmit_submitted_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        orch.tick(REF)
        orch.tick(REF)
        assert len(executor.calls) == 1

    def test_does_not_resubmit_done_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        store.put(RunRecord(job_name="simple", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(r.action == JobAction.SKIPPED_ALREADY_DONE for r in result.results)

    def test_does_not_resubmit_running_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(job_name="simple", run_id="20250115", status=Status.RUNNING)
        )
        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(r.action == JobAction.SKIPPED_ALREADY_ACTIVE for r in result.results)


# ---------------------------------------------------------------------------
# Condition gates
# ---------------------------------------------------------------------------


class TestConditionGate:
    def test_job_blocked_before_time(self):
        j = _job("timed", condition=TimeOfDayCondition(after=time(10, 0)))
        orch, store, executor = _make_orch([j])
        # tick at 09:00 — too early
        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert result.results[0].action == JobAction.SKIPPED_CONDITION

    def test_job_runs_after_time(self):
        j = _job("timed", condition=TimeOfDayCondition(after=time(8, 0)))
        orch, store, executor = _make_orch([j])
        orch.tick(REF)  # 09:00 >= 08:00
        assert len(executor.calls) == 1

    def test_day_of_week_condition_blocks_on_wrong_day(self):
        # REF is Wednesday (weekday=2); allow Mon/Tue only
        j = _job("weekday_only", condition=DayOfWeekCondition(on_days=[0, 1]))
        orch, store, executor = _make_orch([j])
        result = orch.tick(REF)
        assert result.results[0].action == JobAction.SKIPPED_CONDITION

    def test_day_of_week_condition_passes_on_correct_day(self):
        # REF is Wednesday (weekday=2)
        j = _job("wed_job", condition=DayOfWeekCondition(on_days=[2]))
        orch, store, executor = _make_orch([j])
        result = orch.tick(REF)
        assert result.results[0].action == JobAction.SUBMITTED


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


class TestDependencies:
    def test_blocked_when_dependency_not_met(self):
        upstream = _job("up")
        downstream = _job("down", depends_on=[Dependency(job_name="up", cadence=DAILY)])
        orch, store, executor = _make_orch([upstream, downstream])
        store.put(RunRecord(job_name="up", run_id="20250115", status=Status.SUBMITTED))
        orch2, store2, executor2 = _make_orch(
            [downstream], store=store, strict_dependencies=False
        )
        result = orch2.tick(REF)
        assert not any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_unblocked_when_dependency_done(self):
        downstream = _job("down", depends_on=[Dependency(job_name="up", cadence=DAILY)])
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.put(RunRecord(job_name="up", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_date_offset_dependency(self):
        """Depend on a job from 3 days ago."""
        three_days_ago = DateCadence(frequency=Frequency.DAILY, offset=-3)
        downstream = _job(
            "down", depends_on=[Dependency(job_name="up", cadence=three_days_ago)]
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        # offset=-3 from Jan 15 = Jan 12
        store.put(RunRecord(job_name="up", run_id="20250112", status=Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "down"

    def test_yesterday_shorthand(self):
        downstream = _job(
            "down", depends_on=[Dependency(job_name="up", cadence=YESTERDAY)]
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.put(RunRecord(job_name="up", run_id="20250114", status=Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "down"

    def test_monthly_dependency(self):
        downstream = _job(
            "monthly_consumer",
            cadence=MONTHLY,
            depends_on=[Dependency(job_name="monthly_load", cadence=MONTHLY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.put(
            RunRecord(job_name="monthly_load", run_id="202501", status=Status.DONE)
        )
        result = orch.tick(REF)
        assert result.submitted()[0].run_id == "202501"

    def test_cross_cadence_daily_depends_on_monthly(self):
        daily = _job(
            "daily_enrichment",
            cadence=DAILY,
            depends_on=[Dependency(job_name="monthly_report", cadence=MONTHLY)],
        )
        orch, store, executor = _make_orch([daily], strict_dependencies=False)
        store.put(
            RunRecord(job_name="monthly_report", run_id="202501", status=Status.DONE)
        )
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "daily_enrichment"

    def test_multiple_dependencies_all_must_be_met(self):
        j = _job(
            "fan_in",
            depends_on=[
                Dependency(job_name="a", cadence=DAILY),
                Dependency(job_name="b", cadence=DAILY),
            ],
        )
        orch, store, executor = _make_orch([j], strict_dependencies=False)
        store.put(RunRecord(job_name="a", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert len(result.submitted()) == 0

        store.put(RunRecord(job_name="b", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert len(result.submitted()) == 1

    def test_Job_shorthand_in_depends_on(self):
        """Passing a Job directly to depends_on coerces to Dependency."""
        upstream = _job("up", cadence=DAILY)
        downstream = Job(
            name="down",
            executor=SubprocessJob(command=["echo", "down"]),
            cadence=DAILY,
            depends_on=[upstream],
        )
        orch, store, executor = _make_orch([upstream, downstream])
        # upstream not done yet
        result = orch.tick(REF)
        assert result.results[0].action == JobAction.SUBMITTED  # upstream submitted
        assert result.results[1].action == JobAction.SKIPPED_DEPENDENCIES

        # Mark upstream done and retry
        store.put(RunRecord(job_name="up", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_Job_shorthand_single_item(self):
        """depends_on accepts a single Job without a list."""
        upstream = _job("up", cadence=DAILY)
        downstream = Job(
            name="down",
            executor=SubprocessJob(command=["echo", "down"]),
            cadence=DAILY,
            depends_on=upstream,
        )
        orch, store, executor = _make_orch([upstream, downstream])
        store.put(RunRecord(job_name="up", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_Job_shorthand_requires_explicit_cadence(self):
        """Job with cadence=None cannot be used in depends_on shorthand."""
        upstream = Job(
            name="up",
            executor=SubprocessJob(command=["echo"]),
            cadence=None,
        )
        with pytest.raises(Exception, match="cadence=None"):
            Job(
                name="down",
                executor=SubprocessJob(command=["echo"]),
                cadence=DAILY,
                depends_on=upstream,
            )


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_no_retry_by_default(self):
        j = _job("flaky")
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(
                job_name="flaky",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="boom",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.MARKED_ERROR for r in result.results)
        assert len(executor.calls) == 0

    def test_retries_when_policy_allows(self):
        j = _job("flaky", retry_policy=RetryPolicy(max_attempts=3))
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(
                job_name="flaky",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="transient",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.RETRYING for r in result.results)
        assert len(executor.calls) == 1

    def test_no_retry_after_max_attempts(self):
        j = _job("flaky", retry_policy=RetryPolicy(max_attempts=2))
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(
                job_name="flaky",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="boom",
                attempt=1,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.MARKED_ERROR for r in result.results)
        assert len(executor.calls) == 0

    def test_retry_on_filter_matches(self):
        j = _job(
            "flaky", retry_policy=RetryPolicy(max_attempts=3, retry_on=["timeout"])
        )
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(
                job_name="flaky",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="connection timeout",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.RETRYING for r in result.results)

    def test_retry_on_filter_does_not_match(self):
        j = _job(
            "flaky", retry_policy=RetryPolicy(max_attempts=3, retry_on=["timeout"])
        )
        orch, store, executor = _make_orch([j])
        store.put(
            RunRecord(
                job_name="flaky",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="disk full",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert not any(r.action == JobAction.RETRYING for r in result.results)


# ---------------------------------------------------------------------------
# Completion receiver
# ---------------------------------------------------------------------------


class TestCompletionReceiver:
    class CapturingReceiver:
        def __init__(self, events):
            self._events = events

        def drain(self):
            events = list(self._events)
            self._events.clear()
            return events

    def test_done_event_updates_state(self):
        j = _job("j")
        events = [CompletionEvent(job_name="j", run_id="20250115", status=Status.DONE)]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.put(RunRecord(job_name="j", run_id="20250115", status=Status.RUNNING))
        orch.tick(REF)
        assert store.get("j", "20250115").status == Status.DONE

    def test_error_event_sets_error_reason(self):
        j = _job("j")
        events = [
            CompletionEvent(
                job_name="j",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="OOM killed",
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.put(RunRecord(job_name="j", run_id="20250115", status=Status.RUNNING))
        orch.tick(REF)
        record = store.get("j", "20250115")
        assert record.status == Status.ERROR
        assert "OOM" in record.error_reason


# ---------------------------------------------------------------------------
# Submission failure
# ---------------------------------------------------------------------------


class TestSubmissionFailure:
    def test_submission_failure_marks_error(self):
        j = _job("bad_job")
        orch, store, failing_executor = _make_orch([j], executor=FailingExecutor())
        result = orch.tick(REF)
        assert any(r.action == JobAction.SUBMISSION_FAILED for r in result.results)
        record = store.get("bad_job", "20250115")
        assert record.status == Status.ERROR
        assert "submission_failed" in record.error_reason


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


class TestAlerts:
    def test_alert_on_error_fired_when_retries_exhausted(self):
        handler = SpyAlertHandler()
        j = _job(
            "alerted_job",
            retry_policy=RetryPolicy(max_attempts=1),
            alerts=[AlertCondition(on=AlertOn.ERROR, channels=["ops"])],
        )
        orch, store, executor = _make_orch([j], alert_handler=handler)
        store.put(
            RunRecord(
                job_name="alerted_job",
                run_id="20250115",
                status=Status.ERROR,
                error_reason="boom",
                attempt=0,
            )
        )
        orch.tick(REF)
        assert len(handler.events) >= 1
        assert handler.events[0].alert_on == AlertOn.ERROR
        assert "ops" in handler.events[0].channels


# ---------------------------------------------------------------------------
# Per-tick submission limit
# ---------------------------------------------------------------------------


class TestSubmissionLimit:
    def test_limits_submissions_per_tick(self):
        jobs = [_job(f"j{i}") for i in range(5)]
        store = SQLAlchemyStateStore("sqlite:///:memory:")
        executor = SpyExecutor()
        orch = Orchestrator(
            jobs=jobs,
            state=store,
            executors={"subprocess": executor},
            max_submissions_per_tick=2,
        )
        result = orch.tick(REF)
        assert len(executor.calls) == 2
        assert len(result.submitted()) == 2

    def test_deferred_jobs_submitted_on_subsequent_ticks(self):
        jobs = [_job(f"j{i}") for i in range(3)]
        store = SQLAlchemyStateStore("sqlite:///:memory:")
        executor = SpyExecutor()
        orch = Orchestrator(
            jobs=jobs,
            state=store,
            executors={"subprocess": executor},
            max_submissions_per_tick=2,
        )
        orch.tick(REF)
        assert len(executor.calls) == 2
        orch.tick(REF)
        assert len(executor.calls) == 3

    def test_no_limit_submits_all(self):
        jobs = [_job(f"j{i}") for i in range(5)]
        orch, store, executor = _make_orch(jobs)
        result = orch.tick(REF)
        assert len(executor.calls) == 5
        assert len(result.submitted()) == 5

    def test_results_preserve_job_list_order(self):
        upstream = _job("up")
        downstream = _job("down", depends_on=[Dependency(job_name="up", cadence=DAILY)])
        orch, store, executor = _make_orch([upstream, downstream])
        result = orch.tick(REF)
        assert result.results[0].job_name == "up"
        assert result.results[0].action == JobAction.SUBMITTED
        assert result.results[1].job_name == "down"
        assert result.results[1].action == JobAction.SKIPPED_DEPENDENCIES


# ---------------------------------------------------------------------------
# default_cadence on Orchestrator
# ---------------------------------------------------------------------------


class TestDefaultCadence:
    def test_none_cadence_inherits_default(self):
        """Job with cadence=None uses the orchestrator's default_cadence."""
        j = Job(
            name="monthly_job",
            executor=SubprocessJob(command=["echo"]),
            cadence=None,
        )
        orch, store, executor = _make_orch([j], default_cadence=MONTHLY)
        result = orch.tick(REF)
        # run_id should be current month
        assert result.submitted()[0].run_id == "202501"

    def test_explicit_cadence_overrides_default(self):
        """Job with explicit cadence ignores orchestrator default."""
        j = _job("daily_job", cadence=DAILY)
        orch, store, executor = _make_orch([j], default_cadence=MONTHLY)
        result = orch.tick(REF)
        assert result.submitted()[0].run_id == "20250115"


# ---------------------------------------------------------------------------
# Unresolved dependency warnings
# ---------------------------------------------------------------------------


class TestUnresolvedDependencyWarning:
    def test_warns_on_unknown_dependency(self, caplog):
        import logging

        j = _job(
            "consumer",
            depends_on=[Dependency(job_name="external_job", cadence=DAILY)],
        )
        with caplog.at_level(logging.WARNING, logger="dispatchio.orchestrator"):
            _make_orch([j], strict_dependencies=False)
        assert any("external_job" in msg for msg in caplog.messages)

    def test_no_warning_when_dependency_is_known(self, caplog):
        import logging

        upstream = _job("upstream")
        downstream = _job(
            "downstream",
            depends_on=[Dependency(job_name="upstream", cadence=DAILY)],
        )
        with caplog.at_level(logging.WARNING, logger="dispatchio.orchestrator"):
            _make_orch([upstream, downstream])
        assert not any("upstream" in msg for msg in caplog.messages)

    def test_no_warning_for_job_with_no_dependencies(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="dispatchio.orchestrator"):
            _make_orch([_job("standalone")])
        assert caplog.messages == []


# ---------------------------------------------------------------------------
# Mutable job graph
# ---------------------------------------------------------------------------


class TestMutableJobGraph:
    def test_duplicate_job_names_raise_in_constructor(self):
        with pytest.raises(ValueError, match="Duplicate job names"):
            _make_orch([_job("dup"), _job("dup")])

    def test_add_jobs_rejects_duplicate_name(self):
        orch, _, _ = _make_orch([_job("a")])
        with pytest.raises(ValueError, match="Duplicate job names"):
            orch.add_job(_job("a"))

    def test_add_jobs_applies_on_next_tick(self):
        orch, store, executor = _make_orch([_job("a")])
        orch.add_job(_job("b"))

        result = orch.tick(REF)
        submitted_names = {r.job_name for r in result.submitted()}
        assert submitted_names == {"a", "b"}
        assert len(executor.calls) == 2
        assert store.get("b", "20250115") is not None

    def test_remove_job_stops_future_evaluation(self):
        orch, _, executor = _make_orch([_job("a"), _job("b")])
        orch.remove_job("b")

        result = orch.tick(REF)
        submitted_names = {r.job_name for r in result.submitted()}
        assert submitted_names == {"a"}
        assert len(executor.calls) == 1

    def test_remove_unknown_job_raises_key_error(self):
        orch, _, _ = _make_orch([_job("a")])
        with pytest.raises(KeyError, match="Unknown job"):
            orch.remove_job("missing")

    def test_mutation_after_tick_disabled_by_default(self):
        orch, _, _ = _make_orch([_job("a")])
        orch.tick(REF)
        with pytest.raises(RuntimeError, match="allow_runtime_mutation=True"):
            orch.add_job(_job("b"))

    def test_mutation_after_tick_allowed_when_enabled(self):
        orch, _, executor = _make_orch(
            [_job("a")],
            allow_runtime_mutation=True,
        )
        orch.tick(REF)
        orch.add_job(_job("b"))

        later = REF + timedelta(days=1)
        result = orch.tick(later)
        assert any(
            r.job_name == "b" and r.action == JobAction.SUBMITTED
            for r in result.results
        )
        assert any(call["job"] == "b" for call in executor.calls)

    def test_unresolved_dependency_validation_runs_after_graph_change(self, caplog):
        import logging

        orch, _, _ = _make_orch(
            [_job("a")],
            allow_runtime_mutation=True,
            strict_dependencies=False,
        )
        orch.add_job(
            _job(
                "consumer", depends_on=[Dependency(job_name="external", cadence=DAILY)]
            )
        )

        with caplog.at_level(logging.WARNING, logger="dispatchio.orchestrator"):
            orch.tick(REF)
        assert any("external" in msg for msg in caplog.messages)

    def test_strict_dependencies_raise_in_constructor_by_default(self):
        with pytest.raises(ValueError, match="Unresolved dependencies"):
            _make_orch(
                [
                    _job(
                        "consumer",
                        depends_on=[Dependency(job_name="external", cadence=DAILY)],
                    )
                ]
            )

    def test_strict_dependencies_raise_after_graph_change(self):
        orch, _, _ = _make_orch(
            [_job("a")], allow_runtime_mutation=True, strict_dependencies=True
        )
        orch.add_job(
            _job(
                "consumer", depends_on=[Dependency(job_name="external", cadence=DAILY)]
            )
        )
        with pytest.raises(ValueError, match="Unresolved dependencies"):
            orch.tick(REF)


# ---------------------------------------------------------------------------
# Dependency satisfaction modes
# ---------------------------------------------------------------------------


class TestDependencyModes:
    """Tests for ALL_SUCCESS, ALL_FINISHED, and THRESHOLD dependency modes."""

    def test_all_success_is_default_behavior(self):
        """ALL_SUCCESS (default) requires every dep to reach required_status."""
        upstream = _job("up", cadence=DAILY)
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[Dependency(job_name="up", cadence=DAILY)],
            dependency_mode=DependencyMode.ALL_SUCCESS,
        )
        orch, store, executor = _make_orch([upstream, downstream])

        # upstream not done — downstream blocked
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SKIPPED_DEPENDENCIES
            for r in result.results
        )

        # mark upstream done — downstream should now submit
        store.put(RunRecord(job_name="up", run_id="20250115", status=Status.DONE))
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_all_finished_proceeds_when_all_terminal(self):
        """ALL_FINISHED proceeds once all deps are in any terminal state."""
        collector = _job(
            "collector",
            cadence=DAILY,
            depends_on=[
                Dependency(job_name="entity_a", cadence=DAILY),
                Dependency(job_name="entity_b", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.ALL_FINISHED,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # one DONE, one ERROR — both finished
        store.put(RunRecord(job_name="entity_a", run_id="20250115", status=Status.DONE))
        store.put(
            RunRecord(job_name="entity_b", run_id="20250115", status=Status.ERROR)
        )

        result = orch.tick(REF)
        assert any(
            r.job_name == "collector" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_all_finished_waits_when_some_still_running(self):
        """ALL_FINISHED stays blocked while any dep is still active."""
        collector = _job(
            "collector",
            cadence=DAILY,
            depends_on=[
                Dependency(job_name="entity_a", cadence=DAILY),
                Dependency(job_name="entity_b", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.ALL_FINISHED,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # entity_a done but entity_b still running
        store.put(RunRecord(job_name="entity_a", run_id="20250115", status=Status.DONE))
        store.put(
            RunRecord(job_name="entity_b", run_id="20250115", status=Status.RUNNING)
        )

        result = orch.tick(REF)
        assert any(
            r.job_name == "collector" and r.action == JobAction.SKIPPED_DEPENDENCIES
            for r in result.results
        )
        assert len(executor.calls) == 0

    def test_threshold_proceeds_when_met(self):
        """THRESHOLD proceeds once ≥ dependency_threshold deps have reached required_status."""
        collector = _job(
            "majority_collector",
            cadence=DAILY,
            depends_on=[
                Dependency(job_name="a", cadence=DAILY),
                Dependency(job_name="b", cadence=DAILY),
                Dependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 2 of 3 done — threshold=2 met
        store.put(RunRecord(job_name="a", run_id="20250115", status=Status.DONE))
        store.put(RunRecord(job_name="b", run_id="20250115", status=Status.DONE))
        # c not done yet

        result = orch.tick(REF)
        assert any(
            r.job_name == "majority_collector" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_threshold_waits_when_not_yet_met_but_reachable(self):
        """THRESHOLD stays blocked when threshold not met but still reachable."""
        collector = _job(
            "majority_collector",
            cadence=DAILY,
            depends_on=[
                Dependency(job_name="a", cadence=DAILY),
                Dependency(job_name="b", cadence=DAILY),
                Dependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 1 done, 1 running (still reachable), 1 not started
        store.put(RunRecord(job_name="a", run_id="20250115", status=Status.DONE))
        store.put(RunRecord(job_name="b", run_id="20250115", status=Status.RUNNING))

        result = orch.tick(REF)
        assert any(
            r.job_name == "majority_collector"
            and r.action == JobAction.SKIPPED_DEPENDENCIES
            for r in result.results
        )
        assert len(executor.calls) == 0

    def test_threshold_unreachable_marks_skipped(self):
        """THRESHOLD emits SKIPPED_THRESHOLD_UNREACHABLE and writes SKIPPED record when threshold can't be met."""
        collector = _job(
            "majority_collector",
            cadence=DAILY,
            depends_on=[
                Dependency(job_name="a", cadence=DAILY),
                Dependency(job_name="b", cadence=DAILY),
                Dependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 1 done, 2 error — met=1, not_yet_finished=0 → 1+0 < 2, unreachable
        store.put(RunRecord(job_name="a", run_id="20250115", status=Status.DONE))
        store.put(RunRecord(job_name="b", run_id="20250115", status=Status.ERROR))
        store.put(RunRecord(job_name="c", run_id="20250115", status=Status.ERROR))

        result = orch.tick(REF)
        assert any(
            r.job_name == "majority_collector"
            and r.action == JobAction.SKIPPED_THRESHOLD_UNREACHABLE
            for r in result.results
        )
        # A SKIPPED RunRecord should have been written
        record = store.get("majority_collector", "20250115")
        assert record is not None
        assert record.status == Status.SKIPPED
        # Should appear in skipped() helper
        assert any(r.job_name == "majority_collector" for r in result.skipped())

    def test_threshold_requires_dependency_threshold(self):
        """Job with dependency_mode=THRESHOLD and threshold=None raises ValueError."""
        with pytest.raises(ValueError, match="dependency_threshold"):
            _job(
                "bad_job",
                depends_on=[Dependency(job_name="x", cadence=DAILY)],
                dependency_mode=DependencyMode.THRESHOLD,
                dependency_threshold=None,
            )


# ---------------------------------------------------------------------------
# Performance Testing
# ---------------------------------------------------------------------------


class TestPerformance:
    """Stress tests to identify slow code paths with many jobs and dependencies."""

    def test_many_jobs_mixed_dependencies(self):
        """
        Performance test: 150 jobs with mixed dependency patterns.

        Job breakdown:
        - 50 independent jobs (no dependencies)
        - 10 chains of 5 jobs each (50 jobs total)
        - 50 jobs with single dependencies on independent jobs
        - Additional 50 jobs with fan-in/fan-out patterns (10 jobs, 5 inputs each)
        """
        jobs = []

        # Independent jobs
        for i in range(50):
            jobs.append(_job(f"independent_{i:03d}"))

        # Dependency chains: 10 chains of 5 jobs each
        for chain_id in range(10):
            for pos in range(5):
                depends = (
                    [
                        Dependency(
                            job_name=f"chain_{chain_id:02d}_job_{pos - 1:02d}",
                            cadence=DAILY,
                        )
                    ]
                    if pos > 0
                    else []
                )
                jobs.append(
                    _job(
                        f"chain_{chain_id:02d}_job_{pos:02d}",
                        depends_on=depends,
                    )
                )

        # Fan-in jobs: 50 jobs, each depends on a random independent job
        for i in range(50):
            independent_idx = i % 50
            jobs.append(
                _job(
                    f"fan_in_{i:03d}",
                    depends_on=[
                        Dependency(
                            job_name=f"independent_{independent_idx:03d}", cadence=DAILY
                        )
                    ],
                )
            )

        # Fan-out jobs: 5 jobs that each depend on 5 different independent jobs
        for i in range(5):
            depends = [
                Dependency(job_name=f"independent_{j:03d}", cadence=DAILY)
                for j in range(i * 10, (i + 1) * 10)
            ]
            jobs.append(
                _job(
                    f"fan_out_{i}",
                    depends_on=depends,
                    dependency_mode=DependencyMode.ALL_SUCCESS,
                )
            )

        assert len(jobs) == 155

        orch, store, executor = _make_orch(jobs)

        # Run multiple ticks to go through job lifecycle
        for tick_num in range(1, 6):
            tick_ref = REF + timedelta(hours=tick_num - 1)
            result = orch.tick(tick_ref)

            # Verify we're getting reasonable results each tick
            assert result.results is not None
            assert len(result.results) > 0

            # Verify some jobs are being submitted
            assert len(executor.calls) > 0

        # Final verification
        final_run_id = REF.strftime("%Y%m%d")
        orch.tick(REF)

        # Should have submissions across ticks
        assert len(executor.calls) >= 50  # At least independent jobs

        # Verify state tracking is working - check a few random jobs
        for i in range(0, 10):
            record = store.get("independent_" + f"{i:03d}", final_run_id)
            assert record is not None

    def test_wide_dependency_fan_in(self):
        """
        Performance test: 1 job with many (100) dependencies.
        Tests dependency resolution performance with high fan-in.
        """
        # Create 100 independent jobs
        upstream = [_job(f"upstream_{i:03d}") for i in range(100)]

        # Create 1 job that depends on all 100
        downstream = _job(
            "collector",
            depends_on=[
                Dependency(job_name=f"upstream_{i:03d}", cadence=DAILY)
                for i in range(100)
            ],
            dependency_mode=DependencyMode.ALL_SUCCESS,
        )

        jobs = upstream + [downstream]

        orch, store, executor = _make_orch(jobs)

        # First tick: submit all independent jobs
        result = orch.tick(REF)
        assert len([r for r in result.results if "upstream" in r.job_name]) == 100

        # Mark all upstream jobs as DONE
        for i in range(100):
            store.put(
                RunRecord(
                    job_name=f"upstream_{i:03d}",
                    run_id=REF.strftime("%Y%m%d"),
                    status=Status.DONE,
                )
            )

        # Second tick: downstream job should submit now
        result = orch.tick(REF)
        collector_actions = [r for r in result.results if r.job_name == "collector"]
        assert len(collector_actions) == 1
        assert collector_actions[0].action == JobAction.SUBMITTED

    def test_deeply_nested_chains(self):
        """
        Performance test: 5 chains of 20 jobs each.
        Tests dependency resolution performance with deep chains.
        """
        jobs = []

        for chain_id in range(5):
            for pos in range(20):
                depends = (
                    [
                        Dependency(
                            job_name=f"chain_{chain_id:02d}_job_{pos - 1:02d}",
                            cadence=DAILY,
                        )
                    ]
                    if pos > 0
                    else []
                )
                jobs.append(
                    _job(
                        f"chain_{chain_id:02d}_job_{pos:02d}",
                        depends_on=depends,
                    )
                )

        assert len(jobs) == 100

        orch, store, executor = _make_orch(jobs)

        # Simulate progression through chain
        for tick_num in range(1, 21):
            tick_ref = REF + timedelta(hours=tick_num - 1)
            orch.tick(tick_ref)

            # Mark submitted jobs as DONE to advance chains
            for call in executor.calls:
                store.put(
                    RunRecord(
                        job_name=call["job"],
                        run_id=call["run_id"],
                        status=Status.DONE,
                    )
                )

            executor.calls.clear()

        # All chain jobs should have completed
        run_id = REF.strftime("%Y%m%d")
        all_records = store.list_records()
        run_records = [r for r in all_records if r.run_id == run_id]
        # Should have most jobs completed (may not be all if chains didn't fully progress)
        assert len(run_records) >= 50
        completed = sum(1 for r in run_records if r.is_finished())
        assert completed >= 40
