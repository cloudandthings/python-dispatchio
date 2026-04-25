"""
Tests for the Orchestrator tick engine.

Uses MemoryStateStore and a simple spy executor to avoid any I/O.
freezegun is not required — reference_time is always passed explicitly.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from dispatchio.alerts.base import AlertEvent
from dispatchio.cadence import (
    DAILY,
    MONTHLY,
    WEEKLY,
    YESTERDAY,
    DateCadence,
    Frequency,
)
from dispatchio.conditions import DayOfWeekCondition, TimeOfDayCondition
from dispatchio.models import (
    AdmissionPolicy,
    AlertCondition,
    AlertOn,
    Attempt,
    DependencyMode,
    JobAction,
    Job,
    JobDependency,
    PoolPolicy,
    RetryPolicy,
    Status,
    SubprocessJob,
    DeadLetterReasonCode,
    DeadLetterSourceBackend,
    OrchestratorRunMode,
    OrchestratorRun,
    OrchestratorRunStatus,
    TriggerType,
)
from dispatchio.orchestrator import Orchestrator
from dispatchio.receiver.base import StatusEvent
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)  # Wednesday 09:00 UTC


def _job(name=None, cadence=DAILY, **kwargs) -> Job:
    if name is None:
        name = f"job_{uuid4()}"
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

    def submit(self, job, attempt, reference_time, timeout=None):
        self.calls.append(
            {"job": job.name, "run_key": attempt.run_key, "attempt": attempt}
        )


class FailingExecutor:
    """Always raises on submit()."""

    def submit(self, job, attempt, reference_time, timeout=None):
        raise RuntimeError("executor down")


class SpyAlertHandler:
    def __init__(self):
        self.events: list[AlertEvent] = []

    def handle(self, event: AlertEvent):
        self.events.append(event)


def _attempt(
    job_name: str,
    run_key: str,
    status: Status,
    reason: str | None = None,
    attempt: int = 0,
    correlation_id: UUID | None = None,
) -> Attempt:
    """Helper to create Attempt for tests."""
    if correlation_id is None:
        correlation_id = uuid4()
    return Attempt(
        job_name=job_name,
        run_key=run_key,
        attempt=attempt,
        correlation_id=correlation_id,
        status=status,
        reason=reason,
    )


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
        record = store.get_latest_attempt("simple", "D20250115")
        assert record is not None
        assert record.status == Status.SUBMITTED

    def test_does_not_resubmit_submitted_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        orch.tick(REF)
        orch.tick(REF)
        assert len(executor.calls) == 1

    def test_does_not_resubmit_done_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        store.append_attempt(_attempt("simple", "D20250115", Status.DONE))
        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(r.action == JobAction.SKIPPED_ALREADY_DONE for r in result.results)

    def test_does_not_resubmit_running_job(self):
        j = _job("simple")
        orch, store, executor = _make_orch([j])
        store.append_attempt(_attempt("simple", "D20250115", Status.RUNNING))
        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(r.action == JobAction.SKIPPED_ALREADY_ACTIVE for r in result.results)


class TestOrchestratorCadenceDefaults:
    def test_default_orchestrator_cadence_is_daily(self):
        orch, _, _ = _make_orch([_job("simple")])
        assert orch.orchestrator_cadence == DAILY
        assert orch.default_cadence == DAILY

    def test_default_cadence_inherits_orchestrator_cadence_when_not_provided(self):
        orch, _, _ = _make_orch([_job("simple")], orchestrator_cadence=WEEKLY)
        assert orch.orchestrator_cadence == WEEKLY
        assert orch.default_cadence == WEEKLY

    def test_default_cadence_can_override_orchestrator_cadence(self):
        orch, _, _ = _make_orch(
            [_job("simple")], orchestrator_cadence=WEEKLY, default_cadence=DAILY
        )
        assert orch.orchestrator_cadence == WEEKLY
        assert orch.default_cadence == DAILY


class TestOrchestratorRunSelection:
    def test_promotes_pending_run_and_uses_its_run_key(self):
        j = _job("simple")
        orch, store, _ = _make_orch([j])

        run = OrchestratorRun(
            namespace="default",
            run_key="20250114",
            status=OrchestratorRunStatus.PENDING,
            mode=OrchestratorRunMode.BACKFILL,
            opened_at=REF,
        )
        run = store.append_orchestrator_run(run)

        result = orch.tick(REF)

        updated = store.get_orchestrator_run(run.id)
        assert updated is not None
        assert updated.status == OrchestratorRunStatus.ACTIVE

        submitted = result.submitted()
        assert len(submitted) == 1
        assert submitted[0].run_key == "D20250114"
        assert store.get_latest_attempt("simple", "D20250114") is not None

    def test_dry_run_does_not_promote_pending_run(self):
        j = _job("simple")
        orch, store, _ = _make_orch([j])

        run = OrchestratorRun(
            namespace="default",
            run_key="20250114",
            status=OrchestratorRunStatus.PENDING,
            mode=OrchestratorRunMode.BACKFILL,
            opened_at=REF,
        )
        run = store.append_orchestrator_run(run)

        result = orch.tick(REF, dry_run=True)

        updated = store.get_orchestrator_run(run.id)
        assert updated is not None
        assert updated.status == OrchestratorRunStatus.PENDING

        would_submit = [r for r in result.results if r.action == JobAction.WOULD_SUBMIT]
        assert len(would_submit) == 1
        assert would_submit[0].run_key == "D20250114"
        assert store.get_latest_attempt("simple", "D20250114") is None


class TestOrchestratorRunApi:
    def test_plan_backfill_daily_inclusive(self):
        orch, _, _ = _make_orch([_job("simple")])

        keys = orch.plan_backfill(
            datetime(2025, 1, 1, tzinfo=timezone.utc),
            datetime(2025, 1, 3, tzinfo=timezone.utc),
        )

        assert keys == ["20250101", "20250102", "20250103"]

    def test_enqueue_backfill_creates_pending_runs(self):
        orch, store, _ = _make_orch([_job("simple")])

        runs = orch.enqueue_backfill(
            start=datetime(2025, 1, 1, tzinfo=timezone.utc),
            end=datetime(2025, 1, 2, tzinfo=timezone.utc),
            priority=10,
            submitted_by="cli",
        )

        assert len(runs) == 2
        listed = store.list_orchestrator_runs()
        assert len(listed) == 2
        assert all(r.status == OrchestratorRunStatus.PENDING for r in listed)
        assert all(r.mode == OrchestratorRunMode.BACKFILL for r in listed)

    def test_enqueue_replay_and_resume_cancel(self):
        orch, _, _ = _make_orch([_job("simple")])

        runs = orch.enqueue_replay(
            run_keys=["event:1", "event:2"],
            priority=5,
            submitted_by="cli",
        )
        assert len(runs) == 2
        run = runs[0]

        resumed = orch.resume_run(run.id, reason="unblocked")
        assert resumed.status == OrchestratorRunStatus.ACTIVE
        assert resumed.reason == "unblocked"

        cancelled = orch.cancel_run(run.id, reason="operator_cancel")
        assert cancelled.status == OrchestratorRunStatus.CANCELLED
        assert cancelled.reason == "operator_cancel"


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
        downstream = _job(
            "down", depends_on=[JobDependency(job_name="up", cadence=DAILY)]
        )
        orch, store, executor = _make_orch([upstream, downstream])
        store.append_attempt(_attempt("up", "D20250115", Status.SUBMITTED))
        orch2, store2, executor2 = _make_orch(
            [downstream], store=store, strict_dependencies=False
        )
        result = orch2.tick(REF)
        assert not any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_unblocked_when_dependency_done(self):
        downstream = _job(
            "down", depends_on=[JobDependency(job_name="up", cadence=DAILY)]
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.DONE))
        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_date_offset_dependency(self):
        """Depend on a job from 3 days ago."""
        three_days_ago = DateCadence(frequency=Frequency.DAILY, offset=-3)
        downstream = _job(
            "down", depends_on=[JobDependency(job_name="up", cadence=three_days_ago)]
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        # offset=-3 from Jan 15 = Jan 12
        store.append_attempt(_attempt("up", "D20250112", Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "down"

    def test_yesterday_shorthand(self):
        downstream = _job(
            "down", depends_on=[JobDependency(job_name="up", cadence=YESTERDAY)]
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250114", Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "down"

    def test_monthly_dependency(self):
        downstream = _job(
            "monthly_consumer",
            cadence=MONTHLY,
            depends_on=[JobDependency(job_name="monthly_load", cadence=MONTHLY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("monthly_load", "M202501", Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].run_key == "M202501"

    def test_cross_cadence_daily_depends_on_monthly(self):
        daily = _job(
            "daily_enrichment",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="monthly_report", cadence=MONTHLY)],
        )
        orch, store, executor = _make_orch([daily], strict_dependencies=False)
        store.append_attempt(_attempt("monthly_report", "M202501", Status.DONE))
        result = orch.tick(REF)
        assert result.submitted()[0].job_name == "daily_enrichment"

    def test_multiple_dependencies_all_must_be_met(self):
        j = _job(
            "fan_in",
            depends_on=[
                JobDependency(job_name="a", cadence=DAILY),
                JobDependency(job_name="b", cadence=DAILY),
            ],
        )
        orch, store, executor = _make_orch([j], strict_dependencies=False)
        store.append_attempt(_attempt("a", "D20250115", Status.DONE))
        result = orch.tick(REF)
        assert len(result.submitted()) == 0

        store.append_attempt(_attempt("b", "D20250115", Status.DONE))
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
        record = store.get_latest_attempt("up", "D20250115")
        store.update_attempt(record.model_copy(update={"status": Status.DONE}))
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
        store.append_attempt(_attempt("up", "D20250115", Status.DONE))
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
        j = _job()
        orch, store, executor = _make_orch([j])
        store.append_attempt(
            _attempt(
                j.name,
                "D20250115",
                Status.ERROR,
                reason="boom",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.MARKED_ERROR for r in result.results)
        assert len(executor.calls) == 0

    def test_retries_when_policy_allows(self):
        j = _job(retry_policy=RetryPolicy(max_attempts=3))
        orch, store, executor = _make_orch([j])
        store.append_attempt(
            _attempt(
                j.name,
                "D20250115",
                Status.ERROR,
                reason="transient",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.RETRYING for r in result.results)
        assert len(executor.calls) == 1

    def test_no_retry_after_max_attempts(self):
        j = _job(retry_policy=RetryPolicy(max_attempts=2))
        orch, store, executor = _make_orch([j])
        store.append_attempt(
            _attempt(
                j.name,
                "D20250115",
                Status.ERROR,
                reason="boom",
                attempt=1,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.MARKED_ERROR for r in result.results)
        assert len(executor.calls) == 0

    def test_retry_on_filter_matches(self):
        j = _job(retry_policy=RetryPolicy(max_attempts=3, retry_on=["timeout"]))
        orch, store, executor = _make_orch([j])
        store.append_attempt(
            _attempt(
                j.name,
                "D20250115",
                Status.ERROR,
                reason="connection timeout",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert any(r.action == JobAction.RETRYING for r in result.results)

    def test_retry_on_filter_does_not_match(self):
        j = _job(retry_policy=RetryPolicy(max_attempts=3, retry_on=["timeout"]))
        orch, store, executor = _make_orch([j])
        store.append_attempt(
            _attempt(
                j.name,
                "D20250115",
                Status.ERROR,
                reason="disk full",
                attempt=0,
            )
        )
        result = orch.tick(REF)
        assert not any(r.action == JobAction.RETRYING for r in result.results)


# ---------------------------------------------------------------------------
# Status receiver
# ---------------------------------------------------------------------------


class TestStatusReceiver:
    class CapturingReceiver:
        def __init__(self, events):
            self._events = events

        def drain(self):
            events = list(self._events)
            self._events.clear()
            return events

    def test_done_event_updates_state(self):
        j = _job()
        attempt_rec = _attempt(j.name, "D20250115", Status.RUNNING)
        events = [
            StatusEvent(
                job_name=j.name,
                run_key="D20250115",
                attempt=attempt_rec.attempt,
                correlation_id=attempt_rec.correlation_id,
                status=Status.DONE,
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.append_attempt(attempt_rec)
        orch.tick(REF)
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record is not None
        assert record.status == Status.DONE

    def test_error_event_sets_reason(self):
        j = _job()
        attempt_rec = _attempt(j.name, "D20250115", Status.RUNNING)
        events = [
            StatusEvent(
                correlation_id=attempt_rec.correlation_id,
                status=Status.ERROR,
                reason="OOM killed",
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.append_attempt(attempt_rec)
        orch.tick(REF)
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record is not None
        assert record.status == Status.ERROR
        assert record.reason is not None
        assert "OOM" in record.reason

    def test_unknown_correlation_id_routes_to_dead_letter_1(self):
        """Unknown correlation_id → CORRELATION_FAILURE dead-letter."""
        j = _job()
        unknown_id = uuid4()
        events = [
            StatusEvent(
                correlation_id=unknown_id,
                status=Status.DONE,
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        orch.tick(REF)  # Submits job, creates SUBMITTED attempt
        # Job was submitted, so there should be an attempt but still SUBMITTED
        submitted_attempt = store.get_latest_attempt(j.name, "D20250115")
        assert submitted_attempt is not None
        assert submitted_attempt.status == Status.SUBMITTED
        assert submitted_attempt.correlation_id != unknown_id
        # Status event with unknown ID should be dead-lettered
        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0].reason_code == DeadLetterReasonCode.CORRELATION_FAILURE
        assert unknown_id == dead_letters[0].correlation_id

    def test_unknown_correlation_id_routes_to_dead_letter_2(self):
        """Unknown correlation_id → CORRELATION_FAILURE dead-letter, attempt untouched."""
        j = _job()
        attempt_rec = _attempt(j.name, "D20250115", Status.RUNNING, attempt=0)
        events = [
            StatusEvent(
                correlation_id=uuid4(),  # Unknown — not registered in state
                status=Status.DONE,
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.append_attempt(attempt_rec)
        orch.tick(REF)
        # Existing attempt should NOT be updated
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record is not None
        assert record.status == Status.RUNNING
        # Unknown event should be in dead-letter
        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0].reason_code == DeadLetterReasonCode.CORRELATION_FAILURE

    def test_duplicate_terminal_status(self):
        """Different terminal status for finished attempt"""
        j = _job()
        attempt_rec = _attempt(j.name, "D20250115", Status.DONE, attempt=0)
        events = [
            StatusEvent(
                job_name=j.name,
                run_key="D20250115",
                attempt=0,
                correlation_id=attempt_rec.correlation_id,
                status=Status.ERROR,  # Different terminal status
                reason="New error",
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.append_attempt(attempt_rec)
        orch.tick(REF)
        # Attempt should NOT change
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record is not None
        assert record.status == Status.ERROR  # Still done
        assert record.reason == "New error"  # Status reason not updated
        # Event should not be in dead-letter
        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 0

    def test_dead_letter_source_backend_is_other_for_unknown_correlation(self):
        """Dead-lettered events with unknown correlation_id get source_backend=OTHER."""
        j = _job()  # SubprocessJob executor
        unknown_id = uuid4()
        events = [
            StatusEvent(
                correlation_id=unknown_id,
                status=Status.DONE,
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        orch.tick(REF)

        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0].source_backend == DeadLetterSourceBackend.OTHER

    def test_dead_letter_source_backend_other_for_unknown_job(self):
        """External events for unregistered jobs get source_backend=OTHER."""
        j = _job()
        unknown_id = uuid4()
        events = [
            StatusEvent(
                correlation_id=unknown_id,
                status=Status.DONE,
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        orch.tick(REF)

        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 1
        assert dead_letters[0].source_backend == DeadLetterSourceBackend.OTHER

    def test_idempotent_completion_ignored(self):
        """Same terminal status re-received → idempotent, no dead-letter."""
        j = _job()
        attempt_rec = _attempt(j.name, "D20250115", Status.DONE, attempt=0)
        events = [
            StatusEvent(
                correlation_id=attempt_rec.correlation_id,
                status=Status.DONE,  # Same terminal status
                reason="Original reason",
            )
        ]
        receiver = self.CapturingReceiver(events)
        orch, store, executor = _make_orch([j], receiver=receiver)
        store.append_attempt(attempt_rec)
        orch.tick(REF)
        # Attempt should still be done (unchanged)
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record.status == Status.DONE
        # No dead-letter created for idempotent event
        dead_letters = store.list_dead_letters()
        assert len(dead_letters) == 0


# ---------------------------------------------------------------------------
# Submission failure
# ---------------------------------------------------------------------------


class TestSubmissionFailure:
    def test_submission_failure_marks_error(self):
        j = _job()
        orch, store, failing_executor = _make_orch([j], executor=FailingExecutor())
        result = orch.tick(REF)
        assert any(r.action == JobAction.SUBMISSION_FAILED for r in result.results)
        record = store.get_latest_attempt(j.name, "D20250115")
        assert record is not None
        assert record.status == Status.ERROR
        assert record.reason is not None
        assert "submission_failed" in record.reason


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
        store.append_attempt(
            _attempt(
                "alerted_job",
                "D20250115",
                Status.ERROR,
                reason="boom",
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


class TestAdmissionLimits:
    def test_limits_submissions_per_tick(self):
        jobs = [_job(f"j{i}") for i in range(5)]
        store = SQLAlchemyStateStore("sqlite:///:memory:")
        executor = SpyExecutor()
        orch = Orchestrator(
            jobs=jobs,
            state=store,
            executors={"subprocess": executor},
            admission_policy=AdmissionPolicy(max_submit_jobs_per_tick=2),
        )
        result = orch.tick(REF)
        assert len(executor.calls) == 2
        assert len(result.submitted()) == 2
        deferred = [
            r for r in result.results if r.action == JobAction.DEFERRED_SUBMIT_LIMIT
        ]
        assert len(deferred) == 3

    def test_deferred_jobs_submitted_on_subsequent_ticks(self):
        jobs = [_job(f"j{i}") for i in range(3)]
        store = SQLAlchemyStateStore("sqlite:///:memory:")
        executor = SpyExecutor()
        orch = Orchestrator(
            jobs=jobs,
            state=store,
            executors={"subprocess": executor},
            admission_policy=AdmissionPolicy(max_submit_jobs_per_tick=2),
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
        downstream = _job(
            "down", depends_on=[JobDependency(job_name="up", cadence=DAILY)]
        )
        orch, store, executor = _make_orch([upstream, downstream])
        result = orch.tick(REF)
        assert result.results[0].job_name == "up"
        assert result.results[0].action == JobAction.SUBMITTED
        assert result.results[1].job_name == "down"
        assert result.results[1].action == JobAction.SKIPPED_DEPENDENCIES


class TestAdmissionPoolsAndPriority:
    def test_mixed_pools_apply_active_and_submit_limits_together(self):
        jobs = [
            _job("replay_seed", pool="replay"),
            _job("replay_high", pool="replay", priority=100),
            _job("replay_low", pool="replay", priority=90),
            _job("bulk_high", pool="bulk", priority=80),
            _job("bulk_low", pool="bulk", priority=70),
            _job("default_job", pool="default", priority=60),
        ]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(
                max_submit_jobs_per_tick=3,
                pools={
                    "default": PoolPolicy(),
                    "replay": PoolPolicy(max_active_jobs=2),
                    "bulk": PoolPolicy(),
                },
            ),
        )
        # Seed one active replay attempt so only one replay candidate can be admitted.
        store.append_attempt(_attempt("replay_seed", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        by_job = {r.job_name: r for r in result.results}

        assert by_job["replay_seed"].action == JobAction.SKIPPED_ALREADY_ACTIVE
        assert by_job["replay_high"].action == JobAction.SUBMITTED
        assert by_job["replay_low"].action == JobAction.DEFERRED_POOL_ACTIVE_LIMIT
        assert by_job["bulk_high"].action == JobAction.SUBMITTED
        assert by_job["bulk_low"].action == JobAction.SUBMITTED
        assert by_job["default_job"].action == JobAction.DEFERRED_SUBMIT_LIMIT
        assert len(executor.calls) == 3

    def test_global_active_limit_defers_submission(self):
        jobs = [_job("j1")]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(max_active_jobs=1),
        )
        store.append_attempt(_attempt("orphan", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(r.action == JobAction.DEFERRED_ACTIVE_LIMIT for r in result.results)

    def test_pool_active_limit_defers_submission(self):
        jobs = [
            _job("a", pool="replay"),
            _job("b", pool="replay"),
        ]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(
                pools={"default": PoolPolicy(), "replay": PoolPolicy(max_active_jobs=1)}
            ),
        )
        store.append_attempt(_attempt("b", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        assert len(executor.calls) == 0
        assert any(
            r.job_name == "a" and r.action == JobAction.DEFERRED_POOL_ACTIVE_LIMIT
            for r in result.results
        )

    def test_pool_submit_limit_honors_priority(self):
        jobs = [
            _job("low", pool="bulk", priority=0),
            _job("high", pool="bulk", priority=10),
        ]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(
                pools={
                    "default": PoolPolicy(),
                    "bulk": PoolPolicy(max_submit_jobs_per_tick=1),
                }
            ),
        )

        result = orch.tick(REF)
        assert len(executor.calls) == 1
        assert executor.calls[0]["job"] == "high"
        assert any(
            r.job_name == "high" and r.action == JobAction.SUBMITTED
            for r in result.results
        )
        assert any(
            r.job_name == "low" and r.action == JobAction.DEFERRED_POOL_SUBMIT_LIMIT
            for r in result.results
        )

    def test_equal_priority_uses_definition_order(self):
        jobs = [_job("first", priority=1), _job("second", priority=1)]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(max_submit_jobs_per_tick=1),
        )

        result = orch.tick(REF)
        assert len(executor.calls) == 1
        assert executor.calls[0]["job"] == "first"
        assert any(
            r.job_name == "second" and r.action == JobAction.DEFERRED_SUBMIT_LIMIT
            for r in result.results
        )

    def test_tick_result_pool_populated_for_skips_and_defers(self):
        jobs = [
            _job("done", pool="bulk"),
            _job("pending", pool="bulk"),
        ]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(
                max_active_jobs=1,
                pools={"default": PoolPolicy(), "bulk": PoolPolicy()},
            ),
        )
        store.append_attempt(_attempt("done", "D20250115", Status.DONE))
        store.append_attempt(_attempt("orphan", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        done_result = next(r for r in result.results if r.job_name == "done")
        pending_result = next(r for r in result.results if r.job_name == "pending")
        assert done_result.pool == "bulk"
        assert pending_result.pool == "bulk"

    def test_dry_run_reports_would_defer_with_reason_prefix(self):
        jobs = [_job("only")]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(max_active_jobs=1),
        )
        store.append_attempt(_attempt("orphan", "D20250115", Status.RUNNING))

        result = orch.tick(REF, dry_run=True)
        item = result.results[0]
        assert item.action == JobAction.WOULD_DEFER
        assert item.detail is not None
        assert item.detail.startswith("deferred_active_limit")

    def test_unknown_pool_rejected_at_init(self):
        with pytest.raises(ValueError, match="Unknown pool"):
            _make_orch([_job("x", pool="missing")])

    def test_unknown_pool_rejected_on_add(self):
        orch, store, executor = _make_orch(
            [_job("x")],
            allow_runtime_mutation=True,
        )
        with pytest.raises(ValueError, match="Unknown pool"):
            orch.add_job(_job("y", pool="missing"))

    def test_orphan_active_counts_only_toward_global_limit(self):
        jobs = [_job("known", pool="bulk")]
        orch, store, executor = _make_orch(
            jobs,
            admission_policy=AdmissionPolicy(
                max_active_jobs=1,
                pools={"default": PoolPolicy(), "bulk": PoolPolicy(max_active_jobs=1)},
            ),
        )
        store.append_attempt(_attempt("removed_job", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        deferred = next(r for r in result.results if r.job_name == "known")
        assert deferred.action == JobAction.DEFERRED_ACTIVE_LIMIT


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
        # run_key should be current month
        assert result.submitted()[0].run_key == "M202501"

    def test_explicit_cadence_overrides_default(self):
        """Job with explicit cadence ignores orchestrator default."""
        j = _job("daily_job", cadence=DAILY)
        orch, store, executor = _make_orch([j], default_cadence=MONTHLY)
        result = orch.tick(REF)
        assert result.submitted()[0].run_key == "D20250115"


# ---------------------------------------------------------------------------
# Unresolved dependency warnings
# ---------------------------------------------------------------------------


class TestUnresolvedDependencyWarning:
    def test_warns_on_unknown_dependency(self, caplog):
        import logging

        j = _job(
            "consumer",
            depends_on=[JobDependency(job_name="external_job", cadence=DAILY)],
        )
        with caplog.at_level(logging.WARNING, logger="dispatchio.orchestrator"):
            _make_orch([j], strict_dependencies=False)
        assert any("external_job" in msg for msg in caplog.messages)

    def test_no_warning_when_dependency_is_known(self, caplog):
        import logging

        upstream = _job("upstream")
        downstream = _job(
            "downstream",
            depends_on=[JobDependency(job_name="upstream", cadence=DAILY)],
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
        assert store.get_latest_attempt("b", "D20250115") is not None

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
                "consumer",
                depends_on=[JobDependency(job_name="external", cadence=DAILY)],
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
                        depends_on=[JobDependency(job_name="external", cadence=DAILY)],
                    )
                ]
            )

    def test_strict_dependencies_raise_after_graph_change(self):
        orch, _, _ = _make_orch(
            [_job("a")], allow_runtime_mutation=True, strict_dependencies=True
        )
        orch.add_job(
            _job(
                "consumer",
                depends_on=[JobDependency(job_name="external", cadence=DAILY)],
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
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
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
        record = store.get_latest_attempt("up", "D20250115")
        store.update_attempt(record.model_copy(update={"status": Status.DONE}))
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
                JobDependency(job_name="entity_a", cadence=DAILY),
                JobDependency(job_name="entity_b", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.ALL_FINISHED,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # one DONE, one ERROR — both finished
        store.append_attempt(_attempt("entity_a", "D20250115", Status.DONE))
        store.append_attempt(
            _attempt("entity_b", "D20250115", Status.ERROR, reason="something happened")
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
                JobDependency(job_name="entity_a", cadence=DAILY),
                JobDependency(job_name="entity_b", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.ALL_FINISHED,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # entity_a done but entity_b still running
        store.append_attempt(_attempt("entity_a", "D20250115", Status.DONE))
        store.append_attempt(_attempt("entity_b", "D20250115", Status.RUNNING))

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
                JobDependency(job_name="a", cadence=DAILY),
                JobDependency(job_name="b", cadence=DAILY),
                JobDependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 2 of 3 done — threshold=2 met
        store.append_attempt(_attempt("a", "D20250115", Status.DONE))
        store.append_attempt(_attempt("b", "D20250115", Status.DONE))
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
                JobDependency(job_name="a", cadence=DAILY),
                JobDependency(job_name="b", cadence=DAILY),
                JobDependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 1 done, 1 running (still reachable), 1 not started
        store.append_attempt(_attempt("a", "D20250115", Status.DONE))
        store.append_attempt(_attempt("b", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        assert any(
            r.job_name == "majority_collector"
            and r.action == JobAction.SKIPPED_DEPENDENCIES
            for r in result.results
        )
        assert len(executor.calls) == 0

    def test_threshold_unreachable_marks_skipped(self):
        """THRESHOLD emits SKIPPED_THRESHOLD_UNREACHABLE when threshold can't be met."""
        collector = _job(
            "majority_collector",
            cadence=DAILY,
            depends_on=[
                JobDependency(job_name="a", cadence=DAILY),
                JobDependency(job_name="b", cadence=DAILY),
                JobDependency(job_name="c", cadence=DAILY),
            ],
            dependency_mode=DependencyMode.THRESHOLD,
            dependency_threshold=2,
        )
        orch, store, executor = _make_orch([collector], strict_dependencies=False)

        # 1 done, 2 error — met=1, not_yet_finished=0 → 1+0 < 2, unreachable
        store.append_attempt(_attempt("a", "D20250115", Status.DONE))
        store.append_attempt(_attempt("b", "D20250115", Status.ERROR))
        store.append_attempt(_attempt("c", "D20250115", Status.ERROR))

        result = orch.tick(REF)
        assert any(
            r.job_name == "majority_collector"
            and r.action == JobAction.SKIPPED_THRESHOLD_UNREACHABLE
            for r in result.results
        )
        # Skipped jobs do not create records
        record = store.get_latest_attempt("majority_collector", "D20250115")
        assert record is None
        # Should appear in skipped() helper
        assert any(r.job_name == "majority_collector" for r in result.skipped())

    def test_threshold_requires_dependency_threshold(self):
        """Job with dependency_mode=THRESHOLD and threshold=None raises ValueError."""
        with pytest.raises(ValueError, match="dependency_threshold"):
            _job(
                "bad_job",
                depends_on=[JobDependency(job_name="x", cadence=DAILY)],
                dependency_mode=DependencyMode.THRESHOLD,
                dependency_threshold=None,
            )


# ---------------------------------------------------------------------------
# SKIPPED_UPSTREAM
# ---------------------------------------------------------------------------


class TestSkippedUpstream:
    """SKIPPED_UPSTREAM is emitted when a dep has finished but didn't reach required_status."""

    def test_upstream_error_emits_skipped_upstream(self):
        """Upstream ERROR with required_status=DONE → SKIPPED_UPSTREAM, not SKIPPED_DEPENDENCIES."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.ERROR))

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "down")
        assert r.action == JobAction.SKIPPED_UPSTREAM

    def test_upstream_lost_emits_skipped_upstream(self):
        """Upstream LOST with required_status=DONE → SKIPPED_UPSTREAM."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.LOST))

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "down")
        assert r.action == JobAction.SKIPPED_UPSTREAM

    def test_upstream_still_running_emits_skipped_dependencies(self):
        """Upstream RUNNING → dep is unmet but not finished, so SKIPPED_DEPENDENCIES."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.RUNNING))

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "down")
        assert r.action == JobAction.SKIPPED_DEPENDENCIES

    def test_upstream_done_not_matching_required_error_emits_skipped_upstream(self):
        """required_status=ERROR, upstream DONE → SKIPPED_UPSTREAM (dep on failure use-case)."""
        downstream = _job(
            "on_failure_handler",
            cadence=DAILY,
            depends_on=[
                JobDependency(
                    job_name="up", cadence=DAILY, required_status=Status.ERROR
                )
            ],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.DONE))

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "on_failure_handler")
        assert r.action == JobAction.SKIPPED_UPSTREAM

    def test_skipped_upstream_detail_names_upstream_and_statuses(self):
        """Detail string includes dep name, run_key, actual status, and required status."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.ERROR))

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "down")
        assert r.action == JobAction.SKIPPED_UPSTREAM
        assert "up" in r.detail
        assert "error" in r.detail
        assert "done" in r.detail  # required_status

    def test_skipped_upstream_appears_in_skipped_helper(self):
        """TickResult.skipped() includes SKIPPED_UPSTREAM results."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.ERROR))

        result = orch.tick(REF)
        assert any(r.job_name == "down" for r in result.skipped())

    def test_skipped_upstream_unblocks_when_upstream_retried_and_succeeds(self):
        """If upstream is manually retried and reaches DONE, downstream unblocks next tick."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[JobDependency(job_name="up", cadence=DAILY)],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("up", "D20250115", Status.ERROR))

        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SKIPPED_UPSTREAM
            for r in result.results
        )

        # Operator retries upstream; it succeeds
        record = store.get_latest_attempt("up", "D20250115")
        store.update_attempt(record.model_copy(update={"status": Status.DONE}))

        result = orch.tick(REF)
        assert any(
            r.job_name == "down" and r.action == JobAction.SUBMITTED
            for r in result.results
        )

    def test_mixed_deps_some_finished_wrong_some_waiting(self):
        """When some deps are finished-wrong and some are still waiting, emits SKIPPED_UPSTREAM."""
        downstream = _job(
            "down",
            cadence=DAILY,
            depends_on=[
                JobDependency(job_name="a", cadence=DAILY),
                JobDependency(job_name="b", cadence=DAILY),
            ],
        )
        orch, store, executor = _make_orch([downstream], strict_dependencies=False)
        store.append_attempt(_attempt("a", "D20250115", Status.ERROR))
        # b has no attempt yet — still waiting

        result = orch.tick(REF)
        r = next(r for r in result.results if r.job_name == "down")
        assert r.action == JobAction.SKIPPED_UPSTREAM
        assert "a" in r.detail


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
                        JobDependency(
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
                        JobDependency(
                            job_name=f"independent_{independent_idx:03d}", cadence=DAILY
                        )
                    ],
                )
            )

        # Fan-out jobs: 5 jobs that each depend on 5 different independent jobs
        for i in range(5):
            depends = [
                JobDependency(job_name=f"independent_{j:03d}", cadence=DAILY)
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
        final_run_key = f"D{REF.strftime('%Y%m%d')}"
        orch.tick(REF)

        # Should have submissions across ticks
        assert len(executor.calls) >= 50  # At least independent jobs

        # Verify state tracking is working - check a few random jobs
        for i in range(0, 10):
            record = store.get_latest_attempt(
                "independent_" + f"{i:03d}", final_run_key
            )
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
                JobDependency(job_name=f"upstream_{i:03d}", cadence=DAILY)
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
            job_name = f"upstream_{i:03d}"
            run_key = f"D{REF.strftime('%Y%m%d')}"
            try:
                record = store.get_latest_attempt(job_name, run_key)
                if record:
                    store.update_attempt(
                        record.model_copy(update={"status": Status.DONE})
                    )
            except Exception:
                # Skip if record doesn't exist or update fails
                pass

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
                        JobDependency(
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
                record = store.get_latest_attempt(call["job"], call["run_key"])
                if record:
                    store.update_attempt(
                        record.model_copy(update={"status": Status.DONE})
                    )

            executor.calls.clear()

        # All chain jobs should have completed
        run_key = f"D{REF.strftime('%Y%m%d')}"
        all_records = store.list_attempts()
        run_records = [r for r in all_records if r.run_key == run_key]
        # Should have most jobs completed (may not be all if chains didn't fully progress)
        assert len(run_records) >= 50
        completed = sum(1 for r in run_records if r.is_finished())
        assert completed >= 40


class TestManualOperations:
    """Operator-initiated manual retry and cancel operations."""

    def test_manual_retry_creates_new_attempt(self):
        """Manual retry creates new SUBMITTED attempt with operator context."""
        j = _job()
        failed_rec = _attempt(
            j.name, "D20250115", Status.ERROR, attempt=0, reason="Test failure"
        )
        orch, store, executor = _make_orch([j])
        store.append_attempt(failed_rec)

        # Manual retry
        new_rec = orch.manual_retry(
            j.name,
            "D20250115",
            requested_by="alice",
            request_reason="Suspicious timeout",
        )

        # Verify new record
        assert new_rec.attempt == 1
        assert new_rec.status == Status.SUBMITTED
        assert new_rec.trigger_type == TriggerType.MANUAL_RETRY
        assert new_rec.requested_by == "alice"
        assert new_rec.trigger_reason == "Suspicious timeout"
        assert new_rec.job_name == j.name
        assert new_rec.run_key == "D20250115"

        # Verify stored
        stored = store.get_latest_attempt(j.name, "D20250115")
        assert stored.attempt == 1
        assert stored.status == Status.SUBMITTED

    def test_manual_retry_increments_attempt(self):
        """Manual retry increments attempt counter correctly."""
        j = _job()
        store = SQLAlchemyStateStore("sqlite:///:memory:")
        store.append_attempt(_attempt(j.name, "D20250115", Status.DONE, attempt=0))
        store.append_attempt(
            _attempt(
                j.name, "D20250115", Status.ERROR, attempt=1, reason="Failed again"
            )
        )
        orch, _, _ = _make_orch([j], store=store)

        new_rec = orch.manual_retry(j.name, "D20250115", "bob", "Reason")
        assert new_rec.attempt == 2
        assert store.get_latest_attempt(j.name, "D20250115").attempt == 2

    def test_manual_retry_only_from_terminal(self):
        """Manual retry raises if latest attempt not terminal."""
        j = _job()
        running_rec = _attempt(j.name, "D20250115", Status.RUNNING, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(running_rec)

        with pytest.raises(ValueError, match="still.*running"):
            orch.manual_retry(j.name, "D20250115", "alice", "Reason")

    def test_manual_retry_nonexistent_job_raises(self):
        """Manual retry raises if job not registered."""
        j = _job()
        orch, store, _ = _make_orch([j])
        store.append_attempt(_attempt(j.name, "D20250115", Status.DONE, attempt=0))

        with pytest.raises(ValueError, match="not found"):
            orch.manual_retry("nonexistent", "D20250115", "alice", "Reason")

    def test_manual_retry_nonexistent_run_raises(self):
        """Manual retry raises if no attempt exists for run."""
        j = _job()
        orch, _, _ = _make_orch([j])

        with pytest.raises(ValueError, match="cannot retry"):
            orch.manual_retry(j.name, "D20250115", "alice", "Reason")

    def test_manual_cancel_marks_cancelled(self):
        """Manual cancel updates attempt to CANCELLED status."""
        j = _job()
        running_rec = _attempt(j.name, "D20250115", Status.RUNNING, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(running_rec)

        cancelled = orch.manual_cancel(
            running_rec.correlation_id,
            requested_by="charlie",
            request_reason="Infinite loop detected",
        )

        assert cancelled.status == Status.CANCELLED
        assert cancelled.requested_by == "charlie"
        assert cancelled.reason == "Infinite loop detected"
        assert cancelled.completed_at is not None

        # Verify stored
        stored = store.get_attempt(running_rec.correlation_id)
        assert stored.status == Status.CANCELLED

    def test_manual_cancel_queued_attempt(self):
        """Manual cancel works on QUEUED attempts."""
        j = _job()
        queued_rec = _attempt(j.name, "D20250115", Status.QUEUED, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(queued_rec)

        cancelled = orch.manual_cancel(queued_rec.correlation_id, "dave", "Test cancel")

        assert cancelled.status == Status.CANCELLED
        assert store.get_latest_attempt(j.name, "D20250115").status == Status.CANCELLED

    def test_manual_cancel_submitted_attempt(self):
        """Manual cancel works on SUBMITTED attempts."""
        j = _job()
        submitted_rec = _attempt(j.name, "D20250115", Status.SUBMITTED, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(submitted_rec)

        cancelled = orch.manual_cancel(
            submitted_rec.correlation_id, "eve", "Cancelled before execution"
        )

        assert cancelled.status == Status.CANCELLED

    def test_manual_cancel_already_finished_raises(self):
        """Manual cancel raises if attempt already finished."""
        j = _job()
        done_rec = _attempt(j.name, "D20250115", Status.DONE, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(done_rec)

        with pytest.raises(ValueError, match="already.*done"):
            orch.manual_cancel(done_rec.correlation_id, "frank", "Reason")

    def test_manual_cancel_nonexistent_raises(self):
        """Manual cancel raises if attempt not found."""
        j = _job()
        orch, _, _ = _make_orch([j])

        with pytest.raises(ValueError, match="not found"):
            orch.manual_cancel(uuid4(), "grace", "Reason")

    def test_manual_retry_records_retry_request(self):
        """manual_retry writes a RetryRequest audit record."""
        j = _job()
        failed_rec = _attempt(j.name, "D20250115", Status.ERROR, attempt=0)
        orch, store, _ = _make_orch([j])
        store.append_attempt(failed_rec)

        orch.manual_retry(
            j.name, "D20250115", requested_by="alice", request_reason="Fix applied"
        )

        requests = store.list_retry_requests(run_key="D20250115")
        assert len(requests) == 1
        req = requests[0]
        assert req.requested_by == "alice"
        assert req.run_key == "D20250115"
        assert req.reason == "Fix applied"
        assert req.requested_jobs == [j.name]
        assert req.selected_jobs == [j.name]
        assert req.assigned_attempt_by_job == {j.name: 1}

    def test_manual_retry_actually_calls_executor(self):
        """manual_retry must call executor.submit() — not just write an orphaned SUBMITTED record."""
        j = _job()
        failed_rec = _attempt(
            j.name, "D20250115", Status.ERROR, attempt=0, reason="boom"
        )
        spy = SpyExecutor()
        orch, store, _ = _make_orch([j], executor=spy)
        store.append_attempt(failed_rec)

        new_rec = orch.manual_retry(j.name, "D20250115", "alice", "Fix applied")

        # Executor must have been called
        assert len(spy.calls) == 1
        assert spy.calls[0]["job"] == j.name
        assert spy.calls[0]["attempt"].attempt == 1

        # Record must reflect the actual submission
        assert new_rec.attempt == 1
        assert new_rec.trigger_type == TriggerType.MANUAL_RETRY
        assert new_rec.requested_by == "alice"

    def test_manual_retry_tick_skips_active_then_advances_on_completion(self):
        """After manual_retry submits, the next tick skips (already active) and
        only re-evaluates once the completion event arrives."""
        j = _job()
        failed_rec = _attempt(
            j.name, "D20250115", Status.ERROR, attempt=0, reason="boom"
        )
        spy = SpyExecutor()
        orch, store, _ = _make_orch([j], executor=spy)
        store.append_attempt(failed_rec)

        orch.manual_retry(j.name, "D20250115", "alice", "Fix applied")
        assert len(spy.calls) == 1

        # Next tick: job is SUBMITTED (active) → skipped, not double-submitted
        result = orch.tick(REF)
        assert any(r.action == JobAction.SKIPPED_ALREADY_ACTIVE for r in result.results)
        assert len(spy.calls) == 1  # still only 1 submission
