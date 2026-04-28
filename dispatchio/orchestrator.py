"""
Orchestrator — the tick engine.

The Orchestrator is stateless between runs. Each tick() call has five phases:

  1. Drain status events from the receiver and update state.
  2. Detect lost jobs (via poke or timeout) and mark them LOST.
  3. Evaluate every Job — pure planning, no side effects:
    a. Skip if already active (SUBMITTED/RUNNING) or finished (DONE/ERROR/LOST/SKIPPED).
    b. Skip if time condition is not met.
    c. Skip if any dependency is not satisfied.
    d. If ERROR/LOST, apply retry logic.
    e. Otherwise, return a pending-submission intent.
  4. Run admission control (active and per-tick limits, global and per-pool).
  5. Execute admitted submissions concurrently via a thread pool.

Separating evaluation (phase 3) from execution (phase 5) means the
planning phase reads a consistent state snapshot, and admission control
can be applied cleanly before any side effects occur.

The caller is responsible for scheduling tick() (e.g. EventBridge cron,
a local cron job, or a simple while loop).
"""

from __future__ import annotations

import calendar
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from collections.abc import Iterable
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from dispatchio.datastore.base import DataStore

from dispatchio.admission import AdmissionCandidate, build_admission_plan
from dispatchio.alerts.base import AlertEvent, AlertHandler, LogAlertHandler
from dispatchio.cadence import DAILY, Cadence, DateCadence
from dispatchio.conditions import TimeOfDayCondition
from dispatchio.executor.base import Executor, Pokeable
from dispatchio.models import (
    AlertOn,
    Dependency,
    DependencyMode,
    AdmissionPolicy,
    EventDependency,
    JobAction,
    Job,
    JobTickResult,
    Attempt,
    RetryRequest,
    Status,
    TickResult,
    TriggerType,
    DeadLetterReasonCode,
    DeadLetter,
    DeadLetterSourceBackend,
    DeadLetterStatus,
    OrchestratorRunMode,
    OrchestratorRun,
    OrchestratorRunStatus,
)
from dispatchio.receiver.base import StatusEvent, StatusReceiver
from dispatchio.run_key_resolution import (
    resolve_run_key_from_orchestrator,
    resolve_orchestrator_run_key,
)
from dispatchio.state.base import StateStore
from dispatchio.tick_log import TickLogRecord, TickLogStore

logger = logging.getLogger(__name__)

_EXECUTOR_TYPE_TO_SOURCE_BACKEND: dict[str, DeadLetterSourceBackend] = {
    "subprocess": DeadLetterSourceBackend.SUBPROCESS,
    "python": DeadLetterSourceBackend.PYTHON,
    "lambda": DeadLetterSourceBackend.LAMBDA,
    "stepfunctions": DeadLetterSourceBackend.STEPFUNCTIONS,
    "athena": DeadLetterSourceBackend.ATHENA,
    "http": DeadLetterSourceBackend.HTTP,
}


@dataclass
class _PendingSubmission:
    """Carries submission intent out of the planning phase."""

    job: Job
    run_key: str
    attempt: int
    trigger_type: TriggerType = TriggerType.SCHEDULED
    trigger_reason: str | None = None
    params: dict[str, str] = field(default_factory=dict)


class Orchestrator:
    """
    Instantiate once with your job definitions and infrastructure, then
    call tick() on a schedule.

    Args:
        jobs:                   List of Job objects to evaluate each tick.
        state:                  StateStore implementation (memory, filesystem, DynamoDB …).
        executors:              Map of executor-type → Executor.
        receiver:               StatusReceiver polled at the start of each tick.
        alert_handler:          AlertHandler for ERROR/LOST/NOT_STARTED_BY conditions.
        submit_workers:         Max threads used to submit jobs in parallel within a tick.
        admission_policy:       Admission limits for active jobs and per-tick submissions.
        submit_timeout:         Per-submission deadline (seconds) forwarded to
                                executor.submit(). Not yet enforced by local executors;
                                reserved for cloud executors where the API call can block.
        orchestrator_cadence:   Cadence defining the scheduling unit for this orchestrator
                                (distinct from job cadences). Defaults to DAILY.
        default_cadence:        Fallback cadence for jobs that don't specify one.
                                If not provided, defaults to orchestrator_cadence.
        strict_dependencies:    If True, unresolved dependencies raise ValueError
                    If False, unresolved deps warn so cross-orchestrator dependencies stay possible.
        allow_runtime_mutation: If True, jobs can be added/removed after tick() has run.
                    If False, the job graph is frozen after the first tick.
    """

    def __init__(
        self,
        jobs: list[Job],
        state: StateStore,
        executors: dict[str, Executor],
        receiver: StatusReceiver | None = None,
        alert_handler: AlertHandler | None = None,
        submit_workers: int = 8,
        admission_policy: AdmissionPolicy | None = None,
        submit_timeout: float | None = None,
        orchestrator_cadence: Cadence = DAILY,
        default_cadence: Cadence | None = None,
        strict_dependencies: bool = True,
        allow_runtime_mutation: bool = False,
        namespace: str = "default",
        tick_log: TickLogStore | None = None,
        data_store: DataStore | None = None,
        settings: object | None = None,
    ) -> None:
        self.namespace = namespace
        self.tick_log = tick_log
        self.jobs = jobs
        self.state = state
        self.executors = executors
        self.receiver = receiver
        self.alert_handler = alert_handler or LogAlertHandler()
        self.submit_workers = submit_workers
        self.admission_policy = admission_policy or AdmissionPolicy()
        self.submit_timeout = submit_timeout
        # orchestrator_cadence defines the scheduling unit for this orchestrator.
        self.orchestrator_cadence = orchestrator_cadence
        # default_cadence is the fallback for jobs without an explicit cadence.
        if default_cadence is None:
            default_cadence = orchestrator_cadence
        self.default_cadence = default_cadence
        self.strict_dependencies = strict_dependencies
        self.allow_runtime_mutation = allow_runtime_mutation
        self.data_store = data_store
        self.settings = settings

        self._job_index: dict[str, Job] = {}
        self._jobs_dirty = False
        self._has_ticked = False

        self._rebuild_job_index()
        self._validate_job_pools(self.jobs)
        self._validate_dependencies()

    def add_job(self, job: Job) -> None:
        """Add a job definition to the orchestrator."""
        self.add_jobs([job])

    def add_jobs(self, jobs: Iterable[Job]) -> None:
        """
        Add job definitions to the orchestrator.

        Duplicate job names are rejected immediately.
        """
        self._ensure_mutation_allowed()
        new_jobs = list(jobs)
        self._validate_duplicate_job_names(self.jobs + new_jobs)
        self._validate_job_pools(new_jobs)
        self.jobs.extend(new_jobs)
        self._jobs_dirty = True

    def remove_job(self, job_name: str) -> Job:
        """
        Remove a job by name and return the removed definition.

        Raises KeyError when the job is not registered.
        """
        self._ensure_mutation_allowed()
        for idx, job in enumerate(self.jobs):
            if job.name == job_name:
                removed = self.jobs.pop(idx)
                self._jobs_dirty = True
                return removed
        raise KeyError(f"Unknown job: {job_name}")

    def _ensure_mutation_allowed(self) -> None:
        if self._has_ticked and not self.allow_runtime_mutation:
            raise RuntimeError(
                "Job mutation is disabled after the first tick. "
                "Set allow_runtime_mutation=True to enable dynamic changes."
            )

    def _rebuild_job_index(self) -> None:
        self._validate_duplicate_job_names(self.jobs)
        self._job_index = {j.name: j for j in self.jobs}

    def _validate_duplicate_job_names(self, jobs: list[Job]) -> None:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for job in jobs:
            if job.name in seen:
                duplicates.add(job.name)
            seen.add(job.name)
        if duplicates:
            names = ", ".join(sorted(duplicates))
            raise ValueError(f"Duplicate job names found: {names}")

    def _unresolved_dependencies(self) -> list[tuple[str, str]]:
        unresolved: list[tuple[str, str]] = []
        for job in self.jobs:
            for dep in job.depends_on:
                if dep.kind == "job" and dep.job_name not in self._job_index:
                    unresolved.append((job.name, dep.job_name))
        return unresolved

    def _validate_job_pools(self, jobs: Iterable[Job]) -> None:
        declared = set(self.admission_policy.pools.keys())
        for job in jobs:
            if job.pool not in declared:
                raise ValueError(
                    f"Unknown pool {job.pool!r} on job {job.name!r}. "
                    f"Declare it in admission_policy.pools first."
                )

    def _result(
        self,
        *,
        job_name: str,
        run_key: str,
        action: JobAction,
        detail: str | None = None,
        pool: str,
    ) -> JobTickResult:
        return JobTickResult(
            job_name=job_name,
            run_key=run_key,
            pool=pool,
            action=action,
            detail=detail,
        )

    def _validate_dependencies(self) -> None:
        unresolved = self._unresolved_dependencies()
        if not unresolved:
            return

        if self.strict_dependencies:
            detail = ", ".join(f"{job}->{dep}" for job, dep in unresolved)
            raise ValueError(
                f"Unresolved dependencies in orchestrator job graph: {detail}"
            )

        for job_name, dep_name in unresolved:
            logger.warning(
                "Job %r depends on %r, which is not registered in this "
                "orchestrator. If this is a cross-orchestrator dependency "
                "this warning can be ignored.",
                job_name,
                dep_name,
            )

    def _refresh_job_graph_if_dirty(self) -> None:
        if not self._jobs_dirty:
            return
        self._rebuild_job_index()
        self._validate_dependencies()
        self._jobs_dirty = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def plan_backfill(self, start: datetime, end: datetime) -> list[str]:
        """Return orchestrator run keys for an inclusive backfill range."""
        if end < start:
            raise ValueError("end must be >= start")

        if not isinstance(self.orchestrator_cadence, DateCadence):
            raise ValueError(
                "Backfill planning requires a date-based orchestrator cadence"
            )

        keys: list[str] = []
        for ref in self._iter_backfill_reference_times(start, end):
            keys.append(resolve_orchestrator_run_key(self.orchestrator_cadence, ref))
        return sorted(set(keys))

    def enqueue_backfill(
        self,
        *,
        start: datetime,
        end: datetime,
        priority: int = 0,
        submitted_by: str | None = None,
        reason: str | None = None,
        force: bool = False,
    ) -> list[OrchestratorRun]:
        """Create pending backfill runs for an inclusive range."""
        run_keys = self.plan_backfill(start=start, end=end)
        return self._enqueue_run_keys(
            run_keys=run_keys,
            mode=OrchestratorRunMode.BACKFILL,
            priority=priority,
            submitted_by=submitted_by,
            reason=reason,
            force=force,
        )

    def enqueue_replay(
        self,
        *,
        run_keys: list[str],
        priority: int = 0,
        submitted_by: str | None = None,
        reason: str | None = None,
        force: bool = False,
    ) -> list[OrchestratorRun]:
        """Create pending replay runs for explicit orchestrator run keys."""
        if not run_keys:
            raise ValueError("run_keys cannot be empty")
        return self._enqueue_run_keys(
            run_keys=run_keys,
            mode=OrchestratorRunMode.REPLAY,
            priority=priority,
            submitted_by=submitted_by,
            reason=reason,
            force=force,
        )

    def list_runs(
        self,
        *,
        status: OrchestratorRunStatus | None = None,
        mode: OrchestratorRunMode | None = None,
    ) -> list[OrchestratorRun]:
        """List orchestrator runs for this orchestrator."""
        return self.state.list_orchestrator_runs(
            status=status,
            mode=mode,
        )

    def show_run(self, orchestrator_run_id: int) -> OrchestratorRun:
        """Get a single run by ID, scoped to this orchestrator."""
        record = self.state.get_orchestrator_run(orchestrator_run_id)
        if record is None or record.namespace != self.namespace:
            raise ValueError(
                f"Run {orchestrator_run_id} not found for orchestrator {self.namespace!r}"
            )
        return record

    def resume_run(
        self, orchestrator_run_id: int, *, reason: str | None = None
    ) -> OrchestratorRun:
        """Resume a blocked/pending/failed run by moving it back to ACTIVE."""
        record = self.show_run(orchestrator_run_id)
        if record.status == OrchestratorRunStatus.CANCELLED:
            raise ValueError("Cancelled runs cannot be resumed")
        updated = record.model_copy(
            update={
                "status": OrchestratorRunStatus.ACTIVE,
                "activated_at": datetime.now(tz=timezone.utc),
                "reason": reason or record.reason,
                "closed_at": None,
            }
        )
        self.state.update_orchestrator_run(updated)
        return updated

    def cancel_run(
        self, orchestrator_run_id: int, *, reason: str | None = None
    ) -> OrchestratorRun:
        """Cancel a run and mark it terminal."""
        record = self.show_run(orchestrator_run_id)
        updated = record.model_copy(
            update={
                "status": OrchestratorRunStatus.CANCELLED,
                "closed_at": datetime.now(tz=timezone.utc),
                "reason": reason or record.reason,
            }
        )
        self.state.update_orchestrator_run(updated)
        return updated

    def _enqueue_run_keys(
        self,
        *,
        run_keys: list[str],
        mode: OrchestratorRunMode,
        priority: int,
        submitted_by: str | None,
        reason: str | None,
        force: bool,
    ) -> list[OrchestratorRun]:
        created_or_updated: list[OrchestratorRun] = []
        now = datetime.now(tz=timezone.utc)

        for run_key in run_keys:
            existing = self.state.get_orchestrator_run_by_key(run_key)
            if existing is not None:
                if not force:
                    raise ValueError(
                        f"Run already exists for ({self.namespace}, {run_key}). Use force=True to requeue."
                    )
                updated = existing.model_copy(
                    update={
                        "status": OrchestratorRunStatus.PENDING,
                        "mode": mode,
                        "priority": priority,
                        "submitted_by": submitted_by,
                        "reason": reason or existing.reason,
                        "force": True,
                        "closed_at": None,
                    }
                )
                self.state.update_orchestrator_run(updated)
                created_or_updated.append(updated)
                continue

            record = OrchestratorRun(
                namespace=self.namespace,
                run_key=run_key,
                status=OrchestratorRunStatus.PENDING,
                mode=mode,
                priority=priority,
                submitted_by=submitted_by,
                reason=reason,
                force=force,
                opened_at=now,
            )
            record = self.state.append_orchestrator_run(record)
            created_or_updated.append(record)

        return created_or_updated

    def _iter_backfill_reference_times(
        self, start: datetime, end: datetime
    ) -> list[datetime]:
        """Iterate inclusive reference times by orchestrator cadence frequency."""
        from dispatchio.cadence import DateCadence, Frequency

        cadence = self.orchestrator_cadence
        if not isinstance(cadence, DateCadence):
            raise ValueError(
                "Backfill planning requires a date-based orchestrator cadence"
            )

        current = start
        points: list[datetime] = []
        while current <= end:
            points.append(current)
            if cadence.frequency == Frequency.HOURLY:
                current = current + timedelta(hours=1)
            elif cadence.frequency == Frequency.DAILY:
                current = current + timedelta(days=1)
            elif cadence.frequency == Frequency.WEEKLY:
                current = current + timedelta(weeks=1)
            elif cadence.frequency == Frequency.MONTHLY:
                month = current.month + 1
                year = current.year + (month - 1) // 12
                month = ((month - 1) % 12) + 1
                max_day = calendar.monthrange(year, month)[1]
                current = current.replace(
                    year=year, month=month, day=min(current.day, max_day)
                )
            else:  # pragma: no cover
                raise ValueError(f"Unsupported orchestrator cadence: {cadence!r}")
        return points

    def tick(
        self,
        reference_time: datetime | None = None,
        *,
        dry_run: bool = False,
        pool: str | None = None,
    ) -> TickResult:
        """
        Run one evaluation cycle.

        reference_time: the logical "now" for this tick. Defaults to
        datetime.now(tz=timezone.utc). Pass an explicit value to replay
        a specific point in time (useful for testing and backfill).

        dry_run: when True, only the planning phase (phase 3) runs — no
        status events are drained, no lost-job detection, and nothing
        is submitted. Returns a TickResult with WOULD_SUBMIT actions so
        you can see what would have been submitted without touching state.
        """
        if reference_time is None:
            reference_time = datetime.now(tz=timezone.utc)

        self._refresh_job_graph_if_dirty()
        if pool is not None and pool not in self.admission_policy.pools:
            raise ValueError(
                f"Unknown pool {pool!r}. Declared pools: {sorted(self.admission_policy.pools.keys())}"
            )

        orchestrator_run_key = self._resolve_tick_orchestrator_run_key(
            reference_time=reference_time,
            dry_run=dry_run,
        )

        tick_start = time.monotonic()
        result = TickResult(reference_time=reference_time)

        # Phase 1 — apply inbound status events (skipped in dry-run)
        if not dry_run:
            self._drain_receiver(reference_time)

        # Phase 2 — detect lost jobs (skipped in dry-run: would write state)
        if not dry_run:
            result.results.extend(
                self._check_lost_jobs(
                    reference_time, orchestrator_run_key=orchestrator_run_key
                )
            )

        # Phase 3 — evaluate each job (planning only; no state writes)
        # outcomes preserves job-list order so results are reported in order.
        outcomes: list[JobTickResult | _PendingSubmission | None] = []

        # Determine if this tick is processing a backfill run (for RunContext)
        orch_run = self.state.get_orchestrator_run_by_key(orchestrator_run_key)
        is_backfill = (
            orch_run is not None and orch_run.mode == OrchestratorRunMode.BACKFILL
        )

        for job in self.jobs:
            if pool is not None and job.pool != pool:
                continue
            effective_cadence = job.cadence or self.default_cadence
            run_key = self._resolve_run_key_for_tick(
                effective_cadence=effective_cadence,
                reference_time=reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )

            if job.runs is not None:
                outcomes.extend(
                    self._evaluate_runs(
                        job,
                        run_key,
                        effective_cadence,
                        reference_time,
                        orchestrator_run_key=orchestrator_run_key,
                        is_backfill=is_backfill,
                    )
                )
            else:
                outcomes.append(
                    self._evaluate_job(
                        job,
                        run_key,
                        effective_cadence,
                        reference_time,
                        orchestrator_run_key=orchestrator_run_key,
                    )
                )

        # Phase 4 — admission planning (active and per-tick limits)
        pending_indices = [
            i for i, o in enumerate(outcomes) if isinstance(o, _PendingSubmission)
        ]
        candidates: list[AdmissionCandidate] = []
        for idx in pending_indices:
            pending_item = outcomes[idx]
            assert isinstance(pending_item, _PendingSubmission)
            candidates.append(
                AdmissionCandidate(
                    index=idx,
                    job_name=pending_item.job.name,
                    pool=pending_item.job.pool,
                    priority=pending_item.job.priority,
                    definition_order=idx,
                )
            )

        active_attempts = self.state.list_attempts()
        job_pool_by_name = {job.name: job.pool for job in self.jobs}
        admission_plan = build_admission_plan(
            candidates=candidates,
            active_attempts=active_attempts,
            job_pool_by_name=job_pool_by_name,
            policy=self.admission_policy,
        )

        if dry_run:
            # Convert pending submissions to WOULD_SUBMIT results; don't execute
            for i in admission_plan.admitted_indices:
                ps = outcomes[i]
                assert isinstance(ps, _PendingSubmission)
                outcomes[i] = self._result(
                    job_name=ps.job.name,
                    run_key=ps.run_key,
                    pool=ps.job.pool,
                    action=JobAction.WOULD_SUBMIT,
                )
            for idx, deferred in admission_plan.deferred_by_index.items():
                ps = outcomes[idx]
                assert isinstance(ps, _PendingSubmission)
                outcomes[idx] = self._result(
                    job_name=ps.job.name,
                    run_key=ps.run_key,
                    pool=ps.job.pool,
                    action=JobAction.WOULD_DEFER,
                    detail=f"{deferred.action.value} {deferred.detail}",
                )
        else:
            # Phase 5 — execute submissions concurrently; slot results back
            #            into their original positions to preserve job-list order.
            pending = [
                outcomes[i] for i in admission_plan.admitted_indices
            ]  # all _PendingSubmission
            submit_results = self._execute_submissions(pending, reference_time)
            for idx, tick_result in zip(
                admission_plan.admitted_indices, submit_results
            ):
                outcomes[idx] = tick_result
            for idx, deferred in admission_plan.deferred_by_index.items():
                ps = outcomes[idx]
                assert isinstance(ps, _PendingSubmission)
                outcomes[idx] = self._result(
                    job_name=ps.job.name,
                    run_key=ps.run_key,
                    pool=ps.job.pool,
                    action=deferred.action,
                    detail=deferred.detail,
                )

        for outcome in outcomes:
            if isinstance(outcome, JobTickResult):
                result.results.append(outcome)
            # None  → silently skipped (e.g. SKIPPED status)
            # _PendingSubmission still in outcomes → should not happen

        self._has_ticked = True

        if self.tick_log is not None:
            try:
                self.tick_log.append(
                    TickLogRecord(
                        ticked_at=datetime.now(tz=timezone.utc).isoformat(),
                        reference_time=reference_time.isoformat(),
                        duration_seconds=round(time.monotonic() - tick_start, 3),
                        actions=[
                            {
                                "job_name": r.job_name,
                                "run_key": r.run_key,
                                "action": r.action.value,
                                "detail": r.detail,
                            }
                            for r in result.results
                        ],
                    )
                )
            except Exception as exc:
                logger.warning("Failed to write tick log entry: %s", exc)

        return result

    def _resolve_tick_orchestrator_run_key(
        self,
        *,
        reference_time: datetime,
        dry_run: bool,
    ) -> str:
        """
        Select the orchestrator run key for this tick.

        Selection order:
          1. Existing ACTIVE run
          2. Highest-priority PENDING run (promoted to ACTIVE unless dry_run)
          3. Derived scheduled run key from orchestrator_cadence
             (persisted as ACTIVE scheduled run unless dry_run)
        """
        active_runs = self.state.list_orchestrator_runs(
            status=OrchestratorRunStatus.ACTIVE,
        )
        if active_runs:
            selected = sorted(
                active_runs,
                key=lambda r: (-r.priority, r.activated_at or r.opened_at),
            )[0]
            return selected.run_key

        pending_runs = self.state.list_orchestrator_runs(
            status=OrchestratorRunStatus.PENDING,
        )
        if pending_runs:
            selected = sorted(
                pending_runs,
                key=lambda r: (-r.priority, r.opened_at),
            )[0]
            if not dry_run:
                activated = selected.model_copy(
                    update={
                        "status": OrchestratorRunStatus.ACTIVE,
                        "activated_at": reference_time,
                    }
                )
                self.state.update_orchestrator_run(activated)
            return selected.run_key

        scheduled_run_key = resolve_orchestrator_run_key(
            orchestrator_cadence=self.orchestrator_cadence,
            reference_time=reference_time,
        )

        existing = self.state.get_orchestrator_run_by_key(scheduled_run_key)
        if existing is not None:
            if existing.status == OrchestratorRunStatus.PENDING and not dry_run:
                activated = existing.model_copy(
                    update={
                        "status": OrchestratorRunStatus.ACTIVE,
                        "activated_at": reference_time,
                    }
                )
                self.state.update_orchestrator_run(activated)
            return existing.run_key

        if not dry_run:
            self.state.append_orchestrator_run(
                OrchestratorRun(
                    namespace=self.namespace,
                    run_key=scheduled_run_key,
                    status=OrchestratorRunStatus.ACTIVE,
                    mode=OrchestratorRunMode.SCHEDULED,
                    opened_at=reference_time,
                    activated_at=reference_time,
                )
            )
        return scheduled_run_key

    def _resolve_run_key_for_tick(
        self,
        *,
        effective_cadence: Cadence,
        reference_time: datetime,
        orchestrator_run_key: str,
    ) -> str:
        """Resolve a run key from the selected orchestrator run key."""
        return resolve_run_key_from_orchestrator(
            orchestrator_run_key=orchestrator_run_key,
            job_cadence=effective_cadence,
        )

    # ------------------------------------------------------------------
    # Phase 1 — drain status events
    # ------------------------------------------------------------------

    def _drain_receiver(self, reference_time: datetime) -> None:
        if self.receiver is None:
            return
        events: list[StatusEvent] = []
        try:
            events = self.receiver.drain()
        except Exception as exc:
            logger.error("Error draining receiver: %s", exc)
            return
        logger.info("Drained %d status events from receiver", len(events))

        for event in events:
            self._apply_status_event(event, reference_time)

    def _apply_status_event(self, event: StatusEvent, reference_time: datetime) -> None:
        """
        Apply a status event, routing through strict or legacy correlation.

        When correlation_id is present, validates
        full identity (job_name, run_key, attempt) before applying update.
        Mismatches are dead-lettered.
        """
        now = event.occurred_at or reference_time

        # Ignore RUNNING events (no longer used for liveness detection)
        if event.status == Status.RUNNING:
            return

        # Require run_key
        try:
            correlation_id = event.correlation_id
        except ValueError as exc:
            self._dead_letter_status(
                event,
                now,
                DeadLetterReasonCode.VALIDATION_FAILURE,
                f"Missing correlation_id: {exc}",
            )
            return

        if correlation_id is None:
            self._dead_letter_status(
                event,
                now,
                DeadLetterReasonCode.VALIDATION_FAILURE,
                "Missing correlation_id",
            )
            return

        # Validate correlation_id matches an existing attempt record
        existing = self.state.get_attempt(correlation_id)
        if existing is None:
            self._dead_letter_status(
                event,
                now,
                DeadLetterReasonCode.CORRELATION_FAILURE,
                f"Unknown correlation_id {correlation_id}",
            )
            return

        # Validations passed — apply update
        record = existing.model_copy(
            update={
                "status": event.status,
                "completed_at": now,
                "reason": event.reason or existing.reason,
                "status_event_trace": event.trace or None,
            }
        )
        self.state.update_attempt(record)

        logger.info(
            "Applied status event (strict): %s/%s/%d → %s (correlation_id=%s)",
            existing.job_name,
            existing.run_key,
            existing.attempt,
            event.status.value,
            correlation_id,
        )

        job = self._job_index.get(existing.job_name)
        if job:
            self._check_terminal_alerts(job, record)

    def _dead_letter_status(
        self,
        event: StatusEvent,
        now: datetime,
        reason_code: DeadLetterReasonCode,
        reason_detail: str,
    ) -> None:
        """
        Route a status event to dead-letter.

        Stores dead-letter record with full audit trail, logs warning,
        and triggers alert for operator visibility.
        """

        # TODO: Maybe we want to also send in the JOB_NAME for context.
        # job = self._job_index.get(event.job_name)
        # source_backend = (
        #     _EXECUTOR_TYPE_TO_SOURCE_BACKEND.get(
        #        job.executor.type, DeadLetterSourceBackend.OTHER
        #     )
        #     if job is not None
        #     else DeadLetterSourceBackend.OTHER
        # )

        dead_letter = DeadLetter(
            occurred_at=now,
            source_backend=DeadLetterSourceBackend.OTHER,
            reason_code=reason_code,
            reason_detail=reason_detail,
            status=DeadLetterStatus.OPEN,
            namespace=self.namespace,
            correlation_id=event.correlation_id,
            raw_payload=event.model_dump(mode="json"),
        )
        self.state.append_dead_letter(dead_letter)

        logger.warning(
            "Dead-lettered status event: %s reason=%s detail=%s",
            f"{event.correlation_id}",
            reason_code.value,
            reason_detail,
        )

    # ------------------------------------------------------------------
    # Phase 2 — detect lost jobs
    # ------------------------------------------------------------------

    def _check_lost_jobs(
        self, reference_time: datetime, *, orchestrator_run_key: str
    ) -> list[JobTickResult]:
        results = []
        for job in self.jobs:
            effective_cadence = job.cadence or self.default_cadence
            run_key = self._resolve_run_key_for_tick(
                effective_cadence=effective_cadence,
                reference_time=reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )
            record = self.state.get_latest_attempt(job.name, run_key)
            if record is None or not record.is_active():
                continue

            executor = self.executors.get(job.executor.type)

            # Poke-based liveness check — preferred over heartbeat for local executors.
            # Executors opt in by implementing the Pokeable protocol.
            if executor is not None and isinstance(executor, Pokeable):
                poke_status = executor.poke(record)
                if poke_status is not None and poke_status != Status.RUNNING:
                    detail = f"poke: process exited with status={poke_status.value}"
                    update: dict = {
                        "status": poke_status,
                        "completed_at": reference_time,
                    }
                    if poke_status == Status.ERROR:
                        update["reason"] = (
                            "process exited without posting a completion event"
                        )
                    updated = record.model_copy(update=update)
                    self.state.update_attempt(updated)
                    logger.warning(
                        "Job %s/%s %s via poke.",
                        job.name,
                        run_key,
                        poke_status.value,
                    )
                    if poke_status == Status.ERROR:
                        self._emit_alert(AlertOn.ERROR, job, run_key, detail, updated)
                        results.append(
                            self._result(
                                job_name=job.name,
                                run_key=run_key,
                                pool=job.pool,
                                action=JobAction.MARKED_ERROR,
                                detail=detail,
                            )
                        )
                    # DONE via poke is a success — no result entry needed
                    continue
                # poke returned RUNNING or None — executor cannot determine status
        return results

    # ------------------------------------------------------------------
    # Phase 3 — expand and evaluate jobs with a runs callable
    # ------------------------------------------------------------------

    def _evaluate_runs(
        self,
        job: Job,
        base_run_key: str,
        cadence: Cadence,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
        is_backfill: bool,
    ) -> list[JobTickResult | _PendingSubmission | None]:
        from dispatchio.date_context import DateContext
        from dispatchio.runs import RunContext

        dates_cfg = getattr(self.settings, "dates", None)
        week_start_day = getattr(dates_cfg, "week_start_day", 0) if dates_cfg else 0
        quarter_start_month = (
            getattr(dates_cfg, "quarter_start_month", 1) if dates_cfg else 1
        )

        rc = RunContext(
            reference_time=reference_time,
            dates=DateContext(
                reference_time,
                week_start_day=week_start_day,
                quarter_start_month=quarter_start_month,
            ),
            is_backfill=is_backfill,
            job_name=job.name,
        )

        try:
            specs = job.runs(rc)
        except Exception as exc:
            logger.error("runs() callable failed for %s: %s", job.name, exc)
            return [
                self._result(
                    job_name=job.name,
                    run_key=base_run_key,
                    pool=job.pool,
                    action=JobAction.SUBMISSION_FAILED,
                    detail=f"runs() error: {exc}",
                )
            ]

        results: list[JobTickResult | _PendingSubmission | None] = []
        for spec in specs:
            if spec.run_key is not None:
                effective_run_key = spec.run_key
            elif spec.variant is not None:
                effective_run_key = f"{base_run_key}:{spec.variant}"
            else:
                effective_run_key = base_run_key

            outcome = self._evaluate_job(
                job,
                effective_run_key,
                cadence,
                reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )
            if isinstance(outcome, _PendingSubmission) and spec.params:
                outcome.params = dict(spec.params)
            results.append(outcome)

        return results

    # ------------------------------------------------------------------
    # Phase 3 — evaluate a single job (planning; returns intent, not action)
    # ------------------------------------------------------------------

    def _evaluate_job(
        self,
        job: Job,
        run_key: str,
        cadence: Cadence,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> JobTickResult | _PendingSubmission | None:
        existing = self.state.get_latest_attempt(job.name, run_key)

        if existing and existing.is_active():
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_ALREADY_ACTIVE,
            )

        if existing and existing.status == Status.DONE:
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_ALREADY_DONE,
            )

        if existing and existing.status in (Status.ERROR, Status.LOST):
            return self._plan_retry(
                job,
                existing,
                run_key,
                cadence,
                reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )

        # Condition gate
        if job.condition is not None:
            if not job.condition.is_met(reference_time, cadence):
                return self._result(
                    job_name=job.name,
                    run_key=run_key,
                    pool=job.pool,
                    action=JobAction.SKIPPED_CONDITION,
                    detail=f"condition not met: {type(job.condition).__name__}",
                )

        # Dependency check
        dep_result = self._check_dependencies(
            job,
            run_key,
            reference_time,
            orchestrator_run_key=orchestrator_run_key,
        )
        if dep_result is not None:
            return dep_result

        # NOT_STARTED_BY alert check (before submitting)
        self._check_not_started_by_alert(
            job, run_key, reference_time, existing, cadence
        )

        return _PendingSubmission(
            job=job,
            run_key=run_key,
            attempt=0,
            trigger_type=TriggerType.SCHEDULED,
        )

    # ------------------------------------------------------------------
    # Retry planning (part of phase 3)
    # ------------------------------------------------------------------

    def _plan_retry(
        self,
        job: Job,
        record: Attempt,
        run_key: str,
        cadence: Cadence,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> JobTickResult | _PendingSubmission:
        policy = job.retry_policy
        next_attempt = record.attempt + 1

        if next_attempt >= policy.max_attempts:
            detail = (
                f"max_attempts={policy.max_attempts} reached. "
                f"Last reason: {record.reason}"
            )
            self._emit_alert(AlertOn.ERROR, job, run_key, detail, record)
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.MARKED_ERROR,
                detail=detail,
            )

        if policy.retry_on and record.reason:
            if not any(pat in record.reason for pat in policy.retry_on):
                detail = f"reason does not match retry_on patterns: {policy.retry_on}"
                self._emit_alert(AlertOn.ERROR, job, run_key, detail, record)
                return self._result(
                    job_name=job.name,
                    run_key=run_key,
                    pool=job.pool,
                    action=JobAction.MARKED_ERROR,
                    detail=detail,
                )

        if job.condition and not job.condition.is_met(reference_time, cadence):
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_CONDITION,
                detail=f"retry: condition not met: {type(job.condition).__name__}",
            )

        dep_result = self._check_dependencies(
            job,
            run_key,
            reference_time,
            orchestrator_run_key=orchestrator_run_key,
        )
        if dep_result is not None:
            return dep_result

        return _PendingSubmission(
            job=job,
            run_key=run_key,
            attempt=next_attempt,
            trigger_type=TriggerType.AUTO_RETRY,
            trigger_reason="auto_retry_policy",
        )

    # ------------------------------------------------------------------
    # Phase 5 — concurrent submission
    # ------------------------------------------------------------------

    def _execute_submissions(
        self,
        pending: list[_PendingSubmission],
        reference_time: datetime,
    ) -> list[JobTickResult]:
        if not pending:
            return []

        # Pre-allocate result slots so we can write back by index from threads,
        # preserving the input order in the returned list.
        results: list[JobTickResult | None] = [None] * len(pending)

        with ThreadPoolExecutor(
            max_workers=min(self.submit_workers, len(pending))
        ) as pool:
            future_to_idx = {
                pool.submit(
                    self._submit,
                    ps.job,
                    ps.run_key,
                    reference_time,
                    ps.attempt,
                    ps.trigger_type,
                    ps.trigger_reason,
                    params=ps.params,
                ): i
                for i, ps in enumerate(pending)
            }
            for future in as_completed(future_to_idx):
                idx = future_to_idx[future]
                try:
                    results[idx] = future.result()
                except Exception as exc:
                    # _submit() catches its own exceptions; this is a safety net.
                    ps = pending[idx]
                    logger.error(
                        "Unexpected error in submission thread for %s/%s: %s",
                        ps.job.name,
                        ps.run_key,
                        exc,
                    )
                    results[idx] = JobTickResult(
                        job_name=ps.job.name,
                        run_key=ps.run_key,
                        pool=ps.job.pool,
                        action=JobAction.SUBMISSION_FAILED,
                        detail=str(exc),
                    )

        return [r for r in results if r is not None]

    # ------------------------------------------------------------------
    # Submission (called from thread pool in phase 5)
    # ------------------------------------------------------------------

    def _submit(
        self,
        job: Job,
        run_key: str,
        reference_time: datetime,
        attempt: int,
        trigger_type: TriggerType = TriggerType.SCHEDULED,
        trigger_reason: str | None = None,
        requested_by: str | None = None,
        params: dict[str, str] | None = None,
    ) -> JobTickResult:
        executor_type = job.executor.type
        executor = self.executors.get(executor_type)
        if executor is None:
            msg = f"No executor registered for type '{executor_type}'"
            logger.error(msg)
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SUBMISSION_FAILED,
                detail=msg,
            )

        now = reference_time
        correlation_id = uuid4()
        record = Attempt(
            namespace=self.namespace,
            job_name=job.name,
            run_key=run_key,
            attempt=attempt,
            correlation_id=correlation_id,
            status=Status.SUBMITTED,
            submitted_at=now,
            trigger_type=trigger_type,
            trigger_reason=trigger_reason,
            requested_by=requested_by,
            params=params or {},
            trace={},
        )
        self.state.append_attempt(record)

        try:
            executor.submit(job, record, reference_time, timeout=self.submit_timeout)

            # Optionally retrieve executor reference if executor implements it
            if hasattr(executor, "get_executor_reference"):
                ref = executor.get_executor_reference(record.correlation_id)
                if ref is not None:
                    record = record.model_copy(update={"trace": {"executor": ref}})
                    self.state.update_attempt(record)

            logger.info("Submitted %s/%s (attempt %d)", job.name, run_key, attempt)
            action = JobAction.RETRYING if attempt > 0 else JobAction.SUBMITTED
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=action,
                detail=f"attempt={attempt}",
            )
        except Exception as exc:
            error_msg = str(exc)
            logger.error(
                "Submission failed for %s/%s: %s", job.name, run_key, error_msg
            )
            failed_record = record.model_copy(
                update={
                    "status": Status.ERROR,
                    "reason": f"submission_failed: {error_msg}",
                    "completed_at": now,
                }
            )
            self.state.update_attempt(failed_record)
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SUBMISSION_FAILED,
                detail=error_msg,
            )

    # ------------------------------------------------------------------
    # Dependency resolution
    # ------------------------------------------------------------------

    def _dep_record(
        self,
        dep: Dependency,
        job: Job,
        reference_time: datetime,
        orchestrator_run_key: str,
    ):
        """Fetch the relevant record for a dependency, based on its type and cadence."""
        cadence = dep.cadence or job.cadence or self.default_cadence
        run_key = self._resolve_run_key_for_tick(
            effective_cadence=cadence,
            reference_time=reference_time,
            orchestrator_run_key=orchestrator_run_key,
        )
        if isinstance(dep, EventDependency):
            record = self.state.get_event(dep.event_name, run_key)
        else:
            record = self.state.get_latest_attempt(dep.job_name, run_key)
        return record

    def _unmet_dependencies(
        self, job: Job, reference_time: datetime, *, orchestrator_run_key: str
    ) -> list[Dependency]:
        unmet = []
        for dep in job.depends_on:
            record = self._dep_record(dep, job, reference_time, orchestrator_run_key)
            if record is None or record.status != dep.required_status:
                unmet.append(dep)
        return unmet

    def _dep_is_finished(
        self,
        dep: Dependency,
        job: Job,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> bool:
        record = self._dep_record(dep, job, reference_time, orchestrator_run_key)
        return record is not None and record.is_finished()

    def _dep_status_matches(
        self,
        dep: Dependency,
        job: Job,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> bool:
        record = self._dep_record(dep, job, reference_time, orchestrator_run_key)
        return record is not None and record.status == dep.required_status

    def _format_dependency_run_ref(
        self,
        dep: Dependency,
        job: Job,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> str:
        cadence = dep.cadence or job.cadence or self.default_cadence
        run_key = self._resolve_run_key_for_tick(
            effective_cadence=cadence,
            reference_time=reference_time,
            orchestrator_run_key=orchestrator_run_key,
        )
        dep_name = dep.event_name if isinstance(dep, EventDependency) else dep.job_name
        return f"{dep_name}[{run_key}]"

    def _check_dependencies(
        self,
        job: Job,
        run_key: str,
        reference_time: datetime,
        *,
        orchestrator_run_key: str,
    ) -> JobTickResult | None:
        """
        Returns a blocking JobTickResult if deps are not satisfied, or None if clear to proceed.
        """
        if not job.depends_on:
            return None

        mode = job.dependency_mode

        if mode == DependencyMode.ALL_SUCCESS:
            unmet = self._unmet_dependencies(
                job,
                reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )
            if not unmet:
                return None

            # Deps that have finished in a state that can't satisfy required_status.
            # These need operator attention; still-waiting deps may resolve on their own.
            upstream_finished = [
                (dep, self._dep_record(dep, job, reference_time, orchestrator_run_key))
                for dep in unmet
                if self._dep_is_finished(
                    dep, job, reference_time, orchestrator_run_key=orchestrator_run_key
                )
            ]
            if upstream_finished:
                detail = ", ".join(
                    f"{self._format_dependency_run_ref(d, job, reference_time, orchestrator_run_key=orchestrator_run_key)}"
                    f" is {r.status.value} (required: {d.required_status.value})"
                    for d, r in upstream_finished
                )
                return self._result(
                    job_name=job.name,
                    run_key=run_key,
                    pool=job.pool,
                    action=JobAction.SKIPPED_UPSTREAM,
                    detail=detail,
                )

            detail = ", ".join(
                self._format_dependency_run_ref(
                    d,
                    job,
                    reference_time,
                    orchestrator_run_key=orchestrator_run_key,
                )
                + f"!={d.required_status.value}"
                for d in unmet
            )
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_DEPENDENCIES,
                detail=detail,
            )

        if mode == DependencyMode.ALL_FINISHED:
            unfinished = [
                dep
                for dep in job.depends_on
                if not self._dep_is_finished(
                    dep,
                    job,
                    reference_time,
                    orchestrator_run_key=orchestrator_run_key,
                )
            ]
            if not unfinished:
                return None
            detail = ", ".join(
                f"{self._format_dependency_run_ref(d, job, reference_time, orchestrator_run_key=orchestrator_run_key)} not finished"
                for d in unfinished
            )
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_DEPENDENCIES,
                detail=detail,
            )

        # THRESHOLD mode
        threshold = job.dependency_threshold  # validated non-None by Job validator
        deps = job.depends_on
        met = [
            dep
            for dep in deps
            if self._dep_status_matches(
                dep,
                job,
                reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )
        ]

        if len(met) >= threshold:
            return None  # satisfied

        not_yet_finished = [
            dep
            for dep in deps
            if not self._dep_is_finished(
                dep,
                job,
                reference_time,
                orchestrator_run_key=orchestrator_run_key,
            )
        ]
        if len(met) + len(not_yet_finished) < threshold:
            detail = (
                f"threshold={threshold} unreachable: "
                f"{len(met)}/{len(deps)} succeeded, {len(not_yet_finished)} still running"
            )
            return self._result(
                job_name=job.name,
                run_key=run_key,
                pool=job.pool,
                action=JobAction.SKIPPED_THRESHOLD_UNREACHABLE,
                detail=detail,
            )

        detail = f"threshold={threshold}: {len(met)}/{len(deps)} succeeded"
        return self._result(
            job_name=job.name,
            run_key=run_key,
            pool=job.pool,
            action=JobAction.SKIPPED_DEPENDENCIES,
            detail=detail,
        )

    # ------------------------------------------------------------------
    # Alert helpers
    # ------------------------------------------------------------------

    def _check_terminal_alerts(self, job: Job, record: Attempt) -> None:
        for cond in job.alerts:
            if cond.on == AlertOn.ERROR and record.status == Status.ERROR:
                self._emit_alert(
                    AlertOn.ERROR,
                    job,
                    record.run_key,
                    record.reason,
                    record,
                    cond.channels,
                )
            elif cond.on == AlertOn.SUCCESS and record.status == Status.DONE:
                self._emit_alert(
                    AlertOn.SUCCESS,
                    job,
                    record.run_key,
                    "Job completed successfully",
                    record,
                    cond.channels,
                )

    def _check_not_started_by_alert(
        self,
        job: Job,
        run_key: str,
        reference_time: datetime,
        existing: Attempt | None,
        cadence: Cadence,
    ) -> None:
        if existing is not None:
            return
        for cond in job.alerts:
            if cond.on != AlertOn.NOT_STARTED_BY or cond.not_by is None:
                continue
            gate = TimeOfDayCondition(after=cond.not_by)
            if gate.is_met(reference_time, cadence):
                self._emit_alert(
                    AlertOn.NOT_STARTED_BY,
                    job,
                    run_key,
                    f"Job not started by {cond.not_by}",
                    None,
                    cond.channels,
                )

    def _emit_alert(
        self,
        alert_on: AlertOn,
        job: Job,
        run_key: str,
        detail: str | None,
        record: Attempt | None,
        channels: list[str] | None = None,
    ) -> None:
        if channels is None:
            channels = next((c.channels for c in job.alerts if c.on == alert_on), [])
        event = AlertEvent(
            alert_on=alert_on,
            job_name=job.name,
            run_key=run_key,
            channels=channels or [],
            detail=detail,
            occurred_at=datetime.now(tz=timezone.utc),
            record=record,
        )
        try:
            self.alert_handler.handle(event)
        except Exception as exc:
            logger.error("Alert handler raised: %s", exc)

    def manual_retry(
        self,
        job_name: str,
        run_key: str,
        requested_by: str,
        request_reason: str,
    ) -> Attempt:
        """
        Create a new manual retry attempt for a job.

        The latest attempt must be in a terminal state (DONE, ERROR, LOST, CANCELLED).
        Creates a new attempt with status SUBMITTED, trigger_type=MANUAL_RETRY,
        and operator context (who/why).

        Args:
            job_name: Job name (must be registered in this orchestrator)
            run_key: The logical run identifier
            requested_by: Name/ID of operator(eg user) requesting retry
            request_reason: Explanation for manual retry

        Returns:
            The newly created Attempt (SUBMITTED status)

        Raises:
            ValueError: If job not found, or latest attempt not in terminal state
        """
        # Validate job exists
        job = self._job_index.get(job_name)
        if job is None:
            raise ValueError(f"Job '{job_name}' not found in orchestrator")

        # Get latest attempt
        latest = self.state.get_latest_attempt(job_name, run_key)
        if latest is None:
            raise ValueError(
                f"No existing attempt for {job_name}/{run_key}; "
                "cannot retry non-existent job"
            )

        # Ensure latest is terminal
        if not latest.is_finished():
            raise ValueError(
                f"Cannot retry {job_name}/{run_key} while attempt {latest.attempt} "
                f"is still {latest.status.value} (not terminal)"
            )

        new_attempt_num = latest.attempt + 1
        now = datetime.now(tz=timezone.utc)

        # Submit via the normal execution path so the job is actually dispatched.
        # _submit writes the Attempt (SUBMITTED) then calls executor.submit().
        self._submit(
            job=job,
            run_key=run_key,
            reference_time=now,
            attempt=new_attempt_num,
            trigger_type=TriggerType.MANUAL_RETRY,
            trigger_reason=request_reason,
            requested_by=requested_by,
        )

        new_record = self.state.get_latest_attempt(job_name, run_key)
        if new_record is None or new_record.attempt != new_attempt_num:
            # This should never happen, but sanity check that the new attempt was created.
            raise RuntimeError(
                f"Failed to create new attempt for {job_name}/{run_key} "
                f"during manual retry"
            )

        retry_request = RetryRequest(
            requested_at=now,
            requested_by=requested_by,
            run_key=run_key,
            requested_jobs=[job_name],
            cascade=False,
            reason=request_reason,
            selected_jobs=[job_name],
            assigned_attempt_by_job={job_name: new_attempt_num},
        )
        self.state.append_retry_request(retry_request)

        logger.info(
            "Manual retry submitted: %s/%s attempt %d (operator=%s reason=%s)",
            job_name,
            run_key,
            new_attempt_num,
            requested_by,
            request_reason,
        )

        return new_record

    def manual_cancel(
        self, correlation_id: str, requested_by: str, request_reason: str
    ) -> Attempt:
        """
        Manually cancel a running or queued attempt.

        The attempt must be in an active state (SUBMITTED, QUEUED, RUNNING).
        Updates status to CANCELLED with operator context.

        Args:
            correlation_id: UUID of the attempt to cancel
            requested_by: Name/ID of operator requesting cancellation
            request_reason: Explanation for cancellation

        Returns:
            The updated Attempt (CANCELLED status)

        Raises:
            ValueError: If attempt not found or already terminal
        """
        # Look up attempt by ID
        attempt = self.state.get_attempt(correlation_id)
        if attempt is None:
            raise ValueError(f"Attempt {correlation_id} not found; cannot cancel")

        # Ensure not already finished
        if attempt.is_finished():
            raise ValueError(
                f"Cannot cancel {attempt.job_name}/{attempt.run_key}/{attempt.attempt}; "
                f"already {attempt.status.value}"
            )

        # Update to CANCELLED with operator context
        cancelled_record = attempt.model_copy(
            update={
                "status": Status.CANCELLED,
                "completed_at": datetime.now(tz=timezone.utc),
                "reason": request_reason,
                "requested_by": requested_by,
            }
        )
        self.state.update_attempt(cancelled_record)

        logger.info(
            "Manual cancel: %s/%s/%d (requested_by=%s reason=%s)",
            attempt.job_name,
            attempt.run_key,
            attempt.attempt,
            requested_by,
            request_reason,
        )

        return cancelled_record
