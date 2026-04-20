"""
Orchestrator — the tick engine.

The Orchestrator is stateless between runs. Each tick() call has five phases:

  1. Drain completion events from the receiver and update state.
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

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Iterable
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from dispatchio.datastore.base import DataStore

from dispatchio.admission import AdmissionCandidate, build_admission_plan
from dispatchio.alerts.base import AlertEvent, AlertHandler, LogAlertHandler
from dispatchio.cadence import DAILY, Cadence
from dispatchio.conditions import TimeOfDayCondition
from dispatchio.executor.base import Executor, Pokeable
from dispatchio.models import (
    AlertOn,
    Dependency,
    DependencyMode,
    AdmissionPolicy,
    JobAction,
    Job,
    JobTickResult,
    AttemptRecord,
    RetryRequest,
    Status,
    TickResult,
    TriggerType,
    DeadLetterReasonCode,
    DeadLetterRecord,
    DeadLetterSourceBackend,
    DeadLetterStatus,
)
from dispatchio.receiver.base import CompletionEvent, CompletionReceiver
from dispatchio.run_id import resolve_run_id
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
    logical_run_id: str
    attempt: int
    trigger_type: TriggerType = TriggerType.SCHEDULED
    trigger_reason: str | None = None


class Orchestrator:
    """
    Instantiate once with your job definitions and infrastructure, then
    call tick() on a schedule.

    Args:
        jobs:                   List of Job objects to evaluate each tick.
        state:                  StateStore implementation (memory, filesystem, DynamoDB …).
        executors:              Map of executor-type → Executor.
        receiver:               CompletionReceiver polled at the start of each tick.
        alert_handler:          AlertHandler for ERROR/LOST/NOT_STARTED_BY conditions.
        submit_workers:         Max threads used to submit jobs in parallel within a tick.
        admission_policy:       Admission limits for active jobs and per-tick submissions.
        submit_timeout:         Per-submission deadline (seconds) forwarded to
                                executor.submit(). Not yet enforced by local executors;
                                reserved for cloud executors where the API call can block.
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
        receiver: CompletionReceiver | None = None,
        alert_handler: AlertHandler | None = None,
        submit_workers: int = 8,
        admission_policy: AdmissionPolicy | None = None,
        submit_timeout: float | None = None,
        default_cadence: Cadence = DAILY,
        strict_dependencies: bool = True,
        allow_runtime_mutation: bool = False,
        name: str = "default",
        tick_log: TickLogStore | None = None,
        data_store: DataStore | None = None,
    ) -> None:
        self.name = name
        self.tick_log = tick_log
        self.jobs = jobs
        self.state = state
        self.executors = executors
        self.receiver = receiver
        self.alert_handler = alert_handler or LogAlertHandler()
        self.submit_workers = submit_workers
        self.admission_policy = admission_policy or AdmissionPolicy()
        self.submit_timeout = submit_timeout
        self.default_cadence = default_cadence
        self.strict_dependencies = strict_dependencies
        self.allow_runtime_mutation = allow_runtime_mutation
        self.data_store = data_store

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
                if dep.job_name not in self._job_index:
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
        run_id: str,
        action: JobAction,
        detail: str | None = None,
        pool: str,
    ) -> JobTickResult:
        return JobTickResult(
            job_name=job_name,
            run_id=run_id,
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
        completion events are drained, no lost-job detection, and nothing
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

        tick_start = time.monotonic()
        result = TickResult(reference_time=reference_time)

        # Phase 1 — apply inbound completion events (skipped in dry-run)
        if not dry_run:
            self._drain_receiver(reference_time)

        # Phase 2 — detect lost jobs (skipped in dry-run: would write state)
        if not dry_run:
            result.results.extend(self._check_lost_jobs(reference_time))

        # Phase 3 — evaluate each job (planning only; no state writes)
        # outcomes preserves job-list order so results are reported in order.
        outcomes: list[JobTickResult | _PendingSubmission | None] = []
        for job in self.jobs:
            if pool is not None and job.pool != pool:
                continue
            effective_cadence = job.cadence or self.default_cadence
            logical_run_id = resolve_run_id(effective_cadence, reference_time)
            outcomes.append(
                self._evaluate_job(
                    job, logical_run_id, effective_cadence, reference_time
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
                    run_id=ps.logical_run_id,
                    pool=ps.job.pool,
                    action=JobAction.WOULD_SUBMIT,
                )
            for idx, deferred in admission_plan.deferred_by_index.items():
                ps = outcomes[idx]
                assert isinstance(ps, _PendingSubmission)
                outcomes[idx] = self._result(
                    job_name=ps.job.name,
                    run_id=ps.logical_run_id,
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
                    run_id=ps.logical_run_id,
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
                                "run_id": r.run_id,
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

    # ------------------------------------------------------------------
    # Phase 1 — drain completion events
    # ------------------------------------------------------------------

    def _drain_receiver(self, reference_time: datetime) -> None:
        if self.receiver is None:
            return
        events: list[CompletionEvent] = []
        try:
            events = self.receiver.drain()
        except Exception as exc:
            logger.error("Error draining receiver: %s", exc)
            return

        for event in events:
            self._apply_completion_event(event, reference_time)

    def _apply_completion_event(
        self, event: CompletionEvent, reference_time: datetime
    ) -> None:
        """
        Apply a completion event, routing through strict or legacy correlation.

        Strict mode (Phase 2): when dispatchio_attempt_id is present, validates
        full identity (job_name, logical_run_id, attempt) before applying update.
        Mismatches are dead-lettered.

        Legacy mode: when dispatchio_attempt_id is absent, falls back to latest
        attempt by (job_name, logical_run_id). Used for external events and
        backward compatibility with pre-Phase-2 reporters.
        """
        now = event.occurred_at or reference_time

        # Ignore RUNNING events (no longer used for liveness detection)
        if event.status == Status.RUNNING:
            return

        # Require logical_run_id
        try:
            logical_run_id = event.get_logical_run_id()
        except ValueError as exc:
            self._dead_letter_completion(
                event,
                now,
                DeadLetterReasonCode.VALIDATION_FAILURE,
                f"Missing logical_run_id: {exc}",
            )
            return

        attempt_id = event.dispatchio_attempt_id

        if attempt_id is not None:
            # ---- Phase 2 strict path ----------------------------------------
            attempt = event.attempt

            # Look up attempt by dispatchio_attempt_id (authoritative)
            existing = self.state.get_attempt(attempt_id)

            if existing is None:
                self._dead_letter_completion(
                    event,
                    now,
                    DeadLetterReasonCode.CORRELATION_FAILURE,
                    f"Unknown dispatchio_attempt_id {attempt_id}",
                )
                return

            # Validate identity match (job_name, logical_run_id, attempt)
            if not (
                existing.job_name == event.job_name
                and existing.logical_run_id == logical_run_id
                and (attempt is None or existing.attempt == attempt)
            ):
                self._dead_letter_completion(
                    event,
                    now,
                    DeadLetterReasonCode.CORRELATION_FAILURE,
                    f"Identity mismatch: event=({event.job_name}, {logical_run_id}, {attempt}), "
                    f"stored=({existing.job_name}, {existing.logical_run_id}, {existing.attempt})",
                )
                return

            # Check for duplicate terminal status
            if existing.is_finished():
                if existing.status == event.status:
                    logger.debug(
                        "Idempotent completion event: %s/%s/%d already %s",
                        event.job_name,
                        logical_run_id,
                        existing.attempt,
                        existing.status.value,
                    )
                    return
                self._dead_letter_completion(
                    event,
                    now,
                    DeadLetterReasonCode.CONFLICT_FAILURE,
                    f"Conflicting terminal status: stored={existing.status.value}, "
                    f"event={event.status.value}",
                )
                return

            # All validations passed — apply update
            record = existing.model_copy(
                update={
                    "status": event.status,
                    "completed_at": now,
                    "reason": event.reason or existing.reason,
                    "completion_event_trace": event.trace or None,
                }
            )
            self.state.update_attempt(record)

            logger.info(
                "Applied completion event (strict): %s/%s/%d → %s (attempt_id=%s)",
                event.job_name,
                logical_run_id,
                existing.attempt,
                event.status.value,
                attempt_id,
            )

        else:
            # ---- Legacy / external-event path --------------------------------
            logger.debug(
                "Completion event without dispatchio_attempt_id for %s/%s — using legacy path",
                event.job_name,
                logical_run_id,
            )
            existing = self.state.get_latest_attempt(event.job_name, logical_run_id)

            if existing is None:
                # Auto-create a record for this external/legacy event
                record = AttemptRecord(
                    job_name=event.job_name,
                    logical_run_id=logical_run_id,
                    attempt=0,
                    dispatchio_attempt_id=uuid4(),
                    status=event.status,
                    completed_at=now,
                    reason=event.reason,
                    trace={},
                    completion_event_trace=event.trace or None,
                )
                self.state.append_attempt(record)
            elif existing.is_finished():
                logger.debug(
                    "Ignoring completion for already-finished %s/%s (%s)",
                    event.job_name,
                    logical_run_id,
                    existing.status.value,
                )
                return
            else:
                record = existing.model_copy(
                    update={
                        "status": event.status,
                        "completed_at": now,
                        "reason": event.reason or existing.reason,
                        "completion_event_trace": event.trace or None,
                    }
                )
                self.state.update_attempt(record)

            logger.info(
                "Applied completion event (legacy): %s/%s → %s",
                event.job_name,
                logical_run_id,
                event.status.value,
            )

        job = self._job_index.get(event.job_name)
        if job:
            self._check_terminal_alerts(job, record)

    def _dead_letter_completion(
        self,
        event: CompletionEvent,
        now: datetime,
        reason_code: DeadLetterReasonCode,
        reason_detail: str,
    ) -> None:
        """
        Route a completion event to dead-letter.

        Stores dead-letter record with full audit trail, logs warning,
        and triggers alert for operator visibility.
        """
        job = self._job_index.get(event.job_name)
        source_backend = (
            _EXECUTOR_TYPE_TO_SOURCE_BACKEND.get(
                job.executor.type, DeadLetterSourceBackend.OTHER
            )
            if job is not None
            else DeadLetterSourceBackend.OTHER
        )

        dead_letter = DeadLetterRecord(
            dead_letter_id=uuid4(),
            occurred_at=now,
            source_backend=source_backend,
            reason_code=reason_code,
            reason_detail=reason_detail,
            status=DeadLetterStatus.OPEN,
            job_name=event.job_name,
            logical_run_id=event.logical_run_id,
            attempt=event.attempt,
            dispatchio_attempt_id=event.dispatchio_attempt_id,
            raw_payload=event.model_dump(mode="json"),
        )
        self.state.append_dead_letter(dead_letter)

        logger.warning(
            "Dead-lettered completion event: %s reason=%s detail=%s (id=%s)",
            f"{event.job_name}/{event.logical_run_id}/{event.attempt}",
            reason_code.value,
            reason_detail,
            dead_letter.dead_letter_id,
        )

    # ------------------------------------------------------------------
    # Phase 2 — detect lost jobs
    # ------------------------------------------------------------------

    def _check_lost_jobs(self, reference_time: datetime) -> list[JobTickResult]:
        results = []
        for job in self.jobs:
            effective_cadence = job.cadence or self.default_cadence
            logical_run_id = resolve_run_id(effective_cadence, reference_time)
            record = self.state.get_latest_attempt(job.name, logical_run_id)
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
                        logical_run_id,
                        poke_status.value,
                    )
                    if poke_status == Status.ERROR:
                        self._emit_alert(
                            AlertOn.ERROR, job, logical_run_id, detail, updated
                        )
                        results.append(
                            self._result(
                                job_name=job.name,
                                run_id=logical_run_id,
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
    # Phase 3 — evaluate a single job (planning; returns intent, not action)
    # ------------------------------------------------------------------

    def _evaluate_job(
        self,
        job: Job,
        logical_run_id: str,
        cadence: Cadence,
        reference_time: datetime,
    ) -> JobTickResult | _PendingSubmission | None:
        existing = self.state.get_latest_attempt(job.name, logical_run_id)

        if existing and existing.is_active():
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_ALREADY_ACTIVE,
            )

        if existing and existing.status == Status.DONE:
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_ALREADY_DONE,
            )

        if existing and existing.status in (Status.ERROR, Status.LOST):
            return self._plan_retry(
                job, existing, logical_run_id, cadence, reference_time
            )

        # Condition gate
        if job.condition is not None:
            if not job.condition.is_met(reference_time, cadence):
                return self._result(
                    job_name=job.name,
                    run_id=logical_run_id,
                    pool=job.pool,
                    action=JobAction.SKIPPED_CONDITION,
                    detail=f"condition not met: {type(job.condition).__name__}",
                )

        # Dependency check
        dep_result = self._check_dependencies(job, logical_run_id, reference_time)
        if dep_result is not None:
            return dep_result

        # NOT_STARTED_BY alert check (before submitting)
        self._check_not_started_by_alert(
            job, logical_run_id, reference_time, existing, cadence
        )

        return _PendingSubmission(
            job=job,
            logical_run_id=logical_run_id,
            attempt=0,
            trigger_type=TriggerType.SCHEDULED,
        )

    # ------------------------------------------------------------------
    # Retry planning (part of phase 3)
    # ------------------------------------------------------------------

    def _plan_retry(
        self,
        job: Job,
        record: AttemptRecord,
        logical_run_id: str,
        cadence: Cadence,
        reference_time: datetime,
    ) -> JobTickResult | _PendingSubmission:
        policy = job.retry_policy
        next_attempt = record.attempt + 1

        if next_attempt >= policy.max_attempts:
            detail = (
                f"max_attempts={policy.max_attempts} reached. "
                f"Last error: {record.reason}"
            )
            self._emit_alert(AlertOn.ERROR, job, logical_run_id, detail, record)
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.MARKED_ERROR,
                detail=detail,
            )

        if policy.retry_on and record.reason:
            if not any(pat in record.reason for pat in policy.retry_on):
                detail = f"reason does not match retry_on patterns: {policy.retry_on}"
                self._emit_alert(AlertOn.ERROR, job, logical_run_id, detail, record)
                return self._result(
                    job_name=job.name,
                    run_id=logical_run_id,
                    pool=job.pool,
                    action=JobAction.MARKED_ERROR,
                    detail=detail,
                )

        if job.condition and not job.condition.is_met(reference_time, cadence):
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_CONDITION,
                detail=f"retry: condition not met: {type(job.condition).__name__}",
            )

        dep_result = self._check_dependencies(job, logical_run_id, reference_time)
        if dep_result is not None:
            return dep_result

        return _PendingSubmission(
            job=job,
            logical_run_id=logical_run_id,
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
                    ps.logical_run_id,
                    reference_time,
                    ps.attempt,
                    ps.trigger_type,
                    ps.trigger_reason,
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
                        ps.logical_run_id,
                        exc,
                    )
                    results[idx] = JobTickResult(
                        job_name=ps.job.name,
                        run_id=ps.logical_run_id,
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
        logical_run_id: str,
        reference_time: datetime,
        attempt: int,
        trigger_type: TriggerType = TriggerType.SCHEDULED,
        trigger_reason: str | None = None,
        operator_name: str | None = None,
    ) -> JobTickResult:
        executor_type = job.executor.type
        executor = self.executors.get(executor_type)
        if executor is None:
            msg = f"No executor registered for type '{executor_type}'"
            logger.error(msg)
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SUBMISSION_FAILED,
                detail=msg,
            )

        now = reference_time
        dispatchio_attempt_id = uuid4()
        record = AttemptRecord(
            job_name=job.name,
            logical_run_id=logical_run_id,
            attempt=attempt,
            dispatchio_attempt_id=dispatchio_attempt_id,
            status=Status.SUBMITTED,
            submitted_at=now,
            trigger_type=trigger_type,
            trigger_reason=trigger_reason,
            operator_name=operator_name,
            trace={},
        )
        self.state.append_attempt(record)

        try:
            executor.submit(job, record, reference_time, timeout=self.submit_timeout)

            # Optionally retrieve executor reference if executor implements it
            if hasattr(executor, "get_executor_reference"):
                ref = executor.get_executor_reference(record.dispatchio_attempt_id)
                if ref is not None:
                    record = record.model_copy(update={"trace": {"executor": ref}})
                    self.state.update_attempt(record)

            logger.info(
                "Submitted %s/%s (attempt %d)", job.name, logical_run_id, attempt
            )
            action = JobAction.RETRYING if attempt > 0 else JobAction.SUBMITTED
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=action,
                detail=f"attempt={attempt}",
            )
        except Exception as exc:
            error_msg = str(exc)
            logger.error(
                "Submission failed for %s/%s: %s", job.name, logical_run_id, error_msg
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
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SUBMISSION_FAILED,
                detail=error_msg,
            )

    # ------------------------------------------------------------------
    # Dependency resolution
    # ------------------------------------------------------------------

    def _unmet_dependencies(
        self, job: Job, reference_time: datetime
    ) -> list[Dependency]:
        unmet = []
        for dep in job.depends_on:
            cadence = dep.cadence or job.cadence or self.default_cadence
            dep_logical_run_id = resolve_run_id(cadence, reference_time)
            record = self.state.get_latest_attempt(dep.job_name, dep_logical_run_id)
            if record is None or record.status != dep.required_status:
                unmet.append(dep)
        return unmet

    def _dep_is_finished(
        self, dep: Dependency, job: Job, reference_time: datetime
    ) -> bool:
        cadence = dep.cadence or job.cadence or self.default_cadence
        dep_logical_run_id = resolve_run_id(cadence, reference_time)
        record = self.state.get_latest_attempt(dep.job_name, dep_logical_run_id)
        return record is not None and record.is_finished()

    def _dep_status_matches(
        self, dep: Dependency, job: Job, reference_time: datetime
    ) -> bool:
        cadence = dep.cadence or job.cadence or self.default_cadence
        dep_logical_run_id = resolve_run_id(cadence, reference_time)
        record = self.state.get_latest_attempt(dep.job_name, dep_logical_run_id)
        return record is not None and record.status == dep.required_status

    def _check_dependencies(
        self, job: Job, logical_run_id: str, reference_time: datetime
    ) -> JobTickResult | None:
        """
        Returns a blocking JobTickResult if deps are not satisfied, or None if clear to proceed.
        """
        if not job.depends_on:
            return None

        mode = job.dependency_mode

        if mode == DependencyMode.ALL_SUCCESS:
            unmet = self._unmet_dependencies(job, reference_time)
            if not unmet:
                return None
            detail = ", ".join(
                f"{d.job_name}[{resolve_run_id(d.cadence or job.cadence or self.default_cadence, reference_time)}]"
                f"!={d.required_status.value}"
                for d in unmet
            )
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_DEPENDENCIES,
                detail=detail,
            )

        if mode == DependencyMode.ALL_FINISHED:
            unfinished = [
                dep
                for dep in job.depends_on
                if not self._dep_is_finished(dep, job, reference_time)
            ]
            if not unfinished:
                return None
            detail = ", ".join(
                f"{d.job_name}[{resolve_run_id(d.cadence or job.cadence or self.default_cadence, reference_time)}] not finished"
                for d in unfinished
            )
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_DEPENDENCIES,
                detail=detail,
            )

        # THRESHOLD mode
        threshold = job.dependency_threshold  # validated non-None by Job validator
        deps = job.depends_on
        met = [
            dep for dep in deps if self._dep_status_matches(dep, job, reference_time)
        ]

        if len(met) >= threshold:
            return None  # satisfied

        not_yet_finished = [
            dep for dep in deps if not self._dep_is_finished(dep, job, reference_time)
        ]
        if len(met) + len(not_yet_finished) < threshold:
            detail = (
                f"threshold={threshold} unreachable: "
                f"{len(met)}/{len(deps)} succeeded, {len(not_yet_finished)} still running"
            )
            return self._result(
                job_name=job.name,
                run_id=logical_run_id,
                pool=job.pool,
                action=JobAction.SKIPPED_THRESHOLD_UNREACHABLE,
                detail=detail,
            )

        detail = f"threshold={threshold}: {len(met)}/{len(deps)} succeeded"
        return self._result(
            job_name=job.name,
            run_id=logical_run_id,
            pool=job.pool,
            action=JobAction.SKIPPED_DEPENDENCIES,
            detail=detail,
        )

    # ------------------------------------------------------------------
    # Alert helpers
    # ------------------------------------------------------------------

    def _check_terminal_alerts(self, job: Job, record: AttemptRecord) -> None:
        for cond in job.alerts:
            if cond.on == AlertOn.ERROR and record.status == Status.ERROR:
                self._emit_alert(
                    AlertOn.ERROR,
                    job,
                    record.logical_run_id,
                    record.reason,
                    record,
                    cond.channels,
                )
            elif cond.on == AlertOn.SUCCESS and record.status == Status.DONE:
                self._emit_alert(
                    AlertOn.SUCCESS,
                    job,
                    record.logical_run_id,
                    "Job completed successfully",
                    record,
                    cond.channels,
                )

    def _check_not_started_by_alert(
        self,
        job: Job,
        logical_run_id: str,
        reference_time: datetime,
        existing: AttemptRecord | None,
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
                    logical_run_id,
                    f"Job not started by {cond.not_by}",
                    None,
                    cond.channels,
                )

    def _emit_alert(
        self,
        alert_on: AlertOn,
        job: Job,
        logical_run_id: str,
        detail: str | None,
        record: AttemptRecord | None,
        channels: list[str] | None = None,
    ) -> None:
        if channels is None:
            channels = next((c.channels for c in job.alerts if c.on == alert_on), [])
        event = AlertEvent(
            alert_on=alert_on,
            job_name=job.name,
            run_id=logical_run_id,
            channels=channels or [],
            detail=detail,
            occurred_at=datetime.now(tz=timezone.utc),
            record=record,
        )
        try:
            self.alert_handler.handle(event)
        except Exception as exc:
            logger.error("Alert handler raised: %s", exc)

    # ------------------------------------------------------------------
    # Phase 3 — Manual retry/cancel operations
    # ------------------------------------------------------------------

    def manual_retry(
        self,
        job_name: str,
        logical_run_id: str,
        operator_name: str,
        operator_reason: str,
    ) -> AttemptRecord:
        """
        Create a new manual retry attempt for a job.

        The latest attempt must be in a terminal state (DONE, ERROR, LOST, CANCELLED).
        Creates a new attempt with status SUBMITTED, trigger_type=MANUAL_RETRY,
        and operator context (who/why).

        Args:
            job_name: Job name (must be registered in this orchestrator)
            logical_run_id: The logical run identifier
            operator_name: Name/ID of operator requesting retry
            operator_reason: Explanation for manual retry

        Returns:
            The newly created AttemptRecord (SUBMITTED status)

        Raises:
            ValueError: If job not found, or latest attempt not in terminal state
        """
        # Validate job exists
        job = self._job_index.get(job_name)
        if job is None:
            raise ValueError(f"Job '{job_name}' not found in orchestrator")

        # Get latest attempt
        latest = self.state.get_latest_attempt(job_name, logical_run_id)
        if latest is None:
            raise ValueError(
                f"No existing attempt for {job_name}/{logical_run_id}; "
                "cannot retry non-existent job"
            )

        # Ensure latest is terminal
        if not latest.is_finished():
            raise ValueError(
                f"Cannot retry {job_name}/{logical_run_id} while attempt {latest.attempt} "
                f"is still {latest.status.value} (not terminal)"
            )

        new_attempt_num = latest.attempt + 1
        now = datetime.now(tz=timezone.utc)

        # Submit via the normal execution path so the job is actually dispatched.
        # _submit writes the AttemptRecord (SUBMITTED) then calls executor.submit().
        self._submit(
            job=job,
            logical_run_id=logical_run_id,
            reference_time=now,
            attempt=new_attempt_num,
            trigger_type=TriggerType.MANUAL_RETRY,
            trigger_reason=operator_reason,
            operator_name=operator_name,
        )

        new_record = self.state.get_latest_attempt(job_name, logical_run_id)

        retry_request = RetryRequest(
            requested_at=now,
            requested_by=operator_name,
            logical_run_id=logical_run_id,
            requested_jobs=[job_name],
            cascade=False,
            reason=operator_reason,
            selected_jobs=[job_name],
            assigned_attempt_by_job={job_name: new_attempt_num},
        )
        self.state.append_retry_request(retry_request)

        logger.info(
            "Manual retry submitted: %s/%s attempt %d (operator=%s reason=%s)",
            job_name,
            logical_run_id,
            new_attempt_num,
            operator_name,
            operator_reason,
        )

        return new_record

    def manual_cancel(
        self, dispatchio_attempt_id: UUID, operator_name: str, operator_reason: str
    ) -> AttemptRecord:
        """
        Manually cancel a running or queued attempt.

        The attempt must be in an active state (SUBMITTED, QUEUED, RUNNING).
        Updates status to CANCELLED with operator context.

        Args:
            dispatchio_attempt_id: UUID of the attempt to cancel
            operator_name: Name/ID of operator requesting cancellation
            operator_reason: Explanation for cancellation

        Returns:
            The updated AttemptRecord (CANCELLED status)

        Raises:
            ValueError: If attempt not found or already terminal
        """
        # Look up attempt by ID
        attempt = self.state.get_attempt(dispatchio_attempt_id)
        if attempt is None:
            raise ValueError(
                f"Attempt {dispatchio_attempt_id} not found; cannot cancel"
            )

        # Ensure not already finished
        if attempt.is_finished():
            raise ValueError(
                f"Cannot cancel {attempt.job_name}/{attempt.logical_run_id}/{attempt.attempt}; "
                f"already {attempt.status.value}"
            )

        # Update to CANCELLED with operator context
        cancelled_record = attempt.model_copy(
            update={
                "status": Status.CANCELLED,
                "completed_at": datetime.now(tz=timezone.utc),
                "reason": operator_reason,
                "operator_name": operator_name,
            }
        )
        self.state.update_attempt(cancelled_record)

        logger.info(
            "Manual cancel: %s/%s/%d (operator=%s reason=%s)",
            attempt.job_name,
            attempt.logical_run_id,
            attempt.attempt,
            operator_name,
            operator_reason,
        )

        return cancelled_record
