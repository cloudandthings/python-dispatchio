"""
Orchestrator — the tick engine.

The Orchestrator is stateless between runs. Each tick() call has five phases:

  1. Drain completion events from the receiver and update state.
  2. Detect lost jobs (heartbeat timeout exceeded) and mark them LOST.
  3. Evaluate every Job — pure planning, no side effects:
       a. Skip if already active (SUBMITTED/RUNNING) or finished (DONE/ERROR/LOST/SKIPPED).
       b. Skip if time condition is not met.
       c. Skip if any dependency is not satisfied.
       d. If ERROR/LOST, apply retry logic.
       e. Otherwise, return a pending-submission intent.
  4. Apply the per-tick submission cap (if configured).
  5. Execute pending submissions concurrently via a thread pool.

Separating evaluation (phase 3) from execution (phase 5) means the
planning phase reads a consistent state snapshot, and the per-tick cap
can be applied cleanly before any side effects occur.

The caller is responsible for scheduling tick() (e.g. EventBridge cron,
a local cron job, or a simple while loop).
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from collections.abc import Iterable

from dispatchio.alerts.base import AlertEvent, AlertHandler, LogAlertHandler
from dispatchio.cadence import DAILY, Cadence
from dispatchio.conditions import TimeOfDayCondition
from dispatchio.executor.base import Executor
from dispatchio.models import (
    AlertOn,
    Dependency,
    DependencyMode,
    JobAction,
    Job,
    JobTickResult,
    RunRecord,
    Status,
    TickResult,
)
from dispatchio.receiver.base import CompletionEvent, CompletionReceiver
from dispatchio.run_id import resolve_run_id
from dispatchio.state.base import StateStore

logger = logging.getLogger(__name__)


@dataclass
class _PendingSubmission:
    """Carries submission intent out of the planning phase."""

    job: Job
    run_id: str
    attempt: int


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
        submit_concurrency:     Max threads used to submit jobs in parallel within a tick.
        max_submissions_per_tick: Cap on submissions per tick; excess jobs are deferred
                                  to the next tick naturally (no state written for them).
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
        submit_concurrency: int = 8,
        max_submissions_per_tick: int | None = None,
        submit_timeout: float | None = None,
        default_cadence: Cadence = DAILY,
        strict_dependencies: bool = True,
        allow_runtime_mutation: bool = False,
    ) -> None:
        self.jobs = jobs
        self.state = state
        self.executors = executors
        self.receiver = receiver
        self.alert_handler = alert_handler or LogAlertHandler()
        self.submit_concurrency = submit_concurrency
        self.max_submissions_per_tick = max_submissions_per_tick
        self.submit_timeout = submit_timeout
        self.default_cadence = default_cadence
        self.strict_dependencies = strict_dependencies
        self.allow_runtime_mutation = allow_runtime_mutation

        self._job_index: dict[str, Job] = {}
        self._jobs_dirty = False
        self._has_ticked = False

        self._rebuild_job_index()
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

    def tick(self, reference_time: datetime | None = None) -> TickResult:
        """
        Run one evaluation cycle.

        reference_time: the logical "now" for this tick. Defaults to
        datetime.now(tz=timezone.utc). Pass an explicit value to replay
        a specific point in time (useful for testing and backfill).
        """
        if reference_time is None:
            reference_time = datetime.now(tz=timezone.utc)

        self._refresh_job_graph_if_dirty()

        result = TickResult(reference_time=reference_time)

        # Phase 1 — apply inbound completion events
        self._drain_receiver(reference_time)

        # Phase 2 — detect lost jobs
        result.results.extend(self._check_lost_jobs(reference_time))

        # Phase 3 — evaluate each job (planning only; no state writes)
        # outcomes preserves job-list order so results are reported in order.
        outcomes: list[JobTickResult | _PendingSubmission | None] = []
        for job in self.jobs:
            effective_cadence = job.cadence or self.default_cadence
            run_id = resolve_run_id(effective_cadence, reference_time)
            outcomes.append(
                self._evaluate_job(job, run_id, effective_cadence, reference_time)
            )

        # Phase 4 — apply per-tick submission cap
        pending_indices = [
            i for i, o in enumerate(outcomes) if isinstance(o, _PendingSubmission)
        ]
        if self.max_submissions_per_tick is not None:
            pending_indices = pending_indices[: self.max_submissions_per_tick]

        # Phase 5 — execute submissions concurrently; slot results back
        #            into their original positions to preserve job-list order.
        pending = [outcomes[i] for i in pending_indices]  # all _PendingSubmission
        submit_results = self._execute_submissions(pending, reference_time)
        for idx, tick_result in zip(pending_indices, submit_results):
            outcomes[idx] = tick_result

        for outcome in outcomes:
            if isinstance(outcome, JobTickResult):
                result.results.append(outcome)
            # None  → silently skipped (e.g. SKIPPED status)
            # _PendingSubmission still in outcomes → was beyond the cap; deferred

        self._has_ticked = True
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
        now = event.occurred_at or reference_time
        existing = self.state.get(event.job_name, event.run_id)

        if event.status == Status.RUNNING:
            # Heartbeat
            self.state.heartbeat(event.job_name, event.run_id, at=now)
            return

        if existing is None:
            logger.warning(
                "Received completion for unknown run %s/%s — creating record.",
                event.job_name,
                event.run_id,
            )
            record = RunRecord(
                job_name=event.job_name,
                run_id=event.run_id,
                status=event.status,
                completed_at=now,
                error_reason=event.error_reason,
                metadata=event.metadata,
            )
        else:
            if existing.is_finished():
                logger.debug(
                    "Ignoring completion event for already-finished run %s/%s (%s)",
                    event.job_name,
                    event.run_id,
                    existing.status,
                )
                return
            record = existing.model_copy(
                update={
                    "status": event.status,
                    "completed_at": now,
                    "error_reason": event.error_reason,
                    "metadata": {**existing.metadata, **event.metadata},
                }
            )

        self.state.put(record)
        logger.info(
            "Applied completion event: %s/%s → %s",
            event.job_name,
            event.run_id,
            event.status.value,
        )

        job = self._job_index.get(event.job_name)
        if job:
            self._check_terminal_alerts(job, record)

    # ------------------------------------------------------------------
    # Phase 2 — detect lost jobs
    # ------------------------------------------------------------------

    def _check_lost_jobs(self, reference_time: datetime) -> list[JobTickResult]:
        results = []
        for job in self.jobs:
            if job.heartbeat is None:
                continue
            effective_cadence = job.cadence or self.default_cadence
            run_id = resolve_run_id(effective_cadence, reference_time)
            record = self.state.get(job.name, run_id)
            if record is None or record.status != Status.RUNNING:
                continue
            if record.last_heartbeat_at is None:
                continue

            last_hb = record.last_heartbeat_at
            ref = reference_time
            if last_hb.tzinfo is None:
                last_hb = last_hb.replace(tzinfo=timezone.utc)
            if ref.tzinfo is None:
                ref = ref.replace(tzinfo=timezone.utc)

            elapsed = (ref - last_hb).total_seconds()
            if elapsed > job.heartbeat.timeout_seconds:
                detail = (
                    f"No heartbeat for {elapsed:.0f}s "
                    f"(timeout={job.heartbeat.timeout_seconds}s)"
                )
                lost_record = record.model_copy(update={"status": Status.LOST})
                self.state.put(lost_record)
                logger.warning("Job %s/%s marked LOST. %s", job.name, run_id, detail)

                self._emit_alert(AlertOn.LOST, job, run_id, detail, lost_record)

                results.append(
                    JobTickResult(
                        job_name=job.name,
                        run_id=run_id,
                        action=JobAction.MARKED_LOST,
                        detail=detail,
                    )
                )
        return results

    # ------------------------------------------------------------------
    # Phase 3 — evaluate a single job (planning; returns intent, not action)
    # ------------------------------------------------------------------

    def _evaluate_job(
        self,
        job: Job,
        run_id: str,
        cadence: Cadence,
        reference_time: datetime,
    ) -> JobTickResult | _PendingSubmission | None:
        existing = self.state.get(job.name, run_id)

        if existing and existing.is_active():
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.SKIPPED_ALREADY_ACTIVE,
            )

        if existing and existing.status == Status.DONE:
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.SKIPPED_ALREADY_DONE,
            )

        if existing and existing.status == Status.SKIPPED:
            return None  # silently skip

        if existing and existing.status in (Status.ERROR, Status.LOST):
            return self._plan_retry(job, existing, run_id, cadence, reference_time)

        # Condition gate
        if job.condition is not None:
            if not job.condition.is_met(reference_time, cadence):
                return JobTickResult(
                    job_name=job.name,
                    run_id=run_id,
                    action=JobAction.SKIPPED_CONDITION,
                    detail=f"condition not met: {type(job.condition).__name__}",
                )

        # Dependency check
        dep_result = self._check_dependencies(job, run_id, reference_time)
        if dep_result is not None:
            return dep_result

        # NOT_STARTED_BY alert check (before submitting)
        self._check_not_started_by_alert(job, run_id, reference_time, existing, cadence)

        return _PendingSubmission(job=job, run_id=run_id, attempt=0)

    # ------------------------------------------------------------------
    # Retry planning (part of phase 3)
    # ------------------------------------------------------------------

    def _plan_retry(
        self,
        job: Job,
        record: RunRecord,
        run_id: str,
        cadence: Cadence,
        reference_time: datetime,
    ) -> JobTickResult | _PendingSubmission:
        policy = job.retry_policy
        next_attempt = record.attempt + 1

        if next_attempt >= policy.max_attempts:
            detail = (
                f"max_attempts={policy.max_attempts} reached. "
                f"Last error: {record.error_reason}"
            )
            self._emit_alert(AlertOn.ERROR, job, run_id, detail, record)
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.MARKED_ERROR,
                detail=detail,
            )

        if policy.retry_on and record.error_reason:
            if not any(pat in record.error_reason for pat in policy.retry_on):
                detail = (
                    f"error_reason does not match retry_on patterns: {policy.retry_on}"
                )
                self._emit_alert(AlertOn.ERROR, job, run_id, detail, record)
                return JobTickResult(
                    job_name=job.name,
                    run_id=run_id,
                    action=JobAction.MARKED_ERROR,
                    detail=detail,
                )

        if job.condition and not job.condition.is_met(reference_time, cadence):
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.SKIPPED_CONDITION,
                detail=f"retry: condition not met: {type(job.condition).__name__}",
            )

        dep_result = self._check_dependencies(job, run_id, reference_time)
        if dep_result is not None:
            return dep_result

        return _PendingSubmission(job=job, run_id=run_id, attempt=next_attempt)

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

        if len(pending) == 1:
            return [
                self._submit(
                    pending[0].job,
                    pending[0].run_id,
                    reference_time,
                    pending[0].attempt,
                )
            ]

        # Pre-allocate result slots so we can write back by index from threads,
        # preserving the input order in the returned list.
        results: list[JobTickResult | None] = [None] * len(pending)

        with ThreadPoolExecutor(
            max_workers=min(self.submit_concurrency, len(pending))
        ) as pool:
            future_to_idx = {
                pool.submit(
                    self._submit,
                    ps.job,
                    ps.run_id,
                    reference_time,
                    ps.attempt,
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
                        ps.run_id,
                        exc,
                    )
                    results[idx] = JobTickResult(
                        job_name=ps.job.name,
                        run_id=ps.run_id,
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
        run_id: str,
        reference_time: datetime,
        attempt: int,
    ) -> JobTickResult:
        executor_type = job.executor.type
        executor = self.executors.get(executor_type)
        if executor is None:
            msg = f"No executor registered for type '{executor_type}'"
            logger.error(msg)
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.SUBMISSION_FAILED,
                detail=msg,
            )

        now = reference_time
        record = RunRecord(
            job_name=job.name,
            run_id=run_id,
            status=Status.SUBMITTED,
            attempt=attempt,
            submitted_at=now,
        )
        self.state.put(record)

        try:
            executor.submit(job, run_id, reference_time, timeout=self.submit_timeout)
            logger.info("Submitted %s/%s (attempt %d)", job.name, run_id, attempt)
            action = JobAction.RETRYING if attempt > 0 else JobAction.SUBMITTED
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=action,
                detail=f"attempt={attempt}",
            )
        except Exception as exc:
            error_msg = str(exc)
            logger.error("Submission failed for %s/%s: %s", job.name, run_id, error_msg)
            failed_record = record.model_copy(
                update={
                    "status": Status.ERROR,
                    "error_reason": f"submission_failed: {error_msg}",
                    "completed_at": now,
                }
            )
            self.state.put(failed_record)
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
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
            dep_run_id = resolve_run_id(cadence, reference_time)
            record = self.state.get(dep.job_name, dep_run_id)
            if record is None or record.status != dep.required_status:
                unmet.append(dep)
        return unmet

    def _dep_is_finished(
        self, dep: Dependency, job: Job, reference_time: datetime
    ) -> bool:
        cadence = dep.cadence or job.cadence or self.default_cadence
        dep_run_id = resolve_run_id(cadence, reference_time)
        record = self.state.get(dep.job_name, dep_run_id)
        return record is not None and record.is_finished()

    def _dep_status_matches(
        self, dep: Dependency, job: Job, reference_time: datetime
    ) -> bool:
        cadence = dep.cadence or job.cadence or self.default_cadence
        dep_run_id = resolve_run_id(cadence, reference_time)
        record = self.state.get(dep.job_name, dep_run_id)
        return record is not None and record.status == dep.required_status

    def _check_dependencies(
        self, job: Job, run_id: str, reference_time: datetime
    ) -> JobTickResult | None:
        """
        Returns a blocking JobTickResult if deps are not satisfied, or None if clear to proceed.
        For THRESHOLD unreachability, also writes a SKIPPED RunRecord to state.
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
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
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
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
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
            self.state.put(
                RunRecord(
                    job_name=job.name,
                    run_id=run_id,
                    status=Status.SKIPPED,
                )
            )
            return JobTickResult(
                job_name=job.name,
                run_id=run_id,
                action=JobAction.SKIPPED_THRESHOLD_UNREACHABLE,
                detail=detail,
            )

        detail = f"threshold={threshold}: {len(met)}/{len(deps)} succeeded"
        return JobTickResult(
            job_name=job.name,
            run_id=run_id,
            action=JobAction.SKIPPED_DEPENDENCIES,
            detail=detail,
        )

    # ------------------------------------------------------------------
    # Alert helpers
    # ------------------------------------------------------------------

    def _check_terminal_alerts(self, job: Job, record: RunRecord) -> None:
        for cond in job.alerts:
            if cond.on == AlertOn.ERROR and record.status == Status.ERROR:
                self._emit_alert(
                    AlertOn.ERROR,
                    job,
                    record.run_id,
                    record.error_reason,
                    record,
                    cond.channels,
                )
            elif cond.on == AlertOn.SUCCESS and record.status == Status.DONE:
                self._emit_alert(
                    AlertOn.SUCCESS,
                    job,
                    record.run_id,
                    "Job completed successfully",
                    record,
                    cond.channels,
                )

    def _check_not_started_by_alert(
        self,
        job: Job,
        run_id: str,
        reference_time: datetime,
        existing: RunRecord | None,
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
                    run_id,
                    f"Job not started by {cond.not_by}",
                    None,
                    cond.channels,
                )

    def _emit_alert(
        self,
        alert_on: AlertOn,
        job: Job,
        run_id: str,
        detail: str | None,
        record: RunRecord | None,
        channels: list[str] | None = None,
    ) -> None:
        if channels is None:
            channels = next((c.channels for c in job.alerts if c.on == alert_on), [])
        event = AlertEvent(
            alert_on=alert_on,
            job_name=job.name,
            run_id=run_id,
            channels=channels or [],
            detail=detail,
            occurred_at=datetime.now(tz=timezone.utc),
            record=record,
        )
        try:
            self.alert_handler.handle(event)
        except Exception as exc:
            logger.error("Alert handler raised: %s", exc)
