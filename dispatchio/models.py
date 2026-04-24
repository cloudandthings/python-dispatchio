"""
Core Pydantic models for Dispatchio.

All domain concepts live here: job definitions, run records, dependencies,
retry policies, and executor configs.
"""

from __future__ import annotations

from datetime import datetime, time
from enum import Enum
from typing import Annotated, Any, Literal
from collections.abc import Sequence
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from dispatchio.cadence import Cadence
from dispatchio.conditions import AnyCondition


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class Status(str, Enum):
    """Lifecycle states for a single job attempt."""

    SUBMITTED = "submitted"  # handed off to executor, awaiting start confirmation
    QUEUED = "queued"  # optional intermediate state; executor confirms queuing
    RUNNING = "running"  # executor confirmed start or job posted RUNNING status
    DONE = "done"  # completed successfully
    ERROR = "error"  # failed; reason populated
    LOST = "lost"  # detected lost (e.g. via poke or timeout) — retry policy applied
    CANCELLED = "cancelled"  # explicitly cancelled by user

    # Convenience groupings (not stored as status values)
    @classmethod
    def finished(cls) -> frozenset[Status]:
        return _FINISHED_STATUSES

    @classmethod
    def active(cls) -> frozenset[Status]:
        return _ACTIVE_STATUSES


# Defined after the class so enum members exist; returned by finished()/active().
_FINISHED_STATUSES: frozenset[Status] = frozenset(
    {Status.DONE, Status.ERROR, Status.LOST, Status.CANCELLED}
)
_ACTIVE_STATUSES: frozenset[Status] = frozenset(
    {Status.SUBMITTED, Status.QUEUED, Status.RUNNING}
)


# ---------------------------------------------------------------------------
# Trigger type and dead letter enums
# ---------------------------------------------------------------------------


class TriggerType(str, Enum):
    """Why an attempt was created."""

    SCHEDULED = "scheduled"  # triggered by scheduler/cadence
    AUTO_RETRY = "auto_retry"  # triggered by automatic retry policy
    MANUAL_RETRY = "manual_retry"  # triggered by user/manual request
    ADHOC = "adhoc"  # triggered by an explicit run-file --run-key submission


class DeadLetterReasonCode(str, Enum):
    """Why a completion event was rejected (dead-lettered)."""

    VALIDATION_FAILURE = (
        "validation_failure"  # payload missing required fields or invalid schema
    )
    CORRELATION_FAILURE = (
        "correlation_failure"  # correlation_id + identity fields do not resolve
    )
    POLICY_REJECT = "policy_reject"  # invalid state transition or other orchestration rule violation


class DeadLetterStatus(str, Enum):
    """Status of a dead-letter entry."""

    OPEN = "open"  # dead-letter recorded, not yet reviewed
    REPLAYED = "replayed"  # operator replayed and resolved
    IGNORED = "ignored"  # marked as safe to ignore


class DeadLetterSourceBackend(str, Enum):
    """Which backend/source submitted the dead-lettered completion."""

    SUBPROCESS = "subprocess"
    PYTHON = "python"
    LAMBDA = "lambda"
    STEPFUNCTIONS = "stepfunctions"
    ATHENA = "athena"
    HTTP = "http"
    OTHER = "other"


# ---------------------------------------------------------------------------
# Dependency mode
# ---------------------------------------------------------------------------


class DependencyMode(str, Enum):
    """Controls how a job's dependency set is evaluated."""

    ALL_SUCCESS = "all_success"  # default — every dep must reach required_status
    ALL_FINISHED = "all_finished"  # proceed once all deps are in any finished state
    THRESHOLD = (
        "threshold"  # proceed once ≥ dependency_threshold deps reach required_status
    )


# ---------------------------------------------------------------------------
# Attempt Record — the core state store unit (immutable sequence per logical run)
# ---------------------------------------------------------------------------


class AttemptRecord(BaseModel):
    """
    A single immutable execution instance (attempt) of a job for a given run_key.
    This is the unit stored in the run_attempts table.

    One job run (e.g., a daily schedule) can have multiple attempts due to
    retries. Attempts are numbered sequentially starting at 0.

    Operator context fields track manual operations (retry, cancel).
    """

    namespace: str = "default"  # orchestrator name; partitions records across pipelines
    job_name: str
    run_key: str  # e.g. "20250115", "202501", "2025011502"
    attempt: int  # 0-indexed; incremented for each retry/re-execution
    correlation_id: UUID  # opaque UUID; primary correlation key
    status: Status
    reason: str | None = None  # status reason
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    trigger_type: TriggerType = TriggerType.SCHEDULED
    trigger_reason: str | None = None
    trace: dict[str, Any] = Field(default_factory=dict)  # executor/upstream metadata
    completion_event_trace: dict[str, Any] | None = (
        None  # raw completion payload snapshot
    )
    # Operator context for manual operations
    requested_by: str | None = None  # who initiated manual operation (audit)

    def is_finished(self) -> bool:
        return self.status in Status.finished()

    def is_active(self) -> bool:
        return self.status in Status.active()


# ---------------------------------------------------------------------------
# Dead Letter Record — rejected completion events
# ---------------------------------------------------------------------------


class DeadLetterRecord(BaseModel):
    """
    A completion event that was rejected (dead-lettered) due to validation
    failure, status conflict, or other issue.

    Allows audit trail of problematic completions.
    """

    dead_letter_id: UUID = Field(default_factory=uuid4)
    occurred_at: datetime
    source_backend: DeadLetterSourceBackend
    reason_code: DeadLetterReasonCode
    reason_detail: str | None = None
    status: DeadLetterStatus = DeadLetterStatus.OPEN
    namespace: str = "default"  # orchestrator namespace that produced this dead letter
    # Event identity fields (attempted correlation)
    job_name: str | None = None
    run_key: str | None = None
    attempt: int | None = None
    correlation_id: UUID | None = None
    # Raw event payload for audit
    raw_payload: dict[str, Any] = Field(default_factory=dict)
    # Resolution tracking
    resolved_at: datetime | None = None
    resolver_notes: str | None = None


# ---------------------------------------------------------------------------
# RetryRequest — audit record for manual retry operations
# ---------------------------------------------------------------------------


class RetryRequest(BaseModel):
    """
    Audit record for a manual retry request.

    Created once per `dispatchio retry create` invocation so that operators
    can answer: who requested a retry, why, and which attempt numbers were
    assigned.
    """

    retry_request_id: UUID = Field(default_factory=uuid4)
    requested_at: datetime
    requested_by: str
    run_key: str
    requested_jobs: list[str] = Field(default_factory=list)
    cascade: bool = True
    reason: str | None = None
    selected_jobs: list[str] = Field(default_factory=list)
    assigned_attempt_by_job: dict[str, int] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


class JobDependency(BaseModel):
    """
    Declares that a job depends on another job reaching a given status
    for a particular run_key (resolved from a Cadence at tick time).

    Examples:
        JobDependency(job_name="ingest", cadence=DAILY)
            -> wait for 'ingest' to be DONE for today

        JobDependency(job_name="monthly_load", cadence=MONTHLY)
            -> wait for 'monthly_load' to be DONE for current month

        JobDependency(job_name="ingest", cadence=DateCadence(frequency=Frequency.DAILY, offset=-3))
            -> wait for 'ingest' to be DONE for 3 days ago
    """

    kind: Literal["job"] = "job"
    job_name: str
    cadence: Cadence | None = None
    required_status: Status = Status.DONE

    @classmethod
    def from_job(cls, job: Job, required_status: Status = Status.DONE) -> JobDependency:
        """
        Create a JobDependency from a Job, inheriting its cadence.

        Example:
             dep = JobDependency.from_job(ingest)
             -> wait for 'ingest' to be DONE for the same cadence as this job
        """
        return cls(
            job_name=job.name, cadence=job.cadence, required_status=required_status
        )


class EventDependency(BaseModel):
    """
    TODO
    """

    kind: Literal["event"] = "event"
    event_name: str
    cadence: Cadence | None = None
    required_status: Status = Status.DONE


Dependency = Annotated[JobDependency | EventDependency, Field(discriminator="kind")]

# ---------------------------------------------------------------------------
# Event
# ---------------------------------------------------------------------------


class Event(BaseModel):
    namespace: str = "default"  # target orchestrator namespace
    name: str
    run_key: str
    status: Status = Status.DONE
    occurred_at: datetime = Field(default_factory=datetime.now)
    trace: dict[str, Any] = Field(default_factory=dict)

    def is_finished(self) -> bool:
        return self.status in Status.finished()


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


class RetryPolicy(BaseModel):
    """
    Controls whether and when a job is retried after ERROR or LOST.

    By default no retries occur (max_attempts=1).
    Set retry_on to a list of substrings: if error_reason contains any of
    them the job will be retried. An empty retry_on with max_attempts > 1
    retries on any error.
    """

    max_attempts: int = 1
    retry_on: list[str] = Field(default_factory=list)
    # empty = retry on any error (up to max_attempts)
    # populated = only retry when error_reason contains one of these strings


# ---------------------------------------------------------------------------
# Admission policy
# ---------------------------------------------------------------------------


class PoolPolicy(BaseModel):
    """Admission limits for a single pool."""

    max_active_jobs: int | None = None
    max_submit_jobs_per_tick: int | None = None

    @model_validator(mode="after")
    def _validate_positive_limits(self) -> PoolPolicy:
        for field_name in ("max_active_jobs", "max_submit_jobs_per_tick"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                raise ValueError(f"{field_name} must be a positive integer when set")
        return self


class AdmissionPolicy(BaseModel):
    """Global and per-pool admission limits."""

    max_active_jobs: int | None = None
    max_submit_jobs_per_tick: int | None = None
    pools: dict[str, PoolPolicy] = Field(
        default_factory=lambda: {"default": PoolPolicy()}
    )

    @model_validator(mode="after")
    def _validate_positive_limits(self) -> AdmissionPolicy:
        if "default" not in self.pools:
            self.pools["default"] = PoolPolicy()
        for field_name in ("max_active_jobs", "max_submit_jobs_per_tick"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                raise ValueError(f"{field_name} must be a positive integer when set")
        return self


# ---------------------------------------------------------------------------

# Alert conditions
# ---------------------------------------------------------------------------


class AlertOn(str, Enum):
    ERROR = "error"  # job reached ERROR after all retries
    LOST = "lost"  # job marked LOST
    SUCCESS = "success"  # job reached DONE
    NOT_STARTED_BY = "not_started_by"  # job not submitted by a given time


class AlertCondition(BaseModel):
    on: AlertOn
    not_by: time | None = None  # wall-clock time — required when on=NOT_STARTED_BY
    channels: list[str] = Field(default_factory=list)  # passed to alert handler


# ---------------------------------------------------------------------------
# Executor configs  (discriminated union)
# ---------------------------------------------------------------------------


class SubprocessJob(BaseModel):
    """Run the job as a local subprocess. Good for local dev and testing."""

    type: Literal["subprocess"] = "subprocess"
    command: list[str]
    env: dict[str, str] = Field(default_factory=dict)
    # {run_key}, {job_name}, {reference_time} are interpolated into command strings


class HttpJob(BaseModel):
    """Trigger the job via an HTTP request."""

    type: Literal["http"] = "http"
    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] = Field(default_factory=dict)
    # {run_key}, {job_name}, {reference_time} interpolated into url and body values


class PythonJob(BaseModel):
    """
        Run the job by calling a Python callable via the Dispatchio CLI.

    Two forms:
            entry_point="mypackage.jobs:run_ingest"  — runs via `dispatchio run`
            script="/abs/path/worker.py", function="run_ingest"  — runs via `dispatchio run-script`

    Run key and reporter are injected as env vars (DISPATCHIO_RUN_KEY, DISPATCHIO_DROP_DIR)
    by the executor — job functions need no Dispatchio imports.

    pythonpath entries are prepended to PYTHONPATH in the spawned subprocess,
    useful when the module is not installed as a package.
    """

    type: Literal["python"] = "python"
    entry_point: str | None = None
    script: str | None = None
    function: str | None = None
    pythonpath: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_entry(self) -> PythonJob:
        has_entry = self.entry_point is not None
        has_script = self.script is not None and self.function is not None
        if not has_entry and not has_script:
            raise ValueError(
                "PythonJob requires either 'entry_point' (\"module:function\") "
                "or both 'script' and 'function'."
            )
        if self.script is not None and self.function is None:
            raise ValueError("PythonJob: 'function' is required when 'script' is set.")
        return self


class LambdaJob(BaseModel):
    """Run the job by invoking an AWS Lambda function asynchronously."""

    type: Literal["lambda"] = "lambda"
    function_name: str
    payload_template: dict[str, Any] | None = None


class StepFunctionJob(BaseModel):
    """Run the job by starting an AWS Step Functions execution asynchronously."""

    type: Literal["stepfunctions"] = "stepfunctions"
    state_machine_arn: str
    payload_template: dict[str, Any] | None = None


class AthenaJob(BaseModel):
    """Run the job by submitting an Athena query execution."""

    type: Literal["athena"] = "athena"
    query_string: str
    database: str
    output_location: str
    workgroup: str = "primary"


ExecutorConfig = Annotated[
    SubprocessJob | HttpJob | PythonJob | LambdaJob | StepFunctionJob | AthenaJob,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Job definition
# ---------------------------------------------------------------------------


def _normalise_dep(item: Job | Dependency) -> Dependency:
    """Convert anything dep-like to a Dependency."""
    if isinstance(item, JobDependency):
        return item
    if isinstance(item, Job):
        return JobDependency.from_job(item)
    if isinstance(item, EventDependency):
        return item
    raise TypeError(f"Cannot convert {item!r} to Dependency.")


class Job(BaseModel):
    """
    Declares a job: what it does, when it runs, what it depends on,
    and how errors are handled.

    `cadence` controls how this job's run_key is computed from the tick's
    reference_time. None means "inherit the orchestrator's default_cadence".
    """

    name: str
    executor: ExecutorConfig
    cadence: Cadence | None = None
    condition: AnyCondition | None = None
    depends_on: list[Dependency] = Field(default_factory=list)
    dependency_mode: DependencyMode = DependencyMode.ALL_SUCCESS
    dependency_threshold: int | None = None
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    alerts: list[AlertCondition] = Field(default_factory=list)
    pool: str = "default"
    priority: int = 0

    @model_validator(mode="after")
    def _check_dependency_threshold(self) -> Job:
        if self.dependency_mode == DependencyMode.THRESHOLD:
            if self.dependency_threshold is None or self.dependency_threshold <= 0:
                raise ValueError(
                    "Job with dependency_mode=THRESHOLD requires dependency_threshold "
                    "to be a positive integer."
                )
        return self

    @field_validator("depends_on", mode="before")
    @classmethod
    def _coerce_deps(cls, v: Any) -> Any:
        # Accept a single Job or Dependency — wrap in a list.
        if not isinstance(v, list):
            v = [v]
        result = []
        for item in v:
            if isinstance(item, Job):
                if item.cadence is None:
                    raise ValueError(
                        f"Job {item.name!r} has cadence=None; set an explicit "
                        "cadence on it, or use JobDependency(...) directly."
                    )
                result.append(JobDependency.from_job(item))
            else:
                result.append(item)
        return result

    @classmethod
    def create(
        cls,
        name: str,
        executor: ExecutorConfig,
        depends_on: Sequence[Job | Dependency] | Dependency | Job | None = None,
        **kwargs,
    ) -> Job:
        """
        Alternate constructor to allow positional name or executor.

        Job.create("my_job", PythonJob(...))
        """
        if not isinstance(depends_on, list) and depends_on is not None:
            depends_on = [depends_on]
        return cls(
            name=name,
            executor=executor,
            depends_on=[_normalise_dep(d) for d in (depends_on or [])],
            **kwargs,
        )


# ---------------------------------------------------------------------------
# Tick result — what the orchestrator reports after each tick
# ---------------------------------------------------------------------------


class JobAction(str, Enum):
    SUBMITTED = "submitted"
    WOULD_SUBMIT = "would_submit"  # dry-run: would have been submitted
    WOULD_DEFER = "would_defer"  # dry-run: would have been deferred by admission
    SKIPPED_CONDITION = "skipped_condition"
    SKIPPED_DEPENDENCIES = "skipped_dependencies"
    SKIPPED_THRESHOLD_UNREACHABLE = (
        "skipped_threshold_unreachable"  # threshold can no longer be met
    )
    SKIPPED_ALREADY_ACTIVE = "skipped_already_active"
    SKIPPED_ALREADY_DONE = "skipped_already_done"
    MARKED_LOST = "marked_lost"
    MARKED_ERROR = "marked_error"  # retry exhausted → ERROR
    RETRYING = "retrying"
    SUBMISSION_FAILED = "submission_failed"
    DEFERRED_ACTIVE_LIMIT = "deferred_active_limit"
    DEFERRED_POOL_ACTIVE_LIMIT = "deferred_pool_active_limit"
    DEFERRED_SUBMIT_LIMIT = "deferred_submit_limit"
    DEFERRED_POOL_SUBMIT_LIMIT = "deferred_pool_submit_limit"
    ALERT_SENT = "alert_sent"


_SKIPPED_ACTIONS: frozenset[JobAction] = frozenset(
    {
        JobAction.SKIPPED_CONDITION,
        JobAction.SKIPPED_DEPENDENCIES,
        JobAction.SKIPPED_THRESHOLD_UNREACHABLE,
        JobAction.SKIPPED_ALREADY_ACTIVE,
        JobAction.SKIPPED_ALREADY_DONE,
    }
)


class JobTickResult(BaseModel):
    job_name: str
    run_key: str
    pool: str = "default"
    action: JobAction
    detail: str | None = None


class TickResult(BaseModel):
    reference_time: datetime
    results: list[JobTickResult] = Field(default_factory=list)

    def submitted(self) -> list[JobTickResult]:
        return [r for r in self.results if r.action == JobAction.SUBMITTED]

    def skipped(self) -> list[JobTickResult]:
        return [r for r in self.results if r.action in _SKIPPED_ACTIONS]


# ---------------------------------------------------------------------------
# Orchestrator Run Record — first-class run model for backfill and replay
# ---------------------------------------------------------------------------


class OrchestratorRunStatus(str, Enum):
    """Lifecycle states for an orchestrator run."""

    PENDING = "pending"  # queued, waiting to become active
    ACTIVE = "active"  # currently being worked on
    BLOCKED = "blocked"  # paused due to external condition or operator intervention
    COMPLETED = "completed"  # successfully finished all jobs
    FAILED = "failed"  # terminated with unrecoverable error
    CANCELLED = "cancelled"  # explicitly cancelled by operator


class OrchestratorRunMode(str, Enum):
    """Why this orchestrator run was created."""

    SCHEDULED = "scheduled"  # normal wall-clock scheduling
    BACKFILL = "backfill"  # operator-initiated date range backfill
    REPLAY = "replay"  # operator-initiated replay of specific run key(s)


class OrchestratorRunRecord(BaseModel):
    """
    A first-class persisted orchestrator run, the outer unit of scheduling
    and backfill/replay coordination.

    An orchestrator run encapsulates one scheduling unit and tracks its
    lifecycle from submission through completion. Job attempts within the run
    reference the same run_key to derive their per-job run IDs.
    """

    orchestrator_run_id: UUID = Field(default_factory=uuid4)
    namespace: str  # orchestrator name; partitions runs across pipelines
    run_key: str  # identity of this orchestrator run, e.g. "20250115" or "event:12345"
    status: OrchestratorRunStatus
    mode: OrchestratorRunMode
    priority: int = 0  # higher priority runs are activated first when pending
    submitted_by: str | None = None  # user/system that submitted this run
    reason: str | None = None  # operator-provided reason (for audit)
    force: bool = False  # if true, allow re-running an already-completed run
    replay_group_id: UUID | None = None  # links multiple replay runs submitted together
    checkpoint: dict[str, Any] = Field(
        default_factory=dict
    )  # run-level checkpoint for resume/diagnostics
    opened_at: datetime
    activated_at: datetime | None = None
    closed_at: datetime | None = None
