"""
Core Pydantic models for Dispatchio.

All domain concepts live here: job definitions, run records, dependencies,
retry policies, and executor configs.
"""

from __future__ import annotations

from datetime import datetime, time
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from dispatchio.cadence import Cadence
from dispatchio.conditions import AnyCondition


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------


class Status(str, Enum):
    """Lifecycle states for a single job run."""

    PENDING = "pending"  # known to scheduler, not yet submitted
    SUBMITTED = "submitted"  # handed off to executor, awaiting start confirmation
    RUNNING = "running"  # executor confirmed start or job posted RUNNING status
    DONE = "done"  # completed successfully
    ERROR = "error"  # failed; error_reason populated
    LOST = "lost"  # detected lost (e.g. via poke or timeout) — retry policy applied
    SKIPPED = "skipped"  # explicitly skipped (e.g. upstream permanently failed)

    # Convenience groupings (not stored as status values)
    @classmethod
    def finished(cls) -> frozenset[Status]:
        return frozenset({cls.DONE, cls.ERROR, cls.LOST, cls.SKIPPED})

    @classmethod
    def active(cls) -> frozenset[Status]:
        return frozenset({cls.SUBMITTED, cls.RUNNING})


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
# Run Record — the state store unit
# ---------------------------------------------------------------------------


class RunRecord(BaseModel):
    """
    A single execution instance of a job for a given run_id.
    This is the unit stored in the state store.
    """

    job_name: str
    run_id: str  # e.g. "20250115", "202501", "2025011502"
    status: Status
    attempt: int = 0  # 0-indexed; increments on each retry
    submitted_at: datetime | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_reason: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    executor_reference: dict[str, Any] | None = None

    def is_finished(self) -> bool:
        return self.status in Status.finished()

    def is_active(self) -> bool:
        return self.status in Status.active()


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


class Dependency(BaseModel):
    """
    Declares that a job depends on another job reaching a given status
    for a particular run_id (resolved from a Cadence at tick time).

    Examples:
        Dependency(job_name="ingest", cadence=DAILY)
            -> wait for 'ingest' to be DONE for today

        Dependency(job_name="monthly_load", cadence=MONTHLY)
            -> wait for 'monthly_load' to be DONE for current month

        Dependency(job_name="ingest", cadence=DateCadence(frequency=Frequency.DAILY, offset=-3))
            -> wait for 'ingest' to be DONE for 3 days ago
    """

    job_name: str
    cadence: Cadence | None = None
    required_status: Status = Status.DONE

    @classmethod
    def from_job(cls, job: Job, required_status: Status = Status.DONE) -> Dependency:
        """
        Create a Dependency from a Job, inheriting its cadence.

        Example:
             dep = Dependency.from_job(ingest)
             -> wait for 'ingest' to be DONE for the same cadence as this job
        """
        return cls(
            job_name=job.name, cadence=job.cadence, required_status=required_status
        )


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
    # {run_id}, {job_name}, {reference_time} are interpolated into command strings


class HttpJob(BaseModel):
    """Trigger the job via an HTTP request."""

    type: Literal["http"] = "http"
    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] = Field(default_factory=dict)
    # {run_id}, {job_name}, {reference_time} interpolated into url and body values


class PythonJob(BaseModel):
    """
    Run the job by calling a Python callable via `dispatchio run`.

    Two forms:
      entry_point="mypackage.jobs:run_ingest"  — module must be importable
      script="/abs/path/worker.py", function="run_ingest"  — file loaded directly

    Run ID and reporter are injected as env vars (DISPATCHIO_RUN_ID, DISPATCHIO_DROP_DIR)
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
    if isinstance(item, Dependency):
        return item
    if isinstance(item, Job):
        return Dependency.from_job(item)
    raise TypeError(f"Cannot convert {item!r} to Dependency.")


class Job(BaseModel):
    """
    Declares a job: what it does, when it runs, what it depends on,
    and how errors are handled.

    `cadence` controls how this job's run_id is computed from the tick's
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
                        "cadence on it, or use Dependency(...) directly."
                    )
                result.append(Dependency.from_job(item))
            else:
                result.append(item)
        return result

    @classmethod
    def create(
        cls,
        name: str,
        executor: ExecutorConfig,
        depends_on: list[Job | Dependency] | Dependency | Job | None = None,
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
    ALERT_SENT = "alert_sent"


class JobTickResult(BaseModel):
    job_name: str
    run_id: str
    action: JobAction
    detail: str | None = None


class TickResult(BaseModel):
    reference_time: datetime
    results: list[JobTickResult] = Field(default_factory=list)

    def submitted(self) -> list[JobTickResult]:
        return [r for r in self.results if r.action == JobAction.SUBMITTED]

    def skipped(self) -> list[JobTickResult]:
        return [r for r in self.results if r.action.value.startswith("skipped")]
