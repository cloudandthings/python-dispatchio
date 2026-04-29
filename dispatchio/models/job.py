from __future__ import annotations

from datetime import datetime, time
from typing import Annotated, Any, Literal
from collections.abc import Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from dispatchio.cadence import Cadence
from dispatchio.conditions import AnyCondition
from dispatchio.models.enums import (
    AlertOn,
    DependencyMode,
    JobAction,
    Status,
    _SKIPPED_ACTIONS,
)


# ---------------------------------------------------------------------------
# Dependency
# ---------------------------------------------------------------------------


class JobDependency(BaseModel):
    """
    Declares that a job depends on another job reaching a given status
    for a particular run_key (resolved from a Cadence at tick time).
    """

    kind: Literal["job"] = "job"
    job_name: str
    cadence: Cadence | None = None
    required_status: Status = Status.DONE

    @classmethod
    def from_job(cls, job: Job, required_status: Status = Status.DONE) -> JobDependency:
        return cls(
            job_name=job.name, cadence=job.cadence, required_status=required_status
        )


class EventDependency(BaseModel):
    """TODO"""

    kind: Literal["event"] = "event"
    event_name: str
    cadence: Cadence | None = None
    required_status: Status = Status.DONE


Dependency = Annotated[JobDependency | EventDependency, Field(discriminator="kind")]


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------


class RetryPolicy(BaseModel):
    """
    Controls whether and when a job is retried after ERROR or LOST.

    max_attempts=1 means no retries. Set retry_on to a list of substrings;
    an empty retry_on with max_attempts > 1 retries on any error.
    """

    max_attempts: int = 1
    retry_on: list[str] = Field(default_factory=list)


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
# Alert condition
# ---------------------------------------------------------------------------


class AlertCondition(BaseModel):
    on: AlertOn
    not_by: time | None = None
    channels: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Executor configs
# ---------------------------------------------------------------------------


class SubprocessJob(BaseModel):
    """Run the job as a local subprocess."""

    type: Literal["subprocess"] = "subprocess"
    command: list[str]
    env: dict[str, str] = Field(default_factory=dict)


class HttpJob(BaseModel):
    """Trigger the job via an HTTP request."""

    type: Literal["http"] = "http"
    url: str
    method: str = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] = Field(default_factory=dict)


class PythonJob(BaseModel):
    """
    Run the job by calling a Python callable via the Dispatchio CLI.

    Two forms:
        entry_point="mypackage.jobs:run_ingest"  — runs via `dispatchio run`
        script="/abs/path/worker.py", function="run_ingest"  — runs via `dispatchio run-script`
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
    """Run the job by submitting an Athena query execution.

    Supply exactly one of `query_string` (inline Jinja template) or
    `query_template_uri` (s3://bucket/key to a Jinja .sql template).
    At execution time the template is rendered with attempt.params plus the
    standard context variables: run_key, job_name, reference_time.
    """

    type: Literal["athena"] = "athena"
    query_string: str | None = None
    query_template_uri: str | None = None
    database: str
    output_location: str
    workgroup: str = "primary"

    @model_validator(mode="after")
    def _validate_query_source(self) -> AthenaJob:
        has_inline = self.query_string is not None
        has_uri = self.query_template_uri is not None
        if not has_inline and not has_uri:
            raise ValueError(
                "AthenaJob requires either query_string or query_template_uri"
            )
        if has_inline and has_uri:
            raise ValueError(
                "AthenaJob accepts only one of query_string or query_template_uri, not both"
            )
        return self


ExecutorConfig = Annotated[
    SubprocessJob | HttpJob | PythonJob | LambdaJob | StepFunctionJob | AthenaJob,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Job definition
# ---------------------------------------------------------------------------


def _normalise_dep(item: Job | Dependency) -> Dependency:
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
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

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
    # Callable[[RunContext], list[RunSpec]] | None — stored as Any to avoid
    # a circular import; the orchestrator validates it at runtime.
    runs: Any = Field(default=None, exclude=True)

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
        runs: Any = None,
        **kwargs,
    ) -> Job:
        if not isinstance(depends_on, list) and depends_on is not None:
            depends_on = [depends_on]
        return cls(
            name=name,
            executor=executor,
            depends_on=[_normalise_dep(d) for d in (depends_on or [])],
            runs=runs,
            **kwargs,
        )


# ---------------------------------------------------------------------------
# Tick result
# ---------------------------------------------------------------------------


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
