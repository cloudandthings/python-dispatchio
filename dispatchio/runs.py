"""
RunSpec and RunContext — the contract for multi-run jobs.

A job with a `runs` callable returns a list of RunSpec objects each tick.
The orchestrator evaluates each spec independently, using its run_key and
params to drive submission.

    def my_runs(rc: RunContext) -> list[RunSpec]:
        return [
            RunSpec(variant="v1", params={"date": rc.dates["day0_yyyymmddD"]}),
            RunSpec(variant="v2", params={"date": rc.dates["mon0_first_yyyymmddD"]}),
        ]

    job = Job.create("my_job", executor=..., runs=my_runs)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dispatchio.date_context import DateContext


@dataclass
class RunSpec:
    """
    Describes one run to be submitted for a job tick.

    Exactly one of `variant` or `run_key` may be set:
      - variant: appended to the tick's base run_key as "<base>:<variant>"
      - run_key: overrides the tick run_key entirely (use for collapsed keys
                 such as monthly keys generated during a daily backfill)
      - neither: the tick's base run_key is used as-is

    Params are stored in attempt.params and injected into executors
    as DISPATCHIO_PARAM_{KEY.upper()} environment variables.
    """

    variant: str | None = None
    run_key: str | None = None
    params: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.variant is not None and self.run_key is not None:
            raise ValueError("RunSpec: set either 'variant' or 'run_key', not both.")


@dataclass
class RunContext:
    """
    Context passed to a job's `runs` callable each tick.

    Attributes:
        reference_time: The logical tick time.
        ctx:            DateContext for resolving date variables.
        is_backfill:    True when the orchestrator is processing a backfill run.
        job_name:       Name of the job being evaluated.
    """

    reference_time: datetime
    dates: DateContext
    is_backfill: bool
    job_name: str
