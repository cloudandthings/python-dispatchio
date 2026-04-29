from __future__ import annotations

from pathlib import Path

from beartype import beartype

from dispatchio.config.factory import orchestrator
from dispatchio.config.settings import DispatchioSettings
from dispatchio.models import Job
from dispatchio.orchestrator import Orchestrator
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor


@beartype
def aws_orchestrator(
    jobs: list[Job] | None = None,
    config: str | Path | DispatchioSettings | None = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """Build an Orchestrator with both core and AWS executors enabled."""
    o = orchestrator(
        jobs=jobs,
        config=config,
        **orchestrator_kwargs,
    )
    o.executors.update(
        {
            "lambda": LambdaExecutor(),
            "stepfunctions": StepFunctionsExecutor(),
            "athena": AthenaExecutor(),
        }
    )
    return o
