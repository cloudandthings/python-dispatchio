from __future__ import annotations

from pathlib import Path

from beartype import beartype

from dispatchio.config.loader import orchestrator_from_config
from dispatchio.config.settings import DispatchioSettings
from dispatchio.models import Job
from dispatchio.orchestrator import Orchestrator
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor


@beartype
def aws_orchestrator_from_config(
    jobs: list[Job] | None = None,
    config: str | Path | DispatchioSettings | None = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """Build an Orchestrator with both core and AWS executors enabled."""
    orchestrator = orchestrator_from_config(
        jobs=jobs,
        config=config,
        **orchestrator_kwargs,
    )
    orchestrator.executors.update(
        {
            "lambda": LambdaExecutor(),
            "stepfunctions": StepFunctionsExecutor(),
            "athena": AthenaExecutor(),
        }
    )
    return orchestrator
