from __future__ import annotations

from dispatchio.config.settings import (
    DispatchioSettings,
    ReceiverSettings,
    StateSettings,
)
from dispatchio.models import Job, SubprocessJob
from dispatchio_aws.config import aws_orchestrator_from_config
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor


def test_aws_orchestrator_registers_aws_executors() -> None:
    settings = DispatchioSettings(
        state=StateSettings(
            backend="sqlalchemy", connection_string="sqlite:///:memory:"
        ),
        receiver=ReceiverSettings(backend="none"),
    )
    jobs = [Job(name="j", executor=SubprocessJob(command=["echo", "{run_key}"]))]

    orchestrator = aws_orchestrator_from_config(jobs=jobs, config=settings)

    assert "lambda" in orchestrator.executors
    assert "stepfunctions" in orchestrator.executors
    assert "athena" in orchestrator.executors
    assert isinstance(orchestrator.executors["lambda"], LambdaExecutor)
    assert isinstance(orchestrator.executors["stepfunctions"], StepFunctionsExecutor)
    assert isinstance(orchestrator.executors["athena"], AthenaExecutor)
