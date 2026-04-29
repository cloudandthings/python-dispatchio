from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.executor.base import BaseExecutor
from dispatchio.models import Attempt, Job, Status, StepFunctionJob
from dispatchio_aws.executor.base import build_execution_context, render_payload


@beartype
class StepFunctionsExecutor(BaseExecutor):
    """Submit StepFunctionJob runs via start_execution and support poke()."""

    def __init__(
        self,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
    ) -> None:
        self._client = client or boto3.client("stepfunctions", region_name=region)
        self._references: dict[str, dict[str, Any]] = {}  # keyed by str(correlation_id)

    def submit(
        self,
        job: Job,
        attempt: Attempt,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, StepFunctionJob):
            raise TypeError(
                f"StepFunctionsExecutor requires StepFunctionJob, got {type(cfg).__name__}"
            )

        # Include attempt number in execution name so retries get unique names
        execution_name = f"{job.name}--{attempt.run_key}--{attempt.attempt}"
        if len(execution_name) > 80:
            raise ValueError(
                "Step Functions execution name must be <= 80 characters: "
                f"{execution_name!r}"
            )

        context = build_execution_context(
            attempt=attempt,
            reference_time_iso=reference_time.isoformat(),
        )
        payload = render_payload(cfg.payload_template, context)

        response = self._client.start_execution(
            stateMachineArn=cfg.state_machine_arn,
            name=execution_name,
            input=json.dumps(payload),
        )
        execution_arn = response["executionArn"]
        self._references[str(attempt.correlation_id)] = {
            "execution_arn": execution_arn,
            "execution_name": execution_name,
        }

    def poke(self, record: Attempt) -> Status | None:
        executor_trace = record.trace.get("executor", {})
        execution_arn = executor_trace.get("execution_arn")
        if not execution_arn:
            return None

        response = self._client.describe_execution(executionArn=execution_arn)
        raw_status = response.get("status", "RUNNING")
        if raw_status in {"RUNNING", "PENDING_REDRIVE"}:
            return Status.RUNNING
        if raw_status == "SUCCEEDED":
            return Status.DONE
        return Status.ERROR

    def get_executor_reference(self, correlation_id: UUID) -> dict[str, Any] | None:
        return self._references.get(str(correlation_id))
