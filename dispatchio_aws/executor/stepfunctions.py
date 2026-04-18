from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import Job, RunRecord, Status, StepFunctionJob
from dispatchio_aws.executor.base import build_execution_context, render_payload


@beartype
class StepFunctionsExecutor:
    """Submit StepFunctionJob runs via start_execution and support poke()."""

    def __init__(
        self,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
    ) -> None:
        self._client = client or boto3.client("stepfunctions", region_name=region)
        self._references: dict[tuple[str, str], dict[str, Any]] = {}

    def submit(
        self,
        job: Job,
        run_id: str,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, StepFunctionJob):
            raise TypeError(
                f"StepFunctionsExecutor requires StepFunctionJob, got {type(cfg).__name__}"
            )

        execution_name = f"{job.name}--{run_id}"
        if len(execution_name) > 80:
            raise ValueError(
                "Step Functions execution name must be <= 80 characters: "
                f"{execution_name!r}"
            )

        context = build_execution_context(
            job_name=job.name,
            run_id=run_id,
            reference_time_iso=reference_time.isoformat(),
        )
        payload = render_payload(cfg.payload_template, context)

        response = self._client.start_execution(
            stateMachineArn=cfg.state_machine_arn,
            name=execution_name,
            input=json.dumps(payload),
        )
        execution_arn = response["executionArn"]
        self._references[(job.name, run_id)] = {
            "execution_arn": execution_arn,
            "execution_name": execution_name,
        }

    def poke(self, record: RunRecord) -> Status | None:
        execution_arn = None
        if record.metadata:
            execution_arn = record.metadata.get("execution_arn")
        if execution_arn is None and record.executor_reference:
            execution_arn = record.executor_reference.get("execution_arn")
        if not execution_arn:
            return None

        response = self._client.describe_execution(executionArn=execution_arn)
        raw_status = response.get("status", "RUNNING")
        if raw_status in {"RUNNING", "PENDING_REDRIVE"}:
            return Status.RUNNING
        if raw_status == "SUCCEEDED":
            return Status.DONE
        return Status.ERROR

    def get_executor_reference(
        self, job_name: str, run_id: str
    ) -> dict[str, Any] | None:
        return self._references.get((job_name, run_id))
