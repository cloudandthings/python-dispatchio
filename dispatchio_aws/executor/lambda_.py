from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import AttemptRecord, Job, LambdaJob
from dispatchio_aws.executor.base import build_execution_context, render_payload


@beartype
class LambdaExecutor:
    """Submit LambdaJob runs via asynchronous Lambda invocation."""

    def __init__(
        self,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
    ) -> None:
        self._client = client or boto3.client("lambda", region_name=region)
        self._references: dict[
            str, dict[str, Any]
        ] = {}  # keyed by str(dispatchio_attempt_id)

    def submit(
        self,
        job: Job,
        attempt: AttemptRecord,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, LambdaJob):
            raise TypeError(
                f"LambdaExecutor requires LambdaJob, got {type(cfg).__name__}"
            )

        context = build_execution_context(
            attempt=attempt,
            reference_time_iso=reference_time.isoformat(),
        )
        payload = render_payload(cfg.payload_template, context)

        response = self._client.invoke(
            FunctionName=cfg.function_name,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8"),
        )
        status_code = response.get("StatusCode")
        if status_code != 202:
            raise RuntimeError(
                f"Lambda async invocation failed for {cfg.function_name}: status={status_code}"
            )

        request_id = response.get("ResponseMetadata", {}).get("RequestId")
        self._references[str(attempt.dispatchio_attempt_id)] = {
            "function_name": cfg.function_name,
            "request_id": request_id,
        }

    def get_executor_reference(
        self, dispatchio_attempt_id: UUID
    ) -> dict[str, Any] | None:
        return self._references.get(str(dispatchio_attempt_id))
