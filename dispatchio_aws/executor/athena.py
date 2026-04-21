from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import AttemptRecord, AthenaJob, Job, Status


@beartype
class AthenaExecutor:
    """Submit AthenaJob runs via start_query_execution and support poke()."""

    def __init__(
        self,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
    ) -> None:
        self._client = client or boto3.client("athena", region_name=region)
        self._references: dict[str, dict[str, Any]] = {}  # keyed by str(correlation_id)

    def submit(
        self,
        job: Job,
        attempt: AttemptRecord,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, AthenaJob):
            raise TypeError(
                f"AthenaExecutor requires AthenaJob, got {type(cfg).__name__}"
            )

        response = self._client.start_query_execution(
            QueryString=cfg.query_string,
            QueryExecutionContext={"Database": cfg.database},
            ResultConfiguration={"OutputLocation": cfg.output_location},
            WorkGroup=cfg.workgroup,
        )
        query_execution_id = response["QueryExecutionId"]
        self._references[str(attempt.correlation_id)] = {
            "query_execution_id": query_execution_id,
            "workgroup": cfg.workgroup,
        }

    def poke(self, record: AttemptRecord) -> Status | None:
        executor_trace = record.trace.get("executor", {})
        query_execution_id = executor_trace.get("query_execution_id")
        if not query_execution_id:
            return None

        response = self._client.get_query_execution(
            QueryExecutionId=query_execution_id,
        )
        raw_state = response["QueryExecution"]["Status"]["State"]
        if raw_state in {"QUEUED", "RUNNING"}:
            return Status.RUNNING
        if raw_state == "SUCCEEDED":
            return Status.DONE
        return Status.ERROR

    def get_executor_reference(self, correlation_id: UUID) -> dict[str, Any] | None:
        return self._references.get(str(correlation_id))
