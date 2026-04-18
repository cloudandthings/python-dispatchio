from __future__ import annotations

from datetime import datetime
from typing import Any

import boto3
from beartype import beartype
from botocore.client import BaseClient

from dispatchio.models import AthenaJob, Job, RunRecord, Status


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
        self._references: dict[tuple[str, str], dict[str, Any]] = {}

    def submit(
        self,
        job: Job,
        run_id: str,
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
        self._references[(job.name, run_id)] = {
            "query_execution_id": query_execution_id,
            "workgroup": cfg.workgroup,
        }

    def poke(self, record: RunRecord) -> Status | None:
        query_execution_id = None
        if record.metadata:
            query_execution_id = record.metadata.get("query_execution_id")
        if query_execution_id is None and record.executor_reference:
            query_execution_id = record.executor_reference.get("query_execution_id")
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

    def get_executor_reference(
        self, job_name: str, run_id: str
    ) -> dict[str, Any] | None:
        return self._references.get((job_name, run_id))
