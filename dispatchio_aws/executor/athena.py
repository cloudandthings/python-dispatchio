from __future__ import annotations

from datetime import datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import boto3
from beartype import beartype
from botocore.client import BaseClient
from jinja2 import Environment, StrictUndefined, UndefinedError

from dispatchio.executor.base import BaseExecutor
from dispatchio.models import Attempt, AthenaJob, Job, Status


@beartype
class AthenaExecutor(BaseExecutor):
    """Submit AthenaJob runs via start_query_execution and support poke()."""

    def __init__(
        self,
        region: str | None = None,
        *,
        client: BaseClient | None = None,
        s3_client: BaseClient | None = None,
    ) -> None:
        self._client = client or boto3.client("athena", region_name=region)
        self._s3_client = s3_client or boto3.client("s3", region_name=region)
        self._references: dict[str, dict[str, Any]] = {}  # keyed by str(correlation_id)

    def _fetch_s3_template(self, uri: str) -> str:
        parsed = urlparse(uri)
        if parsed.scheme != "s3":
            raise ValueError(f"query_template_uri must be an s3:// URI, got: {uri!r}")
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        response = self._s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")

    def _render_query(
        self,
        template: str,
        attempt: Attempt,
        reference_time: datetime,
    ) -> str:
        env = Environment(undefined=StrictUndefined, autoescape=False)
        context: dict[str, Any] = {
            **attempt.params,
            "run_key": attempt.run_key,
            "job_name": attempt.job_name,
            "reference_time": reference_time.isoformat(),
        }
        try:
            return env.from_string(template).render(**context)
        except UndefinedError as exc:
            raise ValueError(
                f"Athena query template for job {attempt.job_name!r} references "
                f"an undefined variable: {exc}"
            ) from exc

    def submit(
        self,
        job: Job,
        attempt: Attempt,
        reference_time: datetime,
        timeout: float | None = None,
    ) -> None:
        cfg = job.executor
        if not isinstance(cfg, AthenaJob):
            raise TypeError(
                f"AthenaExecutor requires AthenaJob, got {type(cfg).__name__}"
            )

        if cfg.query_template_uri is not None:
            raw_template = self._fetch_s3_template(cfg.query_template_uri)
        else:
            assert cfg.query_string is not None
            raw_template = cfg.query_string

        query_string = self._render_query(raw_template, attempt, reference_time)

        response = self._client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={"Database": cfg.database},
            ResultConfiguration={"OutputLocation": cfg.output_location},
            WorkGroup=cfg.workgroup,
        )
        query_execution_id = response["QueryExecutionId"]
        self._references[str(attempt.correlation_id)] = {
            "query_execution_id": query_execution_id,
            "workgroup": cfg.workgroup,
        }

    def poke(self, record: Attempt) -> Status | None:
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
