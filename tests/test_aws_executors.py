from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import boto3
from botocore.stub import ANY, Stubber
from moto import mock_aws
import pytest

from dispatchio.models import (
    Attempt,
    AthenaJob,
    Job,
    LambdaJob,
    Status,
    StepFunctionJob,
    TriggerType,
)
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor


def _ref_time() -> datetime:
    return datetime(2026, 4, 18, 0, 0, tzinfo=timezone.utc)


def _make_attempt(job_name: str, run_key: str, attempt: int = 0) -> Attempt:
    return Attempt(
        job_name=job_name,
        run_key=run_key,
        attempt=attempt,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={},
    )


def test_lambda_executor_submit_tracks_reference() -> None:
    client = boto3.client("lambda", region_name="eu-west-1")
    stubber = Stubber(client)
    stubber.add_response(
        "invoke",
        {
            "StatusCode": 202,
            "ResponseMetadata": {"RequestId": "req-123"},
        },
        {
            "FunctionName": "dispatchio-worker",
            "InvocationType": "Event",
            "Payload": ANY,
        },
    )
    stubber.activate()

    job = Job(
        name="ingest",
        executor=LambdaJob(function_name="dispatchio-worker"),
    )
    attempt = _make_attempt("ingest", "20260418")
    executor = LambdaExecutor(client=client)

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    reference = executor.get_executor_reference(attempt.correlation_id)

    assert reference is not None
    assert reference["function_name"] == "dispatchio-worker"

    stubber.deactivate()


@mock_aws
def test_stepfunctions_executor_submit_tracks_execution_arn() -> None:
    client = boto3.client("stepfunctions", region_name="eu-west-1")
    state_machine = client.create_state_machine(
        name="dispatchio-state-machine",
        definition='{"StartAt":"Done","States":{"Done":{"Type":"Succeed"}}}',
        roleArn="arn:aws:iam::123456789012:role/mock-role",
    )

    job = Job(
        name="transform",
        executor=StepFunctionJob(state_machine_arn=state_machine["stateMachineArn"]),
    )
    attempt = _make_attempt("transform", "20260418")
    executor = StepFunctionsExecutor(region="eu-west-1")

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    reference = executor.get_executor_reference(attempt.correlation_id)

    assert reference is not None
    assert reference["execution_arn"].startswith("arn:")


@mock_aws
def test_athena_executor_submit_tracks_query_execution_id() -> None:
    job = Job(
        name="athena-job",
        executor=AthenaJob(
            query_string="SELECT 1",
            database="default",
            output_location="s3://dispatchio-test-bucket/results/",
        ),
    )
    attempt = _make_attempt("athena-job", "20260418")
    executor = AthenaExecutor(region="eu-west-1")

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    reference = executor.get_executor_reference(attempt.correlation_id)

    assert reference is not None
    assert reference["query_execution_id"]


@mock_aws
def test_athena_executor_renders_params_into_query() -> None:
    """Jinja template variables in query_string are replaced with attempt.params."""
    captured: list[str] = []
    athena_client = boto3.client("athena", region_name="eu-west-1")
    stubber = Stubber(athena_client)
    stubber.add_response(
        "start_query_execution",
        {"QueryExecutionId": "qid-rendered"},
        expected_params={
            "QueryString": ANY,
            "QueryExecutionContext": {"Database": "events_db"},
            "ResultConfiguration": {"OutputLocation": "s3://bucket/out/"},
            "WorkGroup": "primary",
        },
    )

    def _capture(QueryString, **_):
        captured.append(QueryString)
        return {"QueryExecutionId": "qid-rendered"}

    athena_client.start_query_execution = lambda **kw: captured.append(
        kw["QueryString"]
    ) or {"QueryExecutionId": "qid-rendered"}  # noqa: E731

    job = Job(
        name="events",
        executor=AthenaJob(
            query_string="SELECT * FROM t WHERE dt BETWEEN '{{ start_date }}' AND '{{ end_date }}'",
            database="events_db",
            output_location="s3://bucket/out/",
        ),
    )
    attempt = Attempt(
        job_name="events",
        run_key="D20260418",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={},
        params={"start_date": "2026-04-01", "end_date": "2026-04-18"},
    )
    executor = AthenaExecutor.__new__(AthenaExecutor)
    executor._client = athena_client
    executor._s3_client = boto3.client("s3", region_name="eu-west-1")
    executor._references = {}

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    assert len(captured) == 1
    assert "2026-04-01" in captured[0]
    assert "2026-04-18" in captured[0]


@mock_aws
def test_athena_executor_renders_standard_context_vars() -> None:
    """run_key and reference_time are available as template variables."""
    captured: list[str] = []

    job = Job(
        name="ctx-job",
        executor=AthenaJob(
            query_string="-- {{ run_key }} {{ job_name }}",
            database="db",
            output_location="s3://bucket/out/",
        ),
    )
    attempt = Attempt(
        job_name="ctx-job",
        run_key="D20260418",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={},
    )
    athena_client = boto3.client("athena", region_name="eu-west-1")
    athena_client.start_query_execution = lambda **kw: captured.append(
        kw["QueryString"]
    ) or {"QueryExecutionId": "qid-ctx"}  # noqa: E731

    executor = AthenaExecutor.__new__(AthenaExecutor)
    executor._client = athena_client
    executor._s3_client = boto3.client("s3", region_name="eu-west-1")
    executor._references = {}

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    assert "D20260418" in captured[0]
    assert "ctx-job" in captured[0]


@mock_aws
def test_athena_executor_fetches_s3_template() -> None:
    """query_template_uri causes the template to be fetched from S3."""
    s3 = boto3.client("s3", region_name="eu-west-1")
    s3.create_bucket(
        Bucket="my-queries",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    template = "SELECT * FROM t WHERE dt = '{{ run_date }}'"
    s3.put_object(
        Bucket="my-queries", Key="queries/events.sql.j2", Body=template.encode()
    )

    job = Job(
        name="s3-job",
        executor=AthenaJob(
            query_template_uri="s3://my-queries/queries/events.sql.j2",
            database="db",
            output_location="s3://bucket/out/",
        ),
    )
    attempt = Attempt(
        job_name="s3-job",
        run_key="D20260418",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={},
        params={"run_date": "2026-04-18"},
    )

    captured: list[str] = []
    athena_client = boto3.client("athena", region_name="eu-west-1")
    athena_client.start_query_execution = lambda **kw: captured.append(
        kw["QueryString"]
    ) or {"QueryExecutionId": "qid-s3"}  # noqa: E731

    executor = AthenaExecutor.__new__(AthenaExecutor)
    executor._client = athena_client
    executor._s3_client = s3
    executor._references = {}

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    assert len(captured) == 1
    assert "2026-04-18" in captured[0]


def test_athena_job_requires_exactly_one_query_source() -> None:
    with pytest.raises(Exception, match="query_string or query_template_uri"):
        AthenaJob(database="db", output_location="s3://b/o/")

    with pytest.raises(Exception, match="only one"):
        AthenaJob(
            query_string="SELECT 1",
            query_template_uri="s3://b/k",
            database="db",
            output_location="s3://b/o/",
        )


def test_stepfunctions_poke_maps_terminal_statuses(monkeypatch) -> None:
    executor = StepFunctionsExecutor.__new__(StepFunctionsExecutor)

    class _Client:
        def describe_execution(self, executionArn: str) -> dict[str, str]:
            _ = executionArn
            return {"status": "SUCCEEDED"}

    executor._client = _Client()

    record = Attempt(
        job_name="transform",
        run_key="20260418",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={
            "executor": {"execution_arn": "arn:aws:states:eu-west-1:123:execution:x:y"}
        },
    )

    assert executor.poke(record) == Status.DONE


def test_athena_poke_maps_terminal_statuses(monkeypatch) -> None:
    executor = AthenaExecutor.__new__(AthenaExecutor)

    class _Client:
        def get_query_execution(self, QueryExecutionId: str):
            _ = QueryExecutionId
            return {"QueryExecution": {"Status": {"State": "FAILED"}}}

    executor._client = _Client()

    record = Attempt(
        job_name="athena-job",
        run_key="20260418",
        attempt=0,
        correlation_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={"executor": {"query_execution_id": "qid-123"}},
    )

    assert executor.poke(record) == Status.ERROR
