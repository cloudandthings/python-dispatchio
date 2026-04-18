from __future__ import annotations

from datetime import datetime, timezone

import boto3
from botocore.stub import ANY, Stubber
from moto import mock_aws

from dispatchio.models import (
    AthenaJob,
    Job,
    LambdaJob,
    RunRecord,
    Status,
    StepFunctionJob,
)
from dispatchio_aws.executor.athena import AthenaExecutor
from dispatchio_aws.executor.lambda_ import LambdaExecutor
from dispatchio_aws.executor.stepfunctions import StepFunctionsExecutor


def _ref_time() -> datetime:
    return datetime(2026, 4, 18, 0, 0, tzinfo=timezone.utc)


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
    executor = LambdaExecutor(client=client)

    executor.submit(job=job, run_id="20260418", reference_time=_ref_time())
    reference = executor.get_executor_reference("ingest", "20260418")

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
    executor = StepFunctionsExecutor(region="eu-west-1")

    executor.submit(job=job, run_id="20260418", reference_time=_ref_time())
    reference = executor.get_executor_reference("transform", "20260418")

    assert reference is not None
    assert reference["execution_arn"].startswith("arn:")


@mock_aws
def test_athena_executor_submit_tracks_query_execution_id() -> None:
    # moto provides Athena API surfaces and accepts default DB/workgroup in tests.
    job = Job(
        name="athena-job",
        executor=AthenaJob(
            query_string="SELECT 1",
            database="default",
            output_location="s3://dispatchio-test-bucket/results/",
            workgroup="primary",
        ),
    )
    executor = AthenaExecutor(region="eu-west-1")

    executor.submit(job=job, run_id="20260418", reference_time=_ref_time())
    reference = executor.get_executor_reference("athena-job", "20260418")

    assert reference is not None
    assert reference["query_execution_id"]


def test_stepfunctions_poke_maps_terminal_statuses(monkeypatch) -> None:
    executor = StepFunctionsExecutor.__new__(StepFunctionsExecutor)

    class _Client:
        def describe_execution(self, executionArn: str) -> dict[str, str]:
            _ = executionArn
            return {"status": "SUCCEEDED"}

    executor._client = _Client()

    record = RunRecord(
        job_name="transform",
        run_id="20260418",
        status=Status.RUNNING,
        executor_reference={
            "execution_arn": "arn:aws:states:eu-west-1:123:execution:x:y"
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

    record = RunRecord(
        job_name="athena-job",
        run_id="20260418",
        status=Status.RUNNING,
        executor_reference={"query_execution_id": "qid-123"},
    )

    assert executor.poke(record) == Status.ERROR
