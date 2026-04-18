from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import boto3
from botocore.stub import ANY, Stubber
from moto import mock_aws

from dispatchio.models import (
    AttemptRecord,
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


def _make_attempt(
    job_name: str, logical_run_id: str, attempt: int = 0
) -> AttemptRecord:
    return AttemptRecord(
        job_name=job_name,
        logical_run_id=logical_run_id,
        attempt=attempt,
        dispatchio_attempt_id=uuid4(),
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
    reference = executor.get_executor_reference(attempt.dispatchio_attempt_id)

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
    reference = executor.get_executor_reference(attempt.dispatchio_attempt_id)

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
    attempt = _make_attempt("athena-job", "20260418")
    executor = AthenaExecutor(region="eu-west-1")

    executor.submit(job=job, attempt=attempt, reference_time=_ref_time())
    reference = executor.get_executor_reference(attempt.dispatchio_attempt_id)

    assert reference is not None
    assert reference["query_execution_id"]


def test_stepfunctions_poke_maps_terminal_statuses(monkeypatch) -> None:
    executor = StepFunctionsExecutor.__new__(StepFunctionsExecutor)

    class _Client:
        def describe_execution(self, executionArn: str) -> dict[str, str]:
            _ = executionArn
            return {"status": "SUCCEEDED"}

    executor._client = _Client()

    record = AttemptRecord(
        job_name="transform",
        logical_run_id="20260418",
        attempt=0,
        dispatchio_attempt_id=uuid4(),
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

    record = AttemptRecord(
        job_name="athena-job",
        logical_run_id="20260418",
        attempt=0,
        dispatchio_attempt_id=uuid4(),
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={"executor": {"query_execution_id": "qid-123"}},
    )

    assert executor.poke(record) == Status.ERROR
