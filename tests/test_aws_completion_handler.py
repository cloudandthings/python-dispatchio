from __future__ import annotations

from uuid import uuid4

import boto3
from moto import mock_aws

from dispatchio.models import AttemptRecord, Status, TriggerType
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio_aws.receiver.sqs import SQSReceiver
from dispatchio_aws.worker.completion_handler import handler


@mock_aws
def test_stepfunctions_completion_handler_posts_to_sqs(monkeypatch, tmp_path) -> None:
    # Reset module-level singleton so this test gets its own DB connection.
    monkeypatch.setattr("dispatchio_aws.worker.completion_handler._store", None)

    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "sqs")
    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")

    # Seed the state store with the attempt the completion handler will look up
    db_path = tmp_path / "dispatchio.db"
    monkeypatch.setenv("DISPATCHIO_STATE__CONNECTION_STRING", f"sqlite:///{db_path}")

    store = SQLAlchemyStateStore(f"sqlite:///{db_path}")
    correlation_id = uuid4()
    execution_arn = (
        "arn:aws:states:eu-west-1:123456789012:execution:my-sfn:ingest--20260418--0"
    )
    record = AttemptRecord(
        job_name="ingest",
        run_key="20260418",
        attempt=0,
        correlation_id=correlation_id,
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={
            "executor": {
                "execution_arn": execution_arn,
                "execution_name": "ingest--20260418--0",
            }
        },
    )
    store.append_attempt(record)

    event = {
        "source": "aws.states",
        "detail": {
            "status": "SUCCEEDED",
            "executionArn": execution_arn,
        },
    }

    response = handler(event, None)
    assert response["status"] == "ok"
    assert response["job_name"] == "ingest"
    assert response["run_key"] == "20260418"
    assert response["attempt"] == 0
    assert response["correlation_id"] == str(correlation_id)

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].status == Status.DONE
    assert events[0].correlation_id == correlation_id


@mock_aws
def test_athena_completion_handler_resolves_context_from_state(
    monkeypatch, tmp_path
) -> None:
    # Reset module-level singleton so this test gets its own DB connection.
    monkeypatch.setattr("dispatchio_aws.worker.completion_handler._store", None)

    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__BACKEND", "sqs")
    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")

    db_path = tmp_path / "dispatchio.db"
    monkeypatch.setenv("DISPATCHIO_STATE__CONNECTION_STRING", f"sqlite:///{db_path}")

    store = SQLAlchemyStateStore(f"sqlite:///{db_path}")
    correlation_id = uuid4()
    record = AttemptRecord(
        job_name="athena-job",
        run_key="20260418",
        attempt=0,
        correlation_id=correlation_id,
        status=Status.RUNNING,
        trigger_type=TriggerType.SCHEDULED,
        trace={"executor": {"query_execution_id": "qid-123", "workgroup": "primary"}},
    )
    store.append_attempt(record)

    event = {
        "source": "aws.athena",
        "detail": {
            "currentState": "SUCCEEDED",
            "queryExecutionId": "qid-123",
        },
    }

    response = handler(event, None)
    assert response["status"] == "ok"
    assert response["correlation_id"] == str(correlation_id)

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].status == Status.DONE
    assert events[0].correlation_id == correlation_id
