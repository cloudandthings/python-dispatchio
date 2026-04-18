from __future__ import annotations

import boto3
from moto import mock_aws

from dispatchio.models import RunRecord, Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore
from dispatchio_aws.receiver.sqs import SQSReceiver
from dispatchio_aws.worker.completion_handler import handler


@mock_aws
def test_stepfunctions_completion_handler_posts_to_sqs(monkeypatch) -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")

    event = {
        "source": "aws.states",
        "detail": {
            "status": "SUCCEEDED",
            "name": "ingest--20260418",
        },
    }

    response = handler(event, None)
    assert response["status"] == "ok"

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].job_name == "ingest"
    assert events[0].run_id == "20260418"
    assert events[0].status == Status.DONE


@mock_aws
def test_athena_completion_handler_resolves_context_from_state(
    monkeypatch, tmp_path
) -> None:
    sqs = boto3.client("sqs", region_name="eu-west-1")
    queue_url = sqs.create_queue(QueueName="dispatchio-completions")["QueueUrl"]

    monkeypatch.setenv("DISPATCHIO_RECEIVER__QUEUE_URL", queue_url)
    monkeypatch.setenv("DISPATCHIO_RECEIVER__REGION", "eu-west-1")

    db_path = tmp_path / "dispatchio.db"
    monkeypatch.setenv("DISPATCHIO_STATE__CONNECTION_STRING", f"sqlite:///{db_path}")

    store = SQLAlchemyStateStore(f"sqlite:///{db_path}")
    store.put(
        RunRecord(
            job_name="athena-job",
            run_id="20260418",
            status=Status.RUNNING,
            executor_reference={"query_execution_id": "qid-123"},
        )
    )

    event = {
        "source": "aws.athena",
        "detail": {
            "currentState": "SUCCEEDED",
            "queryExecutionId": "qid-123",
        },
    }

    response = handler(event, None)
    assert response["status"] == "ok"

    receiver = SQSReceiver(queue_url=queue_url, region="eu-west-1")
    events = receiver.drain()

    assert len(events) == 1
    assert events[0].job_name == "athena-job"
    assert events[0].run_id == "20260418"
    assert events[0].status == Status.DONE
