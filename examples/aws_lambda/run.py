"""AWS Lambda example runner.

This example requires dispatchio[aws] and real AWS resources:
- Lambda function (DISPATCHIO_LAMBDA_FUNCTION_NAME)
- SQS queue for completion events
- SQL state database (RDS/Aurora recommended)

Run one tick:
    python examples/aws_lambda/run.py
"""

from __future__ import annotations

import logging

from examples.aws_lambda.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


if __name__ == "__main__":
    orchestrator.tick()
