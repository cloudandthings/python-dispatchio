# AWS Lambda Example (Optional Package)

This example uses the optional AWS package, not core dispatchio.

Install:

```bash
pip install "dispatchio[aws]"
```

Required infrastructure:

- A Lambda function to execute your job code.
- An SQS queue for completion events.
- A SQL database for state (RDS/Aurora recommended).

Environment variables:

```bash
export DISPATCHIO_CONFIG=examples/aws_lambda/dispatchio.toml
export DISPATCHIO_LAMBDA_FUNCTION_NAME=dispatchio-ingest
export DISPATCHIO_RECEIVER__QUEUE_URL=https://sqs.eu-west-1.amazonaws.com/123456789012/dispatchio-completions
export DISPATCHIO_RECEIVER__REGION=eu-west-1
# For production, prefer RDS instead of sqlite
export DISPATCHIO_STATE__CONNECTION_STRING=postgresql+psycopg://user:pass@host:5432/dispatchio
```

Run one tick:

```bash
python examples/aws_lambda/run.py
```

Publish external dependency events (single command sends two events):

```bash
python examples/aws_lambda/external_event_producer.py 20250115
```

This emits:

- `event.user_registered`
- `event.kyc_passed`

Both are sent through the configured receiver backend (typically SQS in AWS).
