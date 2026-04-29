# Reference Architecture: AWS Athena Pipeline

This guide walks through deploying dispatchio on AWS to orchestrate parameterised
Athena queries. It is the canonical pattern for teams running SQL-based data
pipelines on a serverless AWS stack.

## Architecture overview

```
EventBridge (every 5 min)
        │
        ▼
  Lambda: orchestrator
        │  reads config from SSM
        │  reads/writes state from RDS (PostgreSQL)
        │
        ├──► Athena (fetches .sql.j2 from S3, renders with params, submits query)
        │         │
        │         └──► EventBridge (aws.athena state change event)
        │                    │
        │                    ▼
        │              Lambda: eventbridge_handler
        │                    │  looks up attempt in RDS by query_execution_id
        │                    │  posts StatusEvent to SQS
        │                    │
        │                    ▼
        │                   SQS (completion event)
        │                    │
        │                    └──► Lambda: orchestrator (next tick drains via SQSReceiver)
        │
        └──► (other job types: Step Functions, …)

Operator workstation
        │  DISPATCHIO_CONFIG=ssm:///myapp/dispatchio
        │  direct connection to RDS
        │
        └──► dispatchio CLI  (status, backfill-enqueue, dispatch, …)
```

## Components

| Component | Service | Purpose |
|-----------|---------|---------|
| Orchestrator trigger | EventBridge | Fires every 5 minutes |
| Orchestrator Lambda | Lambda | Runs `aws_orchestrator(...).tick()` |
| Completion handler Lambda | Lambda | Triggered by EventBridge Athena/Step Functions state-change events; looks up the attempt in RDS and posts a `StatusEvent` to SQS |
| State store | RDS PostgreSQL | Persists attempts, runs, dead-letters |
| Config | SSM Parameter Store | Distributes `dispatchio.toml` to both Lambdas and operators |
| Completion events | SQS | Decouples Athena completion signalling from the orchestrator tick |
| Query templates | S3 | Stores versioned Jinja `.sql.j2` files |
| Query execution | Athena | Runs rendered SQL, writes results to S3 |

## Infrastructure inventory

| Resource | Type | Count | Notes |
|----------|------|-------|-------|
| Orchestrator schedule | EventBridge rule (scheduled) | 1 | Fires every 5 minutes; target is the orchestrator Lambda |
| Athena state-change rule | EventBridge rule (event pattern) | 1 | Source `aws.athena`, routes to the completion handler Lambda |
| Step Functions state-change rule | EventBridge rule (event pattern) | 1 | Source `aws.states`, routes to the completion handler Lambda; only needed if using Step Functions jobs |
| Orchestrator Lambda | Lambda function | 1 | Runs `aws_orchestrator(...).tick()`; bundled with job definitions |
| Completion handler Lambda | Lambda function | 1 | Provided by dispatchio (`dispatchio_aws.worker.eventbridge_handler`); no job-specific code required |
| State store | RDS PostgreSQL instance | 1 | Persists all attempts, orchestrator runs, and dead-letters; must be reachable from both Lambdas and operator workstations |
| Config parameter | SSM Parameter Store (String or SecureString) | 1 | Holds the full `dispatchio.toml`; read by both Lambdas and the CLI |
| Completion queue | SQS standard queue | 1 | Receives `StatusEvent` messages from the completion handler Lambda; drained by the orchestrator Lambda at each tick |
| Query template bucket | S3 bucket | 1 | Stores versioned `.sql.j2` Jinja templates; read by the orchestrator Lambda at query submission time |
| Athena results bucket | S3 bucket | 1 (or shared) | Athena writes query output here; referenced by `output_location` in each `AthenaJob` |
| Athena workgroup | Athena workgroup | 1 | Scopes query execution; enforces output location and cost controls |

## Config: SSM Parameter Store

Store your `dispatchio.toml` as a single SSM parameter (String or SecureString).
Both the Lambda and your local CLI resolve it via the `ssm://` URI scheme.

```toml
# Content of SSM parameter /myapp/dispatchio

[state]
backend = "sqlalchemy"
connection_string = "postgresql+psycopg://user:pass@rds-host:5432/dispatchio"

[receiver]
backend = "sqs"
queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789/dispatchio-completions"

[data_store]
backend = "none"
```

**Both Lambdas:** set the environment variable `DISPATCHIO_CONFIG=ssm:///myapp/dispatchio`.

**Operator local:** set `DISPATCHIO_CONFIG=ssm:///myapp/dispatchio` in your shell
profile, together with valid AWS credentials. The CLI reads config from SSM and
connects directly to RDS.

```python
# orchestrator_handler.py
import os
from dispatchio_aws.config import aws_orchestrator
from myapp.jobs import JOBS

def handler(event, context):
    orch = aws_orchestrator(jobs=JOBS)  # resolves config from DISPATCHIO_CONFIG
    orch.tick()
```

The completion handler Lambda is provided by dispatchio itself:

```python
# eventbridge_handler.py
from dispatchio_aws.worker.eventbridge_handler import handler
```

It handles both `aws.athena` and `aws.states` (Step Functions) EventBridge events.
Configure EventBridge rules to route those sources to this Lambda. It reads config
from `DISPATCHIO_CONFIG` (same SSM parameter), looks up the attempt in RDS by
the executor-specific identifier stored at submission time, and posts a
`StatusEvent` to the SQS queue.

## Job definitions: Python bundled with the Lambda

Job definitions are Python code (`DAILY`, `MONTHLY`, dependency logic, the `runs()`
callable). Bundle them with the Lambda package. Query *content* (SQL) lives on S3
separately, so changing a query doesn't require a Lambda redeploy.

```python
# myapp/jobs.py
from dispatchio import RunContext, RunSpec
from dispatchio.cadence import DAILY
from dispatchio.models import AthenaJob

OUTPUT_BUCKET = "s3://my-results/athena/"

def events_runs(rc: RunContext) -> list[RunSpec]:
    if rc.is_backfill:
        return [RunSpec(
            run_key=f"M{rc.dates['mon0_yyyymm']}",
            params={
                "start_date": rc.dates["mon0_first_yyyymmddD"],
                "end_date":   rc.dates["mon0_last_yyyymmddD"],
            },
        )]
    # Normal daily: month-to-date, plus previous month in the first 5 days
    runs = [RunSpec(
        variant="mtd",
        params={
            "start_date": rc.dates["mon0_first_yyyymmddD"],
            "end_date":   rc.dates["day0_yyyymmddD"],
        },
    )]
    if rc.dates["day0_yyyymm"] != rc.dates["day5_yyyymm"]:
        runs.append(RunSpec(
            variant="prev_month",
            params={
                "start_date": rc.dates["mon1_first_yyyymmddD"],
                "end_date":   rc.dates["mon1_last_yyyymmddD"],
            },
        ))
    return runs

JOBS = [
    Job(
        name="daily_events",
        cadence=DAILY,
        runs=events_runs,
        executor=AthenaJob(
            query_template_uri="s3://my-queries/events/daily_events.sql.j2",
            database="events_db",
            output_location=OUTPUT_BUCKET,
            workgroup="dispatchio",
        ),
    ),
]
```

## Query templates: Jinja SQL on S3

Templates are `.sql.j2` files in S3. At execution time `AthenaExecutor` fetches the
template and renders it with `attempt.params` plus the standard context variables
`run_key`, `job_name`, and `reference_time`.

```sql
-- s3://my-queries/events/daily_events.sql.j2
SELECT
    event_date,
    event_type,
    count(*)              AS event_count,
    count(DISTINCT user_id) AS unique_users
FROM events_db.raw_events
WHERE event_date BETWEEN DATE '{{ start_date }}' AND DATE '{{ end_date }}'
GROUP BY 1, 2
ORDER BY 1, 2
```

Updating a query is a single S3 `put_object` — no Lambda redeployment required.

## Operator workflow

With `DISPATCHIO_CONFIG=ssm:///myapp/dispatchio` set locally and RDS reachable:

```bash
# Check today's run status
dispatchio status --job daily_events

# Backfill a full year (Athena jobs collapse daily ticks → monthly run_keys)
dispatchio backfill-plan --start 2025-01-01 --end 2025-12-31
dispatchio backfill-enqueue --start 2025-01-01 --end 2025-12-31 \
    --submitted-by bjorn --reason "initial load"

# Ad-hoc run with explicit params (bypasses tick, immediate submission)
dispatchio dispatch --job daily_events --run-key D20260429 \
    --param start_date=2026-04-01 --param end_date=2026-04-29

# Cancel a stuck run
dispatchio cancel --job daily_events --run-key D20260429
```

## IAM permissions

The two Lambda execution roles need different permission sets.

**Orchestrator Lambda** — submits queries and drains completions:

```json
{
  "Effect": "Allow",
  "Action": [
    "ssm:GetParameter",
    "athena:StartQueryExecution",
    "s3:GetObject",
    "s3:PutObject",
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage"
  ],
  "Resource": ["..."]
}
```

**Completion handler Lambda** — looks up attempts and posts completion events:

```json
{
  "Effect": "Allow",
  "Action": [
    "ssm:GetParameter",
    "sqs:SendMessage"
  ],
  "Resource": ["..."]
}
```

The orchestrator Lambda role should have **read-only** access to the query template bucket. Write
access belongs to your CI/CD pipeline role only — this limits the blast radius if the
orchestrator Lambda role is compromised to data reads, not code injection.

## Terraform

The snippets below illustrate how to wire up the reference architecture. They are
meant as a starting point, not a production-ready module — VPC/subnet configuration,
RDS provisioning, and Lambda packaging are left to your own conventions.

```hcl
# ── Variables ────────────────────────────────────────────────────────────────

variable "prefix"                 { default = "myapp" }
variable "region"                 { default = "eu-west-1" }
variable "rds_connection_string"  { sensitive = true }
variable "orchestrator_zip"       { description = "Path to the built orchestrator Lambda zip" }
variable "s3_bucket"              { description = "S3 bucket holding .sql.j2 templates" }
variable "athena_output_location" { description = "s3:// URI for Athena query results" }

locals {
  ssm_config_path = "/${var.prefix}/dispatchio"
  dispatchio_env  = { DISPATCHIO_CONFIG = "ssm://${local.ssm_config_path}" }
}

# ── SSM: dispatchio config ────────────────────────────────────────────────────

resource "aws_ssm_parameter" "dispatchio_config" {
  name  = local.ssm_config_path
  type  = "SecureString"
  value = <<-TOML
    [state]
    backend           = "sqlalchemy"
    connection_string = "${var.rds_connection_string}"

    [receiver]
    backend   = "sqs"
    queue_url = "${aws_sqs_queue.completions.url}"
    region    = "${var.region}"

    [data_store]
    backend = "none"
  TOML
}

# ── SQS: completion events ────────────────────────────────────────────────────

resource "aws_sqs_queue" "completions" {
  name                       = "${var.prefix}-dispatchio-completions"
  message_retention_seconds  = 86400  # 1 day
  visibility_timeout_seconds = 60
}

# ── IAM: orchestrator Lambda ──────────────────────────────────────────────────

resource "aws_iam_role" "orchestrator" {
  name               = "${var.prefix}-dispatchio-orchestrator"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "orchestrator" {
  role   = aws_iam_role.orchestrator.id
  policy = data.aws_iam_policy_document.orchestrator.json
}

data "aws_iam_policy_document" "orchestrator" {
  statement {
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.dispatchio_config.arn]
  }
  statement {
    actions   = ["athena:StartQueryExecution"]
    resources = ["*"]
  }
  statement {
    actions   = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.s3_bucket}/*"]
  }
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${var.athena_output_location}/*"]
  }
  statement {
    actions   = ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"]
    resources = [aws_sqs_queue.completions.arn]
  }
}

# ── Lambda: orchestrator ──────────────────────────────────────────────────────

resource "aws_lambda_function" "orchestrator" {
  function_name = "${var.prefix}-dispatchio-orchestrator"
  role          = aws_iam_role.orchestrator.arn
  filename      = var.orchestrator_zip
  handler       = "orchestrator_handler.handler"
  runtime       = "python3.12"
  timeout       = 240  # ticks should finish well within 5 min

  environment {
    variables = local.dispatchio_env
  }
}

# ── EventBridge: scheduled trigger (every 5 min) ──────────────────────────────

resource "aws_cloudwatch_event_rule" "orchestrator_schedule" {
  name                = "${var.prefix}-dispatchio-tick"
  schedule_expression = "rate(5 minutes)"
}

resource "aws_cloudwatch_event_target" "orchestrator_schedule" {
  rule = aws_cloudwatch_event_rule.orchestrator_schedule.name
  arn  = aws_lambda_function.orchestrator.arn
}

resource "aws_lambda_permission" "orchestrator_schedule" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orchestrator.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.orchestrator_schedule.arn
}

# ── IAM: completion handler Lambda ───────────────────────────────────────────

resource "aws_iam_role" "eventbridge_handler" {
  name               = "${var.prefix}-dispatchio-completion-handler"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy" "eventbridge_handler" {
  role   = aws_iam_role.eventbridge_handler.id
  policy = data.aws_iam_policy_document.eventbridge_handler.json
}

data "aws_iam_policy_document" "eventbridge_handler" {
  statement {
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.dispatchio_config.arn]
  }
  statement {
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.completions.arn]
  }
}

# ── Lambda: completion handler ────────────────────────────────────────────────
# dispatchio_aws.worker.eventbridge_handler is bundled in the same zip as the
# orchestrator (it has no job-specific code), so the same package is reused here.

resource "aws_lambda_function" "eventbridge_handler" {
  function_name = "${var.prefix}-dispatchio-completion-handler"
  role          = aws_iam_role.eventbridge_handler.arn
  filename      = var.orchestrator_zip
  handler       = "dispatchio_aws.worker.eventbridge_handler.handler"
  runtime       = "python3.12"
  timeout       = 30

  environment {
    variables = local.dispatchio_env
  }
}

# ── EventBridge: Athena state changes → completion handler ────────────────────

resource "aws_cloudwatch_event_rule" "athena_state_change" {
  name = "${var.prefix}-dispatchio-athena-completion"
  event_pattern = jsonencode({
    source      = ["aws.athena"]
    detail-type = ["Athena Query State Change"]
    detail      = { currentState = ["SUCCEEDED", "FAILED", "CANCELLED"] }
  })
}

resource "aws_cloudwatch_event_target" "athena_state_change" {
  rule = aws_cloudwatch_event_rule.athena_state_change.name
  arn  = aws_lambda_function.eventbridge_handler.arn
}

resource "aws_lambda_permission" "athena_state_change" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.eventbridge_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.athena_state_change.arn
}

# ── EventBridge: Step Functions state changes → completion handler (optional) ──

resource "aws_cloudwatch_event_rule" "sfn_state_change" {
  name = "${var.prefix}-dispatchio-sfn-completion"
  event_pattern = jsonencode({
    source      = ["aws.states"]
    detail-type = ["Step Functions Execution Status Change"]
    detail      = { status = ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"] }
  })
}

resource "aws_cloudwatch_event_target" "sfn_state_change" {
  rule = aws_cloudwatch_event_rule.sfn_state_change.name
  arn  = aws_lambda_function.eventbridge_handler.arn
}

resource "aws_lambda_permission" "sfn_state_change" {
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.eventbridge_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.sfn_state_change.arn
}

# ── Shared ────────────────────────────────────────────────────────────────────

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ── Athena workgroup ──────────────────────────────────────────────────────────

resource "aws_athena_workgroup" "dispatchio" {
  name = "${var.prefix}-dispatchio"
  configuration {
    result_configuration {
      output_location = var.athena_output_location
    }
  }
}
```

> **Note:** The completion handler Lambda reuses the same zip as the orchestrator
> Lambda — `dispatchio_aws.worker.eventbridge_handler` ships as part of `dispatchio_aws`
> and requires no job-specific code. Both Lambdas only need
> `DISPATCHIO_CONFIG` in their environment; all other settings are read from SSM at
> runtime.

## Security note on query templates

Jinja rendering is the only execution surface introduced by S3 templates. The
renderer uses `StrictUndefined` (undefined variables raise an error rather than
silently producing empty strings) and SQL auto-escaping is disabled by design
(Athena takes raw SQL, not a parameterised driver). Mitigation:

1. Orchestrator Lambda role has S3 **read-only** on the template bucket.
2. CI/CD pipeline role has S3 **write** — gate this behind code review.
3. Enable S3 versioning and CloudTrail on the template bucket for audit.
4. Restrict `query_template_uri` values to a known bucket prefix via IAM
   resource conditions if you want an additional policy layer.
