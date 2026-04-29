# dispatchio[aws] — Design and Implementation Plan

## Overview

`dispatchio[aws]` is an optional AWS extension providing cloud-native infrastructure
for dispatchio: RDS-backed state, SQS-based completion events, and executors for
Lambda, Step Functions, and Athena. The core package uses SQLAlchemy for state
management, making RDS (PostgreSQL, MySQL, Aurora) a natural fit. Everything here
requires `boto3` and is installed via `pip install dispatchio[aws]`.

---

## Current state (as of April 2026)

The core `dispatchio` package has evolved:
- **State store:** SQLAlchemy-based (`SQLAlchemyStateStore`) with in-memory and SQLite support
- **Configuration:** Pydantic-based settings with environment variable + TOML support
- **Executor protocol:** Extensible; supports `Pokeable` for liveness detection
- **No AWS extension yet:** This document outlines the plan to add `dispatchio_aws`

The most relevant current state store is `SQLAlchemyStateStore`, which can connect
to any SQLAlchemy-compatible database: SQLite, PostgreSQL (RDS), MySQL (RDS Aurora),
etc. This is the foundation for the AWS integration.

---

## Repo structure

Keep everything in the same repo, as a separate Python package under `dispatchio_aws/`.
The core `dispatchio` package is dependency-lite (SQLAlchemy is already required);
the AWS extension adds `boto3` for cloud services.

```
python-dispatchio/                  ← repo root
│
├── dispatchio/                     ← core package (uses SQLAlchemy)
│   ├── state/
│   │   └── sqlalchemy_.py          ← Universal state store (RDS, SQLite, etc.)
│   └── ...
│
├── dispatchio_aws/                 ← AWS extension package (new)
│   ├── pyproject.toml              ← separate build config; depends on dispatchio + boto3
│   ├── dispatchio_aws/
│   │   ├── __init__.py
│   │   ├── receiver/
│   │   │   └── sqs.py              ← SQS completion receiver
│   │   ├── reporter/
│   │   │   └── sqs.py              ← SQS completion reporter (worker side)
│   │   ├── executor/
│   │   │   ├── lambda_.py          ← async Lambda invocation
│   │   │   ├── stepfunctions.py    ← async Step Functions execution
│   │   │   └── athena.py           ← Athena query submission
│   │   ├── worker/
│   │   │   ├── lambda_handler.py   ← harness for dispatchio jobs running in Lambda
│   │   │   └── eventbridge_handler.py ← thin Lambda for EventBridge → SQS
│   │   └── config.py               ← AWS-specific settings + orchestrator factory
│   └── tests/
│       └── ...                     ← AWS code can be tested using moto
│
├── examples/
│   └── aws_lambda/                 ← end-to-end Lambda example
│
└── pyproject.toml                  ← core; [aws] extra points at dispatchio_aws
```

### Key design decisions

- **No S3+SQLite state store in `dispatchio_aws`:** The core `SQLAlchemyStateStore`
  already handles database connections. For AWS, point it to RDS and let SQLAlchemy
  handle the transport — cleaner than download/upload cycles.
- **State lifecycle in RDS:** RDS (Aurora/managed) is always-on and transactional,
  making it ideal for tick-based state updates. No S3 bucket needed.
- **Decoupling:** The two packages are decoupled at runtime (the core works without
  `boto3`). Cross-cutting changes can be coordinated in one PR.


### Installation

```bash
pip install dispatchio           # core only
pip install dispatchio[aws]      # core + dispatchio_aws + boto3
```

The core `pyproject.toml` already declares `[aws] = ["boto3>=1.26"]`. The AWS package
can be co-published to PyPI as `dispatchio-aws` and listed there as well.

---

## Architecture: the serverless tick

```
EventBridge (cron)
       │
       ▼
  Orchestrator Lambda          ← runs tick(), reads/writes state to RDS, submits jobs
       │  └─ reads/writes to RDS Aurora or managed RDS instance
       │  └─ polls SQS for completion events (Phase 1: primary completion path)
       │  └─ pokes RUNNING jobs via service APIs (Phase 2: safe fallback)
       │  └─ evaluates job graph
       │  └─ submits pending jobs → stores external ID in RunRecord.metadata
       └─ state persisted to RDS
              │
              ├── Worker Lambda A  ──────────────────────────────┐
              ├── Step Function B  ──── EventBridge ──┐          │
              └── Athena Query C  ───── EventBridge ──┴─► Thin completion Lambda
                                                               │
                                                               ▼
                                                    reads (job_name, run_id) from
                                                    metadata via external ID lookup
                                                               │
                                                               ▼
                                                           SQS queue
```

The orchestrator Lambda is short-lived and stateless. State persistence is entirely
in RDS (Aurora or managed RDS instance, provisioned outside this package). Jobs are
fire-and-forget from the orchestrator's perspective; completions arrive via SQS on
the next tick.

There are two completion paths, both converging on the same SQS queue:

- **Primary (push):** the job or thin Lambda posts to SQS immediately on completion.
- **Fallback (poke):** the orchestrator actively checks RUNNING jobs each tick via
  the relevant service API. Handles dropped SQS messages and detects crashes
  without requiring completion events.

---

## Components

### 1. RDS State Store Configuration

The core `SQLAlchemyStateStore` is the universal state backend. For AWS deployments,
configure it to point to an RDS instance (Aurora PostgreSQL, Aurora MySQL, or
managed RDS).

**Setup in dispatchio_aws:**

```python
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore

# Create a store pointing to RDS Aurora PostgreSQL
state_store = SQLAlchemyStateStore(
    connection_string="postgresql://user:password@my-db.rds.amazonaws.com:5432/dispatchio"
)
```

**Or via TOML config:**

```toml
[dispatchio.state]
backend = "sqlalchemy"
connection_string = "postgresql://user:password@my-db.rds.amazonaws.com:5432/dispatchio"
# Optionally: connection pool size, timeout, etc.
pool_size = 10
pool_recycle = 3600
```

**Database schema:**

The schema is created automatically by SQLAlchemy. Minimal table:

```sql
CREATE TABLE run_records (
    job_name        VARCHAR NOT NULL,
    run_id          VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,
    submitted_at    TIMESTAMP,
    started_at      TIMESTAMP,
    completed_at    TIMESTAMP,
    error_reason    TEXT,
    metadata        JSON,
    executor_reference JSON,
    PRIMARY KEY (job_name, run_id)
);
```

**Why RDS over S3+SQLite:**

- **Always-on:** No download/upload cycles. State is immediately visible to the next tick.
- **Transactional:** ACID guarantees for concurrent ticks (if ever needed).
- **Queryable:** Ad-hoc inspection via any SQL client for debugging and support.
- **Scalable:** Managed service handles backups, replication, failover.
- **Cost-effective:** No Lambda storage concerns; minimal I/O per tick.

**Connection pooling note:**

For Lambda, set `pool_pre_ping=True` to validate connections before use (RDS connections
may be recycled between Lambda invocations). The `SQLAlchemyStateStore` already handles
this via Pydantic settings.

---

Polls SQS at the start of each tick. Each message is a JSON completion event:

```json
{"job_name": "ingest", "run_id": "20260414", "status": "DONE"}
{"job_name": "transform", "run_id": "20260414", "status": "ERROR", "error_reason": "..."}
```

Messages are deleted from the queue after being processed. The receiver should handle
batching (up to 10 messages per `receive_message` call) and visibility timeouts.

```python
class SQSReceiver:
    def __init__(self, queue_url: str, region: str | None = None) -> None: ...
    def drain(self) -> list[CompletionEvent]: ...
```

---

### 3. SQS reporter  `dispatchio_aws.reporter.sqs`

The worker-side counterpart to the SQS receiver. Called by the job harness
(or directly by Lambda handlers) to post completion events.

```python
class SQSReporter:
    def __init__(self, queue_url: str, region: str | None = None) -> None: ...
    def report(self, job_name: str, run_id: str, status: Status, ...) -> None: ...
```

For Lambda workers, the queue URL is injected as `DISPATCHIO_SQS_QUEUE_URL`
(already referenced in the loader). Workers use `run_job()` with the SQS reporter
auto-detected from the env var, identical to the filesystem flow.

---

### 4. Lambda executor  `dispatchio_aws.executor.lambda_`

Invokes a Lambda function asynchronously (`InvocationType=Event`). The event
payload is a JSON object built from the execution context:

```json
{"run_id": "20260414", "job_name": "ingest"}
```

The user can customise the payload shape by supplying a `payload_fn` on `LambdaJob`:

```python
@dataclass
class LambdaJob:
    type: Literal["lambda"] = "lambda"
    function_name: str = ""
    # Optional: override the payload. Receives context dict, returns serialisable dict.
    # Defaults to passing context as-is.
    payload_fn: Callable[[dict], dict] | None = None
```

The invoked Lambda is responsible for posting DONE/ERROR to SQS when it finishes.
A thin harness (`dispatchio_aws.worker.lambda_handler`) makes this easy:

```python
# worker Lambda handler
from dispatchio_aws.worker.lambda_handler import dispatchio_handler

@dispatchio_handler          # posts DONE/ERROR to SQS automatically
def handler(run_id: str) -> None:
    # business logic — same signature-based injection as PythonJob
    ...
```

---

### 5. Step Functions executor  `dispatchio_aws.executor.stepfunctions`

Starts a Step Functions execution asynchronously (`start_execution`). The input
JSON follows the same customisable payload pattern as Lambda.

**Completion notification:** an EventBridge rule on `Step Functions Execution Status
Change` triggers the shared thin completion Lambda (see §7). No changes are required
to the state machine itself.

**run_id resolution in the thin Lambda:** the executor embeds `job_name` and `run_id`
in the execution name (`{job_name}--{run_id}`), which is included in the EventBridge
event payload. The thin Lambda parses them directly from the event — no state store
lookup needed for Step Functions.

The `executionArn` is stored in `RunRecord.metadata` at submission time for poke
support (see §8).

```python
@dataclass
class StepFunctionJob:
    type: Literal["stepfunctions"] = "stepfunctions"
    state_machine_arn: str = ""
    payload_fn: Callable[[dict], dict] | None = None
```

---

### 6. Athena executor  `dispatchio_aws.executor.athena`

Submits an Athena query directly via `start_query_execution` (no bridge Lambda
needed). The `QueryExecutionId` returned by the API is stored in `RunRecord.metadata`
at submission time.

**Completion notification:** an EventBridge rule on `Athena Query State Change`
triggers the shared thin completion Lambda (see §7). Because Athena EventBridge
events contain only the `QueryExecutionId` and not dispatchio context, the thin Lambda
performs a single metadata lookup in the state store to resolve `(job_name, run_id)`.

The `QueryExecutionId` also enables poke support (see §8) — the orchestrator calls
`get_query_execution` each tick for RUNNING Athena jobs.

```python
@dataclass
class AthenaJob:
    type: Literal["athena"] = "athena"
    query_string: str = ""
    database: str = ""
    output_location: str = ""   # s3://bucket/prefix/
    workgroup: str = "primary"
```

---

### 7. Thin completion Lambda  `dispatchio_aws.worker.eventbridge_handler`

A single reusable Lambda triggered by EventBridge rules for all supported services.
It normalises service-specific event shapes into dispatchio completion events and
posts them to SQS.

```text
EventBridge (Step Functions status change)  ─┐
EventBridge (Athena query state change)     ─┴─► eventbridge_handler Lambda ──► SQS
EventBridge (Glue job state change, etc.)  ─┘
```

**Resolving `(job_name, run_id)` from the event:**

| Service | Strategy |
|---|---|
| Step Functions | Parse directly from execution name (`{job_name}--{run_id}`) |
| Athena | Look up `QueryExecutionId` in state store metadata |
| Glue / others | Look up job run ID in state store metadata |

The metadata lookup is a read-only query. For S3+SQLite the handler downloads the
state file and queries `SELECT job_name, run_id FROM run_records WHERE metadata LIKE ?`.
For DynamoDB a GSI on the metadata field makes this efficient.

```python
# dispatchio_aws/worker/eventbridge_handler.py

def handler(event, context):
    source = event["source"]           # "aws.states", "aws.athena", ...
    status, external_id = _parse_event(source, event)
    job_name, run_id = _resolve_dispatchio_context(source, external_id, event)
    _post_to_sqs(job_name, run_id, status)
```

Users deploy this Lambda once per account and attach EventBridge rules for whichever
services they use. `dispatchio_aws` ships the handler code; a CDK construct, Terraform or
CloudFormation template is provided to wire up the rules.

---

### 8. Poke mechanism  `dispatchio_aws.executor.base`

An optional protocol that AWS executors implement. The orchestrator calls `poke()`
on each tick for jobs in RUNNING state, as a safe fallback alongside the push path.

```python
class Pokeable(Protocol):
    def poke(self, record: AttemptRecord) -> Status | None:
        """
        Check whether the job is still running by querying the service directly.
        Returns the current Status, or None if the execution ID is not available.
        """
        ...
```

Executor implementations:

```python
# AthenaExecutor
def poke(self, record: AttemptRecord) -> Status | None:
    query_id = record.metadata.get("query_execution_id")
    if not query_id:
        return None
    state = self._client.get_query_execution(
        QueryExecutionId=query_id
    )["QueryExecution"]["Status"]["State"]
    return _athena_state_to_status(state)   # SUCCEEDED→DONE, FAILED→ERROR, RUNNING→RUNNING

# StepFunctionsExecutor
def poke(self, record: AttemptRecord) -> Status | None:
    arn = record.metadata.get("execution_arn")
    if not arn:
        return None
    status = self._client.describe_execution(
        executionArn=arn
    )["status"]
    return _sfn_status_to_status(status)    # SUCCEEDED→DONE, FAILED→ERROR, RUNNING→RUNNING
```

The orchestrator checks for `Pokeable` executors during Phase 2 (detect lost jobs)
and calls `poke()` for any RUNNING record. If the service confirms the job is done,
the state is updated directly without waiting for SQS. This enables direct liveness
checks for cloud-managed jobs — the service itself is the source of truth.

---

## Configuration

`dispatchio_aws` extends `DispatchioSettings` with AWS-specific models. The TOML
config would look like:

```toml
[dispatchio]
name = "daily-etl"
default_cadence = "daily"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "postgresql://user:password@my-aurora.rds.amazonaws.com:5432/dispatchio"
pool_size = 10
pool_recycle = 3600

[dispatchio.receiver]
backend   = "sqs"
queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789/dispatchio-completions"
region    = "eu-west-1"
```

Job definitions use the new executor types:

```python
from dispatchio_aws.executor import LambdaJob, StepFunctionJob

ingest = Job.create(
    "ingest",
    executor=LambdaJob(function_name="my-ingest-function"),
)

report = Job.create(
    "weekly-report",
    executor=StepFunctionJob(
        state_machine_arn="arn:aws:states:...",
        payload_fn=lambda ctx: {"week": ctx["run_id"], "env": "prod"},
    ),
    depends_on=ingest,
)
```

An `orchestrator_from_config()` equivalent in `dispatchio_aws` wires up the
AWS-specific backends automatically:

```python
from dispatchio_aws.config import aws_orchestrator_from_config

orchestrator = aws_orchestrator_from_config(JOBS, config="dispatchio.toml")
```

---

## Lambda entry point (orchestrator tick)

The orchestrator Lambda handler is thin:

```python
# handler.py
from dispatchio_aws.config import aws_orchestrator_from_config
from my_project.jobs import JOBS

orchestrator = aws_orchestrator_from_config(JOBS)

def handler(event, context):
    orchestrator.tick()
```

The orchestrator connects to RDS, runs the tick, and returns. State is persisted
directly to the database (no download/upload needed). The whole invocation should
complete in seconds. EventBridge fires this on a schedule (e.g. every 5 minutes).

---

## Implementation roadmap

### Phase 1 — Foundation (unblock everything else)

1. Create `dispatchio_aws/` package structure and `pyproject.toml`
2. **RDS configuration:** Add Pydantic settings extension for RDS connection strings
3. **SQS receiver** — implement `SQSReceiver.drain()`
4. **SQS reporter** — implement `SQSReporter.report()` for worker-side completion
5. **Orchestrator factory** — `aws_orchestrator_from_config()` wiring RDS + SQS

*(No S3+SQLite state store in `dispatchio_aws`: use core `SQLAlchemyStateStore` → RDS)*

### Phase 2 — Lambda executor (primary use case)

1. **Lambda executor** — `LambdaJob` model + `LambdaExecutor.submit()`, stores `RequestId` in metadata
2. **Lambda worker harness** — `@dispatchio_handler` decorator for worker Lambdas
3. **AWS orchestrator factory** — `aws_orchestrator_from_config()` wiring all of the above
4. **End-to-end example** — `examples/aws_lambda/` with a two-job pipeline

### Phase 3 — Poke protocol (likely already done in core)

The `Pokeable` protocol is already in the core; orchestrators can check for it
during Phase 2 (lost detection).

If not yet implemented, add:

1. **`Pokeable` protocol in core** — optional method on executors; orchestrator calls it during Phase 2 (lost detection) for RUNNING jobs
2. **`PythonJobExecutor.poke()`** — check whether the spawned subprocess PID is still alive
3. **`SubprocessExecutor.poke()`** — same; both enable active liveness checks for local jobs

### Phase 4 — Step Functions + thin completion Lambda

1. **Step Functions executor** — `StepFunctionJob` + `start_execution`; embeds `job_name--run_id` in execution name; stores `executionArn` in metadata
2. **`StepFunctionsExecutor.poke()`** — `describe_execution` for RUNNING records
3. **Thin completion Lambda** — shared `eventbridge_handler`; EventBridge rules for Step Functions; CDK construct or CloudFormation template

### Phase 5 — Athena + extend thin Lambda

1. **Athena executor** — `AthenaJob` + `start_query_execution`; stores `QueryExecutionId` in metadata
2. **`AthenaExecutor.poke()`** — `get_query_execution` for RUNNING records
3. **Extend thin completion Lambda** — add Athena EventBridge rule; metadata lookup for ID resolution
4. Evaluate Glue, EMR Serverless — same pattern; add rules + poke methods per service

### Phase 6 — DynamoDB state store (optional, future)

An alternative to RDS if DynamoDB is preferred for cost/scale reasons:

1. **DynamoDB state store** — implement `DynamoDBStateStore` as alternative to RDS
2. Add GSI on metadata for efficient `QueryExecutionId` / `executionArn` lookups by the thin completion Lambda
3. Document trade-offs (cost, scale, query patterns) vs. RDS

*(Lower priority: RDS covers most use cases; DynamoDB adds complexity.)*

---

## Open questions

| Question | Notes |
|---|---|
| RDS connection pooling in Lambda | Lambda may recycle TCP connections between invocations. Use `pool_pre_ping=True` to validate connections. The core `SQLAlchemyStateStore` already supports this. |
| RDS multi-tick concurrency | Ticks are designed to be non-overlapping (one EventBridge trigger = one Lambda), so RDS transactions are straightforward. If concurrent ticks are ever needed, versioning or row-level locks can guard updates. |
| `payload_fn` serialisability | A Python callable cannot be expressed in TOML. Accept both a callable (in code) and a dict template (`{"week": "{run_id}"}` with string substitution) so config-file users are not blocked. |
| Poke in core vs. AWS | `Pokeable` protocol belongs in core (PythonJob and SubprocessJob benefit immediately). AWS executor poke implementations stay in `dispatchio_aws`. The orchestrator only imports the protocol, not any concrete implementation. |
| Execution name length | Step Functions execution names are capped at 80 characters. `{job_name}--{run_id}` must fit; enforce this at submit time with a clear error. |
| Completion Lambda state access | The thin Lambda needs to look up `(job_name, run_id)` from service-specific IDs. For RDS, a simple query suffices; add an index on metadata if lookups become slow. For DynamoDB, add a GSI. |
| CDK vs. CloudFormation vs. Terraform | Ship a CDK construct for the EventBridge rules + thin Lambda wiring. Users without CDK can use the synthesised CloudFormation template directly. |
| Versioning | Release `dispatchio` and `dispatchio-aws` at the same version. Simpler to reason about compatibility; accept that both are released even when only one changes. |
| Testing | Unit tests use `moto` (mocking AWS services). Integration tests require a real AWS environment (RDS + SQS) or LocalStack; mark with `pytest -m integration` and keep them opt-in. |
