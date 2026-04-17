# dispatchio[aws] — Design and Implementation Plan

## Overview

`dispatchio[aws]` is an optional AWS extension providing cloud-native infrastructure
for dispatchio: S3-backed state, SQS-based completion events, and executors for
Lambda, Step Functions, and Athena. The core package remains dependency-free;
everything here requires `boto3` and is installed via `pip install dispatchio[aws]`.

---

## Repo structure

Keep everything in the same repo, as a separate Python package under `dispatchio_aws/`.
The loader already gates on `ImportError`, so the two packages are decoupled at runtime.

```
python-dispatchio/                  ← repo root
│
├── dispatchio/                     ← core package (current)
│   └── ...
│
├── dispatchio_aws/                 ← AWS extension package (new)
│   ├── pyproject.toml              ← separate build config; depends on dispatchio + boto3
│   ├── dispatchio_aws/
│   │   ├── __init__.py
│   │   ├── state/
│   │   │   ├── s3_sqlite.py        ← S3+SQLite state store
│   │   │   └── dynamodb.py         ← DynamoDB state store (already stubbed)
│   │   ├── receiver/
│   │   │   └── sqs.py              ← SQS completion receiver (already stubbed)
│   │   ├── reporter/
│   │   │   └── sqs.py              ← SQS completion reporter (worker side)
│   │   ├── executor/
│   │   │   ├── lambda_.py          ← async Lambda invocation
│   │   │   ├── stepfunctions.py    ← async Step Functions execution
│   │   │   └── athena.py           ← Athena query submission (see notes)
│   │   ├── worker/
│   │   │   └── lambda_handler.py   ← harness for dispatchio jobs running in Lambda
│   │   └── config.py               ← AWS-specific settings models + orchestrator factory
│   └── tests/
│       └── ...                     ← AWS code can be tested using moto
│
├── examples/
│   └── aws_lambda/                 ← end-to-end Lambda example
│
└── pyproject.toml                  ← core; [aws] extra points at dispatchio_aws
```

### Why same repo

- Cross-cutting changes (e.g. new context keys, new `StateStore` protocol methods) can
  be made in one PR without coordination between repos.
- CI can run integration tests across both packages together.
- Single place for issues and versioning. The packages can be released independently
  when needed.

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
  Orchestrator Lambda          ← runs tick(), reads/writes state, submits jobs
       │  └─ downloads state.db from S3
       │  └─ polls SQS for completion events (Phase 1: primary completion path)
       │  └─ pokes RUNNING jobs via service APIs (Phase 2: safe fallback)
       │  └─ evaluates job graph
       │  └─ submits pending jobs → stores external ID in RunRecord.metadata
       └─ uploads state.db to S3
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
in S3 (SQLite file) or DynamoDB. Jobs are fire-and-forget from the orchestrator's
perspective; completions arrive via SQS on the next tick.

There are two completion paths, both converging on the same SQS queue:

- **Primary (push):** the job or thin Lambda posts to SQS immediately on completion.
- **Fallback (poke):** the orchestrator actively checks RUNNING jobs each tick via
  the relevant service API. Handles dropped SQS messages and detects crashes
  without requiring completion events.

---

## Components

### 1. S3 + SQLite state store  `dispatchio_aws.state.s3_sqlite`

**Pattern:** download → open → read/write → close → upload, all within a single tick.

This is safe because dispatchio ticks are non-overlapping by design (one Lambda
invocation per EventBridge trigger). If concurrent ticks are ever needed, a DynamoDB
lock or S3 conditional write can guard the upload.

**Why SQLite over S3?**
- The state file can be downloaded locally and queried with any SQL tool for ad-hoc
  inspection — useful for support.
- No extra AWS service (DynamoDB) required; just one S3 bucket.
- The file is self-contained and can be version-controlled or snapshotted by S3.

**Schema (proposed):**

```sql
CREATE TABLE IF NOT EXISTS run_records (
    job_name        TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,
    submitted_at    TEXT,
    started_at      TEXT,
    completed_at    TEXT,
    error_reason    TEXT,
    metadata        TEXT,   -- JSON blob
    executor_reference TEXT,  -- JSON blob for executor liveness tracking
    PRIMARY KEY (job_name, run_id)
);
```

**Interface:**

```python
class S3SQLiteStateStore:
    def __init__(self, bucket: str, key: str, local_path: str | None = None) -> None:
        ...

    def __enter__(self) -> S3SQLiteStateStore:
        # download from S3, open connection
        ...

    def __exit__(self, *_) -> None:
        # close connection, upload to S3
        ...

    # implements StateStore protocol: get, put, list_records
```

The orchestrator factory would wrap the store in a context manager around the tick:

```python
with state_store:
    orchestrator.tick()
```

Alternatively, `tick()` itself could call store lifecycle hooks if the store implements
an optional protocol — keeping the orchestrator unaware of the upload concern.

**Simpler alternative for now:** implement it without a context manager by downloading
at `__init__` and uploading explicitly after `tick()`. The factory function handles
the sequencing.

---

### 2. SQS receiver  `dispatchio_aws.receiver.sqs`  *(already stubbed)*

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

### 7. Thin completion Lambda  `dispatchio_aws.worker.completion_handler`

A single reusable Lambda triggered by EventBridge rules for all supported services.
It normalises service-specific event shapes into dispatchio completion events and
posts them to SQS.

```text
EventBridge (Step Functions status change)  ─┐
EventBridge (Athena query state change)     ─┴─► completion_handler Lambda ──► SQS
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
# dispatchio_aws/worker/completion_handler.py

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
    def poke(self, record: RunRecord) -> Status | None:
        """
        Check whether the job is still running by querying the service directly.
        Returns the current Status, or None if the execution ID is not available.
        """
        ...
```

Executor implementations:

```python
# AthenaExecutor
def poke(self, record: RunRecord) -> Status | None:
    query_id = record.metadata.get("query_execution_id")
    if not query_id:
        return None
    state = self._client.get_query_execution(
        QueryExecutionId=query_id
    )["QueryExecution"]["Status"]["State"]
    return _athena_state_to_status(state)   # SUCCEEDED→DONE, FAILED→ERROR, RUNNING→RUNNING

# StepFunctionsExecutor
def poke(self, record: RunRecord) -> Status | None:
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
backend = "s3-sqlite"
bucket  = "my-dispatchio-state"
key     = "daily-etl/state.db"

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

State is downloaded from S3, the tick runs, state is uploaded back. The whole
invocation should complete in seconds. EventBridge fires this on a schedule
(e.g. every 5 minutes).

---

## Implementation roadmap

### Phase 1 — Foundation (unblock everything else)

1. Create `dispatchio_aws/` package structure and `pyproject.toml`
2. **SQS receiver** — implement `SQSReceiver.drain()` (already stubbed in core loader)
3. **SQS reporter** — implement `SQSReporter.report()` for worker-side completion
4. **S3+SQLite state store** — download/open/write/close/upload lifecycle

### Phase 2 — Lambda executor (primary use case)

1. **Lambda executor** — `LambdaJob` model + `LambdaExecutor.submit()`, stores `RequestId` in metadata
2. **Lambda worker harness** — `@dispatchio_handler` decorator for worker Lambdas
3. **AWS orchestrator factory** — `aws_orchestrator_from_config()` wiring all of the above
4. **End-to-end example** — `examples/aws_lambda/` with a two-job pipeline

### Phase 3 — Poke protocol (core change)

This may have been done already.

1. **`Pokeable` protocol in core** — optional method on executors; orchestrator calls it during Phase 2 (lost detection) for RUNNING jobs
2. **`PythonJobExecutor.poke()`** — check whether the spawned subprocess PID is still alive
3. **`SubprocessExecutor.poke()`** — same; both enable active liveness checks for local jobs

### Phase 4 — Step Functions + thin completion Lambda

1. **Step Functions executor** — `StepFunctionJob` + `start_execution`; embeds `job_name--run_id` in execution name; stores `executionArn` in metadata
2. **`StepFunctionsExecutor.poke()`** — `describe_execution` for RUNNING records
3. **Thin completion Lambda** — shared `completion_handler`; EventBridge rules for Step Functions; CDK construct or CloudFormation template

### Phase 5 — Athena + extend thin Lambda

1. **Athena executor** — `AthenaJob` + `start_query_execution`; stores `QueryExecutionId` in metadata
2. **`AthenaExecutor.poke()`** — `get_query_execution` for RUNNING records
3. **Extend thin completion Lambda** — add Athena EventBridge rule; metadata lookup for ID resolution
4. Evaluate Glue, EMR Serverless — same pattern; add rules + poke methods per service

### Phase 6 — DynamoDB state store

1. **DynamoDB state store** — implement `DynamoDBStateStore` (already stubbed); add GSI on metadata for efficient `QueryExecutionId` / `executionArn` lookups by the thin Lambda

---

## Open questions

| Question | Notes |
|---|---|
| SQLite concurrency | Is one active tick at a time a safe assumption? If EventBridge fires while a previous tick is still running, the S3 upload could conflict. A DynamoDB conditional write (`If-Match` on ETag) would mitigate. |
| `payload_fn` serialisability | A Python callable cannot be expressed in TOML. Accept both a callable and a dict template (`{"week": "{run_id}"}` with string substitution) so config-file users are not blocked. |
| Poke in core vs. AWS | `Pokeable` protocol belongs in core (PythonJob and SubprocessJob benefit immediately). AWS executor poke implementations stay in `dispatchio_aws`. The orchestrator only imports the protocol, not any concrete implementation. |
| Execution name length | Step Functions execution names are capped at 80 characters. `{job_name}--{run_id}` must fit; enforce this at submit time with a clear error. |
| Thin Lambda state access | For S3+SQLite, the completion Lambda downloads the whole file for a single ID lookup — wasteful for large state files. DynamoDB with a GSI is the right answer at scale; document the trade-off clearly. |
| CDK vs. CloudFormation vs. Terraform | Ship a CDK construct for the EventBridge rules + thin Lambda wiring. Users without CDK can use the synthesised CloudFormation template directly. |
| Versioning | Release `dispatchio` and `dispatchio-aws` at the same version. Simpler to reason about compatibility; accept that both are released even when only one changes. |
| Testing | Unit tests use `moto` (already noted in repo structure). Integration tests require a real AWS environment or LocalStack; mark with `pytest -m integration` and keep them opt-in. |
