# Dispatchio

A lightweight, tick-based batch job orchestrator for Python.

Dispatchio is designed for teams running daily/monthly ETL and batch jobs who want
something simpler than Airflow or Prefect, without giving up dependency management,
retry logic, or observability.

## How it works

A short-lived **orchestrator process** runs on a schedule (every 5 minutes via cron
or EventBridge). Each run — called a **tick** — evaluates all your job definitions,
submits any that are ready, and exits. There is no long-running daemon.

Jobs run independently and signal completion by posting a **completion event**
(a small JSON payload). The next tick picks this up and unblocks any waiting
downstream jobs.

```
cron (every 5 min)
    └─► Orchestrator.tick()
            ├─ drain completion events from queue/filesystem
            ├─ detect lost jobs (via poke or timeout)
            └─ for each job:
                  ├─ skip if already active or done
                  ├─ check time condition  (after="01:00")
                  ├─ check dependencies   (job_a[day0] == DONE?)
                  └─ submit if ready ──► ECS / Lambda / subprocess / HTTP
                                              │
                                              └─► posts CompletionEvent when done
```

## Installation

```bash
pip install dispatchio           # core package only
pip install "dispatchio[aws]"    # core + optional AWS extension (dispatchio_aws)
```

Requires Python 3.11+.

`dispatchio_aws` is optional. If you install only `dispatchio`, you still get
core scheduling, SQLAlchemy state, filesystem receiver, and local executors.
AWS receivers/executors are available only when `dispatchio[aws]` is installed.

## Quick start

### 1. Define your jobs

```python
# jobs.py
from dispatchio import Job, SubprocessJob, Dependency, TimeCondition, orchestrator_from_config

JOBS = [
    # Run a data ingest script every day after 1am
    Job(
        name="ingest",
        condition=TimeCondition(after="01:00"),
        executor=SubprocessJob(command=["python", "ingest.py", "--date", "{run_id}"]),
    ),

    # Run transform once ingest is done for the same day
    Job(
        name="transform",
        depends_on=[Dependency(job_name="ingest", run_id_expr="day0")],
        executor=SubprocessJob(command=["python", "transform.py", "--date", "{run_id}"]),
    ),
]

# Auto-discovers dispatchio.toml in current directory; env vars override config file
orchestrator = orchestrator_from_config(JOBS)
```

### 1a. Create a config file

Create `dispatchio.toml` in your project directory:

```toml
[dispatchio]
log_level = "INFO"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[dispatchio.receiver]
backend = "filesystem"
drop_dir = ".dispatchio/completions"
```

For more backends (DynamoDB, SQS, etc.) see [Configuration](#configuration).

### 2. Tick on a schedule

**Cron** (every 5 minutes):
```
*/5 * * * * cd /app && python -c "
from jobs import orchestrator
orchestrator.tick()
"
```

**Or run a tick manually:**
```bash
DISPATCHIO_ORCHESTRATOR=jobs:orchestrator dispatchio tick
```

### 3. Signal completion from your job

**Best practice: use the completion reporter abstraction** (works with any backend):

```python
# ingest.py
from dispatchio.worker.harness import run_job
from dispatchio.completion import get_reporter

def main(run_id: str) -> None:
    """Main job logic — exit or raise exception to signal status."""
    print(f"Ingesting data for {run_id}")
    reporter = get_reporter("ingest")

    try:
        # do work...
        rows = load_data()
        reporter.report_success(run_id, metadata={"rows": rows})
    except Exception as exc:
        reporter.report_error(run_id, str(exc))
        raise

if __name__ == "__main__":
    run_job(job_name="ingest", fn=main)
```

Then submit it as a `PythonJob`:
```python
Job(
    name="ingest",
    executor=PythonJob(script="ingest.py", function="main"),
)
```

The orchestrator **automatically injects the correct backend configuration** — your job code doesn't change whether you're running locally (filesystem) or on AWS (SQS).

**For subprocess jobs**, you can also use `get_reporter()`:

```python
# ingest.sh (or Python subprocess)
from dispatchio.completion import get_reporter

run_id = sys.argv[1]
reporter = get_reporter("ingest")

try:
    # do work...
    reporter.report_success(run_id, metadata={"status": "ok"})
except Exception as exc:
    reporter.report_error(run_id, str(exc))
```

---

**Manual completion events** (if you need custom control):

```python
# At the end of your job script
import json, pathlib, sys, os

run_id  = sys.argv[1]
drop    = pathlib.Path(os.getenv("DISPATCHIO_RECEIVER__DROP_DIR", ".dispatchio/completions"))
drop.mkdir(exist_ok=True)

(drop / f"ingest__{run_id}__done.json").write_text(json.dumps({
    "job_name": "ingest",
    "run_id":   run_id,
    "status":   "done",
}))
```

See [Completion Reporting](docs/completion_reporting.md) for detailed patterns and configuration.

### 3a. Local testing with `simulate()`

For local development, use `simulate()` to run multiple ticks without setting up a scheduler:

```python
from dispatchio import simulate
from jobs import orchestrator

# Run ticks every 0.5 seconds until all jobs are done
simulate(orchestrator, tick_interval=0.5)
```

`simulate()` advances time by one day per tick (configurable), submits jobs, drains completion
events, and exits when all jobs reach a terminal state. Perfect for testing and demos.

---

## Executor types

Dispatchio supports different ways to run your job logic:

### `SubprocessJob` — run shell commands

Best for: shell scripts, existing CLI tools, language-agnostic workloads.

```python
Job(
    name="etl",
    executor=SubprocessJob(command=["python", "etl.py", "--date", "{run_id}"]),
)
```

### `PythonJob` — run Python functions in subprocesses

Best for: Python-only workloads, reusing library code, cleaner than subprocess.

```python
# my_work.py
def ingest(run_id: str) -> None:
    print(f"Ingesting {run_id}")
    # ...

# jobs.py
from dispatchio import PythonJob, Job

Job(
    name="ingest",
    executor=PythonJob(script="my_work.py", function="ingest"),
)
```

Use `PythonJob` with the `run_job()` harness (see [Signal completion](#3-signal-completion-from-your-job)).

### AWS executors (`dispatchio[aws]` only)

AWS-backed executors are provided by the optional package and are configured
through `dispatchio_aws.config.aws_orchestrator_from_config`.

Supported executor configs:

- `LambdaJob`
- `StepFunctionJob`
- `AthenaJob`

Example (optional package):

```python
from dispatchio import Job, LambdaJob
from dispatchio_aws.config import aws_orchestrator_from_config

JOBS = [
    Job.create(
        "ingest",
        executor=LambdaJob(function_name="dispatchio-ingest"),
    )
]

orchestrator = aws_orchestrator_from_config(JOBS, config="dispatchio.toml")
```

See `examples/aws_lambda/README.md` for a complete optional AWS setup.

---

## Core concepts

### Run IDs

Every job execution is identified by a **run_id** — a string derived from the
tick's reference time using an expression.

| Expression | Resolves to (ref = 2025-01-15 09:30) |
|---|---|
| `day0` | `20250115` (today) |
| `day1` | `20250114` (yesterday) |
| `day3` | `20250112` (3 days ago) |
| `mon0` | `202501` (current month) |
| `mon1` | `202412` (previous month) |
| `week0` | `20250113` (Monday of current week) |
| `hour0` | `2025011509` (current hour) |
| `my-id` | `my-id` (literal — for ad-hoc runs) |

The same expression system is used in dependencies, so cross-job temporal
relationships are expressed cleanly:

```python
# transform depends on ingest for the same day
Dependency(job_name="ingest", run_id_expr="day0")

# report depends on ingest from 3 days ago
Dependency(job_name="ingest", run_id_expr="day3")

# monthly rollup depends on daily load for this month
Dependency(job_name="daily_load", run_id_expr="mon0")
```

### Job status lifecycle

```
PENDING → SUBMITTED → RUNNING → DONE
                    ↘          ↘
                     ERROR       (retry → SUBMITTED)
                     LOST        (poke detection → retry or alert)
                     SKIPPED
```

### Time conditions

Gate a job behind a wall-clock window within each day:

```python
TimeCondition(after="01:00")             # not before 1am
TimeCondition(after="02:00", before="06:00")  # only between 2am and 6am
```

### Dependencies

A job is only submitted once all its declared dependencies are in the required
status for their respective run_ids:

```python
Job(
    name="report",
    depends_on=[
        Dependency(job_name="ingest",    run_id_expr="day0"),
        Dependency(job_name="transform", run_id_expr="day0"),
    ],
    ...
)
```

Both `ingest` and `transform` must be `DONE` for today before `report` is submitted.

### Retry policy

```python
from dispatchio import RetryPolicy

RetryPolicy(max_attempts=1)                    # default — no retries
RetryPolicy(max_attempts=3)                    # retry up to 3 times on any error
RetryPolicy(max_attempts=3, retry_on=["timeout", "503"])  # only retry on matching errors
```

`retry_on` matches substrings of the `error_reason` field in the completion event.
If `retry_on` is empty, any error triggers a retry (up to `max_attempts`).

### Alerts

```python
from dispatchio import AlertCondition, AlertOn

Job(
    name="critical_load",
    alerts=[
        AlertCondition(on=AlertOn.ERROR, channels=["ops-slack"]),
        AlertCondition(on=AlertOn.NOT_STARTED_BY, not_by="03:00", channels=["ops-pager"]),
        AlertCondition(on=AlertOn.SUCCESS, channels=["data-team"]),
    ],
    ...
)
```

Alert conditions:
- `ERROR` — fired when retries are exhausted
- `LOST` — fired when a job is detected as lost (via poke or timeout)
- `NOT_STARTED_BY` — fired when the job hasn't been submitted by a given time
- `SUCCESS` — fired when the job completes successfully

By default alerts are written to the Python log. Replace `LogAlertHandler` with
your own implementation (SNS, Apprise, PagerDuty) and pass it to the orchestrator:

```python
class MySNSAlertHandler:
    def handle(self, event: AlertEvent) -> None:
        sns.publish(TopicArn="...", Message=event.model_dump_json())

orchestrator = Orchestrator(..., alert_handler=MySNSAlertHandler())
```

---

## Template variables

The following variables are interpolated into executor `command` strings and HTTP
`url`/`body` values at submission time:

| Variable | Value |
|---|---|
| `{run_id}` | Resolved run_id for this job (e.g. `20250115`) |
| `{job_name}` | The job's name |
| `{reference_time}` | ISO-8601 reference datetime of the tick |

```python
SubprocessJob(command=["python", "etl.py", "--date", "{run_id}", "--job", "{job_name}"])
HttpJob(url="https://api.example.com/jobs/{job_name}/run/{run_id}", method="POST")
```

---

## Configuration

Dispatchio separates **infrastructure config** (state backend, receiver, log level)
from **job logic** (job definitions, dependencies, retry policies). Infrastructure
config belongs in a file or environment variables; job logic stays in Python.

### Config file

Create `dispatchio.toml` in your project directory (or wherever you run Dispatchio from):

```toml
# dispatchio.toml

[dispatchio]
log_level = "INFO"

[dispatchio.state]
backend = "filesystem"
root    = ".dispatchio/state"       # relative paths resolve from this file's location

[dispatchio.receiver]
backend  = "filesystem"
drop_dir = ".dispatchio/completions"
```

For AWS deployments:

```toml
[dispatchio.state]
backend    = "dynamodb"
table_name = "dispatchio-state"
region     = "eu-west-1"

[dispatchio.receiver]
backend   = "sqs"
queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789/dispatchio-completions"
region    = "eu-west-1"
```

The config file can also live as a `[dispatchio]` section inside an existing
`pyproject.toml` — Dispatchio extracts its section automatically.

**Relative paths** in the config file (e.g. `root`, `drop_dir`) are resolved
relative to the directory containing the config file, not the process working
directory. This ensures consistent behaviour regardless of where you invoke Dispatchio.

### Config file discovery

`load_config()` and `orchestrator_from_config()` find the config file in this
order (first match wins):

1. Explicit `config=` argument: `orchestrator_from_config(JOBS, config="prod.toml")`
2. `DISPATCHIO_CONFIG` environment variable (local path or `ssm://` with `dispatchio[aws]`)
3. `./dispatchio.toml` in the current working directory
4. `~/.dispatchio.toml` user-level fallback

If no file is found, settings come from environment variables and built-in
defaults only — valid for container deployments that configure everything via env.

### Environment variable overrides

Any config value can be overridden with an environment variable. The convention
is `DISPATCHIO_` prefix for top-level fields and double-underscore (`__`) to address
nested fields:

| Environment variable | Config equivalent |
|---|---|
| `DISPATCHIO_LOG_LEVEL=DEBUG` | `log_level = "DEBUG"` |
| `DISPATCHIO_STATE__BACKEND=dynamodb` | `[dispatchio.state] backend = "dynamodb"` |
| `DISPATCHIO_STATE__TABLE_NAME=my-table` | `[dispatchio.state] table_name = "my-table"` |
| `DISPATCHIO_STATE__ROOT=/var/state` | `[dispatchio.state] root = "/var/state"` |
| `DISPATCHIO_RECEIVER__BACKEND=sqs` | `[dispatchio.receiver] backend = "sqs"` |
| `DISPATCHIO_RECEIVER__QUEUE_URL=https://...` | `[dispatchio.receiver] queue_url = "..."` |
| `DISPATCHIO_RECEIVER__DROP_DIR=/tmp/drops` | `[dispatchio.receiver] drop_dir = "/tmp/drops"` |

Environment variables always win over the config file.

### Using `orchestrator_from_config`

This is the recommended way to build an Orchestrator for anything beyond local dev:

```python
# jobs.py
from dispatchio import Job, SubprocessJob, Dependency, orchestrator_from_config

JOBS = [
    Job(
        name="ingest",
        executor=SubprocessJob(command=["python", "ingest.py", "--date", "{run_id}"]),
    ),
    Job(
        name="transform",
        depends_on=[Dependency(job_name="ingest", run_id_expr="day0")],
        executor=SubprocessJob(command=["python", "transform.py", "--date", "{run_id}"]),
    ),
]

# Auto-discovers dispatchio.toml; env vars override config file values
orchestrator = orchestrator_from_config(JOBS)
```

Or load settings programmatically and pass them directly:

```python
from dispatchio import DispatchioSettings, orchestrator_from_config
from dispatchio.config import StateSettings, ReceiverSettings

settings = DispatchioSettings(
    log_level="DEBUG",
    state=StateSettings(backend="memory"),        # useful in tests
    receiver=ReceiverSettings(backend="none"),
)
orchestrator = orchestrator_from_config(JOBS, config=settings)
```

### AWS Parameter Store (SSM)

With `dispatchio[aws]` installed, point `DISPATCHIO_CONFIG` at an SSM parameter that
contains the TOML config as its value:

```bash
# Store config in SSM (one-time setup)
aws ssm put-parameter \
    --name /myapp/dispatchio/config \
    --type SecureString \
    --value "$(cat dispatchio.prod.toml)"

# Use it at runtime
export DISPATCHIO_CONFIG=ssm:///myapp/dispatchio/config
```

The SSM source is fetched once at startup. Individual fields can still be
overridden via environment variables after the SSM config is loaded.

### Completion Event Abstraction

The `orchestrator_from_config()` function automatically configures job completion
reporting based on the receiver backend in your config file. Jobs use the
`get_reporter()` function to report their completion — the job code remains
**backend-agnostic**.

**Example: swap backends without changing job code**

Local development (filesystem):
```toml
[dispatchio.receiver]
backend = "filesystem"
drop_dir = ".dispatchio/completions"
```

AWS deployment (SQS) — same code works!
```toml
[dispatchio.receiver]
backend = "sqs"
queue_url = "https://sqs.us-east-1.amazonaws.com/..."
region = "us-east-1"
```

Your job code:
```python
from dispatchio.completion import get_reporter

reporter = get_reporter("my_job")
reporter.report_success(run_id, metadata={"rows": 1000})
```

No changes needed. See [Completion Reporting](docs/completion_reporting.md) for full details.

---

## CLI reference

The CLI connects to a Dispatchio instance by importing an `Orchestrator` object
from a Python module. Configure it via flag or environment variable.

```bash
export DISPATCHIO_ORCHESTRATOR=myproject.jobs:orchestrator
export DISPATCHIO_STATE_DIR=/var/dispatchio/state
```

### `dispatchio tick`

Run one orchestrator tick.

```bash
dispatchio tick
dispatchio tick --reference-time "2025-01-15T02:00:00"   # replay a specific time
dispatchio tick --orchestrator myproject.jobs:orchestrator
```

### `dispatchio status`

Show the status of job runs.

```bash
dispatchio status
dispatchio status --job ingest
dispatchio status --job ingest --run-id 20250115
dispatchio status --status error
```

### `dispatchio record set`

Manually override a run record. Useful to unblock a stuck job or mark a
completed job as done when the completion event was lost.

```bash
dispatchio record set ingest 20250115 done
dispatchio record set ingest 20250115 error --reason "manual reset"
```

---

## Multi-master setup

Run multiple independent orchestrators that share the same state store.
Each owns its own list of jobs; cross-master dependencies work automatically
because they all read from the same store.

```python
# master_a.py — owns ingest + transform
orchestrator = Orchestrator(jobs=[ingest, transform], state=shared_store, ...)

# master_b.py — owns report, depends on transform from master_a
report = Job(
    name="report",
    depends_on=[Dependency(job_name="transform", run_id_expr="day0")],
    ...
)
orchestrator = Orchestrator(jobs=[report], state=shared_store, ...)
```

No direct coupling between masters — `transform`'s state is just a record in
the shared store.

---

## Examples

Several working examples are in [examples/](examples/):

- [hello_world](examples/hello_world/) — minimal two-job example
- [cadence](examples/cadence/) — cross-cadence dependencies (daily → monthly)
- [conditions](examples/conditions/) — time conditions and schedule gates
- [dependency_modes](examples/dependency_modes/) — dependency strategies (threshold, best-effort)
- [dynamic_registration](examples/dynamic_registration/) — register jobs at runtime
- [subprocess_example](examples/subprocess_example/) — subprocess jobs with retries
- [multi_orchestrator](examples/multi_orchestrator/) — multiple independent orchestrators sharing state

Run any example:

```bash
pip install -e ".[dev]"
python examples/hello_world/run.py
```

Each example uses `simulate()` for local testing and includes a `dispatchio.toml` config file.

---

## Testing

### Unit test your job logic

Job functions (for `PythonJob`) are regular Python functions — test them directly:

```python
# my_work.py
def ingest(run_id: str) -> None:
    # your logic here
    ...

# test_my_work.py
from my_work import ingest

def test_ingest():
    ingest("20250115")  # call it like any function
    # assert expected results
```

### Test orchestrator logic

For testing job dependencies, conditions, and retry logic, use an in-memory state store:

```python
from dispatchio import Orchestrator, RunRecord, Status
from dispatchio.state.sqlalchemy_ import SQLAlchemyStateStore

def test_dependencies():
    # In-memory SQLite for testing
    state = SQLAlchemyStateStore("sqlite:///:memory:")

    orchestrator = Orchestrator(
        jobs=JOBS,
        state=state,
        receiver=None,  # no receiver needed for testing
        executors={...},
    )

    # Seed state with preconditions
    state.put(RunRecord(job_name="upstream", run_id="20250115", status=Status.DONE))

    # Tick and verify downstream is submitted
    result = orchestrator.tick()
    assert any(e.job_name == "downstream" for e in result.results)
```

---

## Extending Dispatchio

### Custom executor

```python
from dispatchio.executor.base import Executor
from dispatchio.models import Job
from datetime import datetime

class MyExecutor:
    def submit(self, job: Job, run_id: str, reference_time: datetime) -> None:
        # kick off the job however you like — must return immediately
        ...

orchestrator = Orchestrator(..., executors={"my_type": MyExecutor()})
```

Add `type: Literal["my_type"]` to a custom config class and use it in
`Job.executor`.

### Custom state backend

```python
from dispatchio.models import RunRecord, Status

class MyStateStore:
    def get(self, job_name: str, run_id: str) -> RunRecord | None: ...
    def put(self, record: RunRecord) -> None: ...
    def list_records(self, job_name=None, status=None) -> list[RunRecord]: ...
```

Any object implementing these three methods works — no base class required.

### Custom completion receiver

```python
class MyReceiver:
    def drain(self) -> list[CompletionEvent]:
        # return pending events and clear them
        ...
```

---

## Roadmap

- `dispatchio[aws]` — DynamoDB state store, SQS receiver, ECS and Lambda executors + SSM config source
- HTTP receiver — FastAPI-based endpoint for non-AWS environments
- Cron-style schedule conditions
- Web UI / status dashboard
