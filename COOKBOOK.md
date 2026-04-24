# Dispatchio Cookbook

A collection of runnable examples, each in its own directory under
[`examples/`](examples/). To run any example:

```
python examples/<name>/run.py
```

> **Production note:** `run.py` uses `run_loop()` to drive multiple ticks
> locally. In production, replace it with a single `orchestrator.tick()`
> call triggered by your scheduler (EventBridge, cron, Kubernetes CronJob, …).

---

## Hello World

> The simplest possible Dispatchio pipeline: The Job class creates two jobs and a dependency between them.

**Tags:** `PythonJob` · `dependencies` · `getting-started`

**Run:** `python examples/hello_world/run.py`

**`jobs.py`**

```python
"""
Hello World — Explicit Job and Orchestrator definitions.

This is the explicit approach: Job objects are defined here and wired into
an Orchestrator.  Use this pattern for complex pipelines with cross-file
dependencies, dynamic registration, or graph loading.

For simple cases, see my_work.py which uses the @job decorator instead —
no separate jobs.py needed.

Run with:
  python examples/hello_world/run.py
"""

import os
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

hello_world = Job.create(
    "hello_world",
    # default_cadence is set to DAILY in dispatchio.toml
    # cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="hello_world",
    ),
)

goodbye_world = Job.create(
    name="goodbye_world",
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="goodbye_world",
    ),
    depends_on=hello_world,
    # default_cadence is set to DAILY in dispatchio.toml
    # cadence=DAILY,
)

JOBS = [hello_world, goodbye_world]
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""
Hello World worker functions — with @job decorators.

Decorating with @job co-locates the job definition with the function, so you
don't need a separate jobs.py for simple cases.  Run the whole file with:

    dispatchio run-file examples/hello_world/my_work.py

To run for an explicit run key (e.g. today's date or an event identifier):

    dispatchio run-file examples/hello_world/my_work.py --run-key D20260423

If a dependency isn't satisfied yet, the downstream job is held back.
Override that with --ignore-dependencies.

For complex pipelines (cross-file deps, dynamic registration, graph loading)
the explicit jobs.py approach is still available — see jobs.py in this directory.
"""

import time

from dispatchio import DAILY, job


@job(cadence=DAILY)
def hello_world(run_key: str) -> None:
    print(f"Hello, World! Running for {run_key}.")
    time.sleep(0.3)


@job(cadence=DAILY, depends_on=hello_world)
def goodbye_world(run_key: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_key}.")
    time.sleep(0.3)
```

---

## Subprocess Jobs

> Demonstrates SubprocessJob — the executor type for running external processes or scripts. Key difference from PythonJob: the worker script must call run_job() explicitly, and the job's env config must inject DISPATCHIO_JOB_RUN_KEY and DISPATCHIO_DROP_DIR so completion can be reported back. Includes a job that deliberately fails to show retry behaviour and the final ERROR state.

**Tags:** `SubprocessJob` · `retries` · `error-handling`

**Run:** `python examples/subprocess_example/run.py`

**`jobs.py`**

```python
"""
Subprocess example — demonstrates SubprocessJob.

Two jobs:
  1. generate  — runs a subprocess command, succeeds.
  2. summarize — depends on generate, always fails (shows retries + ERROR).

Key difference from PythonJob:
  SubprocessJob does not get the dispatchio harness automatically. The worker
  script must call run_job() itself, and the job's `env` config must inject
  DISPATCHIO_RUN_KEY and DISPATCHIO_DROP_DIR so run_job() can report completion
  back to the orchestrator.

  sys.executable is used instead of "python" so the subprocess inherits the
  same interpreter (and installed packages) as the orchestrator process —
  this keeps the example cross-platform without requiring "python" on PATH.

Run with:
    python examples/subprocess_example/run.py
"""

import os
import sys
from pathlib import Path

from dispatchio import Job, RetryPolicy, SubprocessJob, orchestrator

BASE = Path(__file__).parent

# Must match receiver.drop_dir in dispatchio.toml.
# Injected into each subprocess so run_job() can auto-configure FilesystemReporter.
DROP_DIR = str(BASE / ".dispatchio" / "completions")

ENV = {"DISPATCHIO_RUN_KEY": "{run_key}", "DISPATCHIO_DROP_DIR": DROP_DIR}

generate = Job.create(
    "generate",
    SubprocessJob(
        command=[sys.executable, str(BASE / "my_work.py"), "generate"], env=ENV
    ),
)
summarize = Job.create(
    "summarize",
    SubprocessJob(
        command=[sys.executable, str(BASE / "my_work.py"), "summarize"], env=ENV
    ),
    depends_on=[generate],
    retry_policy=RetryPolicy(max_attempts=2),
)

JOBS = [generate, summarize]

CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""
Worker functions for the SubprocessJob example.

Unlike PythonJob, SubprocessJob does NOT get the dispatchio harness
automatically — the worker script must call run_job() itself.

The orchestrator injects receiver configuration as env vars:
  - DISPATCHIO_CONFIG

run_job() uses these to auto-configure the reporter. You can also manually
use get_reporter() for explicit control:

    from dispatchio.completion import get_reporter
    reporter = get_reporter("my_job")
    reporter.report_success(run_key, metadata={"items": 100})

Usage (dispatched by SubprocessJob in jobs.py):
    python my_work.py generate
    python my_work.py summarize
"""

import sys

from dispatchio.worker.harness import run_job


def generate(run_key: str) -> None:
    """Simulate generating data — always succeeds."""
    items = list(range(10))
    print(f"Generated {len(items)} items for run_key={run_key}")
    # run_job() automatically reports success with metadata


def summarize(run_key: str) -> None:
    """Deliberately fail to demonstrate error handling and retries."""
    print(f"Attempting to summarize {run_key}...")
    raise RuntimeError("Data source unavailable — demonstrating error handling")
    # run_job() automatically reports error and raises SystemExit(1)


_DISPATCH = {"generate": generate, "summarize": summarize}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _DISPATCH:
        print(f"Usage: python my_work.py <{'|'.join(_DISPATCH)}>", file=sys.stderr)
        sys.exit(1)
    fn_name = sys.argv[1]
    run_job(fn_name, _DISPATCH[fn_name])
```

---

## Dependency Modes

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/dependency_modes/run.py`

**`jobs.py`**

```python
"""
Dependency Modes example.

Demonstrates ALL_FINISHED and THRESHOLD dependency modes for downstream jobs.

Jobs in this example:
  entity_a, entity_b, entity_c  — three daily entity-processing jobs (no deps)

  best_effort_collector  — depends on all three entity jobs, dependency_mode=ALL_FINISHED
    Runs once all entity jobs have finished, regardless of success or failure.
    Useful when you want to process whatever data is available.

  majority_collector  — depends on all three entity jobs, dependency_mode=THRESHOLD,
                        dependency_threshold=2
    Runs as soon as 2 of 3 entity jobs have succeeded.
    Useful when partial success is sufficient to proceed.

The run.py script seeds the state store so that entity_a=DONE, entity_b=DONE,
entity_c=ERROR (simulating a partial success scenario), then calls run_loop():
  - majority_collector: threshold=2 met (2/3 succeeded) → SUBMITTED
  - best_effort_collector: all entities are in a finished state → SUBMITTED

Run with:
  python examples/dependency_modes/run.py
"""

import os
from pathlib import Path

from dispatchio import (
    DependencyMode,
    Job,
    PythonJob,
    orchestrator,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# Three independent entity-processing jobs.
entity_a = Job.create(
    "entity_a",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_a"),
)

entity_b = Job.create(
    "entity_b",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_b"),
)

entity_c = Job.create(
    "entity_c",
    PythonJob(script=str(BASE / "my_work.py"), function="entity_c"),
)

# Runs once all entity jobs have finished — regardless of success or failure.
# ALL_FINISHED proceeds as soon as every dep is in a finished state
# (DONE, ERROR, LOST, or SKIPPED).
best_effort_collector = Job.create(
    "best_effort_collector",
    PythonJob(script=str(BASE / "my_work.py"), function="best_effort_collector"),
    depends_on=[
        entity_a,
        entity_b,
        entity_c,
    ],
    dependency_mode=DependencyMode.ALL_FINISHED,
)

# Runs as soon as 2 of 3 entity jobs have succeeded.
# THRESHOLD proceeds once ≥ dependency_threshold deps reach required_status (DONE).
majority_collector = Job.create(
    "majority_collector",
    PythonJob(script=str(BASE / "my_work.py"), function="majority_collector"),
    depends_on=[
        entity_a,
        entity_b,
        entity_c,
    ],
    dependency_mode=DependencyMode.THRESHOLD,
    dependency_threshold=2,
)

JOBS = [entity_a, entity_b, entity_c, best_effort_collector, majority_collector]
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""
Dependency modes example worker functions.

Pure Python callables — no Dispatchio imports needed.
"""

import time


def entity_a(run_key: str) -> None:
    print(f"entity_a processing complete for {run_key}.")
    time.sleep(0.2)


def entity_b(run_key: str) -> None:
    print(f"entity_b processing complete for {run_key}.")
    time.sleep(0.2)


def entity_c(run_key: str) -> None:
    print(f"entity_c processing complete for {run_key}.")
    time.sleep(0.2)


def best_effort_collector(run_key: str) -> None:
    print(
        f"best_effort_collector: all entities finished for {run_key}, collecting results."
    )
    time.sleep(0.2)


def majority_collector(run_key: str) -> None:
    print(
        f"majority_collector: threshold met for {run_key}, proceeding with majority results."
    )
    time.sleep(0.2)
```

**`run.py`**

```python
"""
Dependency Modes demo runner.

Seeds the state store to simulate a partial success scenario:
  entity_a = DONE   (succeeded)
  entity_b = DONE   (succeeded)
  entity_c = ERROR  (failed)

Then calls run_loop() with reference_time=2025-01-15 09:00 UTC.

Expected outcome:
  majority_collector   — SUBMITTED  (threshold=2 met: 2/3 succeeded)
  best_effort_collector — SUBMITTED  (ALL_FINISHED: all entities are finished)

Run with:
  python examples/dependency_modes/run.py
"""

import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from uuid import uuid4
from dispatchio.models import AttemptRecord, TriggerType
from dispatchio import Status, run_loop
from examples.dependency_modes.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

REF = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
RUN_KEY = REF.strftime("%Y%m%d")  # "20250115"


# Seed the state store to simulate entity results without running the jobs.
def _seed(job_name: str, status: Status, reason: str | None = None) -> None:
    orchestrator.state.append_attempt(
        AttemptRecord(
            job_name=job_name,
            run_key=RUN_KEY,
            attempt=0,
            correlation_id=uuid4(),
            status=status,
            reason=reason,
            trigger_type=TriggerType.SCHEDULED,
            trace={},
        )
    )


_seed("entity_a", Status.DONE)
_seed("entity_b", Status.DONE)
_seed("entity_c", Status.ERROR, reason="simulated failure")

run_loop(
    orchestrator,
    reference_time=REF,
)

print("\nAttempt history with trigger metadata:")
for attempt_record in orchestrator.state.list_attempts(run_key=RUN_KEY):
    print(
        f"- {attempt_record.job_name} "
        f"attempt={attempt_record.attempt} "
        f"status={attempt_record.status.value} "
        f"reason={attempt_record.reason} "
        f"trigger={attempt_record.trigger_type.value} "
        f"trigger_reason={attempt_record.trigger_reason}"
    )
```

**`dispatchio.toml`**

```toml
# Dispatchio configuration — dependency_modes example (local dev)

[dispatchio]
log_level = "INFO"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[dispatchio.receiver]
backend  = "filesystem"
drop_dir = ".dispatchio/completions"
```

---

## Schedule Conditions

> Gates job execution with typed conditions: TimeOfDayCondition restricts a job to a time window, DayOfWeekCondition limits it to certain weekdays, and AllOf composes multiple gates with logical AND. If a condition is not met on a given tick, the job is skipped (SKIPPED_CONDITION) and re-evaluated next tick — no state is written and no retries are consumed.

**Tags:** `conditions` · `TimeOfDayCondition` · `DayOfWeekCondition` · `AllOf` · `scheduling`

**Run:** `python examples/conditions/run.py`

**`jobs.py`**

```python
"""
Schedule Conditions example.

Demonstrates typed conditions as execution gates on daily jobs.

Each condition is evaluated against the tick's reference_time. When the
condition is not met the job reports SKIPPED_CONDITION and is re-evaluated
on the next tick — no RunRecord is written and no retry attempt is consumed.

Jobs in this example:
  daily_ingest     — no condition; runs unconditionally once per day
  morning_report   — TimeOfDayCondition: only after 08:00 UTC
  weekday_digest   — DayOfWeekCondition: Mon–Fri only
  after_hours_batch — AllOf: after 18:00 UTC AND Mon–Fri

The run_loop() call uses reference_time=2025-01-15 18:30 UTC (a Wednesday),
so all conditions are satisfied and every job completes. Try changing the
reference time to explore blocking:
  07:00 UTC       — morning_report and after_hours_batch stay blocked
  Saturday 18:30  — weekday_digest and after_hours_batch stay blocked

Run with:
  python examples/conditions/run.py
"""

import os
from datetime import time
from pathlib import Path

from dispatchio import (
    AllOf,
    DayOfWeekCondition,
    Job,
    PythonJob,
    TimeOfDayCondition,
    orchestrator,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# Unconditional — ingests source data on every tick, once per day.
daily_ingest = Job.create(
    "daily_ingest",
    PythonJob(script=str(BASE / "my_work.py"), function="daily_ingest"),
)

# Only runs after 08:00 UTC — no value in generating a report at midnight.
# Depends on daily_ingest so source data is ready first.
morning_report = Job.create(
    "morning_report",
    PythonJob(script=str(BASE / "my_work.py"), function="morning_report"),
    depends_on=daily_ingest,
    condition=TimeOfDayCondition(after=time(8, 0)),
)

# Only runs Mon–Fri — the digest is a business-day publication.
weekday_digest = Job.create(
    "weekday_digest",
    PythonJob(script=str(BASE / "my_work.py"), function="weekday_digest"),
    condition=DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),  # 0=Mon … 4=Fri
)

# Runs only after 18:00 UTC on a weekday — end-of-business batch window.
# AllOf gates combine with logical AND.
after_hours_batch = Job.create(
    "after_hours_batch",
    PythonJob(script=str(BASE / "my_work.py"), function="after_hours_batch"),
    condition=AllOf(
        conditions=[
            TimeOfDayCondition(after=time(18, 0)),
            DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
        ]
    ),
)

JOBS = [daily_ingest, morning_report, weekday_digest, after_hours_batch]
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

---

## Cadence & Cross-Cadence Dependencies

> Demonstrates typed Cadence objects — DAILY, WEEKLY, MONTHLY, YESTERDAY — and how a daily job can depend on the current month's run of a slower upstream job (cross-cadence dependency). Each job independently resolves its own run_id from the reference_time; a Dependency's cadence is independent of the depending job's cadence, enabling fine-grained synchronisation across different scheduling frequencies.

**Tags:** `cadence` · `DAILY` · `MONTHLY` · `WEEKLY` · `YESTERDAY` · `cross-cadence` · `dependencies`

**Run:** `python examples/cadence/run.py`

**`jobs.py`**

```python
"""
Cadence & Cross-Cadence Dependencies example.

Demonstrates typed Cadence objects and cross-cadence dependencies — where
a faster job (daily) must wait for a slower job (monthly) to finish for
the same logical period before it can proceed.

Each job resolves its own run_key independently from the reference_time:
  monthly_ledger  → "202501"   (current calendar month)
  daily_reconcile → "20250115" (current calendar day)
  weekly_summary  → "20250113" (Monday of current week)
  yesterday_load  → "20250114" (previous calendar day)

Cross-cadence dependency:
  daily_reconcile and weekly_summary both carry a Dependency with
  cadence=MONTHLY, so they wait for monthly_ledger/202501 to be DONE
  before they submit — regardless of their own (daily / weekly) run_keys.

YESTERDAY shorthand:
  yesterday_load uses YESTERDAY = DateCadence(frequency=DAILY, offset=-1),
  which always resolves to the previous day. Useful for "reprocess last
  night's data" patterns that must not affect the current day's run_key.

Run with:
  python examples/cadence/run.py
"""

import os
from pathlib import Path

from dispatchio import (
    DAILY,
    MONTHLY,
    WEEKLY,
    YESTERDAY,
    JobDependency,
    Job,
    PythonJob,
    orchestrator,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# One run per calendar month — run_key = "202501", "202502", …
monthly_ledger = Job.create(
    "monthly_ledger",
    PythonJob(script=str(BASE / "my_work.py"), function="monthly_ledger"),
    cadence=MONTHLY,
)

# Daily job that blocks until the current month's ledger is ready.
# This job's run_key is "20250115"; the dependency resolves separately to
# the MONTHLY run_key "202501" and waits for monthly_ledger/202501 to be DONE.
daily_reconcile = Job.create(
    "daily_reconcile",
    PythonJob(script=str(BASE / "my_work.py"), function="daily_reconcile"),
    cadence=DAILY,
    depends_on=[JobDependency(job_name="monthly_ledger", cadence=MONTHLY)],
)

# Weekly job with the same cross-cadence pattern.
weekly_summary = Job.create(
    "weekly_summary",
    PythonJob(script=str(BASE / "my_work.py"), function="weekly_summary"),
    cadence=WEEKLY,
    depends_on=[JobDependency(job_name="monthly_ledger", cadence=MONTHLY)],
)

# YESTERDAY cadence — run_key always resolves to the previous day ("20250114").
# Runs independently; no dependency on the monthly ledger.
yesterday_load = Job.create(
    "yesterday_load",
    PythonJob(script=str(BASE / "my_work.py"), function="yesterday_load"),
    cadence=YESTERDAY,
)

JOBS = [monthly_ledger, daily_reconcile, weekly_summary, yesterday_load]
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

---

## Data Store

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/data_store/run.py`

**`jobs.py`**

```python
"""Job definitions for the data_store example.

Demonstrates the DataStore pattern:
  1. discover — writes a list of entities to the DataStore.
  2. process  — waits for discover to complete, then reads the entity list.

The DataStore is configured in dispatchio.toml ([dispatchio.data_store]).
The orchestrator injects DISPATCHIO_DATA_DIR and DISPATCHIO_DATA_NAMESPACE into
worker subprocesses automatically, so workers call get_data_store() with no
extra configuration.
"""

import os
from pathlib import Path

from dispatchio import DAILY, JobDependency, Job, PythonJob, orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

discover = Job(
    name="discover",
    cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "workers.py"),
        function="discover",
    ),
)

process = Job(
    name="process",
    cadence=DAILY,
    executor=PythonJob(
        script=str(BASE / "workers.py"),
        function="process",
    ),
    depends_on=[JobDependency(job_name="discover", cadence=DAILY)],
)

orchestrator = orchestrator([discover, process], config=CONFIG_FILE)
```

**`run.py`**

```python
"""DataStore example runner.

Shows how the DataStore enables one job to pass structured data to another:
  1. discover runs first and writes a list of entities to the DataStore.
  2. process waits for discover to complete, reads the entity list, and
     processes each one.

    python examples/data_store/run.py

In production, replace run_loop() with a single tick() call:
    orchestrator.tick()
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import run_loop
from examples.data_store.jobs import orchestrator

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

run_loop(orchestrator)
```

**`dispatchio.toml`**

```toml
# Dispatchio configuration — data_store example (local dev)

[dispatchio]
log_level = "INFO"
default_cadence = "daily"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[dispatchio.receiver]
backend  = "filesystem"
drop_dir = ".dispatchio/completions"

# Enable the DataStore so workers can call get_data_store().
[dispatchio.data_store]
backend  = "filesystem"
base_dir = ".dispatchio/data"
namespace = "data-store-example"
```

---

## Custom Pool

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/custom_pool/run.py`

**`jobs.py`**

```python
"""Custom pool example.

Shows how to assign jobs to named admission pools and configure per-pool
limits in dispatchio.toml.
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

JOBS = [
    Job.create(
        "replay_high",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="replay_high"),
        pool="replay",
        priority=20,
    ),
    Job.create(
        "replay_low",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="replay_low"),
        pool="replay",
        priority=10,
    ),
    Job.create(
        "bulk_high",
        executor=PythonJob(script=str(BASE / "my_work.py"), function="bulk_high"),
        pool="bulk",
        priority=5,
    ),
]

orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""No-op worker functions used by the custom pool example."""

from __future__ import annotations


def replay_high(run_key: str) -> None:
    print(f"replay_high for {run_key}")


def replay_low(run_key: str) -> None:
    print(f"replay_low for {run_key}")


def bulk_high(run_key: str) -> None:
    print(f"bulk_high for {run_key}")
```

**`run.py`**

```python
"""Custom pool demo runner.

Run with:
  python examples/custom_pool/run.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from examples.custom_pool.jobs import orchestrator  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


if __name__ == "__main__":
    ref = datetime.now(tz=timezone.utc)

    log.info("Dry-run tick: replay pool capped at 1 submit per tick")
    result = orchestrator.tick(reference_time=ref, dry_run=True)
    for event in result.results:
        suffix = f" ({event.detail})" if event.detail else ""
        log.info(
            "  pool=%s  %s[%s] -> %s%s",
            event.pool,
            event.job_name,
            event.run_key,
            event.action.value,
            suffix,
        )
```

**`dispatchio.toml`**

```toml
log_level = "INFO"
default_cadence = "daily"

[state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[receiver]
backend = "filesystem"
drop_dir = ".dispatchio/completions"

[admission]
max_submit_jobs_per_tick = 2

[admission.pools.default]
# uncapped pool

[admission.pools.replay]
max_submit_jobs_per_tick = 1

[admission.pools.bulk]
# uncapped pool
```

---

## Event Dependencies

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/event_dependencies/run.py`

**`jobs.py`**

```python
"""
Event dependency example.

Shows two patterns:
  1. Single event dependency:
      send_welcome_email depends on user_registered

  2. Two events dependency (fan-in):
      activate_paid_features depends on user_registered AND kyc_passed
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import (
    DAILY,
    Job,
    PythonJob,
    event_dependency,
    orchestrator,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

send_welcome_email = Job.create(
    name="send_welcome_email",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="send_welcome_email"),
    depends_on=[event_dependency("user_registered", cadence=DAILY)],
)

activate_paid_features = Job.create(
    name="activate_paid_features",
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="activate_paid_features",
    ),
    depends_on=[
        event_dependency("user_registered", cadence=DAILY),
        event_dependency("kyc_passed", cadence=DAILY),
    ],
)

JOBS = [send_welcome_email, activate_paid_features]

# strict_dependencies=False allows dependencies that are not local executable jobs.
orchestrator = orchestrator(
    JOBS,
    config=CONFIG_FILE,
)
```

**`my_work.py`**

```python
"""Worker functions for the external events example."""

from __future__ import annotations

import time


def send_welcome_email(run_key: str) -> None:
    print(f"welcome email sent for run_key={run_key}")
    time.sleep(0.1)


def activate_paid_features(run_key: str) -> None:
    print(f"paid features activated for run_key={run_key}")
    time.sleep(0.1)
```

**`run.py`**

```python
"""
Event dependency demo runner.

Run with:
    python examples/event_dependencies/run.py
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio.events import emit_event  # noqa: E402
from examples.event_dependencies.jobs import orchestrator  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def _tick(label: str, reference_time: datetime) -> list[tuple[str, str]]:
    result = orchestrator.tick(reference_time=reference_time)
    rows = [(entry.job_name, entry.action.value) for entry in result.results]

    log.info(label)
    for job_name, action in rows:
        log.info("  %s -> %s", job_name, action)

    return rows


def _submitted(rows: list[tuple[str, str]], job_name: str) -> bool:
    return any(
        row_job == job_name and action in {"submitted", "retrying"}
        for row_job, action in rows
    )


def _log_attempts(run_key: str) -> None:
    attempts = orchestrator.state.list_attempts(run_key=run_key)
    log.info("Attempt history for run_key=%s", run_key)
    for record in attempts:
        log.info(
            "  %s attempt=%d status=%s trigger=%s reason=%s",
            record.job_name,
            record.attempt,
            record.status.value,
            record.trigger_type.value,
            record.trigger_reason,
        )


if __name__ == "__main__":
    reference_time = datetime(2025, 1, 15, 9, 0, tzinfo=timezone.utc)
    run_key = f"D{reference_time.strftime('%Y%m%d')}"

    # Tick 1: nothing submitted yet (no events received).
    tick1 = _tick("Tick 1 - no events", reference_time)
    assert not _submitted(tick1, "send_welcome_email")
    assert not _submitted(tick1, "activate_paid_features")

    # Emit one event.
    emit_event("user_registered", run_key)

    # Tick 2: single-event job is now unblocked; two-event job is still waiting.
    tick2 = _tick("Tick 2 - event.user_registered received", reference_time)
    assert _submitted(tick2, "send_welcome_email")
    assert not _submitted(tick2, "activate_paid_features")

    # Emit the second event required by the fan-in job.
    emit_event("kyc_passed", run_key)

    # Tick 3: two-event dependency fan-in is now satisfied.
    tick3 = _tick("Tick 3 - event.kyc_passed received", reference_time)
    assert _submitted(tick3, "activate_paid_features")

    _log_attempts(run_key)

    log.info("Event dependency demo completed successfully")
```

**`dispatchio.toml`**

```toml
# Dispatchio configuration — event_dependencies example

[dispatchio]
log_level = "INFO"
default_cadence = "daily"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[dispatchio.receiver]
backend  = "filesystem"
drop_dir = ".dispatchio/completions"
```

---

## Dynamic Registration

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/dynamic_registration/run.py`

**`jobs.py`**

```python
"""
Dynamic registration example.

This example shows an orchestrator-first flow:
  1. Build the orchestrator from config.
  2. Register jobs with add_jobs().
  3. Optionally add more jobs after ticks have already run.
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import DAILY, Job, JobDependency, PythonJob, orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

orchestrator = orchestrator(
    config=CONFIG_FILE,
    allow_runtime_mutation=True,
)


def register_bootstrap_jobs() -> None:
    """Register the initial pipeline jobs once."""
    existing = {job.name for job in orchestrator.jobs}
    if "discover" in existing and "transform" in existing:
        return

    discover_job = Job.create(
        "discover",
        PythonJob(script=str(BASE / "my_work.py"), function="discover"),
        cadence=DAILY,
    )
    transform_job = Job.create(
        "transform",
        PythonJob(script=str(BASE / "my_work.py"), function="transform"),
        cadence=DAILY,
        depends_on=[discover_job],
    )
    orchestrator.add_jobs([discover_job, transform_job])


def register_entity_jobs(entities: list[str]) -> None:
    """Register one job per entity after discovery completes."""
    existing = {job.name for job in orchestrator.jobs}
    new_jobs: list[Job] = []

    for entity in entities:
        job_name = f"process_entity_{entity}"
        if job_name in existing:
            continue
        new_jobs.append(
            Job.create(
                job_name,
                PythonJob(script=str(BASE / "my_work.py"), function=job_name),
                cadence=DAILY,
                depends_on=[JobDependency(job_name="transform", cadence=DAILY)],
            )
        )

    if new_jobs:
        orchestrator.add_jobs(new_jobs)
```

**`my_work.py`**

```python
"""
Worker functions used by the dynamic registration example.
"""

from __future__ import annotations

import time


def discover(run_key: str) -> None:
    print(f"discover finished for run_key={run_key}")
    time.sleep(0.1)


def transform(run_key: str) -> None:
    print(f"transform finished for run_key={run_key}")
    time.sleep(0.1)


def process_entity_alpha(run_key: str) -> None:
    print(f"entity alpha processed for run_key={run_key}")
    time.sleep(0.1)


def process_entity_beta(run_key: str) -> None:
    print(f"entity beta processed for run_key={run_key}")
    time.sleep(0.1)
```

**`run.py`**

```python
"""
Dynamic registration demo runner.

Run with:
  python examples/dynamic_registration/run.py
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from examples.dynamic_registration.jobs import (  # noqa: E402
    orchestrator,
    register_bootstrap_jobs,
    register_entity_jobs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


if __name__ == "__main__":
    reference_time = datetime.now(tz=timezone.utc)

    register_bootstrap_jobs()

    for tick_num in range(1, 8):
        if tick_num == 3:
            register_entity_jobs(["alpha", "beta"])
            log.info("Registered dynamic entity jobs after initial ticks")

        result = orchestrator.tick(reference_time=reference_time)
        log.info("Tick %d", tick_num)
        for event in result.results:
            suffix = f" ({event.detail})" if event.detail else ""
            log.info(
                "  %s[%s] -> %s%s",
                event.job_name,
                event.run_key,
                event.action.value,
                suffix,
            )
        time.sleep(0.3)
```

**`dispatchio.toml`**

```toml
log_level = "INFO"
default_cadence = "daily"

[state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[receiver]
backend = "filesystem"
drop_dir = ".dispatchio/completions"

[admission]
max_submit_jobs_per_tick = 10
```

---

## JSON Graph

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/json_graph/run.py`

**`run.py`**

```python
"""JSON Graph example — load a graph artifact and run it.

Demonstrates the JSON authoring mode: the graph is defined entirely in
graph.json with no Python job definitions. The runner only wires infrastructure
(state store, receiver) via dispatchio.toml.

    # Validate the artifact first (optional but recommended in CI):
    dispatchio graph validate examples/json_graph/graph.json

    # Run:
    python examples/json_graph/run.py

In production, replace run_loop() with a single tick() call driven by your
scheduler (EventBridge, cron, etc.):
    orchestrator.tick()
"""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import run_loop
from dispatchio.graph import load_graph, orchestrator_from_graph

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = Path(__file__).parent

spec = load_graph(BASE / "graph.json")
orchestrator = orchestrator_from_graph(spec, config=BASE / "dispatchio.toml")

run_loop(orchestrator)
```

**`dispatchio.toml`**

```toml
# Dispatchio configuration — json_graph example (local dev)

[dispatchio]
log_level = "INFO"
default_cadence = "daily"

[dispatchio.state]
backend = "sqlalchemy"
connection_string = "sqlite:///dispatchio.db"

[dispatchio.receiver]
backend  = "filesystem"
drop_dir = ".dispatchio/completions"
```

---

## Multi-Orchestrator

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/multi_orchestrator/run.py`

**`my_work.py`**

```python
"""Worker functions for the multi-orchestrator example."""

import time


def ingest(run_key: str) -> None:
    """Simulate daily data ingestion."""
    time.sleep(0.1)
    print(f"Ingested daily records for {run_key}.")


def transform(run_key: str) -> None:
    """Simulate daily transformation — depends on ingest."""
    time.sleep(0.1)
    print(f"Transformed daily records for {run_key}.")


def aggregate(run_key: str) -> None:
    """Simulate weekly aggregation."""
    time.sleep(0.1)
    print(f"Aggregated weekly records for {run_key}.")


def report(run_key: str) -> None:
    """Simulate weekly report generation — depends on aggregate."""
    time.sleep(0.1)
    print(f"Generated weekly report for {run_key}.")
```

**`run.py`**

```python
"""
Multi-Orchestrator example — two named orchestrators, one shared database.

  daily-etl      ingest → transform            (DAILY cadence)
  weekly-reports aggregate → report            (WEEKLY cadence)

Both orchestrators share dispatchio-example.db. Namespace isolation means
each only sees its own job state; the --all-namespaces CLI flag lets you
query across both.

Cross-namespace signalling: after daily-etl finishes, it emits a
"daily-complete" event targeting weekly-reports. The aggregate job waits
on that event before running, demonstrating deliberate cross-orchestrator
dependency without any shared job definitions.

Run:
    python examples/multi_orchestrator/run.py

Register as named contexts to query from anywhere:
    dispatchio context add daily-etl     examples/multi_orchestrator/daily.toml
    dispatchio context add weekly-reports examples/multi_orchestrator/weekly.toml
    dispatchio context use daily-etl

    dispatchio status                              # daily-etl jobs
    dispatchio status --context weekly-reports     # weekly-reports jobs
    dispatchio status --all-namespaces             # both, with NAMESPACE column
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import DAILY, WEEKLY, Job, PythonJob, orchestrator, resolve_run_key, run_loop
from dispatchio.events import event_dependency
from dispatchio.models import Event

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = Path(__file__).parent
REFERENCE_TIME = datetime(2026, 4, 14, 9, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# daily-etl — ingest then transform
# ---------------------------------------------------------------------------

ingest = Job.create(
    "ingest",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="ingest"),
)

transform = Job.create(
    "transform",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="transform"),
    depends_on=ingest,
)

daily = orchestrator(
    [ingest, transform],
    config=str(BASE / "daily.toml"),
)

# ---------------------------------------------------------------------------
# weekly-reports — waits for daily signal, then aggregate and report
# ---------------------------------------------------------------------------

# aggregate blocks until daily-etl emits "daily-complete" into this namespace.
# The run_key weekly-reports evaluates is used as-is for the event lookup, so
# the emitter below uses resolve_run_key(WEEKLY, ...) to match it exactly.
aggregate = Job.create(
    "aggregate",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="aggregate"),
    depends_on=[event_dependency("daily-complete")],
)

report = Job.create(
    "report",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="report"),
    depends_on=aggregate,
)

weekly = orchestrator(
    [aggregate, report],
    config=str(BASE / "weekly.toml"),
    strict_dependencies=False,  # EventDependency names are not in the job index
)

# ---------------------------------------------------------------------------
# Step 1 — run daily-etl to completion
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print(f"  daily-etl  (reference: {REFERENCE_TIME.date()})")
print(f"{'─' * 60}\n")
run_loop(daily, reference_time=REFERENCE_TIME, tick_interval=0.5)

# ---------------------------------------------------------------------------
# Step 2 — signal weekly-reports that daily is done
# ---------------------------------------------------------------------------

# The event run_key must match what weekly-reports will evaluate, so we
# resolve the weekly run_key from the same reference time.
weekly_run_key = resolve_run_key(WEEKLY, REFERENCE_TIME)

print(f"\n  → Emitting 'daily-complete' → weekly-reports  (run_key: {weekly_run_key})")
daily.state.set_event(
    Event(namespace="weekly-reports", name="daily-complete", run_key=weekly_run_key)
)

# ---------------------------------------------------------------------------
# Step 3 — run weekly-reports (aggregate is now unblocked by the event)
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print(f"  weekly-reports  (reference: {REFERENCE_TIME.date()})")
print(f"{'─' * 60}\n")
run_loop(
    weekly,
    reference_time=REFERENCE_TIME,
    tick_interval=0.5,
    stop_when=lambda store, jobs, _, __: all(
        (rec := store.get_latest_attempt(j.name, weekly_run_key)) and rec.is_finished()
        for j in jobs
    ),
)

# ---------------------------------------------------------------------------
# Tick log summary for both orchestrators
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print("  Tick log summary")
print(f"{'─' * 60}\n")

for orch in [daily, weekly]:
    if orch.tick_log is None:
        print(f"  {orch.namespace}: no tick log configured")
        continue
    records = orch.tick_log.list(limit=10)
    submitted_total = sum(
        sum(1 for a in r.actions if a["action"] == "submitted") for r in records
    )
    print(f"  {orch.namespace}:")
    print(f"    {len(records)} tick(s), {submitted_total} total submission(s)")
    for r in records:
        n = sum(1 for a in r.actions if a["action"] == "submitted")
        print(f"    [{r.ticked_at}]  ref={r.reference_time[:10]}  {r.duration_seconds:.2f}s  {n} submitted")

# ---------------------------------------------------------------------------
# CLI usage hints
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print("  Register as contexts and query from anywhere:")
print(f"{'─' * 60}\n")
daily_cfg = (BASE / "daily.toml").resolve()
weekly_cfg = (BASE / "weekly.toml").resolve()
print(f"    dispatchio context add daily-etl      {daily_cfg}")
print(f"    dispatchio context add weekly-reports {weekly_cfg}")
print("    dispatchio context use daily-etl")
print()
print("    dispatchio status                              # daily-etl jobs")
print("    dispatchio status --context weekly-reports     # weekly-reports jobs")
print("    dispatchio status --all-namespaces             # both, with NAMESPACE column")
print()
```

---

## AWS Lambda

> Auto-discovered example (add example.toml for richer cookbook metadata).

**Run:** `python examples/aws_lambda/run.py`

**`jobs.py`**

```python
from __future__ import annotations

import os
from pathlib import Path

from dispatchio import Job, LambdaJob
from dispatchio_aws.config import aws_orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

JOBS = [
    Job.create(
        "ingest",
        executor=LambdaJob(
            function_name=os.getenv(
                "DISPATCHIO_LAMBDA_FUNCTION_NAME", "dispatchio-ingest"
            ),
            payload_template={"run_key": "{run_key}", "job_name": "{job_name}"},
        ),
    ),
]

orchestrator = aws_orchestrator(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
from __future__ import annotations

from dispatchio_aws.worker.lambda_handler import dispatchio_handler


@dispatchio_handler(job_name="ingest")
def handler(run_key: str) -> None:
    print(f"ingest lambda processing run_key={run_key}")
```

**`run.py`**

```python
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
```

**`dispatchio.toml`**

```toml
# AWS example configuration (optional dispatchio_aws package)
#
# Environment variables should provide secrets/URLs in real deployments.

[dispatchio]
namespace = "aws-lambda-example"
log_level = "INFO"
default_cadence = "daily"

[dispatchio.state]
backend = "sqlalchemy"
# For local experimentation only. In AWS use PostgreSQL/MySQL on RDS/Aurora.
connection_string = "sqlite:///dispatchio-aws-example.db"

[dispatchio.receiver]
backend = "sqs"
# Override in env for real use.
queue_url = "https://sqs.eu-west-1.amazonaws.com/123456789012/dispatchio-completions"
region = "eu-west-1"
```
