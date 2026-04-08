# Dispatchio Cookbook

A collection of runnable examples, each in its own directory under
[`examples/`](examples/). To run any example:

```
python examples/<name>/run.py
```

> **Production note:** `run.py` uses `simulate()` to drive multiple ticks
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
Hello World — Two Jobs and a Dependency.

Two jobs:
  1. hello_world   — runs immediately, prints a greeting.
  2. goodbye_world — runs after hello_world is done for the same day.

The Job class creates two jobs, and a dependency between them. 

Configuration is loaded from dispatchio.toml in this directory.
For example, default_cadence is set to DAILY so it doesn't have to be specified in the Job definitions.

Run with:
  python examples/hello_world/run.py
"""
import os
from pathlib import Path

from dispatchio import Job, PythonJob, orchestrator_from_config

BASE        = Path(__file__).parent
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
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""
Hello World worker functions.

Pure Python callables — no Dispatchio imports needed.
`dispatchio run` handles the job lifecycle (run_id resolution, completion events).

In a real project each job would typically live in its own module or be part
of an installed package. Here both functions share one file for simplicity.
"""

import time


def hello_world(run_id: str) -> None:
    print(f"Hello, World! Running for {run_id}.")
    time.sleep(0.3)


def goodbye_world(run_id: str) -> None:
    print(f"Goodbye, World! Wrapping up {run_id}.")
    time.sleep(0.3)
```

---

## Subprocess Jobs

> Demonstrates SubprocessJob — the executor type for running external processes or scripts. Key difference from PythonJob: the worker script must call run_job() explicitly, and the job's env config must inject DISPATCHIO_RUN_ID and DISPATCHIO_DROP_DIR so completion can be reported back. Includes a job that deliberately fails to show retry behaviour and the final ERROR state.

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
  DISPATCHIO_RUN_ID and DISPATCHIO_DROP_DIR so run_job() can report completion
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

from dispatchio import DAILY, Job, RetryPolicy, SubprocessJob, orchestrator_from_config

BASE = Path(__file__).parent

# Must match receiver.drop_dir in dispatchio.toml.
# Injected into each subprocess so run_job() can auto-configure FilesystemReporter.
_DROP_DIR = str(BASE / ".dispatchio" / "completions")

_ENV = {"DISPATCHIO_RUN_ID": "{run_id}", "DISPATCHIO_DROP_DIR": _DROP_DIR}

_generate = Job.create(
    "generate",
    SubprocessJob(command=[sys.executable, str(BASE / "my_work.py"), "generate"], env=_ENV),
    cadence=DAILY,
)
_summarize = Job.create(
    "summarize",
    SubprocessJob(command=[sys.executable, str(BASE / "my_work.py"), "summarize"], env=_ENV),
    cadence=DAILY,
    depends_on=[_generate],
    retry_policy=RetryPolicy(max_attempts=2),
)

JOBS = [_generate, _summarize]

CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)
```

**`my_work.py`**

```python
"""
Worker functions for the SubprocessJob example.

Unlike PythonJob, SubprocessJob does NOT get the dispatchio harness
automatically — the worker script must call run_job() itself.

The executor injects DISPATCHIO_RUN_ID and DISPATCHIO_DROP_DIR as env vars
(see jobs.py); run_job() picks them up and handles DONE/ERROR reporting.

Usage (dispatched by SubprocessJob in jobs.py):
    python my_work.py generate
    python my_work.py summarize
"""
import sys

from dispatchio.worker.harness import run_job


def generate(run_id: str) -> None:
    """Simulate generating data — always succeeds."""
    items = list(range(10))
    print(f"Generated {len(items)} items for run_id={run_id}")


def summarize(run_id: str) -> None:
    """Deliberately fail to demonstrate error handling and retries."""
    print(f"Attempting to summarize {run_id}...")
    raise RuntimeError("Data source unavailable — demonstrating error handling")


_DISPATCH = {"generate": generate, "summarize": summarize}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in _DISPATCH:
        print(f"Usage: python my_work.py <{'|'.join(_DISPATCH)}>", file=sys.stderr)
        sys.exit(1)
    fn_name = sys.argv[1]
    run_job(fn_name, _DISPATCH[fn_name])
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

The simulate() call uses reference_time=2025-01-15 18:30 UTC (a Wednesday),
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
    DAILY,
    AllOf,
    DayOfWeekCondition,
    Job,
    PythonJob,
    TimeOfDayCondition,
    orchestrator_from_config,
)

BASE        = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# Unconditional — ingests source data on every tick, once per day.
daily_ingest = Job.create(
    "daily_ingest",
    PythonJob(script=str(BASE / "my_work.py"), function="daily_ingest"),
    cadence=DAILY,
)

# Only runs after 08:00 UTC — no value in generating a report at midnight.
# Depends on daily_ingest so source data is ready first.
morning_report = Job.create(
    "morning_report",
    PythonJob(script=str(BASE / "my_work.py"), function="morning_report"),
    cadence=DAILY,
    depends_on=daily_ingest,
    condition=TimeOfDayCondition(after=time(8, 0)),
)

# Only runs Mon–Fri — the digest is a business-day publication.
weekday_digest = Job.create(
    "weekday_digest",
    PythonJob(script=str(BASE / "my_work.py"), function="weekday_digest"),
    cadence=DAILY,
    condition=DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),  # 0=Mon … 4=Fri
)

# Runs only after 18:00 UTC on a weekday — end-of-business batch window.
# AllOf gates combine with logical AND.
after_hours_batch = Job.create(
    "after_hours_batch",
    PythonJob(script=str(BASE / "my_work.py"), function="after_hours_batch"),
    cadence=DAILY,
    condition=AllOf(conditions=[
        TimeOfDayCondition(after=time(18, 0)),
        DayOfWeekCondition(on_days=[0, 1, 2, 3, 4]),
    ]),
)

JOBS = [daily_ingest, morning_report, weekday_digest, after_hours_batch]
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)
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

Each job resolves its own run_id independently from the reference_time:
  monthly_ledger  → "202501"   (current calendar month)
  daily_reconcile → "20250115" (current calendar day)
  weekly_summary  → "20250113" (Monday of current week)
  yesterday_load  → "20250114" (previous calendar day)

Cross-cadence dependency:
  daily_reconcile and weekly_summary both carry a Dependency with
  cadence=MONTHLY, so they wait for monthly_ledger/202501 to be DONE
  before they submit — regardless of their own (daily / weekly) run_ids.

YESTERDAY shorthand:
  yesterday_load uses YESTERDAY = DateCadence(frequency=DAILY, offset=-1),
  which always resolves to the previous day. Useful for "reprocess last
  night's data" patterns that must not affect the current day's run_id.

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
    Dependency,
    Job,
    PythonJob,
    orchestrator_from_config,
)

BASE        = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# One run per calendar month — run_id = "202501", "202502", …
monthly_ledger = Job.create(
    "monthly_ledger",
    PythonJob(script=str(BASE / "my_work.py"), function="monthly_ledger"),
    cadence=MONTHLY,
)

# Daily job that blocks until the current month's ledger is ready.
# This job's run_id is "20250115"; the dependency resolves separately to
# the MONTHLY run_id "202501" and waits for monthly_ledger/202501 to be DONE.
daily_reconcile = Job.create(
    "daily_reconcile",
    PythonJob(script=str(BASE / "my_work.py"), function="daily_reconcile"),
    cadence=DAILY,
    depends_on=[Dependency(job_name="monthly_ledger", cadence=MONTHLY)],
)

# Weekly job with the same cross-cadence pattern.
weekly_summary = Job.create(
    "weekly_summary",
    PythonJob(script=str(BASE / "my_work.py"), function="weekly_summary"),
    cadence=WEEKLY,
    depends_on=[Dependency(job_name="monthly_ledger", cadence=MONTHLY)],
)

# YESTERDAY cadence — run_id always resolves to the previous day ("20250114").
# Runs independently; no dependency on the monthly ledger.
yesterday_load = Job.create(
    "yesterday_load",
    PythonJob(script=str(BASE / "my_work.py"), function="yesterday_load"),
    cadence=YESTERDAY,
)

JOBS = [monthly_ledger, daily_reconcile, weekly_summary, yesterday_load]
orchestrator = orchestrator_from_config(JOBS, config=CONFIG_FILE)
```
