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

from dispatchio import WEEKLY, Job, PythonJob, orchestrator, resolve_run_key, run_loop
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
        print(
            f"    [{r.ticked_at}]  ref={r.reference_time[:10]}  {r.duration_seconds:.2f}s  {n} submitted"
        )

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
print(
    "    dispatchio status --all-namespaces             # both, with NAMESPACE column"
)
print()
