"""
Multi-Orchestrator example — two named orchestrators, one tick log each.

This example creates two independent orchestrators:
  - daily-etl      (ingest → transform, DAILY cadence)
  - weekly-reports (aggregate → report, WEEKLY cadence)

Each has its own config file, its own state directory, and its own tick log.
After running, you can register both as named contexts and query them from
anywhere on your machine:

    dispatchio context add daily-etl examples/multi_orchestrator/daily.toml
    dispatchio context add weekly-reports examples/multi_orchestrator/weekly.toml
    dispatchio context use daily-etl

    dispatchio ticks                          # daily-etl tick history
    dispatchio ticks --context weekly-reports # weekly-reports tick history
    dispatchio status                         # daily-etl job status
    dispatchio status --context weekly-reports

Run:
    python examples/multi_orchestrator/run.py
"""

import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parents[2]))

from dispatchio import (
    Job,
    PythonJob,
    WEEKLY,
    simulate,
    orchestrator_from_config,
    resolve_run_id,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE = Path(__file__).parent
REFERENCE_TIME = datetime(2026, 4, 14, 9, 0, 0, tzinfo=timezone.utc)

# ---------------------------------------------------------------------------
# Daily orchestrator — ingest then transform
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

daily = orchestrator_from_config(
    [ingest, transform],
    config=str(BASE / "daily.toml"),
)

# ---------------------------------------------------------------------------
# Weekly orchestrator — aggregate then report
# ---------------------------------------------------------------------------

aggregate = Job.create(
    "aggregate",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="aggregate"),
)

report = Job.create(
    "report",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="report"),
    depends_on=aggregate,
)

weekly = orchestrator_from_config(
    [aggregate, report],
    config=str(BASE / "weekly.toml"),
)

# ---------------------------------------------------------------------------
# Simulate both orchestrators
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print(f"  Orchestrator: {daily.name}  (reference: {REFERENCE_TIME.date()})")
print(f"{'─' * 60}\n")
simulate(daily, reference_time=REFERENCE_TIME, tick_interval=0.5)

print(f"\n{'─' * 60}")
print(f"  Orchestrator: {weekly.name}  (reference: {REFERENCE_TIME.date()})")
print(f"{'─' * 60}\n")
# simulate() uses a daily run_id by default; supply the correct weekly stop condition
# so it exits once both weekly jobs are done.
weekly_run_id = resolve_run_id(WEEKLY, REFERENCE_TIME)
simulate(
    weekly,
    reference_time=REFERENCE_TIME,
    tick_interval=0.5,
    stop_when=lambda store, jobs, _: all(
        (rec := store.get_latest_attempt(j.name, weekly_run_id)) and rec.is_finished()
        for j in jobs
    ),
)

# ---------------------------------------------------------------------------
# Show tick log summary for each orchestrator
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print("  Tick log summary")
print(f"{'─' * 60}\n")

for orch in [daily, weekly]:
    if orch.tick_log is None:
        print(f"  {orch.name}: no tick log configured")
        continue
    records = orch.tick_log.list(limit=10)
    submitted_total = sum(
        sum(1 for a in r.actions if a["action"] == "submitted") for r in records
    )
    print(f"  {orch.name}:")
    print(f"    {len(records)} tick(s) recorded, {submitted_total} total submission(s)")
    for r in records:
        n_submitted = sum(1 for a in r.actions if a["action"] == "submitted")
        print(
            f"    [{r.ticked_at}]  ref={r.reference_time[:10]}"
            f"  {r.duration_seconds:.2f}s  {n_submitted} submitted"
        )

# ---------------------------------------------------------------------------
# Show how to register these as contexts
# ---------------------------------------------------------------------------

print(f"\n{'─' * 60}")
print("  Register as contexts and query from anywhere:")
print(f"{'─' * 60}\n")
daily_cfg = (BASE / "daily.toml").resolve()
weekly_cfg = (BASE / "weekly.toml").resolve()
print(f"    dispatchio context add daily-etl {daily_cfg}")
print(f"    dispatchio context add weekly-reports {weekly_cfg}")
print("    dispatchio context use daily-etl")
print()
print("    dispatchio ticks                           # daily-etl history")
print("    dispatchio ticks --context weekly-reports  # weekly-reports history")
print("    dispatchio status                          # daily-etl job status")
print("    dispatchio status --context weekly-reports")
print()
