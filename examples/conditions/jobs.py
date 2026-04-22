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
