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

from dispatchio import DAILY, MONTHLY, WEEKLY, YESTERDAY, JobDependency, Job, PythonJob
from dispatchio.config import orchestrator

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
