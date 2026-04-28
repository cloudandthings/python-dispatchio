"""
Parametrized Runs and DateContext example.

Demonstrates RunContext, RunSpec, and DateContext date variables — using a
month-to-date AWS Athena query job.

The job queries a partitioned events table over a date range. The date range
depends on whether the job is running normally or as part of a backfill:

  Normal daily mode:
    - Always queries from the first of the current month up to today (month-to-date).
    - On days 1–5 of a new month, also runs a full previous-month query to process
      any late-arriving data.

  Backfill mode:
    - A single full-month query is run, keyed to the month (e.g. M202601).
    - resolve_run_key(MONTHLY, rc.reference_time) produces the same key for every
      day in January, so backfill-enqueue can iterate daily reference times and
      state idempotency ensures only one submission per month is made.

Run keys produced:

  Normal (any day except days 1–5):
    D20260415:current_month

  Normal (days 1–5 — overlap window active):
    D20260503:current_month
    D20260503:previous_month

  Backfill (one per month regardless of how many days are iterated):
    M202601
    M202602
    ...

CLI usage:

  # Plan a full-year backfill before committing
  dispatchio backfill-plan \\
      --job athena_daily_events \\
      --start 2025-01-01 --end 2025-12-31 \\
      --order asc

  # Enqueue — 12 monthly runs, warns and skips any already DONE
  dispatchio backfill-enqueue \\
      --job athena_daily_events \\
      --start 2025-01-01 --end 2025-12-31 \\
      --order asc --submitted-by bjorn --reason "initial backfill"

  # One-off manual correction with explicit dates, bypassing the runs callable
  dispatchio dispatch \\
      --job athena_daily_events \\
      --variant q1_correction \\
      --param start_date=2025-01-01 \\
      --param end_date=2025-03-31 \\
      --run-date 2025-03-31 \\
      --submitted-by bjorn \\
      --reason "restate Q1 after source fix"
"""

import os
from pathlib import Path

from dispatchio import (
    DAILY,
    MONTHLY,
    Job,
    RunContext,
    RunSpec,
    orchestrator,
    resolve_run_key,
)
from dispatchio_aws import AthenaJob

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

# The query accepts two date parameters injected at submission time.
EVENTS_QUERY = """
    SELECT
        event_date,
        event_type,
        count(*)        AS event_count,
        count(DISTINCT user_id) AS unique_users
    FROM events_db.raw_events
    WHERE event_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
    GROUP BY 1, 2
    ORDER BY 1, 2
"""


def athena_runs(rc: RunContext) -> list[RunSpec]:
    if rc.is_backfill:
        # Backfill mode: return a run_key keyed to the month rather than the day.
        # resolve_run_key(MONTHLY, ...) produces "M202601" for any day in January,
        # so backfill-enqueue can iterate daily reference times across a full year
        # and state idempotency ensures only one submission fires per month.
        return [
            RunSpec(
                run_key=resolve_run_key(MONTHLY, rc.reference_time),
                params={
                    # Dates are dash-delimited (yyyymmddD) to match Athena's DATE literal syntax.
                    "start_date": rc.dates["mon0_first_yyyymmddD"],
                    "end_date": rc.dates["mon0_last_yyyymmddD"],
                },
            )
        ]

    # Normal daily mode: always include a month-to-date run for the current month.
    runs = [
        RunSpec(
            variant="current_month",
            params={
                # First of the current month to today gives the month-to-date window.
                "start_date": rc.dates["mon0_first_yyyymmddD"],
                "end_date": rc.dates["day0_yyyymmddD"],
            },
        )
    ]

    # Overlap window check: compare the month of today against the month of five
    # days ago. If they differ, today is within the first five days of a new month,
    # meaning some late data from last month may still be arriving. Add a full
    # previous-month run to cover it.
    #
    # Example: today = 2026-05-03
    #   day0_yyyymm = "202605"
    #   day5_yyyymm = "202604"  ← different, so we are in the overlap window
    #
    # Example: today = 2026-05-08
    #   day0_yyyymm = "202605"
    #   day5_yyyymm = "202605"  ← same, overlap window has closed
    if rc.dates["day0_yyyymm"] != rc.dates["day5_yyyymm"]:
        runs.append(
            RunSpec(
                variant="previous_month",
                params={
                    "start_date": rc.dates["mon1_first_yyyymmddD"],
                    "end_date": rc.dates["mon1_last_yyyymmddD"],
                },
            )
        )

    return runs


athena_daily_events = Job.create(
    "athena_daily_events",
    AthenaJob(
        query_string=EVENTS_QUERY,
        database="events_db",
        output_location="s3://my-bucket/athena-results/",
    ),
    cadence=DAILY,
    runs=athena_runs,
)

JOBS = [athena_daily_events]
orchestrator = orchestrator(JOBS, config=CONFIG_FILE)
