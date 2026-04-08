"""
Cadence example worker functions.

Pure Python callables — no Dispatchio imports needed.
"""

import time


def monthly_ledger(run_id: str) -> None:
    print(f"Computing monthly ledger for {run_id}.")
    time.sleep(0.3)


def daily_reconcile(run_id: str) -> None:
    print(f"Daily reconcile for {run_id}.")
    time.sleep(0.2)


def weekly_summary(run_id: str) -> None:
    print(f"Weekly summary for {run_id}.")
    time.sleep(0.2)


def yesterday_load(run_id: str) -> None:
    print(f"Loading yesterday's data for {run_id}.")
    time.sleep(0.2)
