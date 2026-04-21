"""
Conditions example worker functions.

Pure Python callables — no Dispatchio imports needed.
"""

import time


def daily_ingest(run_key: str) -> None:
    print(f"Ingesting source data for {run_key}.")
    time.sleep(0.2)


def morning_report(run_key: str) -> None:
    print(f"Morning report ready for {run_key}.")
    time.sleep(0.2)


def weekday_digest(run_key: str) -> None:
    print(f"Weekday digest for {run_key}.")
    time.sleep(0.2)


def after_hours_batch(run_key: str) -> None:
    print(f"After-hours batch complete for {run_key}.")
    time.sleep(0.2)
