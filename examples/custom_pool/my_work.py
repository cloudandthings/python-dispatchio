"""No-op worker functions used by the custom pool example."""

from __future__ import annotations


def replay_high(run_key: str) -> None:
    print(f"replay_high for {run_key}")


def replay_low(run_key: str) -> None:
    print(f"replay_low for {run_key}")


def bulk_high(run_key: str) -> None:
    print(f"bulk_high for {run_key}")
