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
