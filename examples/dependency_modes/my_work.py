"""
Dependency modes example worker functions.

Pure Python callables — no Dispatchio imports needed.
"""

import time


def entity_a(run_id: str) -> None:
    print(f"entity_a processing complete for {run_id}.")
    time.sleep(0.2)


def entity_b(run_id: str) -> None:
    print(f"entity_b processing complete for {run_id}.")
    time.sleep(0.2)


def entity_c(run_id: str) -> None:
    print(f"entity_c processing complete for {run_id}.")
    time.sleep(0.2)


def best_effort_collector(run_id: str) -> None:
    print(
        f"best_effort_collector: all entities finished for {run_id}, collecting results."
    )
    time.sleep(0.2)


def majority_collector(run_id: str) -> None:
    print(
        f"majority_collector: threshold met for {run_id}, proceeding with majority results."
    )
    time.sleep(0.2)
