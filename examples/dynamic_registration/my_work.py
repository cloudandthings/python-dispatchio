"""
Worker functions used by the dynamic registration example.
"""

from __future__ import annotations

import time


def discover(run_id: str) -> None:
    print(f"discover finished for run_id={run_id}")
    time.sleep(0.1)


def transform(run_id: str) -> None:
    print(f"transform finished for run_id={run_id}")
    time.sleep(0.1)


def process_entity_alpha(run_id: str) -> None:
    print(f"entity alpha processed for run_id={run_id}")
    time.sleep(0.1)


def process_entity_beta(run_id: str) -> None:
    print(f"entity beta processed for run_id={run_id}")
    time.sleep(0.1)
