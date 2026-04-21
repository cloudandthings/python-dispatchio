"""Worker functions for the external events example."""

from __future__ import annotations

import time


def send_welcome_email(run_key: str) -> None:
    print(f"welcome email sent for run_key={run_key}")
    time.sleep(0.1)


def activate_paid_features(run_key: str) -> None:
    print(f"paid features activated for run_key={run_key}")
    time.sleep(0.1)
