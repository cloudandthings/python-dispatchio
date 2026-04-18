"""Worker functions for the external events example."""

from __future__ import annotations

import time


def send_welcome_email(run_id: str) -> None:
    print(f"welcome email sent for run_id={run_id}")
    time.sleep(0.1)


def activate_paid_features(run_id: str) -> None:
    print(f"paid features activated for run_id={run_id}")
    time.sleep(0.1)
