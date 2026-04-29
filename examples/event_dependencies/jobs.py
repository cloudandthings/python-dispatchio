"""
Event dependency example.

Shows two patterns:
  1. Single event dependency:
      send_welcome_email depends on user_registered

  2. Two events dependency (fan-in):
      activate_paid_features depends on user_registered AND kyc_passed
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import DAILY, Job, PythonJob, event_dependency
from dispatchio.config import orchestrator

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

send_welcome_email = Job.create(
    name="send_welcome_email",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="send_welcome_email"),
    depends_on=[event_dependency("user_registered", cadence=DAILY)],
)

activate_paid_features = Job.create(
    name="activate_paid_features",
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="activate_paid_features",
    ),
    depends_on=[
        event_dependency("user_registered", cadence=DAILY),
        event_dependency("kyc_passed", cadence=DAILY),
    ],
)

JOBS = [send_welcome_email, activate_paid_features]

# strict_dependencies=False allows dependencies that are not local executable jobs.
orchestrator = orchestrator(
    JOBS,
    config=CONFIG_FILE,
)
