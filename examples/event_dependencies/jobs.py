"""
Event dependency example.

Shows two patterns:
  1. Single event dependency:
      send_welcome_email depends on event.user_registered

  2. Two events dependency (fan-in):
      activate_paid_features depends on event.user_registered AND event.kyc_passed

Event dependency names are validation-only entries in EVENT_DEPENDENCIES.
They are not executable jobs.
"""

from __future__ import annotations

import os
from pathlib import Path

from dispatchio import (
    DAILY,
    EventDependencySpec,
    Job,
    PythonJob,
    event_dependency,
    orchestrator_from_config,
    validate_event_dependencies,
)

BASE = Path(__file__).parent
CONFIG_FILE = os.getenv("DISPATCHIO_CONFIG", str(BASE / "dispatchio.toml"))

EVENT_DEPENDENCIES: tuple[EventDependencySpec, ...] = (
    EventDependencySpec(
        name="event.user_registered",
        cadence="daily (YYYYMMDD)",
        description="User registration event from identity platform",
    ),
    EventDependencySpec(
        name="event.kyc_passed",
        cadence="daily (YYYYMMDD)",
        description="KYC approval event from compliance system",
    ),
)


send_welcome_email = Job.create(
    name="send_welcome_email",
    executor=PythonJob(script=str(BASE / "my_work.py"), function="send_welcome_email"),
    depends_on=[event_dependency("event.user_registered", cadence=DAILY)],
)

activate_paid_features = Job.create(
    name="activate_paid_features",
    executor=PythonJob(
        script=str(BASE / "my_work.py"),
        function="activate_paid_features",
    ),
    depends_on=[
        event_dependency("event.user_registered", cadence=DAILY),
        event_dependency("event.kyc_passed", cadence=DAILY),
    ],
)

JOBS = [send_welcome_email, activate_paid_features]
validate_event_dependencies(JOBS, EVENT_DEPENDENCIES)

# strict_dependencies=False allows dependencies that are not local executable jobs.
orchestrator = orchestrator_from_config(
    JOBS,
    config=CONFIG_FILE,
    strict_dependencies=False,
)
