"""
Dispatchio — lightweight, tick-based batch job orchestrator.

Quick start (local):
    from dispatchio import Job, PythonJob, Dependency, DAILY, local_orchestrator
    from pathlib import Path

    JOBS = [
        Job(
            name="ingest",
            cadence=DAILY,
            executor=PythonJob(entry_point="myproject.jobs:run_ingest"),
        ),
        Job(
            name="transform",
            cadence=DAILY,
            depends_on=[Dependency(job_name="ingest", cadence=DAILY)],
            executor=PythonJob(entry_point="myproject.jobs:run_transform"),
        ),
    ]

    orchestrator = local_orchestrator(JOBS, base_dir=Path("/var/dispatchio"))
    orchestrator.tick()
"""

from pathlib import Path

from dispatchio.cadence import (
    Cadence,
    DateCadence,
    FixedCadence,
    Frequency,
    IncrementalCadence,
    # Constants
    DAILY,
    HOURLY,
    LAST_MONTH,
    LAST_WEEK,
    MONTHLY,
    WEEKLY,
    YESTERDAY,
)
from dispatchio.conditions import (
    AllOf,
    AnyCondition,
    AnyOf,
    Condition,
    DayOfMonthCondition,
    DayOfWeekCondition,
    MinuteOfHourCondition,
    TimeOfDayCondition,
)
from dispatchio.models import (
    AlertCondition,
    AlertOn,
    Dependency,
    DependencyMode,
    HeartbeatPolicy,
    HttpJob,
    Job,
    PythonJob,
    RetryPolicy,
    RunRecord,
    Status,
    SubprocessJob,
    TickResult,
)
from dispatchio.orchestrator import Orchestrator
from dispatchio.run_id import resolve_run_id
from dispatchio.state import FilesystemStateStore, MemoryStateStore
from dispatchio.executor import SubprocessExecutor, PythonJobExecutor
from dispatchio.receiver import FilesystemReceiver
from dispatchio.config import DispatchioSettings, load_config, orchestrator_from_config
from dispatchio.simulate import simulate


def local_orchestrator(
    jobs: list[Job],
    base_dir: str | Path = Path(".dispatchio"),
    **orchestrator_kwargs,
) -> Orchestrator:
    """
    Create an Orchestrator wired for local use with filesystem-backed state,
    subprocess and python executors, and a file-drop completion receiver.

    Directory layout under base_dir:
        state/          RunRecord JSON files
        completions/    Completion event drop directory

    Args:
        jobs:               List of Jobs to evaluate each tick.
        base_dir:           Root directory for state and completions.
                            Created if it doesn't exist. Defaults to .dispatchio/
        **orchestrator_kwargs:
                            Forwarded to Orchestrator (e.g. alert_handler=...).
    """
    base = Path(base_dir)
    completions = base / "completions"
    return Orchestrator(
        jobs=jobs,
        state=FilesystemStateStore(base / "state"),
        executors={
            "subprocess": SubprocessExecutor(),
            "python":     PythonJobExecutor(reporter_env={"DISPATCHIO_DROP_DIR": str(completions)}),
        },
        receiver=FilesystemReceiver(completions),
        **orchestrator_kwargs,
    )


__all__ = [
    # Cadence
    "Cadence",
    "DateCadence",
    "FixedCadence",
    "Frequency",
    "IncrementalCadence",
    "DAILY",
    "HOURLY",
    "LAST_MONTH",
    "LAST_WEEK",
    "MONTHLY",
    "WEEKLY",
    "YESTERDAY",
    # Conditions
    "AllOf",
    "AnyCondition",
    "AnyOf",
    "Condition",
    "DayOfMonthCondition",
    "DayOfWeekCondition",
    "MinuteOfHourCondition",
    "TimeOfDayCondition",
    # Models
    "AlertCondition",
    "AlertOn",
    "Dependency",
    "DependencyMode",
    "HeartbeatPolicy",
    "HttpJob",
    "Job",
    "PythonJob",
    "RetryPolicy",
    "RunRecord",
    "Status",
    "SubprocessJob",
    "TickResult",
    # Orchestrator
    "Orchestrator",
    "local_orchestrator",
    # Common concrete classes (no submodule import needed for basic use)
    "FilesystemStateStore",
    "MemoryStateStore",
    "SubprocessExecutor",
    "PythonJobExecutor",
    "FilesystemReceiver",
    # Config
    "DispatchioSettings",
    "load_config",
    "orchestrator_from_config",
    # Utilities
    "resolve_run_id",
    # Development / demos
    "simulate",
]
