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
from dispatchio.tick_log import FilesystemTickLogStore, TickLogRecord, TickLogStore
from dispatchio.contexts import ContextEntry, ContextStore


def local_orchestrator(
    jobs: list[Job],
    base_dir: str | Path = Path(".dispatchio"),
    name: str = "default",
    **orchestrator_kwargs,
) -> Orchestrator:
    """
    Create an Orchestrator wired for local use with filesystem-backed state,
    subprocess and python executors, and a file-drop completion receiver.

    Directory layout under base_dir:
        state/          RunRecord JSON files
        completions/    Completion event drop directory
        tick_log.jsonl  Append-only tick audit log

    Args:
        jobs:               List of Jobs to evaluate each tick.
        base_dir:           Root directory for state and completions.
                            Created if it doesn't exist. Defaults to .dispatchio/
        name:               Orchestrator name, used in tick log and context registry.
        **orchestrator_kwargs:
                            Forwarded to Orchestrator (e.g. alert_handler=...).
    """
    from dispatchio.tick_log import FilesystemTickLogStore

    base = Path(base_dir)
    completions = base / "completions"
    return Orchestrator(
        jobs=jobs,
        name=name,
        state=FilesystemStateStore(base / "state"),
        executors={
            "subprocess": SubprocessExecutor(),
            "python": PythonJobExecutor(
                reporter_env={"DISPATCHIO_DROP_DIR": str(completions)}
            ),
        },
        receiver=FilesystemReceiver(completions),
        tick_log=FilesystemTickLogStore(base / "tick_log.jsonl"),
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
    # Tick log
    "FilesystemTickLogStore",
    "TickLogRecord",
    "TickLogStore",
    # Contexts
    "ContextEntry",
    "ContextStore",
]
