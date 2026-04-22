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
            depends_on=[JobDependency(job_name="ingest", cadence=DAILY)],
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
from dispatchio.events import EventDependency, event_dependency
from dispatchio.models import (
    AdmissionPolicy,
    AthenaJob,
    AlertCondition,
    AlertOn,
    Dependency,
    DependencyMode,
    HttpJob,
    Job,
    JobDependency,
    LambdaJob,
    PythonJob,
    RetryPolicy,
    PoolPolicy,
    Status,
    StepFunctionJob,
    SubprocessJob,
    TickResult,
)
from dispatchio.orchestrator import Orchestrator
from dispatchio.run_key_resolution import resolve_run_key
from dispatchio.state import SQLAlchemyStateStore
from dispatchio.executor import SubprocessExecutor, PythonJobExecutor
from dispatchio.receiver import FilesystemReceiver
from dispatchio.config import DispatchioSettings, load_config, orchestrator
from dispatchio.reporter import (
    Reporter,
    build_reporter,
    get_reporter,
)
from dispatchio.run_loop import run_loop
from dispatchio.tick_log import FilesystemTickLogStore, TickLogRecord, TickLogStore
from dispatchio.contexts import ContextEntry, ContextStore
from dispatchio.graph import (
    GraphExternalDependency,
    GraphSpec,
    GraphValidationError,
    ProducerInfo,
    dump_schema,
    load_graph,
    orchestrator_from_graph,
    validate_graph,
)
from dispatchio.datastore import (
    DataStore,
    FilesystemDataStore,
    MemoryDataStore,
    dispatchio_read_results,
    dispatchio_write_results,
    get_data_store,
)


def local_orchestrator(
    jobs: list[Job],
    base_dir: str | Path = Path(".dispatchio"),
    name: str = "default",
    data_store: "DataStore | None" = None,
    **orchestrator_kwargs,
) -> Orchestrator:
    """
    Create an Orchestrator wired for local use with filesystem-backed state,
    subprocess and python executors, and a file-drop status receiver.

    Directory layout under base_dir:
        state/          AttemptRecord JSON files
        events/         Status event drop directory
        tick_log.jsonl  Append-only tick audit log

    Args:
        jobs:               List of Jobs to evaluate each tick.
        base_dir:           Root directory for state and status events.
                            Created if it doesn't exist. Defaults to .dispatchio/
        name:               Orchestrator name, used in tick log and context registry.
        data_store:         Optional DataStore for inter-job data passing.
                            If provided, workers can call get_data_store() to
                            read and write structured values.
        **orchestrator_kwargs:
                            Forwarded to Orchestrator (e.g. alert_handler=...).
    """
    from dispatchio.tick_log import FilesystemTickLogStore

    # TODO DISPATCHIO_CONFIG

    base = Path(base_dir)
    events = base / "events"
    db_path = base / "dispatchio.db"
    return Orchestrator(
        jobs=jobs,
        name=name,
        state=SQLAlchemyStateStore(f"sqlite:///{db_path}"),
        executors={
            "subprocess": SubprocessExecutor(),
            "python": PythonJobExecutor(),
        },
        receiver=FilesystemReceiver(events),
        tick_log=FilesystemTickLogStore(base / "tick_log.jsonl"),
        data_store=data_store,
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
    # Event dependency helpers
    "EventDependency",
    "event_dependency",
    # Models
    "AthenaJob",
    "AdmissionPolicy",
    "AlertCondition",
    "AlertOn",
    "Dependency",
    "DependencyMode",
    "HttpJob",
    "Job",
    "JobDependency",
    "LambdaJob",
    "PythonJob",
    "RetryPolicy",
    "PoolPolicy",
    "Status",
    "StepFunctionJob",
    "SubprocessJob",
    "TickResult",
    # Orchestrator
    "Orchestrator",
    "local_orchestrator",
    # Common concrete classes (no submodule import needed for basic use)
    "SQLAlchemyStateStore",
    "SubprocessExecutor",
    "PythonJobExecutor",
    "FilesystemReceiver",
    # Config
    "DispatchioSettings",
    "load_config",
    "orchestrator",
    # Reporters
    "Reporter",
    "get_reporter",
    "build_reporter",
    # Utilities
    "resolve_run_key",
    # Development / demos
    "run_loop",
    # Tick log
    "FilesystemTickLogStore",
    "TickLogRecord",
    "TickLogStore",
    # Contexts
    "ContextEntry",
    "ContextStore",
    # Graph (JSON artifact loading)
    "GraphExternalDependency",
    "GraphSpec",
    "GraphValidationError",
    "ProducerInfo",
    "dump_schema",
    "load_graph",
    "orchestrator_from_graph",
    "validate_graph",
    # DataStore (inter-job data passing)
    "DataStore",
    "FilesystemDataStore",
    "MemoryDataStore",
    "dispatchio_read_results",
    "dispatchio_write_results",
    "get_data_store",
]
