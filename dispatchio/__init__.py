"""
Dispatchio — lightweight, tick-based batch job orchestrator.
"""

# ---------------------------------------------------------------------------
# Eager imports — job authoring primitives (light, no heavy deps)
# ---------------------------------------------------------------------------

from dispatchio.cadence import (
    Cadence,
    DateCadence,
    LiteralCadence,
    Frequency,
    IncrementalCadence,
    # Constants
    ANNUALLY,
    DAILY,
    HOURLY,
    LAST_MONTH,
    LAST_WEEK,
    MONTHLY,
    QUARTERLY,
    WEEKLY,
    YESTERDAY,
)
from dispatchio.date_context import DateContext
from dispatchio.runs import RunContext, RunSpec
from dispatchio.decorator import job
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
from dispatchio.run_key_resolution import resolve_run_key
from dispatchio.config import DispatchioSettings, load_config
# orchestrator() and run_loop() share names with submodules and cannot be
# exported from this namespace without shadowing those modules.
# Import them directly:
#   from dispatchio.config import orchestrator
#   from dispatchio.run_loop import run_loop

# ---------------------------------------------------------------------------
# Lazy imports — runtime / orchestrator setup (heavy deps loaded on demand)
# ---------------------------------------------------------------------------

# Maps public name -> (module_path, attribute)
_LAZY: dict[str, tuple[str, str]] = {
    # Orchestrator
    "Orchestrator":         ("dispatchio.orchestrator",     "Orchestrator"),
    # State
    "SQLAlchemyStateStore": ("dispatchio.state",            "SQLAlchemyStateStore"),
    # Executors
    "SubprocessExecutor":   ("dispatchio.executor",         "SubprocessExecutor"),
    "PythonJobExecutor":    ("dispatchio.executor",         "PythonJobExecutor"),
    # Receiver
    "FilesystemReceiver":   ("dispatchio.receiver",         "FilesystemReceiver"),
    # Reporter
    "Reporter":             ("dispatchio.reporter",         "Reporter"),
    "build_reporter":       ("dispatchio.reporter",         "build_reporter"),
    "get_reporter":         ("dispatchio.reporter",         "get_reporter"),
    # Tick log
    "FilesystemTickLogStore": ("dispatchio.tick_log",       "FilesystemTickLogStore"),
    "TickLogRecord":        ("dispatchio.tick_log",         "TickLogRecord"),
    "TickLogStore":         ("dispatchio.tick_log",         "TickLogStore"),
    # Contexts
    "ContextEntry":         ("dispatchio.contexts",         "ContextEntry"),
    "ContextStore":         ("dispatchio.contexts",         "ContextStore"),
    # Graph
    "GraphExternalDependency": ("dispatchio.graph",         "GraphExternalDependency"),
    "GraphSpec":            ("dispatchio.graph",            "GraphSpec"),
    "GraphValidationError": ("dispatchio.graph",            "GraphValidationError"),
    "ProducerInfo":         ("dispatchio.graph",            "ProducerInfo"),
    "dump_schema":          ("dispatchio.graph",            "dump_schema"),
    "load_graph":           ("dispatchio.graph",            "load_graph"),
    "orchestrator_from_graph": ("dispatchio.graph",         "orchestrator_from_graph"),
    "validate_graph":       ("dispatchio.graph",            "validate_graph"),
    # DataStore
    "DataStore":            ("dispatchio.datastore",        "DataStore"),
    "FilesystemDataStore":  ("dispatchio.datastore",        "FilesystemDataStore"),
    "MemoryDataStore":      ("dispatchio.datastore",        "MemoryDataStore"),
    "dispatchio_read_results":  ("dispatchio.datastore",    "dispatchio_read_results"),
    "dispatchio_write_results": ("dispatchio.datastore",    "dispatchio_write_results"),
    "get_data_store":       ("dispatchio.datastore",        "get_data_store"),
}


def __getattr__(name: str):
    entry = _LAZY.get(name)
    if entry is not None:
        import importlib
        mod = importlib.import_module(entry[0])
        return getattr(mod, entry[1])
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Cadence
    "Cadence",
    "DateCadence",
    "LiteralCadence",
    "Frequency",
    "IncrementalCadence",
    # Decorator
    "job",
    "ANNUALLY",
    "DAILY",
    "HOURLY",
    "LAST_MONTH",
    "LAST_WEEK",
    "MONTHLY",
    "QUARTERLY",
    "WEEKLY",
    "YESTERDAY",
    # Date context + parametrized runs
    "DateContext",
    "RunContext",
    "RunSpec",
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
    # Common concrete classes (no submodule import needed for basic use)
    "SQLAlchemyStateStore",
    "SubprocessExecutor",
    "PythonJobExecutor",
    "FilesystemReceiver",
    # Config
    "DispatchioSettings",
    "load_config",
    # Reporters
    "Reporter",
    "get_reporter",
    "build_reporter",
    # Utilities
    "resolve_run_key",
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
