"""
Dispatchio domain models.

Naming conventions
------------------
Domain concept        Pydantic model         ORM row                    Table
--------------------  ---------------------  -------------------------  --------------------
execution attempt     Attempt                _AttemptRow                attempts
dead letter           DeadLetter             _DeadLetterRow             dead_letters
orchestrator run      OrchestratorRun        _OrchestratorRunRow        orchestrator_runs
retry audit           RetryRequest           _RetryRequestRow           retry_requests
event occurrence      Event                  _EventOccurrenceRow        event_occurrences
namespace identity    NamespaceIdentity      _NamespaceRow              namespaces
job identity          JobIdentity            _JobRow                    jobs
event type identity   EventIdentity          _EventRow                  events

Rules:
  - Pydantic models: plain domain name, no suffix.
  - ORM rows: _{ModelName}Row, private to the state layer.
  - Tables: plural snake_case of the concept.
  - Registry tables (namespaces, jobs, events) hold stable UUID identities
    to support job/namespace/event renames without breaking history.
"""

from dispatchio.models.attempt import Attempt
from dispatchio.models.dead_letter import DeadLetter
from dispatchio.models.enums import (
    AlertOn,
    DeadLetterReasonCode,
    DeadLetterSourceBackend,
    DeadLetterStatus,
    DependencyMode,
    JobAction,
    OrchestratorRunMode,
    OrchestratorRunStatus,
    Status,
    TriggerType,
)
from dispatchio.models.event import Event
from dispatchio.models.job import (
    AdmissionPolicy,
    AlertCondition,
    AthenaJob,
    Dependency,
    EventDependency,
    ExecutorConfig,
    HttpJob,
    Job,
    JobDependency,
    JobTickResult,
    LambdaJob,
    PoolPolicy,
    PythonJob,
    RetryPolicy,
    StepFunctionJob,
    SubprocessJob,
    TickResult,
)
from dispatchio.models.orchestrator import OrchestratorRun
from dispatchio.models.registry import EventIdentity, JobIdentity, NamespaceIdentity
from dispatchio.models.retry import RetryRequest

__all__ = [
    # Attempt
    "Attempt",
    # Dead letter
    "DeadLetter",
    # Enums
    "AlertOn",
    "DeadLetterReasonCode",
    "DeadLetterSourceBackend",
    "DeadLetterStatus",
    "DependencyMode",
    "JobAction",
    "OrchestratorRunMode",
    "OrchestratorRunStatus",
    "Status",
    "TriggerType",
    # Event
    "Event",
    # Job and related
    "AdmissionPolicy",
    "AlertCondition",
    "AthenaJob",
    "Dependency",
    "EventDependency",
    "ExecutorConfig",
    "HttpJob",
    "Job",
    "JobDependency",
    "JobTickResult",
    "LambdaJob",
    "PoolPolicy",
    "PythonJob",
    "RetryPolicy",
    "StepFunctionJob",
    "SubprocessJob",
    "TickResult",
    # Orchestrator run
    "OrchestratorRun",
    # Registry
    "EventIdentity",
    "JobIdentity",
    "NamespaceIdentity",
    # Retry
    "RetryRequest",
]
