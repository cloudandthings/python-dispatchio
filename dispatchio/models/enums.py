from __future__ import annotations

from enum import Enum


class Status(str, Enum):
    """Lifecycle states for a single job attempt."""

    SUBMITTED = "submitted"
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"
    ERROR = "error"
    LOST = "lost"
    CANCELLED = "cancelled"

    @classmethod
    def finished(cls) -> frozenset[Status]:
        return _FINISHED_STATUSES

    @classmethod
    def active(cls) -> frozenset[Status]:
        return _ACTIVE_STATUSES


_FINISHED_STATUSES: frozenset[Status] = frozenset(
    {Status.DONE, Status.ERROR, Status.LOST, Status.CANCELLED}
)
_ACTIVE_STATUSES: frozenset[Status] = frozenset(
    {Status.SUBMITTED, Status.QUEUED, Status.RUNNING}
)


class TriggerType(str, Enum):
    """Why an attempt was created."""

    SCHEDULED = "scheduled"
    AUTO_RETRY = "auto_retry"
    MANUAL_RETRY = "manual_retry"
    ADHOC = "adhoc"


class DeadLetterReasonCode(str, Enum):
    """Why a completion event was rejected (dead-lettered)."""

    VALIDATION_FAILURE = "validation_failure"
    CORRELATION_FAILURE = "correlation_failure"
    POLICY_REJECT = "policy_reject"


class DeadLetterStatus(str, Enum):
    """Status of a dead-letter entry."""

    OPEN = "open"
    REPLAYED = "replayed"
    IGNORED = "ignored"


class DeadLetterSourceBackend(str, Enum):
    """Which backend/source submitted the dead-lettered completion."""

    SUBPROCESS = "subprocess"
    PYTHON = "python"
    LAMBDA = "lambda"
    STEPFUNCTIONS = "stepfunctions"
    ATHENA = "athena"
    HTTP = "http"
    OTHER = "other"


class DependencyMode(str, Enum):
    """Controls how a job's dependency set is evaluated."""

    ALL_SUCCESS = "all_success"
    ALL_FINISHED = "all_finished"
    THRESHOLD = "threshold"


class JobAction(str, Enum):
    """Action taken or deferred for a job during a tick."""

    SUBMITTED = "submitted"
    WOULD_SUBMIT = "would_submit"
    WOULD_DEFER = "would_defer"
    SKIPPED_CONDITION = "skipped_condition"
    SKIPPED_DEPENDENCIES = "skipped_dependencies"
    SKIPPED_THRESHOLD_UNREACHABLE = "skipped_threshold_unreachable"
    SKIPPED_ALREADY_ACTIVE = "skipped_already_active"
    SKIPPED_ALREADY_DONE = "skipped_already_done"
    SKIPPED_UPSTREAM = "skipped_upstream"
    MARKED_LOST = "marked_lost"
    MARKED_ERROR = "marked_error"
    RETRYING = "retrying"
    SUBMISSION_FAILED = "submission_failed"
    DEFERRED_ACTIVE_LIMIT = "deferred_active_limit"
    DEFERRED_POOL_ACTIVE_LIMIT = "deferred_pool_active_limit"
    DEFERRED_SUBMIT_LIMIT = "deferred_submit_limit"
    DEFERRED_POOL_SUBMIT_LIMIT = "deferred_pool_submit_limit"
    ALERT_SENT = "alert_sent"


_SKIPPED_ACTIONS: frozenset[JobAction] = frozenset(
    {
        JobAction.SKIPPED_CONDITION,
        JobAction.SKIPPED_DEPENDENCIES,
        JobAction.SKIPPED_THRESHOLD_UNREACHABLE,
        JobAction.SKIPPED_ALREADY_ACTIVE,
        JobAction.SKIPPED_ALREADY_DONE,
        JobAction.SKIPPED_UPSTREAM,
    }
)


class AlertOn(str, Enum):
    """When to trigger an alert for a job."""

    ERROR = "error"
    LOST = "lost"
    SUCCESS = "success"
    NOT_STARTED_BY = "not_started_by"


class OrchestratorRunStatus(str, Enum):
    """Lifecycle states for an orchestrator run."""

    PENDING = "pending"
    ACTIVE = "active"
    BLOCKED = "blocked"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OrchestratorRunMode(str, Enum):
    """Why this orchestrator run was created."""

    SCHEDULED = "scheduled"
    BACKFILL = "backfill"
    REPLAY = "replay"
