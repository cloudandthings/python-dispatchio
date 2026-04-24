"""
SQLAlchemy StateStore.

Supports any SQLAlchemy-compatible database:
  - SQLite (local default):   "sqlite:///dispatchio.db"
  - SQLite in-memory (tests): "sqlite:///:memory:"
  - PostgreSQL:               "postgresql+psycopg://user:pass@host/db"
  - MySQL:                    "mysql+pymysql://user:pass@host/db"

The schema is created automatically on first instantiation via create_all().
No migration tooling is required for a fresh deployment.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import threading
from collections.abc import Iterator
from typing import Any
from uuid import UUID

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    Uuid,
    cast,
    create_engine,
    desc,
    and_,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.pool import StaticPool

from dispatchio.models import (
    AttemptRecord,
    DeadLetterRecord,
    Event,
    OrchestratorRunRecord,
    OrchestratorRunStatus,
    OrchestratorRunMode,
    RetryRequest,
    Status,
)
from dispatchio.models import (
    TriggerType,
    DeadLetterReasonCode,
    DeadLetterStatus,
    DeadLetterSourceBackend,
)
from dispatchio.state.base import StateStore, AmbiguousNamespaceError

# ---------------------------------------------------------------------------
# Custom type: always returns UTC-aware datetimes (SQLite drops tz info)
# ---------------------------------------------------------------------------


class _UTCDateTime(TypeDecorator):
    """Stores datetimes as UTC; always returns tz-aware datetime on load."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            # Treat naive values as UTC by convention.
            return value.replace(tzinfo=timezone.utc)
        # Persist all timezone-aware values normalized to UTC.
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            # SQLite commonly returns naive values even for tz-aware columns.
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class _Base(DeclarativeBase):
    pass


class _AttemptRecordRow(_Base):
    """Immutable row per attempt."""

    __tablename__ = "run_attempts"

    # PK: correlation_id as UUID string
    correlation_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    # Identity key: (namespace, job_name, run_key, attempt)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    job_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    run_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    # Status and reason
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Timestamps
    submitted_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(
        _UTCDateTime, nullable=True, index=True
    )
    # Trigger and trace
    trigger_type: Mapped[str] = mapped_column(
        String(50), nullable=False, default=TriggerType.SCHEDULED.value
    )
    trigger_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    completion_event_trace: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    # Operator context for manual operations
    requested_by: Mapped[str | None] = mapped_column(String(255), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "namespace", "job_name", "run_key", "attempt", name="uq_run_key_attempt"
        ),
    )


class _RetryRequestRow(_Base):
    """Audit row for a manual retry request."""

    __tablename__ = "retry_requests"

    retry_request_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    requested_at: Mapped[datetime] = mapped_column(
        _UTCDateTime, nullable=False, index=True
    )
    requested_by: Mapped[str] = mapped_column(String(255), nullable=False)
    run_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    requested_jobs: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    cascade: Mapped[bool] = mapped_column(Integer, nullable=False, default=1)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    selected_jobs: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    assigned_attempt_by_job: Mapped[dict] = mapped_column(
        JSON, nullable=False, default=dict
    )


class _DeadLetterRow(_Base):
    """Dead-letter log for rejected completion events."""

    __tablename__ = "dead_letters"

    dead_letter_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(
        _UTCDateTime, nullable=False, index=True
    )
    source_backend: Mapped[str] = mapped_column(String(50), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(50), nullable=False)
    reason_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default=DeadLetterStatus.OPEN.value
    )
    # Event identity
    job_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    run_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    correlation_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    # Audit
    raw_payload: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    resolved_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    resolver_notes: Mapped[str | None] = mapped_column(Text, nullable=True)


class _OrchestratorRunRow(_Base):
    """First-class orchestrator run record for backfill and replay coordination."""

    __tablename__ = "orchestrator_runs"

    orchestrator_run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    # Identity key: (namespace, run_key)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    run_key: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    mode: Mapped[str] = mapped_column(String(50), nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    submitted_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    force: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)
    replay_group_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    checkpoint: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    opened_at: Mapped[datetime] = mapped_column(_UTCDateTime, nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    closed_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)

    __table_args__ = (
        UniqueConstraint("namespace", "run_key", name="uq_orchestrator_run_key"),
    )


class _EventRow(_Base):
    """Row for events."""

    __tablename__ = "events"

    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(
        _UTCDateTime, nullable=False, index=True
    )
    trace: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def _row_to_attempt_record(row: _AttemptRecordRow) -> AttemptRecord:
    attempt_id = row.correlation_id
    if isinstance(attempt_id, str):
        attempt_id = UUID(attempt_id)
    return AttemptRecord(
        namespace=row.namespace or "default",
        job_name=row.job_name,
        run_key=row.run_key,
        attempt=row.attempt,
        correlation_id=attempt_id,
        status=Status(row.status),
        reason=row.reason,
        submitted_at=row.submitted_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        trigger_type=TriggerType(row.trigger_type),
        trigger_reason=row.trigger_reason,
        trace=row.trace or {},
        completion_event_trace=row.completion_event_trace,
        requested_by=row.requested_by,
    )


def _apply_attempt_record_to_row(record: AttemptRecord, row: _AttemptRecordRow) -> None:
    """Write all AttemptRecord fields onto an existing (or new) ORM row in-place."""
    row.correlation_id = record.correlation_id
    row.namespace = record.namespace
    row.job_name = record.job_name
    row.run_key = record.run_key
    row.attempt = record.attempt
    row.status = record.status.value
    row.reason = record.reason
    row.submitted_at = record.submitted_at
    row.started_at = record.started_at
    row.completed_at = record.completed_at
    row.trigger_type = record.trigger_type.value
    row.trigger_reason = record.trigger_reason
    row.trace = record.trace
    row.completion_event_trace = record.completion_event_trace
    row.requested_by = record.requested_by


def _row_to_retry_request(row: _RetryRequestRow) -> RetryRequest:
    return RetryRequest(
        retry_request_id=row.retry_request_id,
        requested_at=row.requested_at,
        requested_by=row.requested_by,
        run_key=row.run_key,
        requested_jobs=row.requested_jobs or [],
        cascade=bool(row.cascade),
        reason=row.reason,
        selected_jobs=row.selected_jobs or [],
        assigned_attempt_by_job=row.assigned_attempt_by_job or {},
    )


def _apply_retry_request_to_row(record: RetryRequest, row: _RetryRequestRow) -> None:
    row.retry_request_id = record.retry_request_id
    row.requested_at = record.requested_at
    row.requested_by = record.requested_by
    row.run_key = record.run_key
    row.requested_jobs = record.requested_jobs
    row.cascade = int(record.cascade)
    row.reason = record.reason
    row.selected_jobs = record.selected_jobs
    row.assigned_attempt_by_job = record.assigned_attempt_by_job


def _row_to_dead_letter_record(row: _DeadLetterRow) -> DeadLetterRecord:
    return DeadLetterRecord(
        dead_letter_id=row.dead_letter_id,
        occurred_at=row.occurred_at,
        source_backend=DeadLetterSourceBackend(row.source_backend),
        reason_code=DeadLetterReasonCode(row.reason_code),
        reason_detail=row.reason_detail,
        status=DeadLetterStatus(row.status),
        namespace=row.namespace or "default",
        job_name=row.job_name,
        run_key=row.run_key,
        attempt=row.attempt,
        correlation_id=row.correlation_id,
        raw_payload=row.raw_payload or {},
        resolved_at=row.resolved_at,
        resolver_notes=row.resolver_notes,
    )


def _apply_dead_letter_record_to_row(
    record: DeadLetterRecord, row: _DeadLetterRow
) -> None:
    """Write all DeadLetterRecord fields onto an existing (or new) ORM row in-place."""
    row.dead_letter_id = record.dead_letter_id
    row.occurred_at = record.occurred_at
    row.source_backend = record.source_backend.value
    row.reason_code = record.reason_code.value
    row.reason_detail = record.reason_detail
    row.status = record.status.value
    row.namespace = record.namespace
    row.job_name = record.job_name
    row.run_key = record.run_key
    row.attempt = record.attempt
    row.correlation_id = record.correlation_id
    row.raw_payload = record.raw_payload
    row.resolved_at = record.resolved_at
    row.resolver_notes = record.resolver_notes


def _row_to_orchestrator_run_record(
    row: _OrchestratorRunRow,
) -> OrchestratorRunRecord:
    """Convert ORM row to OrchestratorRunRecord model."""
    return OrchestratorRunRecord(
        orchestrator_run_id=row.orchestrator_run_id,
        namespace=row.namespace,
        run_key=row.run_key,
        status=OrchestratorRunStatus(row.status),
        mode=OrchestratorRunMode(row.mode),
        priority=row.priority,
        submitted_by=row.submitted_by,
        reason=row.reason,
        force=bool(row.force),
        replay_group_id=row.replay_group_id,
        checkpoint=row.checkpoint or {},
        opened_at=row.opened_at,
        activated_at=row.activated_at,
        closed_at=row.closed_at,
    )


def _apply_orchestrator_run_record_to_row(
    record: OrchestratorRunRecord, row: _OrchestratorRunRow
) -> None:
    """Write all OrchestratorRunRecord fields onto an existing (or new) ORM row in-place."""
    row.orchestrator_run_id = record.orchestrator_run_id
    row.namespace = record.namespace
    row.run_key = record.run_key
    row.status = record.status.value
    row.mode = record.mode.value
    row.priority = record.priority
    row.submitted_by = record.submitted_by
    row.reason = record.reason
    row.force = int(record.force)
    row.replay_group_id = record.replay_group_id
    row.checkpoint = record.checkpoint
    row.opened_at = record.opened_at
    row.activated_at = record.activated_at
    row.closed_at = record.closed_at


def _row_to_event(row: _EventRow) -> Event:
    return Event(
        name=row.name,
        run_key=row.run_key,
        status=Status(row.status),
        occurred_at=row.occurred_at,
        trace=row.trace or {},
    )


@contextmanager
def _nullctx() -> Iterator[None]:
    """A no-op context manager used when no lock is needed."""
    yield


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class SQLAlchemyStateStore(StateStore):
    """StateStore backed by any SQLAlchemy-compatible database."""

    def __init__(
        self,
        connection_string: str,
        namespace: str | None = "default",
        echo: bool = False,
        pool_size: int = 5,
    ) -> None:
        """
        Args:
            connection_string: SQLAlchemy database URL.
            namespace: Scope all reads/writes to this namespace (orchestrator name).
            echo: If True, log all emitted SQL statements (useful for debugging).
            pool_size: Connection pool size (ignored for SQLite).
        """
        super().__init__(namespace=namespace)
        is_sqlite = connection_string.startswith("sqlite")
        is_memory = ":memory:" in connection_string

        engine_kwargs: dict[str, Any] = {"echo": echo}

        if is_sqlite:
            # SQLite needs check_same_thread=False so it can be used across
            # threads (e.g. orchestrator submits on worker threads).
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            if is_memory:
                # In-memory SQLite must share a single connection; otherwise
                # each Session would get an empty database.
                engine_kwargs["poolclass"] = StaticPool
        else:
            engine_kwargs["pool_size"] = pool_size

        self._engine = create_engine(connection_string, **engine_kwargs)
        _Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)
        # Protect in-memory SQLite's StaticPool from concurrent threads
        self._lock: threading.Lock | None = threading.Lock() if is_memory else None

    # ------------------------------------------------------------------
    # StateStore protocol - Attempt API
    # ------------------------------------------------------------------

    def get_latest_attempt(self, job_name: str, run_key: str) -> AttemptRecord | None:
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to get the latest attempt."
            )
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_AttemptRecordRow)
                    .where(
                        and_(
                            _AttemptRecordRow.namespace == self._namespace,
                            _AttemptRecordRow.job_name == job_name,
                            _AttemptRecordRow.run_key == run_key,
                        )
                    )
                    .order_by(desc(_AttemptRecordRow.attempt))
                    .limit(1)
                )
                return _row_to_attempt_record(row) if row is not None else None

    def get_attempt(self, correlation_id: UUID) -> AttemptRecord | None:
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_AttemptRecordRow).where(
                        _AttemptRecordRow.correlation_id == correlation_id
                    )
                )
                return _row_to_attempt_record(row) if row is not None else None

    def append_attempt(self, record: AttemptRecord) -> None:
        """Insert a new attempt row."""
        if self._namespace is None:
            raise AmbiguousNamespaceError("Namespace must be set to append an attempt.")
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = _AttemptRecordRow()
                _apply_attempt_record_to_row(record, row)
                row.namespace = self._namespace  # enforce store namespace
                session.add(row)
                session.commit()

    def update_attempt(self, record: AttemptRecord) -> None:
        """Update an existing attempt row by correlation_id."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                existing = session.scalar(
                    select(_AttemptRecordRow).where(
                        _AttemptRecordRow.correlation_id == record.correlation_id
                    )
                )
                if existing is not None:
                    _apply_attempt_record_to_row(record, existing)
                    session.commit()

    def list_attempts(
        self,
        job_name: str | None = None,
        run_key: str | None = None,
        attempt: int | None = None,
        status: Status | None = None,
    ) -> list[AttemptRecord]:
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                q = select(_AttemptRecordRow)
                if self._namespace is not None:
                    q = q.where(_AttemptRecordRow.namespace == self._namespace)
                if job_name is not None:
                    q = q.where(_AttemptRecordRow.job_name == job_name)
                if run_key is not None:
                    q = q.where(_AttemptRecordRow.run_key == run_key)
                if attempt is not None:
                    q = q.where(_AttemptRecordRow.attempt == attempt)
                if status is not None:
                    q = q.where(_AttemptRecordRow.status == status.value)
                q = q.order_by(
                    _AttemptRecordRow.namespace,
                    _AttemptRecordRow.job_name,
                    _AttemptRecordRow.run_key,
                    desc(_AttemptRecordRow.attempt),
                )
                rows = session.scalars(q).all()
                return [_row_to_attempt_record(r) for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Dead Letter API
    # ------------------------------------------------------------------

    def append_dead_letter(self, record: DeadLetterRecord) -> None:
        """Record a rejected completion event."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to append a dead letter."
            )
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = _DeadLetterRow()
                _apply_dead_letter_record_to_row(record, row)
                row.namespace = self._namespace  # enforce store namespace
                session.add(row)
                session.commit()

    def get_dead_letter(self, dead_letter_id: UUID) -> DeadLetterRecord | None:
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_DeadLetterRow).where(
                        _DeadLetterRow.dead_letter_id == dead_letter_id
                    )
                )
                return _row_to_dead_letter_record(row) if row is not None else None

    def get_attempt_by_executor_trace(
        self, trace_key: str, trace_value: str
    ) -> AttemptRecord | None:
        """
        Find the first attempt whose trace.executor[trace_key] == trace_value.

        Used by AWS completion handlers to correlate by execution_arn or
        query_execution_id stored in trace at submission time.

        Uses a SQL LIKE pre-filter on the JSON column text to avoid a full
        table scan, then does an exact Python-side match for correctness.
        trace_value is typically a unique ARN or ID, so the pre-filter usually
        returns at most one row.
        """
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                rows = session.scalars(
                    select(_AttemptRecordRow).where(
                        cast(_AttemptRecordRow.trace, String).like(f"%{trace_value}%")
                    )
                ).all()
                for row in rows:
                    executor_trace = (row.trace or {}).get("executor", {})
                    if executor_trace.get(trace_key) == trace_value:
                        return _row_to_attempt_record(row)
                return None

    def list_dead_letters(
        self, job_name: str | None = None, status: str | None = None
    ) -> list[DeadLetterRecord]:
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                q = select(_DeadLetterRow)
                if self._namespace is not None:
                    q = q.where(_DeadLetterRow.namespace == self._namespace)
                if job_name is not None:
                    q = q.where(_DeadLetterRow.job_name == job_name)
                if status is not None:
                    q = q.where(_DeadLetterRow.status == status)
                q = q.order_by(_DeadLetterRow.occurred_at.desc())
                rows = session.scalars(q).all()
                return [_row_to_dead_letter_record(r) for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Retry Request API
    # ------------------------------------------------------------------

    def append_retry_request(self, record: RetryRequest) -> None:
        """Record a manual retry request for audit."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = _RetryRequestRow()
                _apply_retry_request_to_row(record, row)
                session.add(row)
                session.commit()

    def list_retry_requests(
        self,
        run_key: str | None = None,
        requested_by: str | None = None,
    ) -> list[RetryRequest]:
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                q = select(_RetryRequestRow)
                if run_key is not None:
                    q = q.where(_RetryRequestRow.run_key == run_key)
                if requested_by is not None:
                    q = q.where(_RetryRequestRow.requested_by == requested_by)
                q = q.order_by(_RetryRequestRow.requested_at.desc())
                rows = session.scalars(q).all()
                return [_row_to_retry_request(r) for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Orchestrator Run API
    # ------------------------------------------------------------------

    def append_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """Insert a new orchestrator run record."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to append an orchestrator run."
            )
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = _OrchestratorRunRow()
                _apply_orchestrator_run_record_to_row(record, row)
                row.namespace = self._namespace  # enforce store namespace
                session.add(row)
                session.commit()

    def get_orchestrator_run(
        self, orchestrator_run_id: UUID
    ) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by its UUID."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_OrchestratorRunRow).where(
                        _OrchestratorRunRow.orchestrator_run_id == orchestrator_run_id
                    )
                )
                return _row_to_orchestrator_run_record(row) if row is not None else None

    def get_orchestrator_run_by_key(self, run_key: str) -> OrchestratorRunRecord | None:
        """Retrieve an orchestrator run by run key."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to get an orchestrator run by key."
            )
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_OrchestratorRunRow).where(
                        and_(
                            _OrchestratorRunRow.namespace == self._namespace,
                            _OrchestratorRunRow.run_key == run_key,
                        )
                    )
                )
                return _row_to_orchestrator_run_record(row) if row is not None else None

    def list_orchestrator_runs(
        self,
        status: OrchestratorRunStatus | None = None,
        mode: OrchestratorRunMode | None = None,
    ) -> list[OrchestratorRunRecord]:
        """
        List orchestrator runs, optionally filtered by status and/or mode. Results sorted by (namespace, opened_at DESC).
        """
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                q = select(_OrchestratorRunRow)
                if self._namespace is not None:
                    q = q.where(_OrchestratorRunRow.namespace == self._namespace)
                if status is not None:
                    q = q.where(_OrchestratorRunRow.status == status.value)
                if mode is not None:
                    q = q.where(_OrchestratorRunRow.mode == mode.value)
                q = q.order_by(
                    _OrchestratorRunRow.namespace,
                    desc(_OrchestratorRunRow.opened_at),
                )
                rows = session.scalars(q).all()
                return [_row_to_orchestrator_run_record(r) for r in rows]

    def update_orchestrator_run(self, record: OrchestratorRunRecord) -> None:
        """
        Update an existing orchestrator run record by orchestrator_run_id.
        Used when changing status, updating checkpoint, or recording activation/closure.
        """
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                existing = session.scalar(
                    select(_OrchestratorRunRow).where(
                        _OrchestratorRunRow.orchestrator_run_id
                        == record.orchestrator_run_id
                    )
                )
                if existing is not None:
                    _apply_orchestrator_run_record_to_row(record, existing)
                    session.commit()

    # ------------------------------------------------------------------
    # StateStore protocol - Event API
    # ------------------------------------------------------------------

    def set_event(self, event: Event) -> None:
        """Upsert an event by (name, run_key). Last write wins."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_EventRow).where(
                        and_(
                            _EventRow.name == event.name,
                            _EventRow.run_key == event.run_key,
                        )
                    )
                )
                if row is None:
                    row = _EventRow(
                        name=event.name,
                        run_key=event.run_key,
                        status=event.status.value,
                        occurred_at=event.occurred_at,
                        trace=event.trace,
                    )
                    session.add(row)
                else:
                    row.status = event.status.value
                    row.occurred_at = event.occurred_at
                    row.trace = event.trace
                session.commit()

    def get_event(self, name: str, run_key: str) -> Event | None:
        """Return the event for (name, run_key), or None."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                row = session.scalar(
                    select(_EventRow).where(
                        and_(
                            _EventRow.name == name,
                            _EventRow.run_key == run_key,
                        )
                    )
                )
                return _row_to_event(row) if row is not None else None

    def list_events(self, run_key: str | None = None) -> list[Event]:
        """List all events, optionally filtered by run_key."""
        with self._lock or _nullctx():
            with Session(self._engine) as session:
                q = select(_EventRow)
                if run_key is not None:
                    q = q.where(_EventRow.run_key == run_key)
                q = q.order_by(_EventRow.name, _EventRow.run_key)
                rows = session.scalars(q).all()
                return [_row_to_event(r) for r in rows]
