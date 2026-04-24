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
from typing import Any, ClassVar
from uuid import UUID, uuid4

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
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.pool import StaticPool

from dispatchio.models import (
    Attempt,
    DeadLetter,
    Event,
    EventIdentity,
    JobIdentity,
    NamespaceIdentity,
    OrchestratorRun,
    OrchestratorRunStatus,
    OrchestratorRunMode,
    RetryRequest,
    Status,
)
from dispatchio.models import (
    TriggerType,
    DeadLetterStatus,
)
from dispatchio.state.base import StateStore, AmbiguousNamespaceError

# ---------------------------------------------------------------------------
# Custom types
# ---------------------------------------------------------------------------


class _UTCDateTime(TypeDecorator):
    """Stores datetimes as UTC; always returns tz-aware datetime on load."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def process_result_value(self, value: datetime | None, dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)


def _enum_type(enum_class):
    class _EnumType(TypeDecorator):
        impl = String(50)
        cache_ok = True

        def process_bind_param(self, value, dialect):
            return value.value if value is not None else None

        def process_result_value(self, value, dialect):
            return enum_class(value) if value is not None else None

    return _EnumType()


_StatusType = _enum_type(Status)
_TriggerTypeType = _enum_type(TriggerType)

# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class _Base(DeclarativeBase):
    _model_class: ClassVar[type | None] = None

    def to_model(self):
        """Convert this ORM row to its corresponding model by copying all column attributes."""
        insp = sa_inspect(type(self))
        data = {attr.key: getattr(self, attr.key) for attr in insp.mapper.column_attrs}
        if self._model_class is None:
            raise NotImplementedError("to_model not implemented for base class")
        return self._model_class(**data)

    def apply(self, record) -> None:
        """Update this ORM row in-place with fields from the given model by copying all column attributes."""
        insp = sa_inspect(type(self))
        for attr in insp.mapper.column_attrs:
            if hasattr(record, attr.key):
                setattr(self, attr.key, getattr(record, attr.key))


# ---------------------------------------------------------------------------
# Registry tables — stable UUID identity for jobs, namespaces, and events
# ---------------------------------------------------------------------------


class _NamespaceRow(_Base):
    """Registry entry for a namespace. Name is the mutable display label."""

    __tablename__ = "namespaces"
    _model_class = NamespaceIdentity

    namespace_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(_UTCDateTime, nullable=False)


class _JobRow(_Base):
    """Registry entry for a job within a namespace."""

    __tablename__ = "jobs"
    _model_class = JobIdentity

    job_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    namespace_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(_UTCDateTime, nullable=False)

    __table_args__ = (UniqueConstraint("namespace_id", "name", name="uq_job_ns_name"),)


class _EventRow(_Base):
    """Registry entry for an event type within a namespace."""

    __tablename__ = "events"
    _model_class = EventIdentity

    event_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    namespace_id: Mapped[UUID] = mapped_column(Uuid, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(_UTCDateTime, nullable=False)

    __table_args__ = (UniqueConstraint("namespace_id", "name", name="uq_event_ns_name"),)


# ---------------------------------------------------------------------------
# History / state tables
# ---------------------------------------------------------------------------


class _AttemptRow(_Base):
    """Immutable row per attempt."""

    __tablename__ = "attempts"
    _model_class = Attempt

    correlation_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
    namespace: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, default="default"
    )
    job_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    run_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[Status] = mapped_column(_StatusType, nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    submitted_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(
        _UTCDateTime, nullable=True, index=True
    )
    trigger_type: Mapped[TriggerType] = mapped_column(
        _TriggerTypeType, nullable=False, default=TriggerType.SCHEDULED
    )
    trigger_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    trace: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    completion_event_trace: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )
    requested_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    job_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint(
            "namespace", "job_name", "run_key", "attempt", name="uq_run_key_attempt"
        ),
    )


class _RetryRequestRow(_Base):
    """Audit row for a manual retry request."""

    __tablename__ = "retry_requests"
    _model_class = RetryRequest

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
    _model_class = DeadLetter

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
    job_name: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    run_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    correlation_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False, default=dict
    )
    resolved_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    resolver_notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Best-effort resolved identity (no FK enforcement)
    namespace_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)
    job_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True)


class _OrchestratorRunRow(_Base):
    """First-class orchestrator run record for backfill and replay coordination."""

    __tablename__ = "orchestrator_runs"
    _model_class = OrchestratorRun

    orchestrator_run_id: Mapped[UUID] = mapped_column(Uuid, primary_key=True)
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
    namespace_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True, index=True)

    __table_args__ = (
        UniqueConstraint("namespace", "run_key", name="uq_orchestrator_run_key"),
    )


class _EventOccurrenceRow(_Base):
    """State row for one event occurrence (namespace, name, run_key)."""

    __tablename__ = "event_occurrences"
    _model_class = Event

    namespace: Mapped[str] = mapped_column(String(255), primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(255), primary_key=True)
    run_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    occurred_at: Mapped[datetime] = mapped_column(
        _UTCDateTime, nullable=False, index=True
    )
    trace: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    event_id: Mapped[UUID | None] = mapped_column(Uuid, nullable=True, index=True)


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
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            if is_memory:
                engine_kwargs["poolclass"] = StaticPool
        else:
            engine_kwargs["pool_size"] = pool_size

        self._engine = create_engine(connection_string, **engine_kwargs)
        _Base.metadata.create_all(self._engine)
        self._Session = sessionmaker(bind=self._engine)
        self._lock: threading.Lock | None = threading.Lock() if is_memory else None

    @contextmanager
    def _session(self) -> Iterator[Session]:
        @contextmanager
        def _nullctx() -> Iterator[None]:
            yield

        with self._lock or _nullctx():
            with Session(self._engine) as session:
                yield session

    # ------------------------------------------------------------------
    # Registry resolution helpers
    # ------------------------------------------------------------------

    def _resolve_namespace_id(self, session: Session, name: str) -> UUID:
        """Get or create a namespace registry entry; return its namespace_id."""
        row = session.scalar(select(_NamespaceRow).where(_NamespaceRow.name == name))
        if row is not None:
            return row.namespace_id
        new_id = uuid4()
        try:
            with session.begin_nested():
                session.add(
                    _NamespaceRow(
                        namespace_id=new_id,
                        name=name,
                        created_at=datetime.now(timezone.utc),
                    )
                )
            return new_id
        except IntegrityError:
            row = session.scalar(select(_NamespaceRow).where(_NamespaceRow.name == name))
            return row.namespace_id  # type: ignore[union-attr]

    def _resolve_job_id(
        self, session: Session, namespace_id: UUID, job_name: str
    ) -> UUID:
        """Get or create a job registry entry; return its job_id."""
        row = session.scalar(
            select(_JobRow).where(
                and_(_JobRow.namespace_id == namespace_id, _JobRow.name == job_name)
            )
        )
        if row is not None:
            return row.job_id
        new_id = uuid4()
        try:
            with session.begin_nested():
                session.add(
                    _JobRow(
                        job_id=new_id,
                        namespace_id=namespace_id,
                        name=job_name,
                        created_at=datetime.now(timezone.utc),
                    )
                )
            return new_id
        except IntegrityError:
            row = session.scalar(
                select(_JobRow).where(
                    and_(
                        _JobRow.namespace_id == namespace_id, _JobRow.name == job_name
                    )
                )
            )
            return row.job_id  # type: ignore[union-attr]

    def _resolve_event_id(
        self, session: Session, namespace_id: UUID, event_name: str
    ) -> UUID:
        """Get or create an event type registry entry; return its event_id."""
        row = session.scalar(
            select(_EventRow).where(
                and_(_EventRow.namespace_id == namespace_id, _EventRow.name == event_name)
            )
        )
        if row is not None:
            return row.event_id
        new_id = uuid4()
        try:
            with session.begin_nested():
                session.add(
                    _EventRow(
                        event_id=new_id,
                        namespace_id=namespace_id,
                        name=event_name,
                        created_at=datetime.now(timezone.utc),
                    )
                )
            return new_id
        except IntegrityError:
            row = session.scalar(
                select(_EventRow).where(
                    and_(
                        _EventRow.namespace_id == namespace_id,
                        _EventRow.name == event_name,
                    )
                )
            )
            return row.event_id  # type: ignore[union-attr]

    def _lookup_job_id(self, session: Session, job_name: str) -> UUID | None:
        """Return the job_id for job_name in the current namespace, or None."""
        if self._namespace is None:
            return None
        ns_row = session.scalar(
            select(_NamespaceRow).where(_NamespaceRow.name == self._namespace)
        )
        if ns_row is None:
            return None
        job_row = session.scalar(
            select(_JobRow).where(
                and_(
                    _JobRow.namespace_id == ns_row.namespace_id,
                    _JobRow.name == job_name,
                )
            )
        )
        return job_row.job_id if job_row is not None else None

    # ------------------------------------------------------------------
    # StateStore protocol - Attempt API
    # ------------------------------------------------------------------

    def get_latest_attempt(self, job_name: str, run_key: str) -> Attempt | None:
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to get the latest attempt."
            )
        with self._session() as session:
            job_id = self._lookup_job_id(session, job_name)
            if job_id is not None:
                q = (
                    select(_AttemptRow)
                    .where(
                        and_(
                            _AttemptRow.job_id == job_id,
                            _AttemptRow.run_key == run_key,
                        )
                    )
                    .order_by(desc(_AttemptRow.attempt))
                    .limit(1)
                )
            else:
                q = (
                    select(_AttemptRow)
                    .where(
                        and_(
                            _AttemptRow.namespace == self._namespace,
                            _AttemptRow.job_name == job_name,
                            _AttemptRow.run_key == run_key,
                        )
                    )
                    .order_by(desc(_AttemptRow.attempt))
                    .limit(1)
                )
            row = session.scalar(q)
            return row.to_model() if row is not None else None

    def get_attempt(self, correlation_id: UUID) -> Attempt | None:
        with self._session() as session:
            row = session.scalar(
                select(_AttemptRow).where(_AttemptRow.correlation_id == correlation_id)
            )
            return row.to_model() if row is not None else None

    def append_attempt(self, record: Attempt) -> None:
        """Insert a new attempt row, resolving and recording the stable job_id."""
        if self._namespace is None:
            raise AmbiguousNamespaceError("Namespace must be set to append an attempt.")
        with self._session() as session:
            ns_id = self._resolve_namespace_id(session, self._namespace)
            job_id = self._resolve_job_id(session, ns_id, record.job_name)
            row = _AttemptRow()
            row.apply(record)
            row.namespace = self._namespace
            row.job_id = job_id
            session.add(row)
            session.commit()

    def update_attempt(self, record: Attempt) -> None:
        """Update an existing attempt row by correlation_id."""
        with self._session() as session:
            existing = session.scalar(
                select(_AttemptRow).where(
                    _AttemptRow.correlation_id == record.correlation_id
                )
            )
            if existing is not None:
                saved_job_id = existing.job_id
                existing.apply(record)
                if saved_job_id is not None and existing.job_id is None:
                    existing.job_id = saved_job_id
                session.commit()

    def list_attempts(
        self,
        job_name: str | None = None,
        run_key: str | None = None,
        attempt: int | None = None,
        status: Status | None = None,
    ) -> list[Attempt]:
        with self._session() as session:
            q = select(_AttemptRow)
            if self._namespace is not None:
                q = q.where(_AttemptRow.namespace == self._namespace)
            if job_name is not None:
                job_id = self._lookup_job_id(session, job_name)
                if job_id is not None:
                    q = q.where(_AttemptRow.job_id == job_id)
                else:
                    q = q.where(_AttemptRow.job_name == job_name)
            if run_key is not None:
                q = q.where(_AttemptRow.run_key == run_key)
            if attempt is not None:
                q = q.where(_AttemptRow.attempt == attempt)
            if status is not None:
                q = q.where(_AttemptRow.status == status)
            q = q.order_by(
                _AttemptRow.namespace,
                _AttemptRow.job_name,
                _AttemptRow.run_key,
                desc(_AttemptRow.attempt),
            )
            rows = session.scalars(q).all()
            return [r.to_model() for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Dead Letter API
    # ------------------------------------------------------------------

    def append_dead_letter(self, record: DeadLetter) -> None:
        """Record a rejected completion event, resolving registry IDs where possible."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to append a dead letter."
            )
        with self._session() as session:
            row = _DeadLetterRow()
            row.apply(record)
            row.namespace = self._namespace
            # Best-effort registry resolution — failures must not block dead-lettering.
            try:
                ns_id = self._resolve_namespace_id(session, self._namespace)
                row.namespace_id = ns_id
                if record.job_name is not None:
                    row.job_id = self._resolve_job_id(session, ns_id, record.job_name)
            except Exception:
                pass
            session.add(row)
            session.commit()

    def get_dead_letter(self, dead_letter_id: UUID) -> DeadLetter | None:
        with self._session() as session:
            row = session.scalar(
                select(_DeadLetterRow).where(
                    _DeadLetterRow.dead_letter_id == dead_letter_id
                )
            )
            return row.to_model() if row is not None else None

    def get_attempt_by_executor_trace(
        self, trace_key: str, trace_value: str
    ) -> Attempt | None:
        """
        Find the first attempt whose trace.executor[trace_key] == trace_value.

        Uses a SQL LIKE pre-filter on the JSON column text to avoid a full table
        scan, then does an exact Python-side match for correctness.
        """
        with self._session() as session:
            rows = session.scalars(
                select(_AttemptRow).where(
                    cast(_AttemptRow.trace, String).like(f"%{trace_value}%")
                )
            ).all()
            for row in rows:
                executor_trace = (row.trace or {}).get("executor", {})
                if executor_trace.get(trace_key) == trace_value:
                    return row.to_model()
            return None

    def list_dead_letters(
        self, job_name: str | None = None, status: str | None = None
    ) -> list[DeadLetter]:
        with self._session() as session:
            q = select(_DeadLetterRow)
            if self._namespace is not None:
                q = q.where(_DeadLetterRow.namespace == self._namespace)
            if job_name is not None:
                job_id = self._lookup_job_id(session, job_name)
                if job_id is not None:
                    q = q.where(_DeadLetterRow.job_id == job_id)
                else:
                    q = q.where(_DeadLetterRow.job_name == job_name)
            if status is not None:
                q = q.where(_DeadLetterRow.status == status)
            q = q.order_by(_DeadLetterRow.occurred_at.desc())
            rows = session.scalars(q).all()
            return [r.to_model() for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Retry Request API
    # ------------------------------------------------------------------

    def append_retry_request(self, record: RetryRequest) -> None:
        """Record a manual retry request for audit."""
        with self._session() as session:
            row = _RetryRequestRow()
            row.apply(record)
            session.add(row)
            session.commit()

    def list_retry_requests(
        self,
        run_key: str | None = None,
        requested_by: str | None = None,
    ) -> list[RetryRequest]:
        with self._session() as session:
            q = select(_RetryRequestRow)
            if run_key is not None:
                q = q.where(_RetryRequestRow.run_key == run_key)
            if requested_by is not None:
                q = q.where(_RetryRequestRow.requested_by == requested_by)
            q = q.order_by(_RetryRequestRow.requested_at.desc())
            rows = session.scalars(q).all()
            return [r.to_model() for r in rows]

    # ------------------------------------------------------------------
    # StateStore protocol - Orchestrator Run API
    # ------------------------------------------------------------------

    def append_orchestrator_run(self, record: OrchestratorRun) -> None:
        """Insert a new orchestrator run record, resolving and recording namespace_id."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to append an orchestrator run."
            )
        with self._session() as session:
            ns_id = self._resolve_namespace_id(session, self._namespace)
            row = _OrchestratorRunRow()
            row.apply(record)
            row.namespace = self._namespace
            row.namespace_id = ns_id
            session.add(row)
            session.commit()

    def get_orchestrator_run(self, orchestrator_run_id: UUID) -> OrchestratorRun | None:
        """Retrieve an orchestrator run by its UUID."""
        with self._session() as session:
            row = session.scalar(
                select(_OrchestratorRunRow).where(
                    _OrchestratorRunRow.orchestrator_run_id == orchestrator_run_id
                )
            )
            return row.to_model() if row is not None else None

    def get_orchestrator_run_by_key(self, run_key: str) -> OrchestratorRun | None:
        """Retrieve an orchestrator run by run key."""
        if self._namespace is None:
            raise AmbiguousNamespaceError(
                "Namespace must be set to get an orchestrator run by key."
            )
        with self._session() as session:
            row = session.scalar(
                select(_OrchestratorRunRow).where(
                    and_(
                        _OrchestratorRunRow.namespace == self._namespace,
                        _OrchestratorRunRow.run_key == run_key,
                    )
                )
            )
            return row.to_model() if row is not None else None

    def list_orchestrator_runs(
        self,
        status: OrchestratorRunStatus | None = None,
        mode: OrchestratorRunMode | None = None,
    ) -> list[OrchestratorRun]:
        """List orchestrator runs, optionally filtered by status and/or mode."""
        with self._session() as session:
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
            return [r.to_model() for r in rows]

    def update_orchestrator_run(self, record: OrchestratorRun) -> None:
        """Update an existing orchestrator run record by orchestrator_run_id."""
        with self._session() as session:
            existing = session.scalar(
                select(_OrchestratorRunRow).where(
                    _OrchestratorRunRow.orchestrator_run_id
                    == record.orchestrator_run_id
                )
            )
            if existing is not None:
                saved_namespace_id = existing.namespace_id
                existing.apply(record)
                if saved_namespace_id is not None and existing.namespace_id is None:
                    existing.namespace_id = saved_namespace_id
                session.commit()

    # ------------------------------------------------------------------
    # StateStore protocol - Event API
    # ------------------------------------------------------------------

    def set_event(self, event: Event) -> None:
        """Upsert an event occurrence by (namespace, name, run_key). Last write wins."""
        with self._session() as session:
            ns_id = self._resolve_namespace_id(session, event.namespace)
            event_id = self._resolve_event_id(session, ns_id, event.name)
            row = session.scalar(
                select(_EventOccurrenceRow).where(
                    and_(
                        _EventOccurrenceRow.namespace == event.namespace,
                        _EventOccurrenceRow.name == event.name,
                        _EventOccurrenceRow.run_key == event.run_key,
                    )
                )
            )
            if row is None:
                row = _EventOccurrenceRow(
                    namespace=event.namespace,
                    name=event.name,
                    run_key=event.run_key,
                    status=event.status.value,
                    occurred_at=event.occurred_at,
                    trace=event.trace,
                    event_id=event_id,
                )
                session.add(row)
            else:
                row.status = event.status.value
                row.occurred_at = event.occurred_at
                row.trace = event.trace
                row.event_id = event_id
            session.commit()

    def get_event(self, name: str, run_key: str) -> Event | None:
        """Return the event for (namespace, name, run_key), or None."""
        if self._namespace is None:
            raise AmbiguousNamespaceError("Namespace must be set to get an event.")
        with self._session() as session:
            row = session.scalar(
                select(_EventOccurrenceRow).where(
                    and_(
                        _EventOccurrenceRow.namespace == self._namespace,
                        _EventOccurrenceRow.name == name,
                        _EventOccurrenceRow.run_key == run_key,
                    )
                )
            )
            return row.to_model() if row is not None else None

    def list_events(self, run_key: str | None = None) -> list[Event]:
        """List events for this store's namespace, optionally filtered by run_key."""
        with self._session() as session:
            q = select(_EventOccurrenceRow)
            if self._namespace is not None:
                q = q.where(_EventOccurrenceRow.namespace == self._namespace)
            if run_key is not None:
                q = q.where(_EventOccurrenceRow.run_key == run_key)
            q = q.order_by(
                _EventOccurrenceRow.namespace,
                _EventOccurrenceRow.name,
                _EventOccurrenceRow.run_key,
            )
            rows = session.scalars(q).all()
            return [r.to_model() for r in rows]

    # ------------------------------------------------------------------
    # Rename / remap
    # ------------------------------------------------------------------

    def rename_job(self, old_name: str, new_name: str) -> None:
        """Rename a job in the registry. Run this before renaming the job in code.

        Raises ValueError if old_name is not found or new_name already exists.
        """
        if self._namespace is None:
            raise AmbiguousNamespaceError("Namespace must be set to rename a job.")
        with self._session() as session:
            ns_row = session.scalar(
                select(_NamespaceRow).where(_NamespaceRow.name == self._namespace)
            )
            if ns_row is None:
                raise ValueError(
                    f"Namespace {self._namespace!r} not found in registry."
                )
            self._rename_registry_entry(
                session, _JobRow, ns_row.namespace_id, old_name, new_name, "job"
            )

    def remap_job(self, old_name: str, new_name: str) -> None:
        """Remap an orphaned job entry to a new name (recovery after code was renamed first).

        Raises ValueError if old_name is not found or new_name already exists.
        """
        self.rename_job(old_name, new_name)

    def rename_namespace(self, old_name: str, new_name: str) -> None:
        """Rename a namespace in the registry. Run this before renaming in code/config.

        Raises ValueError if old_name is not found or new_name already exists.
        """
        with self._session() as session:
            row = session.scalar(
                select(_NamespaceRow).where(_NamespaceRow.name == old_name)
            )
            if row is None:
                raise ValueError(f"Namespace {old_name!r} not found in registry.")
            if session.scalar(
                select(_NamespaceRow).where(_NamespaceRow.name == new_name)
            ) is not None:
                raise ValueError(
                    f"Namespace {new_name!r} already exists in registry."
                )
            row.name = new_name
            session.commit()

    def remap_namespace(self, old_name: str, new_name: str) -> None:
        """Remap an orphaned namespace entry to a new name."""
        self.rename_namespace(old_name, new_name)

    def rename_event(self, old_name: str, new_name: str) -> None:
        """Rename an event type in the registry. Run this before renaming in code.

        Raises ValueError if old_name is not found or new_name already exists.
        """
        if self._namespace is None:
            raise AmbiguousNamespaceError("Namespace must be set to rename an event.")
        with self._session() as session:
            ns_row = session.scalar(
                select(_NamespaceRow).where(_NamespaceRow.name == self._namespace)
            )
            if ns_row is None:
                raise ValueError(
                    f"Namespace {self._namespace!r} not found in registry."
                )
            self._rename_registry_entry(
                session, _EventRow, ns_row.namespace_id, old_name, new_name, "event"
            )

    def remap_event(self, old_name: str, new_name: str) -> None:
        """Remap an orphaned event type entry to a new name."""
        self.rename_event(old_name, new_name)

    @staticmethod
    def _rename_registry_entry(
        session: Session,
        row_class: type,
        namespace_id: UUID,
        old_name: str,
        new_name: str,
        kind: str,
    ) -> None:
        row = session.scalar(
            select(row_class).where(
                and_(row_class.namespace_id == namespace_id, row_class.name == old_name)
            )
        )
        if row is None:
            raise ValueError(f"{kind.capitalize()} {old_name!r} not found in registry.")
        if (
            session.scalar(
                select(row_class).where(
                    and_(
                        row_class.namespace_id == namespace_id,
                        row_class.name == new_name,
                    )
                )
            )
            is not None
        ):
            raise ValueError(
                f"{kind.capitalize()} {new_name!r} already exists in registry."
            )
        row.name = new_name
        session.commit()
