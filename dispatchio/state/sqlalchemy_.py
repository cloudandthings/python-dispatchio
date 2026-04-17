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

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (
    JSON,
    DateTime,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from sqlalchemy.pool import StaticPool


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

from dispatchio.models import RunRecord, Status


# ---------------------------------------------------------------------------
# ORM model
# ---------------------------------------------------------------------------


class _Base(DeclarativeBase):
    pass


class _RunRecordRow(_Base):
    __tablename__ = "run_records"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    job_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    run_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    submitted_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(_UTCDateTime, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(
        _UTCDateTime, nullable=True, index=True
    )
    error_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Column name is "metadata" in DB; attribute is metadata_ to avoid shadowing
    # SQLAlchemy's own metadata descriptor on mapped classes.
    metadata_: Mapped[dict[str, Any]] = mapped_column(
        "metadata", JSON, nullable=False, default=dict
    )
    executor_reference: Mapped[dict[str, Any] | None] = mapped_column(
        JSON, nullable=True
    )

    __table_args__ = (UniqueConstraint("job_name", "run_id", name="uq_job_run"),)


# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def _row_to_record(row: _RunRecordRow) -> RunRecord:
    return RunRecord(
        job_name=row.job_name,
        run_id=row.run_id,
        status=Status(row.status),
        attempt=row.attempt,
        submitted_at=row.submitted_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        error_reason=row.error_reason,
        metadata=row.metadata_ or {},
        executor_reference=row.executor_reference,
    )


def _apply_record_to_row(record: RunRecord, row: _RunRecordRow) -> None:
    """Write all RunRecord fields onto an existing (or new) ORM row in-place."""
    row.job_name = record.job_name
    row.run_id = record.run_id
    row.status = record.status.value
    row.attempt = record.attempt
    row.submitted_at = record.submitted_at
    row.started_at = record.started_at
    row.completed_at = record.completed_at
    row.error_reason = record.error_reason
    row.metadata_ = record.metadata
    row.executor_reference = record.executor_reference


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class SQLAlchemyStateStore:
    """StateStore backed by any SQLAlchemy-compatible database."""

    def __init__(
        self,
        connection_string: str,
        echo: bool = False,
        pool_size: int = 5,
    ) -> None:
        """
        Args:
            connection_string: SQLAlchemy database URL.
            echo: If True, log all emitted SQL statements (useful for debugging).
            pool_size: Connection pool size (ignored for SQLite).
        """
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

    # ------------------------------------------------------------------
    # StateStore protocol
    # ------------------------------------------------------------------

    def get(self, job_name: str, run_id: str) -> RunRecord | None:
        with Session(self._engine) as session:
            row = session.scalar(
                select(_RunRecordRow).where(
                    _RunRecordRow.job_name == job_name,
                    _RunRecordRow.run_id == run_id,
                )
            )
            return _row_to_record(row) if row is not None else None

    def put(self, record: RunRecord) -> None:
        with Session(self._engine) as session:
            existing = session.scalar(
                select(_RunRecordRow).where(
                    _RunRecordRow.job_name == record.job_name,
                    _RunRecordRow.run_id == record.run_id,
                )
            )
            if existing is None:
                row = _RunRecordRow()
                _apply_record_to_row(record, row)
                session.add(row)
            else:
                _apply_record_to_row(record, existing)
            session.commit()

    def list_records(
        self,
        job_name: str | None = None,
        status: Status | None = None,
    ) -> list[RunRecord]:
        with Session(self._engine) as session:
            q = select(_RunRecordRow)
            if job_name is not None:
                q = q.where(_RunRecordRow.job_name == job_name)
            if status is not None:
                q = q.where(_RunRecordRow.status == status.value)
            q = q.order_by(_RunRecordRow.job_name, _RunRecordRow.run_id)
            rows = session.scalars(q).all()
            return [_row_to_record(r) for r in rows]
