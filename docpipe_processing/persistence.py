"""SQLAlchemy persistence for Processing domain jobs."""

import os
import sqlite3
from datetime import UTC, datetime
from typing import Any, override
from uuid import UUID

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Engine,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    create_engine,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
)
from sqlalchemy.types import TypeDecorator

from docpipe_processing.domain import ProcessingJob, ProcessingStatus

DEFAULT_DATABASE_URL = 'sqlite:///./processing.db'
DATABASE_URL_ENV_VAR = 'PROCESSING_DATABASE_URL'


class PersistenceError(Exception):
    """Base exception for persistence-specific failures."""


class DuplicateSourceEventError(PersistenceError):
    """Raised when a source event already has a persisted processing job."""

    def __init__(self, source_event_id: str) -> None:
        self.source_event_id = source_event_id
        super().__init__(
            f'A processing job already exists for source event '
            f'{source_event_id!r}.'
        )


class ProcessingJobNotFoundError(PersistenceError):
    """Raised when saving a snapshot for a job that is not persisted."""

    def __init__(self, processing_id: UUID) -> None:
        self.processing_id = processing_id
        super().__init__(f'Processing job {processing_id!s} does not exist.')


class PersistenceIntegrityError(PersistenceError):
    """Raised for a database integrity violation other than a duplicate."""


class Base(DeclarativeBase):
    """Declarative base for Processing-owned persistence models."""


class UTCDateTime(TypeDecorator[datetime]):
    """Persist UTC timestamps while restoring timezone awareness on SQLite."""

    impl = DateTime(timezone=True)
    cache_ok = True

    @override
    def process_bind_param(
        self,
        value: datetime | None,
        dialect: Any,
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('Persisted timestamps must be timezone-aware.')

        normalized = value.astimezone(UTC)
        if dialect.name == 'sqlite':
            return normalized.replace(tzinfo=None)
        return normalized

    @override
    def process_result_value(
        self,
        value: datetime | None,
        dialect: Any,
    ) -> datetime | None:
        del dialect
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class ProcessingJobModel(Base):
    """ORM row for a domain ProcessingJob."""

    __tablename__ = 'processing_jobs'
    __table_args__ = (
        UniqueConstraint(
            'source_event_id',
            name='uq_processing_jobs_source_event_id',
        ),
        CheckConstraint('attempts >= 0', name='ck_processing_jobs_attempts'),
    )

    processing_id: Mapped[UUID] = mapped_column(
        Uuid(as_uuid=True), primary_key=True
    )
    source_event_id: Mapped[str] = mapped_column(String, nullable=False)
    document_id: Mapped[str] = mapped_column(String, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(), nullable=True
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(), nullable=True
    )
    failure_reason: Mapped[str | None] = mapped_column(String, nullable=True)


def get_database_url() -> str:
    """Read the Processing database URL, defaulting to a local SQLite file."""
    return os.environ.get(DATABASE_URL_ENV_VAR) or DEFAULT_DATABASE_URL


def create_database_engine(database_url: str | None = None) -> Engine:
    """Create an engine for an explicit or configured database URL."""
    return create_engine(database_url or get_database_url())


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create an independent SQLAlchemy Session factory for an engine."""
    return sessionmaker(bind=engine, expire_on_commit=False)


def _model_values(job: ProcessingJob) -> dict[str, object]:
    return {
        'processing_id': job.processing_id,
        'source_event_id': job.source_event_id,
        'document_id': job.document_id,
        'correlation_id': job.correlation_id,
        'status': job.status.value,
        'attempts': job.attempts,
        'created_at': job.created_at,
        'updated_at': job.updated_at,
        'started_at': job.started_at,
        'finished_at': job.finished_at,
        'failure_reason': job.failure_reason,
    }


def _model_from_job(job: ProcessingJob) -> ProcessingJobModel:
    return ProcessingJobModel(**_model_values(job))


def _job_from_model(model: ProcessingJobModel) -> ProcessingJob:
    return ProcessingJob(
        processing_id=model.processing_id,
        source_event_id=model.source_event_id,
        document_id=model.document_id,
        correlation_id=model.correlation_id,
        status=ProcessingStatus(model.status),
        attempts=model.attempts,
        created_at=model.created_at,
        updated_at=model.updated_at,
        started_at=model.started_at,
        finished_at=model.finished_at,
        failure_reason=model.failure_reason,
    )


def _is_duplicate_source_event(error: IntegrityError) -> bool:
    original = error.orig
    if not isinstance(original, sqlite3.IntegrityError):
        return False
    return (
        getattr(original, 'sqlite_errorname', None)
        == 'SQLITE_CONSTRAINT_UNIQUE'
        and str(original)
        == 'UNIQUE constraint failed: processing_jobs.source_event_id'
    )


def _raise_integrity_error(
    error: IntegrityError,
    source_event_id: str,
) -> None:
    if _is_duplicate_source_event(error):
        raise DuplicateSourceEventError(source_event_id) from error
    raise PersistenceIntegrityError(
        'The processing job violated a persistence constraint.'
    ) from error


class ProcessingJobRepository:
    """Concrete repository backed by a caller-configured SQLAlchemy session."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def create(self, job: ProcessingJob) -> ProcessingJob:
        """Persist a new job, rejecting a repeated source event explicitly."""
        with self._session_factory() as session:
            session.add(_model_from_job(job))
            try:
                session.commit()
            except IntegrityError as error:
                session.rollback()
                _raise_integrity_error(error, job.source_event_id)
        return job

    def get_by_processing_id(
        self,
        processing_id: UUID,
    ) -> ProcessingJob | None:
        with self._session_factory() as session:
            model = session.get(ProcessingJobModel, processing_id)
            return _job_from_model(model) if model is not None else None

    def get_by_source_event_id(
        self,
        source_event_id: str,
    ) -> ProcessingJob | None:
        with self._session_factory() as session:
            model = session.scalar(
                select(ProcessingJobModel).where(
                    ProcessingJobModel.source_event_id == source_event_id
                )
            )
            return _job_from_model(model) if model is not None else None

    def save(self, job: ProcessingJob) -> ProcessingJob:
        """Persist a new snapshot of a job that already exists."""
        with self._session_factory() as session:
            model = session.get(ProcessingJobModel, job.processing_id)
            if model is None:
                raise ProcessingJobNotFoundError(job.processing_id)

            for field, value in _model_values(job).items():
                setattr(model, field, value)
            try:
                session.commit()
            except IntegrityError as error:
                session.rollback()
                _raise_integrity_error(error, job.source_event_id)
        return job
