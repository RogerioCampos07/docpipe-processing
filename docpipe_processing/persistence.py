"""SQLAlchemy persistence for Processing domain jobs."""

import os
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Iterator, override
from uuid import UUID

from psycopg import errors
from pydantic import ValidationError
from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Engine,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    create_engine,
    select,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import (
    ArgumentError,
    DataError,
    DBAPIError,
    IntegrityError,
    InterfaceError,
    OperationalError,
)
from sqlalchemy.exc import (
    TimeoutError as SQLAlchemyTimeoutError,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
)
from sqlalchemy.types import TypeDecorator

from docpipe_processing.domain import ProcessingJob, ProcessingStatus
from docpipe_processing.processing_input import ProcessingInput

DATABASE_URL_ENV_VAR = 'PROCESSING_DATABASE_URL'


class PersistenceError(Exception):
    """Base exception for persistence-specific failures."""


class PersistenceConfigurationError(PersistenceError):
    """Raised for missing or unsupported database configuration."""


class PersistenceUnavailableError(PersistenceError):
    """Raised when the database cannot complete an operation."""


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
    """Raised when data violates a Processing persistence invariant."""


class ProcessingJobIdentityError(PersistenceIntegrityError):
    """Raised when a persisted job's identity is changed by a snapshot."""

    def __init__(self, processing_id: UUID, fields: tuple[str, ...]) -> None:
        self.processing_id = processing_id
        self.fields = fields
        super().__init__(
            f'Processing job {processing_id!s} has immutable fields that '
            f'do not match the persisted identity: {", ".join(fields)}.'
        )


class Base(DeclarativeBase):
    """Declarative base for Processing-owned persistence models."""


class UTCDateTime(TypeDecorator[datetime]):
    """Persist timezone-aware timestamps normalized to UTC."""

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

        del dialect
        return value.astimezone(UTC)

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
            raise ValueError('Persisted timestamps must be timezone-aware.')
        return value.astimezone(UTC)


class IntegerSize(TypeDecorator[int]):
    """Round-trip Python integers without imposing a BIGINT contract limit."""

    impl = Numeric(asdecimal=True)
    cache_ok = True

    @override
    def process_bind_param(
        self, value: int | None, dialect: Any
    ) -> Decimal | None:
        del dialect
        return Decimal(value) if value is not None else None

    @override
    def process_result_value(
        self, value: Decimal | None, dialect: Any
    ) -> int | None:
        del dialect
        return int(value) if value is not None else None


class ProcessingJobModel(Base):
    """ORM row for a domain ProcessingJob."""

    __tablename__ = 'processing_jobs'
    __table_args__ = (
        UniqueConstraint(
            'source_event_id',
            name='uq_processing_jobs_source_event_id',
        ),
        CheckConstraint('attempts >= 0', name='ck_processing_jobs_attempts'),
        CheckConstraint(
            'num_nonnulls(storage_key, media_type, size_bytes, sha256) '
            'IN (0, 4)',
            name='ck_processing_jobs_input_complete',
        ),
        CheckConstraint(
            'size_bytes >= 0 AND size_bytes = trunc(size_bytes) '
            "AND size_bytes NOT IN ('NaN'::numeric, 'Infinity'::numeric, "
            "'-Infinity'::numeric)",
            name='ck_processing_jobs_input_size',
        ),
        CheckConstraint(
            "sha256 ~ '^[0-9a-f]{64}$'",
            name='ck_processing_jobs_input_sha256',
        ),
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
    # All NULL is reserved for pre-input legacy rows, never new acceptance.
    storage_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    media_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_bytes: Mapped[int | None] = mapped_column(
        IntegerSize(), nullable=True
    )
    sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)


def _validate_database_url(value: str) -> str:
    try:
        url = make_url(value)
        if (
            url.drivername != 'postgresql+psycopg'
            or not url.host
            or not url.database
        ):
            raise ValueError
        # Force validation of malformed ports without exposing the input.
        _ = url.port
    except ArgumentError, ValueError, TypeError:
        raise PersistenceConfigurationError(
            'Expected a postgresql+psycopg URL with host and database.'
        ) from None
    return value


def get_database_url() -> str:
    """Require explicit PostgreSQL configuration, without a SQLite fallback."""
    value = os.environ.get(DATABASE_URL_ENV_VAR, '')
    if not value.strip():
        raise PersistenceConfigurationError(
            f'{DATABASE_URL_ENV_VAR} must be configured.'
        )
    return _validate_database_url(value)


def create_database_engine(database_url: str | None = None) -> Engine:
    """Configure a lazy PostgreSQL engine; the caller owns its lifecycle."""
    url = (
        get_database_url()
        if database_url is None
        else _validate_database_url(database_url)
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=0,
        pool_timeout=5,
        connect_args={'connect_timeout': 5},
        hide_parameters=True,
    )


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
    return (
        isinstance(original, errors.UniqueViolation)
        and original.sqlstate == '23505'
        and original.diag.constraint_name
        == 'uq_processing_jobs_source_event_id'
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


def _input_from_model(
    model: ProcessingJobModel | None,
) -> ProcessingInput | None:
    if model is None or model.storage_key is None:
        return None
    return ProcessingInput.model_validate({
        'storage_key': model.storage_key,
        'media_type': model.media_type,
        'size_bytes': model.size_bytes,
        'sha256': model.sha256,
    })


def _revalidate_input(input: ProcessingInput) -> ProcessingInput:
    """Revalidate raw fields, including instances built without validation."""
    values = {
        'storage_key': getattr(input, 'storage_key', None),
        'media_type': getattr(input, 'media_type', None),
        'size_bytes': getattr(input, 'size_bytes', None),
        'sha256': getattr(input, 'sha256', None),
    }
    try:
        return ProcessingInput.model_validate(values)
    except ValidationError as error:
        raise PersistenceIntegrityError(
            'The Processing input metadata failed validation.'
        ) from error


class ProcessingJobRepository:
    """Concrete repository backed by a caller-configured SQLAlchemy session."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    @contextmanager
    def _session(self, source_event_id: str = '') -> Iterator[Session]:
        try:
            with self._session_factory() as session, session.begin():
                yield session
        except IntegrityError as error:
            _raise_integrity_error(error, source_event_id)
        except (
            OperationalError,
            InterfaceError,
            SQLAlchemyTimeoutError,
        ):
            raise PersistenceUnavailableError(
                'The Processing database operation could not be completed.'
            ) from None
        except DataError:
            raise PersistenceIntegrityError(
                'The Processing database rejected the operation.'
            ) from None
        except DBAPIError:
            raise PersistenceError(
                'The Processing database operation failed.'
            ) from None

    def create(
        self, job: ProcessingJob, *, input: ProcessingInput
    ) -> ProcessingJob:
        """Commit a job and its required input in one PostgreSQL transaction.

        A duplicate always raises; this method never declares equivalence.
        """
        validated_input = _revalidate_input(input)
        model = _model_from_job(job)
        model.storage_key = validated_input.storage_key
        model.media_type = validated_input.media_type
        model.size_bytes = validated_input.size_bytes
        model.sha256 = validated_input.sha256
        with self._session(job.source_event_id) as session:
            session.add(model)
        return job

    def get_by_processing_id(
        self,
        processing_id: UUID,
    ) -> ProcessingJob | None:
        with self._session() as session:
            model = session.get(ProcessingJobModel, processing_id)
            return _job_from_model(model) if model is not None else None

    def get_by_source_event_id(
        self,
        source_event_id: str,
    ) -> ProcessingJob | None:
        with self._session() as session:
            model = session.scalar(
                select(ProcessingJobModel).where(
                    ProcessingJobModel.source_event_id == source_event_id
                )
            )
            return _job_from_model(model) if model is not None else None

    def get_input_by_processing_id(
        self, processing_id: UUID
    ) -> ProcessingInput | None:
        """Return durable input, or None for a missing/legacy job."""
        with self._session() as session:
            model = session.get(ProcessingJobModel, processing_id)
            return _input_from_model(model)

    def get_input_by_source_event_id(
        self, source_event_id: str
    ) -> ProcessingInput | None:
        """Read the persisted input without consulting the producer/broker."""
        with self._session() as session:
            model = session.scalar(
                select(ProcessingJobModel).where(
                    ProcessingJobModel.source_event_id == source_event_id
                )
            )
            return _input_from_model(model)

    def save(self, job: ProcessingJob) -> ProcessingJob:
        """Update a domain snapshot without changing the accepted input."""
        with self._session(job.source_event_id) as session:
            model = session.get(ProcessingJobModel, job.processing_id)
            if model is None:
                raise ProcessingJobNotFoundError(job.processing_id)

            immutable_fields: list[str] = []
            if model.source_event_id != job.source_event_id:
                immutable_fields.append('source_event_id')
            if model.document_id != job.document_id:
                immutable_fields.append('document_id')
            if model.correlation_id != job.correlation_id:
                immutable_fields.append('correlation_id')
            if model.created_at.astimezone(UTC) != job.created_at.astimezone(
                UTC
            ):
                immutable_fields.append('created_at')
            if immutable_fields:
                raise ProcessingJobIdentityError(
                    job.processing_id, tuple(immutable_fields)
                )

            model.status = job.status.value
            model.attempts = job.attempts
            model.updated_at = job.updated_at
            model.started_at = job.started_at
            model.finished_at = job.finished_at
            model.failure_reason = job.failure_reason
        return job
