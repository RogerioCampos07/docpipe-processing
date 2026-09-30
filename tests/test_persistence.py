"""Persistence tests on isolated SQLite databases with real migrations."""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.exc import IntegrityError

from docpipe_processing.domain import (
    ProcessingDomainError,
    ProcessingJob,
    ProcessingStatus,
)
from docpipe_processing.persistence import (
    DATABASE_URL_ENV_VAR,
    DEFAULT_DATABASE_URL,
    DuplicateSourceEventError,
    PersistenceIntegrityError,
    ProcessingJobNotFoundError,
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
    get_database_url,
)

pytestmark = pytest.mark.persistence

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')
CREATED_AT = datetime(2026, 1, 1, 12, tzinfo=UTC)
STARTED_AT = datetime(2026, 1, 1, 12, 1, tzinfo=UTC)
FINISHED_AT = datetime(2026, 1, 1, 12, 2, tzinfo=UTC)
ALEMBIC_CONFIG = Path(__file__).parents[1] / 'alembic.ini'


@dataclass(frozen=True)
class DatabaseFixture:
    path: Path
    url: str


@pytest.fixture
def database(tmp_path: Path) -> DatabaseFixture:
    database_path = tmp_path / 'processing.sqlite3'
    database_url = f'sqlite:///{database_path}'
    config = Config(str(ALEMBIC_CONFIG))
    config.attributes['database_url'] = database_url
    command.upgrade(config, 'head')
    return DatabaseFixture(database_path, database_url)


@pytest.fixture
def repository(database: DatabaseFixture) -> Iterator[ProcessingJobRepository]:
    engine = create_database_engine(database.url)
    yield ProcessingJobRepository(create_session_factory(engine))
    engine.dispose()


def received_job(
    *,
    processing_id: UUID = PROCESSING_ID,
    source_event_id: str = 'event-123',
    correlation_id: str | None = 'correlation-789',
) -> ProcessingJob:
    return ProcessingJob.create(
        processing_id=processing_id,
        source_event_id=source_event_id,
        document_id='document-456',
        correlation_id=correlation_id,
        created_at=CREATED_AT,
    )


def job_in_status(status: ProcessingStatus) -> ProcessingJob:
    job = received_job().start_processing(at=STARTED_AT)
    if status is ProcessingStatus.PROCESSING:
        return job
    if status is ProcessingStatus.COMPLETED:
        return job.complete(at=FINISHED_AT)
    if status is ProcessingStatus.FAILED:
        return job.fail(reason='Unsupported format', at=FINISHED_AT)
    raise AssertionError('This helper expects a started job status.')


def test_initial_migration_creates_jobs_and_version_tables(
    database: DatabaseFixture,
) -> None:
    engine = create_engine(database.url)
    try:
        inspector = inspect(engine)
        assert set(inspector.get_table_names()) == {
            'alembic_version',
            'processing_jobs',
        }
        assert inspector.get_pk_constraint('processing_jobs')[
            'constrained_columns'
        ] == ['processing_id']
        assert inspector.get_unique_constraints('processing_jobs') == [
            {
                'name': 'uq_processing_jobs_source_event_id',
                'column_names': ['source_event_id'],
            }
        ]
        with engine.connect() as connection:
            revision = connection.exec_driver_sql(
                'SELECT version_num FROM alembic_version'
            ).scalar_one()
        assert revision == '0001_processing_jobs'
    finally:
        engine.dispose()


def test_initial_migration_downgrade_removes_only_jobs_table(
    database: DatabaseFixture,
) -> None:
    config = Config(str(ALEMBIC_CONFIG))
    config.attributes['database_url'] = database.url

    command.downgrade(config, 'base')

    engine = create_engine(database.url)
    try:
        assert inspect(engine).get_table_names() == ['alembic_version']
    finally:
        engine.dispose()


def test_create_and_retrieve_received_job_by_both_identifiers(
    repository: ProcessingJobRepository,
) -> None:
    job = received_job()

    assert repository.create(job) == job
    assert repository.get_by_processing_id(PROCESSING_ID) == job
    assert repository.get_by_source_event_id('event-123') == job


def test_queries_return_none_for_missing_jobs(
    repository: ProcessingJobRepository,
) -> None:
    assert repository.get_by_processing_id(UUID(int=0)) is None
    assert repository.get_by_source_event_id('missing-event') is None


def test_correlation_id_none_is_preserved(
    repository: ProcessingJobRepository,
) -> None:
    job = received_job(correlation_id=None)

    repository.create(job)

    recovered = repository.get_by_processing_id(PROCESSING_ID)
    assert recovered is not None
    assert recovered.correlation_id is None


@pytest.mark.parametrize(
    'status',
    [
        ProcessingStatus.PROCESSING,
        ProcessingStatus.COMPLETED,
        ProcessingStatus.FAILED,
    ],
)
def test_lifecycle_snapshots_round_trip(
    repository: ProcessingJobRepository,
    status: ProcessingStatus,
) -> None:
    job = job_in_status(status)
    repository.create(received_job())

    saved = repository.save(job)
    recovered = repository.get_by_processing_id(PROCESSING_ID)

    assert saved == job
    assert recovered == job
    assert recovered is not None
    assert recovered.status is status
    assert recovered.attempts == 1
    assert recovered.failure_reason == job.failure_reason


def test_recovered_job_can_transition_and_save_as_domain_snapshot(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job())
    recovered = repository.get_by_source_event_id('event-123')
    assert recovered is not None

    processing = recovered.start_processing(at=STARTED_AT)
    repository.save(processing)

    assert repository.get_by_processing_id(PROCESSING_ID) == processing


def test_utc_timestamps_remain_aware_and_normalized_after_round_trip(
    repository: ProcessingJobRepository,
) -> None:
    offset = timezone(timedelta(hours=2))
    job = ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id='event-123',
        document_id='document-456',
        correlation_id='correlation-789',
        created_at=datetime(2026, 1, 1, 14, tzinfo=offset),
    )
    processing = job.start_processing(
        at=datetime(2026, 1, 1, 14, 1, tzinfo=offset)
    )
    completed = processing.complete(
        at=datetime(2026, 1, 1, 14, 2, tzinfo=offset)
    )
    repository.create(job)
    repository.save(processing)
    repository.save(completed)

    recovered = repository.get_by_processing_id(PROCESSING_ID)

    assert recovered == completed
    assert recovered is not None
    assert recovered.created_at == CREATED_AT
    assert recovered.updated_at == FINISHED_AT
    assert recovered.started_at == STARTED_AT
    assert recovered.finished_at == FINISHED_AT
    for timestamp in (
        recovered.created_at,
        recovered.updated_at,
        recovered.started_at,
        recovered.finished_at,
    ):
        assert timestamp is not None
        assert timestamp.tzinfo is UTC


def test_failed_snapshot_preserves_failure_reason(
    repository: ProcessingJobRepository,
) -> None:
    failed = job_in_status(ProcessingStatus.FAILED)
    repository.create(received_job())

    repository.save(failed)

    recovered = repository.get_by_processing_id(PROCESSING_ID)
    assert recovered is not None
    assert recovered.status is ProcessingStatus.FAILED
    assert recovered.failure_reason == 'Unsupported format'
    assert recovered.finished_at == FINISHED_AT


def test_duplicate_source_event_raises_explicit_error_and_repository_recovers(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job())

    with pytest.raises(DuplicateSourceEventError) as error:
        repository.create(
            received_job(
                processing_id=UUID('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
            )
        )

    assert error.value.source_event_id == 'event-123'
    assert isinstance(error.value.__cause__, IntegrityError)
    assert repository.get_by_source_event_id('event-123') == received_job()
    other_job = received_job(
        processing_id=UUID('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'),
        source_event_id='event-456',
    )
    assert repository.create(other_job) == other_job


def test_other_integrity_errors_are_not_reported_as_duplicate_events(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job())
    conflicting_id = received_job(source_event_id='event-456')

    with pytest.raises(PersistenceIntegrityError):
        repository.create(conflicting_id)


def test_save_rejects_an_unpersisted_job(
    repository: ProcessingJobRepository,
) -> None:
    with pytest.raises(ProcessingJobNotFoundError):
        repository.save(received_job())


def test_reconstruction_reapplies_domain_invariants(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job())
    with closing(sqlite3.connect(database.path)) as connection:
        connection.execute(
            "UPDATE processing_jobs SET status = 'COMPLETED' "
            'WHERE processing_id = ?',
            (PROCESSING_ID.hex,),
        )
        connection.commit()

    with pytest.raises(ProcessingDomainError):
        repository.get_by_processing_id(PROCESSING_ID)


def test_database_url_uses_environment_override_and_local_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABASE_URL_ENV_VAR, raising=False)
    assert get_database_url() == DEFAULT_DATABASE_URL

    monkeypatch.setenv(DATABASE_URL_ENV_VAR, 'sqlite:///override.sqlite3')
    assert get_database_url() == 'sqlite:///override.sqlite3'


def test_engine_can_be_created_from_an_explicit_url(tmp_path: Path) -> None:
    database_path = tmp_path / 'explicit.sqlite3'
    engine: Engine = create_database_engine(f'sqlite:///{database_path}')
    try:
        assert str(engine.url) == f'sqlite:///{database_path}'
    finally:
        engine.dispose()
