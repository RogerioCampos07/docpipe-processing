"""Persistence tests on isolated PostgreSQL schemas with real migrations."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from threading import Barrier
from typing import Iterator
from uuid import UUID

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import DateTime, create_engine, event, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from docpipe_processing.contracts import (
    DocumentReceivedData,
    DocumentReceivedEvent,
)
from docpipe_processing.domain import (
    ProcessingDomainError,
    ProcessingJob,
    ProcessingStatus,
)
from docpipe_processing.persistence import (
    Base,
    DuplicateSourceEventError,
    PersistenceError,
    PersistenceIntegrityError,
    PersistenceUnavailableError,
    ProcessingJobIdentityError,
    ProcessingJobNotFoundError,
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)
from docpipe_processing.processing_input import ProcessingInput
from tests.conftest import DatabaseFixture

pytestmark = pytest.mark.persistence

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')
CREATED_AT = datetime(2026, 1, 1, 12, tzinfo=UTC)
STARTED_AT = datetime(2026, 1, 1, 12, 1, tzinfo=UTC)
FINISHED_AT = datetime(2026, 1, 1, 12, 2, tzinfo=UTC)
ALEMBIC_CONFIG = Path(__file__).parents[1] / 'alembic.ini'


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
    database.migrate('0001_processing_jobs', downgrade=True)
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
        unique = inspector.get_unique_constraints('processing_jobs')
        assert len(unique) == 1
        assert unique[0]['name'] == 'uq_processing_jobs_source_event_id'
        assert unique[0]['column_names'] == ['source_event_id']
        with engine.connect() as connection:
            revision: str = connection.exec_driver_sql(
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

    assert repository.create(job, input=durable_input()) == job
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

    repository.create(job, input=durable_input())

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
    repository.create(received_job(), input=durable_input())

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
    repository.create(received_job(), input=durable_input())
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
    repository.create(job, input=durable_input())
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
    repository.create(received_job(), input=durable_input())

    repository.save(failed)

    recovered = repository.get_by_processing_id(PROCESSING_ID)
    assert recovered is not None
    assert recovered.status is ProcessingStatus.FAILED
    assert recovered.failure_reason == 'Unsupported format'
    assert recovered.finished_at == FINISHED_AT


def test_duplicate_source_event_raises_explicit_error_and_repository_recovers(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job(), input=durable_input())

    with pytest.raises(DuplicateSourceEventError) as error:
        repository.create(
            received_job(
                processing_id=UUID('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa')
            ),
            input=durable_input(),
        )

    assert error.value.source_event_id == 'event-123'
    assert isinstance(error.value.__cause__, IntegrityError)
    assert repository.get_by_source_event_id('event-123') == received_job()
    other_job = received_job(
        processing_id=UUID('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'),
        source_event_id='event-456',
    )
    assert repository.create(other_job, input=durable_input()) == other_job


def test_other_integrity_errors_are_not_reported_as_duplicate_events(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job(), input=durable_input())
    conflicting_id = received_job(source_event_id='event-456')

    with pytest.raises(PersistenceIntegrityError):
        repository.create(conflicting_id, input=durable_input())


def test_save_rejects_an_unpersisted_job(
    repository: ProcessingJobRepository,
) -> None:
    with pytest.raises(ProcessingJobNotFoundError):
        repository.save(received_job())


@pytest.mark.parametrize(
    'field_name', ['source_event_id', 'document_id', 'correlation_id']
)
def test_save_rejects_identity_changes_without_partial_updates(
    repository: ProcessingJobRepository,
    field_name: str,
) -> None:
    original = received_job()
    repository.create(original, input=durable_input())
    processing = original.start_processing(at=STARTED_AT)
    if field_name == 'source_event_id':
        changed = replace(processing, source_event_id='event-456')
    elif field_name == 'document_id':
        changed = replace(processing, document_id='document-B')
    else:
        changed = replace(processing, correlation_id=None)

    with pytest.raises(ProcessingJobIdentityError) as error:
        repository.save(changed)

    assert error.value.processing_id == PROCESSING_ID
    assert error.value.fields == (field_name,)
    assert repository.get_by_processing_id(PROCESSING_ID) == original
    assert repository.get_by_source_event_id('event-456') is None
    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )
    with pytest.raises(DuplicateSourceEventError):
        repository.create(
            received_job(processing_id=UUID(int=3)), input=durable_input()
        )


def test_save_rejects_changed_created_at_and_state_together(
    repository: ProcessingJobRepository,
) -> None:
    original = received_job()
    repository.create(original, input=durable_input())
    processing = original.start_processing(at=STARTED_AT)
    earlier = CREATED_AT - timedelta(minutes=1)
    changed = replace(processing, created_at=earlier)

    with pytest.raises(ProcessingJobIdentityError) as error:
        repository.save(changed)

    assert error.value.fields == ('created_at',)
    assert repository.get_by_processing_id(PROCESSING_ID) == original
    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )
    with pytest.raises(DuplicateSourceEventError):
        repository.create(
            received_job(processing_id=UUID(int=4)), input=durable_input()
        )


def test_save_accepts_created_at_represented_in_another_timezone(
    repository: ProcessingJobRepository,
) -> None:
    original = received_job()
    repository.create(original, input=durable_input())
    offset_created_at = datetime(
        2026, 1, 1, 14, tzinfo=timezone(timedelta(hours=2))
    )
    equivalent = ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id=original.source_event_id,
        document_id=original.document_id,
        correlation_id=original.correlation_id,
        created_at=offset_created_at,
    )

    assert repository.save(equivalent) == original
    assert repository.get_by_processing_id(PROCESSING_ID) == original
    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )


def test_save_of_legacy_job_does_not_require_or_create_input(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    database.migrate('0001_processing_jobs', downgrade=True)
    legacy = received_job()
    engine = create_database_engine(database.url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    'INSERT INTO processing_jobs '
                    '(processing_id, source_event_id, document_id, '
                    'correlation_id, status, attempts, created_at, '
                    'updated_at) '
                    'VALUES (:processing_id, :source_event_id, :document_id, '
                    ':correlation_id, :status, :attempts, :created_at, '
                    ':updated_at)'
                ),
                {
                    'processing_id': legacy.processing_id,
                    'source_event_id': legacy.source_event_id,
                    'document_id': legacy.document_id,
                    'correlation_id': legacy.correlation_id,
                    'status': legacy.status.value,
                    'attempts': legacy.attempts,
                    'created_at': legacy.created_at,
                    'updated_at': legacy.updated_at,
                },
            )
        database.migrate('head')

        processing = legacy.start_processing(at=STARTED_AT)
        repository.save(processing)

        assert repository.get_by_processing_id(PROCESSING_ID) == processing
        assert repository.get_input_by_processing_id(PROCESSING_ID) is None
    finally:
        engine.dispose()


def test_reconstruction_reapplies_domain_invariants(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job(), input=durable_input())
    engine = create_database_engine(database.url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE processing_jobs SET status = 'COMPLETED' "
                    'WHERE processing_id = :processing_id'
                ),
                {'processing_id': PROCESSING_ID},
            )
    finally:
        engine.dispose()

    with pytest.raises(ProcessingDomainError):
        repository.get_by_processing_id(PROCESSING_ID)


def durable_input() -> ProcessingInput:
    return ProcessingInput(
        storage_key='opaque-object-key',
        media_type='application/pdf; charset=binary',
        size_bytes=128,
        sha256='abcdef0123456789' * 4,
    )


def received_event() -> DocumentReceivedEvent:
    return DocumentReceivedEvent(
        event_id='event-123',
        event_type='document.received',
        event_version=1,
        document_id='document-456',
        occurred_at=CREATED_AT,
        correlation_id='correlation-789',
        data=DocumentReceivedData(
            storage_key='opaque-object-key',
            media_type='application/pdf; charset=binary',
            size_bytes=128,
            sha256='ABCDEF0123456789' * 4,
        ),
    )


def test_durable_input_round_trip_and_save_preserves_input(
    repository: ProcessingJobRepository,
) -> None:
    job = received_job()
    input = durable_input().model_copy(update={'size_bytes': 10**50})
    repository.create(job, input=input)
    repository.save(job.start_processing(at=STARTED_AT))
    assert repository.get_input_by_processing_id(PROCESSING_ID) == input
    assert (
        repository.get_input_by_source_event_id(job.source_event_id) == input
    )
    assert repository.get_input_by_processing_id(UUID(int=0)) is None
    assert repository.get_input_by_source_event_id('missing') is None


def test_valid_input_from_event_is_revalidated_and_normalized_before_create(
    repository: ProcessingJobRepository,
) -> None:
    input = ProcessingInput.from_event(received_event())

    repository.create(received_job(), input=input)

    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )


def assert_invalid_input_rejected_before_session(
    repository: ProcessingJobRepository,
    monkeypatch: pytest.MonkeyPatch,
    input: ProcessingInput,
) -> None:
    def unexpected_session() -> Session:
        raise AssertionError('Invalid input opened a persistence session.')

    with monkeypatch.context() as patcher:
        patcher.setattr(repository, '_session_factory', unexpected_session)
        with pytest.raises(PersistenceIntegrityError) as error:
            repository.create(received_job(), input=input)

    assert 'opaque-object-key' not in str(error.value)
    assert repository.get_by_processing_id(PROCESSING_ID) is None
    assert repository.get_input_by_source_event_id('event-123') is None
    repository.create(received_job(), input=durable_input())
    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )


@pytest.mark.parametrize(
    'updates',
    [
        pytest.param({'storage_key': ''}, id='empty-key'),
        pytest.param({'storage_key': ' \t '}, id='blank-key'),
        pytest.param({'media_type': 'invalid'}, id='invalid-MIME'),
        pytest.param({'size_bytes': -1}, id='negative-size'),
        pytest.param({'size_bytes': True}, id='boolean-size'),
        pytest.param({'size_bytes': '128'}, id='string-size'),
        pytest.param({'sha256': 'not-a-sha256'}, id='invalid-SHA256'),
        pytest.param(
            {
                'storage_key': None,
                'media_type': None,
                'size_bytes': None,
                'sha256': None,
            },
            id='all-fields-null',
        ),
    ],
)
def test_create_revalidates_model_copy_before_opening_session(
    repository: ProcessingJobRepository,
    monkeypatch: pytest.MonkeyPatch,
    updates: dict[str, object],
) -> None:
    invalid = durable_input().model_copy(update=updates)

    assert_invalid_input_rejected_before_session(
        repository, monkeypatch, invalid
    )


@pytest.mark.parametrize('missing_field', ['storage_key', 'media_type'])
def test_create_rejects_incomplete_model_construct_before_opening_session(
    repository: ProcessingJobRepository,
    monkeypatch: pytest.MonkeyPatch,
    missing_field: str,
) -> None:
    values = durable_input().model_dump()
    values.pop(missing_field)
    invalid = ProcessingInput.model_construct(**values)

    assert_invalid_input_rejected_before_session(
        repository, monkeypatch, invalid
    )


def test_create_revalidates_valid_model_construct_and_normalizes_sha256(
    repository: ProcessingJobRepository,
) -> None:
    input = ProcessingInput.model_construct(
        storage_key='opaque-object-key',
        media_type='application/pdf; charset=binary',
        size_bytes=128,
        sha256='ABCDEF0123456789' * 4,
    )

    repository.create(received_job(), input=input)

    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )


def test_missing_input_cannot_create_a_new_acceptance(
    repository: ProcessingJobRepository,
) -> None:
    with pytest.raises(TypeError):
        repository.create(received_job())  # type: ignore[call-arg]
    assert repository.get_by_processing_id(PROCESSING_ID) is None


def test_conflicting_duplicate_never_overwrites_persisted_input(
    repository: ProcessingJobRepository,
) -> None:
    repository.create(received_job(), input=durable_input())
    changed = durable_input().model_copy(
        update={'storage_key': 'other-object'}
    )
    with pytest.raises(DuplicateSourceEventError):
        repository.create(
            received_job(processing_id=UUID(int=2)), input=changed
        )
    assert (
        repository.get_input_by_processing_id(PROCESSING_ID) == durable_input()
    )
    assert repository.get_by_processing_id(UUID(int=2)) is None


def test_new_engine_and_factory_recover_job_and_input(
    database: DatabaseFixture,
) -> None:
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))
    repository.create(received_job(), input=durable_input())
    engine.dispose()
    replacement = create_database_engine(database.url)
    try:
        recovered = ProcessingJobRepository(
            create_session_factory(replacement)
        )
        assert recovered.get_by_processing_id(PROCESSING_ID) == received_job()
        assert (
            recovered.get_input_by_processing_id(PROCESSING_ID)
            == durable_input()
        )
    finally:
        replacement.dispose()


def test_failure_after_insert_rolls_back_entire_acceptance(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:

    class FailingSession(Session):
        pass

    def fail_after_insert(session: Session, context: object) -> None:
        del session, context
        raise RuntimeError('injected failure after flush')

    engine = create_database_engine(database.url)
    event.listen(FailingSession, 'after_flush_postexec', fail_after_insert)
    try:
        failing = ProcessingJobRepository(
            sessionmaker(bind=engine, class_=FailingSession)
        )
        with pytest.raises(RuntimeError, match='injected failure'):
            failing.create(received_job(), input=durable_input())
        assert repository.get_by_processing_id(PROCESSING_ID) is None
        assert repository.get_input_by_processing_id(PROCESSING_ID) is None
        repository.create(received_job(), input=durable_input())
    finally:
        event.remove(FailingSession, 'after_flush_postexec', fail_after_insert)
        engine.dispose()


@pytest.mark.parametrize(
    ('assignment', 'constraint'),
    [
        ('storage_key = NULL', 'ck_processing_jobs_input_complete'),
        ('size_bytes = -1', 'ck_processing_jobs_input_size'),
        ('size_bytes = 1.5', 'ck_processing_jobs_input_size'),
        ("size_bytes = 'NaN'::numeric", 'ck_processing_jobs_input_size'),
        ("size_bytes = 'Infinity'::numeric", 'ck_processing_jobs_input_size'),
        ("sha256 = 'invalid'", 'ck_processing_jobs_input_sha256'),
        ('attempts = -1', 'ck_processing_jobs_attempts'),
    ],
)
def test_postgresql_constraints_reject_invalid_rows_and_rollback(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
    assignment: str,
    constraint: str,
) -> None:
    repository.create(received_job(), input=durable_input())
    engine = create_database_engine(database.url)
    try:
        with (
            pytest.raises(IntegrityError) as error,
            engine.begin() as connection,
        ):
            # Only static SQL assignments from this parametrization are used.
            connection.execute(
                text(f'UPDATE processing_jobs SET {assignment}')
            )
        assert error.value.orig is not None
        assert getattr(error.value.orig, 'diag').constraint_name == constraint
        assert repository.get_by_processing_id(PROCESSING_ID) == received_job()
        assert (
            repository.get_input_by_processing_id(PROCESSING_ID)
            == durable_input()
        )
    finally:
        engine.dispose()


def test_invalid_input_is_rejected_before_insert(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    # The repository rejects invalid values before creating a session.
    invalid = durable_input().model_copy(update={'sha256': 'invalid'})
    with pytest.raises(PersistenceIntegrityError):
        repository.create(received_job(), input=invalid)
    assert repository.get_by_processing_id(PROCESSING_ID) is None
    assert repository.get_input_by_source_event_id('event-123') is None
    repository.create(received_job(), input=durable_input())


def test_postgresql_sha_constraint_rejects_direct_invalid_insert(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    job = received_job()
    engine = create_database_engine(database.url)
    try:
        with (
            pytest.raises(IntegrityError) as error,
            engine.begin() as connection,
        ):
            connection.execute(
                text(
                    'INSERT INTO processing_jobs '
                    '(processing_id, source_event_id, document_id, status, '
                    'attempts, created_at, updated_at, storage_key, '
                    'media_type, size_bytes, sha256) VALUES '
                    '(:processing_id, :source_event_id, :document_id, '
                    ':status, '
                    ':attempts, :created_at, :updated_at, :storage_key, '
                    ':media_type, :size_bytes, :sha256)'
                ),
                {
                    'processing_id': job.processing_id,
                    'source_event_id': job.source_event_id,
                    'document_id': job.document_id,
                    'status': job.status.value,
                    'attempts': job.attempts,
                    'created_at': job.created_at,
                    'updated_at': job.updated_at,
                    'storage_key': 'opaque-object-key',
                    'media_type': 'application/pdf',
                    'size_bytes': 128,
                    'sha256': 'invalid',
                },
            )
        assert error.value.orig is not None
        assert getattr(error.value.orig, 'diag').constraint_name == (
            'ck_processing_jobs_input_sha256'
        )
        assert repository.get_by_processing_id(PROCESSING_ID) is None
        assert repository.get_input_by_processing_id(PROCESSING_ID) is None
    finally:
        engine.dispose()


def test_concurrent_event_creation_is_protected_by_postgresql(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:

    barrier = Barrier(2)

    def create(index: int) -> str:
        engine = create_database_engine(database.url)
        try:
            competing = ProcessingJobRepository(create_session_factory(engine))
            barrier.wait(timeout=10)
            try:
                competing.create(
                    received_job(processing_id=UUID(int=index)),
                    input=durable_input(),
                )
                return 'created'
            except DuplicateSourceEventError:
                return 'duplicate'
        finally:
            engine.dispose()

    with ThreadPoolExecutor(max_workers=2) as executor:
        assert sorted(executor.map(create, [1, 2])) == ['created', 'duplicate']
    job = repository.get_by_source_event_id('event-123')
    assert job is not None
    assert job.attempts == 0
    assert (
        repository.get_input_by_processing_id(job.processing_id)
        == durable_input()
    )


@pytest.mark.parametrize('status', list(ProcessingStatus))
def test_migration_preserves_legacy_job_without_inventing_input(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
    status: ProcessingStatus,
) -> None:
    database.migrate('0001_processing_jobs', downgrade=True)
    engine = create_database_engine(database.url)
    legacy_job = (
        received_job()
        if status is ProcessingStatus.RECEIVED
        else job_in_status(status)
    )
    values = asdict(legacy_job)
    values['status'] = status.value
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    'INSERT INTO processing_jobs '
                    '(processing_id, source_event_id, document_id, '
                    'correlation_id, '
                    'status, attempts, created_at, updated_at, started_at, '
                    'finished_at, failure_reason) VALUES '
                    '(:processing_id, :source_event_id, :document_id, '
                    ':correlation_id, :status, :attempts, :created_at, '
                    ':updated_at, :started_at, :finished_at, :failure_reason)'
                ),
                values,
            )
        database.migrate('head')
        assert repository.get_by_processing_id(PROCESSING_ID) == legacy_job
        assert repository.get_input_by_processing_id(PROCESSING_ID) is None
        database.migrate('0001_processing_jobs', downgrade=True)
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text('SELECT source_event_id FROM processing_jobs')
                ).scalar_one()
                == 'event-123'
            )
        database.migrate('head')
        assert repository.get_input_by_processing_id(PROCESSING_ID) is None
    finally:
        engine.dispose()


def test_schema_matches_metadata_and_migration_history(
    database: DatabaseFixture,
) -> None:

    engine = create_database_engine(database.url)
    try:
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            assert compare_metadata(context, Base.metadata) == []
            assert (
                connection.execute(
                    text('SELECT version_num FROM alembic_version')
                ).scalar_one()
                == '0002_processing_input'
            )
        inspector = inspect(engine)
        columns = {
            c['name']: c for c in inspector.get_columns('processing_jobs')
        }
        for name in ('storage_key', 'media_type', 'size_bytes', 'sha256'):
            assert columns[name]['nullable'] is True
        assert str(columns['processing_id']['type']) == 'UUID'
        timestamp_type = columns['created_at']['type']
        assert isinstance(timestamp_type, DateTime)
        assert timestamp_type.timezone is True
    finally:
        engine.dispose()


def test_database_unavailable_fails_without_sqlite_fallback() -> None:

    engine = create_database_engine(
        'postgresql+psycopg://user:private-secret@127.0.0.1:1/processing_test'
    )
    repository = ProcessingJobRepository(create_session_factory(engine))
    try:
        with pytest.raises(PersistenceUnavailableError) as error:
            repository.create(received_job(), input=durable_input())
        assert 'private-secret' not in str(error.value)
        assert 'postgresql' not in str(error.value)
        with pytest.raises(PersistenceUnavailableError):
            repository.get_by_processing_id(PROCESSING_ID)
    finally:
        engine.dispose()


def test_unrepresentable_postgresql_text_is_controlled_and_not_accepted(
    repository: ProcessingJobRepository,
) -> None:
    input = ProcessingInput.model_validate({
        **durable_input().model_dump(),
        'storage_key': 'opaque\x00key',
    })
    with pytest.raises(PersistenceIntegrityError):
        repository.create(received_job(), input=input)
    assert repository.get_by_processing_id(PROCESSING_ID) is None


def test_missing_schema_is_not_misclassified_as_integrity_or_duplicate(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
) -> None:
    engine = create_database_engine(database.url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    'ALTER TABLE processing_jobs '
                    'RENAME TO unavailable_processing_jobs'
                )
            )
        with pytest.raises(PersistenceError) as error:
            repository.get_by_processing_id(PROCESSING_ID)
        assert type(error.value) is PersistenceError
    finally:
        engine.dispose()
