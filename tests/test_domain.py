from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest

from docpipe_processing.domain import (
    InvalidProcessingTransition,
    ProcessingDomainError,
    ProcessingJob,
    ProcessingStatus,
)

pytestmark = pytest.mark.unit

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')
CREATED_AT = datetime(2026, 1, 1, 12, tzinfo=UTC)
STARTED_AT = datetime(2026, 1, 1, 12, 1, tzinfo=UTC)
FINISHED_AT = datetime(2026, 1, 1, 12, 2, tzinfo=UTC)


def received_job() -> ProcessingJob:
    return ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id='event-123',
        document_id='document-456',
        correlation_id='correlation-789',
        created_at=CREATED_AT,
    )


def job_in_status(status: ProcessingStatus) -> ProcessingJob:
    job = received_job()
    if status is ProcessingStatus.RECEIVED:
        return job

    job = job.start_processing(at=STARTED_AT)
    if status is ProcessingStatus.PROCESSING:
        return job
    if status is ProcessingStatus.COMPLETED:
        return job.complete(at=FINISHED_AT)
    return job.fail(reason='Processing failed', at=FINISHED_AT)


def invoke_operation(job: ProcessingJob, operation: str) -> ProcessingJob:
    if operation == 'start':
        return job.start_processing(at=FINISHED_AT)
    if operation == 'complete':
        return job.complete(at=FINISHED_AT)
    return job.fail(reason='Processing failed', at=FINISHED_AT)


def test_create_job_preserves_identity_and_initial_state() -> None:
    job = received_job()

    assert job.processing_id == PROCESSING_ID
    assert job.source_event_id == 'event-123'
    assert job.document_id == 'document-456'
    assert job.correlation_id == 'correlation-789'
    assert job.status is ProcessingStatus.RECEIVED
    assert job.attempts == 0
    assert job.created_at == CREATED_AT
    assert job.updated_at == CREATED_AT
    assert job.started_at is None
    assert job.finished_at is None
    assert job.failure_reason is None


def test_create_job_allows_missing_correlation_id() -> None:
    job = ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id='event-123',
        document_id='document-456',
        created_at=CREATED_AT,
    )

    assert job.correlation_id is None


def test_received_job_starts_processing_and_increments_attempts() -> None:
    received = received_job()

    processing = received.start_processing(at=STARTED_AT)

    assert received.status is ProcessingStatus.RECEIVED
    assert processing.status is ProcessingStatus.PROCESSING
    assert processing.attempts == 1
    assert processing.started_at == STARTED_AT
    assert processing.updated_at == STARTED_AT
    assert processing.finished_at is None
    assert processing.failure_reason is None


def test_transition_time_is_normalized_to_utc() -> None:
    offset_time = datetime(
        2026,
        1,
        1,
        14,
        1,
        tzinfo=timezone(timedelta(hours=2)),
    )

    processing = received_job().start_processing(at=offset_time)

    assert processing.started_at == STARTED_AT
    assert processing.updated_at.tzinfo is UTC


def test_processing_job_can_complete() -> None:
    processing = job_in_status(ProcessingStatus.PROCESSING)

    completed = processing.complete(at=FINISHED_AT)

    assert completed.status is ProcessingStatus.COMPLETED
    assert completed.attempts == 1
    assert completed.started_at == STARTED_AT
    assert completed.finished_at == FINISHED_AT
    assert completed.updated_at == FINISHED_AT
    assert completed.failure_reason is None
    assert processing.status is ProcessingStatus.PROCESSING


def test_processing_job_can_fail_with_a_summarized_reason() -> None:
    failed = job_in_status(ProcessingStatus.PROCESSING).fail(
        reason='  Unsupported document format  ',
        at=FINISHED_AT,
    )

    assert failed.status is ProcessingStatus.FAILED
    assert failed.started_at == STARTED_AT
    assert failed.finished_at == FINISHED_AT
    assert failed.updated_at == FINISHED_AT
    assert failed.failure_reason == 'Unsupported document format'


@pytest.mark.parametrize(
    ('status', 'operation'),
    [
        (ProcessingStatus.RECEIVED, 'complete'),
        (ProcessingStatus.RECEIVED, 'fail'),
        (ProcessingStatus.PROCESSING, 'start'),
        (ProcessingStatus.COMPLETED, 'start'),
        (ProcessingStatus.COMPLETED, 'fail'),
        (ProcessingStatus.COMPLETED, 'complete'),
        (ProcessingStatus.FAILED, 'start'),
        (ProcessingStatus.FAILED, 'complete'),
        (ProcessingStatus.FAILED, 'fail'),
    ],
)
def test_invalid_transitions_are_rejected(
    status: ProcessingStatus,
    operation: str,
) -> None:
    job = job_in_status(status)

    with pytest.raises(InvalidProcessingTransition):
        invoke_operation(job, operation)


@pytest.mark.parametrize(
    ('field_name', 'value'),
    [
        ('source_event_id', ''),
        ('document_id', '  '),
        ('correlation_id', ''),
    ],
)
def test_required_identifiers_cannot_be_empty(
    field_name: str,
    value: str,
) -> None:
    fields = {
        'processing_id': PROCESSING_ID,
        'source_event_id': 'event-123',
        'document_id': 'document-456',
        'created_at': CREATED_AT,
    }
    fields[field_name] = value

    with pytest.raises(ProcessingDomainError):
        ProcessingJob.create(**fields)  # type: ignore[arg-type]


def test_job_creation_rejects_naive_timestamp() -> None:
    with pytest.raises(ProcessingDomainError, match='timezone-aware'):
        ProcessingJob.create(
            processing_id=PROCESSING_ID,
            source_event_id='event-123',
            document_id='document-456',
            created_at=datetime(2026, 1, 1, 12),
        )


@pytest.mark.parametrize('attempts', [-1, 1.5, True])
def test_attempts_must_be_a_non_negative_integer(attempts: object) -> None:
    with pytest.raises(ProcessingDomainError, match='non-negative integer'):
        replace(received_job(), attempts=attempts)  # type: ignore[arg-type]


def test_starting_before_creation_is_rejected() -> None:
    with pytest.raises(ProcessingDomainError, match='precede updated_at'):
        received_job().start_processing(
            at=datetime(2026, 1, 1, 11, 59, tzinfo=UTC)
        )


def test_completion_before_processing_started_is_rejected() -> None:
    processing = job_in_status(ProcessingStatus.PROCESSING)

    with pytest.raises(ProcessingDomainError, match='precede updated_at'):
        processing.complete(at=CREATED_AT)


def test_failure_requires_a_non_empty_reason() -> None:
    processing = job_in_status(ProcessingStatus.PROCESSING)

    with pytest.raises(
        ProcessingDomainError,
        match='failure reason is required',
    ):
        processing.fail(reason='  ', at=FINISHED_AT)


def test_job_state_cannot_be_changed_directly() -> None:
    job = received_job()

    with pytest.raises(FrozenInstanceError):
        job.status = ProcessingStatus.PROCESSING  # type: ignore[misc]
