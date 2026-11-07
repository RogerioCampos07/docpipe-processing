"""Unit tests for durable event acceptance with a small in-memory store."""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Callable
from uuid import UUID

import pytest

from docpipe_processing.acceptance import (
    AcceptanceResult,
    AcceptanceStoreUnavailable,
    AcceptanceUnavailableError,
    ConflictingEventError,
    EventAcceptance,
    IncompleteLegacyEventError,
    PermanentEventError,
)
from docpipe_processing.domain import ProcessingJob
from docpipe_processing.processing_input import ProcessingInput

pytestmark = pytest.mark.unit
EXPECTED_RECONCILIATION_READS = 2

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')
CREATED_AT = datetime(2026, 1, 1, 12, tzinfo=UTC)


def event_payload(**updates: object) -> bytes:
    payload: dict[str, object] = {
        'event_id': 'evt-synthetic-0001',
        'event_type': 'document.received',
        'event_version': 1,
        'occurred_at': '2026-01-01T12:00:00Z',
        'correlation_id': None,
        'document_id': 'doc-synthetic-0001',
        'data': {
            'storage_key': 'opaque-object-key',
            'media_type': 'application/pdf',
            'size_bytes': 128,
            'sha256': 'a' * 64,
        },
    }
    payload.update(updates)
    return json.dumps(payload).encode()


@dataclass
class FakeAcceptanceStore:
    rows: dict[str, tuple[ProcessingJob, ProcessingInput | None]] = field(
        default_factory=dict
    )
    create_failure: Exception | None = None
    commit_then_fail: bool = False
    create_calls: int = 0
    read_calls: int = 0

    def create(
        self, job: ProcessingJob, *, input: ProcessingInput
    ) -> ProcessingJob:
        self.create_calls += 1
        self.rows[job.source_event_id] = (job, input)
        if self.create_failure is not None:
            if not self.commit_then_fail:
                self.rows.pop(job.source_event_id)
            raise self.create_failure
        return job

    def get_acceptance_by_source_event_id(
        self, source_event_id: str
    ) -> tuple[ProcessingJob, ProcessingInput | None] | None:
        self.read_calls += 1
        return self.rows.get(source_event_id)


def acceptance(
    store: FakeAcceptanceStore,
    *,
    new_id: Callable[[], UUID] = lambda: PROCESSING_ID,
) -> EventAcceptance:
    return EventAcceptance(store, now=lambda: CREATED_AT, new_id=new_id)


def test_new_event_creates_received_job_with_durable_input() -> None:
    store = FakeAcceptanceStore()

    result = acceptance(store).accept(event_payload())

    assert result.result is AcceptanceResult.ACCEPTED
    assert result.job.status.value == 'RECEIVED'
    assert result.job.attempts == 0
    assert result.job.source_event_id == 'evt-synthetic-0001'
    assert store.rows['evt-synthetic-0001'][1] == ProcessingInput(
        storage_key='opaque-object-key',
        media_type='application/pdf',
        size_bytes=128,
        sha256='a' * 64,
    )


def test_equivalent_redelivery_ignores_occurred_at_and_preserves_job() -> None:
    store = FakeAcceptanceStore()
    original = acceptance(store).accept(event_payload())
    advanced = original.job.start_processing(
        at=datetime(2026, 1, 1, 12, 1, tzinfo=UTC)
    )
    store.rows['evt-synthetic-0001'] = (
        advanced,
        store.rows['evt-synthetic-0001'][1],
    )
    payload = json.loads(event_payload())
    payload['occurred_at'] = '2026-02-01T00:00:00Z'

    result = acceptance(store, new_id=lambda: UUID(int=2)).accept(
        json.dumps(payload).encode()
    )

    assert result.result is AcceptanceResult.DUPLICATE
    assert result.job is advanced
    assert result.job.attempts == 1
    assert store.create_calls == 1


@pytest.mark.parametrize(
    ('field', 'value'),
    [
        ('document_id', 'different-document'),
        ('correlation_id', 'different-correlation'),
        ('storage_key', 'different-key'),
        ('media_type', 'application/octet-stream'),
        ('size_bytes', 129),
        ('sha256', 'b' * 64),
    ],
)
def test_event_id_with_changed_persisted_content_is_a_conflict(
    field: str, value: object
) -> None:
    store = FakeAcceptanceStore()
    acceptance(store).accept(event_payload())
    payload = json.loads(event_payload())
    if field in {'document_id', 'correlation_id'}:
        payload[field] = value
    else:
        payload['data'][field] = value

    with pytest.raises(ConflictingEventError):
        acceptance(store).accept(json.dumps(payload).encode())

    assert store.create_calls == 1


def test_legacy_job_without_input_is_not_accepted_as_durable() -> None:
    store = FakeAcceptanceStore()
    result = acceptance(store).accept(event_payload())
    store.rows['evt-synthetic-0001'] = (result.job, None)

    with pytest.raises(IncompleteLegacyEventError):
        acceptance(store).accept(event_payload())

    assert store.create_calls == 1


def test_invalid_contract_is_permanent_without_storage_effect() -> None:
    store = FakeAcceptanceStore()
    payload = json.loads(event_payload())
    payload['event_version'] = 2

    with pytest.raises(PermanentEventError):
        acceptance(store).accept(json.dumps(payload).encode())

    assert store.read_calls == 0
    assert store.create_calls == 0


def test_commit_response_loss_reconciles_equivalent_row() -> None:
    store = FakeAcceptanceStore(
        create_failure=AcceptanceStoreUnavailable('synthetic timeout'),
        commit_then_fail=True,
    )

    result = acceptance(store).accept(event_payload())

    assert result.result is AcceptanceResult.DUPLICATE
    assert result.job.status.value == 'RECEIVED'
    assert store.create_calls == 1
    assert store.read_calls == EXPECTED_RECONCILIATION_READS


def test_reconciliation_failure_stays_unaccepted() -> None:
    class ReconciliationUnavailableStore(FakeAcceptanceStore):
        def get_acceptance_by_source_event_id(
            self, source_event_id: str
        ) -> tuple[ProcessingJob, ProcessingInput | None] | None:
            self.read_calls += 1
            if self.read_calls == EXPECTED_RECONCILIATION_READS:
                raise AcceptanceStoreUnavailable('synthetic database outage')
            return self.rows.get(source_event_id)

    store = ReconciliationUnavailableStore(
        create_failure=AcceptanceStoreUnavailable('synthetic commit timeout'),
        commit_then_fail=True,
    )

    with pytest.raises(AcceptanceUnavailableError):
        acceptance(store).accept(event_payload())

    assert store.create_calls == 1
    assert store.read_calls == EXPECTED_RECONCILIATION_READS


def test_unknown_commit_without_visible_row_remains_unacknowledged() -> None:
    store = FakeAcceptanceStore(
        create_failure=AcceptanceStoreUnavailable('synthetic timeout')
    )

    with pytest.raises(AcceptanceUnavailableError):
        acceptance(store).accept(event_payload())

    assert store.create_calls == 1
    assert store.read_calls == EXPECTED_RECONCILIATION_READS
