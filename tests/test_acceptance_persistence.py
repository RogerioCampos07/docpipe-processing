"""PostgreSQL-backed fault injection for uncertain acceptance commits."""

import json
from dataclasses import dataclass

import pytest

from docpipe_processing.acceptance import (
    AcceptanceStoreUnavailable,
    AcceptanceUnavailableError,
    EventAcceptance,
)
from docpipe_processing.domain import ProcessingJob
from docpipe_processing.persistence import (
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)
from docpipe_processing.processing_input import ProcessingInput
from tests.conftest import DatabaseFixture

pytestmark = pytest.mark.persistence
RECONCILIATION_READ_INDEX = 2


@dataclass
class RepositoryReader:
    repository: ProcessingJobRepository

    def get_acceptance_by_source_event_id(
        self, source_event_id: str
    ) -> tuple[ProcessingJob, ProcessingInput | None] | None:
        return self.repository.get_acceptance_by_source_event_id(
            source_event_id
        )


def _event(event_id: str) -> bytes:
    return json.dumps({
        'event_id': event_id,
        'event_type': 'document.received',
        'event_version': 1,
        'occurred_at': '2026-01-01T00:00:00Z',
        'correlation_id': None,
        'document_id': f'document-{event_id}',
        'data': {
            'storage_key': f'key-{event_id}',
            'media_type': 'application/pdf',
            'size_bytes': 0,
            'sha256': 'a' * 64,
        },
    }).encode()


def test_postgres_commit_response_loss_reconciles_durable_duplicate(
    database: DatabaseFixture,
) -> None:
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))

    class CommitThenLoseResponse(RepositoryReader):
        def create(
            self, job: ProcessingJob, *, input: ProcessingInput
        ) -> ProcessingJob:
            self.repository.create(job, input=input)
            raise AcceptanceStoreUnavailable(
                'fault injected after PostgreSQL commit'
            ) from None

    try:
        result = EventAcceptance(CommitThenLoseResponse(repository)).accept(
            _event('commit-response-lost')
        )

        assert result.result.value == 'duplicate'
        stored = repository.get_acceptance_by_source_event_id(
            'commit-response-lost'
        )
        assert stored is not None
        assert stored[0] == result.job
        assert stored[1] is not None
    finally:
        engine.dispose()


def test_postgres_commit_not_applied_stays_unaccepted_until_redelivery(
    database: DatabaseFixture,
) -> None:
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))

    class FailBeforeCommit(RepositoryReader):
        def create(
            self, job: ProcessingJob, *, input: ProcessingInput
        ) -> ProcessingJob:
            assert (
                self.repository.get_acceptance_by_source_event_id(
                    job.source_event_id
                )
                is None
            )
            del input
            raise AcceptanceStoreUnavailable(
                'fault injected before PostgreSQL write'
            ) from None

    try:
        with pytest.raises(AcceptanceUnavailableError):
            EventAcceptance(FailBeforeCommit(repository)).accept(
                _event('commit-absent')
            )

        assert (
            repository.get_acceptance_by_source_event_id('commit-absent')
            is None
        )
        assert (
            EventAcceptance(repository)
            .accept(_event('commit-absent'))
            .result.value
            == 'accepted'
        )
    finally:
        engine.dispose()


def test_postgres_reconciliation_unavailable_does_not_report_acceptance(
    database: DatabaseFixture,
) -> None:
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))

    @dataclass
    class ReconciliationUnavailable(RepositoryReader):
        reads: int = 0

        def create(
            self, job: ProcessingJob, *, input: ProcessingInput
        ) -> ProcessingJob:
            self.repository.create(job, input=input)
            raise AcceptanceStoreUnavailable(
                'fault injected after PostgreSQL commit'
            ) from None

        def get_acceptance_by_source_event_id(
            self, source_event_id: str
        ) -> tuple[ProcessingJob, ProcessingInput | None] | None:
            self.reads += 1
            if self.reads == RECONCILIATION_READ_INDEX:
                raise AcceptanceStoreUnavailable(
                    'fault injected during reconciliation'
                )
            return super().get_acceptance_by_source_event_id(source_event_id)

    try:
        with pytest.raises(AcceptanceUnavailableError):
            EventAcceptance(ReconciliationUnavailable(repository)).accept(
                _event('reconciliation-unavailable')
            )

        stored = repository.get_acceptance_by_source_event_id(
            'reconciliation-unavailable'
        )
        assert stored is not None
        assert stored[1] is not None
    finally:
        engine.dispose()
