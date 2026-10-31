"""Application queries require only domain objects and a small fake."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

import pytest

from docpipe_processing.application import get_processing_job
from docpipe_processing.domain import ProcessingJob

pytestmark = pytest.mark.unit

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')


@dataclass
class FakeJobReader:
    job: ProcessingJob | None = None
    failure: Exception | None = None
    queries: list[UUID] = field(default_factory=list)

    def get_by_processing_id(
        self, processing_id: UUID
    ) -> ProcessingJob | None:
        self.queries.append(processing_id)
        if self.failure is not None:
            raise self.failure
        return self.job


def test_query_returns_domain_snapshot_without_mutation() -> None:
    job = ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id='synthetic-event',
        document_id='synthetic-document',
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    reader = FakeJobReader(job)

    assert get_processing_job(PROCESSING_ID, reader) is job
    assert reader.queries == [PROCESSING_ID]
    assert job.attempts == 0


def test_query_returns_none_for_missing_job() -> None:
    reader = FakeJobReader()

    assert get_processing_job(PROCESSING_ID, reader) is None
    assert reader.queries == [PROCESSING_ID]


def test_query_propagates_failure_without_reclassifying_it() -> None:
    failure = RuntimeError('synthetic persistence failure')
    reader = FakeJobReader(failure=failure)

    with pytest.raises(RuntimeError) as caught:
        get_processing_job(PROCESSING_ID, reader)

    assert caught.value is failure
    assert reader.queries == [PROCESSING_ID]
