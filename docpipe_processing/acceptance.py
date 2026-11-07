"""Application use case for durable event acceptance."""

from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Callable, Protocol
from uuid import UUID, uuid4

from pydantic import ValidationError

from docpipe_processing.contracts import DocumentReceivedEvent
from docpipe_processing.domain import ProcessingJob
from docpipe_processing.processing_input import ProcessingInput


class AcceptanceStore(Protocol):
    """Persistence operations needed by event acceptance."""

    def create(
        self, job: ProcessingJob, *, input: ProcessingInput
    ) -> ProcessingJob: ...

    def get_acceptance_by_source_event_id(
        self, source_event_id: str
    ) -> tuple[ProcessingJob, ProcessingInput | None] | None: ...


class PermanentEventError(ValueError):
    """An event cannot be accepted under the public input contract."""


class ConflictingEventError(PermanentEventError):
    """The event identity is already used for different accepted content."""


class IncompleteLegacyEventError(PermanentEventError):
    """An existing job does not prove that its input was durably accepted."""


class AcceptanceStoreError(Exception):
    """Base error exposed by the acceptance persistence port."""


class AcceptanceStoreUnavailable(AcceptanceStoreError):
    """The repository could not complete an operation against PostgreSQL."""


class DuplicateAcceptedEvent(AcceptanceStoreError):
    """The source event key already exists in PostgreSQL."""


class AcceptanceUnavailableError(Exception):
    """The event's durable acceptance has not been established yet."""


class AcceptanceResult(StrEnum):
    ACCEPTED = 'accepted'
    DUPLICATE = 'duplicate'


@dataclass(frozen=True, slots=True)
class AcceptedEvent:
    """Logical outcome returned only after durable acceptance is proven."""

    result: AcceptanceResult
    job: ProcessingJob


def _same_accepted_event(
    event: DocumentReceivedEvent,
    job: ProcessingJob,
    stored_input: ProcessingInput | None,
) -> bool:
    if stored_input is None:
        raise IncompleteLegacyEventError(
            'An existing processing job has no durable input.'
        )
    incoming_input = ProcessingInput.from_event(event)
    return (
        job.source_event_id == event.event_id
        and job.document_id == event.document_id
        and job.correlation_id == event.correlation_id
        and stored_input.storage_key == incoming_input.storage_key
        and stored_input.media_type == incoming_input.media_type
        and stored_input.size_bytes == incoming_input.size_bytes
        and stored_input.sha256 == incoming_input.sha256
    )


class EventAcceptance:
    """Validate and atomically persist one public input event."""

    def __init__(
        self,
        store: AcceptanceStore,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        new_id: Callable[[], UUID] = uuid4,
    ) -> None:
        self._store = store
        self._now = now
        self._new_id = new_id

    def accept(self, body: bytes) -> AcceptedEvent:
        try:
            event = DocumentReceivedEvent.model_validate_json(body)
        except ValidationError as error:
            raise PermanentEventError(
                'The event does not match document.received.v1.'
            ) from error

        existing = self._read_existing(event.event_id)
        if existing is not None:
            return self._classify_existing(event, existing)

        job = ProcessingJob.create(
            processing_id=self._new_id(),
            source_event_id=event.event_id,
            document_id=event.document_id,
            correlation_id=event.correlation_id,
            created_at=self._now(),
        )
        processing_input = ProcessingInput.from_event(event)

        try:
            self._store.create(job, input=processing_input)
        except DuplicateAcceptedEvent:
            existing = self._read_existing(event.event_id)
            if existing is None:
                raise AcceptanceUnavailableError(
                    'A concurrent event could not be reconciled.'
                ) from None
            return self._classify_existing(event, existing)
        except AcceptanceStoreUnavailable:
            return self._reconcile_uncertain_commit(event)
        except AcceptanceStoreError:
            raise

        return AcceptedEvent(AcceptanceResult.ACCEPTED, job)

    def _read_existing(
        self, event_id: str
    ) -> tuple[ProcessingJob, ProcessingInput | None] | None:
        try:
            return self._store.get_acceptance_by_source_event_id(event_id)
        except AcceptanceStoreUnavailable:
            raise AcceptanceUnavailableError(
                'PostgreSQL is unavailable for acceptance.'
            ) from None

    def _reconcile_uncertain_commit(
        self, event: DocumentReceivedEvent
    ) -> AcceptedEvent:
        existing = self._read_existing(event.event_id)
        if existing is None:
            raise AcceptanceUnavailableError(
                'The event commit outcome remains unknown.'
            ) from None
        return self._classify_existing(event, existing)

    @staticmethod
    def _classify_existing(
        event: DocumentReceivedEvent,
        existing: tuple[ProcessingJob, ProcessingInput | None],
    ) -> AcceptedEvent:
        job, stored_input = existing
        if not _same_accepted_event(event, job, stored_input):
            raise ConflictingEventError(
                'The event ID is already associated with different content.'
            )
        return AcceptedEvent(AcceptanceResult.DUPLICATE, job)
