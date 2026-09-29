"""Domain model for a single document processing job."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum
from uuid import UUID


class ProcessingStatus(StrEnum):
    """Lifecycle states supported by the Processing domain."""

    RECEIVED = 'RECEIVED'
    PROCESSING = 'PROCESSING'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class ProcessingDomainError(ValueError):
    """Raised when a processing job violates a domain invariant."""


class InvalidProcessingTransition(ProcessingDomainError):
    """Raised when a job is asked to make an unsupported state transition."""

    def __init__(
        self,
        current_status: ProcessingStatus,
        requested_status: ProcessingStatus,
    ) -> None:
        self.current_status = current_status
        self.requested_status = requested_status
        super().__init__(
            'Cannot transition processing job from '
            f'{current_status.value} to {requested_status.value}.'
        )


def _as_utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProcessingDomainError(f'{field_name} must be timezone-aware.')
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class ProcessingJob:
    """Immutable processing job; state changes return a new job snapshot."""

    processing_id: UUID
    source_event_id: str
    document_id: str
    correlation_id: str | None
    status: ProcessingStatus
    attempts: int
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    failure_reason: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.processing_id, UUID):
            raise ProcessingDomainError('processing_id must be a UUID.')
        if (
            not isinstance(self.source_event_id, str)
            or not self.source_event_id.strip()
        ):
            raise ProcessingDomainError('source_event_id is required.')
        if (
            not isinstance(self.document_id, str)
            or not self.document_id.strip()
        ):
            raise ProcessingDomainError('document_id is required.')
        if self.correlation_id is not None and (
            not isinstance(self.correlation_id, str)
            or not self.correlation_id.strip()
        ):
            raise ProcessingDomainError(
                'correlation_id must be non-empty when provided.'
            )
        if not isinstance(self.status, ProcessingStatus):
            raise ProcessingDomainError('status must be a ProcessingStatus.')
        if (
            not isinstance(self.attempts, int)
            or isinstance(self.attempts, bool)
            or self.attempts < 0
        ):
            raise ProcessingDomainError(
                'attempts must be a non-negative integer.'
            )

        created_at = _as_utc(self.created_at, 'created_at')
        updated_at = _as_utc(self.updated_at, 'updated_at')
        started_at = (
            _as_utc(self.started_at, 'started_at')
            if self.started_at is not None
            else None
        )
        finished_at = (
            _as_utc(self.finished_at, 'finished_at')
            if self.finished_at is not None
            else None
        )

        object.__setattr__(self, 'created_at', created_at)
        object.__setattr__(self, 'updated_at', updated_at)
        object.__setattr__(self, 'started_at', started_at)
        object.__setattr__(self, 'finished_at', finished_at)

        if updated_at < created_at:
            raise ProcessingDomainError(
                'updated_at cannot precede created_at.'
            )

        self._validate_status_invariants(
            created_at=created_at,
            updated_at=updated_at,
            started_at=started_at,
            finished_at=finished_at,
        )

        if started_at is not None and started_at < created_at:
            raise ProcessingDomainError(
                'started_at cannot precede created_at.'
            )
        if finished_at is not None and (
            started_at is None or finished_at < started_at
        ):
            raise ProcessingDomainError(
                'finished_at cannot precede started_at.'
            )

    def _validate_status_invariants(
        self,
        *,
        created_at: datetime,
        updated_at: datetime,
        started_at: datetime | None,
        finished_at: datetime | None,
    ) -> None:
        if self.status is ProcessingStatus.RECEIVED:
            valid = (
                self.attempts == 0
                and started_at is None
                and finished_at is None
                and self.failure_reason is None
                and updated_at == created_at
            )
            message = 'A received job must not have started or finished.'
        elif self.status is ProcessingStatus.PROCESSING:
            valid = (
                self.attempts >= 1
                and started_at is not None
                and finished_at is None
                and self.failure_reason is None
                and updated_at == started_at
            )
            message = 'A processing job must have a start time and no finish.'
        elif self.status is ProcessingStatus.COMPLETED:
            valid = (
                self.attempts >= 1
                and started_at is not None
                and finished_at is not None
                and self.failure_reason is None
                and updated_at == finished_at
            )
            message = 'A completed job must have start and finish times.'
        else:
            valid = (
                self.attempts >= 1
                and started_at is not None
                and finished_at is not None
                and updated_at == finished_at
            )
            message = 'A failed job must have start and finish times.'
            if self.failure_reason is None or not self.failure_reason.strip():
                raise ProcessingDomainError(
                    'A failed job must have a failure reason.'
                )

        if not valid:
            raise ProcessingDomainError(message)

    @classmethod
    def create(
        cls,
        *,
        processing_id: UUID,
        source_event_id: str,
        document_id: str,
        created_at: datetime,
        correlation_id: str | None = None,
    ) -> 'ProcessingJob':
        """Create a received job from caller-supplied identity and UTC time."""
        return cls(
            processing_id=processing_id,
            source_event_id=source_event_id,
            document_id=document_id,
            correlation_id=correlation_id,
            status=ProcessingStatus.RECEIVED,
            attempts=0,
            created_at=created_at,
            updated_at=created_at,
        )

    def start_processing(self, *, at: datetime) -> 'ProcessingJob':
        self._require_status(
            ProcessingStatus.RECEIVED,
            ProcessingStatus.PROCESSING,
        )
        transition_at = _as_utc(at, 'at')
        self._require_monotonic_time(transition_at)
        return replace(
            self,
            status=ProcessingStatus.PROCESSING,
            attempts=self.attempts + 1,
            updated_at=transition_at,
            started_at=transition_at,
        )

    def complete(self, *, at: datetime) -> 'ProcessingJob':
        self._require_status(
            ProcessingStatus.PROCESSING,
            ProcessingStatus.COMPLETED,
        )
        transition_at = _as_utc(at, 'at')
        self._require_monotonic_time(transition_at)
        return replace(
            self,
            status=ProcessingStatus.COMPLETED,
            updated_at=transition_at,
            finished_at=transition_at,
        )

    def fail(self, *, reason: str, at: datetime) -> 'ProcessingJob':
        self._require_status(
            ProcessingStatus.PROCESSING,
            ProcessingStatus.FAILED,
        )
        if not isinstance(reason, str) or not reason.strip():
            raise ProcessingDomainError('failure reason is required.')
        transition_at = _as_utc(at, 'at')
        self._require_monotonic_time(transition_at)
        return replace(
            self,
            status=ProcessingStatus.FAILED,
            updated_at=transition_at,
            finished_at=transition_at,
            failure_reason=reason.strip(),
        )

    def _require_status(
        self,
        expected: ProcessingStatus,
        requested: ProcessingStatus,
    ) -> None:
        if self.status is not expected:
            raise InvalidProcessingTransition(self.status, requested)

    def _require_monotonic_time(self, transition_at: datetime) -> None:
        if transition_at < self.updated_at:
            raise ProcessingDomainError(
                'A transition timestamp cannot precede updated_at.'
            )
