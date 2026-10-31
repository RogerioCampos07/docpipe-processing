"""Transport-independent queries for Processing-owned resources."""

from typing import Protocol
from uuid import UUID

from docpipe_processing.domain import ProcessingJob


class ProcessingJobReader(Protocol):
    """The single persistence operation needed by the job query."""

    def get_by_processing_id(
        self, processing_id: UUID
    ) -> ProcessingJob | None: ...


def get_processing_job(
    processing_id: UUID, reader: ProcessingJobReader
) -> ProcessingJob | None:
    """Read the current snapshot without changing state or accepting input."""
    return reader.get_by_processing_id(processing_id)
