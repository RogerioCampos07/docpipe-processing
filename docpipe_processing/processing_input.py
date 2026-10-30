"""Typed, immutable input needed to retrieve a document after acceptance."""

from pydantic import ConfigDict

from docpipe_processing.contracts import (
    DocumentReceivedData,
    DocumentReceivedEvent,
)


class ProcessingInput(DocumentReceivedData):
    """Processing-owned input, reusing the public contract's validation."""

    model_config = ConfigDict(strict=True, extra='ignore', frozen=True)

    @classmethod
    def from_event(cls, event: DocumentReceivedEvent) -> 'ProcessingInput':
        return cls.model_validate(event.data.model_dump())
