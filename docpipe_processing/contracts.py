"""Public input event contracts implemented by Processing."""

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

_RFC3339_TIMESTAMP = re.compile(
    r'\A[0-9]{4}-[0-9]{2}-[0-9]{2}[Tt]'
    r'[0-9]{2}:[0-9]{2}:[0-9]{2}'
    r'(?:\.[0-9]{1,6})?(?:[Zz]|[+-][0-9]{2}:[0-9]{2})\Z'
)
_MIME_TOKEN = r"[A-Za-z0-9!#$%&'*+.^_`|~-]+"
_MIME_QUOTED_STRING = (
    r'"(?:[\t\x20\x21\x23-\x5B\x5D-\x7E]|'
    r'\\[\t\x20-\x7E])*"'
)
_MIME_TYPE = re.compile(
    rf'\A{_MIME_TOKEN}/{_MIME_TOKEN}'
    rf'(?:[ \t]*;[ \t]*{_MIME_TOKEN}[ \t]*=[ \t]*'
    rf'(?:{_MIME_TOKEN}|{_MIME_QUOTED_STRING}))*[ \t]*\Z'
)
_SHA256 = re.compile(r'[0-9a-fA-F]{64}\Z')


def _require_nonblank(value: str, field_name: str) -> str:
    if not value.strip():
        raise ValueError(f'{field_name} must not be empty or whitespace.')
    return value


class DocumentReceivedData(BaseModel):
    """Document reference and metadata carried by document.received.v1."""

    model_config = ConfigDict(strict=True, extra='ignore')

    storage_key: str
    media_type: str
    size_bytes: int = Field(ge=0, strict=True)
    sha256: str

    @field_validator('storage_key')
    @classmethod
    def validate_storage_key(cls, value: str) -> str:
        return _require_nonblank(value, 'storage_key')

    @field_validator('media_type')
    @classmethod
    def validate_media_type(cls, value: str) -> str:
        if _MIME_TYPE.fullmatch(value) is None:
            raise ValueError('media_type must be a valid MIME type.')
        return value

    @field_validator('sha256')
    @classmethod
    def normalize_sha256(cls, value: str) -> str:
        if _SHA256.fullmatch(value) is None:
            raise ValueError(
                'sha256 must contain exactly 64 hexadecimal characters.'
            )
        return value.lower()


class DocumentReceivedEvent(BaseModel):
    """Validated Processing representation of document.received.v1."""

    model_config = ConfigDict(strict=True, extra='ignore')

    event_id: str
    event_type: Literal['document.received']
    event_version: Literal[1]
    occurred_at: datetime
    correlation_id: str | None = None
    document_id: str
    data: DocumentReceivedData

    @field_validator('event_id')
    @classmethod
    def validate_event_id(cls, value: str) -> str:
        return _require_nonblank(value, 'event_id')

    @field_validator('document_id')
    @classmethod
    def validate_document_id(cls, value: str) -> str:
        return _require_nonblank(value, 'document_id')

    @field_validator('correlation_id')
    @classmethod
    def validate_correlation_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _require_nonblank(value, 'correlation_id')

    @field_validator('occurred_at', mode='before')
    @classmethod
    def validate_rfc3339_timestamp(cls, value: object) -> object:
        if isinstance(value, datetime):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError('occurred_at must include a timezone.')
            return value
        if isinstance(value, str) and _RFC3339_TIMESTAMP.fullmatch(value):
            normalized_timestamp = value.replace('Z', '+00:00').replace(
                'z', '+00:00'
            )
            try:
                return datetime.fromisoformat(normalized_timestamp)
            except ValueError as error:
                raise ValueError(
                    'occurred_at must be a valid RFC 3339 timestamp.'
                ) from error
        raise ValueError(
            'occurred_at must be an RFC 3339 timestamp with a timezone '
            'and at most six fractional digits.'
        )

    @field_validator('event_version', mode='before')
    @classmethod
    def validate_event_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError('event_version must be the integer 1.')
        return value

    @field_validator('occurred_at')
    @classmethod
    def normalize_occurred_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError('occurred_at must include a timezone.')
        return value.astimezone(UTC)
