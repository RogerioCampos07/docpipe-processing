import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from docpipe_processing.contracts import DocumentReceivedEvent

pytestmark = pytest.mark.contract

FIXTURE = Path(__file__).parent / 'fixtures' / 'document_received_v1.json'
PAYLOAD_SIZE_BYTES = 128


def valid_data() -> dict[str, object]:
    return {
        'storage_key': 'opaque-object-key-0001',
        'media_type': 'application/pdf',
        'size_bytes': PAYLOAD_SIZE_BYTES,
        'sha256': '0' * 64,
    }


def valid_payload() -> dict[str, object]:
    return {
        'event_id': 'evt-synthetic-0001',
        'event_type': 'document.received',
        'event_version': 1,
        'occurred_at': datetime(2026, 1, 1, 12, tzinfo=UTC),
        'correlation_id': 'corr-synthetic-0001',
        'document_id': 'doc-synthetic-0001',
        'data': valid_data(),
    }


def test_complete_python_payload_validates() -> None:
    event = DocumentReceivedEvent.model_validate(valid_payload())

    assert event.event_id == 'evt-synthetic-0001'
    assert event.event_type == 'document.received'
    assert event.event_version == 1
    assert event.document_id == 'doc-synthetic-0001'
    assert event.data.size_bytes == PAYLOAD_SIZE_BYTES


def test_synthetic_fixture_validates() -> None:
    event = DocumentReceivedEvent.model_validate_json(
        FIXTURE.read_text(encoding='utf-8')
    )

    assert event.event_id == 'evt-synthetic-0001'
    assert event.correlation_id is None
    assert event.data.size_bytes == 0


def test_json_round_trip_preserves_validated_event() -> None:
    event = DocumentReceivedEvent.model_validate(valid_payload())

    recovered = DocumentReceivedEvent.model_validate_json(
        event.model_dump_json()
    )

    assert recovered == event


def test_json_timestamp_is_normalized_to_utc() -> None:
    payload = valid_payload()
    payload['occurred_at'] = '2026-01-01T14:00:00+02:00'

    event = DocumentReceivedEvent.model_validate_json(json.dumps(payload))

    assert event.occurred_at == datetime(2026, 1, 1, 12, tzinfo=UTC)
    assert event.occurred_at.tzinfo is UTC


def test_lowercase_rfc3339_separator_and_utc_marker_are_accepted() -> None:
    payload = valid_payload()
    payload['occurred_at'] = '2026-01-01t12:00:00z'

    event = DocumentReceivedEvent.model_validate_json(json.dumps(payload))

    assert event.occurred_at == datetime(2026, 1, 1, 12, tzinfo=UTC)


@pytest.mark.parametrize(
    'field_name',
    [
        'event_id',
        'event_type',
        'event_version',
        'occurred_at',
        'document_id',
        'data',
    ],
)
def test_missing_required_envelope_field_is_rejected(
    field_name: str,
) -> None:
    payload = valid_payload()
    del payload[field_name]

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize(
    'field_name', ['storage_key', 'media_type', 'size_bytes', 'sha256']
)
def test_missing_required_data_field_is_rejected(field_name: str) -> None:
    payload = valid_payload()
    payload['data'] = {
        key: value for key, value in valid_data().items() if key != field_name
    }

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_incompatible_event_type_is_rejected() -> None:
    payload = valid_payload()
    payload['event_type'] = 'document.processed'

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize('version', [2, 0, '1', True])
def test_incompatible_event_version_is_rejected(version: object) -> None:
    payload = valid_payload()
    payload['event_version'] = version

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize('event_id', ['', ' \t '])
def test_event_id_must_not_be_empty_or_whitespace(event_id: str) -> None:
    payload = valid_payload()
    payload['event_id'] = event_id

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize('document_id', ['', ' \t '])
def test_document_id_must_not_be_empty_or_whitespace(
    document_id: str,
) -> None:
    payload = valid_payload()
    payload['document_id'] = document_id

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_correlation_id_can_be_absent() -> None:
    payload = valid_payload()
    del payload['correlation_id']

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.correlation_id is None


def test_correlation_id_can_be_null() -> None:
    payload = valid_payload()
    payload['correlation_id'] = None

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.correlation_id is None


def test_correlation_id_can_contain_a_valid_string() -> None:
    event = DocumentReceivedEvent.model_validate(valid_payload())

    assert event.correlation_id == 'corr-synthetic-0001'


@pytest.mark.parametrize('correlation_id', ['', ' \t '])
def test_present_correlation_id_must_not_be_empty(
    correlation_id: str,
) -> None:
    payload = valid_payload()
    payload['correlation_id'] = correlation_id

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_naive_occurred_at_is_rejected() -> None:
    payload = valid_payload()
    payload['occurred_at'] = datetime(2026, 1, 1, 12)

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize(
    'timestamp',
    [
        '2026-01-01T12:00:00',
        '2026-01-01 12:00:00Z',
        '2026-01-01T12:00:00.1234567Z',
    ],
)
def test_json_occurred_at_requires_rfc3339_with_timezone(
    timestamp: str,
) -> None:
    payload = valid_payload()
    payload['occurred_at'] = timestamp

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate_json(json.dumps(payload))


@pytest.mark.parametrize('storage_key', ['', ' \t '])
def test_storage_key_must_not_be_empty(storage_key: str) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'storage_key': storage_key}

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_storage_key_is_preserved_as_an_opaque_string() -> None:
    storage_key = 'namespace/folder/object-0001'
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'storage_key': storage_key}

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.data.storage_key == storage_key


@pytest.mark.parametrize(
    'media_type',
    [
        'application/pdf',
        'image/svg+xml',
        'application/pdf; charset=binary',
        'text/plain; title="a document"',
    ],
)
def test_valid_mime_type_is_accepted(media_type: str) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'media_type': media_type}

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.data.media_type == media_type


@pytest.mark.parametrize(
    'media_type',
    [
        '',
        'application',
        '/pdf',
        'application/pdf; charset=',
        'application/pdf; =binary',
        'application/pdf; charset="unterminated',
    ],
)
def test_invalid_mime_type_is_rejected(media_type: str) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'media_type': media_type}

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


@pytest.mark.parametrize('size_bytes', [128, 0])
def test_non_negative_integer_size_is_accepted(size_bytes: int) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'size_bytes': size_bytes}

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.data.size_bytes == size_bytes


@pytest.mark.parametrize('size_bytes', [-1, True, 1.0, '1'])
def test_invalid_size_is_rejected(size_bytes: object) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'size_bytes': size_bytes}

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_lowercase_sha256_is_accepted() -> None:
    sha256 = 'abcdef0123456789' * 4
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'sha256': sha256}

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.data.sha256 == sha256


def test_uppercase_sha256_is_normalized_to_lowercase() -> None:
    sha256 = 'ABCDEF0123456789' * 4
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'sha256': sha256}

    event = DocumentReceivedEvent.model_validate(payload)

    assert event.data.sha256 == sha256.lower()


@pytest.mark.parametrize('sha256', ['a' * 63, 'a' * 65, 'g' * 64])
def test_invalid_sha256_is_rejected(sha256: str) -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'sha256': sha256}

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)


def test_unknown_envelope_field_is_ignored() -> None:
    payload = {**valid_payload(), 'future_metadata': 'ignored'}

    event = DocumentReceivedEvent.model_validate(payload)

    assert 'future_metadata' not in event.model_dump()


def test_unknown_data_field_is_ignored() -> None:
    payload = valid_payload()
    payload['data'] = {**valid_data(), 'future_metadata': 'ignored'}

    event = DocumentReceivedEvent.model_validate(payload)

    assert 'future_metadata' not in event.data.model_dump()


def test_invalid_data_structure_is_rejected() -> None:
    payload = valid_payload()
    payload['data'] = ['not', 'an', 'object']

    with pytest.raises(ValidationError):
        DocumentReceivedEvent.model_validate(payload)
