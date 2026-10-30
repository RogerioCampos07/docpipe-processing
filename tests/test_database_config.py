"""Configuration and typed input tests require no running database."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from docpipe_processing.contracts import (
    DocumentReceivedData,
    DocumentReceivedEvent,
)
from docpipe_processing.persistence import (
    DATABASE_URL_ENV_VAR,
    PersistenceConfigurationError,
    create_database_engine,
    get_database_url,
)
from docpipe_processing.processing_input import ProcessingInput

pytestmark = pytest.mark.unit


@pytest.mark.parametrize('value', [None, '', '   '])
def test_missing_database_url_fails_explicitly(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    monkeypatch.delenv(DATABASE_URL_ENV_VAR, raising=False)
    if value is not None:
        monkeypatch.setenv(DATABASE_URL_ENV_VAR, value)
    with pytest.raises(PersistenceConfigurationError):
        get_database_url()


@pytest.mark.parametrize(
    'value',
    [
        'invalid-secret-value',
        'sqlite:///processing.db',
        'postgresql://user:secret@localhost/processing',
        'postgresql+psycopg://user:secret@localhost',
        'postgresql+psycopg://user:secret@localhost:invalid/processing',
    ],
)
def test_invalid_database_url_is_rejected_without_exposing_it(
    value: str,
) -> None:
    with pytest.raises(PersistenceConfigurationError) as error:
        create_database_engine(value)
    assert value not in str(error.value)
    assert 'secret' not in str(error.value)


def test_engine_configuration_is_lazy_and_uses_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    url = 'postgresql+psycopg://user:secret@127.0.0.1:1/processing'
    monkeypatch.setenv(DATABASE_URL_ENV_VAR, url)
    assert get_database_url() == url
    engine = create_database_engine()
    try:
        assert engine.url.drivername == 'postgresql+psycopg'
        assert engine.hide_parameters is True
    finally:
        engine.dispose()


def test_processing_input_reuses_contract_validation_and_is_immutable() -> (
    None
):
    event = DocumentReceivedEvent(
        event_id='event-1',
        event_type='document.received',
        event_version=1,
        document_id='document-1',
        occurred_at=datetime(2026, 1, 1, tzinfo=UTC),
        data=DocumentReceivedData(
            storage_key='opaque/object',
            media_type='application/pdf',
            size_bytes=0,
            sha256='A' * 64,
        ),
    )
    input = ProcessingInput.from_event(event)
    assert input.sha256 == 'a' * 64
    assert input.size_bytes == 0
    assert input.storage_key == 'opaque/object'
    with pytest.raises(ValidationError, match='frozen'):
        input.storage_key = 'changed'
    with pytest.raises(ValidationError):
        ProcessingInput.model_validate({
            **input.model_dump(),
            'size_bytes': True,
        })
