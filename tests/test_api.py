"""HTTP contracts, privacy and lifecycle without a running database."""

import logging
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from http import HTTPStatus
from typing import Iterator
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from sqlalchemy import Engine
from sqlalchemy.pool import QueuePool

import docpipe_processing.app as app_module
from docpipe_processing.api import get_job_reader
from docpipe_processing.app import app
from docpipe_processing.domain import ProcessingJob, ProcessingStatus
from docpipe_processing.persistence import (
    DATABASE_URL_ENV_VAR,
    PersistenceError,
    PersistenceUnavailableError,
    create_database_engine,
)
from tests.test_application import FakeJobReader

pytestmark = pytest.mark.integration

PROCESSING_ID = UUID('12345678-1234-1678-9234-567812345678')
PATH = f'/v1/processing-jobs/{PROCESSING_ID}'
PRIVATE_REASON = 'synthetic-document-text password=synthetic-private-secret'
PUBLIC_FIELDS = {
    'processing_id',
    'source_event_id',
    'document_id',
    'correlation_id',
    'status',
    'attempts',
    'created_at',
    'updated_at',
    'started_at',
    'finished_at',
    'failure_reason',
}


def snapshot(
    status: ProcessingStatus = ProcessingStatus.RECEIVED,
) -> ProcessingJob:
    job = ProcessingJob.create(
        processing_id=PROCESSING_ID,
        source_event_id=' evento/ç?+ ',
        document_id=' documento/文# ',
        created_at=datetime(
            2026, 1, 1, 14, tzinfo=timezone(timedelta(hours=2))
        ),
    )
    if status is ProcessingStatus.RECEIVED:
        return job
    job = job.start_processing(at=datetime(2026, 1, 1, 12, 1, tzinfo=UTC))
    if status is ProcessingStatus.PROCESSING:
        return job
    finished_at = datetime(2026, 1, 1, 12, 2, tzinfo=UTC)
    if status is ProcessingStatus.COMPLETED:
        return job.complete(at=finished_at)
    return job.fail(reason=PRIVATE_REASON, at=finished_at)


@pytest.fixture
def reader() -> FakeJobReader:
    return FakeJobReader(snapshot())


@pytest.fixture
def client(
    reader: FakeJobReader, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    monkeypatch.delenv(DATABASE_URL_ENV_VAR, raising=False)
    monkeypatch.setitem(
        app.dependency_overrides, get_job_reader, lambda: reader
    )
    with TestClient(app) as http:
        yield http


@pytest.mark.parametrize('status', list(ProcessingStatus))
def test_public_snapshot_is_explicit_safe_and_read_only(
    client: TestClient, reader: FakeJobReader, status: ProcessingStatus
) -> None:
    job = snapshot(status)
    reader.job = job

    first = client.get(PATH)
    second = client.get(PATH)

    assert first.status_code == HTTPStatus.OK
    assert first.headers['cache-control'] == 'no-store'
    assert first.headers['content-type'] == 'application/json'
    body = first.json()
    assert set(body) == PUBLIC_FIELDS
    assert body['processing_id'] == str(PROCESSING_ID)
    assert body['source_event_id'] == job.source_event_id
    assert body['document_id'] == job.document_id
    assert body['correlation_id'] is None
    assert body['status'] == status.value
    assert body['attempts'] == job.attempts
    assert body['created_at'] == '2026-01-01T12:00:00Z'
    assert body['failure_reason'] == (
        'O processamento falhou.'
        if status is ProcessingStatus.FAILED
        else None
    )
    for name in ('updated_at', 'started_at', 'finished_at'):
        value = getattr(job, name)
        assert body[name] == (
            value.isoformat().replace('+00:00', 'Z')
            if value is not None
            else None
        )
    assert PRIVATE_REASON not in first.text
    assert second.json() == body
    assert reader.job is job
    assert reader.queries == [PROCESSING_ID, PROCESSING_ID]


def test_string_correlation_is_preserved(
    client: TestClient, reader: FakeJobReader
) -> None:
    reader.job = replace(snapshot(), correlation_id=' correlação/opaque ')
    assert client.get(PATH).json()['correlation_id'] == ' correlação/opaque '


@pytest.mark.parametrize(
    'identifier',
    [
        '12345678-1234-1678-9234-567812345678',
        '12345678-1234-4678-9234-567812345678',
        '12345678-1234-5678-9234-567812345678',
        '00000000-0000-0000-0000-000000000000',
    ],
)
def test_uuid_version_is_not_restricted(
    client: TestClient, reader: FakeJobReader, identifier: str
) -> None:
    reader.job = replace(snapshot(), processing_id=UUID(identifier))
    response = client.get(f'/v1/processing-jobs/{identifier}')
    assert response.status_code == HTTPStatus.OK
    assert response.json()['processing_id'] == identifier
    assert reader.queries == [UUID(identifier)]


def test_missing_job_returns_public_error(
    client: TestClient, reader: FakeJobReader
) -> None:
    reader.job = None
    response = client.get(PATH)
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        'code': 'processing_job_not_found',
        'message': 'Processamento não encontrado.',
    }


@pytest.mark.parametrize('identifier', ['invalid-private-value', '12345'])
def test_invalid_uuid_never_queries_repository(
    client: TestClient, reader: FakeJobReader, identifier: str
) -> None:
    response = client.get(f'/v1/processing-jobs/{identifier}')
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        'code': 'invalid_processing_id',
        'message': 'Identificador inválido.',
    }
    assert identifier not in response.text
    assert reader.queries == []


@pytest.fixture
def spans(
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[InMemorySpanExporter]:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(app_module, 'TRACER', provider.get_tracer('test'))
    yield exporter
    provider.shutdown()


@pytest.mark.parametrize(
    'case',
    [
        (
            PersistenceUnavailableError(PRIVATE_REASON),
            503,
            'persistence_unavailable',
        ),
        (
            PersistenceError('SELECT secret ' + PRIVATE_REASON),
            500,
            'internal_error',
        ),
        (RuntimeError(PRIVATE_REASON), 500, 'internal_error'),
    ],
)
def test_failures_are_safe_in_responses_logs_and_traces(
    client: TestClient,
    reader: FakeJobReader,
    caplog: pytest.LogCaptureFixture,
    spans: InMemorySpanExporter,
    case: tuple[Exception, int, str],
) -> None:
    failure, expected_status, expected_code = case
    reader.failure = failure
    response = client.get(PATH)
    assert response.status_code == expected_status
    assert response.json()['code'] == expected_code
    assert set(response.json()) == {'code', 'message'}
    assert PRIVATE_REASON not in response.text
    assert 'SELECT' not in response.text
    assert PRIVATE_REASON not in caplog.text
    assert PRIVATE_REASON not in repr([
        dict(span.attributes or {}) for span in spans.get_finished_spans()
    ])
    assert all(not span.events for span in spans.get_finished_spans())


def test_failed_reason_stays_private_in_success_logs_and_traces(
    client: TestClient,
    reader: FakeJobReader,
    caplog: pytest.LogCaptureFixture,
    spans: InMemorySpanExporter,
) -> None:
    reader.job = snapshot(ProcessingStatus.FAILED)
    caplog.set_level(logging.INFO)
    response = client.get(PATH)
    assert response.status_code == HTTPStatus.OK
    assert response.json()['failure_reason'] == 'O processamento falhou.'
    assert PRIVATE_REASON not in response.text
    assert PRIVATE_REASON not in caplog.text
    for span in spans.get_finished_spans():
        assert PRIVATE_REASON not in repr(dict(span.attributes or {}))
        assert not span.events
    assert reader.job.failure_reason == PRIVATE_REASON


@pytest.mark.parametrize(
    'url', [None, '', 'invalid-private-secret', 'sqlite:///private.db']
)
def test_missing_or_invalid_configuration_preserves_operational_endpoints(
    monkeypatch: pytest.MonkeyPatch, url: str | None
) -> None:
    monkeypatch.delenv(DATABASE_URL_ENV_VAR, raising=False)
    if url is not None:
        monkeypatch.setenv(DATABASE_URL_ENV_VAR, url)
    with TestClient(app) as client:
        response = client.get(PATH)
        assert response.status_code == HTTPStatus.SERVICE_UNAVAILABLE
        assert response.json()['code'] == 'persistence_unavailable'
        assert 'private' not in response.text
        assert client.get('/health').json() == {'status': 'ok'}
        assert client.get('/metrics').status_code == HTTPStatus.OK
        assert client.get('/v1/processing-jobs/invalid').status_code == (
            HTTPStatus.UNPROCESSABLE_ENTITY
        )


def test_lazy_engine_is_shared_disposed_and_unavailable_database_returns_503(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_database_engine(
        'postgresql+psycopg://synthetic:private-secret@127.0.0.1:1/'
        'processing_test'
    )
    original_pool = engine.pool
    creations: list[bool] = []

    def engine_factory() -> Engine:
        creations.append(True)
        return engine

    monkeypatch.setattr(app_module, 'create_database_engine', engine_factory)
    with TestClient(app) as client:
        assert isinstance(engine.pool, QueuePool)
        assert engine.pool.checkedout() == 0
        for _ in range(2):
            response = client.get(PATH)
            assert response.status_code == HTTPStatus.SERVICE_UNAVAILABLE
            assert response.json()['code'] == 'persistence_unavailable'
            assert 'private-secret' not in response.text
        assert client.get('/health').status_code == HTTPStatus.OK
        assert client.get('/metrics').status_code == HTTPStatus.OK
    assert creations == [True]
    assert engine.pool is not original_pool
    assert app.state.job_reader is None


def test_domain_observability_uses_route_template_and_trace_context(
    client: TestClient,
    caplog: pytest.LogCaptureFixture,
    spans: InMemorySpanExporter,
) -> None:
    trace_id = '0123456789abcdef0123456789abcdef'
    caplog.set_level(logging.INFO, logger='docpipe_processing.http')
    response = client.get(
        PATH, headers={'traceparent': f'00-{trace_id}-0123456789abcdef-01'}
    )
    assert response.status_code == HTTPStatus.OK
    events = [
        record.__dict__['structured_event']
        for record in caplog.records
        if record.name == 'docpipe_processing.http'
    ]
    assert events[-1]['trace_id'] == trace_id
    assert events[-1]['route'] == '/v1/processing-jobs/{processing_id}'
    metrics = client.get('/metrics').text
    assert 'route="/v1/processing-jobs/{processing_id}"' in metrics
    assert str(PROCESSING_ID) not in metrics
    assert spans.get_finished_spans()[0].name == (
        'GET /v1/processing-jobs/{processing_id}'
    )


def test_openapi_describes_only_approved_domain_operation(
    client: TestClient,
) -> None:
    schema = client.get('/openapi.json').json()
    paths = schema['paths']
    assert set(paths) == {'/health', '/v1/processing-jobs/{processing_id}'}
    operation = paths['/v1/processing-jobs/{processing_id}']
    assert set(operation) == {'get'}
    endpoint = operation['get']
    assert endpoint['parameters'][0]['schema']['format'] == 'uuid'
    assert 'requestBody' not in endpoint
    assert set(endpoint['responses']) == {'200', '404', '422', '500', '503'}
    components = schema['components']['schemas']
    public = components['ProcessingJobResponse']
    assert set(public['properties']) == PUBLIC_FIELDS
    assert set(public['required']) == PUBLIC_FIELDS
    assert public['properties']['attempts']['minimum'] == 0
    assert components['ProcessingStatus']['enum'] == [
        status.value for status in ProcessingStatus
    ]
    for name in (
        'correlation_id',
        'started_at',
        'finished_at',
        'failure_reason',
    ):
        assert {'type': 'null'} in public['properties'][name]['anyOf']
    for status in ('404', '422', '500', '503'):
        assert endpoint['responses'][status]['content']['application/json'][
            'schema'
        ] == {'$ref': '#/components/schemas/ErrorResponse'}
    assert set(components['ErrorResponse']['properties']) == {
        'code',
        'message',
    }
    assert 'ProcessingJobModel' not in components
    assert 'storage_key' not in str(schema)
